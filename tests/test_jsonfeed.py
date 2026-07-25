from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.jsonfeed import JSONFeedCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorError


FEED = {
    "version": "https://jsonfeed.org/version/1.1",
    "title": "Example Feed",
    "home_page_url": "https://example.com/",
    "feed_url": "https://example.com/feed.json",
    "items": [
        {
            "id": "item-1",
            "url": "https://example.com/posts/1",
            "external_url": "https://outside.example/1",
            "title": "Need a better CRM",
            "content_html": "<p>Looking for a <strong>simple</strong> CRM.</p>",
            "summary": "CRM request",
            "date_published": "2026-07-01T10:00:00Z",
            "date_modified": "2026-07-01T11:00:00+00:00",
            "authors": [{"name": "Alice"}, {"name": "Bob"}],
            "tags": ["saas", "crm"],
            "language": "en",
            "attachments": [{"url": "https://example.com/file.pdf", "mime_type": "application/pdf"}],
        },
        {
            "id": "item-2",
            "title": "Second",
            "content_text": "Second body",
            "date_published": "2026-07-02T10:00:00Z",
            "author": {"name": "Charlie"},
        },
        {
            "id": "item-3",
            "title": "Third",
            "summary": "Third summary",
        },
    ],
}


@pytest.mark.asyncio
async def test_json_feed_normalizes_11_items() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=FEED, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        result = await collector.collect(context=CollectorContext(limit=10))

    assert result.primary_count == 3
    assert result.has_more is False
    event = result.events[0]
    assert event.kind is SignalKind.ARTICLE
    assert event.title == "Need a better CRM"
    assert event.content == "Looking for a simple CRM."
    assert event.author == "Alice, Bob"
    assert event.url == "https://example.com/posts/1"
    assert event.created_at == datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    assert event.updated_at == datetime(2026, 7, 1, 11, 0, tzinfo=UTC)
    assert event.metadata["external_url"] == "https://outside.example/1"
    assert event.metadata["tags"] == ["saas", "crm"]
    assert result.events[2].created_at == datetime.fromtimestamp(0, tz=UTC)


@pytest.mark.asyncio
async def test_json_feed_drains_large_feed_across_calls_without_skipping() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=FEED, headers={"ETag": '"v1"'}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        first = await collector.collect(context=CollectorContext(limit=2))
        second = await collector.collect(context=CollectorContext(limit=2), cursor=first.cursor)

    assert [event.metadata["external_id"] for event in first.events] == ["item-1", "item-2"]
    assert first.has_more is True
    assert first.cursor.state["cycle_ids"] == ["item-1", "item-2"]
    assert first.cursor.state["pending_etag"] == '"v1"'

    assert [event.metadata["external_id"] for event in second.events] == ["item-3"]
    assert second.has_more is False
    assert second.cursor.state["cycle_ids"] == []
    assert second.cursor.state["seen_ids"][:3] == ["item-1", "item-2", "item-3"]
    assert second.cursor.state["etag"] == '"v1"'
    assert "If-None-Match" not in requests[1].headers


@pytest.mark.asyncio
async def test_json_feed_conditional_get_after_cycle_is_complete() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json=FEED,
                headers={"ETag": '"v1"', "Last-Modified": "Sat, 25 Jul 2026 12:00:00 GMT"},
                request=request,
            )
        assert request.headers["If-None-Match"] == '"v1"'
        assert request.headers["If-Modified-Since"] == "Sat, 25 Jul 2026 12:00:00 GMT"
        return httpx.Response(304, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        first = await collector.collect(context=CollectorContext(limit=10))
        second = await collector.collect(context=CollectorContext(limit=10), cursor=first.cursor)

    assert second.events == []
    assert second.cursor == first.cursor


@pytest.mark.asyncio
async def test_json_feed_stops_at_previous_seen_item() -> None:
    feeds = [
        {**FEED, "items": FEED["items"][:2]},
        {
            **FEED,
            "items": [
                {"id": "new-1", "content_text": "New", "date_published": "2026-07-03T10:00:00Z"},
                FEED["items"][0],
                FEED["items"][1],
            ],
        },
    ]
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        payload = feeds[calls]
        calls += 1
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        baseline = await collector.collect(context=CollectorContext(limit=10))
        incremental = await collector.collect(context=CollectorContext(limit=10), cursor=baseline.cursor)

    assert [event.metadata["external_id"] for event in incremental.events] == ["new-1"]
    assert incremental.has_more is False
    assert incremental.cursor.state["seen_ids"][0] == "new-1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,match",
    [
        ("not-json", "not valid JSON"),
        ({"version": "https://example.com/version/2", "items": []}, "unsupported JSON Feed version"),
        ({"version": "https://jsonfeed.org/version/1.1", "items": {}}, "items array"),
        ({"version": "https://jsonfeed.org/version/1.1", "items": [{"content_text": "missing id"}]}, "missing required id"),
    ],
)
async def test_json_feed_rejects_malformed_payloads(payload, match: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(payload, str):
            return httpx.Response(200, text=payload, request=request)
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        with pytest.raises(CollectorError, match=match):
            await collector.collect(context=CollectorContext(limit=10))
