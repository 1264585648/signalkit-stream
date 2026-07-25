from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.jsonfeed import JSONFeedCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind


def item(item_id: str, title: str, *, html: bool = False) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": item_id,
        "url": f"https://example.com/{item_id}",
        "title": title,
        "date_published": "2026-07-25T10:00:00Z",
        "date_modified": "2026-07-25T11:00:00Z",
        "authors": [{"name": "Alice"}, {"name": "Bob"}],
        "tags": ["ai", "saas"],
        "language": "en",
    }
    if html:
        payload["content_html"] = "<p>Need a <b>better</b> CRM.</p>"
    else:
        payload["content_text"] = f"body {item_id}"
    return payload


def feed(*items: dict[str, object], next_url: str | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "version": "https://jsonfeed.org/version/1.1",
        "title": "Example Feed",
        "home_page_url": "https://example.com/",
        "items": list(items),
    }
    if next_url:
        payload["next_url"] = next_url
    return payload


@pytest.mark.asyncio
async def test_jsonfeed_normalizes_content_authors_and_dates() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json=feed(item("one", "One", html=True)))
    )
    async with httpx.AsyncClient(transport=transport) as client:
        result = await JSONFeedCollector(
            "https://example.com/feed.json",
            instance="product-feed",
            client=client,
        ).collect()

    assert result.primary_count == 1
    assert len(result.events) == 1
    event = result.events[0]
    assert event.kind is SignalKind.ARTICLE
    assert event.source_instance == "product-feed"
    assert event.title == "One"
    assert event.content == "Need a better CRM."
    assert event.author == "Alice, Bob"
    assert event.created_at == datetime(2026, 7, 25, 10, 0, tzinfo=UTC)
    assert event.updated_at == datetime(2026, 7, 25, 11, 0, tzinfo=UTC)
    assert event.metadata["tags"] == ["ai", "saas"]


@pytest.mark.asyncio
async def test_jsonfeed_resumes_within_page_then_follows_next_url_and_uses_etag() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/feed.json":
            if request.headers.get("If-None-Match") == '"feed-v1"':
                return httpx.Response(304, request=request)
            return httpx.Response(
                200,
                json=feed(
                    item("three", "Three"),
                    item("two", "Two"),
                    item("one", "One"),
                    next_url="https://example.com/page-2.json",
                ),
                headers={"ETag": '"feed-v1"', "Last-Modified": "Sat, 25 Jul 2026 10:00:00 GMT"},
                request=request,
            )
        if request.url.path == "/page-2.json":
            return httpx.Response(200, json=feed(item("zero", "Zero")), request=request)
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        first = await collector.collect(context=CollectorContext(limit=2))
        second = await collector.collect(context=CollectorContext(limit=2), cursor=first.cursor)
        third = await collector.collect(context=CollectorContext(limit=2), cursor=second.cursor)
        fourth = await collector.collect(context=CollectorContext(limit=2), cursor=third.cursor)

    assert [event.metadata["external_id"] for event in first.events] == ["three", "two"]
    assert first.has_more is True
    assert first.cursor is not None
    assert first.cursor.state["page_url"] == "https://example.com/feed.json"
    assert first.cursor.state["item_offset"] == 2

    assert [event.metadata["external_id"] for event in second.events] == ["one"]
    assert second.has_more is True
    assert second.cursor is not None
    assert second.cursor.state["page_url"] == "https://example.com/page-2.json"
    assert second.cursor.state["item_offset"] == 0

    assert [event.metadata["external_id"] for event in third.events] == ["zero"]
    assert third.has_more is False
    assert third.cursor is not None
    assert third.cursor.state["seen_ids"][:4] == ["three", "two", "one", "zero"]

    assert fourth.events == []
    assert fourth.primary_count == 0
    assert requests[-1].headers["If-None-Match"] == '"feed-v1"'
    assert requests[-1].headers["If-Modified-Since"] == "Sat, 25 Jul 2026 10:00:00 GMT"


@pytest.mark.asyncio
async def test_jsonfeed_new_cycle_stops_at_previous_seen_watermark() -> None:
    phase = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal phase
        phase += 1
        if phase == 1:
            return httpx.Response(200, json=feed(item("two", "Two"), item("one", "One")))
        return httpx.Response(
            200,
            json=feed(
                item("three", "Three"),
                item("two", "Two"),
                item("one", "One"),
                next_url="https://example.com/older.json",
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        first = await collector.collect()
        second = await collector.collect(cursor=first.cursor)

    assert [event.metadata["external_id"] for event in first.events] == ["two", "one"]
    assert [event.metadata["external_id"] for event in second.events] == ["three"]
    assert second.primary_count == 1
    assert second.has_more is False
    assert second.cursor is not None
    assert second.cursor.state["seen_ids"][:3] == ["three", "two", "one"]


@pytest.mark.asyncio
async def test_jsonfeed_rejects_unsupported_version() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={"version": "https://example.com/feed/v2", "items": []},
            request=request,
        )
    )
    async with httpx.AsyncClient(transport=transport) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect()

    assert caught.value.kind is CollectorErrorKind.PARSE
