from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.jsonfeed import JSONFeedCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    Cursor,
)


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


@pytest.mark.asyncio
async def test_jsonfeed_rejects_hostile_next_url_and_never_requests_it() -> None:
    """A feed operator must not be able to steer the collector at another host."""

    hostile = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(200, json=feed(item("one", "One"), next_url=hostile))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert caught.value.retryable is False
    assert "next_url" in str(caught.value)
    assert caught.value.details["rejected_url"] == hostile
    assert requested == ["https://example.com/feed.json"]


@pytest.mark.asyncio
async def test_jsonfeed_rejects_cross_scheme_and_cross_port_next_url() -> None:
    for hostile in (
        "http://example.com/page-2.json",
        "https://example.com:8443/page-2.json",
        "https://evil.example.com/page-2.json",
        "https://example.com.evil.test/page-2.json",
    ):
        requested: list[str] = []

        def handler(request: httpx.Request, target: str = hostile) -> httpx.Response:
            requested.append(str(request.url))
            return httpx.Response(200, json=feed(item("one", "One"), next_url=target))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector = JSONFeedCollector("https://example.com/feed.json", client=client)
            with pytest.raises(CollectorError) as caught:
                await collector.collect(context=CollectorContext(limit=1))

        assert caught.value.kind is CollectorErrorKind.PARSE, hostile
        assert requested == ["https://example.com/feed.json"], hostile


@pytest.mark.asyncio
async def test_jsonfeed_accepts_relative_next_url_on_the_same_origin() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/feed.json":
            return httpx.Response(200, json=feed(item("one", "One"), next_url="/page-2.json"))
        return httpx.Response(200, json=feed(item("zero", "Zero")))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        first = await collector.collect(context=CollectorContext(limit=1))

    assert first.has_more is True
    assert first.cursor is not None
    assert first.cursor.state["page_url"] == "https://example.com/page-2.json"


@pytest.mark.asyncio
async def test_jsonfeed_rejects_poisoned_cursor_page_url_before_requesting_it() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
        requested.append(str(request.url))
        return httpx.Response(200, json=feed())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector("https://example.com/feed.json", client=client)
        poisoned = Cursor(
            collector.identity.key,
            {"page_url": "http://169.254.169.254/latest/meta-data/", "item_offset": 0},
        )
        with pytest.raises(CollectorError) as caught:
            await collector.collect(cursor=poisoned)

    assert caught.value.kind is CollectorErrorKind.CURSOR
    assert requested == []


@pytest.mark.asyncio
async def test_jsonfeed_caps_next_url_follows_per_cycle() -> None:
    pages = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal pages
        pages += 1
        return httpx.Response(
            200,
            json=feed(
                item(f"item-{pages}", f"Item {pages}"),
                next_url=f"https://example.com/page-{pages + 1}.json",
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = JSONFeedCollector(
            "https://example.com/feed.json",
            client=client,
            max_page_follows=3,
        )
        result = await collector.collect(context=CollectorContext(limit=1))
        follows = 0
        while result.has_more:
            follows += 1
            assert follows <= 10, "pagination follows were never capped"
            result = await collector.collect(
                context=CollectorContext(limit=1),
                cursor=result.cursor,
            )

    assert follows == 3
    assert any("next_url" in warning for warning in result.warnings)
    assert result.cursor is not None
    assert result.cursor.state["page_url"] is None
