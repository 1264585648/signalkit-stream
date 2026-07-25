from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorError


def item(number: int, *, created: object | None = None) -> dict:
    return {
        "node": {
            "id": number,
            "title": f"Item {number}",
            "body": f"Body {number}",
            "user": {"login": f"user{number}"},
            "url": f"https://example.com/items/{number}",
            "created": created if created is not None else f"2026-07-{number:02d}T10:00:00Z",
            "updated": 1784973600 + number,
            "score": number * 10,
        }
    }


@pytest.mark.asyncio
async def test_rest_normalizes_nested_mapping_and_metadata() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"items": [item(1)]}}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="data.items",
            id_path="node.id",
            kind=SignalKind.ISSUE,
            source="example",
            instance="issues",
            title_path="node.title",
            content_path="node.body",
            author_path="node.user.login",
            url_path="node.url",
            created_at_path="node.created",
            updated_at_path="node.updated",
            metadata_paths={"score": "node.score"},
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=10))

    event = result.events[0]
    assert event.source_key == "example:issues"
    assert event.kind is SignalKind.ISSUE
    assert event.title == "Item 1"
    assert event.content == "Body 1"
    assert event.author == "user1"
    assert event.url == "https://example.com/items/1"
    assert event.created_at == datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    assert event.updated_at == datetime.fromtimestamp(1784973601, tz=UTC)
    assert event.metadata == {"external_id": "1", "score": 10}


@pytest.mark.asyncio
async def test_rest_cursor_pagination_preserves_transport_cursor() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        cursor = request.url.params.get("after")
        if cursor is None:
            payload = {"data": {"items": [item(5), item(4)], "next": "c2"}}
        else:
            assert cursor == "c2"
            payload = {"data": {"items": [item(3)], "next": None}}
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="data.items",
            id_path="node.id",
            content_path="node.body",
            pagination="cursor",
            cursor_param="after",
            next_cursor_path="data.next",
            initial_backfill=True,
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=10))
        second = await collector.collect(context=CollectorContext(limit=10), cursor=first.cursor)

    assert first.has_more is True
    assert first.cursor.state["api_cursor"] == "c2"
    assert second.has_more is False
    assert [event.metadata["external_id"] for event in first.events + second.events] == ["5", "4", "3"]
    assert requests[1].url.params["after"] == "c2"


@pytest.mark.asyncio
async def test_rest_limit_refetches_same_page_before_advancing() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(request.url.params["page"])
        if page == 1:
            payload = {"items": [item(4), item(3), item(2)]}
        elif page == 2:
            payload = {"items": [item(1)]}
        else:
            payload = {"items": []}
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="items",
            id_path="node.id",
            content_path="node.body",
            pagination="page",
            initial_backfill=True,
            client=client,
        )
        one = await collector.collect(context=CollectorContext(limit=2))
        two = await collector.collect(context=CollectorContext(limit=2), cursor=one.cursor)
        three = await collector.collect(context=CollectorContext(limit=2), cursor=two.cursor)
        four = await collector.collect(context=CollectorContext(limit=2), cursor=three.cursor)

    assert [request.url.params["page"] for request in requests] == ["1", "1", "2", "3"]
    assert [event.metadata["external_id"] for event in one.events] == ["4", "3"]
    assert [event.metadata["external_id"] for event in two.events] == ["2"]
    assert [event.metadata["external_id"] for event in three.events] == ["1"]
    assert four.events == []
    assert four.has_more is False


@pytest.mark.asyncio
async def test_rest_default_baseline_does_not_walk_history() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["page"] == "1"
        return httpx.Response(200, json={"items": [item(3), item(2), item(1)]}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="items",
            id_path="node.id",
            content_path="node.body",
            pagination="page",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=2))

    assert result.has_more is False
    assert [event.metadata["external_id"] for event in result.events] == ["3", "2"]
    assert result.cursor.state["seen_ids"] == ["3", "2"]


@pytest.mark.asyncio
async def test_rest_incremental_poll_stops_at_seen_item() -> None:
    payloads = [
        {"items": [item(3), item(2)]},
        {"items": [item(5), item(4), item(3), item(2)]},
    ]
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        payload = payloads[calls]
        calls += 1
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="items",
            id_path="node.id",
            content_path="node.body",
            client=client,
        )
        baseline = await collector.collect(context=CollectorContext(limit=20))
        incremental = await collector.collect(context=CollectorContext(limit=20), cursor=baseline.cursor)

    assert [event.metadata["external_id"] for event in incremental.events] == ["5", "4"]
    assert incremental.has_more is False


@pytest.mark.asyncio
async def test_rest_conditional_get_and_static_request_fields() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.params["status"] == "open"
        assert request.url.params["per_page"] == "5"
        assert request.headers["X-Client"] == "signalkit"
        if calls == 1:
            return httpx.Response(200, json={"items": [item(1)]}, headers={"ETag": '"v1"'}, request=request)
        assert request.headers["If-None-Match"] == '"v1"'
        return httpx.Response(304, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="items",
            id_path="node.id",
            content_path="node.body",
            params={"status": "open"},
            headers={"X-Client": "signalkit"},
            limit_param="per_page",
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=5))
        second = await collector.collect(context=CollectorContext(limit=5), cursor=first.cursor)

    assert second.events == []
    assert second.cursor == first.cursor


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,kwargs,match",
    [
        ("bad-json", {}, "not valid JSON"),
        ({"data": {}}, {}, "items_path"),
        ({"data": {"items": {}}}, {}, "must resolve to an array"),
        ({"data": {"items": [{"node": {}}]}}, {}, "id_path"),
        (
            {"data": {"items": [item(1, created="not-a-date")]}},
            {"created_at_path": "node.created"},
            "not ISO-8601",
        ),
    ],
)
async def test_rest_rejects_malformed_payloads(payload, kwargs, match: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(payload, str):
            return httpx.Response(200, text=payload, request=request)
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = GenericRESTCollector(
            "https://api.example.com/items",
            items_path="data.items",
            id_path="node.id",
            content_path="node.body",
            client=client,
            **kwargs,
        )
        with pytest.raises(CollectorError, match=match):
            await collector.collect(context=CollectorContext(limit=10))
