from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.pipeline import run_collector
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind


def item(item_id: str, *, body: str | None = None) -> dict[str, object]:
    return {
        "id": item_id,
        "title": f"Title {item_id}",
        "body": body or f"Body {item_id}",
        "author": {"name": "alice"},
        "links": {"html": f"https://example.com/items/{item_id}"},
        "created_at": "2026-07-25T10:00:00Z",
        "updated_at": "2026-07-25T11:00:00Z",
    }


def collector(client: httpx.AsyncClient, **kwargs: object) -> GenericRESTCollector:
    return GenericRESTCollector(
        "https://api.example.com/v1/items",
        item_path="data.items",
        id_field="id",
        content_field="body",
        url_field="links.html",
        created_at_field="created_at",
        title_field="title",
        author_field="author.name",
        updated_at_field="updated_at",
        instance="example-items",
        client=client,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_rest_normalizes_nested_fields_and_metadata() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["page"] == "1"
        assert request.url.params["limit"] == "10"
        return httpx.Response(
            200,
            json={"data": {"items": [item("one")] }},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await collector(client, page_size=10).collect(
            context=CollectorContext(limit=10)
        )

    assert result.primary_count == 1
    assert result.has_more is False
    assert len(result.events) == 1
    event = result.events[0]
    assert event.kind is SignalKind.POST
    assert event.source_key == "rest:example-items"
    assert event.title == "Title one"
    assert event.content == "Body one"
    assert event.author == "alice"
    assert event.url == "https://example.com/items/one"
    assert event.created_at == datetime(2026, 7, 25, 10, 0, tzinfo=UTC)
    assert event.updated_at == datetime(2026, 7, 25, 11, 0, tzinfo=UTC)
    assert event.metadata["external_id"] == "one"
    assert event.metadata["raw"]["author"]["name"] == "alice"


@pytest.mark.asyncio
async def test_rest_refetches_partial_page_without_losing_items() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = request.url.params["page"]
        if page == "1":
            return httpx.Response(
                200,
                json={"data": {"items": [item("three"), item("two"), item("one")] }},
                request=request,
            )
        return httpx.Response(200, json={"data": {"items": []}}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = collector(client, page_size=3)
        first = await adapter.collect(context=CollectorContext(limit=2))
        second = await adapter.collect(context=CollectorContext(limit=2), cursor=first.cursor)

    assert [event.metadata["external_id"] for event in first.events] == ["three", "two"]
    assert first.has_more is True
    assert first.cursor is not None and first.cursor.state["page"] == 1
    assert first.cursor.state["cycle_ids"] == ["three", "two"]
    assert [event.metadata["external_id"] for event in second.events] == ["one"]
    assert requests[0].url.params["page"] == "1"
    assert requests[1].url.params["page"] == "1"


@pytest.mark.asyncio
async def test_rest_offset_pagination_and_incremental_watermark() -> None:
    cycle = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cycle
        offset = int(request.url.params["offset"])
        if cycle == 0:
            if offset == 0:
                return httpx.Response(
                    200,
                    json={"data": {"items": [item("two"), item("one")] }},
                    request=request,
                )
            cycle = 1
            return httpx.Response(200, json={"data": {"items": []}}, request=request)

        return httpx.Response(
            200,
            json={"data": {"items": [item("three"), item("two"), item("one")] }},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = collector(client, pagination="offset", page_size=2)
        first = await run_collector(adapter, limit=10)
        second = await run_collector(adapter, limit=10)

    assert [event.metadata["external_id"] for event in first.events] == ["two", "one"]
    assert first.cursor is not None
    assert first.cursor.state["seen_ids"][:2] == ["two", "one"]
    # run_collector without a store starts fresh by design; exercise the saved cursor directly.
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"data": {"items": [item("three"), item("two"), item("one")] }},
                request=request,
            )
        )
    ) as client:
        incremental = collector(client, pagination="offset", page_size=10)
        third = await incremental.collect(
            context=CollectorContext(limit=10),
            cursor=first.cursor,
        )

    assert second.events  # a fresh run may backfill again when no store is supplied
    assert [event.metadata["external_id"] for event in third.events] == ["three"]
    assert third.has_more is False
    assert third.cursor is not None
    assert third.cursor.state["seen_ids"][:3] == ["three", "two", "one"]


@pytest.mark.asyncio
async def test_rest_rejects_invalid_json_shape_and_required_fields() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": {"items": {}}}, request=request)
        )
    ) as client:
        with pytest.raises(CollectorError, match="did not resolve to a list") as caught:
            await collector(client).collect()
    assert caught.value.kind is CollectorErrorKind.PARSE

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"data": {"items": [{"id": "bad"}]}},
                request=request,
            )
        )
    ) as client:
        with pytest.raises(CollectorError, match="missing required field") as caught:
            await collector(client).collect()
    assert caught.value.kind is CollectorErrorKind.PARSE


def test_rest_instance_hash_is_stable_for_query_order() -> None:
    first = GenericRESTCollector(
        "https://api.example.com/items",
        item_path="items",
        id_field="id",
        content_field="body",
        url_field="url",
        created_at_field="created_at",
        query={"b": "2", "a": "1"},
    )
    second = GenericRESTCollector(
        "https://api.example.com/items",
        item_path="items",
        id_field="id",
        content_field="body",
        url_field="url",
        created_at_field="created_at",
        query={"a": "1", "b": "2"},
    )

    assert first.identity == second.identity
