import httpx
import pytest

from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.protocol import CollectorContext


def item(item_id: str) -> dict[str, str]:
    return {
        "id": item_id,
        "body": f"body {item_id}",
        "url": f"https://example.com/{item_id}",
        "created_at": "2026-07-25T10:00:00Z",
    }


def make_collector(client: httpx.AsyncClient, *, initial_backfill: bool) -> GenericRESTCollector:
    return GenericRESTCollector(
        "https://api.example.com/items",
        item_path="items",
        id_field="id",
        content_field="body",
        url_field="url",
        created_at_field="created_at",
        instance="bootstrap",
        page_size=10,
        initial_backfill=initial_backfill,
        client=client,
    )


@pytest.mark.asyncio
async def test_initial_backfill_false_seeds_watermark_without_emitting_history() -> None:
    phase = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal phase
        phase += 1
        if phase == 1:
            payload = {"items": [item("two"), item("one")]}
        else:
            payload = {"items": [item("three"), item("two"), item("one")]}
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = make_collector(client, initial_backfill=False)
        bootstrap = await collector.collect(context=CollectorContext(limit=10))
        incremental = await collector.collect(
            context=CollectorContext(limit=10),
            cursor=bootstrap.cursor,
        )

    assert bootstrap.events == []
    assert bootstrap.primary_count == 0
    assert bootstrap.has_more is False
    assert bootstrap.cursor is not None
    assert bootstrap.cursor.state["seen_ids"] == ["two", "one"]
    assert "without emitting history" in bootstrap.warnings[0]

    assert [event.metadata["external_id"] for event in incremental.events] == ["three"]
    assert incremental.primary_count == 1
    assert incremental.has_more is False
    assert incremental.cursor is not None
    assert incremental.cursor.state["seen_ids"][:3] == ["three", "two", "one"]
