import json

import httpx
import pytest

from signalkit_stream.collectors.hackernews import HackerNewsCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext


@pytest.mark.asyncio
async def test_hackernews_stories_comments_and_seen_cursor() -> None:
    payloads = {
        "/v0/newstories.json": [101, 102],
        "/v0/item/101.json": {
            "id": 101,
            "type": "story",
            "by": "alice",
            "time": 1782896400,
            "title": "Ask HN: CRM for a tiny team?",
            "text": "<p>I need something lightweight.</p>",
            "score": 12,
            "kids": [201],
        },
        "/v0/item/102.json": {
            "id": 102,
            "type": "story",
            "by": "charlie",
            "time": 1782896500,
            "title": "Another story",
        },
        "/v0/item/201.json": {
            "id": 201,
            "type": "comment",
            "by": "bob",
            "time": 1782896500,
            "parent": 101,
            "text": "<p>Try an open source option.</p>",
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = payloads[request.url.path]
        return httpx.Response(200, content=json.dumps(body).encode(), request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        collector = HackerNewsCollector(
            include_comments=True,
            comments_per_story=2,
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=1))
        second = await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert [event.kind for event in first.events] == [SignalKind.STORY, SignalKind.COMMENT]
    assert first.events[1].metadata["parent_event_id"] == first.events[0].id
    assert second.events[0].title == "Another story"
    assert first.events[0].id != second.events[0].id


@pytest.mark.asyncio
async def test_hackernews_refreshes_recent_story_for_new_comments() -> None:
    cycle = 1

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v0/newstories.json":
            return httpx.Response(200, json=[101], request=request)
        if request.url.path == "/v0/item/101.json":
            kids = [201] if cycle == 1 else [201, 202]
            return httpx.Response(
                200,
                json={
                    "id": 101,
                    "type": "story",
                    "by": "alice",
                    "time": 1782896400,
                    "title": "Active discussion",
                    "kids": kids,
                },
                request=request,
            )
        if request.url.path == "/v0/item/201.json":
            return httpx.Response(
                200,
                json={
                    "id": 201,
                    "type": "comment",
                    "by": "bob",
                    "time": 1782896500,
                    "parent": 101,
                    "text": "first",
                },
                request=request,
            )
        if request.url.path == "/v0/item/202.json":
            return httpx.Response(
                200,
                json={
                    "id": 202,
                    "type": "comment",
                    "by": "carol",
                    "time": 1782896600,
                    "parent": 101,
                    "text": "new reply",
                },
                request=request,
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = HackerNewsCollector(
            include_comments=True,
            comments_per_story=1,
            comment_refresh_window=1,
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=10))
        cycle = 2
        second = await collector.collect(
            context=CollectorContext(limit=10),
            cursor=first.cursor,
        )

    assert first.primary_count == 1
    assert [event.metadata["external_id"] for event in first.events] == ["101", "201"]
    assert second.primary_count == 0
    assert [event.metadata["external_id"] for event in second.events] == ["202"]
    assert second.events[0].metadata["parent_event_id"] == first.events[0].id
