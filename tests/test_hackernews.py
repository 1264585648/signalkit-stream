import json

import httpx
import pytest

from signalkit_stream.collectors.hackernews import HackerNewsCollector
from signalkit_stream.models import SignalKind


@pytest.mark.asyncio
async def test_hackernews_stories_and_comments() -> None:
    payloads = {
        "/v0/newstories.json": [101],
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
        return httpx.Response(200, content=json.dumps(body).encode())

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        events = await HackerNewsCollector(
            include_comments=True,
            comments_per_story=2,
            client=client,
        ).collect(limit=1)

    assert [event.kind for event in events] == [SignalKind.STORY, SignalKind.COMMENT]
    assert events[0].title == "Ask HN: CRM for a tiny team?"
    assert events[0].content == "I need something lightweight."
    assert events[1].content == "Try an open source option."
    assert events[1].metadata["parent_event_id"] == events[0].id
