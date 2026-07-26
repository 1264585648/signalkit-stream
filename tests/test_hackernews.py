import json

import httpx
import pytest

from signalkit_stream.collectors.hackernews import HackerNewsCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind


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
async def test_hackernews_non_numeric_story_ids_are_classified_as_parse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["not-an-id"], request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = HackerNewsCollector(client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert caught.value.retryable is False


@pytest.mark.asyncio
async def test_hackernews_html_error_page_is_classified_as_parse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>maintenance</html>", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = HackerNewsCollector(client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert "JSON" in str(caught.value)


@pytest.mark.asyncio
async def test_hackernews_id_list_must_be_an_array() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"nope": True}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = HackerNewsCollector(client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.PARSE


@pytest.mark.asyncio
async def test_hackernews_story_without_id_is_classified_as_parse_error() -> None:
    payloads = {
        "/v0/newstories.json": [101],
        "/v0/item/101.json": {"type": "story", "by": "alice", "title": "No id here"},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payloads[request.url.path], request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = HackerNewsCollector(client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert "id" in str(caught.value)


@pytest.mark.asyncio
async def test_hackernews_non_numeric_comment_id_is_classified_as_parse_error() -> None:
    payloads = {
        "/v0/newstories.json": [101],
        "/v0/item/101.json": {
            "id": 101,
            "type": "story",
            "title": "Story",
            "time": 1782896400,
            "kids": ["oops"],
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payloads[request.url.path], request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = HackerNewsCollector(
            include_comments=True,
            comments_per_story=2,
            client=client,
        )
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.PARSE
