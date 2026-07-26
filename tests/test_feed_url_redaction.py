"""Feed URLs carry operator secrets; they must never be exported on events."""

from __future__ import annotations

import json

import httpx
import pytest

from signalkit_stream.collectors.jsonfeed import JSONFeedCollector
from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.collectors.rss import RSSCollector

SECRET = "SUPERSECRET123"
RSS_URL = f"https://private.example.com/feed.xml?auth_token={SECRET}"
JSON_URL = f"https://private.example.com/feed.json?auth_token={SECRET}"
REST_URL = f"https://private.example.com/api/items?auth_token={SECRET}"

FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>Private</title>
<item>
  <guid>item-1</guid><title>Hello</title>
  <link>https://private.example.com/posts/1</link>
  <description>Body</description>
  <pubDate>Wed, 01 Jul 2026 10:00:00 GMT</pubDate>
</item>
</channel></rss>"""


def _assert_secret_free(events) -> None:
    assert events
    for event in events:
        payload = json.dumps(event.to_dict(), default=str)
        assert SECRET not in payload, payload
        assert SECRET not in event.source_instance
        assert SECRET not in event.id


@pytest.mark.asyncio
async def test_rss_event_never_exports_the_feed_url_query_string() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["auth_token"] == SECRET
        return httpx.Response(200, content=FEED, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RSSCollector(RSS_URL, client=client).collect()

    _assert_secret_free(result.events)
    assert result.events[0].metadata["feed_url"] == "https://private.example.com/feed.xml"
    assert result.events[0].source_instance == "https://private.example.com/feed.xml"


@pytest.mark.asyncio
async def test_jsonfeed_event_never_exports_the_feed_url_query_string() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["auth_token"] == SECRET
        return httpx.Response(
            200,
            json={
                "version": "https://jsonfeed.org/version/1.1",
                "title": "Private",
                "items": [{"id": "one", "title": "Hello", "content_text": "Body"}],
            },
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await JSONFeedCollector(JSON_URL, client=client).collect()

    _assert_secret_free(result.events)
    assert result.events[0].metadata["feed_url"] == "https://private.example.com/feed.json"


@pytest.mark.asyncio
async def test_rest_event_never_exports_the_url_query_string() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["auth_token"] == SECRET
        return httpx.Response(200, json={"items": [{"id": "1", "title": "Hello"}]}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await GenericRESTCollector(
            REST_URL,
            items_path="items",
            id_path="id",
            title_path="title",
            client=client,
        ).collect()

    _assert_secret_free(result.events)
    assert result.events[0].url == "https://private.example.com/api/items"


@pytest.mark.asyncio
async def test_explicit_instance_override_still_wins() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=FEED, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RSSCollector(RSS_URL, instance="private-feed", client=client).collect()

    assert result.events[0].source_instance == "private-feed"
    _assert_secret_free(result.events)
