from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.rss import RSSCollector
from signalkit_stream.protocol import CollectorContext
from signalkit_stream.models import SignalKind


FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <item>
      <guid>item-1</guid>
      <title>Need a better CRM</title>
      <link>https://example.com/posts/1</link>
      <description><![CDATA[<p>Looking for a <b>simple</b> CRM.</p>]]></description>
      <author>alice@example.com</author>
      <pubDate>Wed, 01 Jul 2026 10:00:00 GMT</pubDate>
    </item>
    <item>
      <guid>item-2</guid>
      <title>Second</title>
      <link>https://example.com/posts/2</link>
      <pubDate>Thu, 02 Jul 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


@pytest.mark.asyncio
async def test_rss_collector_normalizes_and_pages_feed() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=FEED,
            headers={"ETag": '"abc"'},
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        collector = RSSCollector("https://example.com/feed.xml", source="example", client=client)
        first = await collector.collect(context=CollectorContext(limit=1))
        second = await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert first.has_more is True
    assert first.primary_count == 1
    assert second.has_more is False
    assert second.primary_count == 1
    event = first.events[0]
    assert event.source == "example"
    assert event.kind is SignalKind.ARTICLE
    assert event.title == "Need a better CRM"
    assert event.content == "Looking for a simple CRM."
    assert event.created_at == datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    assert "If-None-Match" not in requests[1].headers


@pytest.mark.asyncio
async def test_rss_uses_conditional_get_after_feed_is_drained() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, content=FEED, headers={"ETag": '"abc"'}, request=request)
        assert request.headers["If-None-Match"] == '"abc"'
        return httpx.Response(304, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RSSCollector("https://example.com/feed.xml", client=client)
        first = await collector.collect(context=CollectorContext(limit=10))
        second = await collector.collect(context=CollectorContext(limit=10), cursor=first.cursor)

    assert second.events == []
    assert second.cursor == first.cursor
