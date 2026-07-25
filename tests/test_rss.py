from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.rss import RSSCollector
from signalkit_stream.models import SignalKind


FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <link>https://example.com/</link>
    <description>Demo</description>
    <item>
      <guid>item-1</guid>
      <title>Need a better CRM</title>
      <link>https://example.com/posts/1</link>
      <description><![CDATA[<p>Looking for a <b>simple</b> CRM.</p>]]></description>
      <author>alice@example.com</author>
      <pubDate>Wed, 01 Jul 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


@pytest.mark.asyncio
async def test_rss_collector_normalizes_feed() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=FEED))
    async with httpx.AsyncClient(transport=transport) as client:
        events = await RSSCollector(
            "https://example.com/feed.xml",
            source="example",
            client=client,
        ).collect(limit=10)

    assert len(events) == 1
    event = events[0]
    assert event.source == "example"
    assert event.kind is SignalKind.ARTICLE
    assert event.title == "Need a better CRM"
    assert event.content == "Looking for a simple CRM."
    assert event.url == "https://example.com/posts/1"
    assert event.created_at == datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
