from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.rss import RSSCollector
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind
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


BILLION_LAUGHS = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [
  <!ENTITY lol "lol">
  <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
  <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
  <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
  <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">
  <!ENTITY lol5 "&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;">
  <!ENTITY lol6 "&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;">
  <!ENTITY lol7 "&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;">
  <!ENTITY lol8 "&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;">
  <!ENTITY lol9 "&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;">
]>
<rss version="2.0"><channel><title>&lol9;</title>
<item><guid>g</guid><title>&lol9;</title><link>https://example.com/1</link></item>
</channel></rss>"""

EXTERNAL_ENTITY = b"""<?xml version="1.0"?>
<!DOCTYPE feed [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<rss version="2.0"><channel><title>&xxe;</title></channel></rss>"""


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [BILLION_LAUGHS, EXTERNAL_ENTITY], ids=["billion-laughs", "xxe"])
async def test_rss_rejects_dtd_declarations_before_parsing(payload: bytes) -> None:
    """The adapter, not expat's amplification cap, must reject entity payloads.

    Billion laughs is only mitigated by libexpat >= 2.6.0 and the project floor is
    CPython 3.11, so the DTD is refused up front: no legitimate RSS/Atom feed needs one.
    """

    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=payload))
    async with httpx.AsyncClient(transport=transport) as client:
        collector = RSSCollector("https://example.com/feed.xml", client=client)
        with pytest.raises(CollectorError) as caught:
            await collector.collect()

    assert caught.value.kind is CollectorErrorKind.PARSE
    assert caught.value.retryable is False
    assert "DOCTYPE" in str(caught.value)
    # Our own guard raised this: no expat ParseError was involved.
    assert caught.value.__cause__ is None
    assert caught.value.details.get("declaration") == "DOCTYPE"


@pytest.mark.asyncio
async def test_rss_still_accepts_a_doctype_like_string_inside_item_content() -> None:
    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Web tips</title>
<item>
  <guid>item-1</guid><title>Use the short doctype</title>
  <link>https://example.com/posts/1</link>
  <description><![CDATA[Always start pages with <!DOCTYPE html>.]]></description>
  <pubDate>Wed, 01 Jul 2026 10:00:00 GMT</pubDate>
</item>
</channel></rss>"""

    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=payload))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await RSSCollector("https://example.com/feed.xml", client=client).collect()

    assert len(result.events) == 1
    assert result.events[0].title == "Use the short doctype"
    assert result.events[0].content.startswith("Always start pages with")
