"""Deterministic normalization tests for the Atom branch of the RSS adapter.

CHANGELOG.md advertises "RSS / Atom" and docs/COLLECTOR_SDK.md requires a
deterministic normalization test per adapter, but RSSCollector._parse_atom had zero
coverage: tests/test_rss.py and tests/test_first_party_contracts.py only exercised
RSS 2.0.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.rss import RSSCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext

ATOM = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Example Atom Feed</title>
  <link href="https://example.com/" rel="self"/>
  <updated>2026-07-25T12:00:00Z</updated>
  <entry>
    <id>urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</id>
    <title>Atom entry one</title>
    <link href="https://example.com/edit/1" rel="edit"/>
    <link href="https://example.com/posts/1" rel="alternate"/>
    <author><name>Alice Example</name><email>alice@example.com</email></author>
    <category term="saas"/>
    <category term="crm"/>
    <category term="   "/>
    <published>2026-07-25T10:00:00Z</published>
    <updated>2026-07-25T11:30:00Z</updated>
    <content type="html">&lt;p&gt;Looking for a &lt;b&gt;simple&lt;/b&gt; CRM.&lt;/p&gt;</content>
  </entry>
  <entry>
    <title>Atom entry two</title>
    <link href="https://example.com/posts/2"/>
    <summary>Summary only, no content element.</summary>
    <updated>2026-07-24T09:00:00Z</updated>
  </entry>
</feed>
"""


async def _collect(limit: int = 10):
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=ATOM))
    async with httpx.AsyncClient(transport=transport) as client:
        collector = RSSCollector(
            "https://example.com/atom.xml",
            source="atom-example",
            client=client,
        )
        return await collector.collect(context=CollectorContext(limit=limit))


@pytest.mark.asyncio
async def test_atom_entry_is_normalized_from_id_title_link_author_and_dates() -> None:
    result = await _collect()

    assert result.primary_count == 2
    first = result.events[0]
    assert first.source == "atom-example"
    assert first.kind is SignalKind.ARTICLE
    assert first.metadata["feed_title"] == "Example Atom Feed"
    assert first.metadata["external_id"] == "urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a"
    assert first.title == "Atom entry one"
    # rel="alternate" wins over the rel="edit" link that appears first in the document.
    assert first.url == "https://example.com/posts/1"
    assert first.author == "Alice Example"
    assert first.content == "Looking for a simple CRM."
    assert first.created_at == datetime(2026, 7, 25, 10, 0, tzinfo=UTC)
    assert first.updated_at == datetime(2026, 7, 25, 11, 30, tzinfo=UTC)
    assert first.metadata["tags"] == ["saas", "crm"]


@pytest.mark.asyncio
async def test_atom_entry_falls_back_to_summary_link_id_and_updated() -> None:
    result = await _collect()

    second = result.events[1]
    assert second.title == "Atom entry two"
    assert second.content == "Summary only, no content element."
    # No <id>: the alternate link (a bare href with no rel) becomes the external id.
    assert second.metadata["external_id"] == "https://example.com/posts/2"
    assert second.url == "https://example.com/posts/2"
    assert second.author is None
    assert second.metadata["tags"] == []
    # No <published>: <updated> supplies created_at as well.
    assert second.created_at == datetime(2026, 7, 24, 9, 0, tzinfo=UTC)
    assert second.updated_at == datetime(2026, 7, 24, 9, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_atom_events_are_stable_across_recollection() -> None:
    first = await _collect()
    replay = await _collect()

    assert [event.id for event in first.events] == [event.id for event in replay.events]
    assert [event.fingerprint() for event in first.events] == [
        event.fingerprint() for event in replay.events
    ]


@pytest.mark.asyncio
async def test_atom_feed_pages_through_entries_with_an_offset_cursor() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=ATOM))
    async with httpx.AsyncClient(transport=transport) as client:
        collector = RSSCollector("https://example.com/atom.xml", client=client)
        page_one = await collector.collect(context=CollectorContext(limit=1))
        page_two = await collector.collect(
            context=CollectorContext(limit=1),
            cursor=page_one.cursor,
        )

    assert page_one.has_more is True
    assert page_one.cursor is not None and page_one.cursor.state["offset"] == 1
    assert [event.title for event in page_one.events] == ["Atom entry one"]
    assert [event.title for event in page_two.events] == ["Atom entry two"]
    assert page_two.has_more is False
