from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import httpx

from signalkit_stream.collectors._text import html_to_text
from signalkit_stream.collectors.base import HTTPCollector
from signalkit_stream.models import SignalEvent, SignalKind


class RSSCollector(HTTPCollector):
    """Collect RSS/Atom feeds and emit normalized article events."""

    def __init__(
        self,
        url: str,
        *,
        source: str = "rss",
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ) -> None:
        super().__init__(client=client, timeout=timeout)
        self.url = url
        self.source = source

    async def collect(self, *, limit: int = 100) -> list[SignalEvent]:
        if limit < 1:
            return []

        async with self.http_client() as client:
            response = await client.get(self.url)
            response.raise_for_status()

        feed_title, entries = self._parse_feed(response.content)
        events: list[SignalEvent] = []
        for entry in entries[:limit]:
            link = entry["link"] or self.url
            title = html_to_text(entry["title"]) or None
            content = html_to_text(entry["content"] or entry["title"])
            external_id = entry["id"] or link or title or str(len(events))
            events.append(
                SignalEvent(
                    id=SignalEvent.stable_id(self.source, external_id, SignalKind.ARTICLE),
                    source=self.source,
                    kind=SignalKind.ARTICLE,
                    title=title,
                    content=content,
                    author=entry["author"] or None,
                    url=link,
                    created_at=self._parse_time(entry["published"]),
                    metadata={
                        "feed_url": self.url,
                        "feed_title": feed_title,
                        "tags": entry["tags"],
                        "external_id": external_id,
                    },
                )
            )
        return events

    @classmethod
    def _parse_feed(cls, payload: bytes) -> tuple[str | None, list[dict[str, object]]]:
        root = ET.fromstring(payload)
        root_name = cls._local_name(root.tag)
        if root_name == "rss":
            return cls._parse_rss(root)
        if root_name == "feed":
            return cls._parse_atom(root)
        raise ValueError(f"unsupported feed root element: {root_name}")

    @classmethod
    def _parse_rss(cls, root: ET.Element) -> tuple[str | None, list[dict[str, object]]]:
        channel = next((child for child in root if cls._local_name(child.tag) == "channel"), None)
        if channel is None:
            raise ValueError("invalid RSS feed: channel element not found")

        feed_title = cls._child_text(channel, "title")
        entries: list[dict[str, object]] = []
        for item in (child for child in channel if cls._local_name(child.tag) == "item"):
            content = cls._child_text(item, "encoded") or cls._child_text(item, "description")
            author = cls._child_text(item, "creator") or cls._child_text(item, "author")
            tags = [
                (child.text or "").strip()
                for child in item
                if cls._local_name(child.tag) == "category" and (child.text or "").strip()
            ]
            entries.append(
                {
                    "id": cls._child_text(item, "guid") or cls._child_text(item, "link"),
                    "title": cls._child_text(item, "title"),
                    "link": cls._child_text(item, "link"),
                    "content": content,
                    "author": author,
                    "published": cls._child_text(item, "pubDate"),
                    "tags": tags,
                }
            )
        return feed_title, entries

    @classmethod
    def _parse_atom(cls, root: ET.Element) -> tuple[str | None, list[dict[str, object]]]:
        feed_title = cls._child_text(root, "title")
        entries: list[dict[str, object]] = []
        for entry in (child for child in root if cls._local_name(child.tag) == "entry"):
            link = ""
            for child in entry:
                if cls._local_name(child.tag) != "link":
                    continue
                rel = child.attrib.get("rel", "alternate")
                href = child.attrib.get("href", "")
                if href and rel in {"alternate", ""}:
                    link = href
                    break
                if href and not link:
                    link = href

            author = ""
            author_node = next(
                (child for child in entry if cls._local_name(child.tag) == "author"),
                None,
            )
            if author_node is not None:
                author = cls._child_text(author_node, "name")

            tags = [
                child.attrib.get("term", "").strip()
                for child in entry
                if cls._local_name(child.tag) == "category" and child.attrib.get("term", "").strip()
            ]
            entries.append(
                {
                    "id": cls._child_text(entry, "id") or link,
                    "title": cls._child_text(entry, "title"),
                    "link": link,
                    "content": (
                        cls._child_text(entry, "content") or cls._child_text(entry, "summary")
                    ),
                    "author": author,
                    "published": (
                        cls._child_text(entry, "published") or cls._child_text(entry, "updated")
                    ),
                    "tags": tags,
                }
            )
        return feed_title, entries

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].split(":")[-1]

    @classmethod
    def _child_text(cls, element: ET.Element, name: str) -> str:
        for child in element:
            if cls._local_name(child.tag) == name:
                return "".join(child.itertext()).strip()
        return ""

    @staticmethod
    def _parse_time(value: object) -> datetime:
        if not value:
            return datetime.now(UTC)
        text = str(value).strip()
        try:
            parsed = parsedate_to_datetime(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except (TypeError, ValueError, OverflowError):
            pass
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return datetime.now(UTC)
