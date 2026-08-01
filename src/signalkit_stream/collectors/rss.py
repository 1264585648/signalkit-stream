from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import httpx

from signalkit_stream.collectors._text import html_to_text
from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)


class RSSCollector(HTTPCollector):
    """Collect RSS/Atom feeds using conditional requests when checkpoints exist."""

    def __init__(
        self,
        url: str,
        *,
        source: str = "rss",
        instance: str | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.url = url
        self.source = source
        self.instance = instance or url

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        headers: dict[str, str] = {}
        offset = max(0, int(state.get("offset", 0)))
        if cursor and offset == 0:
            if state.get("etag"):
                headers["If-None-Match"] = str(state["etag"])
            if state.get("last_modified"):
                headers["If-Modified-Since"] = str(state["last_modified"])

        async with self.http_client() as client:
            response = await self.request(client, "GET", self.url, context=ctx, headers=headers)

        if response.status_code == 304:
            return CollectorResult(
                events=[],
                cursor=cursor,
                has_more=False,
                primary_count=0,
                rate_limit=self.rate_limit,
            )

        try:
            feed_title, entries = self._parse_feed(response.content)
        except (ET.ParseError, ValueError) as exc:
            raise CollectorError(
                f"failed to parse feed {self.url}: {exc}",
                kind=CollectorErrorKind.PARSE,
                source_key=self.identity.key,
                retryable=False,
            ) from exc

        warnings: list[str] = []
        anchor = str(state.get("page_anchor") or "").strip()
        if offset and anchor:
            expected_index = offset - 1
            anchor_index = next(
                (
                    index
                    for index, entry in enumerate(entries)
                    if self._entry_external_id(entry, index) == anchor
                ),
                None,
            )
            if anchor_index != expected_index:
                # Feeds are frequently mutable newest-first lists. If entries were
                # inserted or reordered between pages, blindly continuing by offset
                # can skip the new head entries. Restarting the current feed snapshot
                # is safe because persistence is idempotent by stable event ID.
                offset = 0
                warnings.append(
                    "RSS/Atom feed changed during pagination; restarted from the beginning to avoid skipped entries"
                )
        elif offset > len(entries):
            offset = 0
            warnings.append(
                "RSS/Atom cursor offset exceeded the current feed; restarted from the beginning"
            )

        selected_entries = entries[offset : offset + ctx.limit]
        events: list[SignalEvent] = []
        selected_external_ids: list[str] = []
        for local_index, entry in enumerate(selected_entries):
            absolute_index = offset + local_index
            link = str(entry["link"] or self.url)
            title = html_to_text(str(entry["title"] or "")) or None
            content = html_to_text(str(entry["content"] or entry["title"] or ""))
            external_id = self._entry_external_id(entry, absolute_index)
            selected_external_ids.append(external_id)
            created_at = self._parse_time(entry["published"])
            updated_at = self._parse_optional_time(entry["updated"])
            events.append(
                SignalEvent(
                    id=SignalEvent.stable_id(
                        self.source,
                        external_id,
                        SignalKind.ARTICLE,
                        source_instance=self.instance,
                    ),
                    source=self.source,
                    source_instance=self.instance,
                    kind=SignalKind.ARTICLE,
                    title=title,
                    content=content,
                    author=str(entry["author"] or "") or None,
                    url=link,
                    created_at=created_at,
                    updated_at=updated_at,
                    metadata={
                        "feed_url": self.url,
                        "feed_title": feed_title,
                        "tags": entry["tags"],
                        "external_id": external_id,
                    },
                )
            )

        next_offset = offset + len(selected_entries)
        has_more = next_offset < len(entries)
        next_cursor = Cursor(
            source_key=self.identity.key,
            state={
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "fetched_at": datetime.now(UTC).isoformat(),
                "offset": next_offset if has_more else 0,
                "page_anchor": selected_external_ids[-1] if has_more and selected_external_ids else None,
            },
        )
        return CollectorResult(
            events=events,
            cursor=next_cursor,
            has_more=has_more,
            primary_count=len(selected_entries),
            rate_limit=self.rate_limit,
            warnings=warnings,
        )

    @staticmethod
    def _entry_external_id(entry: dict[str, object], index: int) -> str:
        return str(entry["id"] or entry["link"] or entry["title"] or f"entry-{index}")

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
                    "updated": cls._child_text(item, "updated"),
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
                    "content": cls._child_text(entry, "content") or cls._child_text(entry, "summary"),
                    "author": author,
                    "published": cls._child_text(entry, "published") or cls._child_text(entry, "updated"),
                    "updated": cls._child_text(entry, "updated"),
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

    @classmethod
    def _parse_optional_time(cls, value: object) -> datetime | None:
        if not value:
            return None
        return cls._parse_time(value)
