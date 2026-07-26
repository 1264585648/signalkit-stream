from __future__ import annotations

import codecs
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
import re
from xml.etree import ElementTree as ET

import httpx

from signalkit_stream.collectors._text import html_to_text, redact_url
from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind, CollectorResult, Cursor


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
        self.exported_url = redact_url(url)
        self.instance = instance or self.exported_url

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        headers: dict[str, str] = {}
        offset = int(cursor.state.get("offset", 0)) if cursor else 0
        if cursor and offset == 0:
            if cursor.state.get("etag"):
                headers["If-None-Match"] = str(cursor.state["etag"])
            if cursor.state.get("last_modified"):
                headers["If-Modified-Since"] = str(cursor.state["last_modified"])

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
        except _DTDForbidden as exc:
            raise CollectorError(
                f"feed {self.exported_url} declares a {exc.declaration} document type "
                "definition, which this adapter refuses to parse",
                kind=CollectorErrorKind.PARSE,
                source_key=self.identity.key,
                retryable=False,
                details={"declaration": exc.declaration},
            ) from None
        except (ET.ParseError, ValueError) as exc:
            raise CollectorError(
                f"failed to parse feed {self.exported_url}: {exc}",
                kind=CollectorErrorKind.PARSE,
                source_key=self.identity.key,
                retryable=False,
            ) from exc

        selected_entries = entries[offset : offset + ctx.limit]
        events: list[SignalEvent] = []
        for entry in selected_entries:
            link = str(entry["link"] or self.exported_url)
            title = html_to_text(str(entry["title"] or "")) or None
            content = html_to_text(str(entry["content"] or entry["title"] or ""))
            external_id = str(entry["id"] or link or title or len(events))
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
                        "feed_url": self.exported_url,
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
            },
        )
        return CollectorResult(
            events=events,
            cursor=next_cursor,
            has_more=has_more,
            primary_count=len(selected_entries),
            rate_limit=self.rate_limit,
        )

    @classmethod
    def _parse_feed(cls, payload: bytes) -> tuple[str | None, list[dict[str, object]]]:
        _reject_dtd(payload)
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


class _DTDForbidden(ValueError):
    """Raised when a feed body carries a DTD/entity declaration."""

    def __init__(self, declaration: str) -> None:
        super().__init__(f"feed declares a {declaration}")
        self.declaration = declaration


_ROOT_ELEMENT = re.compile(r"<[A-Za-z_]")
_DECLARATIONS = (("<!doctype", "DOCTYPE"), ("<!entity", "ENTITY"))


def _reject_dtd(payload: bytes) -> None:
    """Refuse feed bodies whose prolog declares a DTD or an entity.

    Billion laughs is only mitigated by the amplification cap in libexpat >= 2.6.0.
    ``requires-python = ">=3.11"`` guarantees no such floor (CPython 3.11.0-3.11.8 and
    distro builds linked against an older libexpat ship without it), so the DTD subset
    is refused before ElementTree ever sees it. No legitimate RSS/Atom feed needs one.

    Only the prolog is inspected, so a literal ``<!DOCTYPE html>`` inside item content
    or a CDATA section is still collected normally.
    """

    prolog = _prolog(payload)
    for marker, declaration in _DECLARATIONS:
        if marker in prolog:
            raise _DTDForbidden(declaration)


def _prolog(payload: bytes) -> str:
    """Return the lower-cased markup that precedes the root element."""

    if payload.startswith(codecs.BOM_UTF16_LE) or payload.startswith(codecs.BOM_UTF16_BE):
        text = payload.decode("utf-16", errors="replace")
    else:
        body = payload[3:] if payload.startswith(codecs.BOM_UTF8) else payload
        text = body.decode("latin-1", errors="replace")
    match = _ROOT_ELEMENT.search(text)
    return (text[: match.start()] if match else text).lower()
