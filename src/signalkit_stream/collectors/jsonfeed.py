from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping

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


class JSONFeedCollector(HTTPCollector):
    """Collect JSON Feed 1.x documents, including paginated ``next_url`` feeds."""

    def __init__(
        self,
        url: str,
        *,
        source: str = "jsonfeed",
        instance: str | None = None,
        seen_window: int = 500,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not url.strip():
            raise ValueError("JSON Feed URL must not be empty")
        if not source.strip():
            raise ValueError("JSON Feed source must not be empty")
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.url = url
        self.source = source
        self.instance = instance or url
        self.seen_window = max(50, seen_window)

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        next_url = _optional_text(state.get("next_url"))
        request_url = next_url or self.url
        starting_new_cycle = next_url is None
        prior_seen = [str(value) for value in state.get("seen_ids", []) if str(value)]
        prior_seen_set = set(prior_seen)
        watermark_ids = (
            prior_seen if starting_new_cycle else [str(value) for value in state.get("watermark_ids", [])]
        )
        watermark = set(watermark_ids)

        headers: dict[str, str] = {"Accept": "application/feed+json, application/json"}
        if starting_new_cycle:
            etag = _optional_text(state.get("etag"))
            last_modified = _optional_text(state.get("last_modified"))
            if etag:
                headers["If-None-Match"] = etag
            if last_modified:
                headers["If-Modified-Since"] = last_modified

        async with self.http_client() as client:
            response = await self.request(
                client,
                "GET",
                request_url,
                context=ctx,
                headers=headers,
            )

        if response.status_code == 304:
            return CollectorResult(
                events=[],
                cursor=Cursor(
                    self.identity.key,
                    {
                        "next_url": None,
                        "seen_ids": prior_seen,
                        "etag": state.get("etag"),
                        "last_modified": state.get("last_modified"),
                    },
                ),
                has_more=False,
                primary_count=0,
                rate_limit=self.rate_limit,
            )

        payload = self._json_object(response)
        version = str(payload.get("version") or "")
        if not version.startswith("https://jsonfeed.org/version/1"):
            raise self._parse_error(f"unsupported JSON Feed version: {version or 'missing'}")
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raise self._parse_error("JSON Feed items must be a list")

        feed_title = _optional_text(payload.get("title"))
        feed_home = _optional_text(payload.get("home_page_url"))
        events: list[SignalEvent] = []
        processed_ids: list[str] = []
        primary_count = 0
        warnings: list[str] = []
        reached_watermark = False

        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                warnings.append("ignored malformed JSON Feed item")
                continue
            external_id = _optional_text(raw_item.get("id"))
            if not external_id:
                warnings.append("ignored JSON Feed item without required id")
                continue
            if external_id in watermark:
                reached_watermark = True
                break

            primary_count += 1
            if external_id in prior_seen_set:
                continue
            events.append(
                self._item_event(
                    raw_item,
                    external_id=external_id,
                    feed_title=feed_title,
                    feed_home=feed_home,
                )
            )
            processed_ids.append(external_id)
            if primary_count >= ctx.limit:
                break

        response_next_url = _optional_text(payload.get("next_url"))
        page_exhausted = primary_count < ctx.limit or primary_count >= len(raw_items)
        if reached_watermark:
            cursor_next_url = None
        elif page_exhausted:
            cursor_next_url = response_next_url
        else:
            # JSON Feed has no item-offset cursor within a page. Requesting fewer items
            # than the document contains would make a partial page non-resumable, so we
            # treat the current document as the batch boundary and warn instead of
            # pretending ``next_url`` can resume inside this page.
            cursor_next_url = None
            warnings.append(
                "JSON Feed page contained more items than the requested batch; remaining items "
                "on this page are not addressable by next_url"
            )

        has_more = cursor_next_url is not None
        merged_seen = self._merge_seen(
            prior_seen,
            processed_ids,
            prepend=starting_new_cycle,
        )
        next_state: dict[str, Any] = {
            "next_url": cursor_next_url,
            "seen_ids": merged_seen,
            "etag": (
                response.headers.get("ETag") if starting_new_cycle else state.get("etag")
            ),
            "last_modified": (
                response.headers.get("Last-Modified")
                if starting_new_cycle
                else state.get("last_modified")
            ),
        }
        if has_more:
            next_state["watermark_ids"] = watermark_ids

        return CollectorResult(
            events=events,
            cursor=Cursor(self.identity.key, next_state),
            has_more=has_more,
            primary_count=primary_count,
            rate_limit=self.rate_limit,
            warnings=warnings,
        )

    def _item_event(
        self,
        item: Mapping[str, Any],
        *,
        external_id: str,
        feed_title: str | None,
        feed_home: str | None,
    ) -> SignalEvent:
        content_text = _optional_text(item.get("content_text"))
        content_html = _optional_text(item.get("content_html"))
        summary = _optional_text(item.get("summary"))
        content = content_text or html_to_text(content_html) or summary or ""
        title = _optional_text(item.get("title"))
        url = (
            _optional_text(item.get("url"))
            or _optional_text(item.get("external_url"))
            or feed_home
            or self.url
        )
        published = _parse_time(item.get("date_published"))
        modified = _parse_time(item.get("date_modified"))
        created_at = published or modified or datetime.now(UTC)
        authors = _authors(item)
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        attachments = item.get("attachments") if isinstance(item.get("attachments"), list) else []

        return SignalEvent(
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
            author=", ".join(authors) if authors else None,
            url=url,
            created_at=created_at,
            updated_at=modified,
            metadata={
                "external_id": external_id,
                "feed_url": self.url,
                "feed_title": feed_title,
                "external_url": item.get("external_url"),
                "authors": authors,
                "tags": tags,
                "language": item.get("language"),
                "attachments": attachments,
            },
        )

    def _json_object(self, response: httpx.Response) -> Mapping[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise self._parse_error("invalid JSON in JSON Feed response") from exc
        if not isinstance(payload, Mapping):
            raise self._parse_error("JSON Feed response must be an object")
        return payload

    def _parse_error(self, message: str) -> CollectorError:
        return CollectorError(
            message,
            kind=CollectorErrorKind.PARSE,
            source_key=self.identity.key,
            retryable=False,
        )

    def _merge_seen(
        self,
        prior: list[str],
        processed: list[str],
        *,
        prepend: bool,
    ) -> list[str]:
        values = [*processed, *prior] if prepend else [*prior, *processed]
        return list(dict.fromkeys(values))[: self.seen_window]


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_time(value: object) -> datetime | None:
    text = _optional_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _authors(item: Mapping[str, Any]) -> list[str]:
    raw_authors = item.get("authors")
    values: list[object] = raw_authors if isinstance(raw_authors, list) else []
    if not values and isinstance(item.get("author"), Mapping):
        values = [item["author"]]
    result: list[str] = []
    for raw in values:
        if not isinstance(raw, Mapping):
            continue
        name = _optional_text(raw.get("name")) or _optional_text(raw.get("url"))
        if name:
            result.append(name)
    return result
