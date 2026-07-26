from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping
from urllib.parse import urljoin, urlsplit

import httpx

from signalkit_stream.collectors._text import html_to_text, redact_url, validated_seen_window
from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)


DEFAULT_MAX_PAGE_FOLLOWS = 20
_DEFAULT_PORTS = {"http": 80, "https": 443}


class JSONFeedCollector(HTTPCollector):
    """Collect JSON Feed 1.x documents, including paginated ``next_url`` feeds.

    ``next_url`` is remote-controlled input: it is resolved against the configured feed
    URL and only followed when it stays on the same origin (identical scheme, host, and
    port) as that configured URL. Anything else (another host, another port, a scheme
    other than http/https) is rejected with a ``PARSE`` error instead of being fetched
    or persisted into a checkpoint. ``max_page_follows`` additionally bounds how many
    ``next_url`` hops a single polling cycle may take.
    """

    def __init__(
        self,
        url: str,
        *,
        source: str = "jsonfeed",
        instance: str | None = None,
        seen_window: int = 500,
        max_page_follows: int = DEFAULT_MAX_PAGE_FOLLOWS,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not url.strip():
            raise ValueError("JSON Feed URL must not be empty")
        if not source.strip():
            raise ValueError("JSON Feed source must not be empty")
        if max_page_follows < 0:
            raise ValueError("JSON Feed max_page_follows must be >= 0")
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.url = url.strip()
        self.source = source
        self.exported_url = redact_url(self.url)
        self.instance = instance or self.exported_url
        self.seen_window = validated_seen_window(seen_window, label="JSON Feed")
        self.max_page_follows = max_page_follows

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        page_url = _optional_text(state.get("page_url"))
        if page_url is not None:
            page_url = self._checked_page_url(
                page_url,
                field="page_url",
                kind=CollectorErrorKind.CURSOR,
            )
        item_offset = max(0, int(state.get("item_offset", 0))) if page_url else 0
        request_url = page_url or self.url
        starting_new_cycle = page_url is None
        page_follows = 0 if starting_new_cycle else max(0, int(state.get("page_follows", 0)))
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
                        "page_url": None,
                        "item_offset": 0,
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
        if item_offset > len(raw_items):
            raise CollectorError(
                "JSON Feed cursor item_offset is beyond the current page",
                kind=CollectorErrorKind.CURSOR,
                source_key=self.identity.key,
                retryable=False,
                details={"page_url": request_url, "item_offset": item_offset},
            )

        feed_title = _optional_text(payload.get("title"))
        feed_home = _optional_text(payload.get("home_page_url"))
        events: list[SignalEvent] = []
        processed_ids: list[str] = []
        primary_count = 0
        warnings: list[str] = []
        reached_watermark = False
        next_offset = item_offset

        for index in range(item_offset, len(raw_items)):
            raw_item = raw_items[index]
            next_offset = index + 1
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
            if external_id not in prior_seen_set:
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

        raw_next_url = _optional_text(payload.get("next_url"))
        response_next_url = (
            self._checked_page_url(
                raw_next_url,
                field="next_url",
                kind=CollectorErrorKind.PARSE,
            )
            if raw_next_url
            else None
        )
        next_page_follows = page_follows
        if reached_watermark:
            cursor_page_url = None
            cursor_offset = 0
        elif next_offset < len(raw_items):
            cursor_page_url = request_url
            cursor_offset = next_offset
        elif response_next_url and page_follows >= self.max_page_follows:
            warnings.append(
                f"stopped following JSON Feed next_url after {page_follows} pages in one cycle"
            )
            cursor_page_url = None
            cursor_offset = 0
        elif response_next_url:
            cursor_page_url = response_next_url
            cursor_offset = 0
            next_page_follows = page_follows + 1
        else:
            cursor_page_url = None
            cursor_offset = 0

        has_more = cursor_page_url is not None
        merged_seen = self._merge_seen(
            prior_seen,
            processed_ids,
            prepend=starting_new_cycle,
        )
        next_state: dict[str, Any] = {
            "page_url": cursor_page_url,
            "item_offset": cursor_offset,
            "seen_ids": merged_seen,
            "etag": response.headers.get("ETag") if starting_new_cycle else state.get("etag"),
            "last_modified": (
                response.headers.get("Last-Modified")
                if starting_new_cycle
                else state.get("last_modified")
            ),
        }
        if has_more:
            next_state["watermark_ids"] = watermark_ids
            next_state["page_follows"] = next_page_follows

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
            or self.exported_url
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
                "feed_url": self.exported_url,
                "feed_title": feed_title,
                "external_url": item.get("external_url"),
                "authors": authors,
                "tags": tags,
                "language": item.get("language"),
                "attachments": attachments,
            },
        )

    def _checked_page_url(
        self,
        candidate: str,
        *,
        field: str,
        kind: CollectorErrorKind,
    ) -> str:
        """Return ``candidate`` resolved against the feed URL, or raise if off-origin.

        Pagination targets come from the remote feed (or from a checkpoint that a remote
        feed populated earlier), so they are never trusted: only http/https URLs on the
        configured feed's own origin may be requested. This is the control that stops a
        feed operator or an on-path attacker from turning the collector into an SSRF
        probe against link-local or private endpoints.
        """

        resolved = urljoin(self.url, candidate)
        target = urlsplit(resolved)
        base = urlsplit(self.url)
        if target.scheme.lower() not in _DEFAULT_PORTS:
            raise self._page_url_error(
                f"JSON Feed {field} must use http or https",
                candidate=candidate,
                resolved=resolved,
                kind=kind,
            )
        target_origin = _origin(target)
        base_origin = _origin(base)
        if target_origin is None:
            raise self._page_url_error(
                f"JSON Feed {field} is not a usable http(s) URL",
                candidate=candidate,
                resolved=resolved,
                kind=kind,
            )
        if target_origin != base_origin:
            raise self._page_url_error(
                f"JSON Feed {field} must stay on the feed origin {_origin_text(base_origin)}",
                candidate=candidate,
                resolved=resolved,
                kind=kind,
            )
        return resolved

    def _page_url_error(
        self,
        message: str,
        *,
        candidate: str,
        resolved: str,
        kind: CollectorErrorKind,
    ) -> CollectorError:
        return CollectorError(
            message,
            kind=kind,
            source_key=self.identity.key,
            retryable=False,
            details={"rejected_url": candidate, "resolved_url": resolved},
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


def _origin(parts: Any) -> tuple[str, str, int | None] | None:
    """Return a normalized ``(scheme, host, port)`` origin, or ``None`` when malformed."""

    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    try:
        port = parts.port
    except ValueError:
        return None
    if not host or scheme not in _DEFAULT_PORTS:
        return None
    return scheme, host, port or _DEFAULT_PORTS[scheme]


def _origin_text(origin: tuple[str, str, int | None] | None) -> str:
    if origin is None:
        return "the configured feed origin"
    scheme, host, port = origin
    if port == _DEFAULT_PORTS.get(scheme):
        return f"{scheme}://{host}"
    return f"{scheme}://{host}:{port}"


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
