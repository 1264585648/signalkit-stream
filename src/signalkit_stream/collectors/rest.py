from __future__ import annotations

from datetime import UTC, datetime
import json
from typing import Any, Literal, Mapping

import httpx

from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)

PaginationMode = Literal["none", "page", "cursor"]
_MISSING = object()


class GenericRESTCollector(HTTPCollector):
    """Configurable JSON REST-list collector and reference adapter.

    The adapter intentionally targets the common case of a GET endpoint that returns
    an array of objects. Source-specific adapters remain preferable when an API has
    richer semantics that deserve a dedicated contract.
    """

    source = "rest"

    def __init__(
        self,
        url: str,
        *,
        items_path: str,
        id_path: str,
        kind: SignalKind = SignalKind.POST,
        source: str = "rest",
        instance: str | None = None,
        title_path: str | None = None,
        content_path: str | None = None,
        author_path: str | None = None,
        url_path: str | None = None,
        created_at_path: str | None = None,
        updated_at_path: str | None = None,
        metadata_paths: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        pagination: PaginationMode = "none",
        page_param: str = "page",
        page_start: int = 1,
        cursor_param: str = "cursor",
        next_cursor_path: str | None = None,
        limit_param: str | None = None,
        initial_backfill: bool = False,
        seen_window: int = 2000,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        url = url.strip()
        items_path = items_path.strip()
        id_path = id_path.strip()
        source = source.strip()
        if not url:
            raise ValueError("REST URL must not be empty")
        if not items_path:
            raise ValueError("REST items_path must not be empty")
        if not id_path:
            raise ValueError("REST id_path must not be empty")
        if not source:
            raise ValueError("REST source must not be empty")
        if pagination not in {"none", "page", "cursor"}:
            raise ValueError("REST pagination must be 'none', 'page', or 'cursor'")
        if page_start < 0:
            raise ValueError("REST page_start must be >= 0")
        if pagination == "page" and not page_param.strip():
            raise ValueError("REST page_param must not be empty for page pagination")
        if pagination == "cursor":
            if not cursor_param.strip():
                raise ValueError("REST cursor_param must not be empty for cursor pagination")
            if not next_cursor_path or not next_cursor_path.strip():
                raise ValueError("REST next_cursor_path is required for cursor pagination")
        if seen_window < 100:
            raise ValueError("REST seen_window must be >= 100")

        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.url = url
        self.items_path = items_path
        self.id_path = id_path
        self.kind = kind
        self.source = source
        self.instance = instance or url
        self.title_path = _clean_path(title_path)
        self.content_path = _clean_path(content_path)
        self.author_path = _clean_path(author_path)
        self.url_path = _clean_path(url_path)
        self.created_at_path = _clean_path(created_at_path)
        self.updated_at_path = _clean_path(updated_at_path)
        self.metadata_paths = {
            str(key): str(path).strip()
            for key, path in (metadata_paths or {}).items()
            if str(key).strip() and str(path).strip()
        }
        self.params = dict(params or {})
        self.headers = {str(key): str(value) for key, value in (headers or {}).items()}
        self.pagination = pagination
        self.page_param = page_param.strip()
        self.page_start = page_start
        self.cursor_param = cursor_param.strip()
        self.next_cursor_path = _clean_path(next_cursor_path)
        self.limit_param = _clean_path(limit_param)
        self.initial_backfill = initial_backfill
        self.seen_window = seen_window

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        initialized = bool(state.get("initialized", False))
        seen_ids = [str(value) for value in state.get("seen_ids", [])]
        seen = set(seen_ids)
        cycle_ids = [str(value) for value in state.get("cycle_ids", [])]
        cycle_seen = set(cycle_ids)
        page = _state_int(state.get("page"), self.page_start)
        api_cursor = _optional_scalar(state.get("api_cursor"))
        in_progress = bool(cycle_ids) or page != self.page_start or api_cursor is not None

        request_headers = dict(self.headers)
        if not in_progress:
            etag = _optional_string(state.get("etag"))
            last_modified = _optional_string(state.get("last_modified"))
            if etag:
                request_headers["If-None-Match"] = etag
            if last_modified:
                request_headers["If-Modified-Since"] = last_modified

        request_params = dict(self.params)
        if self.limit_param:
            request_params[self.limit_param] = ctx.limit
        if self.pagination == "page":
            request_params[self.page_param] = page
        elif self.pagination == "cursor" and api_cursor is not None:
            request_params[self.cursor_param] = api_cursor

        async with self.http_client() as client:
            response = await self.request(
                client,
                "GET",
                self.url,
                context=ctx,
                params=request_params or None,
                headers=request_headers or None,
                allow_statuses={304},
            )

        if response.status_code == 304:
            return CollectorResult(
                events=[],
                cursor=cursor or Cursor(self.identity.key),
                has_more=False,
                primary_count=0,
                rate_limit=self.rate_limit,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise self._parse_error("REST response was not valid JSON") from exc

        raw_items = _get_path(payload, self.items_path, default=_MISSING)
        if raw_items is _MISSING:
            raise self._parse_error(f"REST items_path {self.items_path!r} was not found")
        if not isinstance(raw_items, list):
            raise self._parse_error(f"REST items_path {self.items_path!r} must resolve to an array")

        events: list[SignalEvent] = []
        emitted_ids: list[str] = []
        reached_known = False
        page_fully_scanned = True
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                continue
            external_id = self._item_id(raw_item)
            if external_id in cycle_seen:
                continue
            if initialized and external_id in seen:
                reached_known = True
                break
            if len(events) >= ctx.limit:
                page_fully_scanned = False
                break
            events.append(self._normalize(raw_item, external_id))
            emitted_ids.append(external_id)
            cycle_seen.add(external_id)

        cycle_ids.extend(emitted_ids)
        next_transport = self._next_transport(payload, page=page, raw_items=raw_items)
        source_exhausted = next_transport is None

        if not initialized and not self.initial_backfill:
            complete = True
        else:
            complete = reached_known or (page_fully_scanned and source_exhausted)

        response_etag = _optional_string(response.headers.get("ETag"))
        response_last_modified = _optional_string(response.headers.get("Last-Modified"))

        if complete:
            merged_seen = list(dict.fromkeys([*cycle_ids, *seen_ids]))[: self.seen_window]
            next_state = {
                "initialized": True,
                "seen_ids": merged_seen,
                "cycle_ids": [],
                "page": self.page_start,
                "api_cursor": None,
                "etag": response_etag or _optional_string(state.get("pending_etag")),
                "last_modified": response_last_modified
                or _optional_string(state.get("pending_last_modified")),
            }
            has_more = False
        else:
            next_page = page
            next_api_cursor = api_cursor
            if page_fully_scanned:
                if self.pagination == "page":
                    next_page = int(next_transport)
                elif self.pagination == "cursor":
                    next_api_cursor = next_transport
            next_state = {
                "initialized": initialized,
                "seen_ids": seen_ids,
                "cycle_ids": cycle_ids,
                "page": next_page,
                "api_cursor": next_api_cursor,
                "etag": _optional_string(state.get("etag")),
                "last_modified": _optional_string(state.get("last_modified")),
                "pending_etag": response_etag or _optional_string(state.get("pending_etag")),
                "pending_last_modified": response_last_modified
                or _optional_string(state.get("pending_last_modified")),
            }
            has_more = True

        return CollectorResult(
            events=events,
            cursor=Cursor(self.identity.key, next_state),
            has_more=has_more,
            primary_count=len(events),
            rate_limit=self.rate_limit,
        )

    def _next_transport(
        self,
        payload: object,
        *,
        page: int,
        raw_items: list[object],
    ) -> str | int | None:
        if self.pagination == "none":
            return None
        if self.pagination == "page":
            return page + 1 if raw_items else None
        value = _get_path(payload, self.next_cursor_path or "", default=None)
        return _optional_scalar(value)

    def _normalize(self, item: Mapping[str, Any], external_id: str) -> SignalEvent:
        title = self._field_text(item, self.title_path)
        content_value = self._field(item, self.content_path)
        content = _to_text(content_value) if content_value is not _MISSING else ""
        author = self._field_text(item, self.author_path)
        item_url = self._field_text(item, self.url_path) or self.url
        created_at = self._field_datetime(item, self.created_at_path) or datetime.fromtimestamp(0, tz=UTC)
        updated_at = self._field_datetime(item, self.updated_at_path)
        metadata: dict[str, Any] = {"external_id": external_id}
        for key, path in self.metadata_paths.items():
            value = _get_path(item, path, default=_MISSING)
            if value is not _MISSING:
                metadata[key] = value

        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                external_id,
                self.kind,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=self.kind,
            title=title,
            content=content,
            author=author,
            url=item_url,
            created_at=created_at,
            updated_at=updated_at,
            metadata=metadata,
        )

    def _item_id(self, item: Mapping[str, Any]) -> str:
        value = _get_path(item, self.id_path, default=_MISSING)
        if value is _MISSING or isinstance(value, (dict, list)):
            raise self._parse_error(f"REST item id_path {self.id_path!r} did not resolve to a scalar")
        text = str(value).strip()
        if not text:
            raise self._parse_error(f"REST item id_path {self.id_path!r} resolved to an empty value")
        return text

    @staticmethod
    def _field(item: Mapping[str, Any], path: str | None) -> object:
        if not path:
            return _MISSING
        return _get_path(item, path, default=_MISSING)

    def _field_text(self, item: Mapping[str, Any], path: str | None) -> str | None:
        value = self._field(item, path)
        if value is _MISSING or value is None:
            return None
        return _to_text(value) or None

    def _field_datetime(self, item: Mapping[str, Any], path: str | None) -> datetime | None:
        value = self._field(item, path)
        if value is _MISSING or value is None or value == "":
            return None
        if isinstance(value, bool):
            raise self._parse_error(f"REST datetime path {path!r} resolved to a boolean")
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=UTC)
            except (OSError, OverflowError, ValueError) as exc:
                raise self._parse_error(f"REST datetime path {path!r} is invalid") from exc
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise self._parse_error(f"REST datetime path {path!r} is not ISO-8601") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _parse_error(self, message: str) -> CollectorError:
        return CollectorError(
            message,
            kind=CollectorErrorKind.PARSE,
            source_key=self.identity.key,
            retryable=False,
        )


def _get_path(value: object, path: str, *, default: object) -> object:
    current = value
    for segment in path.split("."):
        segment = segment.strip()
        if not segment:
            return default
        if isinstance(current, Mapping):
            if segment not in current:
                return default
            current = current[segment]
            continue
        if isinstance(current, list):
            try:
                index = int(segment)
            except ValueError:
                return default
            if index < 0 or index >= len(current):
                return default
            current = current[index]
            continue
        return default
    return current


def _to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _clean_path(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_scalar(value: object) -> str | int | None:
    if value is None or value is _MISSING or isinstance(value, (dict, list, bool)):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    return text or None


def _state_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
