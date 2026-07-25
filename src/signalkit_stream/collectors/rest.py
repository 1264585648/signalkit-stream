from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal, Mapping, Sequence
from urllib.parse import urlencode

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

PaginationMode = Literal["page", "offset"]


class GenericRESTCollector(HTTPCollector):
    """Collect one explicitly configured JSON endpoint.

    This adapter intentionally does not guess arbitrary API semantics. The caller
    provides the item list path plus field mappings, optional fixed query parameters,
    and one of two deterministic pagination modes. It is useful for small public JSON
    APIs and as a reference implementation for third-party adapters.
    """

    def __init__(
        self,
        url: str,
        *,
        item_path: str,
        id_field: str,
        content_field: str,
        url_field: str,
        created_at_field: str,
        title_field: str | None = None,
        author_field: str | None = None,
        updated_at_field: str | None = None,
        source: str = "rest",
        instance: str | None = None,
        kind: SignalKind = SignalKind.POST,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
        query: Mapping[str, str] | None = None,
        pagination: PaginationMode = "page",
        page_param: str = "page",
        offset_param: str = "offset",
        limit_param: str = "limit",
        page_size: int = 100,
        seen_window: int = 500,
        initial_backfill: bool = True,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not url.strip():
            raise ValueError("REST URL must not be empty")
        if not item_path.strip():
            raise ValueError("item_path must not be empty")
        if not id_field.strip():
            raise ValueError("id_field must not be empty")
        if not content_field.strip():
            raise ValueError("content_field must not be empty")
        if not url_field.strip():
            raise ValueError("url_field must not be empty")
        if not created_at_field.strip():
            raise ValueError("created_at_field must not be empty")
        if not source.strip():
            raise ValueError("source must not be empty")
        if method.upper() != "GET":
            raise ValueError("GenericRESTCollector currently supports GET endpoints only")
        if pagination not in {"page", "offset"}:
            raise ValueError("pagination must be 'page' or 'offset'")
        if page_size < 1:
            raise ValueError("page_size must be >= 1")
        if seen_window < 1:
            raise ValueError("seen_window must be >= 1")
        if not isinstance(initial_backfill, bool):
            raise ValueError("initial_backfill must be a boolean")

        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.url = url
        self.item_path = item_path
        self.id_field = id_field
        self.content_field = content_field
        self.url_field = url_field
        self.created_at_field = created_at_field
        self.title_field = title_field
        self.author_field = author_field
        self.updated_at_field = updated_at_field
        self.source = source
        self.instance = instance or _stable_instance(url, query or {})
        self.kind = kind
        self.method = method.upper()
        self.headers = dict(headers or {})
        self.query = dict(query or {})
        self.pagination = pagination
        self.page_param = page_param
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.page_size = page_size
        self.seen_window = seen_window
        self.initial_backfill = initial_backfill

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        if ctx.limit < 1:
            return CollectorResult(
                events=[],
                cursor=cursor,
                primary_count=0,
                has_more=False,
                rate_limit=self.rate_limit,
            )

        state = dict(cursor.state) if cursor else {}
        initialized = bool(state.get("initialized", False))
        seen_ids = [str(value) for value in state.get("seen_ids", []) if str(value)]
        seen = set(seen_ids)
        cycle_ids = [str(value) for value in state.get("cycle_ids", []) if str(value)]
        cycle_seen = set(cycle_ids)
        page = _safe_int(state.get("page"), 1)
        offset = _safe_int(state.get("offset"), 0)

        params = dict(self.query)
        request_size = min(self.page_size, max(ctx.limit, 1))
        params[self.limit_param] = str(request_size)
        if self.pagination == "page":
            params[self.page_param] = str(page)
        else:
            params[self.offset_param] = str(offset)

        async with self.http_client() as client:
            response = await self.request(
                client,
                self.method,
                self.url,
                context=ctx,
                params=params,
                headers=self.headers or None,
            )

        payload = _json_payload(response, source_key=self.identity.key)
        raw_items = _extract_path(payload, self.item_path)
        if not isinstance(raw_items, list):
            raise CollectorError(
                f"REST item_path {self.item_path!r} did not resolve to a list",
                kind=CollectorErrorKind.PARSE,
                source_key=self.identity.key,
                retryable=False,
            )

        if not initialized and not self.initial_backfill:
            bootstrap_ids = [
                _required_text(raw, self.id_field, source_key=self.identity.key)
                for raw in raw_items
                if isinstance(raw, Mapping)
            ]
            return CollectorResult(
                events=[],
                cursor=Cursor(
                    self.identity.key,
                    {
                        "initialized": True,
                        "seen_ids": _merge_seen(bootstrap_ids, seen_ids, limit=self.seen_window),
                        "cycle_ids": [],
                        "page": 1,
                        "offset": 0,
                    },
                ),
                has_more=False,
                primary_count=0,
                rate_limit=self.rate_limit,
                warnings=[
                    "initial_backfill=false seeded the recent REST watermark without emitting history"
                ],
            )

        events: list[SignalEvent] = []
        scanned_ids: list[str] = []
        complete = False
        page_fully_scanned = True

        for raw in raw_items:
            if not isinstance(raw, Mapping):
                continue
            external = _required_text(raw, self.id_field, source_key=self.identity.key)
            scanned_ids.append(external)
            if external in cycle_seen:
                continue
            cycle_seen.add(external)
            cycle_ids.append(external)

            if initialized and external in seen:
                complete = True
                break

            events.append(self._event(raw, external_id=external))
            if len(events) >= ctx.limit:
                page_fully_scanned = False
                break

        raw_exhausted = len(raw_items) < request_size
        if raw_exhausted:
            complete = True

        if complete:
            new_seen = _merge_seen(cycle_ids, seen_ids, limit=self.seen_window)
            next_state: dict[str, Any] = {
                "initialized": True,
                "seen_ids": new_seen,
                "cycle_ids": [],
                "page": 1,
                "offset": 0,
            }
            has_more = False
        elif not page_fully_scanned:
            # The generic protocol cannot resume in the middle of an API page. Refetch
            # the page and skip cycle_ids; this is deterministic and avoids loss.
            next_state = {
                "initialized": initialized,
                "seen_ids": seen_ids,
                "cycle_ids": cycle_ids,
                "page": page,
                "offset": offset,
            }
            has_more = True
        else:
            next_state = {
                "initialized": initialized,
                "seen_ids": seen_ids,
                "cycle_ids": cycle_ids,
                "page": page + 1 if self.pagination == "page" else page,
                "offset": offset + len(raw_items) if self.pagination == "offset" else offset,
            }
            has_more = bool(raw_items)

        return CollectorResult(
            events=events,
            cursor=Cursor(self.identity.key, next_state),
            has_more=has_more,
            primary_count=len(events),
            rate_limit=self.rate_limit,
            warnings=([] if scanned_ids else ["REST page contained no addressable items"]),
        )

    def _event(self, raw: Mapping[str, Any], *, external_id: str) -> SignalEvent:
        content = _required_text(raw, self.content_field, source_key=self.identity.key)
        url = _required_text(raw, self.url_field, source_key=self.identity.key)
        created_text = _required_text(raw, self.created_at_field, source_key=self.identity.key)
        created_at = _parse_datetime(created_text, source_key=self.identity.key)
        updated_at = None
        if self.updated_at_field:
            updated_value = _optional_text(raw, self.updated_at_field)
            if updated_value:
                updated_at = _parse_datetime(updated_value, source_key=self.identity.key)

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
            title=_optional_text(raw, self.title_field),
            content=content,
            author=_optional_text(raw, self.author_field),
            url=url,
            created_at=created_at,
            updated_at=updated_at,
            metadata={
                "external_id": external_id,
                "rest_endpoint": self.url,
                "raw": _json_safe(raw),
            },
        )


def _stable_instance(url: str, query: Mapping[str, str]) -> str:
    material = url
    if query:
        material += "?" + urlencode(sorted((str(key), str(value)) for key, value in query.items()))
    return "rest-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


def _json_payload(response: httpx.Response, *, source_key: str) -> object:
    try:
        return response.json()
    except ValueError as exc:
        raise CollectorError(
            "REST endpoint returned invalid JSON",
            kind=CollectorErrorKind.PARSE,
            source_key=source_key,
            retryable=False,
        ) from exc


def _extract_path(payload: object, path: str) -> object:
    current = payload
    for segment in path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return None
        current = current[segment]
    return current


def _required_text(raw: Mapping[str, Any], path: str, *, source_key: str) -> str:
    value = _extract_path(raw, path)
    if value is None or not str(value).strip():
        raise CollectorError(
            f"REST item is missing required field {path!r}",
            kind=CollectorErrorKind.PARSE,
            source_key=source_key,
            retryable=False,
        )
    return str(value).strip()


def _optional_text(raw: Mapping[str, Any], path: str | None) -> str | None:
    if not path:
        return None
    value = _extract_path(raw, path)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_datetime(value: str, *, source_key: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CollectorError(
            f"REST timestamp is not ISO-8601: {value!r}",
            kind=CollectorErrorKind.PARSE,
            source_key=source_key,
            retryable=False,
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _merge_seen(primary: Sequence[str], prior: Sequence[str], *, limit: int) -> list[str]:
    return list(dict.fromkeys([*primary, *prior]))[:limit]


def _safe_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _json_safe(raw: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(json.dumps(dict(raw), ensure_ascii=False, default=str))
    except (TypeError, ValueError):
        return {key: str(value) for key, value in raw.items()}
