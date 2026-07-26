from __future__ import annotations

from datetime import UTC, datetime
import os
from typing import Any, Mapping

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


class GitHubCollector(HTTPCollector):
    """Incrementally search GitHub issues/PRs and optionally collect comments."""

    API_ROOT = "https://api.github.com"

    def __init__(
        self,
        query: str,
        *,
        token: str | None = None,
        include_comments: bool = False,
        comments_per_item: int = 5,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
        instance: str | None = None,
    ) -> None:
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        if not query.strip():
            raise ValueError("GitHub query must not be empty")
        self.query = query
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.include_comments = include_comments
        self.comments_per_item = max(0, comments_per_item)
        self.source = "github"
        self.instance = instance or self._instance_for_query(query)

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        page = max(1, int(state.get("page", 1)))
        offset = max(0, int(state.get("offset", 0)))
        watermark = str(state.get("watermark", "")).strip() or None
        watermark_time = self._cursor_time(watermark) if watermark else None
        page_size = max(1, min(100, int(state.get("per_page", min(ctx.limit, 100)))))
        effective_query = self.query
        if watermark_time is not None:
            effective_query = f"{effective_query} updated:>{self._github_time(watermark_time)}"

        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        async with self.http_client() as client:
            response = await self.request(
                client,
                "GET",
                f"{self.API_ROOT}/search/issues",
                context=ctx,
                params={
                    "q": effective_query,
                    "per_page": page_size,
                    "page": page,
                    "sort": "updated",
                    "order": "asc",
                },
                headers=headers,
            )
            payload = self._json_payload(response, label="GitHub search")
            if not isinstance(payload, dict):
                raise self._parse_error("GitHub search response must be a JSON object")
            items = payload.get("items", [])
            if not isinstance(items, list):
                raise self._parse_error("GitHub search items must be a list")
            total_count = self._safe_total_count(payload.get("total_count"), fallback=len(items))

            selected_items = items[offset : offset + ctx.limit]
            for item in selected_items:
                if not isinstance(item, dict):
                    raise self._parse_error("GitHub search item must be a JSON object")
            events: list[SignalEvent] = []
            batch_watermark_time = watermark_time
            for item in selected_items:
                event = self._item_event(item)
                events.append(event)
                timestamp = event.updated_at or event.created_at
                if batch_watermark_time is None or timestamp > batch_watermark_time:
                    batch_watermark_time = timestamp
                if self.include_comments and self.comments_per_item and item.get("comments"):
                    events.extend(
                        await self._comments(
                            client,
                            item=item,
                            parent_event_id=event.id,
                            headers=headers,
                            context=ctx,
                        )
                    )

        end_offset = offset + len(selected_items)
        page_has_unprocessed = end_offset < len(items)
        consumed_to_page_end = page * page_size
        source_has_next_page = (
            bool(items)
            and len(items) == page_size
            and consumed_to_page_end < min(total_count, 1000)
        )
        has_more = page_has_unprocessed or source_has_next_page
        if page_has_unprocessed:
            next_state = {
                "page": page,
                "offset": end_offset,
                "per_page": page_size,
                "watermark": watermark or "",
            }
        elif source_has_next_page:
            next_state = {
                "page": page + 1,
                "offset": 0,
                "per_page": page_size,
                "watermark": watermark or "",
            }
        else:
            next_state = {
                "page": 1,
                "offset": 0,
                "per_page": page_size,
                "watermark": (
                    batch_watermark_time.isoformat() if batch_watermark_time else watermark or ""
                ),
            }

        return CollectorResult(
            events=events,
            cursor=Cursor(source_key=self.identity.key, state=next_state),
            has_more=has_more,
            primary_count=len(selected_items),
            rate_limit=self.rate_limit,
        )

    def _item_event(self, item: dict[str, Any]) -> SignalEvent:
        is_pr = "pull_request" in item
        kind = SignalKind.PULL_REQUEST if is_pr else SignalKind.ISSUE
        repository_url = str(item.get("repository_url", ""))
        number = item.get("number")
        external_id = str(item.get("node_id") or f"{repository_url}#{number}")
        raw_labels = item.get("labels")
        labels = [
            label["name"]
            for label in (raw_labels if isinstance(raw_labels, list) else [])
            if isinstance(label, Mapping) and label.get("name")
        ]
        title = item.get("title")

        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                external_id,
                kind,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=kind,
            title=str(title) if title is not None else None,
            content=str(item.get("body") or ""),
            author=_login(item.get("user")),
            url=str(item.get("html_url") or repository_url),
            created_at=self._parse_time(item.get("created_at")),
            updated_at=self._parse_time(item.get("updated_at")) if item.get("updated_at") else None,
            metadata={
                "external_id": external_id,
                "repository_url": repository_url,
                "number": number,
                "state": item.get("state"),
                "comments": item.get("comments", 0),
                "labels": labels,
            },
        )

    async def _comments(
        self,
        client: httpx.AsyncClient,
        *,
        item: dict[str, Any],
        parent_event_id: str,
        headers: dict[str, str],
        context: CollectorContext,
    ) -> list[SignalEvent]:
        repository_url = str(item.get("repository_url", ""))
        number = item.get("number")
        if not repository_url or number is None:
            return []

        response = await self.request(
            client,
            "GET",
            f"{repository_url}/issues/{number}/comments",
            context=context,
            params={"per_page": min(self.comments_per_item, 100), "sort": "created", "direction": "asc"},
            headers=headers,
        )
        payload = self._json_payload(response, label="GitHub comments")
        if not isinstance(payload, list):
            raise self._parse_error("GitHub comments response must be a JSON array")
        comments = payload
        result: list[SignalEvent] = []
        for comment in comments[: self.comments_per_item]:
            if not isinstance(comment, dict):
                raise self._parse_error("GitHub comment must be a JSON object")
            external_id = str(comment.get("node_id") or comment.get("id"))
            result.append(
                SignalEvent(
                    id=SignalEvent.stable_id(
                        self.source,
                        external_id,
                        SignalKind.COMMENT,
                        source_instance=self.instance,
                    ),
                    source=self.source,
                    source_instance=self.instance,
                    kind=SignalKind.COMMENT,
                    content=str(comment.get("body") or ""),
                    author=_login(comment.get("user")),
                    url=str(comment.get("html_url") or item.get("html_url")),
                    created_at=self._parse_time(comment.get("created_at")),
                    updated_at=(
                        self._parse_time(comment.get("updated_at")) if comment.get("updated_at") else None
                    ),
                    metadata={
                        "external_id": external_id,
                        "repository_url": repository_url,
                        "issue_number": number,
                        "parent_event_id": parent_event_id,
                    },
                )
            )
        return result

    def _json_payload(self, response: httpx.Response, *, label: str) -> Any:
        """Decode a JSON body, mapping decode failures onto the PARSE contract.

        GitHub answers with an HTML error/maintenance page under a 200 often enough that
        an unguarded ``response.json()`` turns a transient upstream blip into an
        ``INTERNAL`` error, which the contract validator reserves for broken adapters.
        """

        try:
            return response.json()
        except ValueError as exc:
            raise self._parse_error(
                f"invalid JSON in {label} response",
                details={"content_type": response.headers.get("Content-Type", "")},
            ) from exc

    def _parse_time(self, value: str | None) -> datetime:
        if not value:
            return datetime.now(UTC)
        return self._time(value, field="timestamp", kind=CollectorErrorKind.PARSE)

    def _cursor_time(self, value: str) -> datetime:
        return self._time(value, field="cursor watermark", kind=CollectorErrorKind.CURSOR)

    def _time(self, value: str, *, field: str, kind: CollectorErrorKind) -> datetime:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise CollectorError(
                f"GitHub {field} is not an ISO-8601 timestamp",
                kind=kind,
                source_key=self.identity.key,
                retryable=False,
                details={"field": field, "value": text[:80]},
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _safe_total_count(self, value: object, *, fallback: int) -> int:
        if value is None:
            return fallback
        if isinstance(value, bool):
            raise self._parse_error("GitHub total_count must be an integer")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise self._parse_error("GitHub total_count must be an integer") from exc

    def _parse_error(self, message: str, *, details: Mapping[str, Any] | None = None) -> CollectorError:
        return CollectorError(
            message,
            kind=CollectorErrorKind.PARSE,
            source_key=self.identity.key,
            retryable=False,
            details=details,
        )

    @staticmethod
    def _github_time(value: datetime) -> str:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _instance_for_query(query: str) -> str:
        import hashlib

        digest = hashlib.sha256(query.strip().encode("utf-8")).hexdigest()[:12]
        return f"search-{digest}"


def _login(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    login = value.get("login")
    return str(login) if login is not None else None
