from __future__ import annotations

from datetime import UTC, datetime
import os
from typing import Any

import httpx

from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor


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
        page_size = max(1, min(100, int(state.get("per_page", min(ctx.limit, 100)))))
        effective_query = self.query
        if watermark:
            effective_query = f"{effective_query} updated:>{self._github_time(watermark)}"

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
            payload = response.json()
            items = payload.get("items", []) if isinstance(payload, dict) else []
            total_count = int(payload.get("total_count", len(items))) if isinstance(payload, dict) else len(items)

            selected_items = items[offset : offset + ctx.limit]
            events: list[SignalEvent] = []
            batch_watermark = watermark
            for item in selected_items:
                event = self._item_event(item)
                events.append(event)
                timestamp = event.updated_at or event.created_at
                if batch_watermark is None or timestamp > self._parse_time(batch_watermark):
                    batch_watermark = timestamp.isoformat()
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
                "watermark": batch_watermark or watermark or "",
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
        labels = [label.get("name") for label in item.get("labels", []) if label.get("name")]

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
            title=item.get("title"),
            content=str(item.get("body") or ""),
            author=(item.get("user") or {}).get("login"),
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
        payload = response.json()
        comments = payload if isinstance(payload, list) else []
        result: list[SignalEvent] = []
        for comment in comments[: self.comments_per_item]:
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
                    author=(comment.get("user") or {}).get("login"),
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

    @staticmethod
    def _parse_time(value: str | None) -> datetime:
        if not value:
            return datetime.now(UTC)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

    @staticmethod
    def _github_time(value: str) -> str:
        parsed = GitHubCollector._parse_time(value)
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _instance_for_query(query: str) -> str:
        import hashlib

        digest = hashlib.sha256(query.strip().encode("utf-8")).hexdigest()[:12]
        return f"search-{digest}"
