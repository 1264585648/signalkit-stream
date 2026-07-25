from __future__ import annotations

from datetime import UTC, datetime
import os
from typing import Any

import httpx

from signalkit_stream.collectors.base import HTTPCollector
from signalkit_stream.models import SignalEvent, SignalKind


class GitHubCollector(HTTPCollector):
    """Search public GitHub issues/PRs and optionally collect their comments."""

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
    ) -> None:
        super().__init__(client=client, timeout=timeout)
        self.query = query
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.include_comments = include_comments
        self.comments_per_item = max(0, comments_per_item)
        self.source = "github"

    async def collect(self, *, limit: int = 100) -> list[SignalEvent]:
        if limit < 1:
            return []

        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        events: list[SignalEvent] = []
        remaining = min(limit, 1000)
        page = 1

        async with self.http_client() as client:
            while remaining > 0:
                page_size = min(remaining, 100)
                response = await client.get(
                    f"{self.API_ROOT}/search/issues",
                    params={"q": self.query, "per_page": page_size, "page": page},
                    headers=headers,
                )
                response.raise_for_status()
                items = response.json().get("items", [])
                if not items:
                    break

                for item in items:
                    event = self._item_event(item)
                    events.append(event)
                    if self.include_comments and self.comments_per_item and item.get("comments"):
                        events.extend(
                            await self._comments(
                                client,
                                item=item,
                                parent_event_id=event.id,
                                headers=headers,
                            )
                        )

                remaining -= len(items)
                if len(items) < page_size:
                    break
                page += 1

        return events

    def _item_event(self, item: dict[str, Any]) -> SignalEvent:
        is_pr = "pull_request" in item
        kind = SignalKind.PULL_REQUEST if is_pr else SignalKind.ISSUE
        repository_url = str(item.get("repository_url", ""))
        number = item.get("number")
        external_id = str(item.get("node_id") or f"{repository_url}#{number}")
        labels = [label.get("name") for label in item.get("labels", []) if label.get("name")]

        return SignalEvent(
            id=SignalEvent.stable_id(self.source, external_id, kind),
            source=self.source,
            kind=kind,
            title=item.get("title"),
            content=str(item.get("body") or ""),
            author=(item.get("user") or {}).get("login"),
            url=str(item.get("html_url") or repository_url),
            created_at=self._parse_time(item.get("created_at")),
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
    ) -> list[SignalEvent]:
        repository_url = str(item.get("repository_url", ""))
        number = item.get("number")
        if not repository_url or number is None:
            return []

        response = await client.get(
            f"{repository_url}/issues/{number}/comments",
            params={"per_page": min(self.comments_per_item, 100)},
            headers=headers,
        )
        response.raise_for_status()
        result: list[SignalEvent] = []
        for comment in response.json()[: self.comments_per_item]:
            external_id = str(comment.get("node_id") or comment.get("id"))
            result.append(
                SignalEvent(
                    id=SignalEvent.stable_id(self.source, external_id, SignalKind.COMMENT),
                    source=self.source,
                    kind=SignalKind.COMMENT,
                    content=str(comment.get("body") or ""),
                    author=(comment.get("user") or {}).get("login"),
                    url=str(comment.get("html_url") or item.get("html_url")),
                    created_at=self._parse_time(comment.get("created_at")),
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
