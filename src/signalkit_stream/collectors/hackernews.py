from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Literal

import httpx

from signalkit_stream.collectors._text import html_to_text
from signalkit_stream.collectors.base import HTTPCollector
from signalkit_stream.models import SignalEvent, SignalKind

HNFeed = Literal[
    "topstories",
    "newstories",
    "beststories",
    "askstories",
    "showstories",
    "jobstories",
]


class HackerNewsCollector(HTTPCollector):
    """Collector backed by Hacker News' public Firebase API."""

    API_ROOT = "https://hacker-news.firebaseio.com/v0"

    def __init__(
        self,
        *,
        feed: HNFeed = "newstories",
        include_comments: bool = False,
        comments_per_story: int = 3,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ) -> None:
        super().__init__(client=client, timeout=timeout)
        self.feed = feed
        self.include_comments = include_comments
        self.comments_per_story = max(0, comments_per_story)
        self.source = "hackernews"

    async def collect(self, *, limit: int = 100) -> list[SignalEvent]:
        if limit < 1:
            return []

        async with self.http_client() as client:
            ids_response = await client.get(f"{self.API_ROOT}/{self.feed}.json")
            ids_response.raise_for_status()
            story_ids = list(ids_response.json() or [])[:limit]
            stories = await asyncio.gather(
                *(self._get_item(client, story_id) for story_id in story_ids)
            )

            events: list[SignalEvent] = []
            for story in stories:
                if not story or story.get("deleted") or story.get("dead"):
                    continue
                event = self._story_event(story)
                events.append(event)
                if self.include_comments and self.comments_per_story:
                    comment_ids = list(story.get("kids") or [])[: self.comments_per_story]
                    comments = await asyncio.gather(
                        *(self._get_item(client, comment_id) for comment_id in comment_ids)
                    )
                    events.extend(
                        self._comment_event(comment, story_id=story["id"], parent_event_id=event.id)
                        for comment in comments
                        if comment and not comment.get("deleted") and not comment.get("dead")
                    )

        return events

    async def _get_item(self, client: httpx.AsyncClient, item_id: int) -> dict[str, Any] | None:
        response = await client.get(f"{self.API_ROOT}/item/{item_id}.json")
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else None

    def _story_event(self, item: dict[str, Any]) -> SignalEvent:
        item_id = str(item["id"])
        title = html_to_text(item.get("title")) or None
        text = html_to_text(item.get("text"))
        url = str(item.get("url") or f"https://news.ycombinator.com/item?id={item_id}")
        return SignalEvent(
            id=SignalEvent.stable_id(self.source, item_id, SignalKind.STORY),
            source=self.source,
            kind=SignalKind.STORY,
            title=title,
            content=text or title or "",
            author=item.get("by"),
            url=url,
            created_at=self._timestamp(item.get("time")),
            metadata={
                "external_id": item_id,
                "score": item.get("score"),
                "descendants": item.get("descendants"),
                "hn_type": item.get("type"),
            },
        )

    def _comment_event(
        self,
        item: dict[str, Any],
        *,
        story_id: int,
        parent_event_id: str,
    ) -> SignalEvent:
        item_id = str(item["id"])
        return SignalEvent(
            id=SignalEvent.stable_id(self.source, item_id, SignalKind.COMMENT),
            source=self.source,
            kind=SignalKind.COMMENT,
            content=html_to_text(item.get("text")),
            author=item.get("by"),
            url=f"https://news.ycombinator.com/item?id={item_id}",
            created_at=self._timestamp(item.get("time")),
            metadata={
                "external_id": item_id,
                "story_id": story_id,
                "parent_id": item.get("parent"),
                "parent_event_id": parent_event_id,
            },
        )

    @staticmethod
    def _timestamp(value: int | None) -> datetime:
        if value is None:
            return datetime.now(UTC)
        return datetime.fromtimestamp(value, tz=UTC)
