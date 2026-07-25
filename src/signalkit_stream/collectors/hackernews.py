from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Literal

import httpx

from signalkit_stream.collectors._text import html_to_text
from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor

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
        retry_policy: RetryPolicy | None = None,
        seen_window: int = 500,
    ) -> None:
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.feed = feed
        self.include_comments = include_comments
        self.comments_per_story = max(0, comments_per_story)
        self.seen_window = max(50, seen_window)
        self.source = "hackernews"
        self.instance = feed

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        seen_ids = [int(value) for value in (cursor.state.get("seen_ids", []) if cursor else [])]
        seen = set(seen_ids)

        async with self.http_client() as client:
            ids_response = await self.request(
                client,
                "GET",
                f"{self.API_ROOT}/{self.feed}.json",
                context=ctx,
            )
            all_ids = [int(value) for value in (ids_response.json() or [])]
            candidates = [item_id for item_id in all_ids if item_id not in seen]
            story_ids = candidates[: ctx.limit]
            stories = await asyncio.gather(
                *(self._get_item(client, story_id, context=ctx) for story_id in story_ids)
            )

            events: list[SignalEvent] = []
            processed_ids: list[int] = []
            for story_id, story in zip(story_ids, stories, strict=True):
                processed_ids.append(story_id)
                if not story or story.get("deleted") or story.get("dead"):
                    continue
                event = self._story_event(story)
                events.append(event)
                if self.include_comments and self.comments_per_story:
                    comment_ids = list(story.get("kids") or [])[: self.comments_per_story]
                    comments = await asyncio.gather(
                        *(
                            self._get_item(client, int(comment_id), context=ctx)
                            for comment_id in comment_ids
                        )
                    )
                    events.extend(
                        self._comment_event(comment, story_id=story["id"], parent_event_id=event.id)
                        for comment in comments
                        if comment and not comment.get("deleted") and not comment.get("dead")
                    )

        merged_seen = list(dict.fromkeys([*processed_ids, *seen_ids]))[: self.seen_window]
        next_cursor = Cursor(
            source_key=self.identity.key,
            state={"seen_ids": merged_seen, "feed": self.feed},
        )
        return CollectorResult(
            events=events,
            cursor=next_cursor,
            has_more=len(candidates) > len(story_ids),
            primary_count=len(story_ids),
            rate_limit=self.rate_limit,
        )

    async def _get_item(
        self,
        client: httpx.AsyncClient,
        item_id: int,
        *,
        context: CollectorContext,
    ) -> dict[str, Any] | None:
        response = await self.request(
            client,
            "GET",
            f"{self.API_ROOT}/item/{item_id}.json",
            context=context,
        )
        data = response.json()
        return data if isinstance(data, dict) else None

    def _story_event(self, item: dict[str, Any]) -> SignalEvent:
        item_id = str(item["id"])
        title = html_to_text(item.get("title")) or None
        text = html_to_text(item.get("text"))
        url = str(item.get("url") or f"https://news.ycombinator.com/item?id={item_id}")
        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                item_id,
                SignalKind.STORY,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
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
            id=SignalEvent.stable_id(
                self.source,
                item_id,
                SignalKind.COMMENT,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
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
