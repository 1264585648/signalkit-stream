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
        comment_refresh_window: int = 10,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
        seen_window: int = 500,
    ) -> None:
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.feed = feed
        self.include_comments = include_comments
        self.comments_per_story = max(0, comments_per_story)
        self.comment_refresh_window = max(0, comment_refresh_window)
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
        state = dict(cursor.state) if cursor else {}
        seen_ids = [int(value) for value in state.get("seen_ids", [])]
        refresh_ids = [int(value) for value in state.get("comment_refresh_ids", seen_ids)]
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
                    events.extend(
                        await self._comment_events_for_story(
                            client,
                            story=story,
                            parent_event_id=event.id,
                            context=ctx,
                        )
                    )

            caught_up = len(candidates) <= len(story_ids)
            if (
                caught_up
                and self.include_comments
                and self.comments_per_story
                and self.comment_refresh_window
            ):
                new_story_ids = set(story_ids)
                refresh_story_ids = [
                    story_id
                    for story_id in dict.fromkeys([*processed_ids, *refresh_ids, *seen_ids])
                    if story_id not in new_story_ids
                ][: self.comment_refresh_window]
                refreshed_stories = await asyncio.gather(
                    *(
                        self._get_item(client, story_id, context=ctx)
                        for story_id in refresh_story_ids
                    )
                )
                for story_id, story in zip(
                    refresh_story_ids,
                    refreshed_stories,
                    strict=True,
                ):
                    if not story or story.get("deleted") or story.get("dead"):
                        continue
                    parent_event_id = SignalEvent.stable_id(
                        self.source,
                        str(story_id),
                        SignalKind.STORY,
                        source_instance=self.instance,
                    )
                    events.extend(
                        await self._comment_events_for_story(
                            client,
                            story=story,
                            parent_event_id=parent_event_id,
                            context=ctx,
                        )
                    )

        merged_seen = list(dict.fromkeys([*processed_ids, *seen_ids]))[: self.seen_window]
        merged_refresh = (
            list(dict.fromkeys([*processed_ids, *refresh_ids, *seen_ids]))[
                : self.comment_refresh_window
            ]
            if self.comment_refresh_window
            else []
        )
        next_cursor = Cursor(
            source_key=self.identity.key,
            state={
                "seen_ids": merged_seen,
                "comment_refresh_ids": merged_refresh,
                "feed": self.feed,
            },
        )
        return CollectorResult(
            events=events,
            cursor=next_cursor,
            has_more=len(candidates) > len(story_ids),
            primary_count=len(story_ids),
            rate_limit=self.rate_limit,
        )

    async def _comment_events_for_story(
        self,
        client: httpx.AsyncClient,
        *,
        story: dict[str, Any],
        parent_event_id: str,
        context: CollectorContext,
    ) -> list[SignalEvent]:
        story_id = int(story["id"])
        comment_ids = self._recent_comment_ids(story)
        comments = await asyncio.gather(
            *(
                self._get_item(client, comment_id, context=context)
                for comment_id in comment_ids
            )
        )
        return [
            self._comment_event(
                comment,
                story_id=story_id,
                parent_event_id=parent_event_id,
            )
            for comment in comments
            if comment and not comment.get("deleted") and not comment.get("dead")
        ]

    def _recent_comment_ids(self, story: dict[str, Any]) -> list[int]:
        values: set[int] = set()
        for value in story.get("kids") or []:
            try:
                values.add(int(value))
            except (TypeError, ValueError):
                continue
        # Hacker News item IDs are monotonic, so the highest IDs are the newest
        # direct comments. This avoids permanently pinning collection to the first
        # highly ranked comments on an active thread.
        return sorted(values, reverse=True)[: self.comments_per_story]

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
