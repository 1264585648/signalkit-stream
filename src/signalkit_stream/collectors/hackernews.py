from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Literal, Mapping

import httpx

from signalkit_stream.collectors._text import html_to_text, validated_seen_window
from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)

DEFAULT_COMMENT_CONCURRENCY = 6

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
        comment_concurrency: int = DEFAULT_COMMENT_CONCURRENCY,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
        seen_window: int = 500,
    ) -> None:
        super().__init__(client=client, timeout=timeout, retry_policy=retry_policy)
        self.feed = feed
        self.include_comments = include_comments
        self.comments_per_story = max(0, comments_per_story)
        if comment_concurrency < 1:
            raise ValueError("Hacker News comment_concurrency must be >= 1")
        self.comment_concurrency = comment_concurrency
        self.seen_window = validated_seen_window(seen_window, label="Hacker News")
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
        raw_seen = cursor.state.get("seen_ids", []) if cursor else []
        try:
            seen_ids = [int(value) for value in raw_seen]
        except (TypeError, ValueError) as exc:
            raise CollectorError(
                "Hacker News cursor seen_ids must contain integers",
                kind=CollectorErrorKind.CURSOR,
                source_key=self.identity.key,
                retryable=False,
            ) from exc
        seen = set(seen_ids)

        async with self.http_client() as client:
            ids_response = await self.request(
                client,
                "GET",
                f"{self.API_ROOT}/{self.feed}.json",
                context=ctx,
            )
            all_ids = self._item_ids(ids_response)
            candidates = [item_id for item_id in all_ids if item_id not in seen]
            story_ids = candidates[: ctx.limit]
            stories = await asyncio.gather(
                *(self._get_item(client, story_id, context=ctx) for story_id in story_ids)
            )

            processed_ids: list[int] = []
            live_stories: list[tuple[dict[str, Any], SignalEvent]] = []
            for story_id, story in zip(story_ids, stories, strict=True):
                processed_ids.append(story_id)
                if not story or story.get("deleted") or story.get("dead"):
                    continue
                live_stories.append((story, self._story_event(story)))

            comment_batches = await self._comment_batches(
                client,
                stories=[story for story, _ in live_stories],
                context=ctx,
            )

        events: list[SignalEvent] = []
        for (story, event), comments in zip(live_stories, comment_batches, strict=True):
            events.append(event)
            events.extend(
                self._comment_event(
                    comment,
                    story_id=self._required_id(story),
                    parent_event_id=event.id,
                )
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

    async def _comment_batches(
        self,
        client: httpx.AsyncClient,
        *,
        stories: list[dict[str, Any]],
        context: CollectorContext,
    ) -> list[list[dict[str, Any] | None]]:
        """Fetch every story's comments in one bounded, order-preserving fan-out.

        The per-story ``gather`` used to sit inside a sequential story loop, so a poll
        with ``limit=100`` issued ~100 serialized round trips while holding the runtime's
        per-source concurrency slot. Story/comment pairs are hoisted into a single
        gather bounded by ``comment_concurrency`` instead.
        """

        if not (self.include_comments and self.comments_per_story):
            return [[] for _ in stories]

        pairs = [
            (index, comment_id)
            for index, story in enumerate(stories)
            for comment_id in self._comment_ids(story)
        ]
        semaphore = asyncio.Semaphore(self.comment_concurrency)

        async def fetch(comment_id: int) -> dict[str, Any] | None:
            async with semaphore:
                return await self._get_item(client, comment_id, context=context)

        results = await asyncio.gather(
            *(fetch(comment_id) for _, comment_id in pairs),
            return_exceptions=True,
        )
        batches: list[list[dict[str, Any] | None]] = [[] for _ in stories]
        for (index, _), result in zip(pairs, results, strict=True):
            if isinstance(result, BaseException):
                raise result
            batches[index].append(result)
        return batches

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
        data = self._json_payload(response, label=f"Hacker News item {item_id}")
        return data if isinstance(data, dict) else None

    def _item_ids(self, response: httpx.Response) -> list[int]:
        """Decode the feed's id array, mapping malformed bodies onto the PARSE contract."""

        payload = self._json_payload(response, label=f"Hacker News {self.feed}")
        if payload is None:
            return []
        if not isinstance(payload, list):
            raise self._parse_error(f"Hacker News {self.feed} response must be a JSON array")
        return [self._item_id(value, field=f"{self.feed} id") for value in payload]

    def _comment_ids(self, story: dict[str, Any]) -> list[int]:
        raw_kids = story.get("kids") or []
        if not isinstance(raw_kids, list):
            raise self._parse_error("Hacker News story kids must be a JSON array")
        return [
            self._item_id(value, field="comment id")
            for value in raw_kids[: self.comments_per_story]
        ]

    def _item_id(self, value: object, *, field: str) -> int:
        if isinstance(value, bool):
            raise self._parse_error(f"Hacker News {field} must be an integer")
        try:
            return int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise self._parse_error(
                f"Hacker News {field} must be an integer",
                details={"value": str(value)[:80]},
            ) from exc

    def _required_id(self, item: dict[str, Any]) -> int:
        if "id" not in item:
            raise self._parse_error("Hacker News item is missing its id")
        return self._item_id(item["id"], field="item id")

    def _json_payload(self, response: httpx.Response, *, label: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise self._parse_error(
                f"invalid JSON in {label} response",
                details={"content_type": response.headers.get("Content-Type", "")},
            ) from exc

    def _parse_error(
        self,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> CollectorError:
        return CollectorError(
            message,
            kind=CollectorErrorKind.PARSE,
            source_key=self.identity.key,
            retryable=False,
            details=details,
        )

    def _story_event(self, item: dict[str, Any]) -> SignalEvent:
        item_id = str(self._required_id(item))
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
        item_id = str(self._required_id(item))
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
