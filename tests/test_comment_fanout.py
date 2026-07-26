"""Comment fetches must fan out under a bound instead of running one at a time.

Each collector used to ``await`` its comment request inside the per-item loop, so a
poll with ``limit=100`` issued ~100 serialized round trips while holding the runtime's
per-source concurrency slot. These tests observe real transport concurrency and assert
that emitted event ORDER is still item-then-its-own-comments.
"""

from __future__ import annotations

import asyncio
from typing import Callable

import httpx
import pytest

from signalkit_stream.collectors.github import GitHubCollector
from signalkit_stream.collectors.hackernews import HackerNewsCollector
from signalkit_stream.collectors.reddit import RedditCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext

LATENCY = 0.02


class ConcurrencyTransport(httpx.AsyncBaseTransport):
    """An async transport that records how many requests are in flight at once.

    ``comment_peak`` counts only comment requests, because some adapters already
    fetch their primary items in parallel; the regression under test is specifically
    about comment fetches being serialized.
    """

    def __init__(
        self,
        handler: Callable[[httpx.Request], httpx.Response],
        *,
        comment_path: Callable[[str], bool],
    ) -> None:
        self._handler = handler
        self._comment_path = comment_path
        self.inflight = 0
        self.peak = 0
        self.comment_inflight = 0
        self.comment_peak = 0
        self.paths: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.paths.append(path)
        is_comment = self._comment_path(path)
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        if is_comment:
            self.comment_inflight += 1
            self.comment_peak = max(self.comment_peak, self.comment_inflight)
        try:
            await asyncio.sleep(LATENCY)
            return self._handler(request)
        finally:
            self.inflight -= 1
            if is_comment:
                self.comment_inflight -= 1


def _github_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/search/issues":
        return httpx.Response(
            200,
            json={
                "total_count": 6,
                "items": [
                    {
                        "node_id": f"ISSUE_{number}",
                        "repository_url": "https://api.github.com/repos/acme/app",
                        "number": number,
                        "title": f"Issue {number}",
                        "body": "body",
                        "html_url": f"https://github.com/acme/app/issues/{number}",
                        "created_at": "2026-07-01T10:00:00Z",
                        "updated_at": "2026-07-01T11:00:00Z",
                        "comments": 1,
                        "labels": [],
                        "user": {"login": "alice"},
                    }
                    for number in range(1, 7)
                ],
            },
            request=request,
        )
    number = int(request.url.path.split("/issues/")[1].split("/")[0])
    return httpx.Response(
        200,
        json=[
            {
                "node_id": f"COMMENT_{number}",
                "id": number,
                "body": f"comment on {number}",
                "html_url": f"https://github.com/acme/app/issues/{number}#c",
                "created_at": "2026-07-01T12:00:00Z",
                "user": {"login": "bob"},
            }
        ],
        request=request,
    )


@pytest.mark.asyncio
async def test_github_comment_fetches_fan_out_and_keep_event_order() -> None:
    transport = ConcurrencyTransport(
        _github_handler,
        comment_path=lambda path: path.endswith("/comments"),
    )
    async with httpx.AsyncClient(transport=transport) as client:
        collector = GitHubCollector(
            "is:issue",
            include_comments=True,
            comments_per_item=1,
            comment_concurrency=4,
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=6))

    assert transport.comment_peak > 1, "comment fetches were still sequential"
    assert transport.comment_peak <= 4
    assert [event.kind for event in result.events] == [
        SignalKind.ISSUE,
        SignalKind.COMMENT,
    ] * 6
    assert [event.metadata.get("number") or event.metadata["issue_number"] for event in result.events] == [
        1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6
    ]


def _hn_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path.endswith("/newstories.json"):
        return httpx.Response(200, json=[101, 102, 103, 104], request=request)
    item_id = int(request.url.path.split("/item/")[1].removesuffix(".json"))
    if item_id < 200:
        return httpx.Response(
            200,
            json={
                "id": item_id,
                "type": "story",
                "by": "alice",
                "time": 1782896400,
                "title": f"Story {item_id}",
                "kids": [item_id + 100],
            },
            request=request,
        )
    return httpx.Response(
        200,
        json={
            "id": item_id,
            "type": "comment",
            "by": "bob",
            "time": 1782896500,
            "parent": item_id - 100,
            "text": f"comment {item_id}",
        },
        request=request,
    )


@pytest.mark.asyncio
async def test_hackernews_comment_fetches_fan_out_and_keep_event_order() -> None:
    transport = ConcurrencyTransport(
        _hn_handler,
        comment_path=lambda path: "/item/2" in path,
    )
    async with httpx.AsyncClient(transport=transport) as client:
        collector = HackerNewsCollector(
            include_comments=True,
            comments_per_story=1,
            comment_concurrency=4,
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=4))

    comment_paths = [path for path in transport.paths if "/item/2" in path]
    assert len(comment_paths) == 4
    assert transport.comment_peak > 1, "comment fetches were still sequential"
    assert transport.comment_peak <= 4
    assert [event.kind for event in result.events] == [
        SignalKind.STORY,
        SignalKind.COMMENT,
    ] * 4
    assert [event.metadata["external_id"] for event in result.events] == [
        "101", "201", "102", "202", "103", "203", "104", "204",
    ]


def _reddit_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/api/v1/access_token":
        return httpx.Response(200, json={"access_token": "token", "expires_in": 3600})
    if request.url.path == "/r/SaaS/new":
        return httpx.Response(
            200,
            json={
                "kind": "Listing",
                "data": {
                    "after": None,
                    "children": [
                        {
                            "kind": "t3",
                            "data": {
                                "id": f"p{index}",
                                "name": f"t3_p{index}",
                                "title": f"Post {index}",
                                "selftext": "body",
                                "author": "alice",
                                "permalink": f"/r/SaaS/comments/p{index}/x/",
                                "created_utc": 1784973600,
                                "edited": False,
                            },
                        }
                        for index in range(1, 7)
                    ],
                },
            },
        )
    post_id = request.url.path.rsplit("/", 1)[-1]
    return httpx.Response(
        200,
        json=[
            {"kind": "Listing", "data": {"children": []}},
            {
                "kind": "Listing",
                "data": {
                    "children": [
                        {
                            "kind": "t1",
                            "data": {
                                "id": f"c{post_id}",
                                "name": f"t1_c{post_id}",
                                "body": f"comment on {post_id}",
                                "author": "bob",
                                "created_utc": 1784977200,
                                "edited": False,
                            },
                        }
                    ]
                },
            },
        ],
    )


@pytest.mark.asyncio
async def test_reddit_comment_fetches_fan_out_and_keep_event_order() -> None:
    transport = ConcurrencyTransport(
        _reddit_handler,
        comment_path=lambda path: path.startswith("/comments/"),
    )
    async with httpx.AsyncClient(transport=transport) as client:
        collector = RedditCollector(
            "SaaS",
            client_id="client",
            client_secret="secret",
            user_agent="signalkit-test/1.0",
            include_comments=True,
            comments_per_post=1,
            comment_concurrency=4,
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=6))

    assert transport.comment_peak > 1, "comment fetches were still sequential"
    assert transport.comment_peak <= 4
    assert [event.kind for event in result.events] == [
        SignalKind.POST,
        SignalKind.COMMENT,
    ] * 6
    assert [event.metadata["external_id"] for event in result.events] == [
        "t3_p1", "t1_cp1",
        "t3_p2", "t1_cp2",
        "t3_p3", "t1_cp3",
        "t3_p4", "t1_cp4",
        "t3_p5", "t1_cp5",
        "t3_p6", "t1_cp6",
    ]
