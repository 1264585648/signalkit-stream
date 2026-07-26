from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import httpx
import pytest

from signalkit_stream.collectors import (
    GitHubCollector,
    HackerNewsCollector,
    JSONFeedCollector,
    RSSCollector,
    RedditCollector,
)
from signalkit_stream.collectors.base import Collector
from signalkit_stream.protocol import CollectorContext, Cursor


@dataclass(frozen=True)
class AdapterCase:
    name: str
    handler: Callable[[httpx.Request], httpx.Response]
    build: Callable[[httpx.AsyncClient], Collector]


def rss_handler(request: httpx.Request) -> httpx.Response:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>Example</title><link>https://example.com/</link>
<item>
  <guid>post-1</guid><title>Hello RSS</title>
  <link>https://example.com/posts/1</link>
  <description>Body</description>
  <pubDate>Sat, 25 Jul 2026 12:00:00 GMT</pubDate>
</item>
</channel></rss>"""
    return httpx.Response(200, text=xml, request=request)


def jsonfeed_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "version": "https://jsonfeed.org/version/1.1",
            "title": "Example",
            "items": [
                {
                    "id": "post-1",
                    "title": "Hello JSON Feed",
                    "content_text": "Body",
                    "url": "https://example.com/posts/1",
                    "date_published": "2026-07-25T12:00:00Z",
                }
            ],
        },
        request=request,
    )


def hn_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path.endswith("/newstories.json"):
        return httpx.Response(200, json=[123], request=request)
    if request.url.path.endswith("/item/123.json"):
        return httpx.Response(
            200,
            json={
                "id": 123,
                "type": "story",
                "by": "alice",
                "time": 1784980800,
                "title": "Hello HN",
                "text": "Body",
                "url": "https://example.com/posts/1",
                "kids": [],
            },
            request=request,
        )
    raise AssertionError(f"unexpected Hacker News request: {request.url}")


def github_handler(request: httpx.Request) -> httpx.Response:
    assert request.url.path == "/search/issues"
    return httpx.Response(
        200,
        json={
            "total_count": 1,
            "incomplete_results": False,
            "items": [
                {
                    "id": 1001,
                    "node_id": "I_1001",
                    "number": 1,
                    "title": "Hello GitHub",
                    "body": "Body",
                    "html_url": "https://github.com/acme/app/issues/1",
                    "repository_url": "https://api.github.com/repos/acme/app",
                    "user": {"login": "alice"},
                    "created_at": "2026-07-25T12:00:00Z",
                    "updated_at": "2026-07-25T12:30:00Z",
                    "state": "open",
                    "comments": 0,
                    "labels": [],
                }
            ],
        },
        request=request,
    )


def reddit_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/api/v1/access_token":
        return httpx.Response(
            200,
            json={"access_token": "token", "token_type": "bearer", "expires_in": 3600},
            request=request,
        )
    assert request.url.path == "/r/python/new"
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
                            "name": "t3_post1",
                            "id": "post1",
                            "title": "Hello Reddit",
                            "selftext": "Body",
                            "author": "alice",
                            "permalink": "/r/python/comments/post1/hello/",
                            "url": "https://example.com/posts/1",
                            "created_utc": 1784980800,
                            "edited": False,
                            "score": 5,
                            "num_comments": 0,
                            "subreddit": "python",
                            "over_18": False,
                            "is_self": True,
                        },
                    }
                ],
            },
        },
        request=request,
    )


CASES = [
    AdapterCase(
        "rss",
        rss_handler,
        lambda client: RSSCollector(
            "https://example.com/feed.xml",
            instance="contract",
            client=client,
        ),
    ),
    AdapterCase(
        "jsonfeed",
        jsonfeed_handler,
        lambda client: JSONFeedCollector(
            "https://example.com/feed.json",
            instance="contract",
            client=client,
        ),
    ),
    AdapterCase(
        "hackernews",
        hn_handler,
        lambda client: HackerNewsCollector(feed="newstories", client=client),
    ),
    AdapterCase(
        "github",
        github_handler,
        lambda client: GitHubCollector("is:issue", instance="contract", client=client),
    ),
    AdapterCase(
        "reddit",
        reddit_handler,
        lambda client: RedditCollector(
            "python",
            client_id="client-id",
            client_secret="client-secret",
            user_agent="linux:signalkit-stream:contract (by /u/example)",
            instance="contract",
            client=client,
        ),
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
async def test_first_party_collectors_share_event_and_cursor_contract(case: AdapterCase) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(case.handler)) as client:
        collector = case.build(client)
        first = await collector.collect(context=CollectorContext(limit=1))
        replay = await collector.collect(context=CollectorContext(limit=1))

    assert first.primary_count == 1
    assert first.events
    assert first.cursor.source_key == collector.identity.key
    assert Cursor.from_json(first.cursor.to_json()) == first.cursor

    ids = [event.id for event in first.events]
    assert len(ids) == len(set(ids))
    assert all(event.source_key == collector.identity.key for event in first.events)
    assert all(event.created_at.tzinfo is not None for event in first.events)
    assert all(event.updated_at is None or event.updated_at.tzinfo is not None for event in first.events)

    assert [event.id for event in replay.events] == ids
    assert [event.fingerprint() for event in replay.events] == [
        event.fingerprint() for event in first.events
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
async def test_first_party_collectors_accept_their_own_cursor(case: AdapterCase) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(case.handler)) as client:
        collector = case.build(client)
        first = await collector.collect(context=CollectorContext(limit=1))
        second = await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert second.cursor.source_key == collector.identity.key
    assert all(event.source_key == collector.identity.key for event in second.events)
    if second.has_more:
        assert second.cursor != first.cursor
        assert second.primary_count > 0
