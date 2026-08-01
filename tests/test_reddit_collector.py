from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.reddit import RedditCollector
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind


def post(post_id: str, title: str) -> dict[str, object]:
    return {
        "kind": "t3",
        "data": {
            "id": post_id,
            "name": f"t3_{post_id}",
            "title": title,
            "selftext": f"body {post_id}",
            "author": "alice",
            "subreddit": "SaaS",
            "permalink": f"/r/SaaS/comments/{post_id}/example/",
            "url": f"https://example.com/{post_id}",
            "created_utc": 1784973600,
            "edited": False,
            "score": 7,
            "num_comments": 2,
            "domain": "self.SaaS",
            "is_self": True,
            "locked": False,
            "stickied": False,
            "over_18": False,
            "link_flair_text": "Question",
        },
    }


def listing(*items: dict[str, object], after: str | None = None) -> dict[str, object]:
    return {"kind": "Listing", "data": {"after": after, "children": list(items)}}


def comment(comment_id: str, body: str) -> dict[str, object]:
    return {
        "kind": "t1",
        "data": {
            "id": comment_id,
            "name": f"t1_{comment_id}",
            "body": body,
            "author": "bob",
            "subreddit": "SaaS",
            "permalink": f"/r/SaaS/comments/abc/example/{comment_id}/",
            "created_utc": 1784977200,
            "edited": False,
            "score": 3,
            "parent_id": "t3_abc",
            "link_id": "t3_abc",
        },
    }


@pytest.mark.asyncio
async def test_reddit_oauth_post_comments_and_rate_limit_normalization() -> None:
    token_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        if request.url.path == "/api/v1/access_token":
            token_calls += 1
            assert request.method == "POST"
            assert request.headers["Authorization"].startswith("Basic ")
            assert b"grant_type=client_credentials" in request.content
            return httpx.Response(
                200,
                json={"access_token": "access-1", "token_type": "bearer", "expires_in": 3600},
            )
        if request.url.path == "/r/SaaS/new":
            assert request.headers["Authorization"] == "Bearer access-1"
            assert request.headers["User-Agent"] == "signalkit-test/1.0"
            assert request.url.params["raw_json"] == "1"
            return httpx.Response(
                200,
                json=listing(post("abc", "Need a CRM")),
                headers={
                    "X-Ratelimit-Remaining": "99.0",
                    "X-Ratelimit-Used": "1.0",
                    "X-Ratelimit-Reset": "60.0",
                },
            )
        if request.url.path == "/comments/abc":
            assert request.url.params["sort"] == "new"
            return httpx.Response(
                200,
                json=[
                    listing(),
                    {
                        "kind": "Listing",
                        "data": {
                            "children": [
                                comment("c1", "Try an open source option."),
                                {"kind": "more", "data": {"children": ["c2"]}},
                            ]
                        },
                    },
                ],
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "SaaS",
            client_id="client",
            client_secret="secret",
            user_agent="signalkit-test/1.0",
            include_comments=True,
            comments_per_post=5,
            instance="reddit-leads",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=10))

    assert token_calls == 1
    assert result.primary_count == 1
    assert [item.kind for item in result.events] == [SignalKind.POST, SignalKind.COMMENT]
    assert result.events[0].title == "Need a CRM"
    assert result.events[0].source_instance == "reddit-leads"
    assert result.events[0].url.startswith("https://www.reddit.com/r/SaaS/comments/abc")
    assert result.events[1].metadata["parent_event_id"] == result.events[0].id
    assert result.events[1].content == "Try an open source option."
    assert result.rate_limit is not None
    assert result.rate_limit.limit == 100
    assert result.rate_limit.remaining == 99
    assert result.rate_limit.reset_at is not None
    assert any("comments truncated" in warning for warning in result.warnings)


@pytest.mark.asyncio
async def test_reddit_cursor_pages_then_stops_at_previous_watermark() -> None:
    token_calls = 0
    completed_backfill = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls, completed_backfill
        if request.url.path == "/api/v1/access_token":
            token_calls += 1
            return httpx.Response(200, json={"access_token": "token", "expires_in": 3600})
        if request.url.path != "/r/SaaS/new":
            raise AssertionError(f"unexpected request: {request.url}")
        after = request.url.params.get("after")
        if after == "t3_p2":
            completed_backfill = True
            return httpx.Response(200, json=listing(post("p1", "one")))
        if completed_backfill:
            return httpx.Response(
                200,
                json=listing(post("p4", "four"), post("p3", "three"), after="t3_p3"),
            )
        return httpx.Response(
            200,
            json=listing(post("p3", "three"), post("p2", "two"), after="t3_p2"),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "SaaS",
            client_id="client",
            client_secret="secret",
            user_agent="signalkit-test/1.0",
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=2))
        second = await collector.collect(context=CollectorContext(limit=2), cursor=first.cursor)
        third = await collector.collect(context=CollectorContext(limit=20), cursor=second.cursor)

    assert token_calls == 1
    assert [item.metadata["external_id"] for item in first.events] == ["t3_p3", "t3_p2"]
    assert first.has_more is True
    assert first.cursor is not None and first.cursor.state["after"] == "t3_p2"
    assert [item.metadata["external_id"] for item in second.events] == ["t3_p1"]
    assert second.has_more is False
    assert [item.metadata["external_id"] for item in third.events] == ["t3_p4"]
    assert third.primary_count == 1
    assert third.has_more is False
    assert third.cursor is not None
    assert third.cursor.state["seen_ids"][:4] == ["t3_p4", "t3_p3", "t3_p2", "t3_p1"]


@pytest.mark.asyncio
async def test_reddit_refreshes_recent_post_for_new_comments() -> None:
    cycle = 1

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cycle
        if request.url.path == "/api/v1/access_token":
            return httpx.Response(200, json={"access_token": "token", "expires_in": 3600})
        if request.url.path == "/r/SaaS/new":
            return httpx.Response(200, json=listing(post("abc", "Need a CRM")))
        if request.url.path == "/comments/abc":
            assert request.url.params["sort"] == "new"
            current = (
                comment("c1", "first reply")
                if cycle == 1
                else comment("c2", "new reply")
            )
            return httpx.Response(
                200,
                json=[listing(), listing(current)],
                request=request,
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "SaaS",
            client_id="client",
            client_secret="secret",
            user_agent="signalkit-test/1.0",
            include_comments=True,
            comments_per_post=1,
            comment_refresh_window=1,
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=10))
        cycle = 2
        second = await collector.collect(
            context=CollectorContext(limit=10),
            cursor=first.cursor,
        )

    assert [event.metadata["external_id"] for event in first.events] == ["t3_abc", "t1_c1"]
    assert second.primary_count == 0
    assert [event.metadata["external_id"] for event in second.events] == ["t1_c2"]
    assert second.events[0].metadata["parent_event_id"] == first.events[0].id


@pytest.mark.asyncio
async def test_reddit_zero_remaining_uses_relative_reset_delay() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/access_token":
            return httpx.Response(200, json={"access_token": "token", "expires_in": 3600})
        return httpx.Response(
            200,
            json=listing(),
            headers={
                "X-Ratelimit-Remaining": "0.0",
                "X-Ratelimit-Used": "100.0",
                "X-Ratelimit-Reset": "47.5",
            },
        )

    before = datetime.now(UTC)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await RedditCollector(
            "SaaS",
            client_id="client",
            client_secret="secret",
            user_agent="signalkit-test/1.0",
            client=client,
        ).collect()

    assert result.rate_limit is not None
    assert result.rate_limit.remaining == 0
    assert result.rate_limit.limit == 100
    assert result.rate_limit.retry_after == 47.5
    assert result.rate_limit.reset_at is not None
    assert 46 <= (result.rate_limit.reset_at - before).total_seconds() <= 49


@pytest.mark.asyncio
async def test_reddit_oauth_http_error_is_normalized_as_auth() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "invalid credentials"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "SaaS",
            client_id="bad",
            client_secret="bad",
            user_agent="signalkit-test/1.0",
            client=client,
        )
        with pytest.raises(CollectorError) as caught:
            await collector.collect()

    assert caught.value.kind is CollectorErrorKind.AUTH
    assert caught.value.status_code == 401
    assert caught.value.retryable is False
