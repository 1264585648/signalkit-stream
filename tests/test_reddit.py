from datetime import UTC, datetime

import httpx
import pytest

from signalkit_stream.collectors.reddit import RedditCollector, RedditOAuth
from signalkit_stream.models import SignalKind
from signalkit_stream.protocol import CollectorContext


def post(name: str, created: float, *, title: str | None = None) -> dict:
    raw_id = name.removeprefix("t3_")
    return {
        "kind": "t3",
        "data": {
            "name": name,
            "id": raw_id,
            "title": title or f"Post {raw_id}",
            "selftext": f"Body {raw_id}",
            "author": "alice",
            "permalink": f"/r/python/comments/{raw_id}/post/",
            "url": f"https://example.com/{raw_id}",
            "created_utc": created,
            "edited": False,
            "score": 7,
            "num_comments": 3,
            "subreddit": "python",
            "link_flair_text": "Question",
            "over_18": False,
            "is_self": True,
        },
    }


def comment(name: str, created: float) -> dict:
    raw_id = name.removeprefix("t1_")
    return {
        "kind": "t1",
        "data": {
            "name": name,
            "id": raw_id,
            "body": f"Comment {raw_id}",
            "author": "bob",
            "permalink": f"/r/python/comments/post/comment/{raw_id}/",
            "created_utc": created,
            "edited": created + 30,
            "score": 4,
            "subreddit": "python",
            "parent_id": "t3_post",
            "link_id": "t3_post",
            "link_title": "A post",
            "link_url": "https://example.com/post",
        },
    }


@pytest.mark.asyncio
async def test_reddit_posts_normalize_auth_and_establish_incremental_boundary() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/r/python/new"
        return httpx.Response(
            200,
            json={
                "kind": "Listing",
                "data": {
                    "children": [post("t3_a", 1784973600), post("t3_b", 1784973500)],
                    "after": "t3_b",
                },
            },
            headers={
                "X-Ratelimit-Remaining": "42.5",
                "X-Ratelimit-Reset": "60",
            },
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "r/python",
            oauth=RedditOAuth(access_token="token"),
            user_agent="linux:signalkit-stream:test (by /u/example)",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=2))

    assert result.has_more is False
    assert result.primary_count == 2
    assert result.cursor.state["initialized"] is True
    assert result.cursor.state["seen_ids"] == ["t3_a", "t3_b"]
    assert result.rate_limit is not None
    assert result.rate_limit.remaining == 42
    assert result.rate_limit.reset_at is not None
    assert result.rate_limit.reset_at > datetime.now(UTC)

    event = result.events[0]
    assert event.kind is SignalKind.POST
    assert event.source_key == "reddit:r-python-posts"
    assert event.title == "Post a"
    assert event.content == "Body a"
    assert event.url == "https://www.reddit.com/r/python/comments/a/post/"
    assert event.metadata["outbound_url"] == "https://example.com/a"
    assert requests[0].headers["Authorization"] == "Bearer token"
    assert requests[0].headers["User-Agent"] == "linux:signalkit-stream:test (by /u/example)"


@pytest.mark.asyncio
async def test_reddit_incremental_cycle_pages_until_previous_seen_item() -> None:
    responses = [
        {"children": [post("t3_a", 30), post("t3_b", 20)], "after": "t3_b"},
        {"children": [post("t3_n1", 50), post("t3_n2", 40)], "after": "t3_n2"},
        {"children": [post("t3_n3", 35), post("t3_a", 30)], "after": "t3_a"},
    ]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        data = responses[len(requests) - 1]
        return httpx.Response(200, json={"kind": "Listing", "data": data}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            oauth=RedditOAuth(access_token="token"),
            user_agent="linux:signalkit-stream:test (by /u/example)",
            client=client,
        )
        baseline = await collector.collect(context=CollectorContext(limit=2))
        page_one = await collector.collect(context=CollectorContext(limit=2), cursor=baseline.cursor)
        page_two = await collector.collect(context=CollectorContext(limit=2), cursor=page_one.cursor)

    assert page_one.has_more is True
    assert [event.metadata["external_id"] for event in page_one.events] == ["t3_n1", "t3_n2"]
    assert page_one.cursor.state["after"] == "t3_n2"
    assert requests[2].url.params["after"] == "t3_n2"

    assert page_two.has_more is False
    assert [event.metadata["external_id"] for event in page_two.events] == ["t3_n3"]
    assert page_two.cursor.state["after"] is None
    assert page_two.cursor.state["cycle_ids"] == []
    assert page_two.cursor.state["seen_ids"][:5] == ["t3_n1", "t3_n2", "t3_n3", "t3_a", "t3_b"]


@pytest.mark.asyncio
async def test_reddit_comments_normalize_parent_relationships() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/r/python/comments"
        return httpx.Response(
            200,
            json={"kind": "Listing", "data": {"children": [comment("t1_c", 100)], "after": None}},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            listing="comments",
            oauth=RedditOAuth(access_token="token"),
            user_agent="linux:signalkit-stream:test (by /u/example)",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=10))

    event = result.events[0]
    assert event.kind is SignalKind.COMMENT
    assert event.content == "Comment c"
    assert event.metadata["parent_id"] == "t3_post"
    assert event.metadata["link_id"] == "t3_post"
    assert event.updated_at == datetime.fromtimestamp(130, tz=UTC)


@pytest.mark.asyncio
async def test_reddit_refresh_token_flow_is_cached() -> None:
    token_requests = 0
    listing_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests, listing_requests
        if request.url.path == "/api/v1/access_token":
            token_requests += 1
            assert request.headers["Authorization"].startswith("Basic ")
            assert b"grant_type=refresh_token" in request.content
            return httpx.Response(
                200,
                json={"access_token": "fresh-token", "token_type": "bearer", "expires_in": 3600},
                request=request,
            )
        listing_requests += 1
        assert request.headers["Authorization"] == "Bearer fresh-token"
        return httpx.Response(
            200,
            json={"kind": "Listing", "data": {"children": [], "after": None}},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            oauth=RedditOAuth(
                client_id="client-id",
                client_secret="secret",
                refresh_token="refresh-token",
            ),
            user_agent="linux:signalkit-stream:test (by /u/example)",
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=1))
        second = await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert token_requests == 1
    assert listing_requests == 2
