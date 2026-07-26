from __future__ import annotations

import base64

import httpx
import pytest

from signalkit_stream.collectors.reddit import RedditCollector
from signalkit_stream.config import SourceConfig
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind
from signalkit_stream.registry import default_registry


def listing(request: httpx.Request) -> httpx.Response:
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


@pytest.mark.asyncio
async def test_static_access_token_skips_token_endpoint() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/r/python/new"
        assert request.headers["Authorization"] == "bearer static-token"
        return listing(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            access_token="static-token",
            user_agent="signalkit-test",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=1))

    assert collector.auth_mode == "access_token"
    assert result.primary_count == 1
    assert [request.url.path for request in requests] == ["/r/python/new"]


@pytest.mark.asyncio
async def test_refresh_token_allows_installed_client_empty_secret_and_caches_access_token() -> None:
    token_requests = 0
    listing_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests, listing_requests
        if request.url.path == "/api/v1/access_token":
            token_requests += 1
            body = request.content.decode()
            assert "grant_type=refresh_token" in body
            assert "refresh_token=refresh-value" in body
            expected = base64.b64encode(b"client-id:").decode()
            assert request.headers["Authorization"] == f"Basic {expected}"
            return httpx.Response(
                200,
                json={"access_token": "fresh-token", "expires_in": 3600},
                request=request,
            )
        listing_requests += 1
        assert request.headers["Authorization"] == "bearer fresh-token"
        return listing(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            client_id="client-id",
            client_secret="",
            refresh_token="refresh-value",
            user_agent="signalkit-test",
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=1))
        await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert collector.auth_mode == "refresh_token"
    assert token_requests == 1
    assert listing_requests == 2


@pytest.mark.asyncio
async def test_client_credentials_mode_requests_app_only_token() -> None:
    token_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests
        if request.url.path == "/api/v1/access_token":
            token_requests += 1
            assert request.content.decode() == "grant_type=client_credentials"
            return httpx.Response(
                200,
                json={"access_token": "app-token", "expires_in": 3600},
                request=request,
            )
        assert request.headers["Authorization"] == "bearer app-token"
        return listing(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            client_id="client-id",
            client_secret="client-secret",
            user_agent="signalkit-test",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=1))

    assert collector.auth_mode == "client_credentials"
    assert result.primary_count == 1
    assert token_requests == 1


@pytest.mark.asyncio
async def test_api_401_refreshes_bearer_once_then_retries_request() -> None:
    listing_requests = 0
    token_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal listing_requests, token_requests
        if request.url.path == "/api/v1/access_token":
            token_requests += 1
            return httpx.Response(
                200,
                json={"access_token": "fresh-token", "expires_in": 3600},
                request=request,
            )

        listing_requests += 1
        if request.headers["Authorization"] == "bearer expired-token":
            return httpx.Response(401, json={"message": "Unauthorized"}, request=request)
        assert request.headers["Authorization"] == "bearer fresh-token"
        return listing(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            access_token="expired-token",
            refresh_token="refresh-value",
            client_id="client-id",
            client_secret="client-secret",
            user_agent="signalkit-test",
            client=client,
        )
        result = await collector.collect(context=CollectorContext(limit=1))

    assert collector.auth_mode == "access_token+refresh_token"
    assert result.primary_count == 1
    assert listing_requests == 2
    assert token_requests == 1


@pytest.mark.asyncio
async def test_static_token_401_without_reauthentication_credentials_is_not_retried() -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(401, json={"message": "Unauthorized"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            access_token="expired-token",
            user_agent="signalkit-test",
            client=client,
        )
        with pytest.raises(CollectorError) as caught:
            await collector.collect(context=CollectorContext(limit=1))

    assert caught.value.kind is CollectorErrorKind.AUTH
    assert caught.value.status_code == 401
    assert requests == 1


def test_registry_prefers_access_token_without_app_secret(monkeypatch) -> None:
    monkeypatch.setenv("REDDIT_ACCESS_TOKEN", "static-token")
    monkeypatch.setenv("REDDIT_USER_AGENT", "signalkit-test")
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_REFRESH_TOKEN", raising=False)

    collector = default_registry().create(
        SourceConfig(name="reddit-python", type="reddit", options={"subreddit": "python"})
    )

    assert isinstance(collector, RedditCollector)
    assert collector.auth_mode == "access_token"


def test_registry_refresh_token_requires_client_id(monkeypatch) -> None:
    monkeypatch.delenv("REDDIT_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("REDDIT_REFRESH_TOKEN", "refresh-value")
    monkeypatch.setenv("REDDIT_USER_AGENT", "signalkit-test")
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="REDDIT_CLIENT_ID"):
        default_registry().create(
            SourceConfig(name="reddit-python", type="reddit", options={"subreddit": "python"})
        )
