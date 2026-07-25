import httpx
import pytest

from signalkit_stream.collectors.reddit import RedditCollector, RedditOAuth
from signalkit_stream.protocol import CollectorContext


@pytest.mark.asyncio
async def test_reddit_401_refreshes_static_token_once_and_reuses_refreshed_token() -> None:
    token_requests = 0
    listing_tokens: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests
        if request.url.path == "/api/v1/access_token":
            token_requests += 1
            return httpx.Response(
                200,
                json={"access_token": "refreshed", "expires_in": 3600},
                request=request,
            )

        token = request.headers.get("Authorization", "")
        listing_tokens.append(token)
        if token == "Bearer expired":
            return httpx.Response(401, request=request)
        return httpx.Response(
            200,
            json={"kind": "Listing", "data": {"children": [], "after": None}},
            request=request,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = RedditCollector(
            "python",
            oauth=RedditOAuth(
                access_token="expired",
                client_id="client-id",
                client_secret="secret",
                refresh_token="refresh-token",
            ),
            user_agent="linux:signalkit-stream:test (by /u/example)",
            client=client,
        )
        first = await collector.collect(context=CollectorContext(limit=1))
        await collector.collect(context=CollectorContext(limit=1), cursor=first.cursor)

    assert token_requests == 1
    assert listing_tokens == ["Bearer expired", "Bearer refreshed", "Bearer refreshed"]
