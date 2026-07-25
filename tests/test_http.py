import httpx
import pytest

from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.protocol import CollectorContext, CollectorResult


class StubHTTPCollector(HTTPCollector):
    source = "test-http"
    instance = "default"

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        raise NotImplementedError


@pytest.mark.asyncio
async def test_http_retries_429_and_honors_retry_after() -> None:
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = StubHTTPCollector(
            client=client,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0, jitter_ratio=0),
            sleep=fake_sleep,
        )
        response = await collector.request(
            client,
            "GET",
            "https://example.com/test",
            context=CollectorContext(limit=1),
        )

    assert response.status_code == 200
    assert attempts == 2
    assert sleeps == [0.0]


@pytest.mark.asyncio
async def test_http_retries_500_then_succeeds() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(500 if attempts < 3 else 200, request=request)

    async def no_sleep(delay: float) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = StubHTTPCollector(
            client=client,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0, jitter_ratio=0),
            sleep=no_sleep,
        )
        response = await collector.request(client, "GET", "https://example.com/test")

    assert response.status_code == 200
    assert attempts == 3
