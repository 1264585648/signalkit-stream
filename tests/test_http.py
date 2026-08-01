import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
)


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


@pytest.mark.asyncio
async def test_http_bounds_parallel_requests_per_collector() -> None:
    active = 0
    max_active = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = StubHTTPCollector(client=client, max_concurrency=2)
        await asyncio.gather(
            *(
                collector.request(client, "GET", f"https://example.com/{index}")
                for index in range(6)
            )
        )

    assert max_active == 2


@pytest.mark.asyncio
async def test_standard_rate_limit_reset_is_relative_delay() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "RateLimit-Limit": "100",
                "RateLimit-Remaining": "0",
                "RateLimit-Reset": "30",
            },
            request=request,
        )

    before = datetime.now(UTC)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = StubHTTPCollector(client=client)
        await collector.request(client, "GET", "https://example.com/test")

    assert collector.rate_limit is not None
    assert collector.rate_limit.limit == 100
    assert collector.rate_limit.remaining == 0
    assert collector.rate_limit.retry_after == 30
    assert collector.rate_limit.reset_at is not None
    assert 29 <= (collector.rate_limit.reset_at - before).total_seconds() <= 31


@pytest.mark.asyncio
async def test_http_does_not_sleep_past_collector_deadline() -> None:
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    context = CollectorContext(
        limit=1,
        deadline=datetime.now(UTC) + timedelta(milliseconds=50),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = StubHTTPCollector(
            client=client,
            retry_policy=RetryPolicy(
                max_attempts=3,
                base_delay=1,
                max_delay=1,
                jitter_ratio=0,
            ),
            sleep=fake_sleep,
        )
        with pytest.raises(CollectorError) as caught:
            await collector.request(
                client,
                "GET",
                "https://example.com/test",
                context=context,
            )

    assert caught.value.kind is CollectorErrorKind.TIMEOUT
    assert sleeps == []
