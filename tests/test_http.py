import httpx
import pytest

from signalkit_stream.collectors import _base_impl
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


@pytest.mark.asyncio
async def test_http_client_is_created_once_and_reused_across_polls(monkeypatch) -> None:
    """C1: a fresh AsyncClient per request costs ~750 ms and blocks the loop."""

    constructed = 0
    real_init = httpx.AsyncClient.__init__

    def counting_init(self, *args, **kwargs):
        nonlocal constructed
        constructed += 1
        real_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", counting_init)

    collector = StubHTTPCollector()
    seen: list[httpx.AsyncClient] = []
    for _ in range(3):
        async with collector.http_client() as client:
            seen.append(client)

    assert constructed == 1
    assert seen[0] is seen[1] is seen[2]
    assert not seen[0].is_closed
    await collector.aclose()
    assert seen[0].is_closed


@pytest.mark.asyncio
async def test_http_client_is_not_closed_when_the_context_manager_exits() -> None:
    collector = StubHTTPCollector()
    async with collector.http_client() as client:
        pass
    assert not client.is_closed
    await collector.aclose()
    assert client.is_closed


@pytest.mark.asyncio
async def test_aclose_leaves_an_injected_client_open() -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))) as injected:
        collector = StubHTTPCollector(client=injected)
        async with collector.http_client() as client:
            assert client is injected
        await collector.aclose()
        assert not injected.is_closed


@pytest.mark.asyncio
async def test_aclose_is_idempotent_and_safe_without_a_client() -> None:
    collector = StubHTTPCollector()
    await collector.aclose()
    await collector.aclose()

    collector = StubHTTPCollector()
    async with collector.http_client():
        pass
    await collector.aclose()
    await collector.aclose()


@pytest.mark.asyncio
async def test_collector_base_exposes_a_noop_aclose() -> None:
    class PlainCollector(_base_impl.Collector):
        source = "plain"

        async def collect(self, *, context=None, cursor=None) -> CollectorResult:
            raise NotImplementedError

    await PlainCollector().aclose()
