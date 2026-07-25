import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from signalkit_stream.collectors.base import Collector
from signalkit_stream.config import RuntimeConfig, SinkConfig, SourceConfig, StreamConfig
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
    RateLimitSnapshot,
)
from signalkit_stream.registry import CollectorRegistry
from signalkit_stream.runtime import StreamRuntime
from signalkit_stream.sinks import Sink, SinkRegistry
from signalkit_stream.storage import SQLiteSignalStore


class FakeCollector(Collector):
    source = "fake"

    def __init__(self, instance: str, *, fail: bool = False, rate_limit=None) -> None:
        self.instance = instance
        self.fail = fail
        self.rate_limit_snapshot = rate_limit

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        context = self.context(context)
        self.validate_cursor(cursor)
        if self.fail:
            raise CollectorError(
                "boom",
                kind=CollectorErrorKind.NETWORK,
                source_key=self.identity.key,
                retryable=True,
            )
        position = int(cursor.state.get("position", 0)) if cursor else 0
        event = SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                str(position),
                SignalKind.POST,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.POST,
            content=f"event {position}",
            url=f"https://example.com/{self.instance}/{position}",
            created_at=datetime(2026, 7, 25, tzinfo=UTC),
        )
        return CollectorResult(
            events=[event][: context.limit],
            cursor=Cursor(self.identity.key, {"position": position + 1}),
            primary_count=1,
            rate_limit=self.rate_limit_snapshot,
        )


class FakeSink(Sink):
    def __init__(self, key: str, received: list[str]) -> None:
        self.key = key
        self.received = received

    async def send(self, event: SignalEvent) -> None:
        self.received.append(event.id)


def registry_for(*, fail: bool = False, rate_limit=None) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(
        "fake",
        lambda config: FakeCollector(config.name, fail=fail, rate_limit=rate_limit),
    )
    return registry


def config_for(*, threshold: int = 2, interval: float = 60, with_sink: bool = False) -> StreamConfig:
    return StreamConfig(
        runtime=RuntimeConfig(
            concurrency=2,
            failure_threshold=threshold,
            circuit_cooldown=300,
            failure_backoff_base=5,
        ),
        sources=(SourceConfig("one", "fake", interval=interval, limit=1),),
        sinks=(SinkConfig("brain", "fake-sink"),) if with_sink else (),
    )


@pytest.mark.asyncio
async def test_run_once_persists_events_health_and_resume(tmp_path) -> None:
    now = datetime(2026, 7, 25, 10, 0, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            config_for(),
            store,
            registry=registry_for(),
            now=lambda: now,
        )
        first = await runtime.run_once()
        second = await runtime.run_once()
        health = store.get_source_health("fake:one")

        assert first[0].success is True
        assert first[0].collection.inserted == 1
        assert second[0].collection.inserted == 1
        assert store.count() == 2
        assert health is not None
        assert health.status == "healthy"
        assert health.total_runs == 2
        assert health.total_events == 2
        assert health.consecutive_failures == 0


@pytest.mark.asyncio
async def test_run_once_collects_then_delivers_transactional_outbox(tmp_path) -> None:
    received: list[str] = []
    sink_registry = SinkRegistry()
    sink_registry.register("fake-sink", lambda config: FakeSink(config.name, received))

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            config_for(with_sink=True),
            store,
            registry=registry_for(),
            sink_registry=sink_registry,
        )
        results = await runtime.run_once()
        event_id = results[0].collection.events[0].id
        delivery = store.get_delivery("brain", event_id)

    assert received == [event_id]
    assert runtime.last_delivery_results[0].delivered == 1
    assert delivery.status == "delivered"


@pytest.mark.asyncio
async def test_failures_backoff_then_open_circuit(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config_for(threshold=2), store, registry=registry_for(fail=True))
        first = (await runtime.run_once())[0]
        second = (await runtime.run_once())[0]
        health = store.get_source_health("fake:one")

        assert first.success is False
        assert first.status == "degraded"
        assert first.delay == 5
        assert second.status == "circuit_open"
        assert second.delay == 300
        assert health is not None
        assert health.consecutive_failures == 2
        assert health.total_runs == 2


@pytest.mark.asyncio
async def test_success_waits_for_rate_limit_reset(tmp_path) -> None:
    now = datetime(2026, 7, 25, 10, 0, tzinfo=UTC)
    rate_limit = RateLimitSnapshot(remaining=0, reset_at=now + timedelta(seconds=120))
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            config_for(interval=60),
            store,
            registry=registry_for(rate_limit=rate_limit),
            now=lambda: now,
        )
        result = (await runtime.run_once())[0]

    assert result.delay == 120


@pytest.mark.asyncio
async def test_run_forever_stops_and_cancels_workers(tmp_path) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        stop.set()
        await asyncio.sleep(0)

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            config_for(interval=10),
            store,
            registry=registry_for(),
            sleep=fake_sleep,
        )
        await asyncio.wait_for(runtime.run_forever(stop), timeout=1)
        health = store.get_source_health("fake:one")

    assert sleeps == [10]
    assert health is not None
    assert health.total_runs == 1


def test_runtime_rejects_duplicate_source_identity(tmp_path) -> None:
    registry = CollectorRegistry()
    registry.register("fake", lambda config: FakeCollector("same"))
    config = StreamConfig(
        sources=(
            SourceConfig("a", "fake"),
            SourceConfig("b", "fake"),
        )
    )
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        with pytest.raises(ValueError, match="duplicate collector source identities"):
            StreamRuntime(config, store, registry=registry)
