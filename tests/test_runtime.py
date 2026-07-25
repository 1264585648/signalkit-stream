from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from signalkit_stream.collectors.base import Collector
from signalkit_stream.config import RuntimeSettings, SourceConfig, StreamConfig
from signalkit_stream.health import SQLiteRuntimeStateStore
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
    RateLimitSnapshot,
)
from signalkit_stream.registry import CollectorRegistry
from signalkit_stream.runtime import StreamRuntime
from signalkit_stream.storage import SQLiteSignalStore


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.value = now
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += timedelta(seconds=seconds)

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


class FakeCollector(Collector):
    source = "fake"

    def __init__(
        self,
        instance: str,
        *,
        error: CollectorError | None = None,
        rate_limit: RateLimitSnapshot | None = None,
        received_cursors: list[int] | None = None,
    ) -> None:
        self.instance = instance
        self.error = error
        self.rate_limit_snapshot = rate_limit
        self.received_cursors = received_cursors
        self.calls = 0

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        self.calls += 1
        if self.error is not None:
            raise self.error
        current = int(cursor.state.get("n", 0)) if cursor else 0
        if self.received_cursors is not None:
            self.received_cursors.append(current)
        next_cursor = Cursor(source_key=self.identity.key, state={"n": current + 1})
        return CollectorResult(
            events=[],
            cursor=next_cursor,
            has_more=False,
            primary_count=1,
            rate_limit=self.rate_limit_snapshot,
        )


def make_config(*sources: SourceConfig, **runtime: object) -> StreamConfig:
    return StreamConfig(runtime=RuntimeSettings(**runtime), sources=tuple(sources))


@pytest.mark.asyncio
async def test_failing_source_does_not_stop_healthy_source(tmp_path) -> None:
    registry = CollectorRegistry()
    healthy = FakeCollector("healthy")
    failing = FakeCollector(
        "failing",
        error=CollectorError(
            "boom",
            kind=CollectorErrorKind.NETWORK,
            retryable=True,
        ),
    )
    registry.register("healthy", lambda config: healthy)
    registry.register("failing", lambda config: failing)
    config = make_config(
        SourceConfig(name="healthy", type="healthy", interval=10),
        SourceConfig(name="failing", type="failing", interval=10),
    )
    database = tmp_path / "signals.db"

    with SQLiteSignalStore(database) as event_store, SQLiteRuntimeStateStore(database) as state_store:
        runtime = StreamRuntime(
            config,
            event_store=event_store,
            state_store=state_store,
            registry=registry,
            clock=FakeClock(datetime(2026, 7, 25, tzinfo=UTC)),
        )
        outcomes = await runtime.run_once()

    by_name = {outcome.name: outcome for outcome in outcomes}
    assert by_name["healthy"].ok is True
    assert by_name["failing"].error == "boom"
    assert healthy.calls == 1
    assert failing.calls == 1


@pytest.mark.asyncio
async def test_rate_limited_source_is_persistently_paused_across_restart(tmp_path) -> None:
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    reset = now + timedelta(minutes=2)
    clock = FakeClock(now)
    database = tmp_path / "signals.db"
    config = make_config(SourceConfig(name="limited", type="limited", interval=10))
    collectors: list[FakeCollector] = []
    registry = CollectorRegistry()

    def factory(config: SourceConfig) -> Collector:
        collector = FakeCollector(
            config.name,
            rate_limit=RateLimitSnapshot(remaining=0, reset_at=reset),
        )
        collectors.append(collector)
        return collector

    registry.register("limited", factory)

    with SQLiteSignalStore(database) as event_store, SQLiteRuntimeStateStore(database) as state_store:
        first_runtime = StreamRuntime(
            config,
            event_store=event_store,
            state_store=state_store,
            registry=registry,
            clock=clock,
        )
        first = await first_runtime.run_source("limited")
        assert first.next_delay == 120

    with SQLiteSignalStore(database) as event_store, SQLiteRuntimeStateStore(database) as state_store:
        second_runtime = StreamRuntime(
            config,
            event_store=event_store,
            state_store=state_store,
            registry=registry,
            clock=clock,
        )
        second = await second_runtime.run_source("limited")

    assert second.skipped is True
    assert second.next_delay == 120
    assert collectors[0].calls == 1
    assert collectors[1].calls == 0


@pytest.mark.asyncio
async def test_circuit_breaker_uses_threshold_and_cooldown(tmp_path) -> None:
    clock = FakeClock(datetime(2026, 7, 25, tzinfo=UTC))
    collector = FakeCollector(
        "unstable",
        error=CollectorError(
            "temporary",
            kind=CollectorErrorKind.NETWORK,
            retryable=True,
        ),
    )
    registry = CollectorRegistry()
    registry.register("unstable", lambda config: collector)
    config = make_config(
        SourceConfig(name="unstable", type="unstable", interval=10),
        failure_threshold=2,
        cooldown=300,
    )
    database = tmp_path / "signals.db"

    with SQLiteSignalStore(database) as event_store, SQLiteRuntimeStateStore(database) as state_store:
        runtime = StreamRuntime(
            config,
            event_store=event_store,
            state_store=state_store,
            registry=registry,
            clock=clock,
        )
        first = await runtime.run_source("unstable")
        assert first.next_delay == 10
        clock.advance(10)
        second = await runtime.run_source("unstable")
        assert second.next_delay == 300
        third = await runtime.run_source("unstable")
        health = state_store.get_health("fake:unstable")

    assert third.skipped is True
    assert collector.calls == 2
    assert health is not None and health.consecutive_failures == 2


@pytest.mark.asyncio
async def test_runtime_restart_resumes_persisted_collector_checkpoint(tmp_path) -> None:
    database = tmp_path / "signals.db"
    received: list[int] = []
    registry = CollectorRegistry()
    registry.register(
        "cursor",
        lambda config: FakeCollector(config.name, received_cursors=received),
    )
    config = make_config(SourceConfig(name="resume", type="cursor", interval=10))
    clock = FakeClock(datetime(2026, 7, 25, tzinfo=UTC))

    with SQLiteSignalStore(database) as event_store, SQLiteRuntimeStateStore(database) as state_store:
        runtime = StreamRuntime(
            config,
            event_store=event_store,
            state_store=state_store,
            registry=registry,
            clock=clock,
        )
        await runtime.run_source("resume")

    clock.advance(10)
    with SQLiteSignalStore(database) as event_store, SQLiteRuntimeStateStore(database) as state_store:
        runtime = StreamRuntime(
            config,
            event_store=event_store,
            state_store=state_store,
            registry=registry,
            clock=clock,
        )
        await runtime.run_source("resume")

    assert received == [0, 1]
