import asyncio
from datetime import UTC, datetime, timedelta
import logging
import sqlite3
import time

import pytest

from signalkit_stream.collectors.base import Collector, HTTPCollector
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


class RateLimitedCollector(HTTPCollector):
    """Collector that always answers with a rate-limit error, like a bare HTTP 429."""

    source = "limited"

    def __init__(self, instance: str, *, snapshot: RateLimitSnapshot | None = None) -> None:
        super().__init__()
        self.instance = instance
        self._rate_limit = snapshot

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        raise CollectorError(
            "HTTP 429 from upstream",
            kind=CollectorErrorKind.RATE_LIMIT,
            source_key=self.identity.key,
            retryable=True,
        )


def limited_registry(snapshot: RateLimitSnapshot | None) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(
        "limited",
        lambda config: RateLimitedCollector(config.name, snapshot=snapshot),
    )
    return registry


def limited_config(*, interval: float) -> StreamConfig:
    return StreamConfig(
        runtime=RuntimeConfig(
            concurrency=1,
            failure_threshold=5,
            circuit_cooldown=300,
            failure_backoff_base=5,
        ),
        sources=(SourceConfig("one", "limited", interval=interval, limit=1),),
    )


NOW = datetime(2026, 7, 25, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("snapshot", "expected"),
    [
        (None, 60.0),
        (RateLimitSnapshot(remaining=0, reset_at=NOW + timedelta(seconds=10)), 60.0),
        (RateLimitSnapshot(retry_after=3600), 3600.0),
    ],
    ids=["no-hint", "hint-below-interval", "hint-above-interval"],
)
@pytest.mark.asyncio
async def test_rate_limited_failure_never_polls_faster_than_the_interval(
    tmp_path, snapshot, expected
) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            limited_config(interval=60),
            store,
            registry=limited_registry(snapshot),
            now=lambda: NOW,
        )
        result = (await runtime.run_once())[0]

    assert result.status == "degraded"
    assert result.delay == expected


@pytest.mark.asyncio
async def test_rate_limit_hint_outlives_the_open_circuit(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            limited_config(interval=60),
            store,
            registry=limited_registry(RateLimitSnapshot(retry_after=3600)),
            now=lambda: NOW,
        )
        results = [(await runtime.run_once())[0] for _ in range(6)]

    assert [result.delay for result in results] == [3600.0] * 6
    assert [result.status for result in results[:4]] == ["degraded"] * 4
    assert results[4].status == "circuit_open"


@pytest.mark.asyncio
async def test_ordinary_failure_keeps_backoff_below_the_poll_interval(tmp_path) -> None:
    config = StreamConfig(
        runtime=RuntimeConfig(
            concurrency=1,
            failure_threshold=5,
            circuit_cooldown=300,
            failure_backoff_base=5,
        ),
        sources=(SourceConfig("one", "fake", interval=3600, limit=1),),
    )
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config, store, registry=registry_for(fail=True))
        delays = [(await runtime.run_once())[0].delay for _ in range(3)]

    assert delays == [5, 10, 20]


def sample_event(event_id: str) -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source="fake",
        source_instance="one",
        kind=SignalKind.POST,
        content="reconcile",
        url=f"https://example.com/{event_id}",
        created_at=NOW,
    )


@pytest.mark.parametrize(
    "keep_disabled",
    [False, True],
    ids=["removed-from-config", "disabled-in-config"],
)
def test_sink_dropped_from_config_stops_queueing_pending_deliveries(
    tmp_path, keep_disabled
) -> None:
    received: list[str] = []
    sink_registry = SinkRegistry()
    sink_registry.register("fake-sink", lambda config: FakeSink(config.name, received))
    database = tmp_path / "signals.db"
    source = SourceConfig("one", "fake", interval=60, limit=1)
    with_both = StreamConfig(
        sources=(source,),
        sinks=(SinkConfig("brain", "fake-sink"), SinkConfig("archive", "fake-sink")),
    )
    remaining_sinks: tuple[SinkConfig, ...] = (SinkConfig("brain", "fake-sink"),)
    if keep_disabled:
        remaining_sinks += (SinkConfig("archive", "fake-sink", enabled=False),)
    without_archive = StreamConfig(sources=(source,), sinks=remaining_sinks)

    with SQLiteSignalStore(database) as store:
        StreamRuntime(with_both, store, registry=registry_for(), sink_registry=sink_registry)
        store.write_many([sample_event("sig_before")])
        assert store.get_delivery("archive", "sig_before") is not None

    with SQLiteSignalStore(database) as store:
        StreamRuntime(without_archive, store, registry=registry_for(), sink_registry=sink_registry)
        store.write_many([sample_event("sig_after")])
        archive_after = store.get_delivery("archive", "sig_after")
        brain_after = store.get_delivery("brain", "sig_after")

    assert archive_after is None
    assert brain_after is not None
    assert brain_after.status == "pending"


async def immediate_sleep(delay: float) -> None:
    await asyncio.sleep(0)


class CountingCollector(FakeCollector):
    def __init__(self, instance: str, *, on_collect) -> None:
        super().__init__(instance)
        self._on_collect = on_collect

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        result = await super().collect(context=context, cursor=cursor)
        self._on_collect()
        return result


@pytest.mark.asyncio
async def test_source_loop_survives_unexpected_iteration_failure(
    tmp_path, monkeypatch, caplog
) -> None:
    stop = asyncio.Event()
    collected: list[int] = []

    def on_collect() -> None:
        collected.append(len(collected) + 1)
        if len(collected) >= 3:
            stop.set()

    registry = CollectorRegistry()
    registry.register(
        "counting",
        lambda config: CountingCollector(config.name, on_collect=on_collect),
    )
    config = StreamConfig(
        runtime=RuntimeConfig(concurrency=1),
        sources=(SourceConfig("one", "counting", interval=0.01, limit=1),),
    )

    def locked(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("database is locked")

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        monkeypatch.setattr(store, "upsert_source_health", locked)
        runtime = StreamRuntime(config, store, registry=registry, sleep=immediate_sleep)
        with caplog.at_level(logging.ERROR, logger="signalkit_stream.runtime"):
            await asyncio.wait_for(runtime.run_forever(stop), timeout=10)

    assert len(collected) >= 3
    assert "iteration failed unexpectedly" in caplog.text
    assert "database is locked" in caplog.text


@pytest.mark.asyncio
async def test_run_forever_reports_a_worker_that_ends_on_its_own(
    tmp_path, monkeypatch, caplog
) -> None:
    stop = asyncio.Event()

    async def ends_immediately(source, stop_event) -> None:
        return None

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config_for(), store, registry=registry_for())
        monkeypatch.setattr(runtime, "_source_loop", ends_immediately)
        with caplog.at_level(logging.ERROR, logger="signalkit_stream.runtime"):
            with pytest.raises(RuntimeError, match="ended unexpectedly"):
                await asyncio.wait_for(runtime.run_forever(stop), timeout=10)

    assert "ended on its own" in caplog.text
    assert stop.is_set() is True


@pytest.mark.asyncio
async def test_run_forever_reports_a_worker_that_dies_and_stops_its_siblings(
    tmp_path, monkeypatch, caplog
) -> None:
    stop = asyncio.Event()
    cancelled: list[str] = []

    async def loop_for(source, stop_event) -> None:
        if source.config.name == "boom":
            raise ZeroDivisionError("worker exploded")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(source.config.name)
            raise

    registry = CollectorRegistry()
    registry.register("fake", lambda config: FakeCollector(config.name))
    config = StreamConfig(
        runtime=RuntimeConfig(concurrency=2),
        sources=(
            SourceConfig("healthy", "fake", interval=60, limit=1),
            SourceConfig("boom", "fake", interval=60, limit=1),
        ),
    )
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config, store, registry=registry)
        monkeypatch.setattr(runtime, "_source_loop", loop_for)
        with caplog.at_level(logging.ERROR, logger="signalkit_stream.runtime"):
            with pytest.raises(ZeroDivisionError, match="worker exploded"):
                await asyncio.wait_for(runtime.run_forever(stop), timeout=10)

    assert cancelled == ["healthy"]
    assert stop.is_set() is True
    assert "died with an unhandled exception" in caplog.text


class ArrivalSink(Sink):
    def __init__(self, key: str, received: list[str], arrived: asyncio.Event) -> None:
        self.key = key
        self.received = received
        self.arrived = arrived

    async def send(self, event: SignalEvent) -> None:
        self.received.append(event.id)
        self.arrived.set()


@pytest.mark.asyncio
async def test_run_forever_assembles_source_and_delivery_workers(tmp_path) -> None:
    stop = asyncio.Event()
    arrived = asyncio.Event()
    received: list[str] = []

    async def compressed_sleep(delay: float) -> None:
        # Sub-second waits are the delivery poll interval and are compressed; a source
        # poll interval parks until the shutdown path cancels it.
        await asyncio.sleep(0.001 if delay < 1 else 3600)

    sink_registry = SinkRegistry()
    sink_registry.register(
        "fake-sink",
        lambda config: ArrivalSink(config.name, received, arrived),
    )
    config = StreamConfig(
        runtime=RuntimeConfig(concurrency=1, delivery_interval=0.01),
        sources=(SourceConfig("one", "fake", interval=30, limit=1),),
        sinks=(SinkConfig("brain", "fake-sink"),),
    )

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            config,
            store,
            registry=registry_for(),
            sink_registry=sink_registry,
            sleep=compressed_sleep,
        )
        runner = asyncio.create_task(runtime.run_forever(stop))
        await asyncio.wait_for(arrived.wait(), timeout=15)
        stop.set()
        await asyncio.wait_for(runner, timeout=15)
        delivery = store.get_delivery("brain", received[0])
        health = store.get_source_health("fake:one")

    assert delivery is not None
    assert delivery.status == "delivered"
    assert health is not None
    assert health.total_runs >= 1


@pytest.mark.asyncio
async def test_stop_is_observed_while_a_source_sleeps(tmp_path) -> None:
    stop = asyncio.Event()
    sleeping = asyncio.Event()
    requested: list[float] = []

    async def announcing_sleep(delay: float) -> None:
        requested.append(delay)
        sleeping.set()
        await asyncio.sleep(delay)

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(
            config_for(interval=600),
            store,
            registry=registry_for(),
            sleep=announcing_sleep,
        )
        runner = asyncio.create_task(runtime.run_forever(stop))
        await asyncio.wait_for(sleeping.wait(), timeout=15)
        stop.set()
        started = time.perf_counter()
        await asyncio.wait_for(runner, timeout=15)
        elapsed = time.perf_counter() - started
        health = store.get_source_health("fake:one")

    assert requested == [600]
    assert elapsed < 5
    assert health is not None
    assert health.total_runs == 1


class BlockingCollector(Collector):
    source = "blocking"

    def __init__(self, instance: str, started: asyncio.Event) -> None:
        self.instance = instance
        self.started = started

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        self.validate_cursor(cursor)
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")  # pragma: no cover


@pytest.mark.asyncio
async def test_collection_cancelled_at_shutdown_does_not_advance_checkpoint(tmp_path) -> None:
    stop = asyncio.Event()
    started = asyncio.Event()
    registry = CollectorRegistry()
    registry.register("blocking", lambda config: BlockingCollector(config.name, started))
    config = StreamConfig(
        runtime=RuntimeConfig(concurrency=1),
        sources=(SourceConfig("one", "blocking", interval=60, limit=1),),
    )

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config, store, registry=registry)
        runner = asyncio.create_task(runtime.run_forever(stop))
        await asyncio.wait_for(started.wait(), timeout=15)
        stop.set()
        await asyncio.wait_for(runner, timeout=15)

        assert store.get_checkpoint("blocking:one") is None
        assert store.count() == 0
        assert store.get_source_health("blocking:one") is None


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


class ClosingCollector(FakeCollector):
    """Collector that records how often its pooled HTTP client was released."""

    def __init__(self, instance: str, *, fail_close: bool = False) -> None:
        super().__init__(instance)
        self.closes = 0
        self.collects = 0
        self.fail_close = fail_close

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        self.collects += 1
        return await super().collect(context=context, cursor=cursor)

    async def aclose(self) -> None:
        self.closes += 1
        if self.fail_close:
            raise RuntimeError("close failed")


def closing_registry(collector: ClosingCollector) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register("fake", lambda config: collector)
    return registry


@pytest.mark.asyncio
async def test_run_forever_closes_collector_http_pools_on_shutdown(tmp_path) -> None:
    collector = ClosingCollector("one")
    config = config_for(interval=0.05)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config, store, registry=closing_registry(collector))
        stop = asyncio.Event()
        runner = asyncio.create_task(runtime.run_forever(stop))
        deadline = time.monotonic() + 15
        while collector.collects == 0:
            assert time.monotonic() < deadline, "collector never ran"
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(runner, timeout=15)

    assert collector.closes == 1


@pytest.mark.asyncio
async def test_aclose_releases_collectors_after_a_one_shot_run(tmp_path) -> None:
    collector = ClosingCollector("one")
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config_for(), store, registry=closing_registry(collector))
        await runtime.run_once()
        assert collector.closes == 0

        await runtime.aclose()
        assert collector.closes == 1

        await runtime.aclose()
        assert collector.closes == 2, "aclose must stay safe to call more than once"


@pytest.mark.asyncio
async def test_collector_close_failure_is_logged_without_masking_shutdown(
    tmp_path, caplog
) -> None:
    collector = ClosingCollector("one", fail_close=True)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config_for(), store, registry=closing_registry(collector))
        with caplog.at_level(logging.WARNING, logger="signalkit_stream.runtime"):
            await runtime.aclose()

    assert collector.closes == 1
    assert "collector close failed" in caplog.text
