import asyncio
from datetime import UTC, datetime

import pytest

from signalkit_stream.collectors.base import Collector
from signalkit_stream.config import RuntimeConfig, SourceConfig, StreamConfig
from signalkit_stream.delivery import DeliveryEngine
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)
from signalkit_stream.registry import CollectorRegistry
from signalkit_stream.runtime import StreamRuntime
from signalkit_stream.sinks import Sink, SinkError
from signalkit_stream.storage import SQLiteSignalStore


def event(content: str = "v1") -> SignalEvent:
    return SignalEvent(
        id="sig_fault",
        source="test",
        source_instance="source",
        kind=SignalKind.POST,
        content=content,
        url="https://example.com/fault",
        created_at=datetime(2026, 7, 25, 10, 0, tzinfo=UTC),
    )


def test_collection_transaction_rolls_back_event_checkpoint_and_outbox(tmp_path, monkeypatch) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("brain")

        def fail_checkpoint(*args, **kwargs) -> None:
            raise RuntimeError("crash before commit")

        monkeypatch.setattr(store, "_set_checkpoint", fail_checkpoint)
        with pytest.raises(RuntimeError, match="crash before commit"):
            store.commit_batch(
                [event()],
                source_key="test:source",
                cursor=Cursor("test:source", {"position": 1}),
            )

        assert store.exists("sig_fault") is False
        assert store.get_checkpoint("test:source") is None
        assert store.get_delivery("brain", "sig_fault") is None


def test_collection_commit_survives_restart_with_checkpoint_and_outbox(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("brain")
        result = store.commit_batch(
            [event()],
            source_key="test:source",
            cursor=Cursor("test:source", {"position": 1}),
        )
        assert result.inserted == 1

    with SQLiteSignalStore(database) as store:
        assert store.get("sig_fault") is not None
        checkpoint = store.get_checkpoint("test:source")
        delivery = store.get_delivery("brain", "sig_fault")

    assert checkpoint is not None
    assert checkpoint.cursor.state == {"position": 1}
    assert delivery is not None
    assert delivery.status == "pending"
    assert delivery.attempts == 0


class CancelAfterSideEffectSink(Sink):
    def __init__(self, key: str, calls: list[str]) -> None:
        self.key = key
        self.calls = calls

    async def send(self, item: SignalEvent) -> None:
        self.calls.append(item.id)
        raise asyncio.CancelledError


class RecordingSink(Sink):
    def __init__(self, key: str, calls: list[str]) -> None:
        self.key = key
        self.calls = calls

    async def send(self, item: SignalEvent) -> None:
        self.calls.append(item.id)


class PermanentFailureSink(Sink):
    def __init__(self, key: str) -> None:
        self.key = key

    async def send(self, item: SignalEvent) -> None:
        raise SinkError("permanent failure", retryable=False, status_code=400)


@pytest.mark.asyncio
async def test_delivery_cancel_after_remote_side_effect_replays_after_restart(tmp_path) -> None:
    database = tmp_path / "signals.db"
    external_calls: list[str] = []
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("brain")
        store.write_many([event()])
        engine = DeliveryEngine(store, (CancelAfterSideEffectSink("brain", external_calls),))
        with pytest.raises(asyncio.CancelledError):
            await engine.deliver_once(engine.sinks[0])
        delivery = store.get_delivery("brain", "sig_fault")
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.attempts == 0

    with SQLiteSignalStore(database) as store:
        sink = RecordingSink("brain", external_calls)
        engine = DeliveryEngine(store, (sink,))
        result = await engine.deliver_once(sink)
        delivery = store.get_delivery("brain", "sig_fault")

    assert external_calls == ["sig_fault", "sig_fault"]
    assert result.delivered == 1
    assert delivery is not None
    assert delivery.status == "delivered"


@pytest.mark.asyncio
async def test_multi_sink_partial_failure_is_isolated(tmp_path) -> None:
    database = tmp_path / "signals.db"
    calls: list[str] = []
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("archive")
        store.register_delivery_sink("brain")
        store.write_many([event()])
        archive = RecordingSink("archive", calls)
        brain = PermanentFailureSink("brain")
        engine = DeliveryEngine(store, (archive, brain))

        results = await engine.deliver_all_once()
        archive_delivery = store.get_delivery("archive", "sig_fault")
        brain_delivery = store.get_delivery("brain", "sig_fault")

    assert calls == ["sig_fault"]
    assert {result.sink_key for result in results} == {"archive", "brain"}
    assert archive_delivery is not None and archive_delivery.status == "delivered"
    assert brain_delivery is not None and brain_delivery.status == "dead"


class MutatingSink(Sink):
    def __init__(self, key: str, store: SQLiteSignalStore) -> None:
        self.key = key
        self.store = store

    async def send(self, item: SignalEvent) -> None:
        self.store.write_many([event("v2")])


@pytest.mark.asyncio
async def test_inflight_old_version_cannot_ack_newer_pending_version(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("brain")
        store.write_many([event("v1")])
        sink = MutatingSink("brain", store)
        engine = DeliveryEngine(store, (sink,))

        result = await engine.deliver_once(sink)
        delivery = store.get_delivery("brain", "sig_fault")
        current = store.get("sig_fault")

    assert result.delivered == 0
    assert result.superseded == 1
    assert delivery is not None
    assert delivery.status == "pending"
    assert delivery.attempts == 0
    assert current is not None and current.content == "v2"


class RuntimeCollector(Collector):
    source = "runtime-test"

    def __init__(self, instance: str, *, fail: bool) -> None:
        self.instance = instance
        self.fail = fail

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        if self.fail:
            raise CollectorError(
                "source unavailable",
                kind=CollectorErrorKind.NETWORK,
                source_key=self.identity.key,
                retryable=True,
            )
        item = SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                "1",
                SignalKind.POST,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.POST,
            content="healthy source",
            url=f"https://example.com/{self.instance}",
            created_at=datetime(2026, 7, 25, tzinfo=UTC),
        )
        return CollectorResult(
            events=[item][: ctx.limit],
            cursor=Cursor(self.identity.key, {"done": True}),
            primary_count=1,
        )


@pytest.mark.asyncio
async def test_runtime_source_failure_does_not_block_healthy_source(tmp_path) -> None:
    registry = CollectorRegistry()
    registry.register(
        "runtime-test",
        lambda config: RuntimeCollector(config.name, fail=config.name == "bad"),
    )
    config = StreamConfig(
        runtime=RuntimeConfig(concurrency=2, failure_threshold=3),
        sources=(
            SourceConfig("good", "runtime-test", interval=60, limit=1),
            SourceConfig("bad", "runtime-test", interval=60, limit=1),
        ),
    )

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        runtime = StreamRuntime(config, store, registry=registry)
        results = await runtime.run_once()
        health = {item.source_key: item for item in store.list_source_health()}

        assert store.count() == 1

    by_name = {result.name: result for result in results}
    assert by_name["good"].success is True
    assert by_name["bad"].success is False
    assert health["runtime-test:good"].status == "healthy"
    assert health["runtime-test:bad"].status == "degraded"
