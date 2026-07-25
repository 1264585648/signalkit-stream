from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from signalkit_stream.delivery import (
    DeliveryContext,
    DeliveryDispatcher,
    DeliveryStatus,
    SinkError,
    SQLiteDeliveryStore,
    WebhookSink,
    delivery_idempotency_key,
)
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def event(content: str = "hello") -> SignalEvent:
    return SignalEvent(
        id="sig_delivery",
        source="test",
        source_instance="one",
        kind=SignalKind.POST,
        content=content,
        url="https://example.com/1",
        created_at=datetime(2026, 7, 25, 10, 0, tzinfo=UTC),
    )


class RecordingSink:
    def __init__(self, name: str, *, failures: int = 0, retryable: bool = True) -> None:
        self.name = name
        self.failures = failures
        self.retryable = retryable
        self.calls: list[tuple[SignalEvent, DeliveryContext]] = []

    async def send(self, item: SignalEvent, context: DeliveryContext) -> None:
        self.calls.append((item, context))
        if len(self.calls) <= self.failures:
            raise SinkError("downstream unavailable", retryable=self.retryable)


def seed(database, item: SignalEvent) -> None:
    with SQLiteSignalStore(database) as store:
        store.write_many([item])


@pytest.mark.asyncio
async def test_success_is_not_redelivered_until_event_changes(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed(database, event("v1"))
    sink = RecordingSink("analytics")
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)

    with SQLiteDeliveryStore(database) as store:
        dispatcher = DeliveryDispatcher(store, [sink], now=lambda: now)
        first = await dispatcher.run_once()
        second = await dispatcher.run_once()

        assert first[0].delivered == 1
        assert second[0].attempted == 0
        assert len(sink.calls) == 1
        record = store.get_record("analytics", "sig_delivery")
        assert record is not None
        assert record.status is DeliveryStatus.DELIVERED
        delivered_hash = record.delivered_hash

    seed(database, event("v2"))
    with SQLiteDeliveryStore(database) as store:
        dispatcher = DeliveryDispatcher(store, [sink], now=lambda: now)
        third = await dispatcher.run_once()
        record = store.get_record("analytics", "sig_delivery")

    assert third[0].delivered == 1
    assert len(sink.calls) == 2
    assert record is not None
    assert record.delivered_hash != delivered_hash


@pytest.mark.asyncio
async def test_failure_keeps_delivery_pending_until_backoff_then_retries(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed(database, event())
    sink = RecordingSink("webhook", failures=1)
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    current = [now]

    with SQLiteDeliveryStore(database) as store:
        dispatcher = DeliveryDispatcher(
            store,
            [sink],
            backoff_base=10,
            now=lambda: current[0],
        )
        first = await dispatcher.run_once()
        blocked = await dispatcher.run_once()
        current[0] = now + timedelta(seconds=10)
        retried = await dispatcher.run_once()
        record = store.get_record("webhook", "sig_delivery")

    assert first[0].failed == 1
    assert blocked[0].attempted == 0
    assert retried[0].delivered == 1
    assert record is not None and record.status is DeliveryStatus.DELIVERED
    assert record.attempts == 2
    assert sink.calls[0][1].idempotency_key == sink.calls[1][1].idempotency_key


@pytest.mark.asyncio
async def test_dead_letter_requires_explicit_replay(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed(database, event())
    sink = RecordingSink("broken", failures=10, retryable=False)
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)

    with SQLiteDeliveryStore(database) as store:
        dispatcher = DeliveryDispatcher(store, [sink], now=lambda: now)
        first = await dispatcher.run_once()
        blocked = await dispatcher.run_once()
        replayed = store.replay_dead_letters("broken")
        third = await dispatcher.run_once()
        record = store.get_record("broken", "sig_delivery")

    assert first[0].dead_lettered == 1
    assert blocked[0].attempted == 0
    assert replayed == 1
    assert third[0].dead_lettered == 1
    assert record is not None and record.status is DeliveryStatus.DEAD_LETTER


@pytest.mark.asyncio
async def test_fanout_tracks_each_sink_independently(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed(database, event())
    good = RecordingSink("good")
    bad = RecordingSink("bad", failures=1)
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)

    with SQLiteDeliveryStore(database) as store:
        dispatcher = DeliveryDispatcher(store, [good, bad], now=lambda: now)
        results = await dispatcher.run_once()
        good_record = store.get_record("good", "sig_delivery")
        bad_record = store.get_record("bad", "sig_delivery")

    assert results[0].delivered == 1
    assert results[1].failed == 1
    assert good_record is not None and good_record.status is DeliveryStatus.DELIVERED
    assert bad_record is not None and bad_record.status is DeliveryStatus.FAILED


@pytest.mark.asyncio
async def test_webhook_uses_stable_idempotency_header() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["key"] = request.headers["Idempotency-Key"]
        seen["event"] = request.headers["X-SignalKit-Event-Id"]
        return httpx.Response(202, request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sink = WebhookSink("https://example.com/hook", name="crm", client=client)
        item = event()
        key = delivery_idempotency_key("crm", item.id, item.fingerprint())
        await sink.send(item, DeliveryContext("crm", key, 1))

    assert seen == {"key": key, "event": item.id}


def test_idempotency_key_changes_with_sink_or_event_version() -> None:
    first = delivery_idempotency_key("a", "sig", "hash-1")
    assert first == delivery_idempotency_key("a", "sig", "hash-1")
    assert first != delivery_idempotency_key("b", "sig", "hash-1")
    assert first != delivery_idempotency_key("a", "sig", "hash-2")
