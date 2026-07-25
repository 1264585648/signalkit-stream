import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from signalkit_stream.delivery import DeliveryEngine
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.sinks import Sink, SinkError
from signalkit_stream.storage import SQLiteSignalStore


class RecordingSink(Sink):
    def __init__(self, key: str, failures: list[Exception] | None = None) -> None:
        self.key = key
        self.failures = list(failures or [])
        self.events: list[str] = []

    async def send(self, event: SignalEvent) -> None:
        if self.failures:
            raise self.failures.pop(0)
        self.events.append(event.id)


def event(event_id: str = "sig_delivery") -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source="test",
        kind=SignalKind.POST,
        content="hello",
        url=f"https://example.com/{event_id}",
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_delivery_success_marks_outbox(tmp_path) -> None:
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    sink = RecordingSink("brain")
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event()])
        engine = DeliveryEngine(store, (sink,), now=lambda: now)
        result = await engine.deliver_once(sink)
        record = store.get_delivery("brain", "sig_delivery")

    assert result.attempted == 1
    assert result.delivered == 1
    assert sink.events == ["sig_delivery"]
    assert record.status == "delivered"
    assert record.attempts == 1


@pytest.mark.asyncio
async def test_delivery_retries_then_dead_letters(tmp_path) -> None:
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    clock = [now]
    sink = RecordingSink(
        "brain",
        failures=[
            SinkError("limited", retryable=True, retry_after=10),
            SinkError("bad request", retryable=False, status_code=400),
        ],
    )
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event()])
        engine = DeliveryEngine(
            store,
            (sink,),
            max_attempts=3,
            backoff_base=5,
            now=lambda: clock[0],
        )

        first = await engine.deliver_once(sink)
        record = store.get_delivery("brain", "sig_delivery")
        assert first.failed == 1
        assert record.status == "failed"
        assert record.next_attempt_at == now + timedelta(seconds=10)

        clock[0] = now + timedelta(seconds=11)
        second = await engine.deliver_once(sink)
        record = store.get_delivery("brain", "sig_delivery")

    assert second.dead == 1
    assert record.status == "dead"
    assert record.attempts == 2


@pytest.mark.asyncio
async def test_delivery_forever_stops_cleanly(tmp_path) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    sink = RecordingSink("brain")

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        stop.set()
        await asyncio.sleep(0)

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        engine = DeliveryEngine(store, (sink,), interval=3, sleep=fake_sleep)
        await asyncio.wait_for(engine.run_forever(stop), timeout=1)

    assert sleeps == [3]
