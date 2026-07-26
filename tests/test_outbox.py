from datetime import UTC, datetime, timedelta

from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def make_event(content: str = "hello", *, collected_hour: int = 10) -> SignalEvent:
    return SignalEvent(
        id="sig_outbox",
        source="test",
        kind=SignalKind.POST,
        content=content,
        url="https://example.com/1",
        created_at=datetime(2026, 7, 25, 9, 0, tzinfo=UTC),
        collected_at=datetime(2026, 7, 25, collected_hour, 0, tzinfo=UTC),
    )


def test_outbox_tracks_only_new_or_changed_events(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        assert store.register_delivery_sink("brain") is True
        assert store.register_delivery_sink("brain") is False

        store.write_many([make_event()])
        pending = store.get_delivery("brain", "sig_outbox")
        assert pending is not None
        assert pending.status == "pending"
        assert pending.attempts == 0

        store.mark_delivery_success(
            "brain",
            "sig_outbox",
            delivered_at=datetime(2026, 7, 25, 11, 0, tzinfo=UTC),
        )
        store.write_many([make_event(collected_hour=12)])
        assert store.get_delivery("brain", "sig_outbox").status == "delivered"

        store.write_many([make_event("changed", collected_hour=13)])
        reset = store.get_delivery("brain", "sig_outbox")
        assert reset.status == "pending"
        assert reset.attempts == 0
        assert reset.delivered_at is None


def test_delivery_ready_failure_dead_and_replay(tmp_path) -> None:
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([make_event()])
        store.mark_delivery_failure(
            "brain",
            "sig_outbox",
            error="temporary",
            next_attempt_at=now + timedelta(seconds=30),
            attempted_at=now,
        )

        assert store.list_ready_deliveries("brain", now=now) == []
        assert len(store.list_ready_deliveries("brain", now=now + timedelta(seconds=31))) == 1

        store.mark_delivery_failure(
            "brain",
            "sig_outbox",
            error="permanent",
            next_attempt_at=None,
            dead=True,
            attempted_at=now + timedelta(seconds=31),
        )
        assert store.delivery_counts("brain") == {"dead": 1}
        assert store.retry_dead_deliveries("brain") == 1
        assert store.delivery_counts("brain") == {"pending": 1}


def other_event(event_id: str, content: str = "hello") -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source="test",
        kind=SignalKind.POST,
        content=content,
        url=f"https://example.com/{event_id}",
        created_at=datetime(2026, 7, 25, 9, 0, tzinfo=UTC),
        collected_at=datetime(2026, 7, 25, 10, 0, tzinfo=UTC),
    )


def test_batched_upsert_fires_outbox_triggers_exactly_once_per_transition(tmp_path) -> None:
    """The upsert must reproduce the insert / content-change / no-op trigger contract."""

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")

        # 1. insert -> AFTER INSERT trigger creates a pending outbox row per event.
        store.write_many([other_event("sig_a"), other_event("sig_b")])
        assert store.delivery_counts("brain") == {"pending": 2}

        store.mark_delivery_success("brain", "sig_a")
        store.mark_delivery_success("brain", "sig_b")
        assert store.delivery_counts("brain") == {"delivered": 2}

        # 2. no-op re-write of both rows must not touch the outbox at all.
        result = store.write_many([other_event("sig_a"), other_event("sig_b")])
        assert (result.inserted, result.updated, result.unchanged) == (0, 0, 2)
        assert store.delivery_counts("brain") == {"delivered": 2}

        # 3. one content change inside a mixed batch resets only that event.
        mixed = store.write_many(
            [
                other_event("sig_a"),  # unchanged
                other_event("sig_b", "changed"),  # content change
                other_event("sig_c"),  # brand new
            ]
        )
        assert (mixed.inserted, mixed.updated, mixed.unchanged) == (1, 1, 1)
        assert store.delivery_counts("brain") == {"delivered": 1, "pending": 2}
        assert store.get_delivery("brain", "sig_a").status == "delivered"
        reset = store.get_delivery("brain", "sig_b")
        assert reset.status == "pending"
        assert reset.attempts == 0
        assert reset.delivered_at is None
        assert store.get_delivery("brain", "sig_c").status == "pending"


def test_register_sink_can_backfill_existing_events(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.write_many([make_event()])
        store.register_delivery_sink("archive", backfill=True)

        delivery = store.get_delivery("archive", "sig_outbox")
        assert delivery is not None
        assert delivery.status == "pending"

        store.disable_delivery_sink("archive")
        store.write_many(
            [
                SignalEvent(
                    id="sig_after_disable",
                    source="test",
                    kind=SignalKind.POST,
                    content="later",
                    url="https://example.com/2",
                    created_at=datetime(2026, 7, 25, tzinfo=UTC),
                )
            ]
        )
        assert store.get_delivery("archive", "sig_after_disable") is None
