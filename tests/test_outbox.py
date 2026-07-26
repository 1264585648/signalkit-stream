from datetime import UTC, datetime, timedelta, timezone

from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore

PLUS_EIGHT = timezone(timedelta(hours=8))


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


def test_non_utc_delivery_timestamps_are_normalized_before_comparison(tmp_path) -> None:
    """A ``+08:00`` retry time used to sort by wall clock, stalling delivery for 8 hours."""

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([make_event()])
        store.mark_delivery_failure(
            "brain",
            "sig_outbox",
            error="temporary",
            next_attempt_at=datetime(2026, 7, 26, 12, 0, tzinfo=PLUS_EIGHT),  # 04:00Z
            attempted_at=datetime(2026, 7, 26, 11, 0, tzinfo=PLUS_EIGHT),  # 03:00Z
        )

        stored = store._connection.execute(
            "SELECT next_attempt_at, updated_at FROM deliveries"
        ).fetchone()
        assert stored["next_attempt_at"] == "2026-07-26T04:00:00+00:00"
        assert stored["updated_at"] == "2026-07-26T03:00:00+00:00"

        assert store.list_ready_deliveries("brain", now=datetime(2026, 7, 26, 3, 59, tzinfo=UTC)) == []
        ready = store.list_ready_deliveries("brain", now=datetime(2026, 7, 26, 4, 1, tzinfo=UTC))
        assert len(ready) == 1
        assert ready[0].next_attempt_at == datetime(2026, 7, 26, 4, 0, tzinfo=UTC)
        assert ready[0].updated_at == datetime(2026, 7, 26, 3, 0, tzinfo=UTC)

        # A caller-supplied non-UTC "now" is normalized on the comparison side too.
        assert store.list_ready_deliveries(
            "brain", now=datetime(2026, 7, 26, 11, 59, tzinfo=PLUS_EIGHT)
        ) == []
        assert len(
            store.list_ready_deliveries("brain", now=datetime(2026, 7, 26, 12, 1, tzinfo=PLUS_EIGHT))
        ) == 1

        store.mark_delivery_success(
            "brain",
            "sig_outbox",
            delivered_at=datetime(2026, 7, 26, 20, 0, tzinfo=PLUS_EIGHT),  # 12:00Z
        )
        delivered = store._connection.execute(
            "SELECT delivered_at, updated_at FROM deliveries"
        ).fetchone()
        assert delivered["delivered_at"] == "2026-07-26T12:00:00+00:00"
        assert delivered["updated_at"] == "2026-07-26T12:00:00+00:00"


def test_ready_deliveries_are_fifo_by_instant_not_by_wall_clock(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([other_event("sig_early"), other_event("sig_late")])

        # sig_early is genuinely older (15:00Z) but its raw string sorts last.
        store.mark_delivery_failure(
            "brain",
            "sig_early",
            error="boom",
            next_attempt_at=None,
            attempted_at=datetime(2026, 7, 26, 23, 0, tzinfo=PLUS_EIGHT),  # 15:00Z
        )
        store.mark_delivery_failure(
            "brain",
            "sig_late",
            error="boom",
            next_attempt_at=None,
            attempted_at=datetime(2026, 7, 26, 16, 0, tzinfo=UTC),  # 16:00Z
        )

        order = [
            record.event_id
            for record in store.list_ready_deliveries(
                "brain", now=datetime(2026, 7, 27, tzinfo=UTC)
            )
        ]
        assert order == ["sig_early", "sig_late"]


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
