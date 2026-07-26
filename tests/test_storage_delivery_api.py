"""Store methods the delivery-engine rework consumes (joined ready query, hash-only
lookup, batch outcome applier, optimistic concurrency)."""

from datetime import UTC, datetime, timedelta, timezone

from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import DeliveryOutcome, SQLiteSignalStore

PLUS_EIGHT = timezone(timedelta(hours=8))


def event(event_id: str, content: str = "hello", *, collected_hour: int = 10) -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source="test",
        kind=SignalKind.POST,
        content=content,
        url=f"https://example.com/{event_id}",
        created_at=datetime(2026, 7, 25, 9, 0, tzinfo=UTC),
        collected_at=datetime(2026, 7, 25, collected_hour, 0, tzinfo=UTC),
    )


def _ready_payload_plan(store: SQLiteSignalStore) -> str:
    """Explain the exact SQL ``list_ready_delivery_payloads`` runs, via a traced replay."""

    statements: list[str] = []
    store._connection.set_trace_callback(statements.append)
    try:
        store.list_ready_delivery_payloads("brain", limit=10, now=datetime(2026, 7, 26, tzinfo=UTC))
    finally:
        store._connection.set_trace_callback(None)

    # sqlite3 traces statements with their parameters already expanded.
    sql = next(text for text in statements if "JOIN signals" in text)
    rows = store._connection.execute("EXPLAIN QUERY PLAN " + sql).fetchall()
    return " | ".join(str(row["detail"]) for row in rows)


def test_joined_ready_query_returns_payloads_and_uses_the_ready_index(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event("sig_a", "body-a"), event("sig_b", "body-b")])

        plan_text = _ready_payload_plan(store)
        assert "idx_deliveries_ready" in plan_text
        # The signals side must be an id lookup, never a scan of the events table.
        assert "SCAN s" not in plan_text
        assert "SEARCH s USING INDEX sqlite_autoindex_signals_1 (id=?)" in plan_text

        ready = store.list_ready_delivery_payloads("brain", now=datetime(2026, 7, 26, tzinfo=UTC))

        assert [item.event_id for item in ready] == ["sig_a", "sig_b"]
        assert [item.event.content for item in ready] == ["body-a", "body-b"]
        assert [item.status for item in ready] == ["pending", "pending"]
        assert [item.attempts for item in ready] == [0, 0]
        assert ready[0].next_attempt_at is None
        # The joined row must expose the *delivery* updated_at, not the event's.
        assert ready[0].updated_at == store.get_delivery("brain", "sig_a").updated_at
        assert ready[0].event == store.get("sig_a")


def test_joined_ready_query_honours_status_retry_and_limit_filters(tmp_path) -> None:
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event(f"sig_{index}") for index in range(4)])

        store.mark_delivery_success("brain", "sig_0")
        store.mark_delivery_failure(
            "brain", "sig_1", error="later", next_attempt_at=now + timedelta(minutes=5)
        )
        store.mark_delivery_failure(
            "brain", "sig_2", error="due", next_attempt_at=now - timedelta(minutes=5)
        )

        ready = {item.event_id for item in store.list_ready_delivery_payloads("brain", now=now)}
        assert ready == {"sig_2", "sig_3"}

        assert len(store.list_ready_delivery_payloads("brain", limit=1, now=now)) == 1
        assert store.list_ready_delivery_payloads("brain", limit=0, now=now) == []
        assert store.list_ready_delivery_payloads("other", now=now) == []


def test_joined_ready_query_skips_rows_whose_event_is_gone(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event("sig_a")])
        with store._write_transaction():
            store._connection.execute("DELETE FROM signals WHERE id = 'sig_a'")

        assert store.list_ready_delivery_payloads("brain") == []
        # list_ready_deliveries still surfaces the orphan so the engine can tombstone it.
        assert [record.event_id for record in store.list_ready_deliveries("brain")] == ["sig_a"]


def test_get_event_hash_matches_the_stored_fingerprint(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        first = event("sig_a")
        store.write_many([first])

        assert store.get_event_hash("sig_a") == first.fingerprint()
        assert store.get_event_hash("sig_missing") is None

        changed = event("sig_a", "changed", collected_hour=11)
        store.write_many([changed])
        assert store.get_event_hash("sig_a") == changed.fingerprint()
        assert store.get_event_hash("sig_a") != first.fingerprint()


def test_apply_delivery_outcomes_applies_a_whole_batch_in_one_transaction(tmp_path) -> None:
    when = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    retry_at = datetime(2026, 7, 26, 12, 5, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event(f"sig_{index}") for index in range(3)])

        assert store.apply_delivery_outcomes([]) == 0

        applied = store.apply_delivery_outcomes(
            [
                DeliveryOutcome("delivered", 1, None, None, when, "brain", "sig_0", when),
                DeliveryOutcome("failed", 2, retry_at, "boom", when, "brain", "sig_1"),
                DeliveryOutcome("dead", 8, None, "fatal", when, "brain", "sig_2"),
                DeliveryOutcome("delivered", 1, None, None, when, "brain", "sig_missing"),
            ]
        )

        assert applied == 3
        assert store.delivery_counts("brain") == {"delivered": 1, "failed": 1, "dead": 1}

        delivered = store.get_delivery("brain", "sig_0")
        assert (delivered.status, delivered.attempts) == ("delivered", 1)
        assert delivered.delivered_at == when
        assert delivered.updated_at == when

        failed = store.get_delivery("brain", "sig_1")
        assert (failed.status, failed.attempts, failed.last_error) == ("failed", 2, "boom")
        assert failed.next_attempt_at == retry_at

        dead = store.get_delivery("brain", "sig_2")
        assert (dead.status, dead.attempts) == ("dead", 8)
        assert dead.next_attempt_at is None


def test_apply_delivery_outcomes_normalizes_offsets_and_respects_expectations(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event("sig_a"), event("sig_b")])
        original = store.get_delivery("brain", "sig_a").updated_at
        stale = original - timedelta(seconds=1)

        applied = store.apply_delivery_outcomes(
            [
                DeliveryOutcome(
                    "delivered",
                    1,
                    None,
                    None,
                    datetime(2026, 7, 26, 20, 0, tzinfo=PLUS_EIGHT),  # 12:00Z
                    "brain",
                    "sig_a",
                    datetime(2026, 7, 26, 20, 0, tzinfo=PLUS_EIGHT),
                    expected_updated_at=original,
                ),
                DeliveryOutcome(
                    "delivered",
                    1,
                    None,
                    None,
                    datetime(2026, 7, 26, 13, 0, tzinfo=UTC),
                    "brain",
                    "sig_b",
                    expected_updated_at=stale,
                ),
            ]
        )

        assert applied == 1
        row = store._connection.execute(
            "SELECT updated_at, delivered_at FROM deliveries WHERE event_id = 'sig_a'"
        ).fetchone()
        assert row["updated_at"] == "2026-07-26T12:00:00+00:00"
        assert row["delivered_at"] == "2026-07-26T12:00:00+00:00"
        assert store.get_delivery("brain", "sig_b").status == "pending"


def test_delivery_outcome_accepts_the_documented_tuple_order() -> None:
    when = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    row = ("failed", 3, when + timedelta(minutes=1), "boom", when, "brain", "sig_a")
    outcome = DeliveryOutcome(*row)

    assert outcome.status == "failed"
    assert outcome.attempts == 3
    assert outcome.next_attempt_at == when + timedelta(minutes=1)
    assert outcome.last_error == "boom"
    assert outcome.updated_at == when
    assert outcome.sink_key == "brain"
    assert outcome.event_id == "sig_a"
    assert outcome.delivered_at is None
    assert outcome.expected_updated_at is None


def test_mark_methods_report_whether_the_row_was_updated(tmp_path) -> None:
    when = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event("sig_a"), event("sig_b")])

        assert store.mark_delivery_success("brain", "sig_a", delivered_at=when) is True
        assert store.mark_delivery_success("brain", "sig_missing", delivered_at=when) is False
        assert (
            store.mark_delivery_failure(
                "brain", "sig_b", error="boom", next_attempt_at=None, attempted_at=when
            )
            is True
        )
        assert (
            store.mark_delivery_failure(
                "brain", "sig_missing", error="boom", next_attempt_at=None, attempted_at=when
            )
            is False
        )


def test_optimistic_failure_cannot_bury_a_newer_pending_state(tmp_path) -> None:
    """A late failure for an old payload must not mark a re-collected event dead."""

    when = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event("sig_a", "v1")])
        in_flight = store.list_ready_delivery_payloads("brain")[0]

        # While the send is in flight the collector re-collects a corrected version, and
        # the AFTER UPDATE OF event_hash trigger re-pends the row with a newer updated_at.
        store.write_many([event("sig_a", "v2-corrected", collected_hour=11)])
        assert store.get_delivery("brain", "sig_a").updated_at != in_flight.updated_at

        superseded = store.mark_delivery_failure(
            "brain",
            "sig_a",
            error="HTTP 400 for the old payload",
            next_attempt_at=None,
            dead=True,
            attempted_at=when,
            expected_updated_at=in_flight.updated_at,
        )

        assert superseded is False
        current = store.get_delivery("brain", "sig_a")
        assert current.status == "pending"
        assert current.attempts == 0
        assert current.last_error is None
        assert [item.event.content for item in store.list_ready_delivery_payloads("brain")] == [
            "v2-corrected"
        ]

        # Without the guard the very same call buries the newer pending state.
        assert (
            store.mark_delivery_failure(
                "brain",
                "sig_a",
                error="HTTP 400 for the old payload",
                next_attempt_at=None,
                dead=True,
                attempted_at=when,
            )
            is True
        )
        assert store.get_delivery("brain", "sig_a").status == "dead"


def test_optimistic_success_cannot_bury_a_newer_pending_state(tmp_path) -> None:
    when = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.register_delivery_sink("brain")
        store.write_many([event("sig_a", "v1")])
        in_flight = store.list_ready_delivery_payloads("brain")[0]

        store.write_many([event("sig_a", "v2", collected_hour=11)])

        assert (
            store.mark_delivery_success(
                "brain", "sig_a", delivered_at=when, expected_updated_at=in_flight.updated_at
            )
            is False
        )
        assert store.get_delivery("brain", "sig_a").status == "pending"

        current = store.get_delivery("brain", "sig_a")
        assert (
            store.mark_delivery_success(
                "brain", "sig_a", delivered_at=when, expected_updated_at=current.updated_at
            )
            is True
        )
        assert store.get_delivery("brain", "sig_a").status == "delivered"
