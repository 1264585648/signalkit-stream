from datetime import UTC, datetime
import sqlite3

import pytest

from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import Cursor
from signalkit_stream.storage import SQLiteSignalStore, SourceHealth


def make_event(content: str = "A body", *, collected_day: int = 25) -> SignalEvent:
    return SignalEvent(
        id="sig_1",
        source="test",
        source_instance="instance-a",
        kind=SignalKind.POST,
        title="A title",
        content=content,
        author="alice",
        url="https://example.com/1",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        updated_at=datetime(2026, 7, 2, tzinfo=UTC),
        collected_at=datetime(2026, 7, collected_day, tzinfo=UTC),
        metadata={"x": 1},
    )


def numbered_event(index: int, content: str | None = None) -> SignalEvent:
    return SignalEvent(
        id=f"sig_{index}",
        source="test",
        source_instance="instance-a",
        kind=SignalKind.POST,
        content=content if content is not None else f"body-{index}",
        url=f"https://example.com/{index}",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        collected_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


def test_sqlite_store_insert_unchanged_update(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        first = store.write_many([make_event()])
        unchanged = store.write_many([make_event(collected_day=26)])
        updated = store.write_many([make_event("Changed", collected_day=27)])

        assert (first.inserted, first.updated, first.unchanged) == (1, 0, 0)
        assert (unchanged.inserted, unchanged.updated, unchanged.unchanged) == (0, 0, 1)
        assert (updated.inserted, updated.updated, updated.unchanged) == (0, 1, 0)
        assert store.count() == 1
        assert store.get("sig_1").content == "Changed"


def test_batch_commits_checkpoint_with_events(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        cursor = Cursor(source_key="test:instance-a", state={"page": 2})
        result = store.commit_batch(
            [make_event()],
            source_key="test:instance-a",
            cursor=cursor,
        )
        checkpoint = store.get_checkpoint("test:instance-a")

        assert result.inserted == 1
        assert checkpoint is not None
        assert checkpoint.cursor == cursor
        assert checkpoint.last_success_at is not None


def test_source_health_round_trip(tmp_path) -> None:
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    health = SourceHealth(
        source_key="test:instance-a",
        status="degraded",
        updated_at=now,
        last_attempt_at=now,
        last_success_at=datetime(2026, 7, 25, 11, 0, tzinfo=UTC),
        last_error="timeout",
        consecutive_failures=2,
        total_runs=7,
        total_events=42,
    )
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.upsert_source_health(health)
        assert store.get_source_health(health.source_key) == health
        assert store.list_source_health() == [health]


def test_mixed_batch_write_result_counts_match_stored_rows(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        store.write_many([numbered_event(index) for index in range(5)])

        result = store.write_many(
            [
                numbered_event(0),  # unchanged
                numbered_event(1),  # unchanged
                numbered_event(2, "changed-2"),  # updated
                numbered_event(3, "changed-3"),  # updated
                numbered_event(4),  # unchanged
                numbered_event(5),  # inserted
                numbered_event(6),  # inserted
            ]
        )

        assert (result.inserted, result.updated, result.unchanged) == (2, 2, 3)
        assert result.changed == 4
        assert store.count() == 7
        assert store.get("sig_2").content == "changed-2"
        assert store.get("sig_4").content == "body-4"
        # Unchanged rows are not rewritten at all, so their fingerprints are untouched.
        hashes = {
            str(row["id"]): str(row["event_hash"])
            for row in store._connection.execute("SELECT id, event_hash FROM signals")
        }
        assert hashes["sig_4"] == numbered_event(4).fingerprint()
        assert hashes["sig_2"] == numbered_event(2, "changed-2").fingerprint()


def test_duplicate_ids_inside_one_batch_are_counted_once_per_transition(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        same_twice = store.write_many([numbered_event(1), numbered_event(1)])
        assert (same_twice.inserted, same_twice.updated, same_twice.unchanged) == (1, 0, 1)

        changed_twice = store.write_many(
            [numbered_event(2), numbered_event(2, "second-version")]
        )
        assert (changed_twice.inserted, changed_twice.updated, changed_twice.unchanged) == (
            1,
            1,
            0,
        )
        assert store.get("sig_2").content == "second-version"
        assert store.count() == 2


def test_large_batch_is_chunked_beyond_the_variable_limit(tmp_path) -> None:
    events = [numbered_event(index) for index in range(1200)]
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        first = store.write_many(events)
        second = store.write_many(events)

        assert (first.inserted, first.updated, first.unchanged) == (1200, 0, 0)
        assert (second.inserted, second.updated, second.unchanged) == (0, 0, 1200)
        assert store.count() == 1200


def test_row_already_written_by_another_store_is_upserted_not_rejected(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as writer_b, SQLiteSignalStore(database) as writer_a:
        writer_b.write_many([numbered_event(1, "from-b")])

        result = writer_a.commit_batch(
            [numbered_event(1, "from-a"), numbered_event(2)],
            source_key="test:instance-a",
            cursor=Cursor(source_key="test:instance-a", state={"page": 1}),
        )

        assert (result.inserted, result.updated, result.unchanged) == (1, 1, 0)
        assert writer_a.get("sig_1").content == "from-a"
        assert writer_a.get("sig_2") is not None
        assert writer_a.get_checkpoint("test:instance-a") is not None


def test_concurrent_writer_cannot_discard_a_page_or_its_checkpoint(tmp_path) -> None:
    """A second writer racing the dedup probe must not cost us the whole page.

    Before the batch became one ``BEGIN IMMEDIATE`` transaction, the per-row
    ``SELECT event_hash`` ran outside any transaction. Writer B could commit ``sig_1``
    inside that window, writer A's plain ``INSERT`` then raised
    ``UNIQUE constraint failed: signals.id``, and ``with connection:`` rolled back the
    *entire* page -- losing the unrelated brand-new ``sig_2`` and leaving the checkpoint
    unadvanced, which ``pipeline.run_collector`` reports as a failed run forever.
    """

    database = tmp_path / "signals.db"
    store_a = SQLiteSignalStore(database)
    store_b = SQLiteSignalStore(database, timeout=0.2)
    interleaved: list[str] = []

    def interleave(statement: str) -> None:
        # sqlite3 traces a statement before it starts stepping, so hooking the dedup
        # SELECT would let writer B commit before writer A's read snapshot is even
        # taken. Hooking the write is the real check-then-act window.
        if "INSERT INTO signals" not in statement or interleaved:
            return
        interleaved.append(statement)
        try:
            store_b.write_many([numbered_event(1, "from-b")])
        except sqlite3.OperationalError as exc:
            # Correct post-fix behavior: writer B is serialized behind writer A's
            # BEGIN IMMEDIATE instead of slipping between A's read and A's write.
            interleaved.append(f"serialized: {exc}")

    try:
        store_a._connection.set_trace_callback(interleave)
        result = store_a.commit_batch(
            [numbered_event(1, "from-a"), numbered_event(2)],
            source_key="test:instance-a",
            cursor=Cursor(source_key="test:instance-a", state={"page": 7}),
        )
        store_a._connection.set_trace_callback(None)

        assert interleaved, "the signals write was never observed"
        assert result.inserted + result.updated == 2
        assert store_a.get("sig_1") is not None
        assert store_a.get("sig_2") is not None
        checkpoint = store_a.get_checkpoint("test:instance-a")
        assert checkpoint is not None
        assert checkpoint.cursor.state == {"page": 7}
    finally:
        store_a._connection.set_trace_callback(None)
        store_a.close()
        store_b.close()

    with SQLiteSignalStore(database) as store:
        assert store.count() == 2
        assert store.get_checkpoint("test:instance-a") is not None


def test_write_many_batch_is_all_or_nothing_when_the_write_lock_is_held(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.write_many([numbered_event(0)])

    locker = sqlite3.connect(database)
    locker.execute("BEGIN IMMEDIATE")
    try:
        with SQLiteSignalStore(database, timeout=0) as store:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                store.commit_batch(
                    [numbered_event(1), numbered_event(2)],
                    source_key="test:instance-a",
                    cursor=Cursor(source_key="test:instance-a"),
                )
            assert store.count() == 1
            assert store.get_checkpoint("test:instance-a") is None
    finally:
        locker.rollback()
        locker.close()


def test_legacy_schema_is_migrated(tmp_path) -> None:
    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE signals (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            kind TEXT NOT NULL,
            title TEXT,
            content TEXT NOT NULL,
            author TEXT,
            url TEXT NOT NULL,
            created_at TEXT NOT NULL,
            collected_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        );
        """
    )
    connection.close()

    with SQLiteSignalStore(path) as store:
        columns = {
            row["name"] for row in store._connection.execute("PRAGMA table_info(signals)")
        }
    assert {"schema_version", "source_instance", "updated_at", "event_hash"} <= columns
