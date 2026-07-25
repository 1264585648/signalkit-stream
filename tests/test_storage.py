from datetime import UTC, datetime
import sqlite3

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
