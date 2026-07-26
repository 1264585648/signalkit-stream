from __future__ import annotations

from datetime import UTC, datetime
import sqlite3

import pytest

from signalkit_stream.diagnostics import DiagnosticStatus, doctor
from signalkit_stream.maintenance import backup_database
from signalkit_stream.migrations import migrate_database
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.observability import read_snapshot
from signalkit_stream.sqlite_ops import probe_write_lock
from signalkit_stream.storage import SQLiteSignalStore


def event(event_id: str) -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source="test",
        kind=SignalKind.POST,
        content=f"event {event_id}",
        url=f"https://example.com/{event_id}",
        created_at=datetime(2026, 7, 26, tzinfo=UTC),
    )


def write_config(path, database) -> None:
    path.write_text(
        f'''[runtime]
database = "{database.as_posix()}"

[[sources]]
name = "hn"
type = "hackernews"
feed = "newstories"
''',
        encoding="utf-8",
    )


def _pragma(connection: sqlite3.Connection, name: str) -> object:
    return connection.execute(f"PRAGMA {name}").fetchone()[0]


def test_store_enables_wal_and_relaxed_durability_pragmas(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        assert str(_pragma(store._connection, "journal_mode")).lower() == "wal"
        assert int(_pragma(store._connection, "synchronous")) == 1  # NORMAL
        assert int(_pragma(store._connection, "foreign_keys")) == 1

    # journal_mode is persistent, so a fresh connection inherits WAL from the file.
    plain = sqlite3.connect(database)
    try:
        assert str(_pragma(plain, "journal_mode")).lower() == "wal"
    finally:
        plain.close()


def test_open_reader_does_not_block_a_store_write(tmp_path) -> None:
    """With a rollback journal an open read transaction blocks every commit."""

    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.write_many([event("one")])

    reader = sqlite3.connect(database, timeout=0)
    reader.execute("BEGIN")
    assert reader.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 1
    try:
        with SQLiteSignalStore(database, timeout=0) as store:
            assert store.write_many([event("two")]).inserted == 1
        # The reader keeps its own snapshot until it ends its transaction.
        assert reader.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 1
    finally:
        reader.rollback()
        reader.close()

    assert read_snapshot(database).signals_total == 2


def test_read_only_snapshot_works_while_a_writer_holds_the_write_lock(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.write_many([event("one")])

        writer = sqlite3.connect(database, timeout=0)
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("UPDATE signals SET content = 'uncommitted' WHERE id = 'one'")
            snapshot = read_snapshot(database)
        finally:
            writer.rollback()
            writer.close()

    assert snapshot.signals_total == 1


def test_store_open_tolerates_a_refused_journal_mode_switch(tmp_path) -> None:
    database = tmp_path / "rollback-journal.db"
    connection = sqlite3.connect(database)
    try:
        migrate_database(connection)
    finally:
        connection.close()

    locker = sqlite3.connect(database)
    locker.execute("BEGIN IMMEDIATE")
    try:
        with SQLiteSignalStore(database, timeout=0) as store:
            assert str(_pragma(store._connection, "journal_mode")).lower() == "delete"
            assert int(_pragma(store._connection, "synchronous")) == 1
            assert store.count() == 0
    finally:
        locker.rollback()
        locker.close()

    with SQLiteSignalStore(database) as store:
        assert str(_pragma(store._connection, "journal_mode")).lower() == "wal"


def test_store_rejects_negative_busy_timeout(tmp_path) -> None:
    with pytest.raises(ValueError, match="timeout must be >= 0"):
        SQLiteSignalStore(tmp_path / "signals.db", timeout=-0.1)


def test_write_lock_failure_is_fast_atomic_and_recoverable(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.write_many([event("one")])

    locker = sqlite3.connect(database)
    locker.execute("BEGIN IMMEDIATE")
    try:
        with SQLiteSignalStore(database, timeout=0) as store:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                store.write_many([event("two")])
            assert store.count() == 1
            assert store.get("two") is None
    finally:
        locker.rollback()
        locker.close()

    with SQLiteSignalStore(database, timeout=0) as store:
        assert store.count() == 1
        result = store.write_many([event("two")])
        assert result.inserted == 1
        assert store.count() == 2


def test_write_lock_probe_is_non_mutating_and_detects_contention(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.write_many([event("one")])

    available = probe_write_lock(database, timeout=0)
    assert available.available is True
    assert available.error is None
    assert available.journal_mode is not None
    assert available.busy_timeout_ms == 0

    with SQLiteSignalStore(database) as store:
        assert store.count() == 1

    locker = sqlite3.connect(database)
    locker.execute("BEGIN IMMEDIATE")
    try:
        busy = probe_write_lock(database, timeout=0)
        assert busy.available is False
        assert busy.error is not None
        assert "locked" in busy.error.lower() or "busy" in busy.error.lower()
    finally:
        locker.rollback()
        locker.close()

    recovered = probe_write_lock(database, timeout=0)
    assert recovered.available is True
    with SQLiteSignalStore(database) as store:
        assert store.count() == 1


def test_doctor_reports_write_lock_pass_and_busy_warning(tmp_path) -> None:
    database = tmp_path / "signals.db"
    config = tmp_path / "signalkit.toml"
    write_config(config, database)
    with SQLiteSignalStore(database):
        pass

    healthy = doctor(config)
    healthy_lock = next(check for check in healthy.checks if check.name == "database-write-lock")
    assert healthy.ok is True
    assert healthy_lock.status is DiagnosticStatus.PASS
    assert healthy_lock.details["journal_mode"] is not None

    locker = sqlite3.connect(database)
    locker.execute("BEGIN IMMEDIATE")
    try:
        busy = doctor(config)
    finally:
        locker.rollback()
        locker.close()

    busy_lock = next(check for check in busy.checks if check.name == "database-write-lock")
    assert busy.ok is True
    assert busy_lock.status is DiagnosticStatus.WARN
    assert "another writer may be active" in busy_lock.message
    assert busy_lock.details["error"]

    recovered = doctor(config)
    recovered_lock = next(
        check for check in recovered.checks if check.name == "database-write-lock"
    )
    assert recovered_lock.status is DiagnosticStatus.PASS


def test_wal_backup_reads_last_committed_snapshot_while_writer_is_active(tmp_path) -> None:
    database = tmp_path / "signals.db"
    backup = tmp_path / "backup.db"
    with SQLiteSignalStore(database) as store:
        store.write_many([event("one")])

    writer = sqlite3.connect(database)
    try:
        mode = str(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        assert mode == "wal"
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE signals SET content = ? WHERE id = ?",
            ("uncommitted", "one"),
        )

        result = backup_database(database, backup)
        assert result.quick_check == "ok"

        with SQLiteSignalStore(backup) as store:
            backed_up = store.get("one")
            assert backed_up is not None
            assert backed_up.content == "event one"

        with SQLiteSignalStore(database, timeout=0) as reader:
            current = reader.get("one")
            assert current is not None
            assert current.content == "event one"
    finally:
        writer.rollback()
        writer.close()

    with SQLiteSignalStore(database) as store:
        current = store.get("one")
        assert current is not None
        assert current.content == "event one"
