from datetime import UTC, datetime
import importlib
import inspect
import sqlite3

import pytest

from signalkit_stream import storage as storage_module
from signalkit_stream.migrations import (
    DATABASE_SCHEMA_VERSION,
    DatabaseMigrationError,
    DatabaseSchemaTooNew,
    get_database_schema_version,
    migrate_database,
)
from signalkit_stream.storage import SQLiteSignalStore


def test_migrations_module_is_the_only_persistent_schema_definition() -> None:
    """Guard against re-introducing a duplicate (and therefore drifting) DDL copy."""

    source = inspect.getsource(storage_module)
    assert "CREATE TABLE" not in source
    assert "CREATE INDEX" not in source
    assert "CREATE TRIGGER" not in source

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("signalkit_stream._storage_impl")


def test_new_database_is_initialized_at_current_schema_version(tmp_path) -> None:
    path = tmp_path / "new.db"

    with SQLiteSignalStore(path) as store:
        assert store.database_schema_version == DATABASE_SCHEMA_VERSION
        tables = {
            row[0]
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert {"signals", "checkpoints", "source_health", "delivery_sinks", "deliveries"} <= tables


def test_legacy_unversioned_database_migrates_and_preserves_signal(tmp_path) -> None:
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
        INSERT INTO signals (
            id, source, kind, title, content, author, url,
            created_at, collected_at, metadata_json
        ) VALUES (
            'sig_legacy', 'rss', 'article', 'Legacy', 'Body', 'alice',
            'https://example.com/legacy', '2026-07-01T00:00:00+00:00',
            '2026-07-25T00:00:00+00:00', '{}'
        );
        """
    )
    connection.close()

    with SQLiteSignalStore(path) as store:
        event = store.get("sig_legacy")
        assert store.database_schema_version == DATABASE_SCHEMA_VERSION
        assert event is not None
        assert event.title == "Legacy"
        assert event.source_instance == "default"
        assert event.updated_at is None
        assert store.get_checkpoint("rss:default") is None


def test_pre_versioned_current_schema_is_stamped_without_losing_state(tmp_path) -> None:
    path = tmp_path / "pre-versioned.db"
    connection = sqlite3.connect(path)
    migrate_database(connection)
    connection.execute("PRAGMA user_version = 0")
    connection.execute(
        """
        INSERT INTO source_health (
            source_key, status, updated_at, consecutive_failures, total_runs, total_events
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("github:test", "healthy", datetime(2026, 7, 25, tzinfo=UTC).isoformat(), 0, 7, 42),
    )
    connection.commit()
    connection.close()

    with SQLiteSignalStore(path) as store:
        health = store.get_source_health("github:test")
        assert store.database_schema_version == DATABASE_SCHEMA_VERSION
        assert health is not None
        assert health.total_runs == 7
        assert health.total_events == 42


def test_future_database_version_is_rejected_without_mutation(tmp_path) -> None:
    path = tmp_path / "future.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE future_marker (value TEXT NOT NULL)")
    connection.execute("INSERT INTO future_marker VALUES ('keep-me')")
    connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION + 1}")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseSchemaTooNew, match="newer than supported"):
        SQLiteSignalStore(path)

    connection = sqlite3.connect(path)
    try:
        assert get_database_schema_version(connection) == DATABASE_SCHEMA_VERSION + 1
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        marker = connection.execute("SELECT value FROM future_marker").fetchone()
    finally:
        connection.close()

    assert tables == {"future_marker"}
    assert marker == ("keep-me",)


def test_failed_legacy_migration_rolls_back_version_and_created_objects(tmp_path) -> None:
    path = tmp_path / "unknown.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE signals (id TEXT PRIMARY KEY, source TEXT NOT NULL)")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseMigrationError, match="known SignalKit Stream schema"):
        SQLiteSignalStore(path)

    connection = sqlite3.connect(path)
    try:
        assert get_database_schema_version(connection) == 0
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()

    assert tables == {"signals"}
