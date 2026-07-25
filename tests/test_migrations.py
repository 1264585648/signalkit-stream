import sqlite3

import pytest

from signalkit_stream.migrations import (
    PERSISTENCE_SCHEMA_VERSION,
    migrate_database,
    schema_status,
)
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def test_schema_status_missing_database_is_non_mutating(tmp_path) -> None:
    database = tmp_path / "signals.db"

    status = schema_status(database)

    assert status.exists is False
    assert status.current_version == 0
    assert status.target_version == PERSISTENCE_SCHEMA_VERSION
    assert database.exists() is False


def test_migrate_fresh_database_creates_versioned_schema(tmp_path) -> None:
    database = tmp_path / "signals.db"

    status = migrate_database(database)

    assert status.exists is True
    assert status.current is True
    assert status.current_version == 1
    assert [item.version for item in status.migrations] == [1]
    assert status.migrations[0].name == "adopt_current_storage_schema"

    connection = sqlite3.connect(database)
    try:
        row = connection.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
    finally:
        connection.close()
    assert row == [(1, "adopt_current_storage_schema")]


def test_migrate_adopts_existing_unversioned_database_without_losing_events(tmp_path) -> None:
    database = tmp_path / "signals.db"
    event = SignalEvent(
        id="sig_before_migrations",
        source="test",
        kind=SignalKind.POST,
        content="preserve me",
        url="https://example.com/preserve",
    )
    with SQLiteSignalStore(database) as store:
        store.save_many([event])

    before = schema_status(database)
    assert before.current_version == 0

    after = migrate_database(database)

    assert after.current_version == 1
    with SQLiteSignalStore(database) as store:
        preserved = store.get(event.id)
    assert preserved is not None
    assert preserved.content == "preserve me"


def test_migrate_is_idempotent(tmp_path) -> None:
    database = tmp_path / "signals.db"

    first = migrate_database(database)
    second = migrate_database(database)

    assert first.current_version == second.current_version == 1
    assert len(second.migrations) == 1


def test_migrate_rejects_database_newer_than_current_build(tmp_path) -> None:
    database = tmp_path / "signals.db"
    migrate_database(database)
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) VALUES (99, 'future', 'future')"
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="newer than this SignalKit build"):
        migrate_database(database)


def test_migrate_rejects_downgrade(tmp_path) -> None:
    database = tmp_path / "signals.db"
    migrate_database(database)

    with pytest.raises(RuntimeError, match="downgrades are not supported"):
        migrate_database(database, target_version=0)
