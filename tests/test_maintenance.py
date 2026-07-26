from datetime import UTC, datetime
import json
import os
import sqlite3
import sys

import pytest

import signalkit_stream.maintenance as maintenance
from signalkit_stream.maintenance import backup_database, main, verify_database
from signalkit_stream.migrations import DATABASE_SCHEMA_VERSION
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def seed(path) -> None:
    with SQLiteSignalStore(path) as store:
        store.save_many(
            [
                SignalEvent(
                    id="sig_backup",
                    source="test",
                    kind=SignalKind.POST,
                    content="preserve",
                    url="https://example.com/backup",
                    created_at=datetime(2026, 7, 26, tzinfo=UTC),
                )
            ]
        )


def test_backup_database_preserves_store_state_and_schema_version(tmp_path) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)

    result = backup_database(source, destination)

    assert result.quick_check == "ok"
    assert result.pages > 0
    assert result.schema_version == DATABASE_SCHEMA_VERSION
    assert destination.exists()

    verified = verify_database(destination)
    assert verified.ok is True
    assert verified.schema_status == "current"
    assert verified.schema_version == DATABASE_SCHEMA_VERSION

    with SQLiteSignalStore(destination) as store:
        restored = store.get("sig_backup")
    assert restored is not None
    assert restored.content == "preserve"


def test_backup_refuses_overwrite_and_same_path(tmp_path) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    destination.write_bytes(b"existing")

    with pytest.raises(FileExistsError):
        backup_database(source, destination)
    with pytest.raises(ValueError, match="must differ"):
        backup_database(source, source)


def test_backup_overwrite_replaces_existing_destination_atomically(tmp_path) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    destination.write_bytes(b"old")

    result = backup_database(source, destination, overwrite=True)

    assert result.quick_check == "ok"
    assert verify_database(destination).ok is True
    assert destination.read_bytes() != b"old"


def test_failed_backup_keeps_existing_destination(tmp_path, monkeypatch) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    destination.write_bytes(b"known-good-old-backup")
    monkeypatch.setattr(maintenance, "_quick_check", lambda connection: "forced failure")

    with pytest.raises(RuntimeError, match="integrity check failed"):
        backup_database(source, destination, overwrite=True)

    assert destination.read_bytes() == b"known-good-old-backup"
    assert not list(tmp_path.glob(f".{destination.name}.*.tmp"))


def test_backup_retries_publication_when_destination_is_transiently_locked(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    destination.write_bytes(b"known-good-old-backup")

    real_replace = os.replace
    attempts: list[int] = []

    def flaky_replace(src, dst):  # noqa: ANN001, ANN202
        attempts.append(1)
        if len(attempts) < 3:
            raise PermissionError(13, "The process cannot access the file")
        return real_replace(src, dst)

    monkeypatch.setattr(maintenance.os, "replace", flaky_replace)
    monkeypatch.setattr(maintenance, "_PUBLISH_RETRY_DELAY", 0.0)

    result = backup_database(source, destination, overwrite=True)

    assert len(attempts) == 3
    assert result.quick_check == "ok"
    assert verify_database(destination).ok is True


def test_backup_gives_up_after_bounded_retries_and_preserves_prior_backup(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    destination.write_bytes(b"known-good-old-backup")

    attempts: list[int] = []

    def always_locked(src, dst):  # noqa: ANN001, ANN202
        attempts.append(1)
        raise PermissionError(13, "The process cannot access the file")

    monkeypatch.setattr(maintenance.os, "replace", always_locked)
    monkeypatch.setattr(maintenance, "_PUBLISH_RETRY_DELAY", 0.0)

    with pytest.raises(PermissionError):
        backup_database(source, destination, overwrite=True)

    assert len(attempts) == maintenance._PUBLISH_ATTEMPTS
    assert destination.read_bytes() == b"known-good-old-backup"
    assert not list(tmp_path.glob(f".{destination.name}.*.tmp"))


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="only Windows refuses to rename over a destination that is still open",
)
def test_backup_over_open_destination_keeps_prior_backup_and_cleans_up(tmp_path) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    backup_database(source, destination)
    original = destination.read_bytes()

    with destination.open("rb") as reader:
        reader.read(16)
        with pytest.raises(OSError):
            backup_database(source, destination, overwrite=True)

    assert destination.read_bytes() == original
    assert verify_database(destination).ok is True
    assert not list(tmp_path.glob(f".{destination.name}.*.tmp"))


def test_verify_reports_migration_required_without_modifying_database(tmp_path) -> None:
    database = tmp_path / "legacy-version.db"
    seed(database)
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA user_version = 0")
    connection.commit()
    connection.close()

    result = verify_database(database)

    assert result.quick_check == "ok"
    assert result.schema_status == "migration_required"
    assert result.ok is False

    connection = sqlite3.connect(database)
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    finally:
        connection.close()
    assert version == 0


def test_verify_reports_future_and_invalid_schema(tmp_path) -> None:
    future = tmp_path / "future.db"
    connection = sqlite3.connect(future)
    connection.execute("CREATE TABLE marker (value TEXT)")
    connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION + 1}")
    connection.commit()
    connection.close()

    future_result = verify_database(future)
    assert future_result.schema_status == "future"
    assert future_result.ok is False

    invalid = tmp_path / "invalid.db"
    connection = sqlite3.connect(invalid)
    connection.execute("CREATE TABLE marker (value TEXT)")
    connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION}")
    connection.commit()
    connection.close()

    invalid_result = verify_database(invalid)
    assert invalid_result.schema_status == "invalid"
    assert invalid_result.schema_error is not None
    assert invalid_result.ok is False


def test_verify_missing_database_raises(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        verify_database(tmp_path / "missing.db")


def test_maintenance_cli_json(tmp_path, capsys) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)

    assert main(["backup", str(source), str(destination), "--format", "json"]) == 0
    backup_payload = json.loads(capsys.readouterr().out)
    assert backup_payload["quick_check"] == "ok"
    assert backup_payload["schema_version"] == DATABASE_SCHEMA_VERSION

    assert main(["verify", str(destination), "--format", "json"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["ok"] is True
    assert verify_payload["schema_status"] == "current"
