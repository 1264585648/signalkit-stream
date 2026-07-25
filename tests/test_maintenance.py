import json

import pytest

from signalkit_stream.maintenance import backup_database, main, verify_database
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
                )
            ]
        )


def test_backup_database_preserves_store_state(tmp_path) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)

    result = backup_database(source, destination)

    assert result.quick_check == "ok"
    assert result.pages > 0
    assert destination.exists()
    verified = verify_database(destination)
    assert verified.ok is True
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


def test_backup_overwrite_replaces_existing_destination(tmp_path) -> None:
    source = tmp_path / "signals.db"
    destination = tmp_path / "backup.db"
    seed(source)
    destination.write_bytes(b"old")

    result = backup_database(source, destination, overwrite=True)

    assert result.quick_check == "ok"
    assert verify_database(destination).ok is True


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

    assert main(["verify", str(destination), "--format", "json"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["ok"] is True
