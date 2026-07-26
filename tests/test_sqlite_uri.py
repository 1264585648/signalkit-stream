from __future__ import annotations

import json
from pathlib import Path

import pytest

from signalkit_stream.diagnostics import DiagnosticStatus, doctor
from signalkit_stream.maintenance import backup_database, verify_database
from signalkit_stream.observability import read_snapshot
from signalkit_stream.sqlite_ops import _sqlite_uri, probe_write_lock
from signalkit_stream.storage import SQLiteSignalStore

# SQLite percent-decodes URI paths, so a directory whose name contains a '%' followed by
# two hex digits silently resolves to a different (missing) file unless it is escaped.
AWKWARD_DIRECTORY_NAMES = ["plain", "pct%41dir", "with space", "amp&dir"]


@pytest.fixture(params=AWKWARD_DIRECTORY_NAMES)
def seeded_database(tmp_path: Path, request: pytest.FixtureRequest) -> Path:
    directory = tmp_path / str(request.param)
    directory.mkdir()
    database = directory / "signals.db"
    with SQLiteSignalStore(database):
        pass
    return database


class _ResolvedPath:
    """Stand-in for a path whose ``resolve()`` result is fixed.

    Used to exercise UNC URI construction without needing an actual network share,
    which would require administrative share setup and be flaky in CI.
    """

    def __init__(self, text: str) -> None:
        self._text = text

    def resolve(self) -> _ResolvedPath:
        return self

    def as_posix(self) -> str:
        return self._text


@pytest.mark.parametrize(
    ("posix_text", "expected"),
    [
        ("C:/data/signals.db", "file:///C:/data/signals.db?mode=ro"),
        ("C:/pct%41dir/signals.db", "file:///C:/pct%2541dir/signals.db?mode=ro"),
        ("C:/with space/signals.db", "file:///C:/with%20space/signals.db?mode=ro"),
        ("C:/amp&dir/signals.db", "file:///C:/amp%26dir/signals.db?mode=ro"),
        ("/var/lib/signalkit/signals.db", "file:///var/lib/signalkit/signals.db?mode=ro"),
        # A UNC path needs a third leading slash, otherwise SQLite parses the server
        # name as a URI authority and reports "invalid uri authority".
        ("//server/share/signals.db", "file:////server/share/signals.db?mode=ro"),
        ("//server/share/pct%41/x.db", "file:////server/share/pct%2541/x.db?mode=ro"),
    ],
)
def test_sqlite_uri_escapes_paths_and_keeps_slash_count(posix_text: str, expected: str) -> None:
    assert _sqlite_uri(_ResolvedPath(posix_text), mode="ro") == expected  # type: ignore[arg-type]


def test_sqlite_uri_honors_requested_mode() -> None:
    assert _sqlite_uri(_ResolvedPath("C:/data/signals.db"), mode="rw").endswith(  # type: ignore[arg-type]
        "?mode=rw"
    )


def _write_config(path: Path, database: Path) -> None:
    path.write_text(
        f"""[runtime]
database = {json.dumps(str(database))}

[[sources]]
name = "hn"
type = "hackernews"
feed = "newstories"
""",
        encoding="utf-8",
    )


def test_verify_database_reads_awkward_paths(seeded_database: Path) -> None:
    result = verify_database(seeded_database)

    assert result.quick_check == "ok"
    assert result.ok is True


def test_backup_database_reads_awkward_source_paths(seeded_database: Path) -> None:
    destination = seeded_database.parent / "backup.db"

    result = backup_database(seeded_database, destination)

    assert result.quick_check == "ok"
    assert verify_database(destination).ok is True


def test_probe_write_lock_opens_awkward_paths(seeded_database: Path) -> None:
    probe = probe_write_lock(seeded_database)

    assert probe.available is True
    assert probe.error is None


def test_read_snapshot_opens_awkward_paths(seeded_database: Path) -> None:
    snapshot = read_snapshot(seeded_database)

    assert snapshot.schema_status == "current"


def test_doctor_opens_awkward_paths(tmp_path: Path, seeded_database: Path) -> None:
    config = tmp_path / "signalkit.toml"
    _write_config(config, seeded_database)

    report = doctor(config)

    integrity = next(check for check in report.checks if check.name == "database-integrity")
    write_lock = next(check for check in report.checks if check.name == "database-write-lock")
    assert integrity.status is DiagnosticStatus.PASS
    assert write_lock.status is DiagnosticStatus.PASS
    assert report.ok is True
