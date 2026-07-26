import sqlite3

from signalkit_stream.diagnostics import DiagnosticStatus, doctor
from signalkit_stream.migrations import DATABASE_SCHEMA_VERSION
from signalkit_stream.storage import SQLiteSignalStore


def _config(tmp_path, database) -> str:
    path = tmp_path / "signalkit.toml"
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
    return str(path)


def _schema_check(report):
    return next(check for check in report.checks if check.name == "database-schema")


def test_doctor_reports_current_schema(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database):
        pass

    check = _schema_check(doctor(_config(tmp_path, database)))

    assert check.status is DiagnosticStatus.PASS
    assert check.details["user_version"] == DATABASE_SCHEMA_VERSION
    assert check.details["supported_version"] == DATABASE_SCHEMA_VERSION


def test_doctor_warns_before_automatic_forward_migration(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database):
        pass
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA user_version = 0")
    connection.close()

    check = _schema_check(doctor(_config(tmp_path, database)))

    assert check.status is DiagnosticStatus.WARN
    assert "forward migration" in check.message


def test_doctor_fails_for_future_schema(tmp_path) -> None:
    database = tmp_path / "signals.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE marker (value TEXT)")
    connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION + 1}")
    connection.close()

    check = _schema_check(doctor(_config(tmp_path, database)))

    assert check.status is DiagnosticStatus.FAIL
    assert "newer than supported" in check.message


def test_doctor_fails_when_current_version_claim_is_incomplete(tmp_path) -> None:
    database = tmp_path / "signals.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE marker (value TEXT)")
    connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION}")
    connection.close()

    check = _schema_check(doctor(_config(tmp_path, database)))

    assert check.status is DiagnosticStatus.FAIL
    assert "claims the current schema version" in check.message
