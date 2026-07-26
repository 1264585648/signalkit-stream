from pathlib import Path
import sqlite3
import tomllib

from signalkit_stream import config as config_module
from signalkit_stream.config import sample_config
from signalkit_stream.diagnostics import DiagnosticStatus, doctor, validate_config_file
from signalkit_stream.migrations import DATABASE_SCHEMA_VERSION, REQUIRED_TABLES
from signalkit_stream.storage import SQLiteSignalStore


def write_config(path: Path, *, database: Path, source: str = "hackernews") -> None:
    if source == "reddit":
        source_block = '''[[sources]]
name = "reddit-leads"
type = "reddit"
subreddit = "SaaS"
'''
    elif source == "missing":
        source_block = '''[[sources]]
name = "missing"
type = "does-not-exist"
'''
    else:
        source_block = '''[[sources]]
name = "hn"
type = "hackernews"
feed = "newstories"
'''
    path.write_text(
        f'''[runtime]
database = "{database.as_posix()}"

{source_block}
''',
        encoding="utf-8",
    )


def test_validate_config_checks_adapter_wiring_without_network(tmp_path) -> None:
    config = tmp_path / "signalkit.toml"
    write_config(config, database=tmp_path / "signals.db")

    report = validate_config_file(config)

    assert report.ok is True
    assert [check.status for check in report.checks] == [
        DiagnosticStatus.PASS,
        DiagnosticStatus.PASS,
    ]
    assert "collector ready" in report.checks[1].message


def test_validate_reports_missing_reddit_credentials_without_secret_values(tmp_path, monkeypatch) -> None:
    for name in ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT"):
        monkeypatch.delenv(name, raising=False)
    config = tmp_path / "signalkit.toml"
    write_config(config, database=tmp_path / "signals.db", source="reddit")

    report = validate_config_file(config)
    serialized = str(report.to_dict())

    assert report.ok is False
    assert report.failures == 1
    assert "REDDIT_CLIENT_ID" in serialized
    assert "client_secret" not in serialized.lower()


def test_validate_reports_unknown_adapter_type(tmp_path) -> None:
    config = tmp_path / "signalkit.toml"
    write_config(config, database=tmp_path / "signals.db", source="missing")

    report = validate_config_file(config)

    assert report.ok is False
    assert any("unknown collector type" in check.message for check in report.checks)


def test_doctor_warns_before_database_exists_then_checks_initialized_database(tmp_path) -> None:
    config = tmp_path / "signalkit.toml"
    database = tmp_path / "nested" / "signals.db"
    write_config(config, database=database)

    before = doctor(config)
    assert before.ok is True
    assert any(
        check.name == "database" and check.status is DiagnosticStatus.WARN
        for check in before.checks
    )

    with SQLiteSignalStore(database):
        pass

    after = doctor(config)
    assert after.ok is True
    assert any(
        check.name == "database-integrity"
        and check.status is DiagnosticStatus.PASS
        and "quick_check passed" in check.message
        for check in after.checks
    )
    schema = next(check for check in after.checks if check.name == "database-schema")
    assert schema.status is DiagnosticStatus.PASS
    assert schema.details["user_version"] == DATABASE_SCHEMA_VERSION
    assert schema.details["supported_version"] == DATABASE_SCHEMA_VERSION


def test_validate_invalid_config_is_single_failure(tmp_path) -> None:
    config = tmp_path / "broken.toml"
    config.write_text("not = [valid", encoding="utf-8")

    report = validate_config_file(config)

    assert report.ok is False
    assert report.failures == 1
    assert report.checks[0].name == "config"


def test_sample_config_is_valid_for_validate_command(tmp_path) -> None:
    config = tmp_path / "signalkit.toml"
    config.write_text(sample_config(), encoding="utf-8")

    report = validate_config_file(config)

    assert report.ok is True


def test_doctor_parses_the_configuration_file_only_once(tmp_path, monkeypatch) -> None:
    config = tmp_path / "signalkit.toml"
    database = tmp_path / "signals.db"
    write_config(config, database=database)
    with SQLiteSignalStore(database):
        pass

    real_load = tomllib.load
    parses: list[int] = []

    def counting_load(handle):  # noqa: ANN001, ANN202
        parses.append(1)
        return real_load(handle)

    monkeypatch.setattr(config_module.tomllib, "load", counting_load)

    report = doctor(config)

    assert report.ok is True
    assert len(parses) == 1


def test_doctor_schema_check_uses_the_shared_required_table_set(tmp_path) -> None:
    config = tmp_path / "signalkit.toml"
    database = tmp_path / "signals.db"
    write_config(config, database=database)
    connection = sqlite3.connect(database)
    try:
        for table in sorted(REQUIRED_TABLES):
            connection.execute(f"CREATE TABLE {table} (value TEXT)")
        connection.execute(f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION}")
        connection.commit()
    finally:
        connection.close()

    complete = next(
        check for check in doctor(config).checks if check.name == "database-schema"
    )
    assert complete.status is DiagnosticStatus.PASS
    assert complete.message == f"database schema version {DATABASE_SCHEMA_VERSION} is current"

    connection = sqlite3.connect(database)
    try:
        connection.execute(f"DROP TABLE {sorted(REQUIRED_TABLES)[0]}")
        connection.commit()
    finally:
        connection.close()

    incomplete = next(
        check for check in doctor(config).checks if check.name == "database-schema"
    )
    assert incomplete.status is DiagnosticStatus.FAIL
    assert "claims the current schema version" in incomplete.message


def test_doctor_probes_write_lock_after_database_checks(tmp_path) -> None:
    config = tmp_path / "signalkit.toml"
    database = tmp_path / "signals.db"
    write_config(config, database=database)
    with SQLiteSignalStore(database):
        pass

    names = [check.name for check in doctor(config).checks]

    assert names == [
        "config",
        "source:hn",
        "database-path",
        "database-integrity",
        "database-schema",
        "database-write-lock",
    ]
