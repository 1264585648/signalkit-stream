from pathlib import Path

from signalkit_stream.config import sample_config
from signalkit_stream.diagnostics import DiagnosticStatus, doctor, validate_config_file
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
    assert schema.details["user_version"] == 0


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
