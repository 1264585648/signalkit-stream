from __future__ import annotations

import argparse

import signalkit_stream
from signalkit_stream.cli import build_parser


REQUIRED_TOP_LEVEL_API = {
    "SCHEMA_VERSION",
    "PROTOCOL_VERSION",
    "SignalEvent",
    "SignalKind",
    "SourceIdentity",
    "Cursor",
    "CollectorContext",
    "CollectorResult",
    "CollectorError",
    "CollectorErrorKind",
    "RateLimitSnapshot",
    "validate_collector_result",
    "RuntimeConfig",
    "SourceConfig",
    "SinkConfig",
    "StreamConfig",
    "load_config",
    "CollectionResult",
    "run_collector",
    "SourceRunResult",
    "StreamRuntime",
    "SQLiteSignalStore",
    "Checkpoint",
    "SourceHealth",
    "StoreWriteResult",
    "DATABASE_SCHEMA_VERSION",
    "DatabaseSchemaError",
    "DatabaseSchemaTooNew",
    "DatabaseMigrationError",
    "get_database_schema_version",
    "migrate_database",
    "validate_database_schema",
    "Sink",
    "SinkError",
    "StdoutSink",
    "JsonlSink",
    "WebhookSink",
    "DeliveryEngine",
    "DeliveryResult",
    "DeliveryRecord",
    "DiagnosticStatus",
    "DiagnosticCheck",
    "DiagnosticReport",
    "doctor",
    "validate_config_file",
    "validate_stream_config",
    "BackupResult",
    "VerifyResult",
    "backup_database",
    "verify_database",
    "SourceStatusSnapshot",
    "SinkStatusSnapshot",
    "StreamSnapshot",
    "read_snapshot",
    "format_snapshot",
    "LogFormat",
    "TextLogFormatter",
    "JsonLogFormatter",
    "configure_logging",
}

REQUIRED_COMMANDS = {
    "init",
    "validate",
    "doctor",
    "run",
    "collect",
    "show",
    "checkpoint",
    "status",
    "deliveries",
    "retry-deliveries",
    "db",
}


def _subcommands(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    action = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    return dict(action.choices)


def _option(parser: argparse.ArgumentParser, option: str) -> argparse.Action:
    return next(action for action in parser._actions if option in action.option_strings)


def test_required_top_level_python_api_remains_exported() -> None:
    exported = set(signalkit_stream.__all__)

    assert REQUIRED_TOP_LEVEL_API <= exported
    for name in REQUIRED_TOP_LEVEL_API:
        assert hasattr(signalkit_stream, name), name


def test_required_operator_cli_commands_remain_available() -> None:
    commands = _subcommands(build_parser())

    assert REQUIRED_COMMANDS <= set(commands)
    assert {"rss", "jsonfeed", "hn", "github", "reddit"} <= set(
        _subcommands(commands["collect"])
    )
    assert {"backup", "verify"} <= set(_subcommands(commands["db"]))


def test_required_automation_options_keep_supported_values() -> None:
    commands = _subcommands(build_parser())

    status_format = _option(commands["status"], "--format")
    assert set(status_format.choices or ()) >= {"table", "json", "prometheus"}
    assert _option(commands["status"], "--verbose") is not None

    run_log_format = _option(commands["run"], "--log-format")
    assert set(run_log_format.choices or ()) == {"text", "json"}

    reddit = _subcommands(commands["collect"])["reddit"]
    for option in (
        "--access-token-env",
        "--refresh-token-env",
        "--client-id-env",
        "--client-secret-env",
        "--user-agent-env",
    ):
        assert _option(reddit, option) is not None

    for command_name in ("validate", "doctor"):
        output_format = _option(commands[command_name], "--format")
        assert set(output_format.choices or ()) == {"table", "json"}
