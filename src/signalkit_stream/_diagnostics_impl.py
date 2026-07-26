from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import os
from pathlib import Path
import sqlite3
from typing import Any

from signalkit_stream.config import StreamConfig, load_config
from signalkit_stream.registry import CollectorRegistry, default_registry
from signalkit_stream.sinks import SinkRegistry, default_sink_registry
from signalkit_stream.sqlite_ops import _sqlite_uri


class DiagnosticStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass(slots=True, frozen=True)
class DiagnosticCheck:
    name: str
    status: DiagnosticStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(slots=True, frozen=True)
class DiagnosticReport:
    checks: tuple[DiagnosticCheck, ...]

    @property
    def ok(self) -> bool:
        return all(check.status is not DiagnosticStatus.FAIL for check in self.checks)

    @property
    def warnings(self) -> int:
        return sum(check.status is DiagnosticStatus.WARN for check in self.checks)

    @property
    def failures(self) -> int:
        return sum(check.status is DiagnosticStatus.FAIL for check in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "warnings": self.warnings,
            "failures": self.failures,
            "checks": [check.to_dict() for check in self.checks],
        }


def validate_config_file(
    path: str | Path,
    *,
    collector_registry: CollectorRegistry | None = None,
    sink_registry: SinkRegistry | None = None,
) -> DiagnosticReport:
    """Validate configuration and integration wiring without making network requests."""

    try:
        config = load_config(path)
    except (OSError, ValueError) as exc:
        return DiagnosticReport(
            (
                DiagnosticCheck(
                    "config",
                    DiagnosticStatus.FAIL,
                    str(exc),
                    {"path": str(path)},
                ),
            )
        )
    return validate_stream_config(
        config,
        collector_registry=collector_registry,
        sink_registry=sink_registry,
    )


def validate_stream_config(
    config: StreamConfig,
    *,
    collector_registry: CollectorRegistry | None = None,
    sink_registry: SinkRegistry | None = None,
) -> DiagnosticReport:
    collectors = collector_registry or default_registry()
    sinks = sink_registry or default_sink_registry()
    checks: list[DiagnosticCheck] = [
        DiagnosticCheck(
            "config",
            DiagnosticStatus.PASS,
            f"configuration parsed; {sum(source.enabled for source in config.sources)} enabled source(s), "
            f"{sum(sink.enabled for sink in config.sinks)} enabled sink(s)",
        )
    ]

    for source in config.sources:
        if not source.enabled:
            checks.append(
                DiagnosticCheck(
                    f"source:{source.name}",
                    DiagnosticStatus.WARN,
                    f"source is disabled ({source.type})",
                )
            )
            continue
        try:
            collector = collectors.create(source)
        except Exception as exc:
            checks.append(
                DiagnosticCheck(
                    f"source:{source.name}",
                    DiagnosticStatus.FAIL,
                    str(exc),
                    {"type": source.type},
                )
            )
        else:
            checks.append(
                DiagnosticCheck(
                    f"source:{source.name}",
                    DiagnosticStatus.PASS,
                    f"collector ready: {collector.identity.key}",
                    {"type": source.type},
                )
            )

    for sink in config.sinks:
        if not sink.enabled:
            checks.append(
                DiagnosticCheck(
                    f"sink:{sink.name}",
                    DiagnosticStatus.WARN,
                    f"sink is disabled ({sink.type})",
                )
            )
            continue
        try:
            created = sinks.create(sink)
        except Exception as exc:
            checks.append(
                DiagnosticCheck(
                    f"sink:{sink.name}",
                    DiagnosticStatus.FAIL,
                    str(exc),
                    {"type": sink.type},
                )
            )
        else:
            checks.append(
                DiagnosticCheck(
                    f"sink:{sink.name}",
                    DiagnosticStatus.PASS,
                    f"sink ready: {created.key}",
                    {"type": sink.type},
                )
            )

    return DiagnosticReport(tuple(checks))


def doctor(
    path: str | Path,
    *,
    collector_registry: CollectorRegistry | None = None,
    sink_registry: SinkRegistry | None = None,
) -> DiagnosticReport:
    """Run local diagnostics without contacting third-party services.

    Credential checks are performed indirectly by source/sink factories. Secret values
    are never included in the returned diagnostics.
    """

    validation = validate_config_file(
        path,
        collector_registry=collector_registry,
        sink_registry=sink_registry,
    )
    if validation.failures and not any(check.name == "config" and check.status is DiagnosticStatus.PASS for check in validation.checks):
        return validation

    try:
        config = load_config(path)
    except (OSError, ValueError):  # already represented by validation
        return validation

    checks = list(validation.checks)
    checks.extend(_database_checks(config.runtime.database))
    return DiagnosticReport(tuple(checks))


def _database_checks(database: str) -> list[DiagnosticCheck]:
    if database == ":memory:":
        return [
            DiagnosticCheck(
                "database",
                DiagnosticStatus.WARN,
                "runtime uses an in-memory SQLite database; state will not survive restart",
            )
        ]

    path = Path(database).expanduser()
    parent = path.parent if path.parent != Path("") else Path(".")
    checks: list[DiagnosticCheck] = []
    writable_parent = _nearest_existing_parent(parent)
    if not os.access(writable_parent, os.W_OK):
        checks.append(
            DiagnosticCheck(
                "database-path",
                DiagnosticStatus.FAIL,
                f"database parent is not writable: {writable_parent}",
                {"database": str(path)},
            )
        )
    else:
        checks.append(
            DiagnosticCheck(
                "database-path",
                DiagnosticStatus.PASS,
                f"database location is writable: {writable_parent}",
                {"database": str(path)},
            )
        )

    if not path.exists():
        checks.append(
            DiagnosticCheck(
                "database",
                DiagnosticStatus.WARN,
                "database does not exist yet; it will be initialized on first run",
                {"database": str(path)},
            )
        )
        return checks
    if not path.is_file():
        checks.append(
            DiagnosticCheck(
                "database",
                DiagnosticStatus.FAIL,
                "database path exists but is not a regular file",
                {"database": str(path)},
            )
        )
        return checks

    uri = _sqlite_uri(path, mode="ro")
    try:
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        quick = connection.execute("PRAGMA quick_check").fetchone()
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        tables = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        checks.append(
            DiagnosticCheck(
                "database",
                DiagnosticStatus.FAIL,
                f"SQLite diagnostic failed: {exc}",
                {"database": str(path)},
            )
        )
        return checks
    finally:
        try:
            connection.close()
        except (UnboundLocalError, sqlite3.Error):
            pass

    quick_value = str(quick[0]) if quick is not None else "unknown"
    if quick_value.lower() == "ok":
        checks.append(
            DiagnosticCheck(
                "database-integrity",
                DiagnosticStatus.PASS,
                "SQLite quick_check passed",
            )
        )
    else:
        checks.append(
            DiagnosticCheck(
                "database-integrity",
                DiagnosticStatus.FAIL,
                f"SQLite quick_check returned: {quick_value}",
            )
        )

    required = {"signals", "checkpoints", "source_health", "delivery_sinks", "deliveries"}
    missing = sorted(required - tables)
    if missing:
        checks.append(
            DiagnosticCheck(
                "database-schema",
                DiagnosticStatus.WARN,
                f"database is missing Stream tables: {', '.join(missing)}; initialization/migration is required",
                {"user_version": user_version},
            )
        )
    else:
        checks.append(
            DiagnosticCheck(
                "database-schema",
                DiagnosticStatus.PASS,
                "required Stream tables are present",
                {"user_version": user_version},
            )
        )
    return checks


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate
