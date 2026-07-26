from __future__ import annotations

from pathlib import Path
import sqlite3

from signalkit_stream._diagnostics_impl import (
    DiagnosticCheck,
    DiagnosticReport,
    DiagnosticStatus,
    doctor as _doctor,
    validate_config_file,
    validate_stream_config,
)
from signalkit_stream.config import load_config
from signalkit_stream.migrations import DATABASE_SCHEMA_VERSION
from signalkit_stream.registry import CollectorRegistry
from signalkit_stream.sinks import SinkRegistry
from signalkit_stream.sqlite_ops import probe_write_lock


def doctor(
    path: str | Path,
    *,
    collector_registry: CollectorRegistry | None = None,
    sink_registry: SinkRegistry | None = None,
) -> DiagnosticReport:
    """Run local diagnostics and report persistent-schema/write-lock compatibility."""

    report = _doctor(
        path,
        collector_registry=collector_registry,
        sink_registry=sink_registry,
    )
    report = _annotate_database_schema(report)
    try:
        config = load_config(path)
    except (OSError, ValueError):
        return report
    return _append_write_lock_probe(report, config.runtime.database)


def _annotate_database_schema(report: DiagnosticReport) -> DiagnosticReport:
    checks: list[DiagnosticCheck] = []
    for check in report.checks:
        if check.name != "database-schema":
            checks.append(check)
            continue

        version = check.details.get("user_version")
        if not isinstance(version, int):
            checks.append(check)
            continue

        details = dict(check.details)
        details["supported_version"] = DATABASE_SCHEMA_VERSION
        if version > DATABASE_SCHEMA_VERSION:
            checks.append(
                DiagnosticCheck(
                    check.name,
                    DiagnosticStatus.FAIL,
                    f"database schema version {version} is newer than supported version "
                    f"{DATABASE_SCHEMA_VERSION}; upgrade SignalKit Stream before startup",
                    details,
                )
            )
        elif version < DATABASE_SCHEMA_VERSION:
            checks.append(
                DiagnosticCheck(
                    check.name,
                    DiagnosticStatus.WARN,
                    f"database schema version {version} requires forward migration to "
                    f"{DATABASE_SCHEMA_VERSION}; migration runs automatically on startup",
                    details,
                )
            )
        elif check.status is not DiagnosticStatus.PASS:
            checks.append(
                DiagnosticCheck(
                    check.name,
                    DiagnosticStatus.FAIL,
                    "database claims the current schema version but required Stream objects "
                    "are missing; restore from backup or repair before startup",
                    details,
                )
            )
        else:
            checks.append(
                DiagnosticCheck(
                    check.name,
                    DiagnosticStatus.PASS,
                    f"database schema version {version} is current",
                    details,
                )
            )
    return DiagnosticReport(tuple(checks))


def _append_write_lock_probe(report: DiagnosticReport, database: str) -> DiagnosticReport:
    if database == ":memory:":
        return report
    path = Path(database).expanduser()
    if not path.exists() or not path.is_file():
        return report

    checks = list(report.checks)
    try:
        probe = probe_write_lock(path)
    except (OSError, sqlite3.Error, ValueError) as exc:
        checks.append(
            DiagnosticCheck(
                "database-write-lock",
                DiagnosticStatus.FAIL,
                f"unable to probe SQLite write availability: {exc}",
                {"database": str(path)},
            )
        )
        return DiagnosticReport(tuple(checks))

    details = {
        "database": probe.database,
        "journal_mode": probe.journal_mode,
        "busy_timeout_ms": probe.busy_timeout_ms,
    }
    if probe.available:
        checks.append(
            DiagnosticCheck(
                "database-write-lock",
                DiagnosticStatus.PASS,
                "SQLite immediate write-lock probe succeeded",
                details,
            )
        )
    else:
        details["error"] = probe.error
        checks.append(
            DiagnosticCheck(
                "database-write-lock",
                DiagnosticStatus.WARN,
                "SQLite write lock is currently busy; another writer may be active. "
                "Use one Stream writer per database and retry diagnostics after writes settle.",
                details,
            )
        )
    return DiagnosticReport(tuple(checks))


__all__ = [
    "DiagnosticCheck",
    "DiagnosticReport",
    "DiagnosticStatus",
    "doctor",
    "validate_config_file",
    "validate_stream_config",
]
