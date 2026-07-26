from __future__ import annotations

from pathlib import Path

from signalkit_stream._diagnostics_impl import (
    DiagnosticCheck,
    DiagnosticReport,
    DiagnosticStatus,
    doctor as _doctor,
    validate_config_file,
    validate_stream_config,
)
from signalkit_stream.migrations import DATABASE_SCHEMA_VERSION
from signalkit_stream.registry import CollectorRegistry
from signalkit_stream.sinks import SinkRegistry


def doctor(
    path: str | Path,
    *,
    collector_registry: CollectorRegistry | None = None,
    sink_registry: SinkRegistry | None = None,
) -> DiagnosticReport:
    """Run local diagnostics and report persistent-schema compatibility."""

    report = _doctor(
        path,
        collector_registry=collector_registry,
        sink_registry=sink_registry,
    )
    return _annotate_database_schema(report)


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


__all__ = [
    "DiagnosticCheck",
    "DiagnosticReport",
    "DiagnosticStatus",
    "doctor",
    "validate_config_file",
    "validate_stream_config",
]
