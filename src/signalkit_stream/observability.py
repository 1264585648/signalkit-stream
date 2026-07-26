from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
from typing import Sequence

from signalkit_stream.maintenance import verify_database
from signalkit_stream.sqlite_ops import _sqlite_uri


@dataclass(slots=True, frozen=True)
class SourceStatusSnapshot:
    source_key: str
    status: str
    consecutive_failures: int
    total_runs: int
    total_events: int
    last_attempt_at: str | None
    last_success_at: str | None
    last_error: str | None


@dataclass(slots=True, frozen=True)
class SinkStatusSnapshot:
    sink_key: str
    enabled: bool
    pending: int = 0
    failed: int = 0
    dead: int = 0
    delivered: int = 0
    attempts: int = 0
    last_failure_at: str | None = None
    last_error: str | None = None


@dataclass(slots=True, frozen=True)
class StreamSnapshot:
    database: str
    collected_at: str
    signals_total: int
    schema_version: int
    supported_schema_version: int
    schema_status: str
    sources: tuple[SourceStatusSnapshot, ...]
    sinks: tuple[SinkStatusSnapshot, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "database": self.database,
            "collected_at": self.collected_at,
            "signals_total": self.signals_total,
            "schema_version": self.schema_version,
            "supported_schema_version": self.supported_schema_version,
            "schema_status": self.schema_status,
            "sources": [asdict(source) for source in self.sources],
            "sinks": [asdict(sink) for sink in self.sinks],
        }


def read_snapshot(path: str | Path) -> StreamSnapshot:
    """Read an operational snapshot without mutating Stream state."""

    database = Path(path).expanduser()
    verification = verify_database(database)
    uri = _sqlite_uri(database, mode="ro")
    connection = sqlite3.connect(uri, uri=True, timeout=0.5)
    connection.row_factory = sqlite3.Row
    try:
        signals_total = _scalar(connection, "SELECT COUNT(*) FROM signals")
        sources = _read_sources(connection)
        sinks = _read_sinks(connection)
    finally:
        connection.close()

    return StreamSnapshot(
        database=str(database),
        collected_at=datetime.now(UTC).isoformat(),
        signals_total=signals_total,
        schema_version=verification.schema_version,
        supported_schema_version=verification.supported_schema_version,
        schema_status=verification.schema_status,
        sources=tuple(sources),
        sinks=tuple(sinks),
    )


def _read_sources(connection: sqlite3.Connection) -> list[SourceStatusSnapshot]:
    if not _table_exists(connection, "source_health"):
        return []
    rows = connection.execute(
        """
        SELECT source_key, status, consecutive_failures, total_runs, total_events,
               last_attempt_at, last_success_at, last_error
        FROM source_health
        ORDER BY source_key
        """
    ).fetchall()
    return [
        SourceStatusSnapshot(
            source_key=str(row["source_key"]),
            status=str(row["status"]),
            consecutive_failures=int(row["consecutive_failures"]),
            total_runs=int(row["total_runs"]),
            total_events=int(row["total_events"]),
            last_attempt_at=(
                str(row["last_attempt_at"]) if row["last_attempt_at"] else None
            ),
            last_success_at=(
                str(row["last_success_at"]) if row["last_success_at"] else None
            ),
            last_error=str(row["last_error"]) if row["last_error"] else None,
        )
        for row in rows
    ]


def _read_sinks(connection: sqlite3.Connection) -> list[SinkStatusSnapshot]:
    if not _table_exists(connection, "delivery_sinks"):
        return []

    delivery_table = _table_exists(connection, "deliveries")
    if delivery_table:
        rows = connection.execute(
            """
            SELECT ds.sink_key, ds.enabled,
                   COALESCE(SUM(CASE WHEN d.status = 'pending' THEN 1 ELSE 0 END), 0) AS pending,
                   COALESCE(SUM(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                   COALESCE(SUM(CASE WHEN d.status = 'dead' THEN 1 ELSE 0 END), 0) AS dead,
                   COALESCE(SUM(CASE WHEN d.status = 'delivered' THEN 1 ELSE 0 END), 0) AS delivered,
                   COALESCE(SUM(d.attempts), 0) AS attempts
            FROM delivery_sinks AS ds
            LEFT JOIN deliveries AS d ON d.sink_key = ds.sink_key
            GROUP BY ds.sink_key, ds.enabled
            ORDER BY ds.sink_key
            """
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT sink_key, enabled, 0 AS pending, 0 AS failed,
                   0 AS dead, 0 AS delivered, 0 AS attempts
            FROM delivery_sinks
            ORDER BY sink_key
            """
        ).fetchall()

    result: list[SinkStatusSnapshot] = []
    for row in rows:
        sink_key = str(row["sink_key"])
        failure_at: str | None = None
        error: str | None = None
        if delivery_table:
            failure = connection.execute(
                """
                SELECT updated_at, last_error
                FROM deliveries
                WHERE sink_key = ?
                  AND status IN ('failed', 'dead')
                  AND last_error IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (sink_key,),
            ).fetchone()
            if failure is not None:
                failure_at = str(failure["updated_at"]) if failure["updated_at"] else None
                error = str(failure["last_error"]) if failure["last_error"] else None

        result.append(
            SinkStatusSnapshot(
                sink_key=sink_key,
                enabled=bool(row["enabled"]),
                pending=int(row["pending"]),
                failed=int(row["failed"]),
                dead=int(row["dead"]),
                delivered=int(row["delivered"]),
                attempts=int(row["attempts"]),
                last_failure_at=failure_at,
                last_error=error,
            )
        )
    return result


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _scalar(connection: sqlite3.Connection, sql: str) -> int:
    try:
        row = connection.execute(sql).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row[0]) if row is not None else 0


def format_snapshot(snapshot: StreamSnapshot, *, output_format: str = "table") -> str:
    if output_format == "json":
        return json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2)
    if output_format == "prometheus":
        return _prometheus(snapshot)
    if output_format != "table":
        raise ValueError("output_format must be 'table', 'json', or 'prometheus'")

    lines = [
        f"signals={snapshot.signals_total} schema={snapshot.schema_version}/"
        f"{snapshot.supported_schema_version} ({snapshot.schema_status})"
    ]
    if snapshot.sources:
        lines.append("")
        lines.append(
            f"{'SOURCE':32} {'STATUS':14} {'FAILS':7} {'RUNS':7} {'EVENTS':8} LAST SUCCESS"
        )
        lines.append("-" * 100)
        for source in snapshot.sources:
            lines.append(
                f"{source.source_key[:32]:32} {source.status[:14]:14} "
                f"{source.consecutive_failures:<7} {source.total_runs:<7} "
                f"{source.total_events:<8} {source.last_success_at or '-'}"
            )
            if source.last_error:
                lines.append(f"  last_error: {source.last_error}")
    if snapshot.sinks:
        lines.append("")
        lines.append(
            f"{'SINK':24} {'ENABLED':8} {'PENDING':8} {'FAILED':8} {'DEAD':8} "
            f"{'DELIVERED':10} {'ATTEMPTS':8}"
        )
        lines.append("-" * 96)
        for sink in snapshot.sinks:
            lines.append(
                f"{sink.sink_key[:24]:24} {str(sink.enabled).lower():8} {sink.pending:<8} "
                f"{sink.failed:<8} {sink.dead:<8} {sink.delivered:<10} {sink.attempts:<8}"
            )
            if sink.last_error:
                lines.append(f"  last_error: {sink.last_error}")
    return "\n".join(lines)


def _prometheus(snapshot: StreamSnapshot) -> str:
    current = 1 if snapshot.schema_status == "current" else 0
    lines = [
        "# HELP signalkit_signals_total Normalized signals currently stored.",
        "# TYPE signalkit_signals_total gauge",
        f"signalkit_signals_total {snapshot.signals_total}",
        "# HELP signalkit_database_schema_version Persistent database schema version.",
        "# TYPE signalkit_database_schema_version gauge",
        f"signalkit_database_schema_version {snapshot.schema_version}",
        "# HELP signalkit_database_schema_supported_version Schema version supported by this build.",
        "# TYPE signalkit_database_schema_supported_version gauge",
        f"signalkit_database_schema_supported_version {snapshot.supported_schema_version}",
        "# HELP signalkit_database_schema_current Whether database schema is current for this build.",
        "# TYPE signalkit_database_schema_current gauge",
        f"signalkit_database_schema_current {current}",
        "# HELP signalkit_source_status Current persisted source health status.",
        "# TYPE signalkit_source_status gauge",
        "# HELP signalkit_source_consecutive_failures Consecutive source failures.",
        "# TYPE signalkit_source_consecutive_failures gauge",
        "# HELP signalkit_source_runs_total Persisted source run count.",
        "# TYPE signalkit_source_runs_total counter",
        "# HELP signalkit_source_events_total Persisted source event count.",
        "# TYPE signalkit_source_events_total counter",
    ]
    for source in snapshot.sources:
        source_label = _label(source.source_key)
        status_label = _label(source.status)
        lines.append(
            f'signalkit_source_status{{source="{source_label}",status="{status_label}"}} 1'
        )
        lines.append(
            f'signalkit_source_consecutive_failures{{source="{source_label}"}} '
            f"{source.consecutive_failures}"
        )
        lines.append(
            f'signalkit_source_runs_total{{source="{source_label}"}} {source.total_runs}'
        )
        lines.append(
            f'signalkit_source_events_total{{source="{source_label}"}} {source.total_events}'
        )

    lines.extend(
        [
            "# HELP signalkit_sink_enabled Whether a configured sink is enabled.",
            "# TYPE signalkit_sink_enabled gauge",
            "# HELP signalkit_deliveries Delivery rows by sink and status.",
            "# TYPE signalkit_deliveries gauge",
            "# HELP signalkit_delivery_attempts_total Persisted delivery attempt count.",
            "# TYPE signalkit_delivery_attempts_total counter",
        ]
    )
    for sink in snapshot.sinks:
        sink_label = _label(sink.sink_key)
        lines.append(
            f'signalkit_sink_enabled{{sink="{sink_label}"}} {1 if sink.enabled else 0}'
        )
        for status, value in (
            ("pending", sink.pending),
            ("failed", sink.failed),
            ("dead", sink.dead),
            ("delivered", sink.delivered),
        ):
            lines.append(
                f'signalkit_deliveries{{sink="{sink_label}",status="{status}"}} {value}'
            )
        lines.append(
            f'signalkit_delivery_attempts_total{{sink="{sink_label}"}} {sink.attempts}'
        )
    return "\n".join(lines) + "\n"


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m signalkit_stream.observability")
    parser.add_argument("database", nargs="?", default="signals.db")
    parser.add_argument("--format", choices=["table", "json", "prometheus"], default="table")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        snapshot = read_snapshot(args.database)
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(json.dumps({"error": str(exc), "database": args.database}, ensure_ascii=False))
        return 1
    print(
        format_snapshot(snapshot, output_format=args.format),
        end=None if args.format == "prometheus" else "\n",
    )
    return 0 if snapshot.schema_status == "current" else 1


if __name__ == "__main__":
    raise SystemExit(main())
