from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Sequence


@dataclass(slots=True, frozen=True)
class SourceStatusSnapshot:
    source_key: str
    status: str
    consecutive_failures: int
    total_runs: int
    total_events: int
    last_success_at: str | None
    last_error: str | None


@dataclass(slots=True, frozen=True)
class SinkStatusSnapshot:
    sink_key: str
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
    sources: tuple[SourceStatusSnapshot, ...]
    sinks: tuple[SinkStatusSnapshot, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "database": self.database,
            "collected_at": self.collected_at,
            "signals_total": self.signals_total,
            "sources": [asdict(source) for source in self.sources],
            "sinks": [asdict(sink) for sink in self.sinks],
        }


def read_snapshot(path: str | Path) -> StreamSnapshot:
    database = Path(path)
    if not database.exists():
        raise FileNotFoundError(database)
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=0.5)
    connection.row_factory = sqlite3.Row
    try:
        signals_total = _scalar(connection, "SELECT COUNT(*) FROM signals")
        sources = _read_sources(connection)
        sinks = _read_sinks(connection)
    finally:
        connection.close()

    return StreamSnapshot(
        database=str(database),
        collected_at=datetime.now().astimezone().isoformat(),
        signals_total=signals_total,
        sources=tuple(sources),
        sinks=tuple(sinks),
    )


def _read_sources(connection: sqlite3.Connection) -> list[SourceStatusSnapshot]:
    if not _table_exists(connection, "source_health"):
        return []
    rows = connection.execute(
        """
        SELECT source_key, status, consecutive_failures, total_runs, total_events,
               last_success_at, last_error
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
            last_success_at=str(row["last_success_at"]) if row["last_success_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
        )
        for row in rows
    ]


def _read_sinks(connection: sqlite3.Connection) -> list[SinkStatusSnapshot]:
    if not _table_exists(connection, "deliveries"):
        return []
    aggregate_rows = connection.execute(
        """
        SELECT sink_key, status, COUNT(*) AS total, COALESCE(SUM(attempts), 0) AS attempts
        FROM deliveries
        GROUP BY sink_key, status
        ORDER BY sink_key, status
        """
    ).fetchall()
    by_sink: dict[str, dict[str, int]] = {}
    attempts: dict[str, int] = {}
    for row in aggregate_rows:
        sink_key = str(row["sink_key"])
        by_sink.setdefault(sink_key, {})[str(row["status"])] = int(row["total"])
        attempts[sink_key] = attempts.get(sink_key, 0) + int(row["attempts"])

    failures: dict[str, tuple[str | None, str | None]] = {}
    for sink_key in by_sink:
        row = connection.execute(
            """
            SELECT updated_at, last_error
            FROM deliveries
            WHERE sink_key = ? AND status IN ('failed', 'dead') AND last_error IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (sink_key,),
        ).fetchone()
        if row is not None:
            failures[sink_key] = (
                str(row["updated_at"]) if row["updated_at"] else None,
                str(row["last_error"]) if row["last_error"] else None,
            )

    result: list[SinkStatusSnapshot] = []
    for sink_key in sorted(by_sink):
        counts = by_sink[sink_key]
        failure_at, error = failures.get(sink_key, (None, None))
        result.append(
            SinkStatusSnapshot(
                sink_key=sink_key,
                pending=counts.get("pending", 0),
                failed=counts.get("failed", 0),
                dead=counts.get("dead", 0),
                delivered=counts.get("delivered", 0),
                attempts=attempts.get(sink_key, 0),
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

    lines = [f"signals={snapshot.signals_total}"]
    if snapshot.sources:
        lines.append("")
        lines.append(f"{'SOURCE':32} {'STATUS':14} {'FAILS':7} {'RUNS':7} {'EVENTS':8}")
        lines.append("-" * 74)
        for source in snapshot.sources:
            lines.append(
                f"{source.source_key[:32]:32} {source.status[:14]:14} "
                f"{source.consecutive_failures:<7} {source.total_runs:<7} {source.total_events:<8}"
            )
    if snapshot.sinks:
        lines.append("")
        lines.append(f"{'SINK':24} {'PENDING':8} {'FAILED':8} {'DEAD':8} {'DELIVERED':10} {'ATTEMPTS':8}")
        lines.append("-" * 82)
        for sink in snapshot.sinks:
            lines.append(
                f"{sink.sink_key[:24]:24} {sink.pending:<8} {sink.failed:<8} {sink.dead:<8} "
                f"{sink.delivered:<10} {sink.attempts:<8}"
            )
            if sink.last_error:
                lines.append(f"  last_error: {sink.last_error}")
    return "\n".join(lines)


def _prometheus(snapshot: StreamSnapshot) -> str:
    lines = [
        "# HELP signalkit_signals_total Normalized signals currently stored.",
        "# TYPE signalkit_signals_total gauge",
        f"signalkit_signals_total {snapshot.signals_total}",
        "# HELP signalkit_source_status Source health status as one-hot gauges.",
        "# TYPE signalkit_source_status gauge",
        "# HELP signalkit_source_consecutive_failures Consecutive source failures.",
        "# TYPE signalkit_source_consecutive_failures gauge",
        "# HELP signalkit_source_runs_total Persisted source run count.",
        "# TYPE signalkit_source_runs_total counter",
        "# HELP signalkit_source_events_total Persisted source event count.",
        "# TYPE signalkit_source_events_total counter",
    ]
    statuses = ("healthy", "degraded", "circuit_open")
    for source in snapshot.sources:
        source_label = _label(source.source_key)
        for status in statuses:
            lines.append(
                f'signalkit_source_status{{source="{source_label}",status="{status}"}} '
                f'{1 if source.status == status else 0}'
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
            "# HELP signalkit_deliveries Delivery rows by sink and status.",
            "# TYPE signalkit_deliveries gauge",
            "# HELP signalkit_delivery_attempts_total Persisted delivery attempt count.",
            "# TYPE signalkit_delivery_attempts_total counter",
        ]
    )
    for sink in snapshot.sinks:
        sink_label = _label(sink.sink_key)
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
    print(format_snapshot(snapshot, output_format=args.format), end=None if args.format == "prometheus" else "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
