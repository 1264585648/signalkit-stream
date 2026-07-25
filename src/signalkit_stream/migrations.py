from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
from typing import Callable, Sequence

from signalkit_stream.storage import SQLiteSignalStore

PERSISTENCE_SCHEMA_VERSION = 1


@dataclass(slots=True, frozen=True)
class MigrationRecord:
    version: int
    name: str
    applied_at: str


@dataclass(slots=True, frozen=True)
class SchemaStatus:
    database: str
    exists: bool
    current_version: int
    target_version: int
    migrations: tuple[MigrationRecord, ...]

    @property
    def current(self) -> bool:
        return self.current_version == self.target_version

    def to_dict(self) -> dict[str, object]:
        return {
            "database": self.database,
            "exists": self.exists,
            "current_version": self.current_version,
            "target_version": self.target_version,
            "current": self.current,
            "migrations": [asdict(item) for item in self.migrations],
        }


Migration = Callable[[sqlite3.Connection], None]


def migrate_database(path: str | Path, *, target_version: int = PERSISTENCE_SCHEMA_VERSION) -> SchemaStatus:
    """Bring a SignalKit SQLite database to a declared persistent schema version.

    Version 1 adopts the current storage schema after running SQLiteSignalStore's
    compatibility initialization. Future releases can append forward-only migrations
    without rewriting the baseline migration record.
    """

    database = Path(path)
    if target_version < 0 or target_version > PERSISTENCE_SCHEMA_VERSION:
        raise ValueError(
            f"target_version must be between 0 and {PERSISTENCE_SCHEMA_VERSION}"
        )
    if database.parent != Path("") and not database.parent.exists():
        raise FileNotFoundError(database.parent)

    # Reuse the production store initializer to normalize any pre-versioning legacy
    # schema before the explicit version ledger adopts it as baseline v1.
    if target_version >= 1:
        with SQLiteSignalStore(database):
            pass

    connection = sqlite3.connect(database)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        _ensure_ledger(connection)
        current = _current_version(connection)
        if current > PERSISTENCE_SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema version {current} is newer than this SignalKit build "
                f"({PERSISTENCE_SCHEMA_VERSION})"
            )
        if current > target_version:
            raise RuntimeError(
                f"database schema version {current} is newer than requested target {target_version}; "
                "downgrades are not supported"
            )

        migrations: dict[int, tuple[str, Migration]] = {
            1: ("adopt_current_storage_schema", _migration_v1),
        }
        for version in range(current + 1, target_version + 1):
            name, migration = migrations[version]
            with connection:
                migration(connection)
                _record_migration(connection, version, name)
    finally:
        connection.close()
    return schema_status(database)


def schema_status(path: str | Path) -> SchemaStatus:
    database = Path(path)
    if not database.exists():
        return SchemaStatus(
            database=str(database),
            exists=False,
            current_version=0,
            target_version=PERSISTENCE_SCHEMA_VERSION,
            migrations=(),
        )

    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        if not _table_exists(connection, "schema_migrations"):
            return SchemaStatus(
                database=str(database),
                exists=True,
                current_version=0,
                target_version=PERSISTENCE_SCHEMA_VERSION,
                migrations=(),
            )
        rows = connection.execute(
            "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
        ).fetchall()
    finally:
        connection.close()

    migrations = tuple(
        MigrationRecord(
            version=int(row["version"]),
            name=str(row["name"]),
            applied_at=str(row["applied_at"]),
        )
        for row in rows
    )
    current = migrations[-1].version if migrations else 0
    return SchemaStatus(
        database=str(database),
        exists=True,
        current_version=current,
        target_version=PERSISTENCE_SCHEMA_VERSION,
        migrations=migrations,
    )


def _migration_v1(connection: sqlite3.Connection) -> None:
    expected: dict[str, set[str]] = {
        "signals": {"id", "source", "kind"},
        "checkpoints": {"source_key", "cursor_json"},
        "source_health": {"source_key", "status"},
        "delivery_sinks": {"sink_key"},
        "deliveries": {"sink_key", "event_id", "status"},
    }
    for table, required_columns in expected.items():
        if not _table_exists(connection, table):
            raise RuntimeError(f"cannot adopt schema: required table {table!r} is missing")
        columns = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
        }
        missing = required_columns - columns
        if missing:
            raise RuntimeError(
                f"cannot adopt schema: table {table!r} is missing columns: "
                + ", ".join(sorted(missing))
            )


def _ensure_ledger(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )
    connection.commit()


def _record_migration(connection: sqlite3.Connection, version: int, name: str) -> None:
    connection.execute(
        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
        (version, name, datetime.now(UTC).isoformat()),
    )


def _current_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def format_status(status: SchemaStatus, *, output_format: str = "table") -> str:
    if output_format == "json":
        return json.dumps(status.to_dict(), ensure_ascii=False, indent=2)
    lines = [
        f"database: {status.database}",
        f"exists: {str(status.exists).lower()}",
        f"schema: {status.current_version}/{status.target_version}",
        f"current: {str(status.current).lower()}",
    ]
    if status.migrations:
        lines.append("migrations:")
        for item in status.migrations:
            lines.append(f"  {item.version}: {item.name} ({item.applied_at})")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m signalkit_stream.migrations")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="Inspect persistent schema version")
    status.add_argument("database", nargs="?", default="signals.db")
    status.add_argument("--format", choices=["table", "json"], default="table")

    migrate = subparsers.add_parser("migrate", help="Apply forward migrations")
    migrate.add_argument("database", nargs="?", default="signals.db")
    migrate.add_argument("--format", choices=["table", "json"], default="table")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "status":
            status = schema_status(args.database)
        else:
            status = migrate_database(args.database)
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        print(json.dumps({"error": str(exc), "database": args.database}, ensure_ascii=False))
        return 1
    print(format_status(status, output_format=args.format))
    return 0 if status.current else 1


if __name__ == "__main__":
    raise SystemExit(main())
