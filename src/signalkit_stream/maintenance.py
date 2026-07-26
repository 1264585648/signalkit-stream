from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Sequence

from signalkit_stream.migrations import (
    DATABASE_SCHEMA_VERSION,
    DatabaseSchemaError,
    get_database_schema_version,
    validate_database_schema,
)
from signalkit_stream.sqlite_ops import _sqlite_uri


@dataclass(slots=True, frozen=True)
class BackupResult:
    source: str
    destination: str
    pages: int
    created_at: str
    quick_check: str
    schema_version: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class VerifyResult:
    database: str
    quick_check: str
    page_count: int
    page_size: int
    schema_version: int
    supported_schema_version: int
    schema_status: str
    schema_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.quick_check.lower() == "ok" and self.schema_status == "current"

    def to_dict(self) -> dict[str, object]:
        return {**asdict(self), "ok": self.ok}


def backup_database(
    source: str | Path,
    destination: str | Path,
    *,
    overwrite: bool = False,
) -> BackupResult:
    """Create a consistent SQLite backup and atomically publish it at destination.

    The source is opened read-only and copied through SQLite's backup API. The copy is
    written to a temporary sibling first, verified with ``PRAGMA quick_check``, and only
    then atomically replaces the requested destination. Existing backups therefore remain
    intact if a new backup fails before verification.
    """

    source_path = Path(source).expanduser()
    destination_path = Path(destination).expanduser()
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(source_path)
    if source_path.resolve() == destination_path.resolve():
        raise ValueError("backup destination must differ from source database")
    if destination_path.exists() and destination_path.is_dir():
        raise IsADirectoryError(destination_path)
    if destination_path.exists() and not overwrite:
        raise FileExistsError(destination_path)
    if not destination_path.parent.exists():
        raise FileNotFoundError(destination_path.parent)

    source_uri = _sqlite_uri(source_path, mode="ro")
    temp_path: Path | None = None
    source_connection = sqlite3.connect(source_uri, uri=True, timeout=1.0)
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{destination_path.name}.",
            suffix=".tmp",
            dir=destination_path.parent,
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)

        destination_connection = sqlite3.connect(temp_path)
        try:
            source_connection.backup(destination_connection)
            destination_connection.commit()
            quick_check = _quick_check(destination_connection)
            if quick_check.lower() != "ok":
                raise RuntimeError(f"backup integrity check failed: {quick_check}")
            pages = int(destination_connection.execute("PRAGMA page_count").fetchone()[0])
            schema_version = get_database_schema_version(destination_connection)
        finally:
            destination_connection.close()

        os.replace(temp_path, destination_path)
        temp_path = None
    finally:
        source_connection.close()
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    return BackupResult(
        source=str(source_path),
        destination=str(destination_path),
        pages=pages,
        created_at=datetime.now(UTC).isoformat(),
        quick_check=quick_check,
        schema_version=schema_version,
    )


def verify_database(path: str | Path) -> VerifyResult:
    """Inspect SQLite integrity and SignalKit schema compatibility without mutation."""

    database = Path(path).expanduser()
    if not database.exists() or not database.is_file():
        raise FileNotFoundError(database)

    uri = _sqlite_uri(database, mode="ro")
    connection = sqlite3.connect(uri, uri=True, timeout=1.0)
    try:
        quick_check = _quick_check(connection)
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        schema_version = get_database_schema_version(connection)
        schema_status, schema_error = _schema_status(connection, schema_version)
    finally:
        connection.close()

    return VerifyResult(
        database=str(database),
        quick_check=quick_check,
        page_count=page_count,
        page_size=page_size,
        schema_version=schema_version,
        supported_schema_version=DATABASE_SCHEMA_VERSION,
        schema_status=schema_status,
        schema_error=schema_error,
    )


def _schema_status(connection: sqlite3.Connection, version: int) -> tuple[str, str | None]:
    if version < DATABASE_SCHEMA_VERSION:
        return "migration_required", None
    if version > DATABASE_SCHEMA_VERSION:
        return "future", (
            f"database schema version {version} is newer than supported version "
            f"{DATABASE_SCHEMA_VERSION}"
        )
    try:
        validate_database_schema(connection)
    except DatabaseSchemaError as exc:
        return "invalid", str(exc)
    return "current", None


def _quick_check(connection: sqlite3.Connection) -> str:
    rows = connection.execute("PRAGMA quick_check").fetchall()
    if not rows:
        return "no result"
    return "; ".join(str(row[0]) for row in rows)


def _format(payload: object, *, output_format: str) -> str:
    if output_format == "json":
        if hasattr(payload, "to_dict"):
            payload = payload.to_dict()  # type: ignore[assignment,union-attr]
        return json.dumps(payload, ensure_ascii=False, indent=2)
    if isinstance(payload, BackupResult):
        return (
            f"backup: {payload.source} -> {payload.destination}\n"
            f"pages: {payload.pages}\nquick_check: {payload.quick_check}\n"
            f"schema_version: {payload.schema_version}"
        )
    if isinstance(payload, VerifyResult):
        lines = [
            f"database: {payload.database}",
            f"quick_check: {payload.quick_check}",
            f"pages: {payload.page_count}",
            f"page_size: {payload.page_size}",
            f"schema: {payload.schema_version}/{payload.supported_schema_version} "
            f"({payload.schema_status})",
        ]
        if payload.schema_error:
            lines.append(f"schema_error: {payload.schema_error}")
        return "\n".join(lines)
    return str(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m signalkit_stream.maintenance")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup = subparsers.add_parser("backup", help="Create a consistent SQLite backup")
    backup.add_argument("source")
    backup.add_argument("destination")
    backup.add_argument("--overwrite", action="store_true")
    backup.add_argument("--format", choices=["table", "json"], default="table")

    verify = subparsers.add_parser(
        "verify",
        help="Run SQLite integrity and SignalKit schema compatibility checks",
    )
    verify.add_argument("database")
    verify.add_argument("--format", choices=["table", "json"], default="table")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "backup":
            result = backup_database(args.source, args.destination, overwrite=args.overwrite)
        else:
            result = verify_database(args.database)
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 1

    print(_format(result, output_format=args.format))
    if isinstance(result, VerifyResult) and not result.ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
