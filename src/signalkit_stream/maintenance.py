from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
from typing import Sequence


@dataclass(slots=True, frozen=True)
class BackupResult:
    source: str
    destination: str
    pages: int
    created_at: str
    quick_check: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class VerifyResult:
    database: str
    quick_check: str
    page_count: int
    page_size: int

    @property
    def ok(self) -> bool:
        return self.quick_check.lower() == "ok"

    def to_dict(self) -> dict[str, object]:
        return {
            **asdict(self),
            "ok": self.ok,
        }


def backup_database(
    source: str | Path,
    destination: str | Path,
    *,
    overwrite: bool = False,
) -> BackupResult:
    source_path = Path(source)
    destination_path = Path(destination)
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(source_path)
    if source_path.resolve() == destination_path.resolve():
        raise ValueError("backup destination must differ from source database")
    if destination_path.exists() and not overwrite:
        raise FileExistsError(destination_path)
    if not destination_path.parent.exists():
        raise FileNotFoundError(destination_path.parent)
    if destination_path.exists() and destination_path.is_dir():
        raise IsADirectoryError(destination_path)

    source_connection = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True, timeout=1.0)
    try:
        destination_connection = sqlite3.connect(destination_path)
        try:
            source_connection.backup(destination_connection)
            destination_connection.commit()
            quick_check = _quick_check(destination_connection)
            pages = int(destination_connection.execute("PRAGMA page_count").fetchone()[0])
        finally:
            destination_connection.close()
    finally:
        source_connection.close()

    if quick_check.lower() != "ok":
        raise RuntimeError(f"backup integrity check failed: {quick_check}")
    return BackupResult(
        source=str(source_path),
        destination=str(destination_path),
        pages=pages,
        created_at=datetime.now(UTC).isoformat(),
        quick_check=quick_check,
    )


def verify_database(path: str | Path) -> VerifyResult:
    database = Path(path)
    if not database.exists() or not database.is_file():
        raise FileNotFoundError(database)
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1.0)
    try:
        quick_check = _quick_check(connection)
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    finally:
        connection.close()
    return VerifyResult(
        database=str(database),
        quick_check=quick_check,
        page_count=page_count,
        page_size=page_size,
    )


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
            f"pages: {payload.pages}\nquick_check: {payload.quick_check}"
        )
    if isinstance(payload, VerifyResult):
        return (
            f"database: {payload.database}\nquick_check: {payload.quick_check}\n"
            f"pages: {payload.page_count}\npage_size: {payload.page_size}"
        )
    return str(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m signalkit_stream.maintenance")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup = subparsers.add_parser("backup", help="Create a consistent SQLite backup")
    backup.add_argument("source")
    backup.add_argument("destination")
    backup.add_argument("--overwrite", action="store_true")
    backup.add_argument("--format", choices=["table", "json"], default="table")

    verify = subparsers.add_parser("verify", help="Run SQLite quick_check")
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
