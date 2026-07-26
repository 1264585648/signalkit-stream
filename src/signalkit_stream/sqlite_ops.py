from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3


@dataclass(slots=True, frozen=True)
class WriteLockProbe:
    database: str
    available: bool
    journal_mode: str | None
    busy_timeout_ms: int | None
    error: str | None = None


def probe_write_lock(
    path: str | Path,
    *,
    timeout: float = 0.05,
) -> WriteLockProbe:
    """Probe whether SQLite can acquire an immediate write transaction.

    The probe opens an existing database in read/write mode, issues ``BEGIN IMMEDIATE``,
    and immediately rolls the transaction back. No application rows or schema objects are
    modified. A busy/locked result is returned as data so diagnostics can warn without
    treating a currently running writer as database corruption.
    """

    if timeout < 0:
        raise ValueError("SQLite lock probe timeout must be >= 0")

    database = Path(path).expanduser()
    uri = f"file:{database.resolve().as_posix()}?mode=rw"
    connection: sqlite3.Connection | None = None
    journal_mode: str | None = None
    busy_timeout_ms: int | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=timeout)
        journal = connection.execute("PRAGMA journal_mode").fetchone()
        journal_mode = str(journal[0]) if journal is not None else None
        busy = connection.execute("PRAGMA busy_timeout").fetchone()
        busy_timeout_ms = int(busy[0]) if busy is not None else None
        connection.execute("BEGIN IMMEDIATE")
        connection.rollback()
        return WriteLockProbe(
            database=str(database),
            available=True,
            journal_mode=journal_mode,
            busy_timeout_ms=busy_timeout_ms,
        )
    except sqlite3.OperationalError as exc:
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
        text = str(exc)
        if "locked" in text.lower() or "busy" in text.lower():
            return WriteLockProbe(
                database=str(database),
                available=False,
                journal_mode=journal_mode,
                busy_timeout_ms=busy_timeout_ms,
                error=text,
            )
        raise
    finally:
        if connection is not None:
            connection.close()


__all__ = ["WriteLockProbe", "probe_write_lock"]
