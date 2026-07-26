from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from urllib.parse import quote


def _sqlite_uri(path: Path, *, mode: str) -> str:
    """Build a SQLite ``file:`` URI that survives ``%``, spaces, and UNC paths.

    SQLite percent-decodes URI paths, so an unescaped ``%`` in a directory name resolves
    to a different (usually missing) file. It also treats the text between the first two
    and third slash as a URI authority, so the two leading slashes of a Windows UNC path
    must be escaped by a third one. This module owns the helper because it has no
    first-party imports, so every SQLite entry point can share it without an import cycle.
    """

    text = path.resolve().as_posix()
    if text.startswith("//"):
        text = "//" + text  # UNC //server/share -> ////server/share
    elif text.startswith("/"):
        text = "//" + text  # POSIX /var/lib -> ///var/lib
    else:
        text = "///" + text  # drive-letter C:/data -> ///C:/data
    return f"file:{quote(text, safe='/:')}?mode={mode}"


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
    uri = _sqlite_uri(database, mode="rw")
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
