from __future__ import annotations

from pathlib import Path
import sqlite3

from signalkit_stream._storage_impl import (
    Checkpoint,
    DeliveryRecord,
    SignalStore,
    SourceHealth,
    SQLiteSignalStore as _UnversionedSQLiteSignalStore,
    StoreWriteResult,
)
from signalkit_stream.migrations import get_database_schema_version, migrate_database


class SQLiteSignalStore(_UnversionedSQLiteSignalStore):
    """SQLite store guarded by explicit forward-only schema migrations.

    ``timeout`` controls how long SQLite waits for a conflicting database lock before
    raising ``sqlite3.OperationalError``. The default matches Python's normal SQLite
    connection behavior while allowing deterministic lock/busy tests and deployments
    with a deliberately shorter or longer contention budget.
    """

    def __init__(
        self,
        path: str | Path = "signals.db",
        *,
        timeout: float = 5.0,
    ) -> None:
        if timeout < 0:
            raise ValueError("SQLite timeout must be >= 0")
        self.path = Path(path)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._connection = sqlite3.connect(self.path, timeout=timeout)
            self._connection.row_factory = sqlite3.Row
            self._initialize()
        except Exception:
            connection = getattr(self, "_connection", None)
            if connection is not None:
                connection.close()
            raise

    def _initialize(self) -> None:
        migrate_database(self._connection)

    @property
    def database_schema_version(self) -> int:
        return get_database_schema_version(self._connection)


__all__ = [
    "Checkpoint",
    "DeliveryRecord",
    "SQLiteSignalStore",
    "SignalStore",
    "SourceHealth",
    "StoreWriteResult",
]
