from __future__ import annotations

from pathlib import Path

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
    """SQLite store guarded by explicit forward-only schema migrations."""

    def __init__(self, path: str | Path = "signals.db") -> None:
        try:
            super().__init__(path)
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
