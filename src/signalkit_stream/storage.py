from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import json
import sqlite3

from signalkit_stream.models import SignalEvent


class SQLiteSignalStore:
    """Small local store with id-based deduplication."""

    def __init__(self, path: str | Path = "signals.db") -> None:
        self.path = Path(path)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self._initialize()

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                title TEXT,
                content TEXT NOT NULL,
                author TEXT,
                url TEXT NOT NULL,
                created_at TEXT NOT NULL,
                collected_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_signals_source_created
                ON signals(source, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_signals_kind_created
                ON signals(kind, created_at DESC);
            """
        )
        self._connection.commit()

    def save_many(self, events: Iterable[SignalEvent]) -> int:
        before = self._connection.total_changes
        self._connection.executemany(
            """
            INSERT OR IGNORE INTO signals (
                id, source, kind, title, content, author, url,
                created_at, collected_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event.id,
                    event.source,
                    event.kind.value,
                    event.title,
                    event.content,
                    event.author,
                    event.url,
                    event.created_at.isoformat(),
                    event.collected_at.isoformat(),
                    json.dumps(dict(event.metadata), ensure_ascii=False, sort_keys=True),
                )
                for event in events
            ],
        )
        self._connection.commit()
        return self._connection.total_changes - before

    def list_recent(
        self,
        *,
        limit: int = 100,
        source: str | None = None,
        kind: str | None = None,
    ) -> list[SignalEvent]:
        conditions: list[str] = []
        params: list[object] = []
        if source:
            conditions.append("source = ?")
            params.append(source)
        if kind:
            conditions.append("kind = ?")
            params.append(kind)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(max(0, limit))
        rows = self._connection.execute(
            f"""
            SELECT * FROM signals
            {where}
            ORDER BY created_at DESC, collected_at DESC
            LIMIT ?
            """,  # noqa: S608 - WHERE fragments are fixed strings, values are parameterized.
            params,
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def count(self) -> int:
        row = self._connection.execute("SELECT COUNT(*) AS total FROM signals").fetchone()
        return int(row["total"])

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> SignalEvent:
        return SignalEvent.from_dict(
            {
                "id": row["id"],
                "source": row["source"],
                "kind": row["kind"],
                "title": row["title"],
                "content": row["content"],
                "author": row["author"],
                "url": row["url"],
                "created_at": row["created_at"],
                "collected_at": row["collected_at"],
                "metadata": json.loads(row["metadata_json"]),
            }
        )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteSignalStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
