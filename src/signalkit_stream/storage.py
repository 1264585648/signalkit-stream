from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
import json
import sqlite3

from signalkit_stream.models import SignalEvent
from signalkit_stream.protocol import Cursor


@dataclass(slots=True, frozen=True)
class StoreWriteResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0

    @property
    def changed(self) -> int:
        return self.inserted + self.updated

    def __add__(self, other: StoreWriteResult) -> StoreWriteResult:
        return StoreWriteResult(
            inserted=self.inserted + other.inserted,
            updated=self.updated + other.updated,
            unchanged=self.unchanged + other.unchanged,
        )


@dataclass(slots=True, frozen=True)
class Checkpoint:
    source_key: str
    cursor: Cursor
    updated_at: datetime
    last_success_at: datetime | None = None
    last_error: str | None = None


@dataclass(slots=True, frozen=True)
class SourceHealth:
    source_key: str
    status: str
    updated_at: datetime
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    total_runs: int = 0
    total_events: int = 0


class SignalStore(Protocol):
    def write_many(self, events: Iterable[SignalEvent]) -> StoreWriteResult: ...

    def commit_batch(
        self,
        events: Iterable[SignalEvent],
        *,
        source_key: str,
        cursor: Cursor | None,
    ) -> StoreWriteResult: ...

    def get_checkpoint(self, source_key: str) -> Checkpoint | None: ...

    def record_failure(self, source_key: str, error: str) -> None: ...


class SQLiteSignalStore:
    """SQLite event store with upserts, checkpoints, and source health."""

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
                schema_version INTEGER NOT NULL DEFAULT 1,
                source TEXT NOT NULL,
                source_instance TEXT NOT NULL DEFAULT 'default',
                kind TEXT NOT NULL,
                title TEXT,
                content TEXT NOT NULL,
                author TEXT,
                url TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                collected_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                event_hash TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                source_key TEXT PRIMARY KEY,
                cursor_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_success_at TEXT,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS source_health (
                source_key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_attempt_at TEXT,
                last_success_at TEXT,
                last_error TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                total_runs INTEGER NOT NULL DEFAULT 0,
                total_events INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self._migrate_legacy_signals()
        self._connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_source_created
                ON signals(source, source_instance, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_signals_kind_created
                ON signals(kind, created_at DESC);
            """
        )
        self._connection.commit()

    def _migrate_legacy_signals(self) -> None:
        columns = {
            str(row["name"])
            for row in self._connection.execute("PRAGMA table_info(signals)").fetchall()
        }
        migrations = {
            "schema_version": "ALTER TABLE signals ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1",
            "source_instance": "ALTER TABLE signals ADD COLUMN source_instance TEXT NOT NULL DEFAULT 'default'",
            "updated_at": "ALTER TABLE signals ADD COLUMN updated_at TEXT",
            "event_hash": "ALTER TABLE signals ADD COLUMN event_hash TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in migrations.items():
            if column not in columns:
                self._connection.execute(statement)

    def write_many(self, events: Iterable[SignalEvent]) -> StoreWriteResult:
        event_list = list(events)
        with self._connection:
            return self._write_many(event_list)

    def save_many(self, events: Iterable[SignalEvent]) -> int:
        """Compatibility helper returning the number of inserted/updated rows."""

        return self.write_many(events).changed

    def commit_batch(
        self,
        events: Iterable[SignalEvent],
        *,
        source_key: str,
        cursor: Cursor | None,
    ) -> StoreWriteResult:
        event_list = list(events)
        now = datetime.now(UTC)
        with self._connection:
            result = self._write_many(event_list)
            if cursor is not None:
                self._set_checkpoint(
                    source_key,
                    cursor,
                    updated_at=now,
                    last_success_at=now,
                    last_error=None,
                )
        return result

    def _write_many(self, events: list[SignalEvent]) -> StoreWriteResult:
        inserted = 0
        updated = 0
        unchanged = 0
        for event in events:
            fingerprint = event.fingerprint()
            row = self._connection.execute(
                "SELECT event_hash FROM signals WHERE id = ?",
                (event.id,),
            ).fetchone()
            if row is None:
                self._connection.execute(
                    """
                    INSERT INTO signals (
                        id, schema_version, source, source_instance, kind, title, content,
                        author, url, created_at, updated_at, collected_at, metadata_json, event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._event_params(event, fingerprint),
                )
                inserted += 1
                continue

            existing_hash = str(row["event_hash"] or "")
            if existing_hash == fingerprint:
                unchanged += 1
                continue

            self._connection.execute(
                """
                UPDATE signals SET
                    schema_version = ?, source = ?, source_instance = ?, kind = ?, title = ?,
                    content = ?, author = ?, url = ?, created_at = ?, updated_at = ?,
                    collected_at = ?, metadata_json = ?, event_hash = ?
                WHERE id = ?
                """,
                (
                    event.schema_version,
                    event.source,
                    event.source_instance,
                    event.kind.value,
                    event.title,
                    event.content,
                    event.author,
                    event.url,
                    event.created_at.isoformat(),
                    event.updated_at.isoformat() if event.updated_at else None,
                    event.collected_at.isoformat(),
                    json.dumps(dict(event.metadata), ensure_ascii=False, sort_keys=True),
                    fingerprint,
                    event.id,
                ),
            )
            updated += 1

        return StoreWriteResult(inserted=inserted, updated=updated, unchanged=unchanged)

    @staticmethod
    def _event_params(event: SignalEvent, fingerprint: str) -> tuple[object, ...]:
        return (
            event.id,
            event.schema_version,
            event.source,
            event.source_instance,
            event.kind.value,
            event.title,
            event.content,
            event.author,
            event.url,
            event.created_at.isoformat(),
            event.updated_at.isoformat() if event.updated_at else None,
            event.collected_at.isoformat(),
            json.dumps(dict(event.metadata), ensure_ascii=False, sort_keys=True),
            fingerprint,
        )

    def get_checkpoint(self, source_key: str) -> Checkpoint | None:
        row = self._connection.execute(
            "SELECT * FROM checkpoints WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        if row is None:
            return None
        return Checkpoint(
            source_key=str(row["source_key"]),
            cursor=Cursor.from_json(str(row["cursor_json"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            last_success_at=(
                datetime.fromisoformat(str(row["last_success_at"]))
                if row["last_success_at"]
                else None
            ),
            last_error=str(row["last_error"]) if row["last_error"] else None,
        )

    def set_checkpoint(self, source_key: str, cursor: Cursor) -> None:
        now = datetime.now(UTC)
        with self._connection:
            self._set_checkpoint(
                source_key,
                cursor,
                updated_at=now,
                last_success_at=now,
                last_error=None,
            )

    def _set_checkpoint(
        self,
        source_key: str,
        cursor: Cursor,
        *,
        updated_at: datetime,
        last_success_at: datetime | None,
        last_error: str | None,
    ) -> None:
        if cursor.source_key != source_key:
            raise ValueError(
                f"checkpoint cursor source {cursor.source_key!r} does not match {source_key!r}"
            )
        self._connection.execute(
            """
            INSERT INTO checkpoints (
                source_key, cursor_json, updated_at, last_success_at, last_error
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                cursor_json = excluded.cursor_json,
                updated_at = excluded.updated_at,
                last_success_at = excluded.last_success_at,
                last_error = excluded.last_error
            """,
            (
                source_key,
                cursor.to_json(),
                updated_at.isoformat(),
                last_success_at.isoformat() if last_success_at else None,
                last_error,
            ),
        )

    def record_failure(self, source_key: str, error: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connection:
            existing = self._connection.execute(
                "SELECT cursor_json, last_success_at FROM checkpoints WHERE source_key = ?",
                (source_key,),
            ).fetchone()
            if existing is None:
                cursor = Cursor(source_key=source_key).to_json()
                last_success = None
            else:
                cursor = str(existing["cursor_json"])
                last_success = existing["last_success_at"]
            self._connection.execute(
                """
                INSERT INTO checkpoints (
                    source_key, cursor_json, updated_at, last_success_at, last_error
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    last_error = excluded.last_error
                """,
                (source_key, cursor, now, last_success, error),
            )

    def upsert_source_health(self, health: SourceHealth) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO source_health (
                    source_key, status, updated_at, last_attempt_at, last_success_at,
                    last_error, consecutive_failures, total_runs, total_events
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    last_attempt_at = excluded.last_attempt_at,
                    last_success_at = excluded.last_success_at,
                    last_error = excluded.last_error,
                    consecutive_failures = excluded.consecutive_failures,
                    total_runs = excluded.total_runs,
                    total_events = excluded.total_events
                """,
                (
                    health.source_key,
                    health.status,
                    health.updated_at.isoformat(),
                    health.last_attempt_at.isoformat() if health.last_attempt_at else None,
                    health.last_success_at.isoformat() if health.last_success_at else None,
                    health.last_error,
                    health.consecutive_failures,
                    health.total_runs,
                    health.total_events,
                ),
            )

    def get_source_health(self, source_key: str) -> SourceHealth | None:
        row = self._connection.execute(
            "SELECT * FROM source_health WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        return self._row_to_health(row) if row is not None else None

    def list_source_health(self) -> list[SourceHealth]:
        rows = self._connection.execute(
            "SELECT * FROM source_health ORDER BY source_key"
        ).fetchall()
        return [self._row_to_health(row) for row in rows]

    def get(self, event_id: str) -> SignalEvent | None:
        row = self._connection.execute(
            "SELECT * FROM signals WHERE id = ?",
            (event_id,),
        ).fetchone()
        return self._row_to_event(row) if row is not None else None

    def exists(self, event_id: str) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM signals WHERE id = ?",
            (event_id,),
        ).fetchone()
        return row is not None

    def list_recent(
        self,
        *,
        limit: int = 100,
        source: str | None = None,
        kind: str | None = None,
        source_instance: str | None = None,
    ) -> list[SignalEvent]:
        conditions: list[str] = []
        params: list[object] = []
        if source:
            conditions.append("source = ?")
            params.append(source)
        if kind:
            conditions.append("kind = ?")
            params.append(kind)
        if source_instance:
            conditions.append("source_instance = ?")
            params.append(source_instance)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(max(0, limit))
        rows = self._connection.execute(
            f"""
            SELECT * FROM signals
            {where}
            ORDER BY created_at DESC, collected_at DESC
            LIMIT ?
            """,
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
                "schema_version": row["schema_version"],
                "source": row["source"],
                "source_instance": row["source_instance"],
                "kind": row["kind"],
                "title": row["title"],
                "content": row["content"],
                "author": row["author"],
                "url": row["url"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "collected_at": row["collected_at"],
                "metadata": json.loads(row["metadata_json"]),
            }
        )

    @staticmethod
    def _row_to_health(row: sqlite3.Row) -> SourceHealth:
        return SourceHealth(
            source_key=str(row["source_key"]),
            status=str(row["status"]),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            last_attempt_at=(
                datetime.fromisoformat(str(row["last_attempt_at"]))
                if row["last_attempt_at"]
                else None
            ),
            last_success_at=(
                datetime.fromisoformat(str(row["last_success_at"]))
                if row["last_success_at"]
                else None
            ),
            last_error=str(row["last_error"]) if row["last_error"] else None,
            consecutive_failures=int(row["consecutive_failures"]),
            total_runs=int(row["total_runs"]),
            total_events=int(row["total_events"]),
        )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteSignalStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
