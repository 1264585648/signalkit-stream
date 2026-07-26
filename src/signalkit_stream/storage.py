from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
import json
import sqlite3

from signalkit_stream.migrations import get_database_schema_version, migrate_database
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


@dataclass(slots=True, frozen=True)
class DeliveryRecord:
    sink_key: str
    event_id: str
    status: str
    attempts: int
    updated_at: datetime
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    delivered_at: datetime | None = None


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
    """SQLite event store with checkpoints, source health, and transactional outbox.

    The persistent schema is owned exclusively by :mod:`signalkit_stream.migrations`;
    opening a store runs the forward-only migration chain and therefore refuses to
    operate on a database written by a newer SignalKit Stream release.

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
            self._configure_connection(self._connection)
            self._initialize()
        except Exception:
            connection = getattr(self, "_connection", None)
            if connection is not None:
                connection.close()
            raise

    @staticmethod
    def _configure_connection(connection: sqlite3.Connection) -> None:
        """Apply the SQLite pragmas the storage layer depends on.

        ``journal_mode=WAL`` is a persistent property of the database file, while
        ``synchronous`` and ``foreign_keys`` are per-connection and must be re-applied by
        every connection this layer opens. WAL keeps readers (``signalkit status``,
        ``doctor``, ``read_snapshot``) working while a writer holds the write lock, and
        with ``synchronous=NORMAL`` the commit-per-delivery path stops fsyncing twice per
        row.

        Switching an existing rollback-journal database to WAL needs the write lock, so a
        busy database is deliberately left in its current journal mode instead of failing
        to open: a slower journal mode is always preferable to an unusable store, and the
        next open of an uncontended database performs the switch.
        """

        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            connection.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass

    def _initialize(self) -> None:
        migrate_database(self._connection)

    @property
    def database_schema_version(self) -> int:
        return get_database_schema_version(self._connection)

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

    def register_delivery_sink(self, sink_key: str, *, backfill: bool = False) -> bool:
        if not sink_key.strip():
            raise ValueError("sink_key must not be empty")
        now = datetime.now(UTC).isoformat()
        with self._connection:
            existing = self._connection.execute(
                "SELECT 1 FROM delivery_sinks WHERE sink_key = ?",
                (sink_key,),
            ).fetchone()
            self._connection.execute(
                """
                INSERT INTO delivery_sinks (sink_key, enabled, created_at, updated_at)
                VALUES (?, 1, ?, ?)
                ON CONFLICT(sink_key) DO UPDATE SET
                    enabled = 1,
                    updated_at = excluded.updated_at
                """,
                (sink_key, now, now),
            )
            if backfill:
                self._connection.execute(
                    """
                    INSERT INTO deliveries (
                        sink_key, event_id, status, attempts, next_attempt_at,
                        last_error, delivered_at, updated_at
                    )
                    SELECT ?, id, 'pending', 0, NULL, NULL, NULL, ? FROM signals WHERE 1
                    ON CONFLICT(sink_key, event_id) DO NOTHING
                    """,
                    (sink_key, now),
                )
        return existing is None

    def disable_delivery_sink(self, sink_key: str) -> None:
        with self._connection:
            self._connection.execute(
                "UPDATE delivery_sinks SET enabled = 0, updated_at = ? WHERE sink_key = ?",
                (datetime.now(UTC).isoformat(), sink_key),
            )

    def list_ready_deliveries(
        self,
        sink_key: str,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[DeliveryRecord]:
        ready_at = (now or datetime.now(UTC)).isoformat()
        rows = self._connection.execute(
            """
            SELECT * FROM deliveries
            WHERE sink_key = ?
              AND status IN ('pending', 'failed')
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY updated_at, event_id
            LIMIT ?
            """,
            (sink_key, ready_at, max(0, limit)),
        ).fetchall()
        return [self._row_to_delivery(row) for row in rows]

    def mark_delivery_success(
        self,
        sink_key: str,
        event_id: str,
        *,
        delivered_at: datetime | None = None,
    ) -> None:
        when = delivered_at or datetime.now(UTC)
        with self._connection:
            self._connection.execute(
                """
                UPDATE deliveries SET
                    status = 'delivered',
                    attempts = attempts + 1,
                    next_attempt_at = NULL,
                    last_error = NULL,
                    delivered_at = ?,
                    updated_at = ?
                WHERE sink_key = ? AND event_id = ?
                """,
                (when.isoformat(), when.isoformat(), sink_key, event_id),
            )

    def mark_delivery_failure(
        self,
        sink_key: str,
        event_id: str,
        *,
        error: str,
        next_attempt_at: datetime | None,
        dead: bool = False,
        attempted_at: datetime | None = None,
    ) -> None:
        when = attempted_at or datetime.now(UTC)
        with self._connection:
            self._connection.execute(
                """
                UPDATE deliveries SET
                    status = ?,
                    attempts = attempts + 1,
                    next_attempt_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE sink_key = ? AND event_id = ?
                """,
                (
                    "dead" if dead else "failed",
                    next_attempt_at.isoformat() if next_attempt_at and not dead else None,
                    error,
                    when.isoformat(),
                    sink_key,
                    event_id,
                ),
            )

    def retry_dead_deliveries(self, sink_key: str) -> int:
        now = datetime.now(UTC).isoformat()
        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE deliveries SET
                    status = 'pending', attempts = 0, next_attempt_at = NULL,
                    last_error = NULL, delivered_at = NULL, updated_at = ?
                WHERE sink_key = ? AND status = 'dead'
                """,
                (now, sink_key),
            )
        return int(cursor.rowcount)

    def delivery_counts(self, sink_key: str | None = None) -> dict[str, int]:
        if sink_key:
            rows = self._connection.execute(
                "SELECT status, COUNT(*) AS total FROM deliveries WHERE sink_key = ? GROUP BY status",
                (sink_key,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT status, COUNT(*) AS total FROM deliveries GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["total"]) for row in rows}

    def get_delivery(self, sink_key: str, event_id: str) -> DeliveryRecord | None:
        row = self._connection.execute(
            "SELECT * FROM deliveries WHERE sink_key = ? AND event_id = ?",
            (sink_key, event_id),
        ).fetchone()
        return self._row_to_delivery(row) if row is not None else None

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

    @staticmethod
    def _row_to_delivery(row: sqlite3.Row) -> DeliveryRecord:
        return DeliveryRecord(
            sink_key=str(row["sink_key"]),
            event_id=str(row["event_id"]),
            status=str(row["status"]),
            attempts=int(row["attempts"]),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            next_attempt_at=(
                datetime.fromisoformat(str(row["next_attempt_at"]))
                if row["next_attempt_at"]
                else None
            ),
            last_error=str(row["last_error"]) if row["last_error"] else None,
            delivered_at=(
                datetime.fromisoformat(str(row["delivered_at"]))
                if row["delivered_at"]
                else None
            ),
        )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteSignalStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


__all__ = [
    "Checkpoint",
    "DeliveryRecord",
    "SQLiteSignalStore",
    "SignalStore",
    "SourceHealth",
    "StoreWriteResult",
]
