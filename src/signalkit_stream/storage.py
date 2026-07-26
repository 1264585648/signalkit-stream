from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
import json
import sqlite3

from signalkit_stream.migrations import get_database_schema_version, migrate_database
from signalkit_stream.models import SignalEvent
from signalkit_stream.protocol import Cursor

# SQLITE_MAX_VARIABLE_NUMBER defaults to 32766 on modern SQLite and 999 on builds older
# than 3.32, so batched ``IN (...)`` reads are chunked well below the lower bound.
_ID_CHUNK_SIZE = 500


def _utc_iso(value: datetime) -> str:
    """Serialize a datetime as a lexicographically sortable UTC ISO-8601 string.

    Every timestamp this module stores is compared and ordered by SQLite as text
    (``next_attempt_at <= ?`` for delivery readiness, ``ORDER BY updated_at`` for outbox
    FIFO), so a value carrying any offset other than ``+00:00`` sorts by its wall clock
    rather than by its instant. Naive values are treated as UTC, mirroring
    :func:`signalkit_stream.models._utc`, which already normalizes every ``SignalEvent``
    timestamp before it reaches the ``signals`` table.

    No migration ships for pre-existing rows: no shipped code path can persist a
    non-UTC delivery timestamp (``DeliveryEngine`` derives all of them from
    ``datetime.now(UTC)``, and the outbox triggers copy ``NEW.collected_at``, already
    normalized by ``SignalEvent``), such a row could only come from a custom embedding
    passing its own offset-aware datetime, and any row like that self-heals on its next
    ``mark_delivery_*``/``retry_dead_deliveries`` write.
    """

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).isoformat()
    return value.astimezone(UTC).isoformat()


_UPSERT_SIGNAL = """
INSERT INTO signals (
    id, schema_version, source, source_instance, kind, title, content,
    author, url, created_at, updated_at, collected_at, metadata_json, event_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    schema_version = excluded.schema_version,
    source = excluded.source,
    source_instance = excluded.source_instance,
    kind = excluded.kind,
    title = excluded.title,
    content = excluded.content,
    author = excluded.author,
    url = excluded.url,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at,
    collected_at = excluded.collected_at,
    metadata_json = excluded.metadata_json,
    event_hash = excluded.event_hash
WHERE signals.event_hash <> excluded.event_hash
"""


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


@dataclass(slots=True, frozen=True)
class ReadyDelivery:
    """One ready outbox row joined to the event payload it should deliver.

    ``updated_at`` is the delivery row's own timestamp, not the event's. Pass it back as
    ``expected_updated_at`` to :meth:`SQLiteSignalStore.mark_delivery_success` /
    :meth:`SQLiteSignalStore.mark_delivery_failure` so the outcome of this attempt cannot
    overwrite a newer ``pending`` state written by the outbox update trigger in the
    meantime.
    """

    event: SignalEvent
    status: str
    attempts: int
    updated_at: datetime
    next_attempt_at: datetime | None = None

    @property
    def event_id(self) -> str:
        return self.event.id


@dataclass(slots=True, frozen=True)
class DeliveryOutcome:
    """One delivery attempt result, for :meth:`SQLiteSignalStore.apply_delivery_outcomes`.

    Field order matches the ``(status, attempts, next_attempt_at, last_error, updated_at,
    sink_key, event_id)`` tuple shape, so ``DeliveryOutcome(*row)`` works. ``attempts`` is
    absolute (the caller already knows the prior count from :class:`ReadyDelivery`), not a
    relative increment. ``expected_updated_at`` is the optimistic-concurrency guard and is
    optional per row.
    """

    status: str
    attempts: int
    next_attempt_at: datetime | None
    last_error: str | None
    updated_at: datetime
    sink_key: str
    event_id: str
    delivered_at: datetime | None = None
    expected_updated_at: datetime | None = None


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

    @contextmanager
    def _write_transaction(self) -> Iterator[None]:
        """Run a body inside one ``BEGIN IMMEDIATE`` write transaction.

        ``BEGIN IMMEDIATE`` takes the write lock before the first statement, so a
        read-then-write sequence (dedup probe then upsert, sink probe then backfill) is
        serialized against other writers instead of racing between the read and the
        write. It also avoids WAL's ``SQLITE_BUSY_SNAPSHOT`` upgrade failure, which a
        deferred read/write transaction can hit without the busy handler being consulted.
        """

        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._connection.rollback()
            raise
        self._connection.commit()

    def write_many(self, events: Iterable[SignalEvent]) -> StoreWriteResult:
        event_list = list(events)
        with self._write_transaction():
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
        with self._write_transaction():
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
        """Apply one page of events with a chunked pre-read plus a single ``executemany``.

        Must be called inside :meth:`_write_transaction`. Because the write lock is held
        for the whole batch, the pre-read is authoritative for the duration of the write,
        which is what makes the returned ``StoreWriteResult`` exact. The statement is an
        upsert anyway, so even a hypothetical concurrent writer produces an update rather
        than an ``IntegrityError`` that would roll back the entire page and its
        checkpoint. The ``WHERE signals.event_hash <> excluded.event_hash`` guard keeps a
        content-identical row completely untouched, so the ``AFTER UPDATE OF event_hash``
        outbox trigger stays silent for no-op writes.
        """

        if not events:
            return StoreWriteResult()

        known = self._existing_event_hashes([event.id for event in events])
        inserted = 0
        updated = 0
        unchanged = 0
        params: list[tuple[object, ...]] = []
        for event in events:
            fingerprint = event.fingerprint()
            existing_hash = known.get(event.id)
            if existing_hash is None:
                inserted += 1
            elif existing_hash == fingerprint:
                unchanged += 1
                continue
            else:
                updated += 1
            known[event.id] = fingerprint
            params.append(self._event_params(event, fingerprint))

        if params:
            self._connection.executemany(_UPSERT_SIGNAL, params)

        return StoreWriteResult(inserted=inserted, updated=updated, unchanged=unchanged)

    def _existing_event_hashes(self, event_ids: list[str]) -> dict[str, str]:
        """Read stored fingerprints for many ids using chunked ``IN (...)`` lookups."""

        unique_ids = list(dict.fromkeys(event_ids))
        hashes: dict[str, str] = {}
        for start in range(0, len(unique_ids), _ID_CHUNK_SIZE):
            chunk = unique_ids[start : start + _ID_CHUNK_SIZE]
            placeholders = ", ".join("?" * len(chunk))
            rows = self._connection.execute(
                f"SELECT id, event_hash FROM signals WHERE id IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                hashes[str(row["id"])] = str(row["event_hash"] or "")
        return hashes

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
            _utc_iso(event.created_at),
            _utc_iso(event.updated_at) if event.updated_at else None,
            _utc_iso(event.collected_at),
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
                _utc_iso(updated_at),
                _utc_iso(last_success_at) if last_success_at else None,
                last_error,
            ),
        )

    def record_failure(self, source_key: str, error: str) -> None:
        now = _utc_iso(datetime.now(UTC))
        with self._write_transaction():
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
                    _utc_iso(health.updated_at),
                    _utc_iso(health.last_attempt_at) if health.last_attempt_at else None,
                    _utc_iso(health.last_success_at) if health.last_success_at else None,
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
        """Enable a delivery sink, returning ``True`` only for a first-time registration.

        While a sink is disabled the outbox triggers skip it, so every event collected in
        that window has no delivery row at all. Re-enabling therefore always backfills,
        regardless of ``backfill``: without it those events would never be delivered to
        this sink, silently breaking at-least-once for the whole disabled window. The
        backfill is ``ON CONFLICT DO NOTHING``, so it only ever fills genuine holes and
        never resets a delivery row that already exists (delivered rows stay delivered,
        failed rows keep their attempt count and retry schedule).
        """

        if not sink_key.strip():
            raise ValueError("sink_key must not be empty")
        now = _utc_iso(datetime.now(UTC))
        with self._write_transaction():
            existing = self._connection.execute(
                "SELECT enabled FROM delivery_sinks WHERE sink_key = ?",
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
            was_disabled = existing is not None and not int(existing["enabled"])
            if backfill or was_disabled:
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
                (_utc_iso(datetime.now(UTC)), sink_key),
            )

    def list_ready_deliveries(
        self,
        sink_key: str,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[DeliveryRecord]:
        ready_at = _utc_iso(now or datetime.now(UTC))
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

    def list_ready_delivery_payloads(
        self,
        sink_key: str,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[ReadyDelivery]:
        """Return ready outbox rows joined to their event payloads in one statement.

        Equivalent to :meth:`list_ready_deliveries` followed by one :meth:`get` per row,
        but as a single indexed query: the drain loop no longer issues ``1 + N`` reads per
        batch on the asyncio event loop. Rows whose event has been deleted are not
        returned at all (the join drops them); use :meth:`list_ready_deliveries` if the
        caller needs to see and tombstone those.

        Ordering, readiness and status filtering are identical to
        :meth:`list_ready_deliveries`, and ``idx_deliveries_ready`` still drives the scan.
        The readiness filter, ``ORDER BY`` and ``LIMIT`` are applied to the narrow delivery
        rows in a subquery before the join: SQLite cannot satisfy this ``ORDER BY`` from the
        index (the ``next_attempt_at IS NULL OR ...`` disjunction breaks the range), so
        joining first would push every matching *wide* signals row through the sorter.
        Measured on a 20k-row outbox with 2 KB payloads, join-then-sort took 25.5 ms
        against 12.7 ms for sort-then-join.

        ``ReadyDelivery.updated_at`` is the delivery row's timestamp and is the value to
        pass back as ``expected_updated_at``.
        """

        ready_at = _utc_iso(now or datetime.now(UTC))
        rows = self._connection.execute(
            """
            SELECT
                d.event_id AS delivery_event_id,
                d.status AS delivery_status,
                d.attempts AS delivery_attempts,
                d.updated_at AS delivery_updated_at,
                d.next_attempt_at AS delivery_next_attempt_at,
                s.*
            FROM (
                SELECT event_id, status, attempts, updated_at, next_attempt_at
                FROM deliveries
                WHERE sink_key = ?
                  AND status IN ('pending', 'failed')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ORDER BY updated_at, event_id
                LIMIT ?
            ) d
            JOIN signals s ON s.id = d.event_id
            ORDER BY d.updated_at, d.event_id
            """,
            (sink_key, ready_at, max(0, limit)),
        ).fetchall()
        return [
            ReadyDelivery(
                event=self._row_to_event(row),
                status=str(row["delivery_status"]),
                attempts=int(row["delivery_attempts"]),
                updated_at=datetime.fromisoformat(str(row["delivery_updated_at"])),
                next_attempt_at=(
                    datetime.fromisoformat(str(row["delivery_next_attempt_at"]))
                    if row["delivery_next_attempt_at"]
                    else None
                ),
            )
            for row in rows
        ]

    def get_event_hash(self, event_id: str) -> str | None:
        """Return the stored content fingerprint of an event, or ``None`` if it is gone.

        The post-send supersede check only needs to know whether the payload changed while
        the sink call was in flight, so it does not need a full row read plus
        ``json.loads`` plus ``SignalEvent.from_dict`` the way :meth:`get` does.
        """

        row = self._connection.execute(
            "SELECT event_hash FROM signals WHERE id = ?",
            (event_id,),
        ).fetchone()
        return str(row["event_hash"]) if row is not None else None

    def apply_delivery_outcomes(self, outcomes: Iterable[DeliveryOutcome]) -> int:
        """Apply many delivery attempt results in one transaction, returning rows changed.

        One ``executemany`` inside one ``BEGIN IMMEDIATE`` replaces one transaction and
        one commit per delivered event. ``attempts`` is written absolutely rather than as
        ``attempts + 1`` so a retried batch cannot double-count.

        Outcomes carrying ``expected_updated_at`` are applied only while the row still
        holds that timestamp; the return value is the number of rows actually updated, so
        a caller comparing it against the number of outcomes learns that some rows were
        superseded (and must be left pending) without needing a second read.

        Unlike :meth:`mark_delivery_failure`, ``delivered_at`` is written on every outcome,
        so a failure outcome also clears a stale ``delivered_at``.
        """

        params = [
            (
                outcome.status,
                outcome.attempts,
                _utc_iso(outcome.next_attempt_at) if outcome.next_attempt_at else None,
                outcome.last_error,
                _utc_iso(outcome.delivered_at) if outcome.delivered_at else None,
                _utc_iso(outcome.updated_at),
                outcome.sink_key,
                outcome.event_id,
                _utc_iso(outcome.expected_updated_at) if outcome.expected_updated_at else None,
            )
            for outcome in outcomes
        ]
        if not params:
            return 0

        before = self._connection.total_changes
        with self._write_transaction():
            self._connection.executemany(
                """
                UPDATE deliveries SET
                    status = ?,
                    attempts = ?,
                    next_attempt_at = ?,
                    last_error = ?,
                    delivered_at = ?,
                    updated_at = ?
                WHERE sink_key = ? AND event_id = ?
                  AND (? IS NULL OR updated_at = ?)
                """,
                [row + (row[-1],) for row in params],
            )
        return self._connection.total_changes - before

    def mark_delivery_success(
        self,
        sink_key: str,
        event_id: str,
        *,
        delivered_at: datetime | None = None,
        expected_updated_at: datetime | None = None,
    ) -> bool:
        """Mark one delivery delivered, returning whether the row was actually updated.

        ``expected_updated_at`` is an optimistic-concurrency guard: pass the
        ``updated_at`` the row had when it was read and the update applies only while the
        row still holds it. ``False`` means the row was superseded (the outbox update
        trigger re-pended it because the event content changed while the send was in
        flight) or no longer exists, and the caller must leave it pending rather than
        recording an outcome for a payload that is no longer current.
        """

        when = delivered_at or datetime.now(UTC)
        with self._write_transaction():
            cursor = self._connection.execute(
                """
                UPDATE deliveries SET
                    status = 'delivered',
                    attempts = attempts + 1,
                    next_attempt_at = NULL,
                    last_error = NULL,
                    delivered_at = ?,
                    updated_at = ?
                WHERE sink_key = ? AND event_id = ?
                  AND (? IS NULL OR updated_at = ?)
                """,
                (
                    _utc_iso(when),
                    _utc_iso(when),
                    sink_key,
                    event_id,
                    *self._expected_pair(expected_updated_at),
                ),
            )
            changed = int(cursor.rowcount)
        return changed > 0

    def mark_delivery_failure(
        self,
        sink_key: str,
        event_id: str,
        *,
        error: str,
        next_attempt_at: datetime | None,
        dead: bool = False,
        attempted_at: datetime | None = None,
        expected_updated_at: datetime | None = None,
    ) -> bool:
        """Mark one delivery failed or dead, returning whether the row was updated.

        ``expected_updated_at`` closes a real at-least-once hole: without it the failure
        path blind-UPDATEs and can bury a newer ``pending`` state, so a late HTTP 400 for
        an old payload marks the row ``dead`` even though the collector has already
        re-collected a corrected version that would have delivered fine. ``False`` means
        the row was superseded or is gone; leave it pending.
        """

        when = attempted_at or datetime.now(UTC)
        with self._write_transaction():
            cursor = self._connection.execute(
                """
                UPDATE deliveries SET
                    status = ?,
                    attempts = attempts + 1,
                    next_attempt_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE sink_key = ? AND event_id = ?
                  AND (? IS NULL OR updated_at = ?)
                """,
                (
                    "dead" if dead else "failed",
                    _utc_iso(next_attempt_at) if next_attempt_at and not dead else None,
                    error,
                    _utc_iso(when),
                    sink_key,
                    event_id,
                    *self._expected_pair(expected_updated_at),
                ),
            )
            changed = int(cursor.rowcount)
        return changed > 0

    @staticmethod
    def _expected_pair(expected_updated_at: datetime | None) -> tuple[str | None, str | None]:
        """Bind the optional ``AND updated_at = ?`` guard as ``(? IS NULL OR ...)``."""

        if expected_updated_at is None:
            return (None, None)
        stamp = _utc_iso(expected_updated_at)
        return (stamp, stamp)

    def retry_dead_deliveries(self, sink_key: str) -> int:
        now = _utc_iso(datetime.now(UTC))
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
    "DeliveryOutcome",
    "DeliveryRecord",
    "ReadyDelivery",
    "SQLiteSignalStore",
    "SignalStore",
    "SourceHealth",
    "StoreWriteResult",
]
