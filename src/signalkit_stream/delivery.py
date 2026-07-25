from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Protocol

import httpx

from signalkit_stream.models import SignalEvent


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass(slots=True, frozen=True)
class DeliveryContext:
    sink_name: str
    idempotency_key: str
    attempt: int


class SinkError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


class Sink(Protocol):
    name: str

    async def send(self, event: SignalEvent, context: DeliveryContext) -> None: ...


@dataclass(slots=True, frozen=True)
class DeliveryCandidate:
    event: SignalEvent
    event_hash: str
    attempts: int = 0


@dataclass(slots=True, frozen=True)
class DeliveryRecord:
    sink_name: str
    event_id: str
    event_hash: str
    delivered_hash: str | None
    status: DeliveryStatus
    attempts: int
    last_error: str | None
    next_attempt_at: datetime | None
    delivered_at: datetime | None
    updated_at: datetime


@dataclass(slots=True, frozen=True)
class DeliveryRunResult:
    sink_name: str
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    dead_lettered: int = 0
    skipped: int = 0


class DeliveryStore(Protocol):
    def pending(
        self,
        sink_name: str,
        *,
        limit: int,
        now: datetime,
    ) -> list[DeliveryCandidate]: ...

    def mark_delivered(
        self,
        sink_name: str,
        candidate: DeliveryCandidate,
        *,
        at: datetime,
    ) -> DeliveryRecord: ...

    def mark_failed(
        self,
        sink_name: str,
        candidate: DeliveryCandidate,
        error: str,
        *,
        at: datetime,
        next_attempt_at: datetime | None,
        dead_letter: bool,
    ) -> DeliveryRecord: ...

    def replay_dead_letters(self, sink_name: str, *, event_id: str | None = None) -> int: ...


class SQLiteDeliveryStore:
    """Durable per-sink delivery state over events stored in SignalKit SQLite.

    Delivery state is deliberately independent from collection checkpoints. A source
    can advance collection even when a sink is unavailable; pending events remain
    discoverable by comparing the current signal fingerprint with delivered_hash.
    """

    def __init__(self, path: str | Path = "signals.db") -> None:
        self.path = Path(path)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._initialize()

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS delivery_state (
                sink_name TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_hash TEXT NOT NULL,
                delivered_hash TEXT,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_attempt_at TEXT,
                delivered_at TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (sink_name, event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_delivery_state_sink_status
                ON delivery_state(sink_name, status, next_attempt_at);
            """
        )
        self._connection.commit()

    def pending(
        self,
        sink_name: str,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[DeliveryCandidate]:
        if limit < 1:
            return []
        moment = _utc(now or datetime.now(UTC))
        rows = self._connection.execute(
            """
            SELECT s.*, d.event_hash AS delivery_event_hash,
                   d.delivered_hash, d.status AS delivery_status,
                   d.attempts AS delivery_attempts, d.next_attempt_at
            FROM signals AS s
            LEFT JOIN delivery_state AS d
              ON d.sink_name = ? AND d.event_id = s.id
            WHERE s.event_hash != COALESCE(d.delivered_hash, '')
              AND NOT (
                    d.status = 'dead_letter'
                AND d.event_hash = s.event_hash
              )
              AND (
                    d.event_hash IS NULL
                 OR d.event_hash != s.event_hash
                 OR d.next_attempt_at IS NULL
                 OR d.next_attempt_at <= ?
              )
            ORDER BY s.created_at ASC, s.id ASC
            LIMIT ?
            """,
            (sink_name, moment.isoformat(), limit),
        ).fetchall()
        return [
            DeliveryCandidate(
                event=self._row_to_event(row),
                event_hash=str(row["event_hash"]),
                attempts=(
                    int(row["delivery_attempts"] or 0)
                    if row["delivery_event_hash"] == row["event_hash"]
                    else 0
                ),
            )
            for row in rows
        ]

    def mark_delivered(
        self,
        sink_name: str,
        candidate: DeliveryCandidate,
        *,
        at: datetime | None = None,
    ) -> DeliveryRecord:
        moment = _utc(at or datetime.now(UTC))
        attempts = candidate.attempts + 1
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO delivery_state (
                    sink_name, event_id, event_hash, delivered_hash, status, attempts,
                    last_error, next_attempt_at, delivered_at, updated_at
                ) VALUES (?, ?, ?, ?, 'delivered', ?, NULL, NULL, ?, ?)
                ON CONFLICT(sink_name, event_id) DO UPDATE SET
                    event_hash = excluded.event_hash,
                    delivered_hash = excluded.delivered_hash,
                    status = excluded.status,
                    attempts = excluded.attempts,
                    last_error = NULL,
                    next_attempt_at = NULL,
                    delivered_at = excluded.delivered_at,
                    updated_at = excluded.updated_at
                """,
                (
                    sink_name,
                    candidate.event.id,
                    candidate.event_hash,
                    candidate.event_hash,
                    attempts,
                    moment.isoformat(),
                    moment.isoformat(),
                ),
            )
        return self.get_record(sink_name, candidate.event.id)  # type: ignore[return-value]

    def mark_failed(
        self,
        sink_name: str,
        candidate: DeliveryCandidate,
        error: str,
        *,
        at: datetime | None = None,
        next_attempt_at: datetime | None = None,
        dead_letter: bool = False,
    ) -> DeliveryRecord:
        moment = _utc(at or datetime.now(UTC))
        retry_at = _utc(next_attempt_at) if next_attempt_at else None
        attempts = candidate.attempts + 1
        status = DeliveryStatus.DEAD_LETTER if dead_letter else DeliveryStatus.FAILED
        with self._connection:
            existing = self.get_record(sink_name, candidate.event.id)
            delivered_hash = existing.delivered_hash if existing else None
            delivered_at = existing.delivered_at if existing else None
            self._connection.execute(
                """
                INSERT INTO delivery_state (
                    sink_name, event_id, event_hash, delivered_hash, status, attempts,
                    last_error, next_attempt_at, delivered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sink_name, event_id) DO UPDATE SET
                    event_hash = excluded.event_hash,
                    delivered_hash = excluded.delivered_hash,
                    status = excluded.status,
                    attempts = excluded.attempts,
                    last_error = excluded.last_error,
                    next_attempt_at = excluded.next_attempt_at,
                    delivered_at = excluded.delivered_at,
                    updated_at = excluded.updated_at
                """,
                (
                    sink_name,
                    candidate.event.id,
                    candidate.event_hash,
                    delivered_hash,
                    status.value,
                    attempts,
                    error,
                    retry_at.isoformat() if retry_at else None,
                    delivered_at.isoformat() if delivered_at else None,
                    moment.isoformat(),
                ),
            )
        return self.get_record(sink_name, candidate.event.id)  # type: ignore[return-value]

    def get_record(self, sink_name: str, event_id: str) -> DeliveryRecord | None:
        row = self._connection.execute(
            "SELECT * FROM delivery_state WHERE sink_name = ? AND event_id = ?",
            (sink_name, event_id),
        ).fetchone()
        if row is None:
            return None
        return DeliveryRecord(
            sink_name=str(row["sink_name"]),
            event_id=str(row["event_id"]),
            event_hash=str(row["event_hash"]),
            delivered_hash=str(row["delivered_hash"]) if row["delivered_hash"] else None,
            status=DeliveryStatus(str(row["status"])),
            attempts=int(row["attempts"]),
            last_error=str(row["last_error"]) if row["last_error"] else None,
            next_attempt_at=_parse_time(row["next_attempt_at"]),
            delivered_at=_parse_time(row["delivered_at"]),
            updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
        )

    def list_records(
        self,
        *,
        sink_name: str | None = None,
        status: DeliveryStatus | str | None = None,
        limit: int = 100,
    ) -> list[DeliveryRecord]:
        conditions: list[str] = []
        params: list[object] = []
        if sink_name:
            conditions.append("sink_name = ?")
            params.append(sink_name)
        if status:
            conditions.append("status = ?")
            params.append(status.value if isinstance(status, DeliveryStatus) else str(status))
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(max(0, limit))
        rows = self._connection.execute(
            f"SELECT * FROM delivery_state {where} ORDER BY updated_at DESC LIMIT ?",  # noqa: S608
            params,
        ).fetchall()
        return [
            DeliveryRecord(
                sink_name=str(row["sink_name"]),
                event_id=str(row["event_id"]),
                event_hash=str(row["event_hash"]),
                delivered_hash=str(row["delivered_hash"]) if row["delivered_hash"] else None,
                status=DeliveryStatus(str(row["status"])),
                attempts=int(row["attempts"]),
                last_error=str(row["last_error"]) if row["last_error"] else None,
                next_attempt_at=_parse_time(row["next_attempt_at"]),
                delivered_at=_parse_time(row["delivered_at"]),
                updated_at=_parse_time(row["updated_at"]) or datetime.now(UTC),
            )
            for row in rows
        ]

    def replay_dead_letters(self, sink_name: str, *, event_id: str | None = None) -> int:
        conditions = ["sink_name = ?", "status = 'dead_letter'"]
        params: list[object] = [sink_name]
        if event_id:
            conditions.append("event_id = ?")
            params.append(event_id)
        with self._connection:
            cursor = self._connection.execute(
                f"""
                UPDATE delivery_state
                SET status = 'failed', attempts = 0, next_attempt_at = NULL,
                    updated_at = ?
                WHERE {' AND '.join(conditions)}
                """,  # noqa: S608 - fragments are fixed; values parameterized.
                (datetime.now(UTC).isoformat(), *params),
            )
        return int(cursor.rowcount)

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

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteDeliveryStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class StdoutSink:
    """JSONL sink; injectable writer keeps tests and embedding deterministic."""

    def __init__(self, name: str = "stdout", *, writer: Callable[[str], Any] = print) -> None:
        self.name = name
        self._writer = writer

    async def send(self, event: SignalEvent, context: DeliveryContext) -> None:
        self._writer(json.dumps(event.to_dict(), ensure_ascii=False, sort_keys=True))


class WebhookSink:
    """Send one normalized event per HTTP POST with a stable idempotency key."""

    def __init__(
        self,
        url: str,
        *,
        name: str = "webhook",
        headers: Mapping[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ) -> None:
        if not url.strip():
            raise ValueError("webhook URL must not be empty")
        self.name = name
        self.url = url
        self.headers = dict(headers or {})
        self._client = client
        self.timeout = timeout

    async def send(self, event: SignalEvent, context: DeliveryContext) -> None:
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": context.idempotency_key,
            "X-SignalKit-Event-Id": event.id,
            **self.headers,
        }
        payload = {"event": event.to_dict()}
        if self._client is not None:
            response = await self._client.post(self.url, json=payload, headers=headers)
        else:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                response = await client.post(self.url, json=payload, headers=headers)
        if 200 <= response.status_code < 300:
            return
        retryable = response.status_code in {408, 425, 429, 500, 502, 503, 504}
        raise SinkError(
            f"webhook HTTP {response.status_code}: {response.text[:300]}",
            retryable=retryable,
            status_code=response.status_code,
        )


class DeliveryDispatcher:
    """Fan out stored events independently to one or more durable sinks."""

    def __init__(
        self,
        store: DeliveryStore,
        sinks: Iterable[Sink],
        *,
        max_attempts: int = 5,
        backoff_base: float = 5.0,
        max_backoff: float = 300.0,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if backoff_base <= 0 or max_backoff <= 0:
            raise ValueError("delivery backoff values must be > 0")
        self.store = store
        self.sinks = tuple(sinks)
        names = [sink.name for sink in self.sinks]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate sink names: {', '.join(duplicates)}")
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.max_backoff = max_backoff
        self._now = now
        self._sleep = sleep

    async def run_once(self, *, limit_per_sink: int = 100) -> list[DeliveryRunResult]:
        results: list[DeliveryRunResult] = []
        for sink in self.sinks:
            results.append(await self._deliver_sink(sink, limit=limit_per_sink))
        return results

    async def run_forever(
        self,
        *,
        interval: float = 1.0,
        limit_per_sink: int = 100,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        if interval <= 0:
            raise ValueError("delivery interval must be > 0")
        stop = stop_event or asyncio.Event()
        while not stop.is_set():
            await self.run_once(limit_per_sink=limit_per_sink)
            if stop.is_set():
                break
            sleep_task = asyncio.create_task(self._sleep(interval))
            stop_task = asyncio.create_task(stop.wait())
            done, pending = await asyncio.wait(
                {sleep_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if sleep_task in done:
                sleep_task.result()

    async def _deliver_sink(self, sink: Sink, *, limit: int) -> DeliveryRunResult:
        now = self._now()
        candidates = self.store.pending(sink.name, limit=limit, now=now)
        delivered = 0
        failed = 0
        dead_lettered = 0
        for candidate in candidates:
            attempt = candidate.attempts + 1
            context = DeliveryContext(
                sink_name=sink.name,
                idempotency_key=delivery_idempotency_key(
                    sink.name,
                    candidate.event.id,
                    candidate.event_hash,
                ),
                attempt=attempt,
            )
            try:
                await sink.send(candidate.event, context)
            except asyncio.CancelledError:
                raise
            except SinkError as exc:
                is_dead = (not exc.retryable) or attempt >= self.max_attempts
                retry_at = None if is_dead else self._retry_at(attempt)
                self.store.mark_failed(
                    sink.name,
                    candidate,
                    str(exc),
                    at=self._now(),
                    next_attempt_at=retry_at,
                    dead_letter=is_dead,
                )
                if is_dead:
                    dead_lettered += 1
                else:
                    failed += 1
            except Exception as exc:
                is_dead = attempt >= self.max_attempts
                retry_at = None if is_dead else self._retry_at(attempt)
                self.store.mark_failed(
                    sink.name,
                    candidate,
                    repr(exc),
                    at=self._now(),
                    next_attempt_at=retry_at,
                    dead_letter=is_dead,
                )
                if is_dead:
                    dead_lettered += 1
                else:
                    failed += 1
            else:
                self.store.mark_delivered(sink.name, candidate, at=self._now())
                delivered += 1
        return DeliveryRunResult(
            sink_name=sink.name,
            attempted=len(candidates),
            delivered=delivered,
            failed=failed,
            dead_lettered=dead_lettered,
        )

    def _retry_at(self, attempt: int) -> datetime:
        delay = min(self.max_backoff, self.backoff_base * (2 ** max(0, attempt - 1)))
        return self._now() + timedelta(seconds=delay)


def delivery_idempotency_key(sink_name: str, event_id: str, event_hash: str) -> str:
    raw = f"{sink_name}\x1f{event_id}\x1f{event_hash}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"skd_{digest}"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value)).astimezone(UTC)
