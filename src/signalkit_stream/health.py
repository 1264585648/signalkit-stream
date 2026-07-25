from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
import sqlite3

from signalkit_stream.protocol import RateLimitSnapshot


@dataclass(slots=True, frozen=True)
class SourceHealth:
    source_key: str
    status: str
    consecutive_failures: int
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    paused_until: datetime | None = None
    rate_limit_remaining: int | None = None
    rate_limit_reset_at: datetime | None = None
    updated_at: datetime | None = None


class RuntimeStateStore(Protocol):
    def get_health(self, source_key: str) -> SourceHealth | None: ...

    def list_health(self) -> list[SourceHealth]: ...

    def record_attempt(self, source_key: str, *, at: datetime) -> None: ...

    def record_success(
        self,
        source_key: str,
        *,
        at: datetime,
        rate_limit: RateLimitSnapshot | None = None,
        paused_until: datetime | None = None,
    ) -> None: ...

    def record_failure(
        self,
        source_key: str,
        error: str,
        *,
        at: datetime,
        consecutive_failures: int,
        paused_until: datetime | None,
        rate_limit: RateLimitSnapshot | None = None,
    ) -> None: ...


class SQLiteRuntimeStateStore:
    """Persist source health and scheduler pause state in SQLite."""

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
            CREATE TABLE IF NOT EXISTS source_health (
                source_key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                last_success_at TEXT,
                last_error TEXT,
                paused_until TEXT,
                rate_limit_remaining INTEGER,
                rate_limit_reset_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_source_health_status
                ON source_health(status, updated_at DESC);
            """
        )
        self._connection.commit()

    def get_health(self, source_key: str) -> SourceHealth | None:
        row = self._connection.execute(
            "SELECT * FROM source_health WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        return self._row_to_health(row) if row is not None else None

    def list_health(self) -> list[SourceHealth]:
        rows = self._connection.execute(
            "SELECT * FROM source_health ORDER BY source_key"
        ).fetchall()
        return [self._row_to_health(row) for row in rows]

    def record_attempt(self, source_key: str, *, at: datetime) -> None:
        now = _utc(at)
        with self._connection:
            existing = self.get_health(source_key)
            self._connection.execute(
                """
                INSERT INTO source_health (
                    source_key, status, consecutive_failures, last_attempt_at,
                    last_success_at, last_error, paused_until,
                    rate_limit_remaining, rate_limit_reset_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    status = excluded.status,
                    last_attempt_at = excluded.last_attempt_at,
                    updated_at = excluded.updated_at
                """,
                (
                    source_key,
                    "running",
                    existing.consecutive_failures if existing else 0,
                    now.isoformat(),
                    existing.last_success_at.isoformat() if existing and existing.last_success_at else None,
                    existing.last_error if existing else None,
                    existing.paused_until.isoformat() if existing and existing.paused_until else None,
                    existing.rate_limit_remaining if existing else None,
                    existing.rate_limit_reset_at.isoformat()
                    if existing and existing.rate_limit_reset_at
                    else None,
                    now.isoformat(),
                ),
            )

    def record_success(
        self,
        source_key: str,
        *,
        at: datetime,
        rate_limit: RateLimitSnapshot | None = None,
        paused_until: datetime | None = None,
    ) -> None:
        now = _utc(at)
        paused = _utc(paused_until) if paused_until else None
        reset_at = _utc(rate_limit.reset_at) if rate_limit and rate_limit.reset_at else None
        remaining = rate_limit.remaining if rate_limit else None
        status = "paused" if paused and paused > now else "healthy"
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO source_health (
                    source_key, status, consecutive_failures, last_attempt_at,
                    last_success_at, last_error, paused_until,
                    rate_limit_remaining, rate_limit_reset_at, updated_at
                ) VALUES (?, ?, 0, ?, ?, NULL, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    status = excluded.status,
                    consecutive_failures = 0,
                    last_attempt_at = excluded.last_attempt_at,
                    last_success_at = excluded.last_success_at,
                    last_error = NULL,
                    paused_until = excluded.paused_until,
                    rate_limit_remaining = excluded.rate_limit_remaining,
                    rate_limit_reset_at = excluded.rate_limit_reset_at,
                    updated_at = excluded.updated_at
                """,
                (
                    source_key,
                    status,
                    now.isoformat(),
                    now.isoformat(),
                    paused.isoformat() if paused else None,
                    remaining,
                    reset_at.isoformat() if reset_at else None,
                    now.isoformat(),
                ),
            )

    def record_failure(
        self,
        source_key: str,
        error: str,
        *,
        at: datetime,
        consecutive_failures: int,
        paused_until: datetime | None,
        rate_limit: RateLimitSnapshot | None = None,
    ) -> None:
        now = _utc(at)
        paused = _utc(paused_until) if paused_until else None
        reset_at = _utc(rate_limit.reset_at) if rate_limit and rate_limit.reset_at else None
        remaining = rate_limit.remaining if rate_limit else None
        status = "open" if paused and paused > now else "error"
        with self._connection:
            existing = self.get_health(source_key)
            self._connection.execute(
                """
                INSERT INTO source_health (
                    source_key, status, consecutive_failures, last_attempt_at,
                    last_success_at, last_error, paused_until,
                    rate_limit_remaining, rate_limit_reset_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    status = excluded.status,
                    consecutive_failures = excluded.consecutive_failures,
                    last_attempt_at = excluded.last_attempt_at,
                    last_error = excluded.last_error,
                    paused_until = excluded.paused_until,
                    rate_limit_remaining = excluded.rate_limit_remaining,
                    rate_limit_reset_at = excluded.rate_limit_reset_at,
                    updated_at = excluded.updated_at
                """,
                (
                    source_key,
                    status,
                    consecutive_failures,
                    now.isoformat(),
                    existing.last_success_at.isoformat() if existing and existing.last_success_at else None,
                    error,
                    paused.isoformat() if paused else None,
                    remaining,
                    reset_at.isoformat() if reset_at else None,
                    now.isoformat(),
                ),
            )

    @staticmethod
    def _row_to_health(row: sqlite3.Row) -> SourceHealth:
        return SourceHealth(
            source_key=str(row["source_key"]),
            status=str(row["status"]),
            consecutive_failures=int(row["consecutive_failures"]),
            last_attempt_at=_parse_datetime(row["last_attempt_at"]),
            last_success_at=_parse_datetime(row["last_success_at"]),
            last_error=str(row["last_error"]) if row["last_error"] else None,
            paused_until=_parse_datetime(row["paused_until"]),
            rate_limit_remaining=(
                int(row["rate_limit_remaining"])
                if row["rate_limit_remaining"] is not None
                else None
            ),
            rate_limit_reset_at=_parse_datetime(row["rate_limit_reset_at"]),
            updated_at=_parse_datetime(row["updated_at"]),
        )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteRuntimeStateStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value)).astimezone(UTC)
