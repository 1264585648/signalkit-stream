from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
import json
from typing import Any, Mapping
from uuid import uuid4

from signalkit_stream.models import SignalEvent, SignalKind


PROTOCOL_VERSION = 1


@dataclass(slots=True, frozen=True)
class SourceIdentity:
    source: str
    instance: str = "default"

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("source must not be empty")
        if not self.instance.strip():
            raise ValueError("instance must not be empty")

    @property
    def key(self) -> str:
        return f"{self.source}:{self.instance}"


@dataclass(slots=True, frozen=True)
class Cursor:
    """Versioned opaque checkpoint owned by one source instance."""

    source_key: str
    state: Mapping[str, Any] = field(default_factory=dict)
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if not self.source_key.strip():
            raise ValueError("cursor source_key must not be empty")
        if self.version < 1:
            raise ValueError("cursor version must be >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "source_key": self.source_key,
            "state": dict(self.state),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Cursor:
        return cls(
            source_key=str(data["source_key"]),
            state=data.get("state", {}),
            version=int(data.get("version", PROTOCOL_VERSION)),
        )

    @classmethod
    def from_json(cls, value: str) -> Cursor:
        payload = json.loads(value)
        if not isinstance(payload, dict):
            raise ValueError("cursor JSON must contain an object")
        return cls.from_dict(payload)


@dataclass(slots=True, frozen=True)
class RawEvent:
    """Optional adapter-level envelope before normalization."""

    source: SourceIdentity
    external_id: str
    kind: SignalKind
    payload: Mapping[str, Any]
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.external_id.strip():
            raise ValueError("external_id must not be empty")


@dataclass(slots=True, frozen=True)
class RateLimitSnapshot:
    limit: int | None = None
    remaining: int | None = None
    reset_at: datetime | None = None
    retry_after: float | None = None


@dataclass(slots=True, frozen=True)
class CollectorContext:
    limit: int = 100
    request_id: str = field(default_factory=lambda: uuid4().hex)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    deadline: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("collector context limit must be >= 1")


@dataclass(slots=True)
class CollectorResult:
    events: list[SignalEvent]
    cursor: Cursor | None = None
    has_more: bool = False
    primary_count: int = 0
    rate_limit: RateLimitSnapshot | None = None
    warnings: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.primary_count < 0:
            raise ValueError("primary_count must be >= 0")


class CollectorErrorKind(StrEnum):
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    NETWORK = "network"
    HTTP = "http"
    PARSE = "parse"
    CURSOR = "cursor"
    CONFIG = "config"
    INTERNAL = "internal"


class CollectorError(RuntimeError):
    """Stable error contract surfaced by collectors and the runtime."""

    def __init__(
        self,
        message: str,
        *,
        kind: CollectorErrorKind = CollectorErrorKind.INTERNAL,
        source_key: str | None = None,
        retryable: bool = False,
        status_code: int | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.source_key = source_key
        self.retryable = retryable
        self.status_code = status_code
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": str(self),
            "kind": self.kind.value,
            "source_key": self.source_key,
            "retryable": self.retryable,
            "status_code": self.status_code,
            "details": self.details,
        }
