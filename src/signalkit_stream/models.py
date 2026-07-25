from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
import hashlib
import json
from typing import Any, Mapping


SCHEMA_VERSION = 1


class SignalKind(StrEnum):
    """Source-agnostic categories emitted by collectors."""

    ARTICLE = "article"
    COMMENT = "comment"
    ISSUE = "issue"
    POST = "post"
    PULL_REQUEST = "pull_request"
    STORY = "story"
    OTHER = "other"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(slots=True, frozen=True)
class SignalEvent:
    """Normalized, versioned event emitted by SignalKit Stream.

    ``id`` identifies the source-native object and is stable across recollection.
    ``updated_at`` represents source-side mutation time where the source exposes it.
    ``collected_at`` is intentionally excluded from the content fingerprint so polling
    the same object does not create a false update.
    """

    id: str
    source: str
    kind: SignalKind
    content: str
    url: str
    created_at: datetime
    collected_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    title: str | None = None
    author: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    source_instance: str = "default"
    updated_at: datetime | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("event id must not be empty")
        if not self.source.strip():
            raise ValueError("event source must not be empty")
        if not self.source_instance.strip():
            raise ValueError("event source_instance must not be empty")
        if not self.url.strip():
            raise ValueError("event url must not be empty")
        if self.schema_version < 1:
            raise ValueError("schema_version must be >= 1")
        if not isinstance(self.kind, SignalKind):
            object.__setattr__(self, "kind", SignalKind(str(self.kind)))
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "collected_at", _utc(self.collected_at))
        if self.updated_at is not None:
            object.__setattr__(self, "updated_at", _utc(self.updated_at))

    @classmethod
    def stable_id(
        cls,
        source: str,
        external_id: str,
        kind: SignalKind | str,
        *,
        source_instance: str = "default",
    ) -> str:
        """Build a deterministic ID from immutable source identity."""

        kind_value = kind.value if isinstance(kind, SignalKind) else str(kind)
        value = (
            f"{source.strip().lower()}\x1f{source_instance.strip().lower()}\x1f"
            f"{kind_value}\x1f{external_id.strip()}"
        )
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
        return f"sig_{digest}"

    @property
    def source_key(self) -> str:
        return f"{self.source}:{self.source_instance}"

    def fingerprint(self) -> str:
        """Hash source-visible fields for idempotent insert/update detection."""

        payload = {
            "schema_version": self.schema_version,
            "source": self.source,
            "source_instance": self.source_instance,
            "kind": self.kind.value,
            "title": self.title,
            "content": self.content,
            "author": self.author,
            "url": self.url,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata": dict(self.metadata),
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["kind"] = self.kind.value
        data["created_at"] = self.created_at.isoformat()
        data["collected_at"] = self.collected_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat() if self.updated_at else None
        data["metadata"] = dict(self.metadata)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SignalEvent:
        collected_at = data.get("collected_at")
        updated_at = data.get("updated_at")
        return cls(
            id=str(data["id"]),
            source=str(data["source"]),
            source_instance=str(data.get("source_instance", "default")),
            kind=SignalKind(str(data["kind"])),
            title=data.get("title"),
            content=str(data.get("content", "")),
            author=data.get("author"),
            url=str(data["url"]),
            created_at=datetime.fromisoformat(str(data["created_at"])),
            collected_at=(
                datetime.fromisoformat(str(collected_at)) if collected_at else datetime.now(UTC)
            ),
            updated_at=datetime.fromisoformat(str(updated_at)) if updated_at else None,
            metadata=data.get("metadata", {}),
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
        )
