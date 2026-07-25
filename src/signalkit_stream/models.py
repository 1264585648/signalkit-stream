from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
import hashlib
from typing import Any, Mapping


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
    """Normalized event handed from SignalKit Stream to downstream consumers."""

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

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("event id must not be empty")
        if not self.source.strip():
            raise ValueError("event source must not be empty")
        if not self.url.strip():
            raise ValueError("event url must not be empty")
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "collected_at", _utc(self.collected_at))

    @classmethod
    def stable_id(cls, source: str, external_id: str, kind: SignalKind | str) -> str:
        """Build a deterministic id so repeated collection is naturally deduplicated."""

        value = f"{source.strip().lower()}\x1f{kind}\x1f{external_id.strip()}"
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
        return f"sig_{digest}"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["kind"] = self.kind.value
        data["created_at"] = self.created_at.isoformat()
        data["collected_at"] = self.collected_at.isoformat()
        data["metadata"] = dict(self.metadata)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SignalEvent:
        return cls(
            id=str(data["id"]),
            source=str(data["source"]),
            kind=SignalKind(str(data["kind"])),
            title=data.get("title"),
            content=str(data.get("content", "")),
            author=data.get("author"),
            url=str(data["url"]),
            created_at=datetime.fromisoformat(str(data["created_at"])),
            collected_at=datetime.fromisoformat(str(data["collected_at"])),
            metadata=data.get("metadata", {}),
        )
