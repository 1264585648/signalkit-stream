from __future__ import annotations

from signalkit_stream.collectors.base import Collector
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)


def validate_collector_result(
    collector: Collector,
    result: CollectorResult,
    *,
    context: CollectorContext,
    previous_cursor: Cursor | None = None,
) -> None:
    """Validate the invariants required by the Stream runtime.

    This is both a runtime safety boundary and a reusable contract check for
    third-party adapters. It intentionally validates source-agnostic semantics,
    not source-specific payload details.
    """

    source_key = collector.identity.key
    if result.primary_count < 0:
        _violation(source_key, "primary_count must be >= 0")
    if result.primary_count > context.limit:
        _violation(
            source_key,
            f"primary_count={result.primary_count} exceeds context.limit={context.limit}",
        )
    if result.has_more and result.cursor is None:
        _violation(source_key, "has_more=True requires a continuation cursor")
    if result.cursor is not None and result.cursor.source_key != source_key:
        _violation(
            source_key,
            f"cursor belongs to {result.cursor.source_key!r}, expected {source_key!r}",
        )

    event_ids: set[str] = set()
    for event in result.events:
        if event.source_key != source_key:
            _violation(
                source_key,
                f"event {event.id!r} belongs to {event.source_key!r}, expected {source_key!r}",
            )
        if event.id in event_ids:
            _violation(source_key, f"duplicate event id in one collector batch: {event.id!r}")
        event_ids.add(event.id)
        if event.created_at.utcoffset() is None:
            _violation(source_key, f"event {event.id!r} created_at must be timezone-aware")
        if event.updated_at is not None and event.updated_at.utcoffset() is None:
            _violation(source_key, f"event {event.id!r} updated_at must be timezone-aware")
        if event.collected_at.utcoffset() is None:
            _violation(source_key, f"event {event.id!r} collected_at must be timezone-aware")

    if result.rate_limit is not None:
        snapshot = result.rate_limit
        if snapshot.limit is not None and snapshot.limit < 0:
            _violation(source_key, "rate limit must be >= 0")
        if snapshot.remaining is not None and snapshot.remaining < 0:
            _violation(source_key, "rate-limit remaining must be >= 0")
        if snapshot.retry_after is not None and snapshot.retry_after < 0:
            _violation(source_key, "rate-limit retry_after must be >= 0")

    if previous_cursor is not None and previous_cursor.source_key != source_key:
        _violation(
            source_key,
            f"previous cursor belongs to {previous_cursor.source_key!r}, expected {source_key!r}",
        )


def _violation(source_key: str, message: str) -> None:
    raise CollectorError(
        f"collector contract violation: {message}",
        kind=CollectorErrorKind.INTERNAL,
        source_key=source_key,
        retryable=False,
        details={"contract_violation": message},
    )
