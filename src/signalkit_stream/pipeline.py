from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Mapping

from signalkit_stream.collectors.base import Collector
from signalkit_stream.contracts import validate_collector_result
from signalkit_stream.models import SignalEvent
from signalkit_stream.protocol import CollectorContext, CollectorError, Cursor, RateLimitSnapshot
from signalkit_stream.storage import SignalStore, StoreWriteResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CollectionResult:
    events: list[SignalEvent]
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    cursor: Cursor | None = None
    has_more: bool = False
    pages: int = 0
    primary_count: int = 0
    rate_limit: RateLimitSnapshot | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def changed(self) -> int:
        return self.inserted + self.updated


def _record_failure(store: SignalStore | None, source_key: str, error: str) -> None:
    """Best-effort failure bookkeeping that can never mask the original exception.

    ``record_failure`` opens its own write transaction, so it is most likely to raise
    exactly when the collection failure was itself database related (locked database,
    disk full). Letting that secondary error escape would replace the real
    ``CollectorError`` on its way out of ``run_collector`` and lose the failure record
    *and* the diagnosis.
    """

    if store is None:
        return
    try:
        store.record_failure(source_key, error)
    except Exception:
        logger.exception("failed to record collection failure for source=%s", source_key)


async def run_collector(
    collector: Collector,
    *,
    limit: int = 100,
    store: SignalStore | None = None,
    resume: bool = True,
    max_pages: int = 100,
    metadata: Mapping[str, Any] | None = None,
) -> CollectionResult:
    """Drain resumable collector pages up to ``limit`` primary items.

    Each page is contract-validated before it can touch persistence, then atomically
    stored with its checkpoint. If a later page fails, a subsequent run resumes from
    the last committed page, yielding at-least-once collection with idempotent storage.
    """

    if limit < 1:
        raise ValueError("limit must be >= 1")
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")

    source_key = collector.identity.key
    cursor: Cursor | None = None
    if store is not None and resume:
        checkpoint = store.get_checkpoint(source_key)
        if checkpoint is not None:
            cursor = checkpoint.cursor

    events: list[SignalEvent] = []
    writes = StoreWriteResult()
    warnings: list[str] = []
    primary_count = 0
    pages = 0
    last_rate_limit: RateLimitSnapshot | None = None
    has_more = False

    try:
        while primary_count < limit and pages < max_pages:
            remaining = limit - primary_count
            context = CollectorContext(limit=remaining, metadata=metadata or {})
            previous_cursor = cursor
            page = await collector.collect(context=context, cursor=cursor)
            validate_collector_result(
                collector,
                page,
                context=context,
                previous_cursor=previous_cursor,
            )
            pages += 1
            events.extend(page.events)
            warnings.extend(page.warnings)
            primary_count += page.primary_count
            last_rate_limit = page.rate_limit or last_rate_limit
            has_more = page.has_more
            cursor = page.cursor

            if store is not None:
                writes = writes + store.commit_batch(
                    page.events,
                    source_key=source_key,
                    cursor=cursor,
                )

            if not page.has_more:
                break
            if page.primary_count == 0:
                warnings.append("collector reported has_more with zero primary items; stopped")
                break
            if cursor == previous_cursor:
                warnings.append("collector cursor did not advance; stopped to prevent pagination loop")
                break

        if pages >= max_pages and has_more:
            warnings.append(f"stopped after max_pages={max_pages}")
    except CollectorError as exc:
        _record_failure(store, source_key, str(exc))
        raise
    except Exception as exc:
        _record_failure(store, source_key, repr(exc))
        raise

    return CollectionResult(
        events=events,
        inserted=writes.inserted,
        updated=writes.updated,
        unchanged=writes.unchanged,
        cursor=cursor,
        has_more=has_more,
        pages=pages,
        primary_count=primary_count,
        rate_limit=last_rate_limit,
        warnings=warnings,
    )
