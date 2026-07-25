from __future__ import annotations

from dataclasses import dataclass

from signalkit_stream.collectors.base import Collector
from signalkit_stream.models import SignalEvent
from signalkit_stream.storage import SQLiteSignalStore


@dataclass(slots=True)
class CollectionResult:
    events: list[SignalEvent]
    inserted: int


async def run_collector(
    collector: Collector,
    *,
    limit: int = 100,
    store: SQLiteSignalStore | None = None,
) -> CollectionResult:
    """Run a collector and optionally persist its events."""

    events = await collector.collect(limit=limit)
    inserted = store.save_many(events) if store is not None else 0
    return CollectionResult(events=events, inserted=inserted)
