"""SignalKit Stream: reliable public-signal ingestion for AI agents."""

from signalkit_stream.models import SCHEMA_VERSION, SignalEvent, SignalKind
from signalkit_stream.pipeline import CollectionResult, run_collector
from signalkit_stream.protocol import (
    PROTOCOL_VERSION,
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
    RateLimitSnapshot,
    RawEvent,
    SourceIdentity,
)
from signalkit_stream.storage import Checkpoint, SQLiteSignalStore, StoreWriteResult

__all__ = [
    "Checkpoint",
    "CollectionResult",
    "CollectorContext",
    "CollectorError",
    "CollectorErrorKind",
    "CollectorResult",
    "Cursor",
    "PROTOCOL_VERSION",
    "RateLimitSnapshot",
    "RawEvent",
    "SCHEMA_VERSION",
    "SQLiteSignalStore",
    "SignalEvent",
    "SignalKind",
    "SourceIdentity",
    "StoreWriteResult",
    "run_collector",
]

__version__ = "0.2.0"
