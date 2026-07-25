"""SignalKit Stream: reliable public-signal ingestion for AI agents."""

from signalkit_stream.config import RuntimeConfig, SourceConfig, StreamConfig, load_config
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
from signalkit_stream.runtime import SourceRunResult, StreamRuntime
from signalkit_stream.storage import (
    Checkpoint,
    SQLiteSignalStore,
    SourceHealth,
    StoreWriteResult,
)

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
    "RuntimeConfig",
    "SCHEMA_VERSION",
    "SQLiteSignalStore",
    "SignalEvent",
    "SignalKind",
    "SourceConfig",
    "SourceHealth",
    "SourceIdentity",
    "SourceRunResult",
    "StoreWriteResult",
    "StreamConfig",
    "StreamRuntime",
    "load_config",
    "run_collector",
]

__version__ = "0.3.0"
