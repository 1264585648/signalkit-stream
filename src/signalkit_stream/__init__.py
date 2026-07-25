"""SignalKit Stream: reliable public-signal ingestion for AI agents."""

from signalkit_stream.config import (
    RuntimeConfig,
    SinkConfig,
    SourceConfig,
    StreamConfig,
    load_config,
)
from signalkit_stream.delivery import DeliveryEngine, DeliveryResult
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
from signalkit_stream.sinks import JsonlSink, Sink, SinkError, StdoutSink, WebhookSink
from signalkit_stream.storage import (
    Checkpoint,
    DeliveryRecord,
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
    "DeliveryEngine",
    "DeliveryRecord",
    "DeliveryResult",
    "JsonlSink",
    "PROTOCOL_VERSION",
    "RateLimitSnapshot",
    "RawEvent",
    "RuntimeConfig",
    "SCHEMA_VERSION",
    "SQLiteSignalStore",
    "SignalEvent",
    "SignalKind",
    "Sink",
    "SinkConfig",
    "SinkError",
    "SourceConfig",
    "SourceHealth",
    "SourceIdentity",
    "SourceRunResult",
    "StdoutSink",
    "StoreWriteResult",
    "StreamConfig",
    "StreamRuntime",
    "WebhookSink",
    "load_config",
    "run_collector",
]

__version__ = "0.5.0"
