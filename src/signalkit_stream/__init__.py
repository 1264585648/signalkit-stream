"""SignalKit Stream: reliable public-signal ingestion for AI agents."""

from signalkit_stream.config import (
    DeliveryConfig,
    RuntimeConfig,
    SinkConfig,
    SourceConfig,
    StreamConfig,
    load_config,
)
from signalkit_stream.delivery import (
    DeliveryCandidate,
    DeliveryContext,
    DeliveryDispatcher,
    DeliveryRecord,
    DeliveryRunResult,
    DeliveryStatus,
    Sink,
    SinkError,
    SQLiteDeliveryStore,
    StdoutSink,
    WebhookSink,
    delivery_idempotency_key,
)
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
    "DeliveryCandidate",
    "DeliveryConfig",
    "DeliveryContext",
    "DeliveryDispatcher",
    "DeliveryRecord",
    "DeliveryRunResult",
    "DeliveryStatus",
    "PROTOCOL_VERSION",
    "RateLimitSnapshot",
    "RawEvent",
    "RuntimeConfig",
    "SCHEMA_VERSION",
    "SQLiteDeliveryStore",
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
    "delivery_idempotency_key",
    "load_config",
    "run_collector",
]

__version__ = "0.4.0"
