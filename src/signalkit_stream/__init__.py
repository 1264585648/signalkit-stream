"""SignalKit Stream: reliable public-signal ingestion for AI agents."""

from signalkit_stream.config import (
    RuntimeConfig,
    SinkConfig,
    SourceConfig,
    StreamConfig,
    load_config,
)
from signalkit_stream.contracts import validate_collector_result
from signalkit_stream.delivery import DeliveryEngine, DeliveryResult
from signalkit_stream.diagnostics import (
    DiagnosticCheck,
    DiagnosticReport,
    DiagnosticStatus,
    doctor,
    validate_config_file,
    validate_stream_config,
)
from signalkit_stream.logging_utils import (
    JsonLogFormatter,
    LogFormat,
    TextLogFormatter,
    configure_logging,
)
from signalkit_stream.maintenance import (
    BackupResult,
    VerifyResult,
    backup_database,
    verify_database,
)
from signalkit_stream.migrations import (
    DATABASE_SCHEMA_VERSION,
    DatabaseMigrationError,
    DatabaseSchemaError,
    DatabaseSchemaTooNew,
    get_database_schema_version,
    migrate_database,
    validate_database_schema,
)
from signalkit_stream.models import SCHEMA_VERSION, SignalEvent, SignalKind
from signalkit_stream.observability import (
    SinkStatusSnapshot,
    SourceStatusSnapshot,
    StreamSnapshot,
    format_snapshot,
    read_snapshot,
)
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
    "BackupResult",
    "Checkpoint",
    "CollectionResult",
    "CollectorContext",
    "CollectorError",
    "CollectorErrorKind",
    "CollectorResult",
    "Cursor",
    "DATABASE_SCHEMA_VERSION",
    "DatabaseMigrationError",
    "DatabaseSchemaError",
    "DatabaseSchemaTooNew",
    "DeliveryEngine",
    "DeliveryRecord",
    "DeliveryResult",
    "DiagnosticCheck",
    "DiagnosticReport",
    "DiagnosticStatus",
    "JsonLogFormatter",
    "JsonlSink",
    "LogFormat",
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
    "SinkStatusSnapshot",
    "SourceConfig",
    "SourceHealth",
    "SourceIdentity",
    "SourceRunResult",
    "SourceStatusSnapshot",
    "StdoutSink",
    "StoreWriteResult",
    "StreamConfig",
    "StreamRuntime",
    "StreamSnapshot",
    "TextLogFormatter",
    "VerifyResult",
    "WebhookSink",
    "backup_database",
    "configure_logging",
    "doctor",
    "format_snapshot",
    "get_database_schema_version",
    "load_config",
    "migrate_database",
    "read_snapshot",
    "run_collector",
    "validate_collector_result",
    "validate_config_file",
    "validate_database_schema",
    "validate_stream_config",
    "verify_database",
]

__version__ = "0.7.0"
