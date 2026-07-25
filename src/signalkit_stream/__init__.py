"""SignalKit Stream: source ingestion for AI agents."""

from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.pipeline import CollectionResult, run_collector
from signalkit_stream.storage import SQLiteSignalStore

__all__ = [
    "CollectionResult",
    "SignalEvent",
    "SignalKind",
    "SQLiteSignalStore",
    "run_collector",
]

__version__ = "0.1.0"
