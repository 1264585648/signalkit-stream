from datetime import UTC, datetime

from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def make_event(event_id: str = "sig_1") -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source="test",
        kind=SignalKind.POST,
        title="A title",
        content="A body",
        author="alice",
        url="https://example.com/1",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        collected_at=datetime(2026, 7, 25, tzinfo=UTC),
        metadata={"x": 1},
    )


def test_sqlite_store_deduplicates(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        assert store.save_many([make_event()]) == 1
        assert store.save_many([make_event()]) == 0
        assert store.count() == 1
        events = store.list_recent(limit=10)

    assert events == [make_event()]
