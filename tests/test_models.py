from datetime import UTC, datetime

from signalkit_stream.models import SignalEvent, SignalKind


def test_stable_id_is_deterministic() -> None:
    first = SignalEvent.stable_id("github", "abc", SignalKind.ISSUE)
    second = SignalEvent.stable_id("github", "abc", "issue")
    third = SignalEvent.stable_id("github", "abc", "issue", source_instance="other")
    assert first == second
    assert first != third
    assert first.startswith("sig_")


def test_event_serialization_round_trip_and_fingerprint_ignores_collection_time() -> None:
    event = SignalEvent(
        id="sig_1",
        source="test",
        source_instance="source-a",
        kind=SignalKind.POST,
        title="Hello",
        content="World",
        author="alice",
        url="https://example.com/1",
        created_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 1, 2, 12, 0, tzinfo=UTC),
        collected_at=datetime(2026, 1, 3, 12, 0, tzinfo=UTC),
        metadata={"score": 3},
    )
    restored = SignalEvent.from_dict(event.to_dict())
    later = SignalEvent.from_dict({**event.to_dict(), "collected_at": "2026-01-04T12:00:00+00:00"})

    assert restored == event
    assert restored.created_at.tzinfo is not None
    assert event.fingerprint() == later.fingerprint()
