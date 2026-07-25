from datetime import UTC, datetime

from signalkit_stream.models import SignalEvent, SignalKind


def test_stable_id_is_deterministic() -> None:
    first = SignalEvent.stable_id("github", "abc", SignalKind.ISSUE)
    second = SignalEvent.stable_id("github", "abc", "issue")
    assert first == second
    assert first.startswith("sig_")


def test_event_serialization_round_trip() -> None:
    event = SignalEvent(
        id="sig_1",
        source="test",
        kind=SignalKind.POST,
        title="Hello",
        content="World",
        author="alice",
        url="https://example.com/1",
        created_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        metadata={"score": 3},
    )

    restored = SignalEvent.from_dict(event.to_dict())

    assert restored == event
    assert restored.created_at.tzinfo is not None
