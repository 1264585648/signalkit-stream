from datetime import UTC, datetime

import pytest

from signalkit_stream.models import (
    ALLOWED_URL_SCHEMES,
    MAX_AUTHOR_LENGTH,
    MAX_CONTENT_LENGTH,
    MAX_METADATA_BYTES,
    MAX_TITLE_LENGTH,
    SignalEvent,
    SignalKind,
)


def make_event(**overrides) -> SignalEvent:
    fields = {
        "id": "sig_1",
        "source": "test",
        "kind": SignalKind.POST,
        "content": "body",
        "url": "https://example.com/1",
        "created_at": datetime(2026, 7, 26, tzinfo=UTC),
    }
    fields.update(overrides)
    return SignalEvent(**fields)


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


@pytest.mark.parametrize(
    "url",
    [
        'javascript:fetch("//evil")',
        "JavaScript:alert(1)",
        "data:text/html;base64,PHNjcmlwdD4=",
        "vbscript:msgbox(1)",
        "file:///etc/passwd",
        "ftp://example.com/x",
        "/relative/path",
        "example.com/no-scheme",
    ],
)
def test_disallowed_url_schemes_are_rejected(url: str) -> None:
    with pytest.raises(ValueError, match="url scheme"):
        make_event(url=url)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/1",
        "https://example.com/1",
        "HTTPS://example.com/1",
        "  https://example.com/1  ",
        "https://example.com/path?q=1#frag",
    ],
)
def test_http_urls_are_accepted(url: str) -> None:
    assert make_event(url=url).url == url


def test_allowed_scheme_set_is_http_only() -> None:
    # No shipped collector emits mailto: (each falls back to its own feed URL), so the
    # allowlist stays minimal. Widening it must be a deliberate, tested change.
    assert ALLOWED_URL_SCHEMES == frozenset({"http", "https"})


def test_url_is_still_required() -> None:
    with pytest.raises(ValueError, match="url must not be empty"):
        make_event(url="   ")


def test_oversized_title_content_and_author_are_truncated_with_a_marker() -> None:
    event = make_event(
        title="t" * (MAX_TITLE_LENGTH + 500),
        author="a" * (MAX_AUTHOR_LENGTH + 500),
        content="c" * (MAX_CONTENT_LENGTH + 500),
        metadata={"external_id": "abc"},
    )

    assert len(event.title) == MAX_TITLE_LENGTH
    assert len(event.author) == MAX_AUTHOR_LENGTH
    assert len(event.content) == MAX_CONTENT_LENGTH
    assert event.metadata["truncated"] is True
    # Untouched metadata entries survive truncation of the text fields.
    assert event.metadata["external_id"] == "abc"


def test_fields_at_the_cap_are_not_marked_truncated() -> None:
    event = make_event(
        title="t" * MAX_TITLE_LENGTH,
        author="a" * MAX_AUTHOR_LENGTH,
        content="c" * MAX_CONTENT_LENGTH,
    )

    assert len(event.title) == MAX_TITLE_LENGTH
    assert "truncated" not in event.metadata


def test_normal_event_metadata_is_left_exactly_as_provided() -> None:
    metadata = {"score": 3, "tags": ["a", "b"]}
    event = make_event(metadata=metadata)

    assert event.metadata == metadata
    assert "truncated" not in event.metadata


def test_oversized_metadata_drops_the_largest_entries_deterministically() -> None:
    metadata = {
        "external_id": "abc",
        "attachments": ["x" * 200_000],
        "tags": ["y" * 100_000],
    }
    event = make_event(metadata=metadata)
    twin = make_event(metadata=dict(metadata))

    assert event.metadata["truncated"] is True
    assert event.metadata["external_id"] == "abc"
    assert "attachments" not in event.metadata
    # Only as much is dropped as the cap requires: "tags" alone fits.
    assert event.metadata["tags"] == ["y" * 100_000]
    assert len(str(event.metadata).encode("utf-8")) < MAX_METADATA_BYTES

    # Deterministic, so recollecting the same item does not look like an update.
    assert event.fingerprint() == twin.fingerprint()


def test_truncation_survives_a_serialization_round_trip() -> None:
    event = make_event(title="t" * (MAX_TITLE_LENGTH + 1))
    restored = SignalEvent.from_dict(event.to_dict())

    assert restored == event
    assert restored.metadata["truncated"] is True
    assert restored.fingerprint() == event.fingerprint()


def test_unserializable_metadata_does_not_break_the_size_check() -> None:
    event = make_event(metadata={"when": datetime(2026, 7, 26, tzinfo=UTC)})

    assert "truncated" not in event.metadata
    assert event.metadata["when"] == datetime(2026, 7, 26, tzinfo=UTC)
