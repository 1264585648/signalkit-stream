import pytest

from signalkit_stream.protocol import CollectorError, CollectorErrorKind, Cursor, SourceIdentity


def test_cursor_round_trip() -> None:
    cursor = Cursor(source_key="github:search-1", state={"page": 2, "watermark": "x"})
    assert Cursor.from_json(cursor.to_json()) == cursor


def test_source_identity_key() -> None:
    assert SourceIdentity("github", "lead-search").key == "github:lead-search"


def test_collector_error_has_stable_payload() -> None:
    error = CollectorError(
        "rate limited",
        kind=CollectorErrorKind.RATE_LIMIT,
        source_key="github:x",
        retryable=True,
        status_code=429,
    )
    assert error.to_dict()["kind"] == "rate_limit"
    assert error.to_dict()["retryable"] is True


def test_cursor_rejects_empty_source_key() -> None:
    with pytest.raises(ValueError):
        Cursor(source_key="")
