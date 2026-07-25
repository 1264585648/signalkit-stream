import pytest

from signalkit_stream.config import SourceConfig
from signalkit_stream.models import SignalKind
from signalkit_stream.rest_config import build_generic_rest_collector


def test_build_generic_rest_collector_from_source_config(monkeypatch) -> None:
    monkeypatch.setenv("EXAMPLE_TOKEN", "secret")
    collector = build_generic_rest_collector(
        SourceConfig(
            "example-api",
            "rest",
            options={
                "url": "https://api.example.com/issues",
                "items_path": "data.items",
                "id_path": "id",
                "kind": "issue",
                "title_path": "title",
                "content_path": "body",
                "pagination": "cursor",
                "next_cursor_path": "data.next",
                "cursor_param": "after",
                "limit_param": "limit",
                "params": {"state": "open"},
                "headers": {"Accept": "application/json"},
                "token_env": "EXAMPLE_TOKEN",
                "metadata_paths": {"state": "state", "labels": "labels"},
                "initial_backfill": True,
                "seen_window": 500,
            },
        )
    )

    assert collector.kind is SignalKind.ISSUE
    assert collector.pagination == "cursor"
    assert collector.next_cursor_path == "data.next"
    assert collector.params == {"state": "open"}
    assert collector.headers["Authorization"] == "Bearer secret"
    assert collector.metadata_paths == {"state": "state", "labels": "labels"}
    assert collector.initial_backfill is True
    assert collector.seen_window == 500


def test_rest_config_rejects_missing_token_unknown_options_and_invalid_kind(monkeypatch) -> None:
    monkeypatch.delenv("MISSING_TOKEN", raising=False)
    base = {
        "url": "https://api.example.com/items",
        "items_path": "items",
        "id_path": "id",
    }

    with pytest.raises(ValueError, match="environment variable MISSING_TOKEN is not set"):
        build_generic_rest_collector(
            SourceConfig("rest", "rest", options={**base, "token_env": "MISSING_TOKEN"})
        )

    with pytest.raises(ValueError, match="unknown rest options"):
        build_generic_rest_collector(
            SourceConfig("rest", "rest", options={**base, "mystery": True})
        )

    with pytest.raises(ValueError, match="kind must be one of"):
        build_generic_rest_collector(
            SourceConfig("rest", "rest", options={**base, "kind": "banana"})
        )
