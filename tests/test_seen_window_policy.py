"""One documented contract for the `seen_window` option across every adapter.

The option used to mean two different things: the feed/HN/Reddit adapters silently
clamped with ``max(50, seen_window)`` while the REST adapter raised below 100. The
project-wide policy is now fail-fast against a single floor, so an operator who asks
for a dedup window never silently gets a different one.
"""

from __future__ import annotations

import pytest

from signalkit_stream.collectors._text import MIN_SEEN_WINDOW
from signalkit_stream.collectors.hackernews import HackerNewsCollector
from signalkit_stream.collectors.jsonfeed import JSONFeedCollector
from signalkit_stream.collectors.reddit import RedditCollector
from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.config import SourceConfig
from signalkit_stream.registry import default_registry
from signalkit_stream.rest_config import build_generic_rest_collector


def _build(name: str, seen_window: int):
    if name == "jsonfeed":
        return JSONFeedCollector("https://example.com/feed.json", seen_window=seen_window)
    if name == "hackernews":
        return HackerNewsCollector(seen_window=seen_window)
    if name == "reddit":
        return RedditCollector(
            "python",
            access_token="token",
            user_agent="signalkit-test",
            seen_window=seen_window,
        )
    return GenericRESTCollector(
        "https://example.com/api",
        items_path="items",
        id_path="id",
        seen_window=seen_window,
    )


ADAPTERS = ["jsonfeed", "hackernews", "reddit", "rest"]


@pytest.mark.parametrize("name", ADAPTERS)
def test_seen_window_below_the_floor_is_rejected_not_clamped(name: str) -> None:
    with pytest.raises(ValueError, match=f"seen_window must be >= {MIN_SEEN_WINDOW}"):
        _build(name, MIN_SEEN_WINDOW - 1)


@pytest.mark.parametrize("name", ADAPTERS)
def test_seen_window_at_the_floor_is_honoured_exactly(name: str) -> None:
    collector = _build(name, MIN_SEEN_WINDOW)

    assert collector.seen_window == MIN_SEEN_WINDOW


@pytest.mark.parametrize("name", ADAPTERS)
def test_seen_window_must_be_an_integer(name: str) -> None:
    with pytest.raises(ValueError, match="seen_window must be an integer"):
        _build(name, True)  # type: ignore[arg-type]


def test_registry_reports_the_floor_with_the_source_name() -> None:
    config = SourceConfig(name="hn-fast", type="hackernews", options={"seen_window": 10})

    with pytest.raises(ValueError, match="source 'hn-fast': seen_window must be >= 50"):
        default_registry().create(config)


def test_rest_config_reports_the_floor_with_the_source_name() -> None:
    config = SourceConfig(
        name="rest-fast",
        type="rest",
        options={
            "url": "https://example.com/api",
            "items_path": "items",
            "id_path": "id",
            "seen_window": 10,
        },
    )

    with pytest.raises(ValueError, match="source 'rest-fast': seen_window must be >= 50"):
        build_generic_rest_collector(config)
