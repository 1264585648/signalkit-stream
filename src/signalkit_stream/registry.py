from __future__ import annotations

from collections.abc import Callable
import os
from typing import Any, Mapping

from signalkit_stream.collectors import Collector, GitHubCollector, HackerNewsCollector, RSSCollector
from signalkit_stream.config import SourceConfig

CollectorFactory = Callable[[SourceConfig], Collector]


class CollectorRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, CollectorFactory] = {}

    def register(self, source_type: str, factory: CollectorFactory) -> None:
        key = source_type.strip().lower()
        if not key:
            raise ValueError("source type must not be empty")
        if key in self._factories:
            raise ValueError(f"collector type already registered: {key}")
        self._factories[key] = factory

    def create(self, config: SourceConfig) -> Collector:
        source_type = config.type.strip().lower()
        try:
            factory = self._factories[source_type]
        except KeyError as exc:
            known = ", ".join(sorted(self._factories)) or "none"
            raise ValueError(f"unknown collector type {source_type!r}; registered: {known}") from exc
        return factory(config)

    @property
    def types(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


def default_registry() -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register("rss", _rss_factory)
    registry.register("hackernews", _hackernews_factory)
    registry.register("github", _github_factory)
    return registry


def _rss_factory(config: SourceConfig) -> Collector:
    options = dict(config.options)
    _reject_unknown(options, {"url", "source", "instance"}, config)
    url = _required_string(options, "url", config)
    return RSSCollector(
        url,
        source=str(options.get("source", "rss")),
        instance=_optional_string(options.get("instance")),
    )


def _hackernews_factory(config: SourceConfig) -> Collector:
    options = dict(config.options)
    _reject_unknown(options, {"feed", "comments", "seen_window"}, config)
    comments = _nonnegative_int(options.get("comments", 0), "comments", config)
    seen_window = _positive_int(options.get("seen_window", 500), "seen_window", config)
    feed = str(options.get("feed", "newstories"))
    allowed = {
        "topstories",
        "newstories",
        "beststories",
        "askstories",
        "showstories",
        "jobstories",
    }
    if feed not in allowed:
        raise ValueError(f"source {config.name!r}: unsupported Hacker News feed {feed!r}")
    return HackerNewsCollector(
        feed=feed,  # type: ignore[arg-type]
        include_comments=comments > 0,
        comments_per_story=comments,
        seen_window=seen_window,
    )


def _github_factory(config: SourceConfig) -> Collector:
    options = dict(config.options)
    _reject_unknown(options, {"query", "comments", "token_env", "instance"}, config)
    query = _required_string(options, "query", config)
    comments = _nonnegative_int(options.get("comments", 0), "comments", config)
    token_env = str(options.get("token_env", "GITHUB_TOKEN")).strip()
    if not token_env:
        raise ValueError(f"source {config.name!r}: token_env must not be empty")
    return GitHubCollector(
        query,
        token=os.getenv(token_env),
        include_comments=comments > 0,
        comments_per_item=comments,
        instance=_optional_string(options.get("instance")),
    )


def _reject_unknown(options: Mapping[str, Any], allowed: set[str], config: SourceConfig) -> None:
    unknown = set(options) - allowed
    if unknown:
        raise ValueError(
            f"source {config.name!r}: unknown {config.type} options: {', '.join(sorted(unknown))}"
        )


def _required_string(options: Mapping[str, Any], key: str, config: SourceConfig) -> str:
    value = str(options.get(key, "")).strip()
    if not value:
        raise ValueError(f"source {config.name!r}: {key} is required")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _nonnegative_int(value: Any, key: str, config: SourceConfig) -> int:
    parsed = _integer(value, key, config)
    if parsed < 0:
        raise ValueError(f"source {config.name!r}: {key} must be >= 0")
    return parsed


def _positive_int(value: Any, key: str, config: SourceConfig) -> int:
    parsed = _integer(value, key, config)
    if parsed < 1:
        raise ValueError(f"source {config.name!r}: {key} must be >= 1")
    return parsed


def _integer(value: Any, key: str, config: SourceConfig) -> int:
    if isinstance(value, bool):
        raise ValueError(f"source {config.name!r}: {key} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"source {config.name!r}: {key} must be an integer") from exc
