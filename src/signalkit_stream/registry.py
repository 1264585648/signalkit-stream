from __future__ import annotations

from collections.abc import Callable
import os
from typing import Any

from signalkit_stream.collectors import Collector, GitHubCollector, HackerNewsCollector, RSSCollector
from signalkit_stream.config import ConfigError, SourceConfig

CollectorFactory = Callable[[SourceConfig], Collector]


class CollectorRegistry:
    """Maps configuration source types to collector factories."""

    def __init__(self) -> None:
        self._factories: dict[str, CollectorFactory] = {}

    def register(self, source_type: str, factory: CollectorFactory) -> None:
        key = source_type.strip().lower()
        if not key:
            raise ValueError("source type must not be empty")
        self._factories[key] = factory

    def create(self, config: SourceConfig) -> Collector:
        factory = self._factories.get(config.type.lower())
        if factory is None:
            available = ", ".join(sorted(self._factories)) or "none"
            raise ConfigError(
                f"source {config.name!r}: unknown type {config.type!r}; available: {available}"
            )
        return factory(config)

    @property
    def source_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


def default_registry() -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register("rss", _build_rss)
    registry.register("hackernews", _build_hackernews)
    registry.register("hn", _build_hackernews)
    registry.register("github", _build_github)
    return registry


def _build_rss(config: SourceConfig) -> Collector:
    options = dict(config.options)
    url = _required_string(options, "url", config)
    source = _optional_string(options.pop("source", "rss"), "source", config)
    timeout = _optional_number(options.pop("timeout", 20.0), "timeout", config)
    _reject_unknown(options, config)
    return RSSCollector(
        url,
        source=source,
        instance=config.name,
        timeout=timeout,
    )


def _build_hackernews(config: SourceConfig) -> Collector:
    options = dict(config.options)
    feed = _optional_string(options.pop("feed", "newstories"), "feed", config)
    comments = _optional_integer(options.pop("comments", 0), "comments", config)
    timeout = _optional_number(options.pop("timeout", 20.0), "timeout", config)
    seen_window = _optional_integer(options.pop("seen_window", 500), "seen_window", config)
    _reject_unknown(options, config)
    return HackerNewsCollector(
        feed=feed,  # type: ignore[arg-type]
        include_comments=comments > 0,
        comments_per_story=max(0, comments),
        timeout=timeout,
        seen_window=seen_window,
        instance=config.name,
    )


def _build_github(config: SourceConfig) -> Collector:
    options = dict(config.options)
    query = _required_string(options, "query", config)
    comments = _optional_integer(options.pop("comments", 0), "comments", config)
    timeout = _optional_number(options.pop("timeout", 20.0), "timeout", config)
    token_env = _optional_string(options.pop("token_env", "GITHUB_TOKEN"), "token_env", config)
    _reject_unknown(options, config)
    return GitHubCollector(
        query,
        token=os.getenv(token_env),
        include_comments=comments > 0,
        comments_per_item=max(0, comments),
        timeout=timeout,
        instance=config.name,
    )


def _required_string(options: dict[str, Any], key: str, config: SourceConfig) -> str:
    if key not in options:
        raise ConfigError(f"source {config.name!r}: options.{key} is required")
    value = options.pop(key)
    return _optional_string(value, key, config)


def _optional_string(value: Any, key: str, config: SourceConfig) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"source {config.name!r}: options.{key} must be a non-empty string")
    return value


def _optional_integer(value: Any, key: str, config: SourceConfig) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"source {config.name!r}: options.{key} must be an integer")
    return value


def _optional_number(value: Any, key: str, config: SourceConfig) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"source {config.name!r}: options.{key} must be a number")
    if float(value) <= 0:
        raise ConfigError(f"source {config.name!r}: options.{key} must be > 0")
    return float(value)


def _reject_unknown(options: dict[str, Any], config: SourceConfig) -> None:
    if options:
        raise ConfigError(
            f"source {config.name!r}: unknown options: {', '.join(sorted(options))}"
        )
