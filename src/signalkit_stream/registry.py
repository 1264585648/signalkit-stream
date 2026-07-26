from __future__ import annotations

from collections.abc import Callable
import os
from typing import Any, Mapping

from signalkit_stream.collectors import (
    Collector,
    GitHubCollector,
    HackerNewsCollector,
    JSONFeedCollector,
    RSSCollector,
    RedditCollector,
)
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
    registry.register("jsonfeed", _jsonfeed_factory)
    registry.register("hackernews", _hackernews_factory)
    registry.register("github", _github_factory)
    registry.register("reddit", _reddit_factory)
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


def _jsonfeed_factory(config: SourceConfig) -> Collector:
    options = dict(config.options)
    _reject_unknown(options, {"url", "source", "instance", "seen_window"}, config)
    url = _required_string(options, "url", config)
    seen_window = _positive_int(options.get("seen_window", 500), "seen_window", config)
    return JSONFeedCollector(
        url,
        source=str(options.get("source", "jsonfeed")),
        instance=_optional_string(options.get("instance")) or config.name,
        seen_window=seen_window,
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


def _reddit_factory(config: SourceConfig) -> Collector:
    options = dict(config.options)
    _reject_unknown(
        options,
        {
            "subreddit",
            "listing",
            "time_filter",
            "comments",
            "seen_window",
            "instance",
            "access_token_env",
            "refresh_token_env",
            "client_id_env",
            "client_secret_env",
            "user_agent_env",
        },
        config,
    )
    subreddit = _required_string(options, "subreddit", config)
    listing = str(options.get("listing", "new")).strip().lower()
    time_filter = _optional_string(options.get("time_filter"))
    comments = _nonnegative_int(options.get("comments", 0), "comments", config)
    seen_window = _positive_int(options.get("seen_window", 500), "seen_window", config)

    access_token = _optional_environment(
        options.get("access_token_env", "REDDIT_ACCESS_TOKEN"),
        "access_token_env",
        config,
    )
    refresh_token = _optional_environment(
        options.get("refresh_token_env", "REDDIT_REFRESH_TOKEN"),
        "refresh_token_env",
        config,
    )
    client_id = _optional_environment(
        options.get("client_id_env", "REDDIT_CLIENT_ID"),
        "client_id_env",
        config,
    )
    client_secret = _optional_environment(
        options.get("client_secret_env", "REDDIT_CLIENT_SECRET"),
        "client_secret_env",
        config,
    )
    user_agent = _required_environment(
        options.get("user_agent_env", "REDDIT_USER_AGENT"),
        "user_agent_env",
        config,
    )

    if refresh_token and not client_id:
        env_name = str(options.get("client_id_env", "REDDIT_CLIENT_ID")).strip()
        raise ValueError(
            f"source {config.name!r}: environment variable {env_name!r} required "
            "when a Reddit refresh token is configured"
        )
    if not access_token and not refresh_token:
        client_id = client_id or _required_environment(
            options.get("client_id_env", "REDDIT_CLIENT_ID"),
            "client_id_env",
            config,
        )
        client_secret = client_secret or _required_environment(
            options.get("client_secret_env", "REDDIT_CLIENT_SECRET"),
            "client_secret_env",
            config,
        )

    return RedditCollector(
        subreddit,
        access_token=access_token,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        listing=listing,  # type: ignore[arg-type]
        time_filter=time_filter,  # type: ignore[arg-type]
        include_comments=comments > 0,
        comments_per_post=comments,
        seen_window=seen_window,
        instance=_optional_string(options.get("instance")) or config.name,
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


def _required_environment(value: Any, key: str, config: SourceConfig) -> str:
    env_name = str(value).strip()
    if not env_name:
        raise ValueError(f"source {config.name!r}: {key} must not be empty")
    resolved = os.getenv(env_name)
    if not resolved:
        raise ValueError(
            f"source {config.name!r}: environment variable {env_name!r} required by {key} is not set"
        )
    return resolved


def _optional_environment(value: Any, key: str, config: SourceConfig) -> str | None:
    env_name = str(value).strip()
    if not env_name:
        raise ValueError(f"source {config.name!r}: {key} must not be empty")
    resolved = os.getenv(env_name)
    return resolved if resolved else None


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
