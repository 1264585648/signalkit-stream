from __future__ import annotations

from collections.abc import Callable
import os
from typing import Any, Mapping

from signalkit_stream.collectors import (
    Collector,
    GitHubCollector,
    HackerNewsCollector,
    RSSCollector,
    RedditCollector,
    RedditOAuth,
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
            "access_token_env",
            "client_id_env",
            "client_secret_env",
            "refresh_token_env",
            "user_agent",
            "user_agent_env",
            "instance",
            "seen_window",
        },
        config,
    )
    subreddit = _required_string(options, "subreddit", config)
    listing = str(options.get("listing", "posts")).strip().lower()
    if listing not in {"posts", "comments"}:
        raise ValueError(f"source {config.name!r}: listing must be 'posts' or 'comments'")

    access_token_env = str(options.get("access_token_env", "REDDIT_ACCESS_TOKEN")).strip()
    client_id_env = str(options.get("client_id_env", "REDDIT_CLIENT_ID")).strip()
    client_secret_env = str(options.get("client_secret_env", "REDDIT_CLIENT_SECRET")).strip()
    refresh_token_env = str(options.get("refresh_token_env", "REDDIT_REFRESH_TOKEN")).strip()
    user_agent_env = str(options.get("user_agent_env", "REDDIT_USER_AGENT")).strip()
    for key, value in {
        "access_token_env": access_token_env,
        "client_id_env": client_id_env,
        "client_secret_env": client_secret_env,
        "refresh_token_env": refresh_token_env,
        "user_agent_env": user_agent_env,
    }.items():
        if not value:
            raise ValueError(f"source {config.name!r}: {key} must not be empty")

    user_agent = str(options.get("user_agent") or os.getenv(user_agent_env) or "").strip()
    if not user_agent:
        raise ValueError(
            f"source {config.name!r}: configure user_agent or set {user_agent_env}"
        )

    oauth = RedditOAuth(
        access_token=os.getenv(access_token_env),
        client_id=os.getenv(client_id_env),
        client_secret=os.getenv(client_secret_env, ""),
        refresh_token=os.getenv(refresh_token_env),
    )
    if not oauth.access_token and not oauth.can_refresh:
        raise ValueError(
            f"source {config.name!r}: Reddit OAuth requires {access_token_env} or "
            f"{client_id_env} + {refresh_token_env}"
        )

    seen_window = _positive_int(options.get("seen_window", 1000), "seen_window", config)
    if seen_window < 100:
        raise ValueError(f"source {config.name!r}: seen_window must be >= 100")
    return RedditCollector(
        subreddit,
        listing=listing,  # type: ignore[arg-type]
        oauth=oauth,
        user_agent=user_agent,
        instance=_optional_string(options.get("instance")),
        seen_window=seen_window,
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
