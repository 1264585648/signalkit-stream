from __future__ import annotations

from collections.abc import Callable
import os
from typing import Any, Mapping

from signalkit_stream.config import SinkConfig
from signalkit_stream.delivery import Sink, StdoutSink, WebhookSink

SinkFactory = Callable[[SinkConfig], Sink]


class SinkRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, SinkFactory] = {}

    def register(self, sink_type: str, factory: SinkFactory) -> None:
        key = sink_type.strip().lower()
        if not key:
            raise ValueError("sink type must not be empty")
        if key in self._factories:
            raise ValueError(f"sink type already registered: {key}")
        self._factories[key] = factory

    def create(self, config: SinkConfig) -> Sink:
        sink_type = config.type.strip().lower()
        try:
            factory = self._factories[sink_type]
        except KeyError as exc:
            known = ", ".join(sorted(self._factories)) or "none"
            raise ValueError(f"unknown sink type {sink_type!r}; registered: {known}") from exc
        return factory(config)

    @property
    def types(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


def default_sink_registry() -> SinkRegistry:
    registry = SinkRegistry()
    registry.register("stdout", _stdout_factory)
    registry.register("webhook", _webhook_factory)
    return registry


def _stdout_factory(config: SinkConfig) -> Sink:
    options = dict(config.options)
    _reject_unknown(options, set(), config)
    return StdoutSink(name=config.name)


def _webhook_factory(config: SinkConfig) -> Sink:
    options = dict(config.options)
    _reject_unknown(options, {"url", "headers", "bearer_token_env", "timeout"}, config)
    url = _required_string(options, "url", config)
    headers_value = options.get("headers", {})
    if not isinstance(headers_value, Mapping):
        raise ValueError(f"sink {config.name!r}: headers must be a table")
    headers = {str(key): str(value) for key, value in headers_value.items()}
    token_env = _optional_string(options.get("bearer_token_env"))
    if token_env:
        token = os.getenv(token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"
    timeout = _positive_float(options.get("timeout", 20.0), "timeout", config)
    return WebhookSink(
        url,
        name=config.name,
        headers=headers,
        timeout=timeout,
    )


def _reject_unknown(options: Mapping[str, Any], allowed: set[str], config: SinkConfig) -> None:
    unknown = set(options) - allowed
    if unknown:
        raise ValueError(
            f"sink {config.name!r}: unknown {config.type} options: {', '.join(sorted(unknown))}"
        )


def _required_string(options: Mapping[str, Any], key: str, config: SinkConfig) -> str:
    value = str(options.get(key, "")).strip()
    if not value:
        raise ValueError(f"sink {config.name!r}: {key} is required")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive_float(value: Any, key: str, config: SinkConfig) -> float:
    if isinstance(value, bool):
        raise ValueError(f"sink {config.name!r}: {key} must be a number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"sink {config.name!r}: {key} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"sink {config.name!r}: {key} must be > 0")
    return parsed
