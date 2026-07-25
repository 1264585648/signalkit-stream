from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
import hashlib
import json
import os
import sys
from typing import Any, Mapping, TextIO

import httpx

from signalkit_stream.config import SinkConfig
from signalkit_stream.models import SignalEvent


class SinkError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code
        self.retry_after = retry_after


class Sink(ABC):
    key: str

    @abstractmethod
    async def send(self, event: SignalEvent) -> None:
        """Deliver one event or raise ``SinkError``."""

    async def close(self) -> None:
        return None


class StdoutSink(Sink):
    def __init__(self, key: str = "stdout", *, stream: TextIO | None = None) -> None:
        self.key = key
        self.stream = stream or sys.stdout

    async def send(self, event: SignalEvent) -> None:
        self.stream.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
        self.stream.flush()


class JsonlSink(Sink):
    def __init__(self, key: str, path: str | Path) -> None:
        self.key = key
        self.path = Path(path)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)

    async def send(self, event: SignalEvent) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")


class WebhookSink(Sink):
    def __init__(
        self,
        key: str,
        url: str,
        *,
        token: str | None = None,
        timeout: float = 20.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not url.strip():
            raise ValueError("webhook URL must not be empty")
        self.key = key
        self.url = url
        self.token = token
        self.timeout = timeout
        self._client = client

    async def send(self, event: SignalEvent) -> None:
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": delivery_idempotency_key(self.key, event),
            "X-SignalKit-Event-ID": event.id,
            "X-SignalKit-Event-Hash": event.fingerprint(),
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            if self._client is not None:
                response = await self._client.post(self.url, json=event.to_dict(), headers=headers)
            else:
                async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                    response = await client.post(self.url, json=event.to_dict(), headers=headers)
        except httpx.RequestError as exc:
            raise SinkError(f"webhook network error: {exc}", retryable=True) from exc

        if 200 <= response.status_code < 300:
            return
        retryable = response.status_code in {408, 425, 429, 500, 502, 503, 504}
        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        raise SinkError(
            f"webhook HTTP {response.status_code}",
            retryable=retryable,
            status_code=response.status_code,
            retry_after=retry_after,
        )


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
        sink = factory(config)
        if sink.key != config.name:
            raise ValueError(f"sink factory returned key {sink.key!r}, expected {config.name!r}")
        return sink

    @property
    def types(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


def default_sink_registry() -> SinkRegistry:
    registry = SinkRegistry()
    registry.register("stdout", _stdout_factory)
    registry.register("jsonl", _jsonl_factory)
    registry.register("webhook", _webhook_factory)
    return registry


def _stdout_factory(config: SinkConfig) -> Sink:
    _reject_unknown(config.options, set(), config)
    return StdoutSink(config.name)


def _jsonl_factory(config: SinkConfig) -> Sink:
    _reject_unknown(config.options, {"path"}, config)
    return JsonlSink(config.name, _required_string(config.options, "path", config))


def _webhook_factory(config: SinkConfig) -> Sink:
    _reject_unknown(config.options, {"url", "token_env", "timeout"}, config)
    url = _required_string(config.options, "url", config)
    token_env = str(config.options.get("token_env", "")).strip()
    token = os.getenv(token_env) if token_env else None
    timeout = _positive_float(config.options.get("timeout", 20.0), "timeout", config)
    return WebhookSink(config.name, url, token=token, timeout=timeout)


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


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def delivery_idempotency_key(sink_key: str, event: SignalEvent) -> str:
    """Return a stable key for one sink + source-object version.

    The normalized event ID is stable across source mutations, so using only the ID
    would cause a downstream idempotency cache to suppress legitimate updates. The
    content fingerprint makes retries of the same version stable while giving a new
    version a distinct key.
    """

    raw = f"{sink_key}\x1f{event.id}\x1f{event.fingerprint()}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"signalkit:{digest}"
