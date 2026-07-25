from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
import tomllib


@dataclass(slots=True, frozen=True)
class RuntimeConfig:
    database: str = "signals.db"
    concurrency: int = 4
    failure_threshold: int = 5
    circuit_cooldown: float = 300.0
    failure_backoff_base: float = 5.0
    delivery_interval: float = 1.0
    delivery_batch: int = 100
    delivery_max_attempts: int = 8
    delivery_backoff_base: float = 5.0
    delivery_backoff_max: float = 3600.0

    def __post_init__(self) -> None:
        if not self.database.strip():
            raise ValueError("runtime.database must not be empty")
        if self.concurrency < 1:
            raise ValueError("runtime.concurrency must be >= 1")
        if self.failure_threshold < 1:
            raise ValueError("runtime.failure_threshold must be >= 1")
        if self.circuit_cooldown <= 0:
            raise ValueError("runtime.circuit_cooldown must be > 0")
        if self.failure_backoff_base <= 0:
            raise ValueError("runtime.failure_backoff_base must be > 0")
        if self.delivery_interval <= 0:
            raise ValueError("runtime.delivery_interval must be > 0")
        if self.delivery_batch < 1:
            raise ValueError("runtime.delivery_batch must be >= 1")
        if self.delivery_max_attempts < 1:
            raise ValueError("runtime.delivery_max_attempts must be >= 1")
        if self.delivery_backoff_base <= 0 or self.delivery_backoff_max <= 0:
            raise ValueError("runtime delivery backoff values must be > 0")
        if self.delivery_backoff_max < self.delivery_backoff_base:
            raise ValueError("runtime.delivery_backoff_max must be >= delivery_backoff_base")


@dataclass(slots=True, frozen=True)
class SourceConfig:
    name: str
    type: str
    interval: float = 60.0
    limit: int = 100
    enabled: bool = True
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("source.name must not be empty")
        if not self.type.strip():
            raise ValueError(f"source {self.name!r}: type must not be empty")
        if self.interval <= 0:
            raise ValueError(f"source {self.name!r}: interval must be > 0")
        if self.limit < 1:
            raise ValueError(f"source {self.name!r}: limit must be >= 1")


@dataclass(slots=True, frozen=True)
class SinkConfig:
    name: str
    type: str
    enabled: bool = True
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("sink.name must not be empty")
        if not self.type.strip():
            raise ValueError(f"sink {self.name!r}: type must not be empty")


@dataclass(slots=True, frozen=True)
class StreamConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    sources: tuple[SourceConfig, ...] = ()
    sinks: tuple[SinkConfig, ...] = ()

    def __post_init__(self) -> None:
        source_names = [source.name for source in self.sources]
        duplicate_sources = sorted({name for name in source_names if source_names.count(name) > 1})
        if duplicate_sources:
            raise ValueError(f"duplicate source names: {', '.join(duplicate_sources)}")
        if not any(source.enabled for source in self.sources):
            raise ValueError("configuration must contain at least one enabled source")

        sink_names = [sink.name for sink in self.sinks]
        duplicate_sinks = sorted({name for name in sink_names if sink_names.count(name) > 1})
        if duplicate_sinks:
            raise ValueError(f"duplicate sink names: {', '.join(duplicate_sinks)}")


_RUNTIME_KEYS = {
    "database",
    "concurrency",
    "failure_threshold",
    "circuit_cooldown",
    "failure_backoff_base",
    "delivery_interval",
    "delivery_batch",
    "delivery_max_attempts",
    "delivery_backoff_base",
    "delivery_backoff_max",
}
_SOURCE_KEYS = {"name", "type", "interval", "limit", "enabled"}
_SINK_KEYS = {"name", "type", "enabled"}


def load_config(path: str | Path) -> StreamConfig:
    config_path = Path(path)
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    return parse_config(payload)


def parse_config(payload: Mapping[str, Any]) -> StreamConfig:
    unknown_top = set(payload) - {"runtime", "sources", "sinks"}
    if unknown_top:
        raise ValueError(f"unknown top-level configuration keys: {', '.join(sorted(unknown_top))}")

    runtime_payload = payload.get("runtime", {})
    if not isinstance(runtime_payload, Mapping):
        raise ValueError("runtime must be a TOML table")
    unknown_runtime = set(runtime_payload) - _RUNTIME_KEYS
    if unknown_runtime:
        raise ValueError(f"unknown runtime keys: {', '.join(sorted(unknown_runtime))}")
    runtime = RuntimeConfig(
        database=str(runtime_payload.get("database", "signals.db")),
        concurrency=_int(runtime_payload.get("concurrency", 4), "runtime.concurrency"),
        failure_threshold=_int(
            runtime_payload.get("failure_threshold", 5),
            "runtime.failure_threshold",
        ),
        circuit_cooldown=_float(
            runtime_payload.get("circuit_cooldown", 300.0),
            "runtime.circuit_cooldown",
        ),
        failure_backoff_base=_float(
            runtime_payload.get("failure_backoff_base", 5.0),
            "runtime.failure_backoff_base",
        ),
        delivery_interval=_float(
            runtime_payload.get("delivery_interval", 1.0),
            "runtime.delivery_interval",
        ),
        delivery_batch=_int(
            runtime_payload.get("delivery_batch", 100),
            "runtime.delivery_batch",
        ),
        delivery_max_attempts=_int(
            runtime_payload.get("delivery_max_attempts", 8),
            "runtime.delivery_max_attempts",
        ),
        delivery_backoff_base=_float(
            runtime_payload.get("delivery_backoff_base", 5.0),
            "runtime.delivery_backoff_base",
        ),
        delivery_backoff_max=_float(
            runtime_payload.get("delivery_backoff_max", 3600.0),
            "runtime.delivery_backoff_max",
        ),
    )

    source_payloads = payload.get("sources", [])
    if not isinstance(source_payloads, list):
        raise ValueError("sources must be an array of TOML tables")
    sources: list[SourceConfig] = []
    for index, raw in enumerate(source_payloads):
        if not isinstance(raw, Mapping):
            raise ValueError(f"sources[{index}] must be a TOML table")
        name = str(raw.get("name", "")).strip()
        source_type = str(raw.get("type", "")).strip()
        options = {key: value for key, value in raw.items() if key not in _SOURCE_KEYS}
        sources.append(
            SourceConfig(
                name=name,
                type=source_type,
                interval=_float(raw.get("interval", 60.0), f"source {name!r}.interval"),
                limit=_int(raw.get("limit", 100), f"source {name!r}.limit"),
                enabled=_bool(raw.get("enabled", True), f"source {name!r}.enabled"),
                options=options,
            )
        )

    sink_payloads = payload.get("sinks", [])
    if not isinstance(sink_payloads, list):
        raise ValueError("sinks must be an array of TOML tables")
    sinks: list[SinkConfig] = []
    for index, raw in enumerate(sink_payloads):
        if not isinstance(raw, Mapping):
            raise ValueError(f"sinks[{index}] must be a TOML table")
        name = str(raw.get("name", "")).strip()
        sink_type = str(raw.get("type", "")).strip()
        options = {key: value for key, value in raw.items() if key not in _SINK_KEYS}
        sinks.append(
            SinkConfig(
                name=name,
                type=sink_type,
                enabled=_bool(raw.get("enabled", True), f"sink {name!r}.enabled"),
                options=options,
            )
        )

    return StreamConfig(runtime=runtime, sources=tuple(sources), sinks=tuple(sinks))


def sample_config() -> str:
    return '''[runtime]
database = "signals.db"
concurrency = 4
failure_threshold = 5
circuit_cooldown = 300
failure_backoff_base = 5
delivery_interval = 1
delivery_batch = 100
delivery_max_attempts = 8
delivery_backoff_base = 5
delivery_backoff_max = 3600

[[sources]]
name = "hackernews-new"
type = "hackernews"
interval = 60
limit = 50
feed = "newstories"
comments = 0

# [[sources]]
# name = "github-leads"
# type = "github"
# interval = 120
# limit = 50
# query = '"looking for" is:issue is:open'
# comments = 3
# token_env = "GITHUB_TOKEN"

# [[sources]]
# name = "company-blog"
# type = "rss"
# interval = 300
# limit = 100
# url = "https://example.com/feed.xml"

# [[sinks]]
# name = "archive"
# type = "jsonl"
# path = "signals.jsonl"

# [[sinks]]
# name = "brain-webhook"
# type = "webhook"
# url = "https://example.com/signals"
# token_env = "SIGNALKIT_WEBHOOK_TOKEN"
'''


def _int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc


def _float(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a number")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc


def _bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean")
    return value
