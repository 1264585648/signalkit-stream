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
class StreamConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    sources: tuple[SourceConfig, ...] = ()

    def __post_init__(self) -> None:
        names = [source.name for source in self.sources]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate source names: {', '.join(duplicates)}")
        if not any(source.enabled for source in self.sources):
            raise ValueError("configuration must contain at least one enabled source")


_RUNTIME_KEYS = {
    "database",
    "concurrency",
    "failure_threshold",
    "circuit_cooldown",
    "failure_backoff_base",
}
_SOURCE_KEYS = {"name", "type", "interval", "limit", "enabled"}


def load_config(path: str | Path) -> StreamConfig:
    config_path = Path(path)
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    return parse_config(payload)


def parse_config(payload: Mapping[str, Any]) -> StreamConfig:
    unknown_top = set(payload) - {"runtime", "sources"}
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

    return StreamConfig(runtime=runtime, sources=tuple(sources))


def sample_config() -> str:
    return '''[runtime]
database = "signals.db"
concurrency = 4
failure_threshold = 5
circuit_cooldown = 300
failure_backoff_base = 5

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
