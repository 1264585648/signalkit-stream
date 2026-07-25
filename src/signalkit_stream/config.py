from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
import tomllib


class ConfigError(ValueError):
    """Raised when a SignalKit Stream configuration is invalid."""


@dataclass(slots=True, frozen=True)
class RuntimeSettings:
    database: str = "signals.db"
    global_concurrency: int = 4
    provider_concurrency: int = 2
    shutdown_timeout: float = 15.0
    default_interval: float = 60.0
    failure_threshold: int = 3
    cooldown: float = 300.0

    def __post_init__(self) -> None:
        if not self.database.strip():
            raise ConfigError("runtime.database must not be empty")
        if self.global_concurrency < 1:
            raise ConfigError("runtime.global_concurrency must be >= 1")
        if self.provider_concurrency < 1:
            raise ConfigError("runtime.provider_concurrency must be >= 1")
        if self.shutdown_timeout < 0:
            raise ConfigError("runtime.shutdown_timeout must be >= 0")
        if self.default_interval <= 0:
            raise ConfigError("runtime.default_interval must be > 0")
        if self.failure_threshold < 1:
            raise ConfigError("runtime.failure_threshold must be >= 1")
        if self.cooldown <= 0:
            raise ConfigError("runtime.cooldown must be > 0")


@dataclass(slots=True, frozen=True)
class SourceConfig:
    name: str
    type: str
    interval: float
    limit: int = 100
    max_pages: int = 100
    enabled: bool = True
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ConfigError("source.name must not be empty")
        if not self.type.strip():
            raise ConfigError(f"source {self.name!r}: type must not be empty")
        if self.interval <= 0:
            raise ConfigError(f"source {self.name!r}: interval must be > 0")
        if self.limit < 1:
            raise ConfigError(f"source {self.name!r}: limit must be >= 1")
        if self.max_pages < 1:
            raise ConfigError(f"source {self.name!r}: max_pages must be >= 1")


@dataclass(slots=True, frozen=True)
class StreamConfig:
    runtime: RuntimeSettings
    sources: tuple[SourceConfig, ...]

    def __post_init__(self) -> None:
        names = [source.name for source in self.sources]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ConfigError(f"duplicate source names: {', '.join(duplicates)}")

    @property
    def enabled_sources(self) -> tuple[SourceConfig, ...]:
        return tuple(source for source in self.sources if source.enabled)


_RUNTIME_KEYS = {
    "database",
    "global_concurrency",
    "provider_concurrency",
    "shutdown_timeout",
    "default_interval",
    "failure_threshold",
    "cooldown",
}
_SOURCE_KEYS = {"name", "type", "interval", "limit", "max_pages", "enabled", "options"}
_TOP_LEVEL_KEYS = {"runtime", "sources"}


def load_config(path: str | Path) -> StreamConfig:
    config_path = Path(path)
    try:
        with config_path.open("rb") as handle:
            payload = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file not found: {config_path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {config_path}: {exc}") from exc
    return parse_config(payload)


def parse_config(payload: Mapping[str, Any]) -> StreamConfig:
    unknown_top = set(payload) - _TOP_LEVEL_KEYS
    if unknown_top:
        raise ConfigError(f"unknown top-level keys: {', '.join(sorted(unknown_top))}")

    raw_runtime = payload.get("runtime", {})
    if not isinstance(raw_runtime, Mapping):
        raise ConfigError("runtime must be a TOML table")
    unknown_runtime = set(raw_runtime) - _RUNTIME_KEYS
    if unknown_runtime:
        raise ConfigError(f"unknown runtime keys: {', '.join(sorted(unknown_runtime))}")

    runtime = RuntimeSettings(
        database=_string(raw_runtime.get("database", "signals.db"), "runtime.database"),
        global_concurrency=_integer(
            raw_runtime.get("global_concurrency", 4), "runtime.global_concurrency"
        ),
        provider_concurrency=_integer(
            raw_runtime.get("provider_concurrency", 2), "runtime.provider_concurrency"
        ),
        shutdown_timeout=_number(
            raw_runtime.get("shutdown_timeout", 15.0), "runtime.shutdown_timeout"
        ),
        default_interval=_number(
            raw_runtime.get("default_interval", 60.0), "runtime.default_interval"
        ),
        failure_threshold=_integer(
            raw_runtime.get("failure_threshold", 3), "runtime.failure_threshold"
        ),
        cooldown=_number(raw_runtime.get("cooldown", 300.0), "runtime.cooldown"),
    )

    raw_sources = payload.get("sources", [])
    if not isinstance(raw_sources, list):
        raise ConfigError("sources must be an array of tables")

    sources: list[SourceConfig] = []
    for index, raw_source in enumerate(raw_sources):
        label = f"sources[{index}]"
        if not isinstance(raw_source, Mapping):
            raise ConfigError(f"{label} must be a TOML table")
        unknown = set(raw_source) - _SOURCE_KEYS
        if unknown:
            raise ConfigError(f"{label} has unknown keys: {', '.join(sorted(unknown))}")
        if "name" not in raw_source:
            raise ConfigError(f"{label}.name is required")
        if "type" not in raw_source:
            raise ConfigError(f"{label}.type is required")
        options = raw_source.get("options", {})
        if not isinstance(options, Mapping):
            raise ConfigError(f"{label}.options must be a TOML table or inline table")
        sources.append(
            SourceConfig(
                name=_string(raw_source["name"], f"{label}.name"),
                type=_string(raw_source["type"], f"{label}.type").lower(),
                interval=_number(
                    raw_source.get("interval", runtime.default_interval), f"{label}.interval"
                ),
                limit=_integer(raw_source.get("limit", 100), f"{label}.limit"),
                max_pages=_integer(raw_source.get("max_pages", 100), f"{label}.max_pages"),
                enabled=_boolean(raw_source.get("enabled", True), f"{label}.enabled"),
                options=dict(options),
            )
        )

    if not sources:
        raise ConfigError("at least one [[sources]] entry is required")
    return StreamConfig(runtime=runtime, sources=tuple(sources))


def _string(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{path} must be a string")
    return value


def _integer(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{path} must be an integer")
    return value


def _number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{path} must be a number")
    return float(value)


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{path} must be a boolean")
    return value
