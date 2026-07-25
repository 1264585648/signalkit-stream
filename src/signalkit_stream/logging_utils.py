from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import sys
from typing import Literal, TextIO

LogFormat = Literal["text", "json"]

_STANDARD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
}


class JsonLogFormatter(logging.Formatter):
    """Small dependency-free JSON formatter for runtime/service logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_FIELDS or key.startswith("_"):
                continue
            if key in {
                "args",
                "exc_info",
                "exc_text",
                "stack_info",
            }:
                continue
            payload[key] = _json_safe(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


class TextLogFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )


def configure_logging(
    *,
    level: int | str = logging.INFO,
    output_format: LogFormat = "text",
    stream: TextIO | None = None,
    force: bool = True,
) -> None:
    """Configure root logging for a standalone SignalKit Stream process."""

    handler = logging.StreamHandler(stream or sys.stderr)
    if output_format == "json":
        handler.setFormatter(JsonLogFormatter())
    elif output_format == "text":
        handler.setFormatter(TextLogFormatter())
    else:
        raise ValueError("output_format must be 'text' or 'json'")

    root = logging.getLogger()
    if force:
        for existing in list(root.handlers):
            root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)
