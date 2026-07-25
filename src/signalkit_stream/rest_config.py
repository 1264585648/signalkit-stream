from __future__ import annotations

import os
from typing import Any, Mapping

from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.config import SourceConfig
from signalkit_stream.models import SignalKind
from signalkit_stream.registry import CollectorRegistry


def register_generic_rest(registry: CollectorRegistry) -> None:
    """Explicitly opt a collector registry into the generic REST adapter."""

    registry.register("rest", generic_rest_factory)


def generic_rest_factory(config: SourceConfig) -> GenericRESTCollector:
    options = dict(config.options)
    allowed = {
        "url",
        "item_path",
        "id_field",
        "content_field",
        "url_field",
        "created_at_field",
        "title_field",
        "author_field",
        "updated_at_field",
        "source",
        "instance",
        "kind",
        "headers",
        "query",
        "token_env",
        "token_header",
        "token_prefix",
        "pagination",
        "page_param",
        "offset_param",
        "limit_param",
        "page_size",
        "seen_window",
        "initial_backfill",
    }
    unknown = set(options) - allowed
    if unknown:
        raise ValueError(
            f"source {config.name!r}: unknown rest options: {', '.join(sorted(unknown))}"
        )

    headers = _string_mapping(options.get("headers", {}), "headers", config)
    token_env = _optional_string(options.get("token_env"))
    if token_env:
        token = os.getenv(token_env)
        if not token:
            raise ValueError(
                f"source {config.name!r}: environment variable {token_env!r} is not set"
            )
        token_header = _optional_string(options.get("token_header")) or "Authorization"
        token_prefix = str(options.get("token_prefix", "Bearer "))
        headers[token_header] = token_prefix + token

    kind_value = str(options.get("kind", SignalKind.POST.value))
    try:
        kind = SignalKind(kind_value)
    except ValueError as exc:
        raise ValueError(f"source {config.name!r}: unsupported SignalKind {kind_value!r}") from exc

    pagination = str(options.get("pagination", "page")).strip().lower()
    if pagination not in {"page", "offset"}:
        raise ValueError(f"source {config.name!r}: pagination must be page or offset")

    return GenericRESTCollector(
        _required_string(options, "url", config),
        item_path=_required_string(options, "item_path", config),
        id_field=_required_string(options, "id_field", config),
        content_field=_required_string(options, "content_field", config),
        url_field=_required_string(options, "url_field", config),
        created_at_field=_required_string(options, "created_at_field", config),
        title_field=_optional_string(options.get("title_field")),
        author_field=_optional_string(options.get("author_field")),
        updated_at_field=_optional_string(options.get("updated_at_field")),
        source=str(options.get("source", "rest")),
        instance=_optional_string(options.get("instance")) or config.name,
        kind=kind,
        headers=headers,
        query=_string_mapping(options.get("query", {}), "query", config),
        pagination=pagination,  # type: ignore[arg-type]
        page_param=str(options.get("page_param", "page")),
        offset_param=str(options.get("offset_param", "offset")),
        limit_param=str(options.get("limit_param", "limit")),
        page_size=_positive_int(options.get("page_size", 100), "page_size", config),
        seen_window=_positive_int(options.get("seen_window", 500), "seen_window", config),
        initial_backfill=_boolean(
            options.get("initial_backfill", True),
            "initial_backfill",
            config,
        ),
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


def _positive_int(value: Any, key: str, config: SourceConfig) -> int:
    if isinstance(value, bool):
        raise ValueError(f"source {config.name!r}: {key} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"source {config.name!r}: {key} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"source {config.name!r}: {key} must be >= 1")
    return parsed


def _boolean(value: Any, key: str, config: SourceConfig) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"source {config.name!r}: {key} must be a boolean")
    return value


def _string_mapping(value: Any, key: str, config: SourceConfig) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"source {config.name!r}: {key} must be a table")
    return {str(name): str(item) for name, item in value.items()}
