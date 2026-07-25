from __future__ import annotations

import os
from typing import Any, Mapping

from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.config import SourceConfig
from signalkit_stream.models import SignalKind

_ALLOWED = {
    "url",
    "items_path",
    "id_path",
    "kind",
    "source",
    "instance",
    "title_path",
    "content_path",
    "author_path",
    "url_path",
    "created_at_path",
    "updated_at_path",
    "metadata_paths",
    "params",
    "headers",
    "token_env",
    "token_header",
    "token_prefix",
    "pagination",
    "page_param",
    "page_start",
    "cursor_param",
    "next_cursor_path",
    "limit_param",
    "initial_backfill",
    "seen_window",
}


def build_generic_rest_collector(config: SourceConfig) -> GenericRESTCollector:
    """Build the reference REST adapter from a normal ``SourceConfig``.

    Register this factory explicitly with ``CollectorRegistry``. It is intentionally
    not part of the default registry because generic mapping cannot express every API
    correctly; source-specific adapters remain the preferred path when semantics are
    richer than a JSON list endpoint.
    """

    options = dict(config.options)
    unknown = set(options) - _ALLOWED
    if unknown:
        raise ValueError(
            f"source {config.name!r}: unknown rest options: {', '.join(sorted(unknown))}"
        )

    url = _required_string(options, "url", config)
    items_path = _required_string(options, "items_path", config)
    id_path = _required_string(options, "id_path", config)
    kind_raw = str(options.get("kind", SignalKind.POST.value)).strip().lower()
    try:
        kind = SignalKind(kind_raw)
    except ValueError as exc:
        allowed = ", ".join(kind.value for kind in SignalKind)
        raise ValueError(f"source {config.name!r}: kind must be one of: {allowed}") from exc

    params = _string_key_mapping(options.get("params", {}), "params", config)
    headers_raw = _string_key_mapping(options.get("headers", {}), "headers", config)
    headers = {str(key): str(value) for key, value in headers_raw.items()}

    token_env = _optional_string(options.get("token_env"))
    if token_env:
        token = os.getenv(token_env)
        if not token:
            raise ValueError(f"source {config.name!r}: environment variable {token_env} is not set")
        token_header = str(options.get("token_header", "Authorization")).strip()
        if not token_header:
            raise ValueError(f"source {config.name!r}: token_header must not be empty")
        token_prefix = str(options.get("token_prefix", "Bearer "))
        headers[token_header] = f"{token_prefix}{token}"

    metadata_paths_raw = _string_key_mapping(
        options.get("metadata_paths", {}),
        "metadata_paths",
        config,
    )
    metadata_paths = {str(key): str(value) for key, value in metadata_paths_raw.items()}

    pagination = str(options.get("pagination", "none")).strip().lower()
    if pagination not in {"none", "page", "cursor"}:
        raise ValueError(f"source {config.name!r}: pagination must be none, page, or cursor")

    return GenericRESTCollector(
        url,
        items_path=items_path,
        id_path=id_path,
        kind=kind,
        source=str(options.get("source", "rest")),
        instance=_optional_string(options.get("instance")),
        title_path=_optional_string(options.get("title_path")),
        content_path=_optional_string(options.get("content_path")),
        author_path=_optional_string(options.get("author_path")),
        url_path=_optional_string(options.get("url_path")),
        created_at_path=_optional_string(options.get("created_at_path")),
        updated_at_path=_optional_string(options.get("updated_at_path")),
        metadata_paths=metadata_paths,
        params=params,
        headers=headers,
        pagination=pagination,  # type: ignore[arg-type]
        page_param=str(options.get("page_param", "page")),
        page_start=_integer(options.get("page_start", 1), "page_start", config),
        cursor_param=str(options.get("cursor_param", "cursor")),
        next_cursor_path=_optional_string(options.get("next_cursor_path")),
        limit_param=_optional_string(options.get("limit_param")),
        initial_backfill=_boolean(
            options.get("initial_backfill", False),
            "initial_backfill",
            config,
        ),
        seen_window=_integer(options.get("seen_window", 2000), "seen_window", config),
    )


def _required_string(options: Mapping[str, Any], key: str, config: SourceConfig) -> str:
    value = _optional_string(options.get(key))
    if not value:
        raise ValueError(f"source {config.name!r}: {key} is required")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_key_mapping(value: Any, key: str, config: SourceConfig) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"source {config.name!r}: {key} must be a TOML table")
    result: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        text_key = str(raw_key).strip()
        if not text_key:
            raise ValueError(f"source {config.name!r}: {key} contains an empty key")
        result[text_key] = raw_value
    return result


def _integer(value: Any, key: str, config: SourceConfig) -> int:
    if isinstance(value, bool):
        raise ValueError(f"source {config.name!r}: {key} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"source {config.name!r}: {key} must be an integer") from exc


def _boolean(value: Any, key: str, config: SourceConfig) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"source {config.name!r}: {key} must be a boolean")
    return value
