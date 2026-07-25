import pytest

from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.config import SourceConfig
from signalkit_stream.registry import default_registry
from signalkit_stream.rest_config import generic_rest_factory, register_generic_rest


def base_options() -> dict[str, object]:
    return {
        "url": "https://api.example.com/items",
        "item_path": "data.items",
        "id_field": "id",
        "content_field": "body",
        "url_field": "url",
        "created_at_field": "created_at",
    }


def test_generic_rest_is_explicit_opt_in_and_builds_configured_collector(monkeypatch) -> None:
    registry = default_registry()
    assert "rest" not in registry.types
    register_generic_rest(registry)
    monkeypatch.setenv("EXAMPLE_API_TOKEN", "secret")

    options = base_options()
    options.update(
        {
            "title_field": "title",
            "author_field": "author.name",
            "updated_at_field": "updated_at",
            "kind": "article",
            "headers": {"X-Client": "signalkit"},
            "query": {"state": "open"},
            "token_env": "EXAMPLE_API_TOKEN",
            "token_header": "X-API-Key",
            "token_prefix": "Token ",
            "pagination": "offset",
            "page_size": 25,
            "seen_window": 1000,
        }
    )
    adapter = registry.create(SourceConfig("partner-api", "rest", options=options))

    assert isinstance(adapter, GenericRESTCollector)
    assert adapter.identity.key == "rest:partner-api"
    assert adapter.headers["X-Client"] == "signalkit"
    assert adapter.headers["X-API-Key"] == "Token secret"
    assert adapter.query == {"state": "open"}
    assert adapter.pagination == "offset"
    assert adapter.page_size == 25
    assert adapter.seen_window == 1000


def test_generic_rest_factory_validates_required_mapping_and_credentials(monkeypatch) -> None:
    with pytest.raises(ValueError, match="item_path is required"):
        generic_rest_factory(
            SourceConfig(
                "bad",
                "rest",
                options={"url": "https://api.example.com/items"},
            )
        )

    options = base_options()
    options["unknown"] = True
    with pytest.raises(ValueError, match="unknown rest options"):
        generic_rest_factory(SourceConfig("bad", "rest", options=options))

    options = base_options()
    options["headers"] = ["bad"]
    with pytest.raises(ValueError, match="headers must be a table"):
        generic_rest_factory(SourceConfig("bad", "rest", options=options))

    monkeypatch.delenv("MISSING_REST_TOKEN", raising=False)
    options = base_options()
    options["token_env"] = "MISSING_REST_TOKEN"
    with pytest.raises(ValueError, match="MISSING_REST_TOKEN"):
        generic_rest_factory(SourceConfig("bad", "rest", options=options))


def test_register_generic_rest_respects_registry_duplicate_guard() -> None:
    registry = default_registry()
    register_generic_rest(registry)
    with pytest.raises(ValueError, match="already registered"):
        register_generic_rest(registry)
