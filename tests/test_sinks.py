import pytest

from signalkit_stream.config import SinkConfig
from signalkit_stream.delivery import StdoutSink, WebhookSink
from signalkit_stream.sinks import default_sink_registry


def test_sink_registry_builds_first_party_sinks(monkeypatch) -> None:
    registry = default_sink_registry()
    monkeypatch.setenv("HOOK_TOKEN", "secret")

    stdout = registry.create(SinkConfig(name="console", type="stdout"))
    webhook = registry.create(
        SinkConfig(
            name="crm",
            type="webhook",
            options={
                "url": "https://example.com/hook",
                "bearer_token_env": "HOOK_TOKEN",
                "headers": {"X-Tenant": "demo"},
            },
        )
    )

    assert isinstance(stdout, StdoutSink)
    assert stdout.name == "console"
    assert isinstance(webhook, WebhookSink)
    assert webhook.name == "crm"
    assert webhook.headers["Authorization"] == "Bearer secret"
    assert webhook.headers["X-Tenant"] == "demo"


def test_sink_registry_rejects_unknown_types_and_options() -> None:
    registry = default_sink_registry()

    with pytest.raises(ValueError, match="unknown sink type"):
        registry.create(SinkConfig(name="x", type="missing"))

    with pytest.raises(ValueError, match="unknown stdout options"):
        registry.create(SinkConfig(name="console", type="stdout", options={"bad": True}))

    with pytest.raises(ValueError, match="url is required"):
        registry.create(SinkConfig(name="crm", type="webhook"))
