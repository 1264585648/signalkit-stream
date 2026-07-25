import pytest

from signalkit_stream.config import parse_config, sample_config


def test_parse_runtime_source_delivery_and_sink_options() -> None:
    config = parse_config(
        {
            "runtime": {"database": "data/signals.db", "concurrency": 2},
            "delivery": {"batch_size": 25, "max_attempts": 7},
            "sources": [
                {
                    "name": "hn",
                    "type": "hackernews",
                    "interval": 30,
                    "limit": 25,
                    "feed": "askstories",
                    "comments": 2,
                }
            ],
            "sinks": [
                {
                    "name": "crm",
                    "type": "webhook",
                    "url": "https://example.com/hook",
                    "bearer_token_env": "HOOK_TOKEN",
                }
            ],
        }
    )

    assert config.runtime.database == "data/signals.db"
    assert config.runtime.concurrency == 2
    assert config.delivery.batch_size == 25
    assert config.delivery.max_attempts == 7
    assert config.sources[0].options == {"feed": "askstories", "comments": 2}
    assert config.sinks[0].options == {
        "url": "https://example.com/hook",
        "bearer_token_env": "HOOK_TOKEN",
    }


def test_config_rejects_unknown_runtime_delivery_and_duplicates() -> None:
    with pytest.raises(ValueError, match="unknown runtime keys"):
        parse_config({"runtime": {"mystery": 1}, "sources": []})

    with pytest.raises(ValueError, match="unknown delivery keys"):
        parse_config(
            {
                "delivery": {"mystery": 1},
                "sources": [{"name": "hn", "type": "hackernews"}],
            }
        )

    with pytest.raises(ValueError, match="duplicate source names"):
        parse_config(
            {
                "sources": [
                    {"name": "same", "type": "rss", "url": "https://a.example/feed"},
                    {"name": "same", "type": "rss", "url": "https://b.example/feed"},
                ]
            }
        )

    with pytest.raises(ValueError, match="duplicate sink names"):
        parse_config(
            {
                "sources": [{"name": "hn", "type": "hackernews"}],
                "sinks": [
                    {"name": "same", "type": "stdout"},
                    {"name": "same", "type": "stdout"},
                ],
            }
        )


def test_config_requires_enabled_source_and_sample_is_toml(tmp_path) -> None:
    with pytest.raises(ValueError, match="at least one enabled"):
        parse_config({"sources": [{"name": "off", "type": "rss", "enabled": False}]})

    path = tmp_path / "signalkit.toml"
    path.write_text(sample_config(), encoding="utf-8")
    from signalkit_stream.config import load_config

    config = load_config(path)
    assert config.sources[0].name == "hackernews-new"
    assert config.delivery.max_attempts == 5
