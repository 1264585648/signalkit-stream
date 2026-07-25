import pytest

from signalkit_stream.config import load_config, parse_config, sample_config


def test_parse_runtime_source_and_sink_options() -> None:
    config = parse_config(
        {
            "runtime": {
                "database": "data/signals.db",
                "concurrency": 2,
                "delivery_batch": 25,
            },
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
                    "name": "archive",
                    "type": "jsonl",
                    "path": "signals.jsonl",
                    "backfill": True,
                }
            ],
        }
    )

    assert config.runtime.database == "data/signals.db"
    assert config.runtime.concurrency == 2
    assert config.runtime.delivery_batch == 25
    assert config.sources[0].options == {"feed": "askstories", "comments": 2}
    assert config.sinks[0].options == {"path": "signals.jsonl"}
    assert config.sinks[0].backfill is True


def test_config_rejects_unknown_runtime_duplicate_sources_and_sinks() -> None:
    with pytest.raises(ValueError, match="unknown runtime keys"):
        parse_config({"runtime": {"mystery": 1}, "sources": []})

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
    config = load_config(path)
    assert config.sources[0].name == "hackernews-new"
