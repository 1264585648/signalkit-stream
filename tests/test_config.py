import pytest

from signalkit_stream.config import parse_config, sample_config


def test_parse_runtime_and_source_options() -> None:
    config = parse_config(
        {
            "runtime": {"database": "data/signals.db", "concurrency": 2},
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
        }
    )

    assert config.runtime.database == "data/signals.db"
    assert config.runtime.concurrency == 2
    assert config.sources[0].options == {"feed": "askstories", "comments": 2}


def test_config_rejects_unknown_runtime_and_duplicate_sources() -> None:
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


def test_config_requires_enabled_source_and_sample_is_toml(tmp_path) -> None:
    with pytest.raises(ValueError, match="at least one enabled"):
        parse_config({"sources": [{"name": "off", "type": "rss", "enabled": False}]})

    path = tmp_path / "signalkit.toml"
    path.write_text(sample_config(), encoding="utf-8")
    from signalkit_stream.config import load_config

    config = load_config(path)
    assert config.sources[0].name == "hackernews-new"
