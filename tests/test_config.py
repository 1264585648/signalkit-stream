import pytest

from signalkit_stream.config import ConfigError, parse_config


def test_parse_config_applies_runtime_defaults_and_source_options() -> None:
    config = parse_config(
        {
            "runtime": {"database": "data/signals.db", "default_interval": 30},
            "sources": [
                {
                    "name": "hn-new",
                    "type": "hackernews",
                    "options": {"feed": "newstories", "comments": 2},
                }
            ],
        }
    )

    assert config.runtime.database == "data/signals.db"
    assert config.sources[0].interval == 30
    assert config.sources[0].options["comments"] == 2


def test_config_rejects_unknown_keys_and_duplicate_names() -> None:
    with pytest.raises(ConfigError, match="unknown runtime keys"):
        parse_config(
            {
                "runtime": {"threads": 9},
                "sources": [{"name": "one", "type": "rss", "options": {"url": "x"}}],
            }
        )

    with pytest.raises(ConfigError, match="duplicate source names"):
        parse_config(
            {
                "sources": [
                    {"name": "same", "type": "rss", "options": {"url": "https://a"}},
                    {"name": "same", "type": "rss", "options": {"url": "https://b"}},
                ]
            }
        )


def test_config_requires_at_least_one_source() -> None:
    with pytest.raises(ConfigError, match="at least one"):
        parse_config({"runtime": {}})
