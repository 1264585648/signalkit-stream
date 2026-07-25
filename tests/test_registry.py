import pytest

from signalkit_stream.collectors import GitHubCollector, HackerNewsCollector, RSSCollector
from signalkit_stream.config import ConfigError, SourceConfig
from signalkit_stream.registry import default_registry


def test_default_registry_builds_named_source_instances() -> None:
    registry = default_registry()

    rss = registry.create(
        SourceConfig(
            name="blog",
            type="rss",
            interval=60,
            options={"url": "https://example.com/feed.xml"},
        )
    )
    hn = registry.create(
        SourceConfig(
            name="hn-ask",
            type="hackernews",
            interval=60,
            options={"feed": "askstories", "comments": 2},
        )
    )
    github = registry.create(
        SourceConfig(
            name="github-leads",
            type="github",
            interval=60,
            options={"query": '"looking for" is:issue'},
        )
    )

    assert isinstance(rss, RSSCollector)
    assert rss.identity.key == "rss:blog"
    assert isinstance(hn, HackerNewsCollector)
    assert hn.identity.key == "hackernews:hn-ask"
    assert isinstance(github, GitHubCollector)
    assert github.identity.key == "github:github-leads"


def test_registry_rejects_unknown_source_type_and_options() -> None:
    registry = default_registry()

    with pytest.raises(ConfigError, match="unknown type"):
        registry.create(SourceConfig(name="x", type="missing", interval=60))

    with pytest.raises(ConfigError, match="unknown options"):
        registry.create(
            SourceConfig(
                name="blog",
                type="rss",
                interval=60,
                options={"url": "https://example.com/feed.xml", "mystery": True},
            )
        )
