import pytest

from signalkit_stream.collectors import GitHubCollector, HackerNewsCollector, RSSCollector
from signalkit_stream.config import SourceConfig
from signalkit_stream.registry import CollectorRegistry, default_registry


def test_default_registry_builds_first_party_collectors(monkeypatch) -> None:
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "secret")
    registry = default_registry()

    rss = registry.create(
        SourceConfig("rss-a", "rss", options={"url": "https://example.com/feed.xml"})
    )
    hn = registry.create(
        SourceConfig("hn-a", "hackernews", options={"feed": "askstories", "comments": 2})
    )
    github = registry.create(
        SourceConfig(
            "gh-a",
            "github",
            options={"query": "is:issue", "token_env": "TEST_GITHUB_TOKEN"},
        )
    )

    assert isinstance(rss, RSSCollector)
    assert isinstance(hn, HackerNewsCollector)
    assert isinstance(github, GitHubCollector)
    assert github.token == "secret"
    assert registry.types == ("github", "hackernews", "rss")


def test_registry_rejects_unknown_type_option_and_duplicate_registration() -> None:
    registry = CollectorRegistry()
    with pytest.raises(ValueError, match="unknown collector type"):
        registry.create(SourceConfig("x", "missing"))

    registry = default_registry()
    with pytest.raises(ValueError, match="unknown rss options"):
        registry.create(
            SourceConfig(
                "bad-rss",
                "rss",
                options={"url": "https://example.com/feed", "wat": True},
            )
        )

    with pytest.raises(ValueError, match="already registered"):
        registry.register("rss", lambda config: RSSCollector("https://example.com"))
