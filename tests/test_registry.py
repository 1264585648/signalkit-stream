import pytest

from signalkit_stream.collectors import (
    GitHubCollector,
    HackerNewsCollector,
    JSONFeedCollector,
    RSSCollector,
    RedditCollector,
)
from signalkit_stream.config import SourceConfig
from signalkit_stream.registry import CollectorRegistry, default_registry


def test_default_registry_builds_first_party_collectors(monkeypatch) -> None:
    monkeypatch.setenv("TEST_GITHUB_TOKEN", "secret")
    monkeypatch.setenv("TEST_REDDIT_CLIENT_ID", "reddit-id")
    monkeypatch.setenv("TEST_REDDIT_CLIENT_SECRET", "reddit-secret")
    monkeypatch.setenv("TEST_REDDIT_USER_AGENT", "signalkit-test/1.0")
    registry = default_registry()

    rss = registry.create(
        SourceConfig("rss-a", "rss", options={"url": "https://example.com/feed.xml"})
    )
    jsonfeed = registry.create(
        SourceConfig("json-a", "jsonfeed", options={"url": "https://example.com/feed.json"})
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
    reddit = registry.create(
        SourceConfig(
            "reddit-a",
            "reddit",
            options={
                "subreddit": "SaaS",
                "comments": 3,
                "client_id_env": "TEST_REDDIT_CLIENT_ID",
                "client_secret_env": "TEST_REDDIT_CLIENT_SECRET",
                "user_agent_env": "TEST_REDDIT_USER_AGENT",
            },
        )
    )

    assert isinstance(rss, RSSCollector)
    assert isinstance(jsonfeed, JSONFeedCollector)
    assert jsonfeed.identity.key == "jsonfeed:json-a"
    assert isinstance(hn, HackerNewsCollector)
    assert isinstance(github, GitHubCollector)
    assert github.token == "secret"
    assert isinstance(reddit, RedditCollector)
    assert reddit.identity.key == "reddit:reddit-a"
    assert reddit.comments_per_post == 3
    assert registry.types == ("github", "hackernews", "jsonfeed", "reddit", "rss")


def test_registry_requires_reddit_credentials(monkeypatch) -> None:
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USER_AGENT", raising=False)
    registry = default_registry()

    with pytest.raises(ValueError, match="REDDIT_CLIENT_ID"):
        registry.create(SourceConfig("reddit-a", "reddit", options={"subreddit": "SaaS"}))


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
