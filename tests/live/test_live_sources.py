from __future__ import annotations

import os

import pytest

from signalkit_stream.collectors import (
    GitHubCollector,
    HackerNewsCollector,
    JSONFeedCollector,
    RSSCollector,
    RedditCollector,
    RedditOAuth,
)
from signalkit_stream.protocol import CollectorContext

pytestmark = pytest.mark.skipif(
    os.getenv("SIGNALKIT_LIVE") != "1",
    reason="live compatibility tests require SIGNALKIT_LIVE=1",
)


@pytest.mark.asyncio
async def test_hackernews_live_compatibility() -> None:
    collector = HackerNewsCollector(feed="newstories")
    result = await collector.collect(context=CollectorContext(limit=1))

    assert result.cursor.source_key == collector.identity.key
    assert result.primary_count <= 1
    assert all(event.source_key == collector.identity.key for event in result.events)


@pytest.mark.asyncio
async def test_github_live_compatibility() -> None:
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        pytest.skip("GITHUB_TOKEN is not available")

    collector = GitHubCollector(
        "repo:python/cpython is:issue is:open",
        token=token,
        instance="live-cpython-open-issues",
    )
    result = await collector.collect(context=CollectorContext(limit=1))

    assert result.cursor.source_key == collector.identity.key
    assert result.primary_count <= 1
    assert all(event.source_key == collector.identity.key for event in result.events)


@pytest.mark.asyncio
async def test_rss_live_compatibility() -> None:
    url = os.getenv("SIGNALKIT_LIVE_RSS_URL")
    if not url:
        pytest.skip("SIGNALKIT_LIVE_RSS_URL is not configured")

    collector = RSSCollector(url, instance="live-rss")
    result = await collector.collect(context=CollectorContext(limit=1))

    assert result.cursor.source_key == collector.identity.key
    assert result.primary_count <= 1
    assert all(event.source_key == collector.identity.key for event in result.events)


@pytest.mark.asyncio
async def test_json_feed_live_compatibility() -> None:
    url = os.getenv("SIGNALKIT_LIVE_JSON_FEED_URL")
    if not url:
        pytest.skip("SIGNALKIT_LIVE_JSON_FEED_URL is not configured")

    collector = JSONFeedCollector(url, instance="live-json-feed")
    result = await collector.collect(context=CollectorContext(limit=1))

    assert result.cursor.source_key == collector.identity.key
    assert result.primary_count <= 1
    assert all(event.source_key == collector.identity.key for event in result.events)


@pytest.mark.asyncio
async def test_reddit_live_compatibility() -> None:
    subreddit = os.getenv("SIGNALKIT_LIVE_REDDIT_SUBREDDIT")
    user_agent = os.getenv("REDDIT_USER_AGENT")
    access_token = os.getenv("REDDIT_ACCESS_TOKEN")
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")
    refresh_token = os.getenv("REDDIT_REFRESH_TOKEN")
    if not subreddit or not user_agent:
        pytest.skip("Reddit live source/user-agent is not configured")
    if not access_token and not (client_id and refresh_token):
        pytest.skip("Reddit OAuth credentials are not configured")

    collector = RedditCollector(
        subreddit,
        oauth=RedditOAuth(
            access_token=access_token,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        ),
        user_agent=user_agent,
        instance="live-reddit-posts",
    )
    result = await collector.collect(context=CollectorContext(limit=1))

    assert result.cursor.source_key == collector.identity.key
    assert result.primary_count <= 1
    assert all(event.source_key == collector.identity.key for event in result.events)
