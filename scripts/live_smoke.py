from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import json
import os
import sys
from typing import Awaitable, Callable

from signalkit_stream.collectors import (
    GitHubCollector,
    HackerNewsCollector,
    JSONFeedCollector,
    RSSCollector,
    RedditCollector,
)
from signalkit_stream.pipeline import run_collector


@dataclass(slots=True, frozen=True)
class SmokeResult:
    source: str
    status: str
    events: int = 0
    detail: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "status": self.status,
            "events": self.events,
            "detail": self.detail,
        }


async def _run_source(
    source: str,
    operation: Callable[[], Awaitable[object]],
) -> SmokeResult:
    try:
        result = await operation()
    except Exception as exc:
        return SmokeResult(source, "failed", detail=f"{type(exc).__name__}: {exc}")
    events = len(getattr(result, "events", []))
    return SmokeResult(source, "passed", events=events)


async def _hackernews() -> object:
    return await run_collector(HackerNewsCollector(feed="newstories"), limit=1)


async def _github() -> object:
    return await run_collector(
        GitHubCollector(
            "repo:python/cpython is:issue sort:updated-desc",
            token=os.getenv("GITHUB_TOKEN"),
            instance="live-smoke",
        ),
        limit=1,
    )


async def _rss() -> SmokeResult:
    url = os.getenv("SIGNALKIT_LIVE_RSS_URL")
    if not url:
        return SmokeResult(
            "rss",
            "skipped",
            detail="SIGNALKIT_LIVE_RSS_URL is not configured",
        )
    return await _run_source(
        "rss",
        lambda: run_collector(RSSCollector(url, instance="live-smoke"), limit=1),
    )


async def _json_feed() -> SmokeResult:
    url = os.getenv("SIGNALKIT_LIVE_JSON_FEED_URL")
    if not url:
        return SmokeResult(
            "jsonfeed",
            "skipped",
            detail="SIGNALKIT_LIVE_JSON_FEED_URL is not configured",
        )
    return await _run_source(
        "jsonfeed",
        lambda: run_collector(JSONFeedCollector(url, instance="live-smoke"), limit=1),
    )


async def _reddit() -> SmokeResult:
    user_agent = os.getenv("REDDIT_USER_AGENT")
    access_token = os.getenv("REDDIT_ACCESS_TOKEN")
    refresh_token = os.getenv("REDDIT_REFRESH_TOKEN")
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    subreddit = os.getenv("SIGNALKIT_LIVE_REDDIT_SUBREDDIT", "python")

    if not user_agent:
        return SmokeResult(
            "reddit",
            "skipped",
            detail="REDDIT_USER_AGENT is not configured",
        )

    has_static = bool(access_token)
    has_refresh = bool(refresh_token and client_id)
    has_app_only = bool(client_id and client_secret)
    if not (has_static or has_refresh or has_app_only):
        return SmokeResult(
            "reddit",
            "skipped",
            detail=(
                "OAuth credentials not configured; provide REDDIT_ACCESS_TOKEN, "
                "REDDIT_REFRESH_TOKEN + REDDIT_CLIENT_ID, or "
                "REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET"
            ),
        )

    async def collect() -> object:
        collector = RedditCollector(
            subreddit,
            access_token=access_token,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            listing="new",
            instance="live-smoke",
        )
        return await run_collector(collector, limit=1)

    return await _run_source("reddit", collect)


async def run_smoke() -> list[SmokeResult]:
    hackernews, github, rss, json_feed = await asyncio.gather(
        _run_source("hackernews", _hackernews),
        _run_source("github", _github),
        _rss(),
        _json_feed(),
    )
    reddit = await _reddit()
    return [hackernews, github, rss, json_feed, reddit]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run opt-in live compatibility checks.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    results = asyncio.run(run_smoke())
    if args.json:
        print(json.dumps([result.to_dict() for result in results], indent=2))
    else:
        for result in results:
            suffix = f" ({result.detail})" if result.detail else ""
            print(f"{result.source}: {result.status}; events={result.events}{suffix}")

    # Missing optional configuration produces "skipped". Once a source is configured,
    # a compatibility failure must fail the smoke workflow so it becomes visible.
    return 1 if any(result.status == "failed" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
