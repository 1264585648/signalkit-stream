from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import json
import os
import sys
from typing import Awaitable, Callable

from signalkit_stream.collectors import GitHubCollector, HackerNewsCollector, RedditCollector
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


async def _reddit() -> SmokeResult:
    required = {
        "REDDIT_CLIENT_ID": os.getenv("REDDIT_CLIENT_ID"),
        "REDDIT_CLIENT_SECRET": os.getenv("REDDIT_CLIENT_SECRET"),
        "REDDIT_USER_AGENT": os.getenv("REDDIT_USER_AGENT"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        return SmokeResult(
            "reddit",
            "skipped",
            detail="credentials not configured: " + ", ".join(missing),
        )

    collector = RedditCollector(
        "python",
        client_id=required["REDDIT_CLIENT_ID"] or "",
        client_secret=required["REDDIT_CLIENT_SECRET"] or "",
        user_agent=required["REDDIT_USER_AGENT"] or "",
        listing="new",
        instance="live-smoke",
    )
    return await _run_source("reddit", lambda: run_collector(collector, limit=1))


async def run_smoke() -> list[SmokeResult]:
    hackernews, github = await asyncio.gather(
        _run_source("hackernews", _hackernews),
        _run_source("github", _github),
    )
    reddit = await _reddit()
    return [hackernews, github, reddit]


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

    required_failures = [
        result for result in results if result.source in {"hackernews", "github"} and result.status != "passed"
    ]
    configured_reddit_failure = any(
        result.source == "reddit" and result.status == "failed" for result in results
    )
    return 1 if required_failures or configured_reddit_failure else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
