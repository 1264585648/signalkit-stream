from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts import live_smoke


@pytest.mark.asyncio
async def test_unconfigured_optional_sources_are_skipped(monkeypatch) -> None:
    for name in (
        "SIGNALKIT_LIVE_RSS_URL",
        "SIGNALKIT_LIVE_JSON_FEED_URL",
        "REDDIT_USER_AGENT",
        "REDDIT_ACCESS_TOKEN",
        "REDDIT_REFRESH_TOKEN",
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)

    async def fake_required() -> object:
        return SimpleNamespace(events=[object()])

    monkeypatch.setattr(live_smoke, "_hackernews", fake_required)
    monkeypatch.setattr(live_smoke, "_github", fake_required)

    results = await live_smoke.run_smoke()
    by_source = {result.source: result for result in results}

    assert by_source["hackernews"].status == "passed"
    assert by_source["github"].status == "passed"
    assert by_source["rss"].status == "skipped"
    assert by_source["jsonfeed"].status == "skipped"
    assert by_source["reddit"].status == "skipped"


@pytest.mark.asyncio
async def test_reddit_static_token_is_a_valid_live_configuration(monkeypatch) -> None:
    monkeypatch.setenv("REDDIT_USER_AGENT", "signalkit-live-test")
    monkeypatch.setenv("REDDIT_ACCESS_TOKEN", "static-token")
    monkeypatch.delenv("REDDIT_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)

    captured = []

    async def fake_run_collector(collector, *, limit):
        captured.append((collector, limit))
        return SimpleNamespace(events=[])

    monkeypatch.setattr(live_smoke, "run_collector", fake_run_collector)

    result = await live_smoke._reddit()

    assert result.status == "passed"
    assert len(captured) == 1
    collector, limit = captured[0]
    assert collector.auth_mode == "access_token"
    assert collector.subreddit == "python"
    assert limit == 1


def test_main_fails_when_any_configured_probe_fails(monkeypatch, capsys) -> None:
    async def fake_smoke():
        return [
            live_smoke.SmokeResult("hackernews", "passed", events=1),
            live_smoke.SmokeResult("github", "failed", detail="schema changed"),
            live_smoke.SmokeResult("rss", "skipped"),
        ]

    monkeypatch.setattr(live_smoke, "run_smoke", fake_smoke)

    assert live_smoke.main(["--json"]) == 1
    assert '"status": "failed"' in capsys.readouterr().out


def test_main_allows_optional_skips(monkeypatch) -> None:
    async def fake_smoke():
        return [
            live_smoke.SmokeResult("hackernews", "passed", events=1),
            live_smoke.SmokeResult("github", "passed", events=1),
            live_smoke.SmokeResult("reddit", "skipped"),
        ]

    monkeypatch.setattr(live_smoke, "run_smoke", fake_smoke)

    assert live_smoke.main([]) == 0
