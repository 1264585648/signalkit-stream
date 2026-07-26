# Live compatibility testing

SignalKit Stream separates deterministic correctness tests from networked compatibility probes.

Normal pull-request CI uses mocked HTTP transports and local SQLite fixtures. Third-party availability, rate limits, credentials, or policy changes must not decide whether an ordinary code change is correct.

The repository also includes `.github/workflows/live-smoke.yml` for a small real-network compatibility matrix.

## Schedule

The workflow runs:

- manually through `workflow_dispatch`;
- once per day on a scheduled GitHub Actions run.

A live failure is evidence that an external integration needs investigation. It is intentionally separate from normal PR CI.

## Always-probed sources

### Hacker News

The smoke job requests one item from the public Hacker News API through the real first-party collector.

### GitHub

The smoke job searches public CPython issues through the real GitHub collector. In GitHub Actions it uses the workflow's `GITHUB_TOKEN`.

## Optional feed probes

Repository variables can enable real feed targets:

```text
SIGNALKIT_LIVE_RSS_URL
SIGNALKIT_LIVE_JSON_FEED_URL
```

When a variable is absent, that source is reported as `skipped`. Once configured, a request/parsing/contract failure makes the live-smoke command exit non-zero.

Choose small, stable public feeds that the repository is permitted to poll. Do not use private customer endpoints as shared compatibility fixtures.

## Optional Reddit probe

Reddit is skipped unless a user agent and an approved OAuth credential mode are configured.

Repository variable:

```text
SIGNALKIT_LIVE_REDDIT_SUBREDDIT
```

If unset, the smoke script uses `python` as the subreddit name.

Repository secrets can use any currently supported collector credential mode:

```text
REDDIT_USER_AGENT

# Static bearer token
REDDIT_ACCESS_TOKEN

# Refresh-token flow
REDDIT_REFRESH_TOKEN
REDDIT_CLIENT_ID
REDDIT_CLIENT_SECRET   # optional for client types without a secret

# Confidential app-only flow
REDDIT_CLIENT_ID
REDDIT_CLIENT_SECRET
```

The workflow passes credential values only through GitHub Actions secrets. They are not committed to the repository or written into the JSON smoke report.

Before enabling the Reddit live probe, confirm the app/use case is approved under Reddit's current API and builder terms. See `docs/REDDIT.md`.

## Local execution

Run the same smoke script locally:

```bash
GITHUB_TOKEN=... python scripts/live_smoke.py
```

JSON output:

```bash
python scripts/live_smoke.py --json
```

Optional sources are configured with the same environment variables used by the workflow.

Example result:

```json
[
  {"source": "hackernews", "status": "passed", "events": 1, "detail": null},
  {"source": "github", "status": "passed", "events": 1, "detail": null},
  {"source": "rss", "status": "skipped", "events": 0, "detail": "SIGNALKIT_LIVE_RSS_URL is not configured"},
  {"source": "jsonfeed", "status": "skipped", "events": 0, "detail": "SIGNALKIT_LIVE_JSON_FEED_URL is not configured"},
  {"source": "reddit", "status": "skipped", "events": 0, "detail": "REDDIT_USER_AGENT is not configured"}
]
```

`skipped` means an optional source was not configured. `failed` means a configured/required source was contacted (or attempted) and the compatibility probe failed.

## Interpreting failures

A live failure does not automatically imply a Stream code regression. Check, in order:

1. third-party incident/outage status;
2. credential expiry or access approval changes;
3. rate-limit response;
4. upstream API/schema/behavior changes;
5. Stream adapter behavior.

If the upstream behavior changed legitimately, capture the smallest sanitized response fixture needed to reproduce the issue in deterministic tests before changing the adapter.
