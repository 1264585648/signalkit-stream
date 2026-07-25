# Live compatibility testing

Normal SignalKit Stream CI is deterministic and does not depend on third-party availability. Public APIs and feeds can be down, rate-limited, changed, or temporarily inconsistent for reasons unrelated to a code review, so network smoke tests are kept in a separate scheduled/manual workflow.

## Workflow

`.github/workflows/live-smoke.yml` runs once per day and can also be started with `workflow_dispatch`.

The workflow always exercises:

- Hacker News public API
- GitHub issue search using the workflow's built-in `GITHUB_TOKEN`

RSS, JSON Feed, and Reddit checks are enabled when the corresponding repository variables/secrets are configured.

## Repository variables

Optional variables:

```text
SIGNALKIT_LIVE_RSS_URL
SIGNALKIT_LIVE_JSON_FEED_URL
SIGNALKIT_LIVE_REDDIT_SUBREDDIT
```

Choose feeds that you control or consider stable enough to act as compatibility probes. Do not use a third-party feed as a release gate.

## Reddit secrets

The Reddit smoke test remains OAuth-only and is skipped unless the required configuration exists.

```text
REDDIT_USER_AGENT
REDDIT_ACCESS_TOKEN
```

or refresh credentials:

```text
REDDIT_USER_AGENT
REDDIT_CLIENT_ID
REDDIT_CLIENT_SECRET
REDDIT_REFRESH_TOKEN
```

The client/use case must remain approved under Reddit's current API terms and policies. Do not configure the live job to bypass source access controls.

## Running locally

All live tests are skipped by default. Enable them explicitly:

```bash
SIGNALKIT_LIVE=1 pytest -q tests/live
```

Set only the credentials/URLs for probes you intend to execute; missing optional configuration produces skips rather than failures.

## What the smoke suite proves

These tests are deliberately small. They verify that current public response shapes still pass through the production adapters and produce valid SignalKit cursors/events. Source-specific deterministic tests remain responsible for pagination, malformed responses, retries, rate limits, normalization edge cases, and reliability semantics.

A scheduled live failure should trigger investigation, not an automatic conclusion that SignalKit code regressed. Confirm source status, credentials, policy changes, and rate limits before changing an adapter.
