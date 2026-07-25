# Testing strategy

The normal SignalKit Stream CI suite is deterministic and does not require external network access. Public APIs are simulated at the HTTP boundary so failures can be reproduced exactly. Live API checks are kept separate and opt-in.

## Test layers

### 1. Schema and unit tests

Cover stable event IDs, schema serialization, fingerprints, cursor serialization, source identity, error contracts, parsing helpers, and validation.

### 2. Collector contract tests

Every first-party collector must satisfy the same behavioral contract:

- returns `CollectorResult`
- emits valid `SignalEvent` values
- event IDs are stable
- timestamps are timezone-aware
- returned cursors belong to the collector source key
- pagination terminates and advances the cursor
- duplicate collection is safe
- comments preserve parent relationships where applicable

The runtime validates these invariants before any page can mutate SQLite or advance its checkpoint.

### 3. HTTP simulation

Use `httpx.MockTransport` to prove behavior for:

- HTTP 429 and `Retry-After`
- transient 5xx responses
- timeouts and network failures
- authentication failures
- malformed rate-limit headers
- empty and malformed responses

No retry test should rely on real sleeping; inject a sleeper or controllable clock where necessary.

### 4. Storage integration

Use temporary SQLite databases to cover:

- insert / unchanged / update classification
- ID-based idempotency
- atomic event + checkpoint commits
- checkpoint recovery
- failure recording
- schema migration from previous releases
- transactional outbox writes
- delivery retry/dead-letter/replay state
- query and filtering behavior

### 5. Pipeline integration

Run fake resumable collectors through the real pipeline and store. Prove:

- multi-page draining
- checkpoint resume across runs
- maximum-item limits
- pagination-loop protection
- failure leaves the last committed checkpoint position intact
- collector-contract failures cannot commit invalid events

### 6. Recorded API fixtures

When external schemas become more complex, store small sanitized response fixtures captured from official APIs and replay them offline. Fixtures must not contain secrets or unnecessary personal data.

### 7. Live compatibility smoke

`.github/workflows/live-smoke.yml` is a manual `workflow_dispatch` workflow. It exists to detect upstream compatibility drift; it is not part of deterministic PR CI.

The smoke checks:

- Hacker News through its public API
- GitHub issue search using the workflow's GitHub token
- Reddit only when `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, and `REDDIT_USER_AGENT` repository secrets are configured

Reddit is reported as skipped when those credentials are absent. The workflow requests only one primary item per source and never prints secret values.

Run the same probe locally with:

```bash
python scripts/live_smoke.py
```

or emit a machine-readable report:

```bash
python scripts/live_smoke.py --json
```

The GitHub workflow uploads `live-smoke.json` as an artifact for later inspection.

### 8. Restart and end-to-end tests

Long-running behavior is tested by interrupting logical progress between pages and between delivery attempts, restarting from persisted state, and proving that committed signals are not lost and duplicate collection remains idempotent.

## Release gate

A change that affects a public protocol, collector, persistence, or runtime behavior is not complete until all relevant gates pass:

- Ruff lint
- deterministic tests on Python 3.11, 3.12, and 3.13
- project coverage at or above 80%
- `compileall`
- protocol documentation for public contract changes
- collector contract tests for adapter changes
- pagination, retry, and resume tests for networked collectors
- migration tests for persistent schema changes

Pytest JUnit reports are uploaded for every Python matrix job, including failed jobs, so failures can be diagnosed without turning live traffic into a debugging tool.

Coverage is a guardrail rather than a correctness metric. Reliability paths and invariants matter more than maximizing a percentage.
