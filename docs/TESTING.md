# Testing strategy

The normal SignalKit Stream CI suite is deterministic and does not require external network access. Public APIs are simulated at the HTTP boundary so failures can be reproduced exactly.

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

### 3. HTTP simulation

Use `httpx.MockTransport` to prove behavior for:

- HTTP 429 and `Retry-After`
- transient 5xx responses
- timeouts and network failures
- authentication failures
- malformed rate-limit headers
- empty and malformed responses

No retry test should rely on real sleeping; inject a sleeper where necessary.

### 4. Storage integration

Use temporary SQLite databases to cover:

- insert / unchanged / update classification
- ID-based idempotency
- atomic event + checkpoint commits
- checkpoint recovery
- failure recording
- schema migration from previous releases
- query and filtering behavior

### 5. Pipeline integration

Run fake resumable collectors through the real pipeline and store. Prove:

- multi-page draining
- checkpoint resume across runs
- maximum-item limits
- pagination-loop protection
- failure leaves the last committed checkpoint intact

### 6. Recorded API fixtures

When external schemas become more complex, store small sanitized response fixtures captured from official APIs and replay them offline. Fixtures must not contain secrets or unnecessary personal data.

### 7. Live smoke tests

Live GitHub, Hacker News, RSS, and future Reddit checks are useful compatibility probes, but they are separate, opt-in jobs. They must not make deterministic PR CI depend on third-party uptime or credentials.

### 8. Restart and end-to-end tests

The long-running runtime must eventually be tested by deliberately terminating collection between pages and between delivery attempts, restarting it, and proving that no committed signal is lost and duplicates remain idempotent.

## Release gate

A change that affects a public protocol, collector, persistence, or runtime behavior is not complete until all relevant gates pass:

- Ruff lint
- deterministic tests on all supported Python versions
- project coverage at or above 80%
- `compileall`
- protocol documentation for public contract changes
- collector contract tests for adapter changes
- pagination, retry, and resume tests for networked collectors
- migration tests for persistent schema changes

Coverage is a guardrail rather than a correctness metric. Reliability paths and invariants matter more than maximizing a percentage.
