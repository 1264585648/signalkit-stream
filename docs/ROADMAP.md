# SignalKit Stream roadmap

This roadmap defines the boundary for the complete Stream module. Downstream LLM analysis, lead scoring, enrichment, CRM sync, outreach, and autonomous actions are explicitly outside this repository.

The project is now in **1.0 hardening**, not source-foundation construction.

## Core protocol and persistence — implemented

- versioned `SignalEvent` schema
- deterministic source-object identity and mutation fingerprints
- `SourceIdentity`, `Cursor`, `CollectorContext`, and `CollectorResult`
- stable collector error taxonomy
- collector-result contract enforcement before persistence/checkpoint advancement
- shared HTTP retry/backoff, `Retry-After`, and rate-limit snapshots
- atomic SQLite event + checkpoint persistence
- insert/update/unchanged classification
- resumable collection pipeline with pagination-loop guards
- explicit persistent schema version through SQLite `PRAGMA user_version`
- atomic forward migration registry
- legacy unversioned database migration
- future database-version refusal
- deterministic migration and storage tests
- configurable SQLite busy timeout for embedded deployments
- deterministic write-lock contention/recovery coverage
- read-only write-lock diagnostics
- WAL reader/backup behavior under an active writer

`SignalEvent.schema_version` and the persistent SQLite database schema version remain separate compatibility boundaries.

## Runtime layer — implemented

The runtime can execute one-shot cycles or remain alive as an independent ingestion process.

- strict TOML configuration
- source registry / adapter factories
- independent source workers with configurable polling intervals
- bounded global concurrency
- graceful SIGINT/SIGTERM lifecycle
- rate-limit-aware scheduling
- failure backoff and circuit-open cooldown
- persisted source health, last attempt, and last success state
- `signalkit init`, `signalkit run`, `signalkit status`
- text or structured JSON runtime logging
- scheduler tests with injected sleep/clock functions rather than wall-clock waits

## Delivery layer — implemented

Collection and downstream delivery are separated by a durable transactional outbox.

- `Sink` protocol
- stdout sink
- JSONL sink
- webhook sink
- stable version-aware webhook idempotency keys
- one outbox row per sink/event for independent fan-out
- transactional enqueue on signal insert or source-visible mutation
- exponential retry scheduling and `Retry-After`
- dead-letter persistence and replay
- optional historical backfill when enabling a sink
- independent delivery workers and graceful cancellation
- protection against old in-flight deliveries acknowledging newer event versions
- `signalkit deliveries` and `signalkit retry-deliveries`

The delivery contract is at-least-once. Consumers performing non-idempotent side effects should honor the supplied idempotency key.

## Source layer — implemented foundation

First-party adapters:

- RSS / Atom
- JSON Feed 1.x with `next_url` support
- Hacker News
- GitHub issue / pull-request search
- Reddit OAuth

The Reddit adapter supports static access tokens, refresh-token rollover, app-only client credentials, in-memory access-token caching, and a single re-authentication/retry on API HTTP 401 when fresh credentials are available.

The generic JSON REST extension path is also implemented for explicitly mapped GET/list APIs. It remains outside `default_registry()` by design because arbitrary APIs do not share safe ordering, pagination, authentication, or transformation semantics.

All first-party collectors participate in shared deterministic contract tests in addition to source-specific tests.

### Adapter enhancements that are not 1.0 blockers

- deeper comment/thread pagination where bounded top-level comments are insufficient
- stronger RSS behavior for unusual feeds without useful validators or with very large historical backfills
- source-specific tombstone/deletion semantics where an upstream API exposes them
- additional first-party sources only when they justify a dedicated semantic contract

## Operations and developer experience — implemented foundation

- configuration dry-run / validation command
- offline `signalkit doctor`
- credential-environment diagnostics without printing secrets
- SQLite integrity, schema, and write-lock diagnostics
- source health and checkpoint inspection
- delivery state inspection and dead-letter replay
- `signalkit db backup` and `signalkit db verify`
- atomic SQLite backup using SQLite's backup API
- backup / upgrade / restore documentation
- `signalkit status --verbose` for database/source/sink state
- table / JSON / Prometheus output
- dependency-free structured JSON logging utilities
- scheduled/manual live compatibility smoke workflow separated from deterministic PR CI
- live Hacker News/GitHub probes plus optional RSS, JSON Feed, and approved Reddit OAuth probes

See:

- `docs/MIGRATIONS.md`
- `docs/BACKUP.md`
- `docs/OBSERVABILITY.md`
- `docs/OPERATIONS.md`
- `docs/REDDIT.md`
- `docs/LIVE_TESTING.md`
- `docs/COLLECTOR_SDK.md`

## Reliability evidence — implemented

The deterministic suite covers major failure boundaries including:

- rollback of event + checkpoint + outbox when collection commit fails
- restart after committed event/checkpoint/outbox state
- delivery cancellation after a simulated remote side effect and replay after restart
- independent multi-sink partial failure
- protection against in-flight event-version races
- isolation of failing and healthy runtime sources
- first-party collector identity/cursor/timestamp/replay contracts
- migration rollback and future-version refusal
- backup integrity verification and atomic backup replacement
- SQLite immediate write-lock contention without partial persistence
- recovery after a competing writer releases the database
- write-lock diagnostics under both available and busy states
- consistent WAL backup of the last committed snapshot while another writer transaction is active

Live compatibility checks remain outside normal deterministic PR CI so third-party availability cannot decide whether a commit is correct.

## Remaining 1.0 hardening

The remaining work is deliberately narrow.

### 1. End-to-end process lifecycle evidence

Add subprocess-level tests that start the real CLI/runtime process, stop or kill it at controlled points, reopen the real SQLite database, and prove the same restart invariants already covered by component-level fault injection:

- normal SIGTERM shutdown and restart
- restart after a committed source cycle
- abrupt termination with durable checkpoint/outbox recovery
- no requirement to recollect a source merely because delivery was interrupted

The goal is lifecycle evidence through the packaged operator boundary, not a second scheduler implementation.

### 2. Migration compatibility matrix as versions accumulate

Schema version 1 has legacy/unversioned migration coverage. When a second persistent database schema version ships, preserve representative fixtures for every supported released version and test upgrade-to-current while retaining:

- events
- checkpoints
- source health
- sink registration
- delivery/outbox state

Never add a downgrade path. Future-version databases must continue to fail closed.

This work is intentionally triggered by a real schema-v2 change rather than inventing a no-op migration solely to satisfy a matrix.

### 3. Release engineering and public compatibility policy

Before the 1.0 tag:

- final public Python/CLI API review
- explicit compatibility/deprecation policy
- clean wheel/sdist installation smoke test
- changelog and 1.0 release notes
- documented upgrade path from the latest pre-1.0 release
- final architecture / operations / adapter-authoring documentation review
- remove or close stale development branches/PRs that no longer represent current architecture

## 1.0 release gate

SignalKit Stream reaches 1.0 when the module can be installed, configured, started, stopped, upgraded, diagnosed, backed up, restored, monitored, and left running as an independent ingestion service with explicit compatibility boundaries.

Already satisfied:

- core protocol and persistent checkpoint model
- runtime scheduler and graceful lifecycle
- durable delivery/outbox abstraction
- RSS, Hacker News, GitHub, Reddit, JSON Feed, and generic REST extension path
- retry, rate limiting, circuit breaking, and major partial-failure behavior
- persistent schema versioning / forward migration foundation
- source/sink/database diagnostics and `doctor`
- backup/verify and observability foundation
- main operator CLI consolidation
- deterministic multi-version Python CI
- separate scheduled/manual live compatibility workflow
- SQLite busy/lock operational hardening and single-writer guidance

Still required:

- subprocess-level restart/lifecycle evidence
- release-engineering and public compatibility closure
- migration fixture expansion when more than one persistent schema version has actually shipped

The 1.0 gate does not include LLM classification or business logic. Those belong to later SignalKit modules consuming the normalized event stream.
