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

### Adapter enhancements that are not foundation blockers

- deeper comment/thread pagination for sources where bounded top-level comments are insufficient
- stronger RSS behavior for unusual feeds without useful validators or with very large historical backfills
- source-specific tombstone/deletion semantics where an upstream API exposes them
- more first-party sources only when they justify a dedicated semantic contract

## Operations and developer experience — implemented foundation

- configuration dry-run / validation command
- offline `signalkit doctor`
- credential-environment diagnostics without printing secrets
- source health and checkpoint inspection
- delivery state inspection and dead-letter replay
- persistent database schema diagnostics
- atomic SQLite backup using SQLite's backup API
- read-only SQLite integrity + schema verification
- backup / upgrade / restore documentation
- persisted source/sink operational snapshots
- table / JSON / Prometheus exposition output
- dependency-free structured JSON logging utilities
- opt-in live compatibility smoke workflow separated from deterministic PR CI

See:

- `docs/MIGRATIONS.md`
- `docs/BACKUP.md`
- `docs/OBSERVABILITY.md`
- `docs/REDDIT.md`
- `docs/COLLECTOR_SDK.md`

## Reliability evidence — implemented

The deterministic suite now covers major failure boundaries including:

- rollback of event + checkpoint + outbox when collection commit fails
- restart after committed event/checkpoint/outbox state
- delivery cancellation after a simulated remote side effect and replay after restart
- independent multi-sink partial failure
- protection against in-flight event-version races
- isolation of failing and healthy runtime sources
- first-party collector identity/cursor/timestamp/replay contracts
- migration rollback and future-version refusal
- backup integrity verification and atomic backup replacement

Live compatibility checks remain outside normal deterministic PR CI so third-party availability cannot decide whether a commit is correct.

## Remaining 1.0 hardening

The remaining work is deliberately narrow.

### 1. End-to-end process lifecycle tests

Add subprocess-level tests that start the real daemon, terminate it at controlled persistence/delivery boundaries, restart it, and prove the same invariants currently covered by component-level fault injection.

### 2. SQLite operational limits

- deterministic lock/busy behavior tests
- document recommended single-writer deployment model
- validate behavior around WAL/backup/recovery configurations used in production
- make lock-related diagnostics actionable

### 3. Migration compatibility matrix

Schema version 1 has legacy/unversioned migration coverage. As additional persistent schema versions are released, preserve representative fixtures and test every supported released version upgrading to current while retaining:

- events
- checkpoints
- source health
- sink registration
- delivery/outbox state

Never add a downgrade path. Future-version databases must continue to fail closed.

### 4. Operational CLI consolidation

Maintenance and observability APIs are implemented, but the operator experience can be made more cohesive before 1.0:

- fold high-value database verification/backup entry points into the main `signalkit` CLI where appropriate
- provide a richer single status view for sources, sinks, schema state, and delivery backlog
- keep JSON/Prometheus outputs stable for automation

### 5. Live compatibility matrix

Keep live tests opt-in/non-blocking for PR correctness, while expanding the manual/scheduled compatibility matrix across the first-party public sources where stable test targets are available. Reddit live checks remain conditional on approved credentials.

### 6. Release engineering

Before the 1.0 tag:

- final public API review and compatibility policy
- changelog/release notes
- clean-package installation smoke test
- documented upgrade path from the latest pre-1.0 release
- final architecture / operations / adapter-authoring documentation review

## 1.0 release gate

SignalKit Stream reaches 1.0 when the module can be installed, configured, started, stopped, upgraded, diagnosed, backed up, restored, monitored, and left running as an independent ingestion service with explicit compatibility boundaries.

Already satisfied:

- core protocol and persistent checkpoint model
- runtime scheduler and graceful lifecycle
- durable delivery/outbox abstraction
- RSS, Hacker News, GitHub, Reddit, JSON Feed, and generic REST extension path
- retry, rate limiting, circuit breaking, and major partial-failure behavior
- persistent schema versioning / forward migration foundation
- source/sink diagnostics and `doctor`
- backup/verify and observability foundation
- deterministic multi-version Python CI and separate live compatibility workflow

Still required:

- subprocess-level restart/lifecycle evidence
- SQLite lock/busy operational hardening
- operational CLI consolidation
- release-engineering and documentation closure
- migration fixture expansion whenever more than one persistent version has shipped

The 1.0 gate does not include LLM classification or business logic. Those belong to later SignalKit modules consuming the normalized event stream.
