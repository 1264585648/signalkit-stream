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
- subprocess lifecycle tests through the real packaged CLI

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
- process-level replay evidence after abrupt death during a remote webhook side effect
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

- `docs/ARCHITECTURE.md`
- `docs/COMPATIBILITY.md`
- `docs/MIGRATIONS.md`
- `docs/BACKUP.md`
- `docs/OBSERVABILITY.md`
- `docs/OPERATIONS.md`
- `docs/REDDIT.md`
- `docs/LIVE_TESTING.md`
- `docs/COLLECTOR_SDK.md`
- `docs/RELEASE.md`

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
- real CLI SIGTERM shutdown followed by restart and continued collection
- real CLI abrupt kill during an in-flight webhook followed by durable outbox replay
- identical event/version idempotency identity across the interrupted and replayed webhook attempts

Live compatibility checks remain outside normal deterministic PR CI so third-party availability cannot decide whether a commit is correct.

## Release engineering — in final hardening

The repository now includes:

- explicit compatibility/deprecation policy
- pre-1.0 changelog
- release/upgrade checklist
- version-consistency test between package metadata and runtime `__version__`
- wheel + source-distribution build in CI
- clean wheel installation/CLI/config-validation smoke
- clean sdist installation/import/CLI smoke
- reviewed architecture/operations/source-authoring documentation

Normal CI builds release artifacts for inspection but intentionally does not publish packages or tags.

## Migration compatibility matrix as versions accumulate

Schema version 1 has legacy/unversioned migration coverage. When a second persistent database schema version ships, preserve representative fixtures for every supported released version and test upgrade-to-current while retaining:

- events
- checkpoints
- source health
- sink registration
- delivery/outbox state

Never add a downgrade path. Future-version databases must continue to fail closed.

This work is intentionally triggered by a real schema-v2 change rather than inventing a no-op migration solely to satisfy a matrix.

## Remaining 1.0 closure

Before the 1.0 tag/publication:

- choose/finalize the exact 1.0 version commit
- move `CHANGELOG.md` `Unreleased` entries under the 1.0 version/date and write final release notes
- confirm the intended public Python/CLI/config surface against `docs/COMPATIBILITY.md`
- run/inspect deterministic package gates on the exact release commit
- inspect a recent live compatibility smoke separately from correctness CI
- follow `docs/RELEASE.md` backup/upgrade/package inspection checklist
- tag/publish as an explicit maintainer action; ordinary CI has no publish credentials

## 1.0 release gate

SignalKit Stream reaches 1.0 when the module can be installed, configured, started, stopped, upgraded, diagnosed, backed up, restored, monitored, and left running as an independent ingestion service with explicit compatibility boundaries.

Engineering gates already satisfied:

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
- real subprocess restart and delivery-replay lifecycle evidence
- release-package clean-install gates and compatibility/release documentation

The only unconditional work left before calling a particular commit `1.0.0` is final release-version/release-note/API-signoff and the explicit tag/publish action. Migration fixture expansion remains conditional on a real second persistent schema version shipping.

The 1.0 gate does not include LLM classification or business logic. Those belong to later SignalKit modules consuming the normalized event stream.
