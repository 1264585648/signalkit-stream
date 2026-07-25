# SignalKit Stream roadmap

This roadmap defines the boundary for the complete Stream module. Downstream LLM analysis, lead scoring, enrichment, CRM sync, outreach, and autonomous actions are explicitly outside this repository.

## Core protocol and persistence — implemented

- versioned `SignalEvent` schema
- deterministic source-object identity and mutation fingerprints
- `SourceIdentity`, `Cursor`, `CollectorContext`, and `CollectorResult`
- stable collector error taxonomy
- collector SDK with shared HTTP retry/backoff and rate-limit snapshots
- atomic SQLite event + checkpoint persistence
- insert/update/unchanged classification
- resumable collection pipeline with pagination-loop guards
- legacy SQLite migration coverage
- deterministic offline HTTP simulation tests

## Runtime layer — implemented

The runtime can execute one-shot cycles or stay alive as an independent ingestion process.

Implemented:

- strict TOML configuration
- source registry / adapter factories
- independent source workers with configurable polling intervals
- bounded global concurrency
- graceful SIGINT/SIGTERM lifecycle
- rate-limit-aware scheduling
- failure backoff and circuit-open cooldown
- persisted source health, last attempt, and last success state
- `signalkit init`, `signalkit run`, and `signalkit status`
- scheduler tests using injected sleep/clock functions instead of wall-clock waits

Runtime reliability continues to be strengthened by restart/fault-injection tests under the 1.0 gate.

## Delivery layer — implemented

Collection and downstream delivery are separated by a durable transactional outbox.

Implemented:

- `Sink` protocol
- stdout sink
- JSONL sink
- webhook sink
- stable webhook idempotency keys
- one outbox row per sink/event, giving natural fan-out with independent failure state
- transactional enqueue on signal insert or source-visible mutation
- exponential retry scheduling and `Retry-After` support
- dead-letter persistence and replay
- optional historical backfill when enabling a sink
- independent delivery workers and graceful cancellation
- `signalkit deliveries` and `signalkit retry-deliveries`

The delivery contract is at-least-once. Consumers that perform non-idempotent side effects should honor the provided idempotency key.

PostgreSQL, Redis Streams, Kafka, object-storage archives, or additional sink types can be optional integrations later without becoming core dependencies.

## First-party adapter completion — next

With the shared runtime and delivery contracts in place, complete the source set without duplicating infrastructure inside adapters.

Planned order:

1. Reddit adapter using the current official API and app credentials
   - posts
   - comments
   - pagination
   - incremental state
   - authentication and rate-limit handling
2. JSON Feed adapter
3. generic REST adapter / SDK reference
4. stronger RSS behavior for feeds without useful validators and long backfills
5. deeper comment pagination for GitHub/Hacker News where useful
6. common collector contract test suite applied automatically to every first-party adapter

Every adapter must use source-native immutable IDs, emit timezone-aware events, advance a resumable cursor, terminate pagination, and pass source-specific malformed/error fixture tests.

## Operations and developer experience — next

The remaining operational surface before 1.0:

- structured machine-readable logging
- collection and delivery metrics
- sink health / last delivery failure summaries
- credential diagnostics
- `signalkit doctor`
- configuration dry-run / validation command
- database and migration diagnostics
- explicit persistent schema versioning and forward migrations
- debug mode without changing reliability semantics
- documented backup, upgrade, and recovery process

`signalkit status`, checkpoint inspection, delivery counts, and dead-letter replay already exist.

## Reliability and compatibility test completion — next

Before 1.0, add focused tests beyond the deterministic unit/integration suite:

- terminate between source fetch and transaction commit; restart and prove replay is safe
- terminate after event/checkpoint/outbox commit; prove resume starts after committed cursor
- terminate during sink delivery; prove outbox row is retried and no source recollection is needed
- corrupted cursor / malformed persisted state behavior
- SQLite lock/busy behavior and documented operational limits
- multi-source isolation under repeated failures
- multi-sink partial failure behavior
- migration tests across every released persistent schema
- opt-in live compatibility smoke jobs for first-party public APIs

Live tests remain outside normal deterministic PR CI so third-party availability does not decide whether a commit is valid.

## 1.0 release gate

SignalKit Stream reaches 1.0 only when the module can be installed, configured, started, stopped, upgraded, diagnosed, and left running as an independent ingestion service.

Required before 1.0:

- core protocol and persistent checkpoint model complete
- runtime scheduler and graceful lifecycle complete
- durable delivery/outbox abstraction complete
- RSS, Hacker News, GitHub, Reddit, JSON Feed, and generic REST extension path complete
- retry, rate limiting, circuit breaking, restart, and partial failure behavior tested
- explicit migrations tested across released persistent schemas
- source and sink diagnostics plus `doctor` available
- deterministic CI plus opt-in live compatibility smoke tests
- architecture, adapter authoring, operations, migration, and testing documentation complete

The 1.0 gate does not include LLM classification or business logic. Those belong to later SignalKit modules consuming the event stream.
