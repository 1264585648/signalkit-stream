# SignalKit Stream roadmap

This roadmap defines the boundary for the complete Stream module. Downstream LLM analysis, lead scoring, enrichment, CRM sync, outreach, and autonomous actions are explicitly outside this repository.

## Foundation — implemented

The foundation establishes the contracts every runtime and adapter builds on:

- versioned `SignalEvent` schema
- deterministic source-object identity and mutation fingerprints
- `SourceIdentity`, `Cursor`, `CollectorContext`, and `CollectorResult`
- stable collector error taxonomy
- collector SDK with shared HTTP retry/backoff and rate-limit snapshots
- atomic SQLite event + checkpoint persistence
- insert/update/unchanged classification
- resumable collection pipeline with pagination-loop guards
- RSS/Atom, Hacker News, and GitHub adapters
- offline HTTP simulation tests
- legacy SQLite migration coverage

## Runtime layer — implemented

The runtime turns one-shot collection into a process that can run for days or weeks.

Implemented capabilities:

1. strict TOML configuration and validation
2. source registry / adapter factory
3. scheduler with per-source polling intervals
4. bounded global and provider-level concurrency
5. graceful shutdown with a bounded completion window and task cancellation
6. persisted source-aware pause when rate limits are exhausted
7. failure backoff and circuit-breaker cooldown after repeated failures
8. persisted source health, last-attempt, last-success, failure count, pause deadline, and rate-limit state
9. `signalkit run` lifecycle command plus one-cycle smoke mode
10. deterministic scheduler/restart tests with a controllable clock

Runtime release gate:

- restart resumes from persisted checkpoints
- a source pause survives restart
- a failing source does not stop healthy sources
- rate-limited sources do not busy-loop
- repeated failures enter a persisted cooldown
- scheduler timing can be tested without wall-clock sleeps
- SIGINT/SIGTERM request bounded graceful shutdown before cancellation

## Delivery layer — next

Separate collection from destinations with a `Sink` protocol and durable delivery state.

First-party sinks:

1. stdout / JSONL
2. SQLite event-store delivery adapter
3. webhook sink with retry and idempotency key
4. fan-out sink for multiple destinations
5. failed-delivery / dead-letter persistence
6. replay without recollecting upstream sources

Release gate:

- sink failure never advances delivery state incorrectly
- a failed sink delivery can be replayed without recollecting the source
- fan-out defines and tests partial-failure semantics
- webhook retries preserve a stable idempotency key
- delivery restart resumes from durable delivery state

PostgreSQL, Redis Streams, and Kafka can be added later as optional integrations without making them core runtime dependencies.

## Adapter completion

After the shared runtime and delivery contracts are stable, complete the first-party source set:

1. Reddit adapter using the official API and app credentials
   - posts
   - comments
   - pagination
   - incremental state
   - rate-limit handling
2. JSON Feed adapter
3. generic REST adapter example / SDK reference
4. stronger RSS feed-change handling for long backfills
5. deeper comment pagination where source APIs support it

Every first-party adapter must pass the common collector contract suite plus source-specific fixture tests.

## Operations and developer experience

Add the operational surface required to treat Stream as infrastructure:

- structured logs
- source health model exposed through CLI
- collection and delivery metrics
- configuration validation errors with actionable messages
- credential diagnostics
- `signalkit status`
- `signalkit doctor`
- runtime statistics
- debug / dry-run modes
- documented upgrade and migration process

## 1.0 release gate

SignalKit Stream reaches 1.0 only when the module can be installed, configured, started, stopped, upgraded, and left running as an independent ingestion service.

Required before 1.0:

- runtime scheduler and graceful lifecycle complete
- delivery/sink abstraction complete
- RSS, Hacker News, GitHub, Reddit, and generic feed/REST extension path complete
- retry, rate limiting, circuit breaking, and restart behavior tested
- migrations tested across released persistent schemas
- health/status/doctor operational commands available
- deterministic CI plus opt-in live compatibility smoke tests
- architecture, adapter authoring, operations, and testing documentation complete

The 1.0 gate does not include LLM classification or business logic. Those belong to later SignalKit modules consuming the event stream.
