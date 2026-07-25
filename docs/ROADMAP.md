# SignalKit Stream roadmap

This roadmap defines the boundary for the complete Stream module. Downstream LLM analysis, lead scoring, enrichment, CRM sync, outreach, and autonomous actions are explicitly outside this repository.

## Foundation — implemented

The current foundation establishes the contracts future runtime work will build on:

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

## Runtime layer — next

Turn one-shot collection into a process that can run for days or weeks.

Implementation order:

1. TOML configuration and strict validation
2. source registry / adapter factory
3. scheduler with per-source polling intervals
4. bounded global and per-source concurrency
5. graceful shutdown and task cancellation
6. source-aware pause when rate limits are exhausted
7. circuit breaker and cooldown after repeated failures
8. persisted source health / last-attempt / last-success state
9. `signalkit run` lifecycle command

Release gate:

- restart resumes from persisted checkpoints
- SIGINT/SIGTERM cannot corrupt committed state
- a failing source does not stop healthy sources
- rate-limited sources do not busy-loop
- scheduler behavior is covered with a controllable clock, not wall-clock sleeps

## Delivery layer

Separate collection from destinations with a `Sink` protocol.

First-party sinks:

1. stdout / JSONL
2. SQLite event store adapter
3. webhook sink with retry and idempotency key
4. fan-out sink for multiple destinations
5. failed-delivery / dead-letter persistence

Release gate:

- sink failure never advances delivery state incorrectly
- a failed sink delivery can be replayed without recollecting the source
- fan-out defines and tests partial-failure semantics

PostgreSQL, Redis Streams, and Kafka can be added later as optional integrations without making them core runtime dependencies.

## Adapter completion

After the shared runtime contracts are stable, complete the first-party source set:

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
- source health model
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
