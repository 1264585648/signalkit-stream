# Operations guide

SignalKit Stream is designed to run as a small independent ingestion process. This guide covers preflight diagnostics, database behavior, shutdown, checkpoints, delivery failures, and recovery.

## Preflight diagnostics

Before starting a new deployment or after changing configuration, run:

```bash
python -m signalkit_stream.diagnostics signalkit.toml
```

Machine-readable output:

```bash
python -m signalkit_stream.diagnostics signalkit.toml --format json
```

The doctor is deliberately offline. It does not contact Reddit, GitHub, Hacker News, RSS feeds, JSON feeds, REST APIs, or webhooks. It checks:

- the configuration file exists and parses
- every enabled source can be constructed from the current config/environment
- source identities are unique
- every enabled sink can be constructed
- sink keys are unique
- the database parent directory is writable
- an existing SQLite database passes `PRAGMA quick_check`
- an existing SQLite database can acquire an immediate write transaction

Because source construction resolves configured credential environment variables, missing local credentials are reported before the runtime starts. Secrets are not printed by the doctor.

A missing database is a warning, not an error: the runtime will create it on first start. A missing database parent directory is an error so deployment mistakes are not hidden by implicit directory creation during diagnostics.

## Process lifecycle

Start the runtime:

```bash
signalkit run signalkit.toml
```

The runtime handles SIGINT/SIGTERM by stopping source/delivery workers and leaving already committed SQLite transactions durable. Work that had not committed is safe to repeat because collection uses stable source IDs and idempotent persistence.

For deployment systems such as systemd, Docker, Kubernetes, or another supervisor, let the supervisor restart the process after an unexpected exit. SignalKit persists source checkpoints, health, events, and delivery state locally, so a restart does not reset logical progress.

## Database ownership

The default store is SQLite. Treat the database file as durable application state, not a cache.

It contains:

- normalized events
- source checkpoints
- source health
- enabled delivery sinks
- pending/retry/dead/delivered outbox state

Use one writer process per SQLite database unless you have deliberately tested a different topology. SQLite supports concurrent readers, but independent writer processes compete for the same database write lock. `doctor` reports a locked database when it cannot acquire an immediate write transaction.

Back up the database before upgrading across a release that changes persistent schema. Explicit migration/version tooling remains a release-gate item until the persistent schema is declared 1.0-stable.

## Source progress

Inspect source health:

```bash
signalkit status --db signals.db
```

Inspect one checkpoint:

```bash
signalkit checkpoint hackernews:newstories --db signals.db
```

Do not edit cursor JSON manually. A cursor is part of the adapter protocol and may carry pagination/seen-window state needed to avoid gaps.

## Delivery operations

Inspect durable delivery state:

```bash
signalkit deliveries --db signals.db
signalkit deliveries --db signals.db --sink brain --format json
```

A retryable sink failure remains in failed state until its scheduled retry time. A permanent failure or a delivery that exhausts the configured attempt budget becomes `dead`.

Replay dead letters after fixing the downstream problem:

```bash
signalkit retry-deliveries brain --db signals.db
```

This changes dead rows back to pending. It does not recollect the original source.

## At-least-once behavior

A remote sink can accept a request immediately before the SignalKit process dies. On restart, the local outbox row may still be pending, so that event version can be sent again. This is intentional.

Webhook consumers receive an idempotency key for the exact event version and should honor it when their own side effects are not naturally idempotent.

If the source object changes while an older version is in flight, the new source mutation remains pending even if delivery of the old payload returns success. This prevents an old acknowledgement from erasing the newer version.

## Recovery scenarios

### Runtime crashes before a collection transaction commits

The event/checkpoint/outbox transaction rolls back. The source page is fetched again after restart. Stable IDs make this safe.

### Runtime crashes after collection commits

Events, checkpoint, and pending outbox rows are already durable. Collection resumes from the committed cursor.

### Runtime crashes during sink delivery

The source checkpoint is unaffected. If local delivery acknowledgement was not committed, the outbox row is retried after restart.

### One source fails repeatedly

Other source workers continue. The failing source records degraded/circuit-open health and waits according to failure backoff/cooldown.

### One sink fails

Other sinks have independent delivery rows and continue. Fix the failed destination and replay dead letters if necessary.

## Deterministic release checks

The normal CI gate does not depend on third-party network availability. It covers source parsing with mocked HTTP transports plus crash/restart and partial-failure behavior against local SQLite.

Live public-API compatibility checks should remain opt-in/scheduled so Reddit/GitHub/feed outages or external rate limits cannot make ordinary code review nondeterministic.
