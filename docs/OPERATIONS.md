# Operations guide

SignalKit Stream is designed to run as a small independent ingestion process. This guide covers preflight checks, process lifecycle, SQLite ownership, source progress, delivery recovery, backup/restore, and monitoring.

## Preflight

Validate configuration and local credential wiring without polling external sources:

```bash
signalkit validate signalkit.toml
signalkit doctor signalkit.toml
```

Machine-readable output is available for both:

```bash
signalkit validate signalkit.toml --format json
signalkit doctor signalkit.toml --format json
```

The local diagnostic path is deliberately not a live compatibility probe. It checks configuration, enabled source/sink construction, required environment-backed credentials, database paths, SQLite integrity, and persistent schema compatibility without making normal source/webhook availability part of startup validation.

A separate opt-in GitHub Actions live-smoke workflow exists for public API compatibility checks.

## Process lifecycle

Start the runtime:

```bash
signalkit run signalkit.toml
```

Run one collection/delivery cycle:

```bash
signalkit run signalkit.toml --once
```

The long-running runtime handles SIGINT/SIGTERM and stops source/delivery workers cleanly. Already committed SQLite transactions remain durable. Work that was fetched but not committed is safe to repeat because collection uses stable source IDs and idempotent persistence.

For systemd, Docker, Kubernetes, or another supervisor, let the supervisor restart the process after an unexpected exit. Stream persists checkpoints, source health, normalized events, and outbox delivery state locally, so a restart does not reset logical progress.

## SQLite ownership

Treat the configured SQLite database as durable application state, not a cache. It contains:

- normalized events
- source checkpoints
- source health
- delivery-sink registration
- pending / failed / dead / delivered outbox state
- the persistent database schema version

Use **one Stream writer process per SQLite database** unless another topology has been deliberately load/failure tested. SQLite supports concurrent readers, but independent writers compete for the same database write lock.

The remaining 1.0 hardening includes more deterministic lock/busy tests and explicit production guidance for WAL-related deployments.

## Persistent schema and upgrades

Stream records its SQLite layout version in `PRAGMA user_version`.

Startup behavior is fail-closed:

```text
database version < supported -> forward migration
database version = supported -> validate and run
database version > supported -> refuse startup
```

Migrations run atomically before the store is used. Do not manually change `user_version` to bypass an incompatibility.

See `docs/MIGRATIONS.md` for migration and rollback policy.

## Backup before upgrades

Create a consistent backup through SQLite's backup API:

```bash
python -m signalkit_stream.maintenance backup signals.db backups/pre-upgrade.db
```

Verify it before relying on it:

```bash
python -m signalkit_stream.maintenance verify backups/pre-upgrade.db
```

The backup implementation writes a temporary sibling, verifies it with `PRAGMA quick_check`, then atomically publishes the final backup path. With `--overwrite`, an older backup is not replaced until the new copy passes verification.

For the cleanest application-version boundary, stop the writer before deploying a release that may migrate persistent schema.

See `docs/BACKUP.md` for the restore runbook.

## Source progress

Inspect persisted source health:

```bash
signalkit status --db signals.db
```

Inspect one checkpoint:

```bash
signalkit checkpoint hackernews:newstories --db signals.db
```

Do not edit cursor JSON manually. Cursors belong to adapter protocols and may contain pagination, in-progress page, and recent-ID watermark state required to avoid gaps.

## Delivery operations

Inspect durable delivery state:

```bash
signalkit deliveries --db signals.db
signalkit deliveries --db signals.db --sink brain --format json
```

Retryable failures remain eligible for scheduled retry. Permanent failures or exhausted retry budgets become `dead`.

After fixing the downstream destination, replay dead letters:

```bash
signalkit retry-deliveries brain --db signals.db
```

This returns dead rows to pending state. It does not recollect the original source.

## At-least-once behavior

A remote sink can accept a request immediately before the Stream process dies. If local acknowledgement was not committed, the same event version can be sent again after restart. This is intentional at-least-once behavior.

Webhook consumers receive version-aware idempotency headers and should honor the idempotency key when their own side effects are not naturally idempotent.

If a source object changes while an older version is in flight, success for the old payload cannot erase the newer pending outbox version.

## Monitoring

Read a persisted operational snapshot:

```bash
python -m signalkit_stream.observability signals.db
```

JSON and Prometheus exposition text:

```bash
python -m signalkit_stream.observability signals.db --format json
python -m signalkit_stream.observability signals.db --format prometheus
```

Snapshots include schema status, total stored signals, source health/failure counters, per-sink backlog/status counts, attempt totals, and latest persisted errors.

See `docs/OBSERVABILITY.md` for metric names and structured logging utilities.

## Recovery scenarios

### Crash before collection commit

The event/checkpoint/outbox transaction rolls back. The page can be fetched again after restart; stable IDs make replay safe.

### Crash after collection commit

Events, checkpoint, and pending outbox rows are durable. Collection resumes from the committed cursor.

### Crash during sink delivery

The source checkpoint is unaffected. If local delivery acknowledgement was not committed, the outbox row is retried after restart.

### One source fails repeatedly

Other workers continue. The failing source records degraded/circuit-open health and waits according to backoff/cooldown.

### One sink fails

Other sinks use independent delivery rows and continue. Repair the failed destination, then replay dead letters where needed.

### Database is from a newer Stream release

Startup refuses to mutate it. Run the matching/newer application build or restore a compatible backup; do not force the version marker backward.

### Database claims the current version but required objects are missing

Treat it as corruption or unsupported manual schema modification. Preserve the file for diagnosis and recover from a known-good backup instead of allowing the runtime to silently guess repairs.

## Release checks

Normal PR CI is deterministic and independent of third-party availability. It includes mocked HTTP source behavior, collector contracts, persistence/migration checks, fault-injection reliability tests, backup tests, and supported Python-version coverage.

Live source compatibility checks remain isolated from the normal correctness gate so third-party outages, policy changes, credentials, or rate limits cannot make ordinary code review nondeterministic.
