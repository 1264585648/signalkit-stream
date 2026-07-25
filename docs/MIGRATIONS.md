# Persistent schema migrations

SignalKit Stream treats SQLite as durable application state. Persistent schema changes are forward-only and recorded explicitly in `schema_migrations`.

Current persistent schema version: **1**.

## Inspect schema status

```bash
python -m signalkit_stream.migrations status signals.db
```

JSON:

```bash
python -m signalkit_stream.migrations status signals.db --format json
```

A database created by releases before the migration ledger reports version `0` until it is adopted.

## Migrate

Back up the database, stop all writers, then run:

```bash
python -m signalkit_stream.migrations migrate signals.db
```

Version 1 is an adoption migration. It first runs the production store's compatibility initialization, validates that the current core tables/columns exist, and then records the baseline migration without deleting existing events, checkpoints, source health, sink configuration, or delivery rows.

Running the migration command again is idempotent.

## Upgrade procedure

For releases that declare a persistent schema change:

1. stop the SignalKit Stream writer process
2. copy/backup `signals.db`
3. install the new package/release
4. run `python -m signalkit_stream.migrations status signals.db`
5. run `python -m signalkit_stream.migrations migrate signals.db`
6. run `python -m signalkit_stream.diagnostics signalkit.toml`
7. start the runtime
8. inspect source/delivery health

Do not run two different SignalKit versions as writers against the same SQLite file during an upgrade.

## Forward-only policy

SignalKit does not automatically downgrade persistent schema. A database with a migration version newer than the running package is rejected by migration tooling. Restore the pre-upgrade backup if application rollback also requires schema rollback.

Future migrations are appended with monotonically increasing integer versions. Applied migration records are never rewritten to mean something different in a later release.

## Version 1 adoption invariants

The baseline validates the presence of the current durable boundaries:

```text
signals
checkpoints
source_health
delivery_sinks
deliveries
```

and their core identity/status fields before writing migration version 1.

The migration ledger itself is:

```text
schema_migrations(version PRIMARY KEY, name, applied_at)
```

## Backups

For a stopped process, a filesystem copy of the SQLite database is sufficient. For online backups, use SQLite's supported backup mechanisms rather than copying an actively written file without coordinating WAL state.

Treat backups as containing the same potentially sensitive normalized source content as the production database and protect them accordingly.

## Recovery

If migration fails:

- do not repeatedly start the runtime hoping the schema will repair itself
- keep the failed database for investigation
- inspect the error and schema status
- restore the pre-upgrade backup when necessary
- fix the migration/tooling and retry from a known-good copy

Schema migration tests cover fresh initialization, adoption of pre-ledger databases, event preservation, idempotency, future-version rejection, and downgrade rejection.
