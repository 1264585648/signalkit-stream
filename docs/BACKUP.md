# SQLite backup and verification

SignalKit Stream includes a small dependency-free backup utility built on SQLite's backup API. Use it instead of copying an actively written SQLite database file without coordinating WAL state.

## Create a backup

```bash
python -m signalkit_stream.maintenance backup signals.db backups/signals-2026-07-25.db
```

JSON output:

```bash
python -m signalkit_stream.maintenance backup signals.db backup.db --format json
```

The command:

1. opens the source database read-only
2. uses SQLite's consistent backup API to copy into the destination
3. commits the destination
4. runs `PRAGMA quick_check` on the backup
5. reports the copied page count

Existing destinations are refused by default. Use `--overwrite` only when replacement is intentional:

```bash
python -m signalkit_stream.maintenance backup signals.db backup.db --overwrite
```

The source and destination must be different paths.

## Verify a database

```bash
python -m signalkit_stream.maintenance verify signals.db
```

This runs `PRAGMA quick_check` in read-only mode and reports page count/page size. It does not modify the database.

## Upgrade backups

Before a package upgrade that includes persistent schema migrations:

```bash
python -m signalkit_stream.maintenance verify signals.db
python -m signalkit_stream.maintenance backup signals.db backups/pre-upgrade.db
python -m signalkit_stream.migrations status signals.db
python -m signalkit_stream.migrations migrate signals.db
python -m signalkit_stream.diagnostics signalkit.toml
```

For the cleanest upgrade boundary, stop the writer process before backup/migration. The backup API can create a consistent copy while the source is online, but schema migration itself should run with competing writers stopped.

## Restore

Stop SignalKit Stream before replacing its database. Verify the backup first, move the current database aside rather than deleting it immediately, copy the verified backup into the configured database path, then run diagnostics/schema status before restarting.

Backups contain normalized public-source content, author identifiers, URLs, metadata, checkpoints, health state, and delivery state. Protect and retain them according to the same policy as the production database.
