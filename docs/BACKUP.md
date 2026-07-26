# SQLite backup and verification

SignalKit Stream includes dependency-free maintenance tooling built on SQLite's backup API.
Use it instead of raw-copying a database that may be actively written or using WAL files.

## Create a backup

```bash
python -m signalkit_stream.maintenance backup signals.db backups/signals-2026-07-26.db
```

JSON output:

```bash
python -m signalkit_stream.maintenance backup signals.db backup.db --format json
```

The backup path must have an existing parent directory and must differ from the source.
Existing destinations are refused unless `--overwrite` is explicitly supplied.

The implementation deliberately does not write directly into the final destination. It:

1. opens the source SQLite database read-only;
2. creates a temporary file beside the requested backup path;
3. copies a consistent snapshot with SQLite's backup API;
4. commits the temporary database;
5. runs `PRAGMA quick_check` on that copy;
6. records its persistent SignalKit schema version;
7. atomically renames the verified temporary copy to the requested destination.

With `--overwrite`, an existing backup remains untouched until the replacement has passed the
integrity check. A failed replacement therefore does not destroy the last known backup.

```bash
python -m signalkit_stream.maintenance backup signals.db backup.db --overwrite
```

### Concurrent readers can delay publication on Windows

Step 7 renames the verified temporary copy over the destination. POSIX allows that while
another process still has the destination open; Windows refuses it with
`PermissionError: [WinError 5]` until every other handle on the destination is closed.
Anything holding the previous backup open can therefore block publication: another reader,
`signalkit db verify` on the backup file, a running SQLite connection, a file-sync client,
or an antivirus scanner mid-scan.

Publication retries a few times with a short delay, which absorbs a brief scan or read. If
the destination is still held after those attempts, the backup fails with a non-zero exit
code, the previous backup is left untouched, and the temporary copy is removed. Nothing is
corrupted - retry the backup once the other reader has finished, or write the new backup to
a path nothing else holds open.

## Verify a database

```bash
python -m signalkit_stream.maintenance verify signals.db
```

Verification is read-only. It reports:

- SQLite `PRAGMA quick_check`;
- page count and page size;
- database `PRAGMA user_version`;
- the schema version supported by the running SignalKit Stream build;
- schema compatibility status.

The possible schema states are:

```text
current             integrity is OK and the persistent Stream schema is current
migration_required  database is older/unversioned; startup will run forward migration
future              database was written by a newer Stream build; startup will refuse it
invalid             database claims the current version but required objects are missing
```

The command exits non-zero unless both SQLite integrity and Stream schema status are current.
Use JSON output for scripts:

```bash
python -m signalkit_stream.maintenance verify signals.db --format json
```

## Upgrade runbook

For a deployment upgrade:

```bash
python -m signalkit_stream.maintenance verify signals.db
python -m signalkit_stream.maintenance backup signals.db backups/pre-upgrade.db
signalkit doctor signalkit.toml
# install/deploy the new SignalKit Stream build
# start Stream; forward database migrations run before collectors/sinks start
signalkit doctor signalkit.toml
python -m signalkit_stream.maintenance verify signals.db
```

For the cleanest upgrade boundary, stop the Stream writer before switching application
versions. The backup API itself can produce a consistent snapshot from an online SQLite
database, but application-version replacement and forward migration should not race with an
older writer process.

See `docs/MIGRATIONS.md` for the persistent-schema compatibility and rollback policy.

## Restore

1. Stop SignalKit Stream and every process that writes to the configured SQLite database.
2. Verify the candidate backup with the new or intended application build.
3. Move the current database aside instead of deleting it immediately.
4. Copy the verified backup to the configured database path.
5. Run `signalkit doctor` and the maintenance `verify` command.
6. Start SignalKit Stream. If the backup is older than the current persistent schema version,
   the normal startup migration will upgrade it atomically.
7. Re-run diagnostics before deleting any preserved pre-restore database.

A backup contains normalized source content, author identifiers, URLs, metadata, checkpoints,
source health, sink registration, and durable delivery state. Protect and retain it according
to the same policy as the production database.
