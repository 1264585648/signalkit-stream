# Database migrations and recovery

SignalKit Stream stores its persistent schema version in SQLite `PRAGMA user_version`.
The current database schema version is `1`.

This version is separate from the `SignalEvent.schema_version`: the event schema describes
normalized event payloads, while the database schema version describes tables, indexes,
triggers, and other persistent storage objects.

## Startup behavior

Opening `SQLiteSignalStore` performs a local schema compatibility check before the store is
used by collectors or delivery workers.

```text
database version < supported version
        -> run forward migrations atomically

database version == supported version
        -> validate required tables / columns / indexes / triggers

database version > supported version
        -> refuse startup without modifying the database
```

SignalKit Stream never attempts a downgrade. A database written by a newer release must be
opened with that release or a later compatible release.

## Historical databases

Releases before persistent schema versioning used `PRAGMA user_version = 0`. Migration 1 is
the compatibility boundary for those databases. It recognizes the known SignalKit Stream
`signals` table shape, adds columns introduced by the resumable stream core when necessary,
creates runtime health and durable delivery objects when they are absent, and stamps version
`1` only after the transaction commits.

An unrelated or malformed database that merely contains a table named `signals` is rejected
instead of being guessed into a Stream schema.

## Atomicity

Each migration runs inside an SQLite `BEGIN IMMEDIATE` transaction. The schema version is
updated in the same transaction as the schema changes. If a migration fails, the transaction
is rolled back and the previous `user_version` remains unchanged.

This means the durable boundary is:

```text
old schema + old user_version
              OR
new schema + new user_version
```

There should never be a successful startup with a partially applied migration.

## Before upgrading

For a production database, stop the Stream process before replacing the application version.
Do not run two Stream versions against the same SQLite file during an upgrade.

Create a backup while the database is not being written. A simple file copy is sufficient
after the process has stopped. For an online backup, use SQLite's backup API or the SQLite
CLI `.backup` command rather than copying a file while writes are active.

Keep the backup until the upgraded process has started successfully and `signalkit doctor`
reports the expected database schema version.

## Upgrade procedure

1. Stop SignalKit Stream and downstream processes that write to its SQLite database.
2. Back up the database file.
3. Upgrade the SignalKit Stream package or deployment image.
4. Run `signalkit doctor <config>` to inspect configuration and the pre-start schema state.
5. Start SignalKit Stream. Forward migrations run automatically before collection begins.
6. Run `signalkit doctor <config>` again and verify the database schema is current.
7. Keep the backup through an appropriate observation window for the deployment.

`doctor` is read-only with respect to the database. It reports an older/unversioned database
as requiring forward migration and reports a future database version as a failure.

## Recovery

If startup fails during a migration, do not manually change `PRAGMA user_version` to bypass
the error. The version is a compatibility guard, not a cosmetic marker.

Instead:

1. preserve the failed database for diagnosis;
2. inspect the exception and `signalkit doctor` output;
3. restore the pre-upgrade backup if service must be recovered immediately;
4. fix the migration/application incompatibility;
5. retry the upgrade from the preserved backup or a verified copy.

If a database claims the current schema version but required Stream objects are missing,
`SQLiteSignalStore` refuses startup. Treat that as corruption or an unsupported manual schema
change and recover from a known-good backup rather than silently recreating objects.

## Adding a future migration

Persistent schema changes must add the next integer migration to the registry in
`signalkit_stream.migrations` and include tests for:

- a fresh database at the new version;
- every supported older database shape upgrading to the new version;
- data/checkpoint/health/delivery preservation;
- rollback when the migration fails;
- rejection of `user_version` values newer than the running code supports;
- `doctor` reporting before and after migration.

Normal PR CI must remain deterministic and offline. Migration tests use temporary SQLite
files and must not depend on third-party services.
