from __future__ import annotations

from collections.abc import Callable
import sqlite3

DATABASE_SCHEMA_VERSION = 1

REQUIRED_TABLES = frozenset(
    {
        "signals",
        "checkpoints",
        "source_health",
        "delivery_sinks",
        "deliveries",
    }
)
REQUIRED_INDEXES = frozenset(
    {
        "idx_signals_source_created",
        "idx_signals_kind_created",
        "idx_deliveries_ready",
    }
)
REQUIRED_TRIGGERS = frozenset(
    {
        "trg_signals_delivery_insert",
        "trg_signals_delivery_update",
    }
)

_REQUIRED_COLUMNS = {
    "signals": frozenset(
        {
            "id",
            "schema_version",
            "source",
            "source_instance",
            "kind",
            "title",
            "content",
            "author",
            "url",
            "created_at",
            "updated_at",
            "collected_at",
            "metadata_json",
            "event_hash",
        }
    ),
    "checkpoints": frozenset(
        {
            "source_key",
            "cursor_json",
            "updated_at",
            "last_success_at",
            "last_error",
        }
    ),
    "source_health": frozenset(
        {
            "source_key",
            "status",
            "updated_at",
            "last_attempt_at",
            "last_success_at",
            "last_error",
            "consecutive_failures",
            "total_runs",
            "total_events",
        }
    ),
    "delivery_sinks": frozenset({"sink_key", "enabled", "created_at", "updated_at"}),
    "deliveries": frozenset(
        {
            "sink_key",
            "event_id",
            "status",
            "attempts",
            "next_attempt_at",
            "last_error",
            "delivered_at",
            "updated_at",
        }
    ),
}

_LEGACY_SIGNAL_CORE = frozenset(
    {
        "id",
        "source",
        "kind",
        "content",
        "url",
        "created_at",
        "collected_at",
        "metadata_json",
    }
)


class DatabaseSchemaError(RuntimeError):
    """Base error for incompatible or failed persistent-schema operations."""


class DatabaseSchemaTooNew(DatabaseSchemaError):
    """Raised when a database was created by a newer SignalKit Stream release."""


class DatabaseMigrationError(DatabaseSchemaError):
    """Raised when a forward database migration cannot complete atomically."""


Migration = Callable[[sqlite3.Connection], None]


def get_database_schema_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("PRAGMA user_version").fetchone()
    return int(row[0]) if row is not None else 0


def migrate_database(connection: sqlite3.Connection) -> int:
    """Upgrade an SQLite database to the current schema version atomically.

    Historical SignalKit Stream databases were unversioned and therefore report
    ``PRAGMA user_version = 0``. Migration 1 recognizes the known legacy/current
    table shapes, adds the missing Stream objects, and stamps the database only
    after the migration transaction succeeds.
    """

    current = get_database_schema_version(connection)
    if current > DATABASE_SCHEMA_VERSION:
        raise DatabaseSchemaTooNew(
            "database schema version "
            f"{current} is newer than supported version {DATABASE_SCHEMA_VERSION}; "
            "upgrade SignalKit Stream before opening this database"
        )

    while current < DATABASE_SCHEMA_VERSION:
        target = current + 1
        migration = _MIGRATIONS.get(target)
        if migration is None:
            raise DatabaseMigrationError(
                f"no migration path from database schema version {current} to {target}"
            )
        try:
            connection.execute("BEGIN IMMEDIATE")
            migration(connection)
            connection.execute(f"PRAGMA user_version = {target}")
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, DatabaseSchemaError):
                raise
            raise DatabaseMigrationError(
                f"failed to migrate database schema from version {current} to {target}: {exc}"
            ) from exc
        current = target

    validate_database_schema(connection)
    return current


def validate_database_schema(connection: sqlite3.Connection) -> None:
    version = get_database_schema_version(connection)
    if version > DATABASE_SCHEMA_VERSION:
        raise DatabaseSchemaTooNew(
            f"database schema version {version} is newer than supported version "
            f"{DATABASE_SCHEMA_VERSION}"
        )
    if version != DATABASE_SCHEMA_VERSION:
        raise DatabaseSchemaError(
            f"database schema version {version} is not current version {DATABASE_SCHEMA_VERSION}"
        )

    tables = _objects(connection, "table")
    missing_tables = sorted(REQUIRED_TABLES - tables)
    if missing_tables:
        raise DatabaseSchemaError(
            "database schema is incomplete; missing tables: " + ", ".join(missing_tables)
        )

    for table, required in _REQUIRED_COLUMNS.items():
        columns = _columns(connection, table)
        missing = sorted(required - columns)
        if missing:
            raise DatabaseSchemaError(
                f"database table {table!r} is missing columns: {', '.join(missing)}"
            )

    indexes = _objects(connection, "index")
    missing_indexes = sorted(REQUIRED_INDEXES - indexes)
    if missing_indexes:
        raise DatabaseSchemaError(
            "database schema is incomplete; missing indexes: " + ", ".join(missing_indexes)
        )

    triggers = _objects(connection, "trigger")
    missing_triggers = sorted(REQUIRED_TRIGGERS - triggers)
    if missing_triggers:
        raise DatabaseSchemaError(
            "database schema is incomplete; missing triggers: " + ", ".join(missing_triggers)
        )


def _migrate_to_v1(connection: sqlite3.Connection) -> None:
    existing_tables = _objects(connection, "table")
    if "signals" in existing_tables:
        columns = _columns(connection, "signals")
        if not _LEGACY_SIGNAL_CORE <= columns:
            missing = sorted(_LEGACY_SIGNAL_CORE - columns)
            raise DatabaseMigrationError(
                "existing 'signals' table does not match a known SignalKit Stream schema; "
                f"missing core columns: {', '.join(missing)}"
            )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL DEFAULT 1,
            source TEXT NOT NULL,
            source_instance TEXT NOT NULL DEFAULT 'default',
            kind TEXT NOT NULL,
            title TEXT,
            content TEXT NOT NULL,
            author TEXT,
            url TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            collected_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            event_hash TEXT NOT NULL DEFAULT ''
        )
        """
    )

    signal_columns = _columns(connection, "signals")
    additions = {
        "schema_version": (
            "ALTER TABLE signals ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
        ),
        "source_instance": (
            "ALTER TABLE signals ADD COLUMN source_instance TEXT NOT NULL DEFAULT 'default'"
        ),
        "updated_at": "ALTER TABLE signals ADD COLUMN updated_at TEXT",
        "event_hash": "ALTER TABLE signals ADD COLUMN event_hash TEXT NOT NULL DEFAULT ''",
    }
    for column, statement in additions.items():
        if column not in signal_columns:
            connection.execute(statement)

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS checkpoints (
            source_key TEXT PRIMARY KEY,
            cursor_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_success_at TEXT,
            last_error TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS source_health (
            source_key TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_attempt_at TEXT,
            last_success_at TEXT,
            last_error TEXT,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            total_runs INTEGER NOT NULL DEFAULT 0,
            total_events INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_sinks (
            sink_key TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS deliveries (
            sink_key TEXT NOT NULL,
            event_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            last_error TEXT,
            delivered_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (sink_key, event_id)
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signals_source_created
        ON signals(source, source_instance, created_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signals_kind_created
        ON signals(kind, created_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deliveries_ready
        ON deliveries(sink_key, status, next_attempt_at, updated_at)
        """
    )

    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_signals_delivery_insert
        AFTER INSERT ON signals
        BEGIN
            INSERT INTO deliveries (
                sink_key, event_id, status, attempts, next_attempt_at,
                last_error, delivered_at, updated_at
            )
            SELECT sink_key, NEW.id, 'pending', 0, NULL, NULL, NULL, NEW.collected_at
            FROM delivery_sinks
            WHERE enabled = 1
            ON CONFLICT(sink_key, event_id) DO UPDATE SET
                status = 'pending',
                attempts = 0,
                next_attempt_at = NULL,
                last_error = NULL,
                delivered_at = NULL,
                updated_at = excluded.updated_at;
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_signals_delivery_update
        AFTER UPDATE OF event_hash ON signals
        WHEN OLD.event_hash != NEW.event_hash
        BEGIN
            INSERT INTO deliveries (
                sink_key, event_id, status, attempts, next_attempt_at,
                last_error, delivered_at, updated_at
            )
            SELECT sink_key, NEW.id, 'pending', 0, NULL, NULL, NULL, NEW.collected_at
            FROM delivery_sinks
            WHERE enabled = 1
            ON CONFLICT(sink_key, event_id) DO UPDATE SET
                status = 'pending',
                attempts = 0,
                next_attempt_at = NULL,
                last_error = NULL,
                delivered_at = NULL,
                updated_at = excluded.updated_at;
        END
        """
    )

    _validate_v1_shape(connection)


def _validate_v1_shape(connection: sqlite3.Connection) -> None:
    tables = _objects(connection, "table")
    missing_tables = sorted(REQUIRED_TABLES - tables)
    if missing_tables:
        raise DatabaseMigrationError(
            "migration produced an incomplete schema; missing tables: "
            + ", ".join(missing_tables)
        )

    for table, required in _REQUIRED_COLUMNS.items():
        missing = sorted(required - _columns(connection, table))
        if missing:
            raise DatabaseMigrationError(
                f"migration cannot upgrade table {table!r}; missing columns: {', '.join(missing)}"
            )


def _objects(connection: sqlite3.Connection, object_type: str) -> frozenset[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = ?",
        (object_type,),
    ).fetchall()
    return frozenset(str(row[0]) for row in rows)


def _columns(connection: sqlite3.Connection, table: str) -> frozenset[str]:
    rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
    return frozenset(str(row[1]) for row in rows)


_MIGRATIONS: dict[int, Migration] = {1: _migrate_to_v1}
