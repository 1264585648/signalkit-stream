from datetime import UTC, datetime
import json
import sqlite3

from signalkit_stream.migrations import DATABASE_SCHEMA_VERSION
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.observability import format_snapshot, main, read_snapshot
from signalkit_stream.storage import SQLiteSignalStore, SourceHealth


def seed_database(path) -> None:
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    item = SignalEvent(
        id="sig_metrics",
        source="test",
        source_instance="source",
        kind=SignalKind.POST,
        content="Body",
        url="https://example.com/metrics",
        created_at=now,
    )
    with SQLiteSignalStore(path) as store:
        store.upsert_source_health(
            SourceHealth(
                source_key="test:source",
                status="degraded",
                updated_at=now,
                last_attempt_at=now,
                last_success_at=now,
                last_error="temporary source error",
                consecutive_failures=2,
                total_runs=7,
                total_events=42,
            )
        )
        store.register_delivery_sink("archive")
        store.register_delivery_sink('brain"prod')
        store.register_delivery_sink("empty")
        store.write_many([item])

    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            UPDATE deliveries
            SET status = 'delivered', attempts = 1, updated_at = ?
            WHERE sink_key = 'archive'
            """,
            (now.isoformat(),),
        )
        connection.execute(
            """
            UPDATE deliveries
            SET status = 'dead', attempts = 3, last_error = 'webhook 400', updated_at = ?
            WHERE sink_key = ?
            """,
            (now.isoformat(), 'brain"prod'),
        )
        connection.execute("DELETE FROM deliveries WHERE sink_key = 'empty'")
        connection.commit()
    finally:
        connection.close()


def test_snapshot_reads_schema_source_and_sink_health(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed_database(database)

    snapshot = read_snapshot(database)

    assert snapshot.signals_total == 1
    assert snapshot.schema_version == DATABASE_SCHEMA_VERSION
    assert snapshot.supported_schema_version == DATABASE_SCHEMA_VERSION
    assert snapshot.schema_status == "current"

    assert len(snapshot.sources) == 1
    source = snapshot.sources[0]
    assert source.source_key == "test:source"
    assert source.status == "degraded"
    assert source.consecutive_failures == 2
    assert source.total_runs == 7
    assert source.total_events == 42
    assert source.last_attempt_at is not None
    assert source.last_error == "temporary source error"

    sinks = {sink.sink_key: sink for sink in snapshot.sinks}
    assert sinks["archive"].delivered == 1
    assert sinks["archive"].attempts == 1
    assert sinks['brain"prod'].dead == 1
    assert sinks['brain"prod'].attempts == 3
    assert sinks['brain"prod'].last_error == "webhook 400"
    assert sinks["empty"].enabled is True
    assert sinks["empty"].pending == 0
    assert sinks["empty"].attempts == 0


def test_snapshot_json_and_prometheus_formats(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed_database(database)
    snapshot = read_snapshot(database)

    payload = json.loads(format_snapshot(snapshot, output_format="json"))
    assert payload["signals_total"] == 1
    assert payload["schema_status"] == "current"
    assert payload["sources"][0]["status"] == "degraded"

    metrics = format_snapshot(snapshot, output_format="prometheus")
    assert "signalkit_signals_total 1" in metrics
    assert f"signalkit_database_schema_version {DATABASE_SCHEMA_VERSION}" in metrics
    assert "signalkit_database_schema_current 1" in metrics
    assert 'signalkit_source_status{source="test:source",status="degraded"} 1' in metrics
    assert 'signalkit_source_consecutive_failures{source="test:source"} 2' in metrics
    assert 'signalkit_delivery_attempts_total{sink="archive"} 1' in metrics
    assert 'sink="brain\\"prod"' in metrics
    assert 'signalkit_sink_enabled{sink="empty"} 1' in metrics


def test_snapshot_table_includes_last_errors_and_schema(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed_database(database)

    text = format_snapshot(read_snapshot(database), output_format="table")

    assert "schema=" in text
    assert "current" in text
    assert "test:source" in text
    assert "degraded" in text
    assert "temporary source error" in text
    assert 'brain"prod' in text
    assert "webhook 400" in text


def test_snapshot_reports_noncurrent_schema_and_cli_exit_code(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    seed_database(database)
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA user_version = 0")
    connection.commit()
    connection.close()

    snapshot = read_snapshot(database)
    assert snapshot.schema_status == "migration_required"

    assert main([str(database), "--format", "json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_status"] == "migration_required"


def test_prometheus_label_escaping(tmp_path) -> None:
    database = tmp_path / "signals.db"
    seed_database(database)

    metrics = format_snapshot(read_snapshot(database), output_format="prometheus")

    assert 'sink="brain\\"prod"' in metrics
