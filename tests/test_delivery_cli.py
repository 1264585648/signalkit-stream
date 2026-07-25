from datetime import UTC, datetime

from signalkit_stream.cli import main
from signalkit_stream.delivery import DeliveryCandidate, SQLiteDeliveryStore
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def make_event() -> SignalEvent:
    return SignalEvent(
        id="sig_delivery_cli",
        source="test",
        source_instance="one",
        kind=SignalKind.POST,
        content="deliver me",
        url="https://example.com/deliver",
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


def test_delivery_run_status_and_replay_commands(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    config = tmp_path / "signalkit.toml"
    config.write_text(
        f'''[runtime]
database = "{database.as_posix()}"

[[sources]]
name = "unused-source"
type = "hackernews"
enabled = true

[[sinks]]
name = "console"
type = "stdout"
''',
        encoding="utf-8",
    )
    item = make_event()
    with SQLiteSignalStore(database) as store:
        store.write_many([item])

    assert main(["delivery", "run", str(config), "--once"]) == 0
    output = capsys.readouterr().out
    assert '"id": "sig_delivery_cli"' in output
    assert '"sink": "console"' in output

    assert main(["delivery", "status", "--db", str(database), "--format", "json"]) == 0
    status_output = capsys.readouterr().out
    assert '"status": "delivered"' in status_output

    with SQLiteDeliveryStore(database) as store:
        current = store.get_record("console", item.id)
        assert current is not None
        store.mark_failed(
            "console",
            DeliveryCandidate(item, item.fingerprint(), current.attempts),
            "manual failure",
            dead_letter=True,
        )

    assert main(["delivery", "replay", "console", "--db", str(database)]) == 0
    replay_output = capsys.readouterr().out
    assert "Requeued 1" in replay_output
