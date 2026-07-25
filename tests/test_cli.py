from datetime import UTC, datetime

from signalkit_stream.cli import main
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore


def test_show_command_outputs_saved_event(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    event = SignalEvent(
        id="sig_cli",
        source="rss",
        kind=SignalKind.ARTICLE,
        title="Hello signal",
        content="Body",
        url="https://example.com",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    with SQLiteSignalStore(database) as store:
        store.save_many([event])

    assert main(["show", "--db", str(database), "--format", "jsonl"]) == 0
    output = capsys.readouterr().out
    assert "Hello signal" in output
    assert '"source": "rss"' in output
