from datetime import UTC, datetime

from signalkit_stream.cli import main
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import Cursor
from signalkit_stream.storage import SQLiteSignalStore


def test_show_and_checkpoint_commands(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    event = SignalEvent(
        id="sig_cli",
        source="rss",
        source_instance="feed",
        kind=SignalKind.ARTICLE,
        title="Hello signal",
        content="Body",
        url="https://example.com",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    with SQLiteSignalStore(database) as store:
        store.save_many([event])
        store.set_checkpoint("rss:feed", Cursor("rss:feed", {"offset": 0}))

    assert main(["show", "--db", str(database), "--format", "jsonl"]) == 0
    output = capsys.readouterr().out
    assert "Hello signal" in output
    assert '"source": "rss"' in output

    assert main(["checkpoint", "rss:feed", "--db", str(database)]) == 0
    checkpoint_output = capsys.readouterr().out
    assert '"source_key": "rss:feed"' in checkpoint_output
