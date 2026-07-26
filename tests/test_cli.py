import asyncio
from datetime import UTC, datetime
import json
import signal

import pytest

from signalkit_stream.cli import (
    _reddit_credentials,
    _shutdown_signals,
    _stop_signals,
    build_parser,
    main,
)
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import Cursor
from signalkit_stream.storage import SQLiteSignalStore, SourceHealth


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


def test_init_and_status_commands(tmp_path, capsys) -> None:
    config_path = tmp_path / "signalkit.toml"
    database = tmp_path / "signals.db"

    assert main(["init", str(config_path)]) == 0
    assert "hackernews-new" in config_path.read_text(encoding="utf-8")
    assert main(["init", str(config_path)]) == 1
    capsys.readouterr()

    now = datetime(2026, 7, 25, tzinfo=UTC)
    with SQLiteSignalStore(database) as store:
        store.upsert_source_health(
            SourceHealth(
                source_key="hackernews:newstories",
                status="healthy",
                updated_at=now,
                last_attempt_at=now,
                last_success_at=now,
                total_runs=2,
                total_events=20,
            )
        )

    assert main(["status", "--db", str(database), "--format", "json"]) == 0
    output = capsys.readouterr().out
    assert '"source_key": "hackernews:newstories"' in output
    assert '"status": "healthy"' in output


def test_verbose_status_includes_schema_sinks_and_prometheus(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    now = datetime(2026, 7, 25, tzinfo=UTC)
    event = SignalEvent(
        id="sig_status_verbose",
        source="test",
        kind=SignalKind.POST,
        content="Body",
        url="https://example.com/status",
        created_at=now,
    )
    with SQLiteSignalStore(database) as store:
        store.upsert_source_health(
            SourceHealth(
                source_key="test:default",
                status="healthy",
                updated_at=now,
                last_attempt_at=now,
                last_success_at=now,
                total_runs=1,
                total_events=1,
            )
        )
        store.register_delivery_sink("brain")
        store.write_many([event])

    assert main(
        ["status", "--db", str(database), "--verbose", "--format", "json"]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_status"] == "current"
    assert payload["signals_total"] == 1
    assert payload["sources"][0]["source_key"] == "test:default"
    assert payload["sinks"][0]["sink_key"] == "brain"
    assert payload["sinks"][0]["pending"] == 1

    assert main(["status", "--db", str(database), "--format", "prometheus"]) == 0
    metrics = capsys.readouterr().out
    assert "signalkit_database_schema_current 1" in metrics
    assert 'signalkit_sink_enabled{sink="brain"} 1' in metrics


def test_delivery_status_and_dead_letter_replay_commands(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    event = SignalEvent(
        id="sig_delivery_cli",
        source="test",
        kind=SignalKind.POST,
        content="Body",
        url="https://example.com/delivery",
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("brain")
        store.write_many([event])
        store.mark_delivery_failure(
            "brain",
            event.id,
            error="permanent",
            next_attempt_at=None,
            dead=True,
            attempted_at=now,
        )

    assert main(
        ["deliveries", "--db", str(database), "--sink", "brain", "--format", "json"]
    ) == 0
    status_output = capsys.readouterr().out
    assert '"dead": 1' in status_output

    assert main(["retry-deliveries", "brain", "--db", str(database)]) == 0
    retry_output = capsys.readouterr().out
    assert "Queued 1 dead deliveries" in retry_output

    with SQLiteSignalStore(database) as store:
        assert store.delivery_counts("brain") == {"pending": 1}


def test_database_backup_and_verify_commands(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    backup = tmp_path / "backup.db"
    with SQLiteSignalStore(database):
        pass

    assert main(
        ["db", "backup", str(backup), "--db", str(database), "--format", "json"]
    ) == 0
    backup_payload = json.loads(capsys.readouterr().out)
    assert backup_payload["quick_check"] == "ok"
    assert backup_payload["destination"] == str(backup)

    assert main(["db", "verify", "--db", str(backup), "--format", "json"]) == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["ok"] is True
    assert verify_payload["schema_status"] == "current"


def test_reddit_and_jsonfeed_collectors_are_available_in_cli(monkeypatch) -> None:
    parser = build_parser()
    reddit = parser.parse_args(
        [
            "collect",
            "reddit",
            "SaaS",
            "--listing",
            "new",
            "--comments",
            "3",
            "--no-store",
        ]
    )
    jsonfeed = parser.parse_args(
        ["collect", "jsonfeed", "https://example.com/feed.json", "--no-store"]
    )

    assert reddit.collector == "reddit"
    assert reddit.subreddit == "SaaS"
    assert reddit.comments == 3
    assert reddit.access_token_env == "REDDIT_ACCESS_TOKEN"
    assert reddit.refresh_token_env == "REDDIT_REFRESH_TOKEN"
    assert jsonfeed.collector == "jsonfeed"

    for name in (
        "REDDIT_ACCESS_TOKEN",
        "REDDIT_REFRESH_TOKEN",
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit, match="REDDIT_CLIENT_ID"):
        main(["collect", "reddit", "SaaS", "--no-store"])


def test_reddit_cli_accepts_static_access_token_without_client_secret(monkeypatch) -> None:
    parser = build_parser()
    args = parser.parse_args(["collect", "reddit", "python", "--no-store"])
    monkeypatch.setenv("REDDIT_ACCESS_TOKEN", "static-token")
    monkeypatch.setenv("REDDIT_USER_AGENT", "signalkit-cli-test")
    monkeypatch.delenv("REDDIT_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)

    credentials = _reddit_credentials(args)

    assert credentials["access_token"] == "static-token"
    assert credentials["refresh_token"] is None
    assert credentials["client_id"] is None
    assert credentials["client_secret"] is None
    assert credentials["user_agent"] == "signalkit-cli-test"


def test_run_parser_supports_json_logging() -> None:
    args = build_parser().parse_args(["run", "signalkit.toml", "--log-format", "json"])

    assert args.log_format == "json"


def test_validate_and_doctor_commands(tmp_path, capsys) -> None:
    database = tmp_path / "signals.db"
    config = tmp_path / "signalkit.toml"
    config.write_text(
        f'''[runtime]
database = "{database.as_posix()}"

[[sources]]
name = "hn"
type = "hackernews"
feed = "newstories"
''',
        encoding="utf-8",
    )

    assert main(["validate", str(config), "--format", "json"]) == 0
    validate_output = capsys.readouterr().out
    assert '"ok": true' in validate_output
    assert '"source:hn"' in validate_output

    assert main(["doctor", str(config), "--format", "json"]) == 0
    doctor_output = capsys.readouterr().out
    assert '"database-path"' in doctor_output
    assert '"status": "warn"' in doctor_output

    with SQLiteSignalStore(database):
        pass
    assert main(["doctor", str(config)]) == 0
    table_output = capsys.readouterr().out
    assert "database-integrity" in table_output
    assert "PASS" in table_output


class _RecordingLoop:
    """Minimal event-loop stand-in for the shutdown-signal helpers."""

    def __init__(self, *, supports_add_signal_handler: bool) -> None:
        self.supports_add_signal_handler = supports_add_signal_handler
        self.added: list[int] = []
        self.removed: list[int] = []
        self.threadsafe_calls = 0

    def add_signal_handler(self, signum, callback):  # noqa: ANN001, ANN202
        if not self.supports_add_signal_handler:
            raise NotImplementedError
        self.added.append(signum)
        self._callback = callback

    def remove_signal_handler(self, signum):  # noqa: ANN001, ANN202
        self.removed.append(signum)
        return True

    def call_soon_threadsafe(self, callback, *args):  # noqa: ANN001, ANN202
        self.threadsafe_calls += 1
        callback(*args)


def test_shutdown_signals_cover_every_catchable_stop_signal() -> None:
    signals = _shutdown_signals()

    assert signal.SIGINT in signals
    assert signal.SIGTERM in signals
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is None:
        assert len(signals) == 2
    else:
        # Windows delivers CTRL_BREAK_EVENT as SIGBREAK.
        assert sigbreak in signals


def test_stop_signals_prefers_loop_handlers_and_removes_them() -> None:
    loop = _RecordingLoop(supports_add_signal_handler=True)
    stop = asyncio.Event()
    before = {signum: signal.getsignal(signum) for signum in _shutdown_signals()}

    with _stop_signals(loop, stop):
        assert loop.added == list(_shutdown_signals())
        for signum, handler in before.items():
            assert signal.getsignal(signum) is handler

    assert loop.removed == list(_shutdown_signals())
    for signum, handler in before.items():
        assert signal.getsignal(signum) is handler


def test_stop_signals_falls_back_to_signal_signal_and_wakes_loop_threadsafe() -> None:
    loop = _RecordingLoop(supports_add_signal_handler=False)
    stop = asyncio.Event()
    before = {signum: signal.getsignal(signum) for signum in _shutdown_signals()}

    with _stop_signals(loop, stop):
        assert loop.added == []
        installed = signal.getsignal(signal.SIGINT)
        assert callable(installed)
        assert installed is not before[signal.SIGINT]

        installed(signal.SIGINT, None)

        assert stop.is_set()
        assert loop.threadsafe_calls == 1
        # A second Ctrl+C must still force-quit, so the prior disposition is restored
        # as soon as the first one is handled.
        assert signal.getsignal(signal.SIGINT) is before[signal.SIGINT]

    for signum, handler in before.items():
        assert signal.getsignal(signum) is handler
