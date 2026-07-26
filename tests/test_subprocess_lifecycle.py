from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Callable, Iterator

from signalkit_stream.storage import SQLiteSignalStore


@dataclass
class ServerState:
    items: list[str]
    block_first_webhook: bool = False
    webhook_calls: list[dict[str, object]] = field(default_factory=list)
    first_webhook_received: threading.Event = field(default_factory=threading.Event)
    release_first_webhook: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)


def _rss(items: list[str]) -> bytes:
    entries = "".join(
        f"""
        <item>
          <guid>{item}</guid>
          <title>{item}</title>
          <link>https://example.com/{item}</link>
          <description>Body for {item}</description>
          <pubDate>Sun, 26 Jul 2026 05:00:00 GMT</pubDate>
        </item>
        """
        for item in items
    )
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<rss version=\"2.0\"><channel>"
        "<title>Lifecycle Feed</title><link>https://example.com/</link>"
        f"{entries}</channel></rss>"
    ).encode()


@contextmanager
def _server(*, items: list[str], block_first_webhook: bool = False) -> Iterator[tuple[ServerState, str]]:
    state = ServerState(list(items), block_first_webhook=block_first_webhook)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if self.path != "/feed.xml":
                self.send_error(404)
                return
            with state.lock:
                payload = _rss(list(state.items))
            self.send_response(200)
            self.send_header("Content-Type", "application/rss+xml")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if self.path != "/sink":
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            call = {
                "event_id": self.headers.get("X-SignalKit-Event-ID"),
                "idempotency_key": self.headers.get("Idempotency-Key"),
                "payload": payload,
            }
            with state.lock:
                state.webhook_calls.append(call)
                call_number = len(state.webhook_calls)
            if state.block_first_webhook and call_number == 1:
                state.first_webhook_received.set()
                state.release_first_webhook.wait(timeout=15)
            try:
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, format: str, *args: object) -> None:
            return None

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = httpd.server_address
        yield state, f"http://{host}:{port}"
    finally:
        state.release_first_webhook.set()
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _write_config(path: Path, *, database: Path, base_url: str, webhook: bool) -> None:
    sink = (
        f'''\n[[sinks]]
name = "brain"
type = "webhook"
url = "{base_url}/sink"
timeout = 10
'''
        if webhook
        else ""
    )
    path.write_text(
        f'''[runtime]
database = {json.dumps(str(database))}
concurrency = 1
failure_threshold = 3
circuit_cooldown = 1
failure_backoff_base = 0.05
delivery_interval = 0.05
delivery_batch = 10
delivery_max_attempts = 3
delivery_backoff_base = 0.05
delivery_backoff_max = 1

[[sources]]
name = "lifecycle-feed"
type = "rss"
interval = 60
limit = 10
url = "{base_url}/feed.xml"
source = "rss"
instance = "lifecycle"
{sink}''',
        encoding="utf-8",
    )


def _command(config: Path, *extra: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "signalkit_stream.cli",
        "run",
        str(config),
        *extra,
    ]


def _start(config: Path) -> subprocess.Popen[str]:
    kwargs: dict[str, object] = {}
    if sys.platform == "win32":
        # GenerateConsoleCtrlEvent can only target a process group, so the child needs
        # its own group before a graceful CTRL_BREAK_EVENT can reach it.
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(
        _command(config),
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **kwargs,  # type: ignore[arg-type]
    )


def _request_graceful_stop(process: subprocess.Popen[str]) -> None:
    """Ask the runtime to shut down gracefully in a platform-portable way.

    ``Popen.terminate()`` on Windows is ``TerminateProcess``, which no handler can
    intercept, so a graceful stop must be requested with ``CTRL_BREAK_EVENT``.
    """

    if sys.platform == "win32":
        os.kill(process.pid, signal.CTRL_BREAK_EVENT)
    else:
        process.terminate()


def _wait_for(
    predicate: Callable[[], bool],
    *,
    process: subprocess.Popen[str] | None = None,
    timeout: float = 12.0,
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"runtime exited early with code {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        try:
            if predicate():
                return
        except (OSError, sqlite3.Error) as exc:
            last_error = exc
        time.sleep(0.05)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise AssertionError(f"timed out waiting for lifecycle condition{suffix}")


def _committed_source_cycle(database: Path) -> bool:
    if not database.exists():
        return False
    connection = sqlite3.connect(database, timeout=0.1)
    try:
        signal_count = int(connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0])
        checkpoint_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE source_key = 'rss:lifecycle'"
            ).fetchone()[0]
        )
        health = connection.execute(
            "SELECT total_runs, total_events FROM source_health WHERE source_key = 'rss:lifecycle'"
        ).fetchone()
    finally:
        connection.close()
    return bool(signal_count >= 1 and checkpoint_count == 1 and health and health[0] >= 1)


def _pending_delivery(database: Path) -> bool:
    if not database.exists():
        return False
    connection = sqlite3.connect(database, timeout=0.1)
    try:
        row = connection.execute(
            "SELECT status, attempts FROM deliveries WHERE sink_key = 'brain' LIMIT 1"
        ).fetchone()
    finally:
        connection.close()
    return row == ("pending", 0)


def test_graceful_stop_persists_progress_and_restart_collects_new_item(tmp_path) -> None:
    database = tmp_path / "signals.db"
    config = tmp_path / "signalkit.toml"

    with _server(items=["item-1"]) as (state, base_url):
        _write_config(config, database=database, base_url=base_url, webhook=False)
        process = _start(config)
        try:
            _wait_for(lambda: _committed_source_cycle(database), process=process)
            _request_graceful_stop(process)
            stdout, stderr = process.communicate(timeout=10)
            assert process.returncode == 0, f"stdout:\n{stdout}\nstderr:\n{stderr}"
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)

        with SQLiteSignalStore(database) as store:
            assert store.count() == 1
            checkpoint = store.get_checkpoint("rss:lifecycle")
            health = store.get_source_health("rss:lifecycle")
            assert checkpoint is not None
            assert health is not None
            first_runs = health.total_runs

        with state.lock:
            state.items[:] = ["item-2", "item-1"]

        completed = subprocess.run(
            _command(config, "--once"),
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr

    with SQLiteSignalStore(database) as store:
        assert store.count() == 2
        titles = {event.title for event in store.list_recent(limit=10)}
        assert titles == {"item-1", "item-2"}
        health = store.get_source_health("rss:lifecycle")
        assert health is not None
        assert health.total_runs >= first_runs + 1
        assert store.get_checkpoint("rss:lifecycle") is not None


def test_process_kill_during_webhook_replays_same_version_after_restart(tmp_path) -> None:
    database = tmp_path / "signals.db"
    config = tmp_path / "signalkit.toml"

    with _server(items=["item-1"], block_first_webhook=True) as (state, base_url):
        _write_config(config, database=database, base_url=base_url, webhook=True)
        process = _start(config)
        try:
            _wait_for(state.first_webhook_received.is_set, process=process)
            _wait_for(lambda: _pending_delivery(database), process=process)

            process.kill()
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode != 0, f"stdout:\n{stdout}\nstderr:\n{stderr}"
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
            state.release_first_webhook.set()

        with SQLiteSignalStore(database) as store:
            events = store.list_recent(limit=10)
            assert len(events) == 1
            event_id = events[0].id
            delivery = store.get_delivery("brain", event_id)
            assert delivery is not None
            assert delivery.status == "pending"
            assert delivery.attempts == 0
            assert store.get_checkpoint("rss:lifecycle") is not None

        completed = subprocess.run(
            _command(config, "--once"),
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr

        _wait_for(lambda: len(state.webhook_calls) >= 2)
        with state.lock:
            calls = list(state.webhook_calls)

    assert len(calls) == 2
    assert calls[0]["event_id"] == calls[1]["event_id"] == event_id
    assert calls[0]["idempotency_key"] == calls[1]["idempotency_key"]

    with SQLiteSignalStore(database) as store:
        delivery = store.get_delivery("brain", event_id)
        assert delivery is not None
        assert delivery.status == "delivered"
        assert delivery.attempts == 1
        assert store.count() == 1
