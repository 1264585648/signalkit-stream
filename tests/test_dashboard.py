from __future__ import annotations

from datetime import UTC, datetime
from http.client import HTTPResponse
import json
from pathlib import Path
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from signalkit_stream.dashboard import DashboardRepository, create_dashboard_server
from signalkit_stream.dashboard_cli import build_parser as build_dashboard_parser
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.storage import SQLiteSignalStore, SourceHealth


def _event(event_id: str, *, title: str, source: str = "test") -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source=source,
        source_instance="default",
        kind=SignalKind.POST,
        title=title,
        content=f"Body for {title}",
        author="alice",
        url=f"https://example.com/{event_id}",
        created_at=datetime(2026, 8, 1, 8, 0, tzinfo=UTC),
    )


def _start_server(database: Path, *, allow_actions: bool = False):
    server = create_dashboard_server(database, port=0, allow_actions=allow_actions)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, thread, f"http://{host}:{port}"


def _json(response: HTTPResponse) -> dict:
    return json.loads(response.read().decode("utf-8"))


def test_dashboard_repository_filters_literal_wildcards_and_returns_details(tmp_path) -> None:
    database = tmp_path / "signals.db"
    with SQLiteSignalStore(database) as store:
        store.write_many(
            [
                _event("sig_percent", title="100% coverage"),
                _event("sig_plain", title="100x coverage", source="other"),
            ]
        )

    repository = DashboardRepository(database)
    result = repository.events(
        limit=20,
        offset=0,
        source=None,
        kind=None,
        query="%",
    )

    assert result["total"] == 1
    assert result["items"][0]["id"] == "sig_percent"
    assert result["items"][0]["content_truncated"] is False
    detail = repository.event("sig_percent")
    assert detail is not None
    assert detail["source_key"] == "test:default"
    assert detail["metadata"] == {}


def test_dashboard_serves_assets_overview_and_readonly_actions(tmp_path) -> None:
    database = tmp_path / "signals.db"
    now = datetime.now(UTC)
    event = _event("sig_console", title="Console signal")
    with SQLiteSignalStore(database) as store:
        store.upsert_source_health(
            SourceHealth(
                source_key="test:default",
                status="healthy",
                updated_at=now,
                last_attempt_at=now,
                last_success_at=now,
                total_runs=3,
                total_events=1,
            )
        )
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

    server, thread, base = _start_server(database)
    try:
        with urlopen(f"{base}/", timeout=3) as response:
            html = response.read().decode("utf-8")
            assert response.status == 200
            assert "SignalKit Operator Console" in html
            assert "default-src 'self'" in response.headers["Content-Security-Policy"]

        with urlopen(f"{base}/api/overview", timeout=3) as response:
            overview = _json(response)
            assert overview["snapshot"]["signals_total"] == 1
            assert overview["delivery_attention"] == 1
            assert overview["actions_enabled"] is False

        with urlopen(f"{base}/api/events?limit=10&q=Console", timeout=3) as response:
            events = _json(response)
            assert events["total"] == 1
            assert events["items"][0]["title"] == "Console signal"

        request = Request(
            f"{base}/api/sinks/brain/retry-dead",
            method="POST",
            headers={"X-SignalKit-Action": "retry-dead"},
        )
        with pytest.raises(HTTPError) as caught:
            urlopen(request, timeout=3)
        assert caught.value.code == 403
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_dashboard_retry_requires_header_and_requeues_dead_rows(tmp_path) -> None:
    database = tmp_path / "signals.db"
    event = _event("sig_retry", title="Retry me")
    with SQLiteSignalStore(database) as store:
        store.register_delivery_sink("brain")
        store.write_many([event])
        store.mark_delivery_failure(
            "brain",
            event.id,
            error="permanent",
            next_attempt_at=None,
            dead=True,
        )

    server, thread, base = _start_server(database, allow_actions=True)
    try:
        request = Request(f"{base}/api/sinks/brain/retry-dead", method="POST")
        with pytest.raises(HTTPError) as caught:
            urlopen(request, timeout=3)
        assert caught.value.code == 400

        confirmed = Request(
            f"{base}/api/sinks/brain/retry-dead",
            method="POST",
            headers={"X-SignalKit-Action": "retry-dead"},
        )
        with urlopen(confirmed, timeout=3) as response:
            payload = _json(response)
            assert payload == {"sink_key": "brain", "queued": 1}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)

    with SQLiteSignalStore(database) as store:
        assert store.delivery_counts("brain") == {"pending": 1}


def test_dashboard_missing_database_returns_structured_error(tmp_path) -> None:
    database = tmp_path / "missing.db"
    server, thread, base = _start_server(database)
    try:
        with pytest.raises(HTTPError) as caught:
            urlopen(f"{base}/api/overview", timeout=3)
        assert caught.value.code == 503
        payload = json.loads(caught.value.read().decode("utf-8"))
        assert payload["error"]["code"] == "database_missing"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_dashboard_refuses_remote_binding_without_explicit_opt_in(tmp_path) -> None:
    with pytest.raises(ValueError, match="non-loopback"):
        create_dashboard_server(tmp_path / "signals.db", host="0.0.0.0", port=0)


def test_dashboard_cli_parser_exposes_safe_defaults() -> None:
    args = build_dashboard_parser().parse_args([])

    assert args.db == "signals.db"
    assert args.host == "127.0.0.1"
    assert args.port == 8765
    assert args.allow_actions is False
    assert args.allow_remote is False
