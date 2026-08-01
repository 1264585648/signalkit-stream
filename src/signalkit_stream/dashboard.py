from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
import ipaddress
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote, urlparse
import webbrowser

from signalkit_stream.maintenance import verify_database
from signalkit_stream.observability import read_snapshot
from signalkit_stream.storage import SQLiteSignalStore

LOGGER = logging.getLogger(__name__)
_MAX_EVENT_LIMIT = 200
_ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/assets/app.css": ("app.css", "text/css; charset=utf-8"),
    "/assets/app.js": ("app.js", "text/javascript; charset=utf-8"),
}


@dataclass(slots=True, frozen=True)
class DashboardOptions:
    database: Path
    allow_actions: bool = False


class DashboardRepository:
    """Read operational state without mutating the Stream database."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database).expanduser()

    def overview(self, *, allow_actions: bool) -> dict[str, Any]:
        snapshot = read_snapshot(self.database)
        with self._connection() as connection:
            activity = {
                "last_hour": self._scalar(
                    connection,
                    "SELECT COUNT(*) FROM signals WHERE julianday(collected_at) >= julianday('now', '-1 hour')",
                ),
                "last_24h": self._scalar(
                    connection,
                    "SELECT COUNT(*) FROM signals WHERE julianday(collected_at) >= julianday('now', '-24 hours')",
                ),
            }
            source_counts = self._status_counts(connection)
            facets = self._facets(connection)

        delivery_attention = sum(sink.failed + sink.dead for sink in snapshot.sinks)
        return {
            "snapshot": snapshot.to_dict(),
            "activity": activity,
            "source_status_counts": source_counts,
            "delivery_attention": delivery_attention,
            "facets": facets,
            "actions_enabled": allow_actions,
        }

    def events(
        self,
        *,
        limit: int,
        offset: int,
        source: str | None,
        kind: str | None,
        query: str | None,
    ) -> dict[str, Any]:
        limit = max(1, min(_MAX_EVENT_LIMIT, limit))
        offset = max(0, offset)
        conditions: list[str] = []
        params: list[object] = []
        if source:
            conditions.append("source = ?")
            params.append(source)
        if kind:
            conditions.append("kind = ?")
            params.append(kind)
        if query:
            escaped = self._escape_like(query.strip())
            conditions.append(
                "(title LIKE ? ESCAPE '\\' OR content LIKE ? ESCAPE '\\' "
                "OR author LIKE ? ESCAPE '\\' OR url LIKE ? ESCAPE '\\')"
            )
            needle = f"%{escaped}%"
            params.extend([needle, needle, needle, needle])

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection() as connection:
            total = self._scalar(
                connection,
                f"SELECT COUNT(*) FROM signals {where}",
                tuple(params),
            )
            rows = connection.execute(
                f"""
                SELECT id, schema_version, source, source_instance, kind, title, content,
                       author, url, created_at, updated_at, collected_at, metadata_json
                FROM signals
                {where}
                ORDER BY collected_at DESC, created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        return {
            "items": [self._event_row(row, include_metadata=False) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(rows) < total,
        }

    def event(self, event_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT id, schema_version, source, source_instance, kind, title, content,
                       author, url, created_at, updated_at, collected_at, metadata_json
                FROM signals WHERE id = ?
                """,
                (event_id,),
            ).fetchone()
        return self._event_row(row, include_metadata=True) if row is not None else None

    def retry_dead(self, sink_key: str) -> int:
        with SQLiteSignalStore(self.database) as store:
            return store.retry_dead_deliveries(sink_key)

    def verification(self) -> dict[str, Any]:
        return verify_database(self.database).to_dict()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if not self.database.exists():
            raise FileNotFoundError(f"database does not exist: {self.database}")
        uri = f"file:{self.database.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=0.5)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _scalar(
        connection: sqlite3.Connection,
        sql: str,
        params: tuple[object, ...] = (),
    ) -> int:
        row = connection.execute(sql, params).fetchone()
        return int(row[0]) if row is not None else 0

    @staticmethod
    def _status_counts(connection: sqlite3.Connection) -> dict[str, int]:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'source_health'"
        ).fetchone()
        if exists is None:
            return {}
        rows = connection.execute(
            "SELECT status, COUNT(*) AS total FROM source_health GROUP BY status"
        ).fetchall()
        return {str(row["status"]): int(row["total"]) for row in rows}

    @staticmethod
    def _facets(connection: sqlite3.Connection) -> dict[str, list[str]]:
        sources = [
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT source FROM signals ORDER BY source"
            ).fetchall()
        ]
        kinds = [
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT kind FROM signals ORDER BY kind"
            ).fetchall()
        ]
        return {"sources": sources, "kinds": kinds}

    @staticmethod
    def _event_row(row: sqlite3.Row, *, include_metadata: bool) -> dict[str, Any]:
        content = str(row["content"] or "")
        item: dict[str, Any] = {
            "id": str(row["id"]),
            "schema_version": int(row["schema_version"]),
            "source": str(row["source"]),
            "source_instance": str(row["source_instance"]),
            "source_key": f"{row['source']}:{row['source_instance']}",
            "kind": str(row["kind"]),
            "title": str(row["title"]) if row["title"] else None,
            "content": content if include_metadata else content[:600],
            "content_truncated": not include_metadata and len(content) > 600,
            "author": str(row["author"]) if row["author"] else None,
            "url": str(row["url"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]) if row["updated_at"] else None,
            "collected_at": str(row["collected_at"]),
        }
        if include_metadata:
            try:
                metadata = json.loads(str(row["metadata_json"] or "{}"))
            except json.JSONDecodeError:
                metadata = {"_error": "stored metadata is not valid JSON"}
            item["metadata"] = metadata if isinstance(metadata, Mapping) else {"value": metadata}
        return item

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        options: DashboardOptions,
    ) -> None:
        self.options = options
        self.repository = DashboardRepository(options.database)
        super().__init__(server_address, DashboardRequestHandler)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server: DashboardServer
    server_version = "SignalKitConsole/1"
    sys_version = ""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in _ASSETS:
            self._asset(parsed.path)
            return
        if parsed.path == "/api/overview":
            self._api_overview()
            return
        if parsed.path == "/api/verify":
            self._api_verify()
            return
        if parsed.path == "/api/events":
            self._api_events(parse_qs(parsed.query))
            return
        if parsed.path.startswith("/api/events/"):
            event_id = unquote(parsed.path.removeprefix("/api/events/"))
            self._api_event(event_id)
            return
        self._error(HTTPStatus.NOT_FOUND, "not_found", "The requested resource was not found.")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        prefix = "/api/sinks/"
        suffix = "/retry-dead"
        if parsed.path.startswith(prefix) and parsed.path.endswith(suffix):
            sink_key = unquote(parsed.path[len(prefix) : -len(suffix)]).strip("/")
            self._api_retry_dead(sink_key)
            return
        self._error(HTTPStatus.NOT_FOUND, "not_found", "The requested action was not found.")

    def log_message(self, format: str, *args: object) -> None:
        LOGGER.info("dashboard %s - %s", self.address_string(), format % args)

    def _asset(self, path: str) -> None:
        asset_name, content_type = _ASSETS[path]
        try:
            payload = _asset_bytes(asset_name)
        except (FileNotFoundError, OSError) as exc:
            LOGGER.exception("dashboard asset missing: %s", asset_name)
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "asset_missing", str(exc))
            return
        self.send_response(HTTPStatus.OK)
        self._security_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache" if asset_name == "index.html" else "public, max-age=3600")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _api_overview(self) -> None:
        self._run_api(
            lambda: self.server.repository.overview(
                allow_actions=self.server.options.allow_actions
            )
        )

    def _api_verify(self) -> None:
        self._run_api(self.server.repository.verification)

    def _api_events(self, query: dict[str, list[str]]) -> None:
        try:
            limit = self._query_int(query, "limit", 30, minimum=1, maximum=_MAX_EVENT_LIMIT)
            offset = self._query_int(query, "offset", 0, minimum=0, maximum=1_000_000)
        except ValueError as exc:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_query", str(exc))
            return
        self._run_api(
            lambda: self.server.repository.events(
                limit=limit,
                offset=offset,
                source=self._query_text(query, "source"),
                kind=self._query_text(query, "kind"),
                query=self._query_text(query, "q"),
            )
        )

    def _api_event(self, event_id: str) -> None:
        if not event_id or len(event_id) > 512:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_event", "a valid event id is required")
            return
        try:
            event = self.server.repository.event(event_id)
        except (FileNotFoundError, sqlite3.Error, RuntimeError, ValueError) as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "database_unavailable", str(exc))
            return
        if event is None:
            self._error(HTTPStatus.NOT_FOUND, "event_not_found", "Signal event was not found.")
            return
        self._json(HTTPStatus.OK, event)

    def _api_retry_dead(self, sink_key: str) -> None:
        if not self.server.options.allow_actions:
            self._error(
                HTTPStatus.FORBIDDEN,
                "actions_disabled",
                "Dashboard actions are disabled. Start with --allow-actions to enable them.",
            )
            return
        if self.headers.get("X-SignalKit-Action") != "retry-dead":
            self._error(HTTPStatus.BAD_REQUEST, "action_header_required", "Missing action confirmation header.")
            return
        if not sink_key or len(sink_key) > 256:
            self._error(HTTPStatus.BAD_REQUEST, "invalid_sink", "a valid sink key is required")
            return
        self._run_api(
            lambda: {"sink_key": sink_key, "queued": self.server.repository.retry_dead(sink_key)}
        )

    def _run_api(self, operation: Any) -> None:
        try:
            payload = operation()
        except FileNotFoundError as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "database_missing", str(exc))
            return
        except (sqlite3.Error, RuntimeError, ValueError) as exc:
            LOGGER.exception("dashboard API failed")
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, "database_unavailable", str(exc))
            return
        self._json(HTTPStatus.OK, payload)

    def _json(self, status: HTTPStatus, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self._security_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, code: str, message: str) -> None:
        self._json(status, {"error": {"code": code, "message": message}})

    def _security_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            "connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        )

    @staticmethod
    def _query_text(query: dict[str, list[str]], key: str) -> str | None:
        values = query.get(key)
        if not values:
            return None
        value = values[0].strip()
        return value or None

    @staticmethod
    def _query_int(
        query: dict[str, list[str]],
        key: str,
        default: int,
        *,
        minimum: int,
        maximum: int,
    ) -> int:
        raw = query.get(key, [str(default)])[0]
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{key} must be an integer") from exc
        if value < minimum or value > maximum:
            raise ValueError(f"{key} must be between {minimum} and {maximum}")
        return value


@lru_cache(maxsize=8)
def _asset_bytes(name: str) -> bytes:
    if name not in {value[0] for value in _ASSETS.values()}:
        raise FileNotFoundError(name)
    return resources.files("signalkit_stream").joinpath("web_assets", name).read_bytes()


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def create_dashboard_server(
    database: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    allow_actions: bool = False,
    allow_remote: bool = False,
) -> DashboardServer:
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    if not allow_remote and not _is_loopback(host):
        raise ValueError(
            "refusing to bind the unauthenticated dashboard to a non-loopback host; "
            "pass --allow-remote only behind a trusted network or reverse proxy"
        )
    return DashboardServer(
        (host, port),
        DashboardOptions(database=Path(database), allow_actions=allow_actions),
    )


def serve_dashboard(
    database: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    allow_actions: bool = False,
    allow_remote: bool = False,
    open_browser: bool = False,
) -> None:
    server = create_dashboard_server(
        database,
        host=host,
        port=port,
        allow_actions=allow_actions,
        allow_remote=allow_remote,
    )
    bound_host, bound_port = server.server_address[:2]
    browser_host = "127.0.0.1" if bound_host in {"0.0.0.0", "::"} else bound_host
    url = f"http://{browser_host}:{bound_port}/"
    print(f"SignalKit Operator Console: {url}")
    print(f"Database: {Path(database).expanduser()}")
    print(f"Actions: {'enabled' if allow_actions else 'read-only'}")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


__all__ = [
    "DashboardOptions",
    "DashboardRepository",
    "DashboardServer",
    "create_dashboard_server",
    "serve_dashboard",
]
