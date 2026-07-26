from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Iterator, Sequence
import contextlib
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import sys
from types import FrameType

from signalkit_stream.collectors import (
    GitHubCollector,
    HackerNewsCollector,
    JSONFeedCollector,
    RSSCollector,
    RedditCollector,
)
from signalkit_stream.config import load_config, sample_config
from signalkit_stream.diagnostics import DiagnosticReport, doctor, validate_config_file
from signalkit_stream.logging_utils import configure_logging
from signalkit_stream.maintenance import BackupResult, VerifyResult, backup_database, verify_database
from signalkit_stream.models import SignalEvent
from signalkit_stream.observability import format_snapshot, read_snapshot
from signalkit_stream.pipeline import run_collector
from signalkit_stream.runtime import StreamRuntime
from signalkit_stream.storage import SQLiteSignalStore, SourceHealth


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="signalkit",
        description="Collect public web signals into one normalized, resumable event stream.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="Collect from a source")
    collectors = collect.add_subparsers(dest="collector", required=True)

    rss = collectors.add_parser("rss", help="Collect an RSS/Atom feed")
    rss.add_argument("url")
    rss.add_argument("--source", default="rss", help="Logical source name stored on events")
    rss.add_argument("--instance", help="Stable source instance name (default: feed URL)")
    _add_collection_options(rss)

    jsonfeed = collectors.add_parser("jsonfeed", help="Collect a JSON Feed 1.x feed")
    jsonfeed.add_argument("url")
    jsonfeed.add_argument("--source", default="jsonfeed", help="Logical source name stored on events")
    jsonfeed.add_argument("--instance", help="Stable source instance name (default: feed URL)")
    _add_collection_options(jsonfeed)

    hn = collectors.add_parser("hn", help="Collect Hacker News")
    hn.add_argument(
        "--feed",
        default="newstories",
        choices=[
            "topstories",
            "newstories",
            "beststories",
            "askstories",
            "showstories",
            "jobstories",
        ],
    )
    hn.add_argument(
        "--comments",
        type=int,
        default=0,
        metavar="N",
        help="Collect up to N top-level comments per story",
    )
    _add_collection_options(hn)

    github = collectors.add_parser("github", help="Search GitHub issues and pull requests")
    github.add_argument("query", help='GitHub issue search query, e.g. \'"looking for" is:issue\'')
    github.add_argument(
        "--comments",
        type=int,
        default=0,
        metavar="N",
        help="Collect up to N comments per issue/PR",
    )
    github.add_argument(
        "--token-env",
        default="GITHUB_TOKEN",
        help="Environment variable containing a GitHub token (default: GITHUB_TOKEN)",
    )
    github.add_argument("--instance", help="Stable source instance name (default: query hash)")
    _add_collection_options(github)

    reddit = collectors.add_parser("reddit", help="Collect a subreddit through Reddit OAuth")
    reddit.add_argument("subreddit", help="Subreddit name, for example SaaS")
    reddit.add_argument(
        "--listing",
        default="new",
        choices=["new", "hot", "top", "rising"],
        help="Subreddit listing to poll (default: new)",
    )
    reddit.add_argument(
        "--time-filter",
        choices=["hour", "day", "week", "month", "year", "all"],
        help="Reddit time filter used by listings such as top",
    )
    reddit.add_argument(
        "--comments",
        type=int,
        default=0,
        metavar="N",
        help="Collect up to N top-level comments per post",
    )
    reddit.add_argument("--access-token-env", default="REDDIT_ACCESS_TOKEN")
    reddit.add_argument("--refresh-token-env", default="REDDIT_REFRESH_TOKEN")
    reddit.add_argument("--client-id-env", default="REDDIT_CLIENT_ID")
    reddit.add_argument("--client-secret-env", default="REDDIT_CLIENT_SECRET")
    reddit.add_argument("--user-agent-env", default="REDDIT_USER_AGENT")
    reddit.add_argument("--instance", help="Stable source instance name")
    _add_collection_options(reddit)

    init = subparsers.add_parser("init", help="Create a documented runtime configuration")
    init.add_argument("path", nargs="?", default="signalkit.toml")
    init.add_argument("--force", action="store_true", help="Overwrite an existing file")

    validate = subparsers.add_parser(
        "validate",
        help="Validate configuration, adapters, credentials, and sink wiring without network calls",
    )
    validate.add_argument("config", nargs="?", default="signalkit.toml")
    validate.add_argument("--format", choices=["json", "table"], default="table")

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Run local configuration, credential, and SQLite diagnostics",
    )
    doctor_parser.add_argument("config", nargs="?", default="signalkit.toml")
    doctor_parser.add_argument("--format", choices=["json", "table"], default="table")

    run = subparsers.add_parser("run", help="Run configured source and sink workers")
    run.add_argument("config", nargs="?", default="signalkit.toml")
    run.add_argument("--once", action="store_true", help="Run one collection/delivery cycle and exit")
    run.add_argument("--verbose", action="store_true")
    run.add_argument(
        "--log-format",
        choices=["text", "json"],
        default="text",
        help="Runtime log format (default: text)",
    )

    show = subparsers.add_parser("show", help="Read normalized events from SQLite")
    show.add_argument("--db", default="signals.db")
    show.add_argument("--limit", type=int, default=20)
    show.add_argument("--source")
    show.add_argument("--instance")
    show.add_argument("--kind")
    show.add_argument("--format", choices=["jsonl", "json", "table"], default="table")

    checkpoint = subparsers.add_parser("checkpoint", help="Inspect a source checkpoint")
    checkpoint.add_argument("source_key", help="Source key such as hackernews:newstories")
    checkpoint.add_argument("--db", default="signals.db")

    status = subparsers.add_parser("status", help="Inspect persisted runtime health")
    status.add_argument("--db", default="signals.db")
    status.add_argument(
        "--verbose",
        action="store_true",
        help="Include database schema, stored-signal, source, sink, and delivery state",
    )
    status.add_argument(
        "--format",
        choices=["json", "table", "prometheus"],
        default="table",
    )

    deliveries = subparsers.add_parser("deliveries", help="Inspect durable delivery state")
    deliveries.add_argument("--db", default="signals.db")
    deliveries.add_argument("--sink", help="Limit counts to one sink key")
    deliveries.add_argument("--format", choices=["json", "table"], default="table")

    retry_deliveries = subparsers.add_parser(
        "retry-deliveries",
        help="Replay dead-letter deliveries for one sink",
    )
    retry_deliveries.add_argument("sink_key")
    retry_deliveries.add_argument("--db", default="signals.db")

    database = subparsers.add_parser("db", help="SQLite backup and verification operations")
    database_commands = database.add_subparsers(dest="db_command", required=True)

    backup = database_commands.add_parser("backup", help="Create a consistent SQLite backup")
    backup.add_argument("destination")
    backup.add_argument("--db", default="signals.db", help="Source SQLite database")
    backup.add_argument("--overwrite", action="store_true")
    backup.add_argument("--format", choices=["json", "table"], default="table")

    verify = database_commands.add_parser(
        "verify",
        help="Verify SQLite integrity and persistent schema compatibility",
    )
    verify.add_argument("--db", default="signals.db")
    verify.add_argument("--format", choices=["json", "table"], default="table")

    return parser


def _add_collection_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--limit", type=int, default=20, help="Maximum primary items to collect")
    parser.add_argument("--db", default="signals.db", help="SQLite file (default: signals.db)")
    parser.add_argument("--no-store", action="store_true", help="Do not persist events/checkpoints")
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore the stored checkpoint for this run (events remain idempotent)",
    )
    parser.add_argument("--format", choices=["jsonl", "json", "table"], default="table")


def _format_events(events: Sequence[SignalEvent], output_format: str) -> str:
    if output_format == "json":
        return json.dumps([event.to_dict() for event in events], ensure_ascii=False, indent=2)
    if output_format == "jsonl":
        return "\n".join(json.dumps(event.to_dict(), ensure_ascii=False) for event in events)

    if not events:
        return "No events."
    lines = [f"{'SOURCE':22} {'KIND':14} {'AUTHOR':18} TITLE / CONTENT"]
    lines.append("-" * 110)
    for event in events:
        preview = event.title or event.content or ""
        preview = " ".join(preview.split())
        if len(preview) > 48:
            preview = preview[:45] + "..."
        author = (event.author or "-")[:18]
        source = event.source_key[:22]
        lines.append(f"{source:22} {event.kind.value[:14]:14} {author:18} {preview}")
    return "\n".join(lines)


def _format_health(items: Sequence[SourceHealth], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(
            [
                {
                    "source_key": item.source_key,
                    "status": item.status,
                    "updated_at": item.updated_at.isoformat(),
                    "last_attempt_at": (
                        item.last_attempt_at.isoformat() if item.last_attempt_at else None
                    ),
                    "last_success_at": (
                        item.last_success_at.isoformat() if item.last_success_at else None
                    ),
                    "last_error": item.last_error,
                    "consecutive_failures": item.consecutive_failures,
                    "total_runs": item.total_runs,
                    "total_events": item.total_events,
                }
                for item in items
            ],
            ensure_ascii=False,
            indent=2,
        )
    if not items:
        return "No runtime health records."
    lines = [f"{'SOURCE':32} {'STATUS':14} {'FAILS':7} {'RUNS':7} {'EVENTS':8} LAST SUCCESS"]
    lines.append("-" * 100)
    for item in items:
        success = item.last_success_at.isoformat(timespec="seconds") if item.last_success_at else "-"
        lines.append(
            f"{item.source_key[:32]:32} {item.status[:14]:14} "
            f"{item.consecutive_failures:<7} {item.total_runs:<7} "
            f"{item.total_events:<8} {success}"
        )
    return "\n".join(lines)


def _format_delivery_counts(
    counts: dict[str, int],
    *,
    sink_key: str | None,
    output_format: str,
) -> str:
    payload = {
        "sink_key": sink_key,
        "counts": counts,
        "total": sum(counts.values()),
    }
    if output_format == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2)
    if not counts:
        return f"No deliveries{f' for {sink_key}' if sink_key else ''}."
    lines = [f"Delivery state{f' for {sink_key}' if sink_key else ''}"]
    for status, total in sorted(counts.items()):
        lines.append(f"{status:12} {total}")
    lines.append(f"{'total':12} {sum(counts.values())}")
    return "\n".join(lines)


def _format_diagnostics(report: DiagnosticReport, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report.to_dict(), ensure_ascii=False, indent=2)
    if not report.checks:
        return "No diagnostics."
    lines = [f"{'STATUS':7} {'CHECK':32} MESSAGE", "-" * 100]
    for check in report.checks:
        lines.append(f"{check.status.value.upper():7} {check.name[:32]:32} {check.message}")
    lines.append(
        f"result={'ok' if report.ok else 'failed'} "
        f"warnings={report.warnings} failures={report.failures}"
    )
    return "\n".join(lines)


def _format_backup(result: BackupResult, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(result.to_dict(), ensure_ascii=False, indent=2)
    return (
        f"backup: {result.source} -> {result.destination}\n"
        f"pages: {result.pages}\n"
        f"quick_check: {result.quick_check}\n"
        f"schema_version: {result.schema_version}"
    )


def _format_verify(result: VerifyResult, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(result.to_dict(), ensure_ascii=False, indent=2)
    lines = [
        f"database: {result.database}",
        f"quick_check: {result.quick_check}",
        f"pages: {result.page_count}",
        f"page_size: {result.page_size}",
        f"schema: {result.schema_version}/{result.supported_schema_version} "
        f"({result.schema_status})",
    ]
    if result.schema_error:
        lines.append(f"schema_error: {result.schema_error}")
    return "\n".join(lines)


async def _run_collect(args: argparse.Namespace) -> int:
    if args.limit < 1:
        raise SystemExit("--limit must be >= 1")

    if args.collector == "rss":
        collector = RSSCollector(args.url, source=args.source, instance=args.instance)
    elif args.collector == "jsonfeed":
        collector = JSONFeedCollector(args.url, source=args.source, instance=args.instance)
    elif args.collector == "hn":
        collector = HackerNewsCollector(
            feed=args.feed,
            include_comments=args.comments > 0,
            comments_per_story=max(0, args.comments),
        )
    elif args.collector == "github":
        collector = GitHubCollector(
            args.query,
            token=os.getenv(args.token_env),
            include_comments=args.comments > 0,
            comments_per_item=max(0, args.comments),
            instance=args.instance,
        )
    elif args.collector == "reddit":
        credentials = _reddit_credentials(args)
        collector = RedditCollector(
            args.subreddit,
            access_token=credentials["access_token"],
            refresh_token=credentials["refresh_token"],
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            user_agent=credentials["user_agent"] or "",
            listing=args.listing,
            time_filter=args.time_filter,
            include_comments=args.comments > 0,
            comments_per_post=max(0, args.comments),
            instance=args.instance,
        )
    else:  # pragma: no cover - argparse prevents this.
        raise SystemExit(f"unknown collector: {args.collector}")

    if args.no_store:
        result = await run_collector(collector, limit=args.limit)
    else:
        with SQLiteSignalStore(args.db) as store:
            result = await run_collector(
                collector,
                limit=args.limit,
                store=store,
                resume=not args.fresh,
            )

    print(_format_events(result.events, args.format))
    if not args.no_store:
        print(
            (
                f"Collected {result.primary_count} primary items / {len(result.events)} events; "
                f"inserted={result.inserted} updated={result.updated} "
                f"unchanged={result.unchanged}; pages={result.pages}; db={args.db}"
            ),
            file=sys.stderr,
        )
    for warning in result.warnings:
        print(f"warning: {warning}", file=sys.stderr)
    return 0


def _reddit_credentials(args: argparse.Namespace) -> dict[str, str | None]:
    access_token = _optional_env(args.access_token_env)
    refresh_token = _optional_env(args.refresh_token_env)
    client_id = _optional_env(args.client_id_env)
    client_secret = _optional_env(args.client_secret_env)

    if refresh_token and not client_id:
        client_id = _required_env(args.client_id_env)
    if not access_token and not refresh_token:
        client_id = client_id or _required_env(args.client_id_env)
        client_secret = client_secret or _required_env(args.client_secret_env)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "user_agent": _required_env(args.user_agent_env),
    }


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"required environment variable is not set: {name}")
    return value


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def _run_init(args: argparse.Namespace) -> int:
    path = Path(args.path)
    if path.exists() and not args.force:
        print(f"Refusing to overwrite existing configuration: {path}", file=sys.stderr)
        return 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sample_config(), encoding="utf-8")
    print(f"Created {path}")
    return 0


def _run_validate(args: argparse.Namespace) -> int:
    report = validate_config_file(args.config)
    print(_format_diagnostics(report, args.format))
    return 0 if report.ok else 1


def _run_doctor(args: argparse.Namespace) -> int:
    report = doctor(args.config)
    print(_format_diagnostics(report, args.format))
    return 0 if report.ok else 1


_SignalHandler = Callable[[int, FrameType | None], object] | int | signal.Handlers | None


def _shutdown_signals() -> tuple[signal.Signals, ...]:
    """Return the signals this platform can use to request a graceful stop.

    POSIX supervisors use SIGINT/SIGTERM. Windows cannot deliver a catchable SIGTERM at
    all: ``TerminateProcess`` (what ``Popen.terminate()`` calls) is not interceptable, and
    the only catchable supervisor-initiated stop is ``CTRL_BREAK_EVENT``, which Python
    surfaces as ``SIGBREAK``. SIGBREAK only exists on Windows, so POSIX is unaffected.
    """

    signals: list[signal.Signals] = [signal.SIGINT, signal.SIGTERM]
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        signals.append(sigbreak)
    return tuple(signals)


@contextlib.contextmanager
def _stop_signals(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> Iterator[None]:
    """Set ``stop`` when a shutdown signal arrives, restoring prior handlers on exit.

    ``loop.add_signal_handler`` is POSIX-only and raises ``NotImplementedError`` on
    Windows, so this falls back to ``signal.signal``. The fallback hops through
    ``loop.call_soon_threadsafe`` because that writes to the loop's self-pipe, which is
    what wakes a proactor loop blocked in ``GetQueuedCompletionStatus``.
    """

    loop_installed: list[signal.Signals] = []
    previous: dict[int, _SignalHandler] = {}

    def _request_stop(signum: int, frame: FrameType | None) -> None:
        # Capturing SIGINT replaces default_int_handler, so restore the prior disposition
        # immediately: a second Ctrl+C must still force-quit a shutdown that hangs.
        with contextlib.suppress(OSError, ValueError):
            signal.signal(signum, previous.get(signum, signal.SIG_DFL))
        previous.pop(signum, None)
        loop.call_soon_threadsafe(stop.set)

    for signum in _shutdown_signals():
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError, OSError, ValueError):
            pass
        else:
            loop_installed.append(signum)
            continue
        try:
            previous[signum] = signal.signal(signum, _request_stop)
        except (OSError, ValueError):
            # Unsupported signal number, or not running in the main thread.
            continue
    try:
        yield
    finally:
        for signum in loop_installed:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(signum)
        for signum, handler in list(previous.items()):
            with contextlib.suppress(OSError, ValueError):
                signal.signal(signum, handler)


async def _run_runtime(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    configure_logging(
        level=logging.DEBUG if args.verbose else logging.INFO,
        output_format=args.log_format,
        force=True,
    )
    with SQLiteSignalStore(config.runtime.database) as store:
        runtime = StreamRuntime(config, store)
        if args.once:
            try:
                results = await runtime.run_once()
            finally:
                await runtime.aclose()
            for result in results:
                print(
                    json.dumps(
                        {
                            "type": "source",
                            "name": result.name,
                            "source_key": result.source_key,
                            "success": result.success,
                            "status": result.status,
                            "delay": result.delay,
                            "events": len(result.collection.events) if result.collection else 0,
                            "error": result.error,
                        },
                        ensure_ascii=False,
                    )
                )
            for result in runtime.last_delivery_results:
                print(
                    json.dumps(
                        {
                            "type": "delivery",
                            "sink_key": result.sink_key,
                            "attempted": result.attempted,
                            "delivered": result.delivered,
                            "failed": result.failed,
                            "dead": result.dead,
                            "superseded": result.superseded,
                        },
                        ensure_ascii=False,
                    )
                )
            source_success = all(result.success for result in results)
            delivery_success = all(
                result.failed == 0 and result.dead == 0
                for result in runtime.last_delivery_results
            )
            return 0 if source_success and delivery_success else 1

        stop = asyncio.Event()
        with _stop_signals(asyncio.get_running_loop(), stop):
            await runtime.run_forever(stop)
    return 0


def _run_show(args: argparse.Namespace) -> int:
    with SQLiteSignalStore(args.db) as store:
        events = store.list_recent(
            limit=args.limit,
            source=args.source,
            source_instance=args.instance,
            kind=args.kind,
        )
    print(_format_events(events, args.format))
    return 0


def _run_checkpoint(args: argparse.Namespace) -> int:
    with SQLiteSignalStore(args.db) as store:
        checkpoint = store.get_checkpoint(args.source_key)
    if checkpoint is None:
        print("No checkpoint.")
        return 1
    print(
        json.dumps(
            {
                "source_key": checkpoint.source_key,
                "cursor": checkpoint.cursor.to_dict(),
                "updated_at": checkpoint.updated_at.isoformat(),
                "last_success_at": (
                    checkpoint.last_success_at.isoformat() if checkpoint.last_success_at else None
                ),
                "last_error": checkpoint.last_error,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _run_status(args: argparse.Namespace) -> int:
    if args.verbose or args.format == "prometheus":
        try:
            snapshot = read_snapshot(args.db)
        except (FileNotFoundError, sqlite3.Error) as exc:
            print(f"status failed: {exc}", file=sys.stderr)
            return 1
        output = format_snapshot(snapshot, output_format=args.format)
        print(output, end="" if args.format == "prometheus" else "\n")
        return 0 if snapshot.schema_status == "current" else 1

    with SQLiteSignalStore(args.db) as store:
        health = store.list_source_health()
    print(_format_health(health, args.format))
    return 0


def _run_deliveries(args: argparse.Namespace) -> int:
    with SQLiteSignalStore(args.db) as store:
        counts = store.delivery_counts(args.sink)
    print(_format_delivery_counts(counts, sink_key=args.sink, output_format=args.format))
    return 0


def _run_retry_deliveries(args: argparse.Namespace) -> int:
    with SQLiteSignalStore(args.db) as store:
        retried = store.retry_dead_deliveries(args.sink_key)
    print(f"Queued {retried} dead deliveries for retry on {args.sink_key}.")
    return 0


def _run_db(args: argparse.Namespace) -> int:
    try:
        if args.db_command == "backup":
            result = backup_database(args.db, args.destination, overwrite=args.overwrite)
            print(_format_backup(result, args.format))
            return 0
        result = verify_database(args.db)
        print(_format_verify(result, args.format))
        return 0 if result.ok else 1
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        if args.format == "json":
            print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        else:
            print(f"database operation failed: {exc}", file=sys.stderr)
        return 1


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "collect":
        return asyncio.run(_run_collect(args))
    if args.command == "init":
        return _run_init(args)
    if args.command == "validate":
        return _run_validate(args)
    if args.command == "doctor":
        return _run_doctor(args)
    if args.command == "run":
        return asyncio.run(_run_runtime(args))
    if args.command == "show":
        return _run_show(args)
    if args.command == "checkpoint":
        return _run_checkpoint(args)
    if args.command == "status":
        return _run_status(args)
    if args.command == "deliveries":
        return _run_deliveries(args)
    if args.command == "retry-deliveries":
        return _run_retry_deliveries(args)
    if args.command == "db":
        return _run_db(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
