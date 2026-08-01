from __future__ import annotations

import argparse
from collections.abc import Sequence
import sys

from signalkit_stream.dashboard import serve_dashboard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="signalkit-console",
        description="Open the local SignalKit Stream operator console.",
    )
    parser.add_argument("--db", default="signals.db", help="SQLite database (default: signals.db)")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="Bind port (default: 8765)")
    parser.add_argument("--open", action="store_true", help="Open the console in a browser")
    parser.add_argument(
        "--allow-actions",
        action="store_true",
        help="Enable mutating actions such as retrying dead deliveries",
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow a non-loopback bind; the console has no built-in authentication",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        serve_dashboard(
            args.db,
            host=args.host,
            port=args.port,
            allow_actions=args.allow_actions,
            allow_remote=args.allow_remote,
            open_browser=args.open,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"console failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
