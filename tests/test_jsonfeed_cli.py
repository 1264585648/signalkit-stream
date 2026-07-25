import pytest

from signalkit_stream.cli import build_parser, main


def test_jsonfeed_cli_parses_source_options() -> None:
    args = build_parser().parse_args(
        [
            "collect",
            "jsonfeed",
            "https://example.com/feed.json",
            "--source",
            "blog",
            "--instance",
            "product",
            "--seen-window",
            "500",
            "--limit",
            "25",
            "--no-store",
            "--format",
            "jsonl",
        ]
    )

    assert args.collector == "jsonfeed"
    assert args.url == "https://example.com/feed.json"
    assert args.source == "blog"
    assert args.instance == "product"
    assert args.seen_window == 500
    assert args.limit == 25


def test_jsonfeed_cli_rejects_too_small_seen_window() -> None:
    with pytest.raises(SystemExit, match="--seen-window must be >= 100"):
        main(
            [
                "collect",
                "jsonfeed",
                "https://example.com/feed.json",
                "--seen-window",
                "99",
                "--no-store",
            ]
        )
