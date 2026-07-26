"""Helpers shared by the first-party collectors."""

from __future__ import annotations

from html.parser import HTMLParser
import re
from urllib.parse import urlsplit, urlunsplit

MIN_SEEN_WINDOW = 50
"""Smallest dedup window any adapter accepts for its ``seen_window`` option."""


def validated_seen_window(value: int, *, label: str) -> int:
    """Return ``value`` unchanged, or raise ``ValueError`` if it is unusable.

    Every adapter shares this one contract: ``seen_window`` is validated, never
    silently reinterpreted. Clamping (``max(50, seen_window)``) would hand the
    operator a different dedup window than the one they configured, and a window
    that is too small quietly turns into repeated re-emission of old items.
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} seen_window must be an integer")
    if value < MIN_SEEN_WINDOW:
        raise ValueError(f"{label} seen_window must be >= {MIN_SEEN_WINDOW}")
    return value


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data)


def html_to_text(value: str | None) -> str:
    if not value:
        return ""
    parser = _TextExtractor()
    try:
        parser.feed(value)
        text = " ".join(parser.parts)
    except Exception:
        text = value
    return re.sub(r"\s+", " ", text).strip()


def redact_url(value: str) -> str:
    """Return ``value`` without its credentials, query string, or fragment.

    Feed URLs routinely carry the only secret an operator has for that source
    (``?auth_token=...`` for private RSS, Google Alerts, Feedly, per-tenant JSON
    feeds). Anything a collector copies into an event is written to the database,
    the JSONL archive, stdout, and every configured webhook, so the exported form
    of a feed URL keeps only scheme, host, port, and path.
    """

    text = value.strip()
    if not text:
        return text
    try:
        parts = urlsplit(text)
    except ValueError:  # pragma: no cover - urlsplit is extremely permissive
        return text
    if not parts.scheme or not parts.netloc:
        # Not an absolute URL; drop everything from the first '?' or '#' anyway.
        return re.split(r"[?#]", text, maxsplit=1)[0]
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
