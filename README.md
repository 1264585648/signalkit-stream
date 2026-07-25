# SignalKit Stream

SignalKit Stream is a small, pluggable **public-signal ingestion layer for AI agents**. It collects posts, issues, comments, stories, and feed items from different sources, normalizes them into one event schema, deduplicates them, and hands clean data to downstream systems such as intent detection, lead discovery, monitoring, and automation workflows.

The project deliberately stops at ingestion. LLM classification, lead scoring, CRM sync, outreach, and agent actions belong in downstream components.

## Architecture

```text
Reddit / Hacker News / GitHub / RSS / Forums
                    ↓
               Collectors
                    ↓
             SignalEvent schema
                    ↓
          SQLite / stream consumer
                    ↓
  intent detection / scoring / automation
```

## Current MVP

- Unified `SignalEvent` schema with deterministic IDs
- Pluggable async collector interface
- RSS / Atom collector
- Hacker News collector using its public API
- GitHub issue / pull-request search collector
- Optional Hacker News and GitHub comment collection
- SQLite persistence with ID-based deduplication
- CLI for collection and inspection
- Network-free unit tests using mocked HTTP transports
- GitHub Actions CI for Python 3.11, 3.12, and 3.13

### Source support

| Source | Primary items | Comments | Auth | Status |
| --- | --- | --- | --- | --- |
| RSS / Atom | Articles / posts | Feed-dependent | No | Ready |
| Hacker News | Stories | Top-level comments | No | Ready |
| GitHub | Issues / PRs | Issue / PR comments | Optional token | Ready |
| Reddit | Available feeds can use the RSS collector | Feed-dependent | — | Dedicated API adapter planned |
| Generic forums | Add a collector for the site's API/feed | Collector-dependent | Collector-dependent | Extension point ready |

Prefer official APIs and feeds where available, and respect each source's authentication, rate limits, robots rules, and terms.

## Install

Requires Python 3.11+.

```bash
git clone https://github.com/1264585648/signalkit-stream.git
cd signalkit-stream
python -m venv .venv
```

macOS / Linux:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Then:

```bash
python -m pip install -e .
```

For development:

```bash
python -m pip install -e ".[dev]"
```

## Quick start

Collect an RSS/Atom feed:

```bash
signalkit collect rss https://hnrss.org/newest --source hn-rss --limit 20
```

Collect Hacker News stories:

```bash
signalkit collect hn --feed newstories --limit 20
```

Include comments:

```bash
signalkit collect hn --feed askstories --limit 10 --comments 3
```

Search GitHub issues and pull requests:

```bash
signalkit collect github '"looking for" is:issue is:open' --limit 20
```

For higher GitHub API limits, set a token in `GITHUB_TOKEN`:

```bash
export GITHUB_TOKEN=github_pat_xxx
signalkit collect github '"alternative to" is:issue is:open' --comments 3
```

Inspect stored signals:

```bash
signalkit show --limit 20
signalkit show --source github --kind issue --format jsonl
```

Skip SQLite and stream JSONL to another process:

```bash
signalkit collect rss https://example.com/feed.xml --no-store --format jsonl \
  | your-intent-classifier
```

## Event schema

Every collector emits the same `SignalEvent` contract:

```json
{
  "id": "sig_4a88d36e...",
  "source": "github",
  "kind": "issue",
  "title": "Need webhook support",
  "content": "We are looking for webhook support.",
  "author": "alice",
  "url": "https://github.com/acme/app/issues/42",
  "created_at": "2026-07-01T10:00:00+00:00",
  "collected_at": "2026-07-25T14:00:00+00:00",
  "metadata": {
    "number": 42,
    "state": "open",
    "labels": ["feature"]
  }
}
```

Current normalized kinds are `article`, `comment`, `issue`, `post`, `pull_request`, `story`, and `other`. Source-specific fields stay under `metadata`.

## Python API

```python
import asyncio

from signalkit_stream.collectors import HackerNewsCollector
from signalkit_stream.pipeline import run_collector
from signalkit_stream.storage import SQLiteSignalStore


async def main() -> None:
    collector = HackerNewsCollector(
        feed="askstories",
        include_comments=True,
        comments_per_story=3,
    )

    with SQLiteSignalStore("signals.db") as store:
        result = await run_collector(collector, limit=20, store=store)

    print(f"received={len(result.events)} inserted={result.inserted}")


asyncio.run(main())
```

## Adding a collector

Subclass `Collector` or `HTTPCollector`, fetch from an official API/feed, and return `SignalEvent` objects. Use `SignalEvent.stable_id()` with a source-native immutable ID and keep source-specific data in `metadata`.

See `examples/custom_collector.py` for a minimal example.

## Project layout

```text
src/signalkit_stream/
├── collectors/
│   ├── base.py
│   ├── github.py
│   ├── hackernews.py
│   └── rss.py
├── cli.py
├── models.py
├── pipeline.py
└── storage.py

tests/
.github/workflows/ci.yml
```

## Development

```bash
make install
make check
```

Or run checks directly:

```bash
ruff check .
pytest --cov=signalkit_stream --cov-report=term-missing
python -m compileall -q src
```

Collector tests are designed to run without external network access.

## Next milestones

- Dedicated Reddit API collector when credentials/access are configured
- Cursor/checkpoint support for incremental polling
- Retry/backoff and source-aware rate limiting
- Generic webhook and JSON Feed adapters
- Pluggable sinks such as PostgreSQL, Redis Streams, Kafka, and webhooks
- Source health metrics and daemon/scheduler mode

## License

MIT
