# SignalKit Stream

SignalKit Stream is the ingestion boundary for SignalKit: a small, reliable framework that collects public signals from external sources, normalizes them into a versioned event contract, checkpoints progress, deduplicates source objects, and hands clean events to downstream analysis or automation systems.

The module deliberately stops at ingestion. Intent classification, lead scoring, enrichment, CRM sync, outreach, and agent actions belong downstream.

## Architecture

```text
RSS / Hacker News / GitHub / future adapters
                    ↓
               Collectors
                    ↓
       CollectorResult + Cursor
                    ↓
              run_collector
          retry / pagination / resume
                    ↓
        SignalEvent schema v1
                    ↓
      SQLite event + checkpoint store
                    ↓
       downstream sinks / consumers
```

The important boundary is the `SignalEvent` contract. Downstream code should not need source-specific parsing logic.

## Current core

The current core includes:

- versioned `SignalEvent` schema and deterministic IDs
- `SourceIdentity`, `Cursor`, `CollectorContext`, `CollectorResult`, and stable collector errors
- pluggable async collector SDK
- shared HTTP retry/backoff, timeout handling, and rate-limit inspection
- resumable collection with pagination-loop protection
- atomic SQLite event + checkpoint commits
- idempotent inserts and source-object updates using content fingerprints
- migration of databases created by the original 0.1 schema
- RSS / Atom collection with conditional HTTP requests
- Hacker News story and optional top-level comment collection
- GitHub issue / pull-request search with optional comments and resumable pagination
- CLI collection, event inspection, and checkpoint inspection
- offline tests based on mocked transports
- CI on Python 3.11, 3.12, and 3.13

This is not intended as a throwaway MVP. The public contracts in this package are the foundation for the long-running runtime, sinks, and additional adapters documented in `docs/ROADMAP.md`.

## Reliability model

SignalKit Stream uses **at-least-once collection + idempotent persistence**.

For a persisted run, each collector page and its next cursor are committed in one SQLite transaction. If a process dies before that transaction, the same page can be collected again. If it dies after the transaction, the next run resumes from the stored cursor. Stable event IDs and fingerprints make repeated collection safe.

A checkpoint is scoped to a source instance such as:

```text
hackernews:newstories
github:search-<query-hash>
rss:https://example.com/feed.xml
```

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

## CLI

Collect an RSS/Atom feed:

```bash
signalkit collect rss https://hnrss.org/newest --source hn-rss --limit 20
```

Collect Hacker News stories and comments:

```bash
signalkit collect hn --feed askstories --limit 20 --comments 3
```

Search GitHub issues / pull requests:

```bash
signalkit collect github '"looking for" is:issue is:open' --limit 50 --comments 3
```

For higher GitHub API limits:

```bash
export GITHUB_TOKEN=github_pat_xxx
```

Repeated commands resume from the stored checkpoint. Use `--fresh` to ignore the checkpoint for one run while keeping event writes idempotent.

Inspect events:

```bash
signalkit show --limit 20
signalkit show --source github --kind issue --format jsonl
```

Inspect a checkpoint:

```bash
signalkit checkpoint hackernews:newstories
```

Skip persistence and emit JSONL:

```bash
signalkit collect rss https://example.com/feed.xml --no-store --format jsonl \
  | your-intent-classifier
```

## Event contract

```json
{
  "id": "sig_4a88d36e...",
  "schema_version": 1,
  "source": "github",
  "source_instance": "search-67b7...",
  "kind": "issue",
  "title": "Need webhook support",
  "content": "We are looking for webhook support.",
  "author": "alice",
  "url": "https://github.com/acme/app/issues/42",
  "created_at": "2026-07-01T10:00:00+00:00",
  "updated_at": "2026-07-02T10:00:00+00:00",
  "collected_at": "2026-07-25T14:00:00+00:00",
  "metadata": {
    "number": 42,
    "state": "open",
    "labels": ["feature"]
  }
}
```

Normalized kinds are `article`, `comment`, `issue`, `post`, `pull_request`, `story`, and `other`. Source-specific fields stay under `metadata`.

## Collector contract

A collector owns exactly one logical source instance and returns a resumable batch:

```python
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor


async def collect(
    *,
    context: CollectorContext | None = None,
    cursor: Cursor | None = None,
) -> CollectorResult:
    ...
```

`CollectorResult` carries normalized events, the next cursor, whether more data is immediately available, primary-item count, warnings, and source rate-limit information.

See `docs/ARCHITECTURE.md` for invariants and `examples/custom_collector.py` for an adapter example.

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
        result = await run_collector(
            collector,
            limit=50,
            store=store,
        )

    print(
        result.primary_count,
        result.inserted,
        result.updated,
        result.unchanged,
    )


asyncio.run(main())
```

## Development

```bash
make install
make check
```

Or:

```bash
ruff check .
pytest --cov=signalkit_stream --cov-report=term-missing --cov-fail-under=80
python -m compileall -q src
```

The normal test suite must not require external network access. Live API smoke tests, when added, will be kept separate from the deterministic CI gate.

## Source support

| Source | Primary items | Comments | Incremental state | Auth |
| --- | --- | --- | --- | --- |
| RSS / Atom | entries | feed-dependent | ETag / Last-Modified + cursor | no |
| Hacker News | stories | top-level comments | bounded seen-ID cursor | no |
| GitHub | issues / PRs | issue / PR comments | page/offset + update watermark | optional token |
| Reddit | planned adapter | planned | planned | app credentials |
| Generic JSON Feed / REST | planned | adapter-dependent | planned | adapter-dependent |

Prefer official APIs and feeds where available, and respect authentication, rate limits, robots rules, and source terms.

## Roadmap

The implementation order and release gates are maintained in `docs/ROADMAP.md`. The next layer is the long-running runtime: configuration, scheduler, source-aware throttling/circuit breaking, sinks, Reddit, health/metrics, and restart/end-to-end tests.

## License

MIT
