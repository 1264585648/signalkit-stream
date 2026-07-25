# SignalKit Stream

SignalKit Stream is the ingestion boundary for SignalKit: a reliable framework for continuously collecting public signals from external sources, normalizing them into a versioned event contract, checkpointing progress, persisting source health, and handing clean events to downstream analysis or automation systems.

The module deliberately stops at ingestion. Intent classification, lead scoring, enrichment, CRM sync, outreach, and agent actions belong downstream.

## Architecture

```text
TOML config
    ↓
Source registry / factories
    ↓
Runtime scheduler
    ├─ global concurrency
    ├─ provider concurrency
    ├─ rate-limit pauses
    ├─ failure backoff / circuit breaker
    └─ graceful shutdown
    ↓
Collectors
RSS / Hacker News / GitHub / future adapters
    ↓
CollectorResult + Cursor
    ↓
run_collector
retry / pagination / resume
    ↓
SignalEvent schema v1
    ↓
SQLite event + checkpoint store
SQLite source-health store
    ↓
downstream sinks / consumers
```

The important boundary is the `SignalEvent` contract. Downstream code should not need source-specific parsing logic.

## Current module

The current implementation includes:

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
- strict TOML runtime configuration
- source registry / collector factory
- continuous scheduler with per-source polling intervals
- global and provider-level concurrency limits
- persisted rate-limit pauses and source health
- failure backoff and circuit-breaker cooldown
- bounded graceful shutdown
- `signalkit run` continuous lifecycle command
- deterministic offline tests and CI on Python 3.11, 3.12, and 3.13

This is not a throwaway MVP. These contracts are the foundation of the complete Stream module documented in `docs/ROADMAP.md`.

## Reliability model

SignalKit Stream uses **at-least-once collection + idempotent persistence**.

For a persisted run, each collector page and its next cursor are committed in one SQLite transaction. If a process dies before that transaction, the page can be collected again. If it dies after the transaction, the next process resumes from the committed cursor. Stable event IDs and fingerprints make repeated collection safe.

Runtime source health is persisted separately, including last attempt, last success, failure count, pause deadline, and observed rate-limit state. A restart therefore preserves both collection progress and scheduler cooldown state.

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

## Continuous runtime

Copy the example configuration:

```bash
cp examples/signalkit.toml signalkit.toml
```

Example:

```toml
[runtime]
database = "signals.db"
global_concurrency = 4
provider_concurrency = 2
shutdown_timeout = 15
default_interval = 60
failure_threshold = 3
cooldown = 300

[[sources]]
name = "hn-ask"
type = "hackernews"
interval = 60
limit = 50
options = { feed = "askstories", comments = 3 }

[[sources]]
name = "github-demand"
type = "github"
interval = 120
limit = 100
options = { query = '"looking for" is:issue is:open', comments = 3, token_env = "GITHUB_TOKEN" }

[[sources]]
name = "hn-rss"
type = "rss"
interval = 60
options = { url = "https://hnrss.org/newest" }
```

Run continuously:

```bash
signalkit run --config signalkit.toml
```

Run one scheduler cycle and exit, useful for smoke tests or cron:

```bash
signalkit run --config signalkit.toml --once
```

The runtime isolates sources: a failing collector does not stop healthy collectors. Exhausted rate limits and repeated failures create persisted pauses so the process does not busy-loop. SIGINT/SIGTERM requests a graceful stop; collectors get a bounded shutdown window before remaining worker tasks are cancelled.

## One-shot CLI

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
  "source_instance": "github-demand",
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

The normal test suite must not require external network access. Scheduler tests use a controllable clock rather than wall-clock sleeps.

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

The foundation and continuous runtime layers are implemented. The next major layer is delivery: sink protocol, stdout/JSONL, webhook delivery, fan-out, replay, and dead-letter persistence. Adapter completion and operational commands follow after the sink contract is stable.

See `docs/ROADMAP.md` for release gates.

## License

MIT
