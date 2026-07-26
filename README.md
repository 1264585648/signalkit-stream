# SignalKit Stream

SignalKit Stream is a reliable, pluggable public-signal ingestion service for AI agents, lead intelligence, social listening, and downstream automation systems.

It continuously collects public data from external sources, normalizes source-specific payloads into a stable `SignalEvent` contract, checkpoints incremental progress, persists source mutations idempotently, and delivers committed events through durable sinks.

The module deliberately stops at **ingestion and delivery**. Intent classification, lead scoring, enrichment, CRM sync, outreach, and autonomous agent actions belong downstream.

## Architecture

```text
RSS / Atom     JSON Feed     Hacker News     GitHub     Reddit OAuth
     \             |              |             |             /
      \            |              |             |            /
       +---------------------- Collectors -------------------+
                              |
                    CollectorResult + Cursor
                              |
                         StreamRuntime
             pagination / retry / scheduling / rate limits
                   source health / circuit breaking
                              |
                      SignalEvent schema v1
                              |
                   SQLite transaction boundary
              events + checkpoints + source health + outbox
                              |
                         DeliveryEngine
                  retry / dead letter / replay
                              |
                stdout / JSONL / webhook / future sinks
```

For simple third-party JSON APIs, `GenericRESTCollector` is available as an **explicit extension path**. It is intentionally not enabled in the default registry because arbitrary APIs do not share safe authentication, ordering, pagination, or mapping semantics.

The stable downstream boundary is `SignalEvent`: consumers should not need source-specific parsing logic.

## What is implemented

### Ingestion contracts

- versioned `SignalEvent` schema
- deterministic source-object IDs and mutation fingerprints
- `SourceIdentity`, `Cursor`, `CollectorContext`, and `CollectorResult`
- enforced collector-result validation before persistence/checkpoint advancement
- shared async HTTP timeout, retry/backoff, `Retry-After`, and rate-limit handling
- resumable pagination with loop guards
- deterministic first-party collector contract tests

### First-party sources

- RSS / Atom
- JSON Feed 1.x, including `next_url`
- Hacker News stories and bounded top-level comments
- GitHub issue / pull-request search and bounded comments
- Reddit OAuth posts and bounded top-level comments

Reddit supports static access tokens, refresh-token rollover, and confidential-client `client_credentials`; secrets stay in environment variables and are not persisted in events or checkpoints. See `docs/REDDIT.md`.

### Runtime and persistence

- strict TOML configuration and source/sink registries
- independent long-running source workers with bounded global concurrency
- graceful SIGINT/SIGTERM shutdown (on Windows, `CTRL_BREAK_EVENT` delivered as `SIGBREAK`)
- source failure backoff and circuit-open cooldown
- rate-limit-aware scheduling
- persisted source health across restarts
- atomic SQLite event + checkpoint commits
- `PRAGMA user_version`-backed forward database migrations
- refusal to open databases created by a newer unsupported Stream version
- transactional delivery outbox

### Delivery

- stdout sink
- JSONL sink
- webhook sink
- at-least-once delivery
- retry scheduling and `Retry-After`
- dead-letter persistence and replay
- optional historical sink backfill
- event-version-aware webhook idempotency keys
- protection against an old in-flight delivery acknowledging a newer source mutation

### Operations

- config validation without source network calls
- local `doctor` diagnostics
- source health / checkpoint / delivery inspection
- atomic SQLite backup through SQLite's backup API
- read-only database integrity + schema verification
- persisted operational snapshots in table, JSON, and Prometheus exposition formats
- dependency-free structured JSON logging utilities
- deterministic CI on Python 3.11, 3.12, and 3.13
- opt-in live compatibility smoke workflow kept separate from PR correctness CI

## Reliability model

Collection uses **at-least-once collection + idempotent persistence**. Each accepted collector page and its next cursor are committed in one SQLite transaction. If the process dies before commit, the page may be collected again; if it dies after commit, the next process resumes from the committed cursor. Stable IDs and content fingerprints make replay safe.

Delivery uses a **transactional outbox + at-least-once delivery** model. New or source-visible changed signals enqueue delivery state in the same persistence boundary. A sink failure does not move the source checkpoint backward and does not require source recollection.

Webhook deliveries include version-specific idempotency information:

```text
Idempotency-Key: signalkit:<version-digest>
X-SignalKit-Event-ID: <stable-event-id>
X-SignalKit-Event-Hash: <content-fingerprint>
```

Consumers that perform non-idempotent side effects should honor the idempotency key.

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

Install:

```bash
python -m pip install -e .
```

Development dependencies:

```bash
python -m pip install -e ".[dev]"
```

## Long-running runtime

Create a configuration:

```bash
signalkit init
```

Example `signalkit.toml`:

```toml
[runtime]
database = "signals.db"
concurrency = 4
failure_threshold = 5
circuit_cooldown = 300
delivery_interval = 1
delivery_batch = 100
delivery_max_attempts = 8

[[sources]]
name = "hn-ask"
type = "hackernews"
interval = 60
limit = 50
feed = "askstories"
comments = 3

[[sources]]
name = "github-leads"
type = "github"
interval = 120
limit = 50
query = '"looking for" is:issue is:open'
comments = 3
token_env = "GITHUB_TOKEN"

[[sources]]
name = "reddit-saas"
type = "reddit"
interval = 60
limit = 100
subreddit = "SaaS"
listing = "new"
comments = 0
# Credentials are read from REDDIT_* environment variables.

[[sinks]]
name = "archive"
type = "jsonl"
path = "signals.jsonl"
backfill = false

[[sinks]]
name = "brain"
type = "webhook"
url = "https://example.com/signals"
token_env = "SIGNALKIT_WEBHOOK_TOKEN"
backfill = false
```

Validate local configuration and credential wiring without polling third-party sources:

```bash
signalkit validate signalkit.toml
signalkit doctor signalkit.toml
```

Run continuously:

```bash
signalkit run signalkit.toml
```

Run one collection/delivery cycle:

```bash
signalkit run signalkit.toml --once
```

`backfill = true` queues existing stored signals for a newly enabled sink as well as future new/changed events. Existing delivery records remain idempotent.

## One-shot collection

```bash
signalkit collect rss https://hnrss.org/newest --source hn-rss --limit 20
signalkit collect jsonfeed https://example.com/feed.json --limit 20
signalkit collect hn --feed askstories --limit 20 --comments 3
signalkit collect github '"looking for" is:issue is:open' --limit 50 --comments 3
```

Repeated persisted commands resume from the stored checkpoint. Use `--fresh` to ignore the checkpoint for one run while keeping writes idempotent.

Skip persistence and emit JSONL:

```bash
signalkit collect rss https://example.com/feed.xml --no-store --format jsonl \
  | your-consumer
```

## Operations

Inspect persisted events, checkpoints, runtime health, and delivery state:

```bash
signalkit show --limit 20
signalkit show --source github --kind issue --format jsonl
signalkit checkpoint hackernews:newstories
signalkit status
signalkit deliveries
signalkit deliveries --sink brain --format json
```

Replay dead letters:

```bash
signalkit retry-deliveries brain
```

Create and verify a consistent SQLite backup:

```bash
python -m signalkit_stream.maintenance backup signals.db backups/signals.db
python -m signalkit_stream.maintenance verify backups/signals.db
```

Read persisted operational state or Prometheus exposition text:

```bash
python -m signalkit_stream.observability signals.db
python -m signalkit_stream.observability signals.db --format json
python -m signalkit_stream.observability signals.db --format prometheus
```

See `docs/MIGRATIONS.md`, `docs/BACKUP.md`, and `docs/OBSERVABILITY.md` for production upgrade, restore, and monitoring guidance.

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
  "collected_at": "2026-07-26T04:00:00+00:00",
  "metadata": {
    "number": 42,
    "state": "open",
    "labels": ["feature"]
  }
}
```

Normalized kinds are `article`, `comment`, `issue`, `post`, `pull_request`, `story`, and `other`. Source-specific fields remain under `metadata`.

## Collector SDK

A collector owns one logical source instance and returns a resumable batch:

```python
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor


async def collect(
    *,
    context: CollectorContext | None = None,
    cursor: Cursor | None = None,
) -> CollectorResult:
    ...
```

The pipeline validates collector results before any event or checkpoint is committed. Third-party adapters must preserve source identity, stable IDs, timezone-aware timestamps, bounded pagination progress, and resumable cursors.

For a simple JSON GET endpoint, start with:

```python
from signalkit_stream.collectors.rest import GenericRESTCollector
```

For richer APIs, write a source-specific adapter on the shared collector SDK rather than forcing semantics into the generic mapper. See `docs/COLLECTOR_SDK.md` and `docs/GENERIC_REST.md`.

## Sink contract

A sink receives normalized events after persistence/outbox commit:

```python
from signalkit_stream.models import SignalEvent
from signalkit_stream.sinks import Sink


class MySink(Sink):
    key = "my-sink"

    async def send(self, event: SignalEvent) -> None:
        ...
```

Raise `SinkError` to describe retryability, status codes, or source-provided retry delays. Delivery state belongs to `DeliveryEngine`, not the sink implementation.

## Source support

| Source | Primary items | Comments | Incremental state | Auth |
| --- | --- | --- | --- | --- |
| RSS / Atom | entries | source/feed dependent | page offset + ETag / Last-Modified | none |
| JSON Feed 1.x | items | no thread semantics | item offset + `next_url` + seen IDs + validators | endpoint dependent |
| Hacker News | stories | bounded top-level comments | bounded seen-ID cursor | none |
| GitHub | issues / PRs | bounded issue / PR comments | page/offset + update watermark | optional token |
| Reddit | posts | bounded top-level comments | native `after` + seen-ID watermark | OAuth access / refresh / client credentials |
| Generic REST extension | mapped JSON objects | adapter dependent | none/page/cursor + bounded seen IDs | configurable header/env token |

Prefer official APIs and feeds where available. Respect authentication requirements, rate limits, robots rules where applicable, and each source's current terms.

## Development

```bash
make install
make check
```

Equivalent release checks:

```bash
ruff check .
pytest --cov=signalkit_stream --cov-report=term-missing --cov-fail-under=80
python -m compileall -q src
```

Normal PR CI is deterministic and does not depend on third-party uptime. Public API behavior is simulated with mocked HTTP transports. Live compatibility smoke checks are isolated from the normal correctness gate.

## Roadmap

The core ingestion, runtime, durable delivery, first-party source set, persistent schema lifecycle, diagnostics, backup tooling, and observability foundation are implemented. `docs/ROADMAP.md` tracks the remaining 1.0 hardening work rather than a second application layer.

## License

MIT
