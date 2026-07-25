# SignalKit Stream

SignalKit Stream is the ingestion boundary for SignalKit: a reliable, pluggable service that continuously collects public signals from external sources, normalizes them into a versioned event contract, checkpoints progress, persists source mutations idempotently, and delivers events to downstream consumers through durable sinks.

The module deliberately stops at ingestion and delivery. Intent classification, lead scoring, enrichment, CRM sync, outreach, and agent actions belong downstream.

## Architecture

```text
RSS / Hacker News / GitHub / future adapters
                    ↓
               Collectors
                    ↓
        CollectorResult + Cursor
                    ↓
              StreamRuntime
     retry / pagination / scheduling
      rate limits / source health
                    ↓
         SignalEvent schema v1
                    ↓
       SQLite transaction boundary
        events + checkpoints + outbox
                    ↓
              DeliveryEngine
          retry / dead letter / replay
                    ↓
      stdout / JSONL / webhook / future sinks
```

The stable boundary is `SignalEvent`. Downstream consumers should not need source-specific parsing logic.

## Implemented foundation

SignalKit Stream currently includes:

- versioned `SignalEvent` schema, deterministic source-object IDs, and mutation fingerprints
- `SourceIdentity`, `Cursor`, `CollectorContext`, `CollectorResult`, and stable collector errors
- pluggable async collector SDK
- shared HTTP timeout, retry/backoff, `Retry-After`, and rate-limit inspection
- incremental/resumable collection with pagination-loop protection
- RSS / Atom, Hacker News, and GitHub adapters
- strict TOML runtime configuration and collector registry
- long-running independent source workers with bounded global concurrency
- source failure backoff, circuit-open cooldown, rate-limit-aware scheduling, and graceful shutdown
- persisted source health across restarts
- SQLite event store, legacy schema migration, and atomic event/checkpoint writes
- transactional delivery outbox
- stdout, JSONL, and webhook sinks
- independent sink retries, dead letters, replay, and optional historical backfill
- event-version-aware webhook idempotency keys and in-flight mutation protection
- CLI lifecycle, health, checkpoint, and delivery operations
- deterministic offline tests and CI on Python 3.11, 3.12, and 3.13

This is not a throwaway MVP. The current public contracts are the foundation for the remaining adapter and operations work tracked in `docs/ROADMAP.md`.

## Reliability model

Collection uses **at-least-once collection + idempotent persistence**. Each collector page and its next cursor are committed in one SQLite transaction. If the process dies before commit, the page can be collected again. If it dies after commit, the next run resumes from the stored cursor. Stable event IDs and content fingerprints make replay safe.

Delivery uses a **transactional outbox + at-least-once delivery** model. When a new or changed signal is persisted, SQLite creates delivery records for enabled sinks in the same transaction. A sink failure does not move the source checkpoint backward and does not require recollecting the source. Retryable failures are scheduled with backoff; permanent or exhausted failures become dead letters and can be replayed.

Webhook deliveries include stable identifiers for the exact source-object version being sent:

```text
Idempotency-Key: signalkit:<version-digest>
X-SignalKit-Event-ID: <stable-event-id>
X-SignalKit-Event-Hash: <content-fingerprint>
```

The idempotency key is derived from the sink key, stable event ID, and event fingerprint. Retries of the same version therefore share a key, while a legitimate source update gets a new key. Consumers should honor the idempotency key if their side effects are not naturally idempotent.

If a source object changes while an older payload is in flight, SignalKit Stream does not let success for the old payload overwrite the newer pending outbox state. The new version remains pending for a later delivery pass.

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

Run continuously:

```bash
signalkit run signalkit.toml
```

Run one collection/delivery cycle:

```bash
signalkit run signalkit.toml --once
```

`backfill = true` queues existing stored signals for a sink as well as future new/changed events. Existing delivery records are preserved, so restarting with backfill enabled does not requeue already delivered rows.

## One-shot collection

The same collectors can be used without the scheduler.

```bash
signalkit collect rss https://hnrss.org/newest --source hn-rss --limit 20
signalkit collect hn --feed askstories --limit 20 --comments 3
signalkit collect github '"looking for" is:issue is:open' --limit 50 --comments 3
```

Repeated persisted commands resume from the stored checkpoint. Use `--fresh` to ignore the checkpoint for one run while keeping writes idempotent.

Skip persistence and emit JSONL:

```bash
signalkit collect rss https://example.com/feed.xml --no-store --format jsonl \
  | your-consumer
```

## Operations CLI

Inspect events and source progress:

```bash
signalkit show --limit 20
signalkit show --source github --kind issue --format jsonl
signalkit checkpoint hackernews:newstories
signalkit status
```

Inspect delivery state:

```bash
signalkit deliveries
signalkit deliveries --sink brain --format json
```

Replay dead letters for one sink:

```bash
signalkit retry-deliveries brain
```

The next delivery worker cycle will attempt the replayed rows again.

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

`CollectorResult` carries normalized events, the next cursor, whether more data is immediately available, primary-item count, warnings, and source rate-limit information.

## Sink contract

A sink receives normalized events after they have been committed to the event store and outbox:

```python
from signalkit_stream.models import SignalEvent
from signalkit_stream.sinks import Sink


class MySink(Sink):
    key = "my-sink"

    async def send(self, event: SignalEvent) -> None:
        ...
```

Raise `SinkError` to describe retryability, status codes, or a source-provided retry delay. Delivery state is managed by `DeliveryEngine`, not by the sink implementation.

## Python API

```python
import asyncio

from signalkit_stream.config import load_config
from signalkit_stream.runtime import StreamRuntime
from signalkit_stream.storage import SQLiteSignalStore


async def main() -> None:
    config = load_config("signalkit.toml")
    with SQLiteSignalStore(config.runtime.database) as store:
        runtime = StreamRuntime(config, store)
        await runtime.run_forever()


asyncio.run(main())
```

## Development

```bash
make install
make check
```

Equivalent checks:

```bash
ruff check .
pytest --cov=signalkit_stream --cov-report=term-missing --cov-fail-under=80
python -m compileall -q src
```

The normal test suite does not depend on third-party network availability. Public API behavior is simulated with mocked HTTP transports; live compatibility tests remain separate from the deterministic release gate.

## Source support

| Source | Primary items | Comments | Incremental state | Auth |
| --- | --- | --- | --- | --- |
| RSS / Atom | entries | feed-dependent | ETag / Last-Modified + cursor | no |
| Hacker News | stories | top-level comments | bounded seen-ID cursor | no |
| GitHub | issues / PRs | issue / PR comments | page/offset + update watermark | optional token |
| Reddit | planned first-party adapter | planned | planned | app credentials |
| JSON Feed / generic REST | planned extension path | adapter-dependent | planned | adapter-dependent |

Prefer official APIs and feeds where available, and respect authentication, rate limits, robots rules, and source terms.

## Roadmap

`docs/ROADMAP.md` contains the remaining work and release gates. Runtime scheduling and durable sink delivery are now implemented; the next major work is first-party adapter completion, operational diagnostics/metrics, explicit migration tooling, and restart/live compatibility test suites.

## License

MIT
