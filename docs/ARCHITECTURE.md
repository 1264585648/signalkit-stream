# SignalKit Stream architecture

SignalKit Stream owns the external-source ingestion boundary: fetching, source authentication, pagination, checkpoints, normalization, retries, persistence, scheduling, and durable delivery. It does not decide whether a signal is commercially interesting; classification, enrichment, scoring, outreach, CRM synchronization, and agent actions belong downstream.

## Process boundary

```text
RSS / JSON Feed / Hacker News / GitHub / Reddit / explicit REST extensions
                              |
                          collectors
                              |
                   CollectorResult + Cursor
                              |
                  contract validation boundary
                              |
                         run_collector
                              |
                 SQLite transaction boundary
           event + cursor + source-visible outbox mutation
                              |
                         StreamRuntime
                    source health / scheduling
                              |
                         DeliveryEngine
                              |
                   stdout / JSONL / webhook
                              |
                    downstream consumers
```

Source workers and sink workers fail independently. A downstream outage therefore does not force source recollection, and one unhealthy source does not stop other sources.

## Core protocol

Every collector has a stable `SourceIdentity`, receives a `CollectorContext` plus an optional `Cursor`, and returns a `CollectorResult`.

Before any persistence/checkpoint advancement, Stream validates common collector invariants:

1. Every emitted item is a valid, source-agnostic `SignalEvent` belonging to the collector source identity.
2. Event IDs within one result are unique.
3. A returned cursor belongs to the collector's own source key.
4. `primary_count` counts primary source items, not attached comments or derived events, and stays within the requested context limit.
5. `has_more=True` has a resumable continuation boundary and the pipeline additionally requires progress.
6. Event timestamps are timezone-aware and rate-limit values are structurally valid.
7. Downstream consumers never need source-specific parsing logic; source-specific fields stay under `metadata`.

A contract violation fails before event writes or cursor advancement.

## Event identity and mutation

A `SignalEvent.id` is deterministic from source, source instance, event kind, and an immutable source-native external ID. Recollecting the same source object produces the same event ID.

The event fingerprint hashes source-visible content and metadata but intentionally excludes `collected_at`. Persistence therefore distinguishes:

- a new source object: insert
- the same source object with unchanged content: no-op
- the same source object with changed source-visible content: update

An update keeps the stable event ID and requeues delivery for enabled sinks because consumers may need the new source-visible state.

Identity compatibility has two layers:

```text
event.id       stable source-object identity
fingerprint    exact source-visible version identity
```

This distinction is why webhook idempotency keys include the fingerprint rather than only the event ID.

## Collection transaction

For persisted collection, one accepted collector page, its next cursor, and source-visible delivery-outbox changes are committed in one SQLite transaction:

```text
fetch page
    ↓
normalize
    ↓
validate collector result
    ↓
BEGIN
  insert / update events
  database trigger queues enabled sink deliveries
  save next cursor
COMMIT
```

If the process crashes before commit, the page may be fetched again. If it crashes after commit, the next run resumes from the committed cursor. Stable event IDs and fingerprints give collection an **at-least-once collection + idempotent persistence** model.

A source checkpoint never advances merely because a fetch happened; it advances only with the accepted persistence transaction.

## Durable delivery

Each enabled sink has an independent delivery record for each new or changed event. The record is separate from the source checkpoint.

```text
event committed
     ↓
pending delivery
     ↓
sink.send(event)
  ├─ success ─────> delivered
  ├─ retryable ───> failed + next_attempt_at
  └─ permanent / exhausted ──> dead
```

Dead rows can be replayed without recollecting the source. Optional sink backfill creates missing pending rows for events already in the store.

Delivery is at-least-once: a process can terminate after a remote consumer accepted a message but before the local row is marked delivered. Webhook sinks therefore emit a stable idempotency key for the **exact event version**, derived from sink key, stable event ID, and event fingerprint. Retries of one version share a key; a later source mutation produces a different key.

A source mutation can also happen while an older payload is in flight. The database update trigger resets the delivery row to pending. After the sink call returns, `DeliveryEngine` compares the version it sent with the current stored fingerprint; success for an older payload is treated as superseded and never overwrites the newer pending state.

Subprocess lifecycle tests exercise the same semantics through the real CLI: an abrupt process kill after a webhook side effect but before local acknowledgement leaves the delivery pending, and restart replays the same event version with the same idempotency key.

Multiple configured sinks provide fan-out naturally because each `(sink_key, event_id)` row has independent attempts and failure state.

## Runtime scheduling

`StreamRuntime` creates one worker per enabled source and delivery workers for configured sinks. Global source concurrency is bounded. Each source has its own interval and persisted health state.

Successful source runs wait at least their configured interval, extended when the source reports an exhausted rate limit with a reset time. Failures use exponential delay until a configurable failure threshold opens a cooldown circuit. A source failure is recorded without terminating healthy workers.

SIGINT/SIGTERM causes worker cancellation after already committed SQLite transactions remain durable. Uncommitted work is intentionally safe to replay on restart. On Windows the catchable equivalent is `CTRL_BREAK_EVENT`, delivered as `SIGBREAK`; `TerminateProcess` (what `Popen.terminate()` calls) cannot be intercepted by any handler, so a supervisor there must send `CTRL_BREAK_EVENT` to stop the runtime cleanly. See `docs/OPERATIONS.md`.

A worker that ends on its own is treated as a defect rather than normal operation: every source iteration is individually guarded, so the supervisor logs the exit, stops the remaining workers, and re-raises instead of leaving a process that looks healthy while collecting nothing. Restart policy belongs to the process supervisor.

## HTTP policy

`HTTPCollector` centralizes network behavior so adapters remain thin. It owns:

- request timeout handling
- network-error normalization
- bounded exponential backoff with jitter
- `Retry-After` handling
- retry policy for 408, 425, 429, and transient 5xx responses
- rate-limit header inspection
- stable `CollectorError` classification
- one pooled `httpx.AsyncClient` per collector instance, released by `aclose()`
- a response byte cap (`max_response_bytes`) enforced while streaming
- redirect policy, including the cross-origin credential rule below

Adapters should not duplicate retry loops or invent source-specific exception types when the shared contract can represent the failure.

Client construction is expensive and synchronous — it builds a fresh TLS context and CA bundle — so a client is created lazily once per collector and reused for the instance lifetime. `StreamRuntime` closes them after its workers are joined; embedders driving `run_once` directly should call `StreamRuntime.aclose()`.

Redirects are resolved explicitly rather than delegated to the HTTP client, because a client only strips `Authorization` and `Cookie` on a cross-origin hop while an operator-configured auth header keeps travelling. `cross_origin_redirects` selects the policy:

```text
never      refuse every cross-origin hop
anonymous  follow only a request that carries nothing to leak (default)
always     follow, keeping only safe headers
```

Same-origin hops and same-host `http`→`https` upgrades always follow with headers intact. A credentialed cross-origin hop is refused with an actionable error rather than silently stripped, so a redirect can never launder a secret to another host.

Source-specific authentication semantics remain in the source adapter. For example, Reddit owns OAuth access/refresh/app credentials and a single API-401 re-authentication attempt while still using shared HTTP error/retry behavior.

## Pagination safety

The pipeline stops a pagination loop when a collector reports `has_more=True` but emits zero primary items, or when its cursor does not advance. A hard `max_pages` guard provides another safety boundary.

A continuation URL supplied by the remote source is untrusted input. JSON Feed's `next_url` is resolved against the configured feed URL and must land on the same origin, both when it arrives and when it is restored from a checkpoint, so a feed operator cannot point collection at an unrelated host or persist such a target. `max_page_follows` caps remote-directed hops per cycle independently of `max_pages`.

A collector that cannot guarantee forward progress must return `has_more=False` and wait for a future polling cycle.

## Persistence compatibility

Normalized event compatibility and SQLite layout compatibility are independent:

```text
SignalEvent.schema_version  -> downstream event contract
PRAGMA user_version         -> persistent SQLite layout
```

`DATABASE_SCHEMA_VERSION` controls persistent layout startup:

```text
older schema -> atomic forward migration
current      -> validate required objects and run
future       -> fail closed without mutation
```

There is no automatic downgrade path. A failed migration rolls back schema changes and the version marker together.

## SQLite concurrency model

The supported deployment model is **one Stream writer per SQLite database**.

SQLite can serve concurrent readers, and WAL mode can improve reader/writer coexistence, but write transactions still serialize. Stream therefore exposes:

- a configurable SQLite busy timeout for embedding;
- a non-mutating `BEGIN IMMEDIATE`/rollback write-lock probe in `doctor`;
- SQLite-aware backup through the backup API rather than raw live-file copying.

Lock timeouts do not partially commit the failed application transaction. WAL backup tests prove backups observe the last committed snapshot while another writer transaction is active.

## Extension rule

A new source adapter should mostly be translation code:

```text
source API / feed
       ↓
source-native response
       ↓
SignalEvent + Cursor
```

Use a dedicated collector when native OAuth, cursor pagination, update/delete semantics, thread traversal, specialized rate limits, or ordering guarantees matter.

`GenericRESTCollector` is only for explicitly understood JSON GET/list endpoints and intentionally remains outside the default registry; arbitrary APIs do not share one safe guessed semantic contract.

A new sink should mostly be delivery code:

```text
SignalEvent
    ↓
remote destination
```

Authentication, source HTTP retry behavior, persistence, checkpointing, outbox state, delivery retry scheduling, and runtime lifecycle belong to shared infrastructure rather than individual adapters or sinks.

## Operations boundary

Operator/readiness surfaces read the same durable state:

- `signalkit validate`
- `signalkit doctor`
- `signalkit status --verbose`
- `signalkit status --format prometheus`
- `signalkit deliveries`
- `signalkit db backup`
- `signalkit db verify`

Live third-party compatibility probes are separate from deterministic correctness CI.

## Non-goals

The Stream repository does not include:

- LLM intent classification
- lead scoring/ranking
- contact/company enrichment
- CRM synchronization
- outreach generation/sending
- autonomous agent decision loops

Those systems consume normalized committed events downstream and can evolve without changing source-ingestion reliability semantics.
