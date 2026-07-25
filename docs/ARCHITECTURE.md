# SignalKit Stream architecture

SignalKit Stream owns the external-source ingestion boundary: fetching, source authentication, pagination, checkpoints, normalization, retries, persistence, scheduling, and durable delivery. It does not decide whether a signal is commercially interesting; classification, enrichment, scoring, outreach, and agent actions belong downstream.

## Process boundary

```text
source configs
     ↓
StreamRuntime
 ├─ source workers ──> collectors ──> SignalEvent
 │                                  ↓
 │                         SQLite transaction
 │                    event + cursor + delivery outbox
 │                                  ↓
 └─ delivery workers ─────────────> sinks
```

Source workers and sink workers fail independently. A downstream outage therefore does not force source recollection, and one unhealthy source does not stop other sources.

## Core protocol

Every collector has a stable `SourceIdentity`, receives a `CollectorContext` plus an optional `Cursor`, and returns a `CollectorResult`.

The protocol has six invariants:

1. Every emitted item is a valid, source-agnostic `SignalEvent`.
2. A returned cursor belongs to the collector's own source key.
3. `primary_count` counts primary source items, not attached comments or derived events.
4. `has_more=True` means an immediate next invocation can make progress from the returned cursor.
5. Downstream consumers never need source-specific parsing logic.
6. Source-specific fields stay under `metadata`.

## Event identity and mutation

A `SignalEvent.id` is deterministic from source, source instance, event kind, and an immutable source-native external ID. Recollecting the same source object produces the same event ID.

The event fingerprint hashes source-visible content and metadata but intentionally excludes `collected_at`. Persistence therefore distinguishes:

- a new source object: insert
- the same source object with unchanged content: no-op
- the same source object with changed source-visible content: update

An update keeps the stable event ID and requeues delivery for enabled sinks because consumers may need the new source-visible state.

## Collection transaction

For persisted collection, one collector page, its next cursor, and delivery-outbox rows are committed in a single SQLite transaction:

```text
fetch page
    ↓
normalize
    ↓
BEGIN
  insert / update events
  database trigger queues enabled sink deliveries
  save next cursor
COMMIT
```

If the process crashes before commit, the page may be fetched again. If it crashes after commit, the next run resumes from the committed cursor. Stable event IDs and idempotent upserts give collection an **at-least-once collection + idempotent persistence** model.

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

Delivery is also at-least-once: a process can terminate after a remote consumer accepted a message but before the local row is marked delivered. Webhook sinks therefore emit a stable `Idempotency-Key` based on sink key plus event ID. Non-idempotent consumers should honor it.

Multiple configured sinks provide fan-out naturally because each `(sink_key, event_id)` row has independent attempts and failure state.

## Runtime scheduling

`StreamRuntime` creates one worker per enabled source and a delivery worker per enabled sink. Global source concurrency is bounded. Each source has its own interval and persisted health state.

Successful source runs wait at least their configured interval, extended when the source reports an exhausted rate limit with a reset time. Failures use exponential delay until a configurable failure threshold opens a cooldown circuit. A source failure is recorded without terminating healthy workers.

SIGINT/SIGTERM causes worker cancellation after already committed SQLite transactions remain durable. Uncommitted work is intentionally safe to replay on restart.

## HTTP policy

`HTTPCollector` centralizes network behavior so adapters remain thin. It owns:

- request timeout handling
- network-error normalization
- bounded exponential backoff with jitter
- `Retry-After` handling
- retry policy for 408, 425, 429, and transient 5xx responses
- rate-limit header inspection
- stable `CollectorError` classification

Adapters should not duplicate retry loops or invent source-specific exception types when the shared contract can represent the failure.

## Pagination safety

The pipeline stops a pagination loop when a collector reports `has_more=True` but emits zero primary items, or when its cursor does not advance. A hard `max_pages` guard provides another safety boundary.

A collector that cannot guarantee forward progress must return `has_more=False` and wait for a future polling cycle.

## Extension rule

A new source adapter should mostly be translation code:

```text
source API / feed
       ↓
source-native response
       ↓
SignalEvent + Cursor
```

A new sink should mostly be delivery code:

```text
SignalEvent
    ↓
remote destination
```

Authentication, source HTTP retry behavior, persistence, checkpointing, outbox state, delivery retry scheduling, and runtime lifecycle belong to shared infrastructure rather than individual adapters or sinks.
