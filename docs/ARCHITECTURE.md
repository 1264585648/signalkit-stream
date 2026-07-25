# SignalKit Stream architecture

SignalKit Stream owns the external-source ingestion boundary: fetching, source authentication, pagination, checkpoints, normalization, retries, persistence, and delivery. It does not decide whether a signal is commercially interesting; classification, enrichment, scoring, outreach, and agent actions belong downstream.

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

The event fingerprint hashes source-visible content and metadata but intentionally excludes `collected_at`. That lets persistence distinguish three cases:

- a new source object: insert
- the same source object with unchanged content: no-op
- the same source object with changed source-visible content: update

## Persistence and checkpoint transaction

For persisted collection, one collector page and its next cursor are committed in a single SQLite transaction:

```text
fetch page
    ↓
normalize
    ↓
BEGIN
  insert / update events
  save next cursor
COMMIT
```

If the process crashes before commit, the page may be fetched again. If it crashes after commit, the next run resumes from the committed cursor. Stable event IDs and idempotent upserts therefore give SignalKit Stream an **at-least-once collection + idempotent persistence** reliability model.

This model is intentionally simpler and more robust for public-web ingestion than pretending external APIs can provide true end-to-end exactly-once delivery.

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

The runtime stops a pagination loop when a collector reports `has_more=True` but emits zero primary items, or when its cursor does not advance. A hard `max_pages` guard provides an additional safety boundary.

A collector that cannot guarantee forward progress must return `has_more=False` and wait for a future polling cycle.

## Extension rule

A new adapter should be mostly translation code:

```text
source API / feed
       ↓
source-native response
       ↓
SignalEvent + Cursor
```

Authentication, HTTP retry behavior, persistence, checkpointing, and downstream delivery belong to shared infrastructure rather than each adapter.
