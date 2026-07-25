# Collector SDK

SignalKit Stream adapters translate one upstream source into the shared `SignalEvent` and `Cursor` protocol. A custom collector should contain source-specific translation and authentication, not duplicate runtime persistence or scheduling.

## Minimal contract

Subclass `Collector` or `HTTPCollector` and implement:

```python
async def collect(
    *,
    context: CollectorContext | None = None,
    cursor: Cursor | None = None,
) -> CollectorResult:
    ...
```

The runtime validates every returned page before persistence. Contract violations are non-retryable `CollectorErrorKind.INTERNAL` errors, so a broken adapter cannot advance a checkpoint or write events.

Validated invariants include:

- `primary_count` is non-negative and does not exceed `context.limit`
- `has_more=True` has a continuation cursor
- returned cursors belong to the collector's `SourceIdentity`
- every event belongs to the same source instance as the collector
- event IDs are unique inside a batch
- event timestamps are timezone-aware
- rate-limit counters and retry delays are non-negative

Pagination-loop protection remains a runtime concern: `run_collector` stops if a collector says `has_more=True` but returns zero primary items or does not advance its cursor.

## Stable identity

Choose an immutable source-native object ID and use:

```python
SignalEvent.stable_id(
    source,
    external_id,
    kind,
    source_instance=instance,
)
```

Do not derive IDs from mutable titles, text, scores, or timestamps. Source mutations should keep the same event ID so SQLite can classify them as updates through the event fingerprint.

## Cursor rules

A cursor is opaque to the runtime but owned by exactly one source key. Store only the minimum continuation state required to resume collection, for example:

```json
{
  "page": 3,
  "after": "native-cursor",
  "seen_ids": ["newest", "older"]
}
```

Never put credentials or bearer tokens in a cursor. Cursors are persisted to SQLite and may be inspected through the CLI.

A page's events and next cursor are committed atomically. It is therefore safe for a restarted process to recollect the last uncommitted page; stable IDs make the write idempotent.

## HTTP adapters

Prefer `HTTPCollector.request()` instead of calling `httpx` directly. The shared request layer supplies:

- bounded retries for transient HTTP failures
- timeout and network error normalization
- `Retry-After` support
- common rate-limit snapshots
- consistent `CollectorError` categories

Override rate-limit parsing when an API has nonstandard semantics. Reddit is an example: its remaining value can be decimal-like and its reset header represents seconds until reset rather than a Unix timestamp.

## Source-specific parsing

Keep source-only fields under `SignalEvent.metadata`. Downstream consumers should be able to operate on `id`, `source`, `source_instance`, `kind`, `title`, `content`, `author`, `url`, and timestamps without importing an adapter.

If malformed upstream objects can be skipped without corrupting pagination, add a warning to `CollectorResult.warnings`. If the response shape prevents safe continuation, raise a non-retryable parse error instead of silently advancing the cursor.

## Testing an adapter

A first-party or production adapter should have deterministic tests for:

1. normal response normalization
2. stable IDs across recollection
3. pagination and cursor resume
4. duplicate/watermark behavior for polling sources
5. malformed response handling
6. authentication failures
7. 429/5xx/timeout behavior where applicable
8. source-specific rate-limit semantics
9. restart from a persisted checkpoint
10. collector contract validation

Normal CI must not require live credentials or external network access. Use `httpx.MockTransport` fixtures. Live compatibility checks should be opt-in and must not replace deterministic tests.

See `examples/json_api_collector.py` for a cursor-paginated REST reference and `examples/custom_collector.py` for a smaller adapter example.
