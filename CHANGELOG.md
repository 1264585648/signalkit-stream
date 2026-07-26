# Changelog

All notable SignalKit Stream changes are recorded here while the project closes the 1.0 release gate.

The project follows a forward-only persistent-database policy. `SignalEvent.schema_version` and SQLite `PRAGMA user_version` are independent compatibility boundaries; see `docs/COMPATIBILITY.md` and `docs/MIGRATIONS.md`.

## Unreleased

### Added

- first-party RSS / Atom, JSON Feed 1.x, Hacker News, GitHub, and Reddit OAuth collectors
- explicit generic JSON REST extension collector for mapped GET/list APIs
- normalized, versioned `SignalEvent` contract with stable IDs and mutation fingerprints
- resumable cursors and atomic event/checkpoint persistence
- long-running multi-source runtime with bounded concurrency, source health, backoff, circuit cooldown, and graceful signal handling
- transactional delivery outbox with stdout, JSONL, and webhook sinks
- version-aware webhook idempotency headers, retry scheduling, dead letters, replay, and sink backfill
- SQLite persistent schema versioning through `PRAGMA user_version`, atomic forward migrations, and future-version refusal
- `signalkit validate` and `signalkit doctor`
- atomic SQLite backup and read-only verification through `signalkit db backup` / `signalkit db verify`
- persisted source/sink/schema observability in table, JSON, and Prometheus formats
- structured text/JSON runtime logging
- Reddit static access-token, refresh-token, and app-only OAuth credential modes with one 401 re-authentication attempt
- scheduled/manual live compatibility smoke checks separate from deterministic PR CI
- SQLite lock/busy diagnostics, configurable embedded-store timeout, and WAL backup behavior tests
- subprocess lifecycle tests covering graceful SIGTERM restart and abrupt death during an in-flight webhook followed by idempotent replay
- clean wheel and source-distribution installation smoke checks in CI

### Reliability and compatibility

- collector results are validated before any event or checkpoint can be committed
- event/checkpoint/outbox writes are covered by deterministic rollback/restart tests
- source failures are isolated from healthy source workers
- sink failures are isolated through independent durable delivery rows
- an old in-flight delivery cannot acknowledge a newer source mutation
- databases produced by a newer unsupported Stream schema fail closed instead of being modified
- failed backup replacement leaves the previous verified backup intact
- SQLite lock timeouts do not partially persist application writes
- remote webhook side effects can be replayed after abrupt process death with the same version-specific idempotency key

### Security

- JSON Feed `next_url` is validated against the configured feed origin before it is
  requested or persisted, and a poisoned checkpoint is rejected on restore. A feed operator
  could previously redirect collection at arbitrary hosts, including cloud
  instance-metadata endpoints, for up to `max_pages` requests per cycle
- `next_url` follows per cycle are additionally capped by `max_page_follows` (default 20)
- responses are streamed against an 8 MiB cap (`max_response_bytes`) and an oversized
  `Content-Length` is refused before the body is read; an unbounded feed could previously
  exhaust memory
- redirects are no longer followed blindly. Cross-origin hops honour
  `cross_origin_redirects` (`never` / `anonymous` / `always`, default `anonymous`): a hop
  that would carry credentials is refused rather than stripped. `httpx` strips only
  `Authorization` and `Cookie`, so an operator-configured `token_header` such as
  `X-Api-Key` previously survived a redirect to an attacker-controlled host
- feed URLs are redacted (userinfo, query, fragment) before being used as the default
  `source_instance`, in `metadata.feed_url`, in event-URL fallbacks, and in parse-error
  messages. A tokenised private-feed URL was previously written to every stored event and
  POSTed to third-party webhook sinks
- HTTP error messages keep parameter names but redact values, so a failing `?api_key=…`
  request no longer parks the secret in `checkpoints.last_error`,
  `source_health.last_error`, the log stream, or `signalkit status` output
- RSS/Atom bodies declaring a DTD or entity are refused before parsing, instead of relying
  on the ambient expat amplification cap that `requires-python = ">=3.11"` cannot guarantee
- `SignalEvent.url` is restricted to `http`/`https`, and oversized `title`, `content`,
  `author` and `metadata` are truncated with a `metadata["truncated"]` marker

### Fixed

- **Windows had no graceful shutdown at all.** `loop.add_signal_handler` is POSIX-only and
  the resulting `NotImplementedError` was swallowed, so `signalkit run` could not be stopped
  cleanly by any means, including Ctrl+C, and delivery/collector teardown never ran.
  Handlers now fall back to `signal.signal` with a thread-safe loop wake and support
  `SIGBREAK`, so a supervisor can stop the runtime with `CTRL_BREAK_EVENT`
- source and delivery workers no longer die silently. Every loop iteration is guarded and
  logged, and the supervisor notices a worker that ends on its own instead of leaving a
  process that looks healthy while collecting nothing
- a rate-limited source honours `Retry-After` even after its circuit opens, and a `429`
  without rate-limit headers no longer retries faster than the configured poll interval
- the event batch is one atomic `BEGIN IMMEDIATE` transaction with an upsert, so a
  concurrent writer can no longer cost the whole page and its checkpoint an
  `IntegrityError` rollback
- delivery timestamps are normalized to UTC before being stored and compared. A
  non-UTC-offset `next_attempt_at` previously sorted as a string and could stall a due
  delivery for hours
- re-enabling a disabled sink backfills the delivery rows missed while it was disabled,
  closing an at-least-once hole
- sinks removed from the configuration are disabled in the database at startup, so their
  triggers stop queueing `pending` rows that no worker drains
- `record_failure` failing inside a pipeline error handler no longer replaces the original
  collector error or loses the failure record
- GitHub and Hacker News parse failures are classified as `CollectorError` instead of
  escaping as raw `JSONDecodeError` / `ValueError` / `KeyError`, which were reported as
  `INTERNAL` — the code reserved for broken adapters — and counted toward the circuit breaker
- `file:` URIs for SQLite are percent-encoded with the correct slash count, fixing
  `db verify`, `db backup`, `doctor`, `status --verbose` and the write-lock probe for
  database paths containing `%` and for UNC network shares
- backup publication retries briefly when a concurrent reader holds the destination open,
  which `os.replace` cannot overwrite on Windows
- the `User-Agent` reports the installed version instead of a hardcoded `0.2`

### Performance

- `HTTPCollector` reuses one pooled `httpx.AsyncClient` per instance instead of building a
  fresh client per request. Construction cost ~768 ms of synchronous, event-loop-blocking
  work (a fresh certifi CA bundle per instance); steady-state RSS polls went
  **748 ms → 6.4 ms** locally. `StreamRuntime` releases the pools on shutdown, and
  `StreamRuntime.aclose()` covers one-shot and embedded callers
- the store connection enables `journal_mode=WAL`, `synchronous=NORMAL` and
  `foreign_keys=ON`. 200 delivery acknowledgements went **529 ms → 10 ms**, and read-only
  `status` / `doctor` snapshots no longer block behind a writer
- event batches pre-read existing hashes in chunks and apply one `executemany` upsert
  instead of a `SELECT` plus single-row write per event
- comment fetches fan out under a bounded semaphore (`comment_concurrency`, default 6)
  instead of one strictly sequential round trip per item while holding a runtime
  concurrency slot
- new store methods back a batched delivery drain: a joined ready-query, a hash-only
  supersede check, a single-transaction outcome applier, and optimistic-concurrency
  `expected_updated_at` guards

### Changed

- **Breaking (event identity):** for sources whose configured URL carries a query string or
  userinfo, the redaction above changes `source_instance` and therefore every
  `SignalEvent.id` derived from it. The next collection re-inserts those items once as new.
  RSS, JSON Feed and the generic REST collector are affected; sources without a query string
  are unaffected. Operators who need the previous IDs can pin them by passing the full URL
  explicitly as `instance`
- `seen_window` has one contract across every collector: values below 50 are rejected rather
  than silently clamped by some collectors and rejected by others. The generic REST
  collector's previous floor of 100 is lowered to 50 to match
- `HTTPCollector.request` no longer accepts `allow_statuses`. It never had any effect — the
  shared implementation already returns every response below 400
- `SQLiteSignalStore.get` raises for a stored event whose URL scheme is not allowed, rather
  than returning it

### Internal

- `_storage_impl.py`, `_diagnostics_impl.py`, `collectors/_reddit_impl.py` and the
  `collectors/base.py` shim class are merged into their public modules. The dead copies
  included a third, drifting definition of the entire SQLite schema and a Reddit credential
  contract that no longer matched the live one. `migrations.py` is now the single schema
  definition
- CI runs the test suite on `windows-latest` as well as `ubuntu-latest`; the shutdown defect
  above was invisible to a Linux-only matrix

### Documentation

- architecture and current source support in `README.md`
- collector authoring contract in `docs/COLLECTOR_SDK.md`
- generic REST extension guidance in `docs/GENERIC_REST.md`
- Reddit OAuth/policy boundary in `docs/REDDIT.md`
- database migration policy in `docs/MIGRATIONS.md`
- backup/restore runbook in `docs/BACKUP.md`
- monitoring/structured logging in `docs/OBSERVABILITY.md`
- process/SQLite/delivery operations in `docs/OPERATIONS.md`
- live compatibility testing in `docs/LIVE_TESTING.md`
- remaining 1.0 work in `docs/ROADMAP.md`

## Release-note policy

Before cutting a release, move the relevant `Unreleased` entries under the release version/date and add any operator-visible upgrade instructions. Persistent schema changes must explicitly identify their migration version and backup requirements.
