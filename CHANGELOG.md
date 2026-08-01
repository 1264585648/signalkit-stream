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
- bounded active-thread comment refresh for Hacker News and Reddit
- mutation-safe RSS/Atom pagination anchors
- per-collector HTTP request backpressure and deadline-aware retry refusal

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
- GitHub incremental search uses an inclusive second-level watermark overlap to prevent boundary gaps
- GitHub comment collection selects the latest bounded window and reports incomplete search results
- Hacker News and Reddit revisit recent threads after catching up so later comments are not permanently missed
- RSS/Atom collection restarts a changed partial feed snapshot instead of blindly advancing an unsafe offset
- standard and source-specific rate-limit reset headers are normalized without confusing relative seconds and epoch timestamps

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
- collection freshness, bounded replay, and active-thread refresh in `docs/COLLECTION_RELIABILITY.md`
- remaining 1.0 work in `docs/ROADMAP.md`

## Release-note policy

Before cutting a release, move the relevant `Unreleased` entries under the release version/date and add any operator-visible upgrade instructions. Persistent schema changes must explicitly identify their migration version and backup requirements.
