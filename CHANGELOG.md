# Changelog

All notable SignalKit Stream changes are recorded here.

The project follows a forward-only persistent-database policy. `SignalEvent.schema_version` and SQLite `PRAGMA user_version` are independent compatibility boundaries; see `docs/COMPATIBILITY.md` and `docs/MIGRATIONS.md`.

## Unreleased

No changes yet after the 1.0.0rc1 release candidate.

## 1.0.0rc1 - 2026-07-26

This release candidate marks the completed Stream foundation for 1.0 review. It is intended for installation, deployment, upgrade, lifecycle, and compatibility validation before the final `1.0.0` tag.

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
- main operator CLI database backup/verification through `signalkit db backup` / `signalkit db verify`
- `signalkit status --verbose` plus JSON and Prometheus operational output
- atomic SQLite backup and read-only verification APIs
- persisted source/sink/schema observability in table, JSON, and Prometheus formats
- structured text/JSON runtime logging
- Reddit static access-token, refresh-token, and app-only OAuth credential modes with one 401 re-authentication attempt
- scheduled/manual live compatibility smoke checks separate from deterministic PR CI
- SQLite lock/busy diagnostics, configurable embedded-store timeout, and WAL backup behavior tests
- subprocess lifecycle tests covering graceful SIGTERM restart and abrupt death during an in-flight webhook followed by idempotent replay
- clean wheel and source-distribution build/install smoke checks in CI
- explicit intended 1.0 public Python/CLI compatibility inventory and required-subset tests

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
- the supported SQLite deployment model is one Stream writer per database, with concurrent readers/SQLite-aware backup where appropriate
- documented public Python/CLI/automation surfaces are guarded as an additive required subset for the 1.0 line

### Upgrade notes from 0.7.x

- package version becomes `1.0.0rc1`
- persistent SQLite schema remains `DATABASE_SCHEMA_VERSION = 1`
- `SignalEvent.schema_version` remains `1`
- an existing healthy schema-version-1 database requires no SQLite layout migration for this release candidate; startup validates the current schema and continues
- legacy/unversioned supported Stream databases still migrate forward to schema version 1 atomically on startup
- databases with a future/unsupported `PRAGMA user_version` continue to fail closed without mutation
- configuration remains strict; validate the exact deployment configuration before restart
- Reddit one-shot/runtime authentication supports the same environment-backed static access-token, refresh-token, and app-only credential modes

Before deploying the release candidate to an existing environment, use the operator checks:

```bash
signalkit validate signalkit.toml
signalkit doctor signalkit.toml
signalkit db verify --db signals.db
signalkit db backup backups/pre-1.0.0rc1.db --db signals.db
signalkit db verify --db backups/pre-1.0.0rc1.db
```

Stop the old Stream writer before switching application versions when applying an upgrade boundary. Keep the verified pre-upgrade backup through the intended observation window.

### Documentation

- current architecture and source support in `README.md`
- architecture and persistence/delivery invariants in `docs/ARCHITECTURE.md`
- intended 1.0 public API inventory in `docs/PUBLIC_API.md`
- compatibility/deprecation policy in `docs/COMPATIBILITY.md`
- release/upgrade checklist in `docs/RELEASE.md`
- collector authoring contract in `docs/COLLECTOR_SDK.md`
- generic REST extension guidance in `docs/GENERIC_REST.md`
- Reddit OAuth/policy boundary in `docs/REDDIT.md`
- database migration policy in `docs/MIGRATIONS.md`
- backup/restore runbook in `docs/BACKUP.md`
- monitoring/structured logging in `docs/OBSERVABILITY.md`
- process/SQLite/delivery operations in `docs/OPERATIONS.md`
- live compatibility testing in `docs/LIVE_TESTING.md`
- 1.0 closure state in `docs/ROADMAP.md`

### Release-candidate status

`1.0.0rc1` is a release candidate, not the final stable tag. The final `1.0.0` release should be cut only after the exact candidate lineage passes the deterministic package gates, public API review, upgrade checks, and the explicit maintainer release checklist. Package publication and Git tagging are not performed by ordinary CI.

## Release-note policy

Before cutting a release, move relevant `Unreleased` entries under the release version/date and add any operator-visible upgrade instructions. Persistent schema changes must explicitly identify their migration version and backup requirements.
