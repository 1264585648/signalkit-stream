# Compatibility policy

SignalKit Stream has several independent compatibility surfaces. Treating them separately keeps source/API drift, normalized event contracts, SQLite persistence, configuration, and Python APIs from being conflated into one version number.

## Supported Python versions

The current supported interpreter matrix is Python 3.11, 3.12, and 3.13. Normal CI executes the deterministic suite on every supported version. A Python version is not considered supported only because the package happens to import on it.

## Public Python API

The intended public Python surface is the set of documented objects exported from the `signalkit_stream` package plus documented collector/sink extension contracts. `docs/PUBLIC_API.md` is the explicit 1.0 inventory reviewed by deterministic compatibility tests.

Modules/classes/functions whose names begin with `_` are implementation details and are not compatibility promises.

Before 1.0, incompatible public API changes may still occur when they materially improve the long-term contract, but they must be called out in `CHANGELOG.md` and migration/upgrade documentation where relevant.

Starting with 1.0, the project intends to follow semantic-versioning expectations for the documented public Python API:

- patch: compatible fixes and internal changes;
- minor: backward-compatible features/additions;
- major: intentional incompatible public API changes.

## CLI compatibility

Documented top-level commands and automation-oriented output are part of the operator surface. The required 1.0 command/subcommand/options subset is also inventoried in `docs/PUBLIC_API.md` and exercised in CI.

At 1.0:

- existing documented command names/options should not be removed or reinterpreted incompatibly in a minor/patch release;
- JSON field names and Prometheus metric names intended for automation should remain backward-compatible within a major version;
- human-readable table formatting may evolve without being treated as a machine API;
- exit-code semantics documented for validation, doctor, status/readiness, and database verification are compatibility-relevant.

New optional flags/fields/metrics can be added in minor releases.

## Configuration compatibility

TOML configuration is intentionally strict: unknown keys fail validation rather than being silently ignored.

Within a stable major version, existing documented keys should keep their meaning. Renames/removals require an explicit migration path and release-note entry. New optional keys are additive.

Secret values stay outside configuration whenever an environment-backed credential mechanism exists. Environment-variable *names* may be configured; credentials themselves should not be committed to TOML.

## `SignalEvent` compatibility

`SignalEvent.schema_version` versions the normalized downstream event contract. It is independent of the package version and SQLite schema version.

A change that makes an existing serialized event shape incompatible requires a new event schema version plus explicit consumer guidance. Adding source-specific data under `metadata` does not by itself require a new event schema version.

Stable event IDs identify a source-native object; mutation fingerprints identify the exact source-visible version of that object.

## Persistent SQLite compatibility

SQLite layout compatibility is controlled by `PRAGMA user_version` and `DATABASE_SCHEMA_VERSION`, not by `SignalEvent.schema_version`.

Rules:

- older supported schemas migrate forward atomically before the store is used;
- current schemas are validated before runtime work proceeds;
- newer/future schemas fail closed without mutation;
- there is no automatic downgrade path;
- every real new persistent schema version must include upgrade tests from every supported released predecessor and preserve events/checkpoints/source health/sink/outbox state.

Always create/verify a backup before an application upgrade that can migrate persistent schema. See `docs/MIGRATIONS.md` and `docs/BACKUP.md`.

## Collector/source compatibility

First-party collector contracts are stable on the SignalKit side, but upstream services are external dependencies. Reddit, GitHub, Hacker News, RSS publishers, and JSON Feed publishers can change availability, authentication, rate limits, or response behavior independently of this project.

SignalKit therefore separates:

- deterministic adapter correctness tests, which are part of PR CI;
- live compatibility smoke probes, which are scheduled/manual and can fail because of third-party conditions.

A live-source behavior change should be reproduced with the smallest sanitized deterministic fixture before changing adapter semantics.

The generic REST collector is an explicit extension helper, not a promise that arbitrary JSON APIs share one universal semantic contract.

## Delivery compatibility

Delivery is at-least-once. A downstream side effect can happen before local acknowledgement and therefore be replayed after process death.

Webhook consumers should treat the version-specific `Idempotency-Key` as the stable retry key for one sink + one event version. `X-SignalKit-Event-ID` remains stable across source mutations; `X-SignalKit-Event-Hash` identifies the source-visible version.

Changing these semantics incompatibly requires a major-version compatibility review.

## Deprecation policy after 1.0

For a documented public API/CLI/config item that can reasonably be deprecated rather than immediately removed:

1. mark/document the deprecation;
2. provide the replacement/migration path;
3. retain the old behavior through at least one minor release where practical;
4. remove it only in a release whose compatibility level permits the break.

Security, data-corruption, or third-party policy requirements may justify faster changes; those must be prominently documented.
