# Release and upgrade checklist

This runbook is for maintainers preparing a SignalKit Stream release. Publishing/tagging is intentionally a separate explicit action from ordinary PR CI.

## 1. Decide the compatibility level

Review `docs/COMPATIBILITY.md` and classify the release:

- patch: compatible fixes/internal changes;
- minor: backward-compatible additions;
- major: incompatible documented public surface changes.

Before 1.0, document any intentional incompatibility clearly in the changelog even when semantic-versioning stability is not yet promised.

## 2. Review persistent schemas

Check both compatibility boundaries:

```text
SignalEvent.schema_version
DATABASE_SCHEMA_VERSION / PRAGMA user_version
```

If the persistent database schema changed:

- add the next integer migration;
- retain fixtures/coverage for every supported released predecessor;
- prove event/checkpoint/source-health/sink/delivery preservation;
- prove migration rollback;
- update `docs/MIGRATIONS.md` and the changelog.

Do not create downgrade logic. Future-version databases must remain fail-closed.

## 3. Run deterministic release gates

The required PR CI gates are:

- Ruff lint;
- pytest on every supported Python version;
- project coverage at or above the configured threshold;
- `compileall`;
- wheel build;
- source-distribution build;
- clean wheel installation + CLI/config-validation smoke;
- clean sdist installation + import/CLI smoke.

The package job uploads the built distributions as CI artifacts for inspection. It does **not** publish them.

## 4. Review live compatibility separately

Run or inspect a recent `Live Compatibility Smoke` workflow.

Hacker News and GitHub are always probed. RSS/JSON Feed/Reddit probes are conditional on configured repository variables/secrets. A live failure is investigated separately from deterministic correctness because third-party outages, credentials, policy changes, or rate limits are external conditions.

Do not weaken deterministic tests merely to make a live probe green.

## 5. Verify the upgrade path

Before a production upgrade that can touch persistent state:

```bash
signalkit doctor signalkit.toml
signalkit db verify --db signals.db
signalkit db backup backups/pre-upgrade.db --db signals.db
signalkit db verify --db backups/pre-upgrade.db
```

Stop the old writer before switching application versions when a schema migration may run.

After upgrading:

```bash
signalkit doctor signalkit.toml
signalkit db verify --db signals.db
signalkit status --db signals.db --verbose
```

Keep the verified pre-upgrade backup until the new deployment has passed the intended observation window.

## Planned 0.7.x -> 1.0 upgrade expectation

The current pre-1.0 persistence line uses database schema version 1. If 1.0 still uses schema version 1, upgrading from the latest 0.7.x build requires no persistent layout migration: startup validates the existing schema and continues.

If a later pre-1.0 change introduces schema version 2 before the 1.0 tag, this section must be updated with the actual migration/fixture evidence rather than assuming a no-op upgrade.

Operator-visible configuration/CLI changes between the latest 0.7.x build and 1.0 must be listed in the final 1.0 release notes.

## 6. Prepare release notes

Move relevant `CHANGELOG.md` entries from `Unreleased` under the target version/date.

Release notes should include:

- operator-visible features/fixes;
- public Python/CLI/config changes;
- database migration version and backup requirements, if any;
- changed authentication/source requirements;
- known limitations;
- upgrade/rollback references.

Do not copy raw secrets, live test credentials, private source payloads, or customer data into release artifacts/notes.

## 7. Version consistency

Before tagging/publishing, update the package version consistently in:

- `pyproject.toml`;
- `signalkit_stream.__version__`.

The release PR should include a deterministic assertion/check that the two values match.

## 8. Final package inspection

Use the CI `python-distributions` artifact or build locally from the exact release commit. Inspect at minimum:

- package metadata/version;
- README and license metadata;
- expected modules;
- executable `signalkit` entry point;
- clean install behavior.

Do not publish a package built from a different commit than the reviewed/tagged release commit.

## 9. Tag/publish as an explicit action

Tagging and package publication should happen only after the release PR is merged and the exact release commit has passed all deterministic gates.

The normal CI workflow intentionally has no package-registry publish credential or publish step.

After publication, verify installation from the real package registry in a clean environment before announcing the release.

## 10. Post-release

- verify documentation points at the released behavior;
- preserve released migration fixtures when schema versions advance;
- open a new `Unreleased` changelog section if needed;
- monitor scheduled live compatibility checks;
- keep the previous production backup/release available for the documented rollback window.
