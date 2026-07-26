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

## 1.0.0rc1 upgrade expectation

`1.0.0rc1` keeps both compatibility markers unchanged from the current 0.7.x line:

```text
SignalEvent.schema_version = 1
DATABASE_SCHEMA_VERSION = 1
```

A healthy 0.7.x database already at persistent schema version 1 therefore requires no SQLite layout migration for the release candidate. Startup validates the existing schema and continues.

Legacy/unversioned supported Stream databases still migrate forward to schema version 1 atomically. A database carrying a future `PRAGMA user_version` still fails closed without mutation.

The release candidate adds no intentional break to the public Python/CLI subset inventoried in `docs/PUBLIC_API.md`; the compatibility tests run on the exact candidate branch.

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

The release PR includes a deterministic assertion/check that the two values match.

For a release candidate, use a PEP 440 pre-release version such as `1.0.0rc1`. The candidate can be reviewed and built without creating a Git tag or publishing a package.

## 8. Release-candidate signoff boundary

A release-candidate PR is the safe point to stop before irreversible/public release actions.

For `1.0.0rc1`, require:

1. synchronized `1.0.0rc1` package/runtime versions;
2. changelog entries moved under the dated candidate heading;
3. public API compatibility tests green;
4. Python 3.11/3.12/3.13 deterministic tests green;
5. wheel/sdist build and clean-install smoke green;
6. candidate distributions inspected from the exact PR commit;
7. recent live compatibility results reviewed separately;
8. no tag or registry publication performed by the PR itself.

Merging a release-candidate PR only records the reviewed candidate state in the repository. Creating a tag/release or publishing to a registry remains a separate explicit maintainer decision.

## 9. Final package inspection

Use the CI `python-distributions` artifact or build locally from the exact release commit. Inspect at minimum:

- package metadata/version;
- README and license metadata;
- expected modules;
- executable `signalkit` entry point;
- clean install behavior.

Do not publish a package built from a different commit than the reviewed/tagged release commit.

## 10. Promote an RC to final 1.0

After candidate observation/signoff, prepare a final release commit/PR that:

- changes `1.0.0rcN` to `1.0.0` consistently;
- moves any post-RC changelog items into the final release notes;
- re-runs the same deterministic package gates on the exact final commit;
- confirms no incompatible public-surface drift since the reviewed candidate;
- repeats upgrade/package inspection when the candidate changed materially.

Do not assume that a green RC artifact can simply be renamed to a final package: final distributions must be built from the exact final `1.0.0` source commit.

## 11. Tag/publish as an explicit action

Tagging and package publication should happen only after the release PR is merged and the exact release commit has passed all deterministic gates.

The normal CI workflow intentionally has no package-registry publish credential or publish step.

After publication, verify installation from the real package registry in a clean environment before announcing the release.

## 12. Post-release

- verify documentation points at the released behavior;
- preserve released migration fixtures when schema versions advance;
- open a new `Unreleased` changelog section if needed;
- monitor scheduled live compatibility checks;
- keep the previous production backup/release available for the documented rollback window.
