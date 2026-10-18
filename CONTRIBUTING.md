# Contributing

Before contributing, please review the [Troubleshooting & FAQ guide](docs/TROUBLESHOOTING.md) for details on common operational issues and setup challenges, and our [Code of Conduct](CODE_OF_CONDUCT.md) for community standards and expectations.

## Security

Please report security vulnerabilities responsibly by following our [Security Policy](SECURITY.md). Do not open public issues for security-related concerns.

## Dev setup

```bash
cp .env.example .env
docker compose up -d          # Postgres
cargo run -p lumenqraph-indexer -- migrate
cargo run -p lumenqraph-indexer
cargo run -p lumenqraph-api
```

## Before you push

Run the aggregate CI target to reproduce what CI checks locally (minus the
network-dependent E2E suite):

```bash
make ci
```

This runs `cargo fmt`, `cargo clippy`, the unit tests, the TypeScript and Python
SDK checks, the OpenAPI drift check, and `cargo deny`. Run `make help` to see
all available targets, including the individual steps (`fmt`, `lint`, `test`,
`sdk-ts`, `sdk-py`, `openapi-check`, `deny`, `audit`, `dashboards-check`).

If you prefer to run the steps by hand:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo audit
cargo deny check
```

The `cargo audit` and `cargo deny check` commands verify dependency security and license compliance. These must pass in CI; running them locally catches issues before pushing.

### Compile-time query checking

The codebase uses sqlx's offline mode for compile-time verification of SQL queries. After modifying any SQL queries in the code:

```bash
cargo sqlx prepare --database-url "$DATABASE_URL"
```

This generates `.sqlx/` metadata that must be committed alongside your code changes. CI will verify that the metadata is up-to-date; stale metadata will fail the build.

If you see "query not prepared" errors during build, run the command above with your database running (same as `docker compose up -d` in dev setup).

### Postgres-backed tests

`cargo test --workspace` skips anything marked `#[ignore]`, which is every test
that needs a real database (retention pruning, contract-spec versioning, webhook
enqueue). To run those locally with a single command:

```bash
make test-db
```

This target automatically:
- Ensures the Postgres container is running (`make db`)
- Sets `TEST_DATABASE_URL` to the local database
- Runs all ignored tests with `--test-threads=1` for proper isolation

Each test resets the schema to isolate itself. `--test-threads=1` is required, not a preference: each test runs `DROP SCHEMA public CASCADE` to start clean, so two running at once will drop the tables out from under each other.

If you need to run tests against a custom database, you can still use the manual approach:

```bash
export TEST_DATABASE_URL=postgres://user:password@host:port/dbname
cargo test -p lumenqraph-indexer  -- --ignored --test-threads=1
cargo test -p lumenqraph-webhooks -- --ignored --test-threads=1
cargo test -p lumenqraph-api      -- --ignored --test-threads=1
cargo test -p lumenqraph-mcp      -- --ignored --test-threads=1
```

CI runs all of the above against a Postgres service.

### Smoke tests

`crates/lumenqraph-indexer/src/smoke.rs` is a single heavy end-to-end test that
drives the whole pipeline (indexer → Postgres → API-shaped queries → webhook
enqueue → signed delivery). It uses an in-process mock Soroban RPC and a local
HTTP sink — **no live network** — but it is expensive and needs a database, so
it is kept out of the normal test run by two independent gates:

- `#[ignore]` — skipped by `cargo test` unless `--ignored` is passed.
- `#[cfg(feature = "smoke-tests")]` — the module is not even compiled without
  the `smoke-tests` cargo feature, so it can never run in an offline CI job that
  forgets to set `TEST_DATABASE_URL`.

Run it with:

```bash
make test-smoke
```

or manually:

```bash
export TEST_DATABASE_URL=postgres://user:password@host:port/dbname
cargo test -p lumenqraph-indexer --features smoke-tests smoke -- --ignored --test-threads=1
```

## Definition of done

An issue is done when the behaviour it describes works on `main` and a test proves it, not when code that looks related has merged. Several issues were closed without that and turned out not to work (#476; see [docs/KNOWN_GAPS.md](docs/KNOWN_GAPS.md)). A PR that closes an issue must have:

1. **A behaviour-level test that proves the fix.** It exercises the behaviour the issue describes, through the route, CLI command, job or migration a user would hit (for example, a Postgres-backed `#[ignore]` test that calls the endpoint and checks what it returns or writes). A unit test of a helper alone is not enough. Name the test in the PR's "Test proving the fix" field. The test should fail without the change.
2. **Green CI.** Don't close an issue on the strength of a red or skipped run.
3. **Updated docs.** README feature/roadmap entries, `docs/`, `openapi.yaml` and the CHANGELOG `[Unreleased]` section match the new behaviour. If the work is partial, say so, and leave or open a tracking issue rather than closing the original.

Documentation-only and CI-only changes don't need a test, but say in the PR how the change was checked.

Reviewers: before merging a PR that closes a bug issue, check that the named test exists and covers the reported behaviour.

## Database changes

When modifying the schema or adding new tables/functions:

```bash
# 1. Create a new migration file
sqlx migrate add -r <description>

# 2. Edit the generated migrations/NNNN_<description>.sql and .down.sql
#    (Never edit an already-applied migration.)

# 3. Apply it to your local database
cargo run -p lumenqraph-indexer

# 4. Regenerate .sqlx offline metadata after the migration succeeds
cargo sqlx prepare --database-url "$DATABASE_URL"
```

New migrations must be **lock-safe** on large tables (`events`, `token_transfers`, `contract_data`): build indexes `CONCURRENTLY` in their own `-- no-transaction` file, add constraints `NOT VALID` and validate them in a later migration, and avoid column type rewrites. CI lints new and changed migrations with squawk; run `make migrations-lint` locally. See [docs/MIGRATIONS.md](docs/MIGRATIONS.md#writing-lock-safe-migrations).

After running migrations, you must commit `.sqlx/` metadata alongside your code changes. CI verifies this metadata is up-to-date; stale metadata fails the build.

To update schema objects like triggers (`contract_summaries`), edit the relevant migration and include the `CREATE TRIGGER` or `ALTER FUNCTION` statement. Test the change against a live database before committing.

## Conventions

- Shared types and decoding live in `lumenqraph-core`; don't duplicate models.
- DB writes must stay idempotent (key on `event_id`).
- New schema changes go in a new numbered `migrations/NNNN_*.sql` — never edit an
  applied migration.
- Keep raw base64 alongside any decoded representation; decoding is best-effort
  and must never break ingestion.

## Releases

Lumenqraph uses **per-artifact SemVer versioning**. Each publishable artifact is
versioned and tagged independently so that, for example, an SDK bug fix can ship
without a server release:

| Artifact | Version source | Tag format | Publish workflow |
| --- | --- | --- | --- |
| Server (Rust workspace, Docker image) | `Cargo.toml` (`[workspace.package] version`) | `server-vX.Y.Z` | `.github/workflows/docker-publish.yml` |
| TypeScript SDK (`@lumenqraph/sdk`) | `sdk/typescript/package.json` (`version`) | `sdk-ts-vX.Y.Z` | `.github/workflows/npm-publish.yml` |
| Python SDK (`lumenqraph`) | `sdk/python/pyproject.toml` (`[project] version`) | `sdk-py-vX.Y.Z` | `.github/workflows/pypi-publish.yml` |

Rules:

- Versions follow [SemVer](https://semver.org/). Breaking changes bump the major
  version (or the minor version while an artifact is still `0.x`).
- A tag must match the version declared in the artifact's manifest. The publish
  workflows verify this and fail the job on a mismatch, so a tag can never
  publish a version that disagrees with it.
- Tags are created from `main` only, after CI is green.
- Each release gets a `CHANGELOG.md` entry describing user-visible changes and
  any security fixes (see [SECURITY.md](SECURITY.md) for the advisory policy).

### Bumping a version

1. Update the version in the artifact's manifest:
   - Server: `version` under `[workspace.package]` in `Cargo.toml`, then run
     `cargo update -w` so `Cargo.lock` picks up the new workspace versions.
   - TypeScript SDK: `version` in `sdk/typescript/package.json`.
   - Python SDK: `version` under `[project]` in `sdk/python/pyproject.toml`.
2. Add a `CHANGELOG.md` section for the new version.
3. Open a PR, get it merged to `main`, and wait for CI to pass.
4. Tag the merge commit and push the tag:

   ```bash
   git checkout main && git pull
   git tag server-v0.1.0      # or sdk-ts-v0.1.0 / sdk-py-v0.1.0
   git push origin server-v0.1.0
   ```

5. The matching publish workflow builds and pushes the artifact (GHCR image,
   npm package, or PyPI package). Watch the run in the Actions tab.

### Cutting the first release (0.1.0)

All artifacts currently sit at `0.1.0` and no tag has been cut yet. The first
release is made once `main` is green and the critical bugs in the release
tracking issue are fixed:

1. Confirm `make ci` and the full CI workflow pass on `main`.
2. Confirm `CHANGELOG.md` has a `0.1.0` section for each artifact being released.
3. Push the tags for the artifacts you are releasing, e.g.:

   ```bash
   git tag server-v0.1.0 sdk-ts-v0.1.0 sdk-py-v0.1.0
   git push origin server-v0.1.0 sdk-ts-v0.1.0 sdk-py-v0.1.0
   ```

4. Verify the GHCR image, npm package and PyPI package are published with
   versions matching their tags, and create the corresponding GitHub releases.

### Automation

We intend to automate CHANGELOG entries and version bumps with
[release-please](https://github.com/googleapis/release-please) (or
[changesets](https://github.com/changesets/changesets) for the TypeScript SDK),
driven by Conventional Commit messages. Until that lands, bump versions and edit
`CHANGELOG.md` by hand as described above.

## Minimum Supported Rust Version (MSRV)

The Minimum Supported Rust Version (MSRV) for Lumenqraph is **1.84**.

The MSRV is declared in `Cargo.toml` (`rust-version`) and pinned in `rust-toolchain.toml`. Our policy is to support at least the latest stable Rust release minus two versions (stable - 2). When upgrading the MSRV:
- Update `rust-version` in `Cargo.toml`.
- Update `channel` in `rust-toolchain.toml`.
- Ensure CI (`msrv-check` job in `.github/workflows/ci.yml`) passes.
- Document any dependency or std feature requirements motivating the bump.

## Security Considerations

When contributing code, especially around authentication, cryptography, or sensitive data:

1. Use constant-time comparison for secrets and signatures (see [SECURITY.md](SECURITY.md))
2. Never hardcode secrets or API keys
3. Validate all user inputs and external data
4. Avoid leaking sensitive information in error messages
5. Keep dependencies up to date and audit transitive dependencies
