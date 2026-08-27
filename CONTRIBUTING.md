# Contributing

Before contributing, please review the [Troubleshooting & FAQ guide](docs/TROUBLESHOOTING.md) for details on common operational issues and setup challenges, and our [Code of Conduct](CODE_OF_CONDUCT.md) for community standards and expectations.

## Security

Please report security vulnerabilities responsibly by following our [Security Policy](SECURITY.md). Do not open public issues for security-related concerns.

## Dev setup

```bash
cp .env.example .env
docker compose up -d          # Postgres
cargo run -p lumenqraph-indexer
cargo run -p lumenqraph-api
```

## Before you push

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
enqueue). To run those, point `TEST_DATABASE_URL` at a database you don't mind
losing — each test resets the schema to isolate itself:

```bash
docker run -d --rm --name lq-test-pg \
  -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=lumenqraph -p 55433:5432 postgres:16-alpine

export TEST_DATABASE_URL=postgres://postgres:pw@localhost:55433/lumenqraph
cargo test -p lumenqraph-indexer  -- --ignored --test-threads=1
cargo test -p lumenqraph-webhooks -- --ignored --test-threads=1
```

`--test-threads=1` is required, not a preference: each test runs
`DROP SCHEMA public CASCADE` to start clean, so two running at once will drop
the tables out from under each other.

CI runs all of the above against a Postgres service.

## Conventions

- Shared types and decoding live in `lumenqraph-core`; don't duplicate models.
- DB writes must stay idempotent (key on `event_id`).
- New schema changes go in a new numbered `migrations/NNNN_*.sql` — never edit an
  applied migration.
- Keep raw base64 alongside any decoded representation; decoding is best-effort
  and must never break ingestion.

## Security Considerations

When contributing code, especially around authentication, cryptography, or sensitive data:

1. Use constant-time comparison for secrets and signatures (see [SECURITY.md](SECURITY.md))
2. Never hardcode secrets or API keys
3. Validate all user inputs and external data
4. Avoid leaking sensitive information in error messages
5. Keep dependencies up to date and audit transitive dependencies
