# Contributing

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
```

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
