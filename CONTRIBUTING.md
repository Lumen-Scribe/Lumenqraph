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

CI runs the same against a Postgres service.

## Conventions

- Shared types and decoding live in `lumenqraph-core`; don't duplicate models.
- DB writes must stay idempotent (key on `event_id`).
- New schema changes go in a new numbered `migrations/NNNN_*.sql` — never edit an
  applied migration.
- Keep raw base64 alongside any decoded representation; decoding is best-effort
  and must never break ingestion.
