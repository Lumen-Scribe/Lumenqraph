# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning Policy

Lumenqraph follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** version (X.0.0): Incompatible API changes, breaking configuration changes, or removal of deprecated features
- **MINOR** version (0.X.0): New features, additions to the API, or non-breaking enhancements
- **PATCH** version (0.0.X): Bug fixes, security patches, or performance improvements that don't change the API

Breaking changes will be documented in the changelog with migration guidance where applicable.

> **Upgrading a running deployment?**
> See [docs/UPGRADING.md](docs/UPGRADING.md) for step-by-step migration
> instructions, required env var changes, and the full database migration log
> for every release.

## [Unreleased]

### Migration notes

> Full step-by-step instructions: [docs/UPGRADING.md — Unreleased](docs/UPGRADING.md#unreleased--next-release)

**Breaking changes requiring action before deploy:**

- **`WEBHOOK_ENCRYPTION_KEY` is now required** for the webhooks service.
  Webhook secrets are encrypted at rest using `pgcrypto`. Generate the key
  with `openssl rand -hex 32` and set it before deploying — migration
  `0020` backfills existing subscriptions using this key. Deployments
  without it fall back to the insecure hardcoded default.
  See [docs/UPGRADING.md](docs/UPGRADING.md#1-webhook-secrets-are-now-encrypted-at-rest--webhook_encryption_key-required).

- **`token_transfers.kind` column added** (`transfer` | `mint` | `burn` |
  `clawback`). Clients reading transfer payloads by positional index must
  update. Migration `0015` backfills existing rows as `"transfer"`.

- **Uniform keyset pagination on every list endpoint (#445).** Transfers,
  swaps, NFTs, liquidity, events and webhook deliveries all return
  `{ data, has_more, next_cursor }` and accept `after=<next_cursor>`.
  `offset` is capped at 10,000 and answered with `Deprecation: true`.
  `GET /webhooks/:id/deliveries` no longer returns `deliveries`,
  `total_count`, `limit` or `offset`; per-status counts moved behind
  `?include_summary=true`. Migration `0024` adds the supporting index.

- **Webhook deliveries no longer follow redirects (#447).** A `3xx` from a
  subscriber endpoint is recorded as a failed attempt
  (`last_error: "redirects are not followed: …"`).

- **CORS is now same-origin only by default.** Set `CORS_ALLOWED_ORIGINS`
  if your frontend is on a different origin.

- **GraphQL introspection and GraphiQL are off by default.** Set
  `GRAPHQL_INTROSPECTION_ENABLED=true` in non-production environments if needed.

- **Migrations are an explicit step (#473).** Run `lumenqraph-indexer migrate`
  before starting services; compose (`migrate` init service), `fly.toml`
  (`release_command`) and `scripts/run-all-in-one.sh` do this already. The
  API, webhooks and MCP server now refuse to start if the schema is behind
  their build. Databases set up only with `scripts/setup_db.sh` must be
  migrated once with `lumenqraph-indexer migrate`.

- **`docker-compose.full.yml` requires `POSTGRES_PASSWORD` (#472)** and, like
  `docker-compose.yml`, publishes Postgres on `127.0.0.1` only.

**Database migrations applied:** `0010` through `0021` (run automatically by
the indexer on startup). Stop the webhooks service before deploying to avoid
a write conflict on `webhook_deliveries` during migration `0008`.

**New environment variables:** `WEBHOOK_ENCRYPTION_KEY`, `CORS_ALLOWED_ORIGINS`,
`GRAPHQL_MAX_DEPTH`, `GRAPHQL_MAX_COMPLEXITY`, `GRAPHQL_INTROSPECTION_ENABLED`,
`RATE_LIMIT_TRUST_XFF`, `RATE_LIMIT_BACKEND`, `REDIS_URL`,
`RPC_ROUTE_RATE_LIMIT_PER_MIN`, `RPC_REQUIRE_API_KEY`, `RPC_TIMEOUT_SECS`,
`READYZ_LAG_THRESHOLD`, `READYZ_MAX_AGE_SECS`, `HEALTH_MAX_LAG_LEDGERS`,
`HEALTH_MAX_STALE_SECS`, `ENRICHMENT_WARN_THRESHOLD`, `SPEC_CACHE_MAX_ENTRIES`,
`SPEC_VERSION_RETENTION`, `KEY_TEMPLATES`, `BALANCE_KEY_SYMBOL`,
`BALANCE_KEY_DURABILITY`, `DATABASE_MAX_CONNECTIONS`, `DATABASE_MIN_CONNECTIONS`,
`DATABASE_ACQUIRE_TIMEOUT_SECS`, `DATABASE_IDLE_TIMEOUT_SECS`.

### Changed
- **MSRV raised to Rust 1.85 (#532).** `Cargo.lock` now pins `zeroize 1.9.0`,
  `uuid 1.23.4` and `proptest 1.11.0`, all of which require Rust 1.85
  (`zeroize` also uses edition 2024). `rust-version` in `Cargo.toml`,
  `rust-toolchain.toml` and the Docker builder image now all agree on 1.85,
  so local builds, the MSRV CI job and the Docker image compile the lockfile.
  Contributors on an older toolchain must upgrade (`rustup update 1.85`).

### Added
- `lumenqraph-indexer migrate` and a startup schema-compatibility check in
  every service (#473).
- `scripts/check_env_example.py`, run in CI, fails when the code reads an
  environment variable that `.env.example` doesn't document (#471).
- Demo dataset (`make seed`, #93), a Postgres advisory-lock guard against
  concurrent indexers (#94), config range clamping at startup (#95) and strkey
  edge-case tests (#96).
- Keyset cursor pagination for REST `/events` and `/transfers` endpoints
- Trailing re-scan mechanism for shallow reorg detection
- Contract summaries table to cache contract statistics for faster queries
- Supply-chain security: `cargo-audit` and `cargo-deny` in CI pipeline
- SECURITY.md with vulnerability disclosure policy
- GraphQL query depth and complexity limits to prevent DoS
- Constant-time signature verification for webhook deliveries
- IP-based bucketing for anonymous rate limiting
- Separate rate limiting for RPC-backed routes (tighter limits)
- Explicit request body size limits on API endpoints
- CORS allowlist configuration (replaces permissive defaults)
- Streaming webhook response bodies with size caps
- SSRF protection for webhook delivery URLs
- Non-root container user for Docker images
- GraphQL introspection and GraphiQL disabled in production by default
- Graceful shutdown handling (SIGTERM) for Docker/Kubernetes deployments
- Jitter to webhook retry backoff to prevent thundering herd
- Token-bucket rate limiter to prevent boundary bursts
- **#469 — Built-in `healthcheck` subcommand** on every service binary
  (`lumenqraph-api healthcheck`, `-indexer`, `-webhooks`, `-mcp`): GETs the
  service's own health endpoint on localhost and exits 0/1. The Dockerfile
  gains a `HEALTHCHECK`, and `docker-compose.full.yml` uses it for every
  service, so health no longer depends on `curl`, which the runtime image
  does not ship. ([#469](https://github.com/Lumen-Scribe/Lumenqraph/issues/469))
- **#470 — Optional `mcp` service in `docker-compose.full.yml`** (profile
  `mcp`, streamable HTTP on `:8090`); README, ARCHITECTURE and DEPLOYMENT now
  agree on four service binaries, and the architecture diagrams include MCP.
  ([#470](https://github.com/Lumen-Scribe/Lumenqraph/issues/470))
- **#467 — Python SDK parity with the API and the TypeScript SDK:** webhook
  create/list/update/delete, interface history and diff, single-event and
  transaction-event lookups, stats, contract refresh, generated SDK download,
  SSE event streaming, GraphQL event paging, and pydantic models generated
  from `openapi.yaml` (`lumenqraph.models`) with a CI drift check.
  ([#467](https://github.com/Lumen-Scribe/Lumenqraph/issues/467))

### Fixed
- `START_LEDGER` now clamped to RPC retention window on fresh start
- Documentation clarified `MAX_LOOKBACK_LEDGERS` vs `MAX_CATCHUP_LEDGERS`
- **#406 — XDR decoder stack-overflow on deeply nested `ScVal`.** `read_scval`
  and `read_map` now thread a recursion depth counter and return the `_xdr`
  fallback for any value nested beyond 256 levels (larger than any legitimate
  Soroban value). A malformed or malicious input can no longer crash the
  indexer or API process. ([#406](https://github.com/Lumen-Scribe/Lumenqraph/issues/406))
- **#407 — Protocol 23 address types decoded to garbage.** `read_address` now
  handles `SC_ADDRESS_TYPE_MUXED_ACCOUNT` (→ `M…` strkey),
  `SC_ADDRESS_TYPE_CLAIMABLE_BALANCE` (→ `B…` strkey), and
  `SC_ADDRESS_TYPE_LIQUIDITY_POOL` (→ `L…` strkey). Unknown address types
  fall back to `_xdr` without misaligning the cursor — fixing downstream
  corruption when these addresses appeared inside a vec or map.
  ([#407](https://github.com/Lumen-Scribe/Lumenqraph/issues/407))

### Changed
- **#468 — Python SDK requires Python 3.10+** (3.8 and 3.9 are end-of-life).
  CI tests 3.10–3.14, runs `mypy --strict`, and now covers the sync client.
  ([#468](https://github.com/Lumen-Scribe/Lumenqraph/issues/468))
- **#467 — Python SDK signature fixes:** `list_transfers` requires
  `contract_id` (there is no global `/transfers` route), `list_contracts`
  takes `limit`/`after` (cursor) instead of `offset`, and `redrive_webhook`
  sends `since` as a query parameter, as the API expects. The unused `httpx`
  dependency was dropped.
  ([#467](https://github.com/Lumen-Scribe/Lumenqraph/issues/467))

### Changed (JSON shape — review before upgrading)
- **#408 — `SCV_ERROR` now decodes to `{"error":{"type":…,"code":…}}`** instead
  of the opaque `{"_error":true}`. Contract errors carry a numeric `code`;
  host errors carry a named `SCErrorCode` string (e.g. `"ExceededLimit"`).
  Consumers that matched the old `_error` key must be updated to read
  `error.type` and `error.code`.
  ([#408](https://github.com/Lumen-Scribe/Lumenqraph/issues/408))
- **#409 — `u256`/`i256` values now decode to decimal strings** (e.g.
  `"115792089…"` / `"-1"`) instead of the old `{"_u256_hex":"…"}` object.
  This aligns with the existing `u128`/`i128` behaviour and restores the
  decode → `/call` round trip for 256-bit types. Consumers that parsed the
  `_u256_hex` key must switch to reading the string value directly.
  ([#409](https://github.com/Lumen-Scribe/Lumenqraph/issues/409))

### Summary of merged work since 0.1.0

About 110 pull requests have merged since the `0.1.0` section was written
(no release has been tagged yet; see [#542](https://github.com/Lumen-Scribe/Lumenqraph/issues/542)).
Grouped by area, with the main PRs for each. Features marked *experimental*
are incomplete on `main`; see the README roadmap and
[docs/KNOWN_GAPS.md](docs/KNOWN_GAPS.md).

- **Ingestion and indexer:** batch inserts and pool tuning (#167); resumable,
  streaming backfill (#315, #319); configurable RPC timeout and per-call retry
  with jitter (#173); leader lock, maintenance-command locking and CLI parsing
  (#171, #484); ledger timestamp validation and parse-error tracking (#313,
  #512); missed-ledger-range tracking (#479); multi-RPC failover via `RPC_URLS`
  (#512); configurable per-key state templates (#169); token-transfer `kind`
  (`mint`/`burn`/`clawback`) and numeric amounts (#186, #189, #320).
- **Decoding and specs:** XDR recursion limit, Protocol 23 address types,
  `SCV_ERROR` and 256-bit decoding (#504, #505); event-prefix matching, named
  contract errors and severity levels (#502, #510, #511); spec caching,
  validation and re-enrichment (#183, #311); parameter reordering detected as
  a breaking change (#341); SAC detection and unsupported-type errors (#184,
  #328).
- **API:** keyset pagination on every list endpoint (#498); transaction and
  single-event lookups, time-series stats and SDK generation endpoints (#161,
  #479, #496); `POST /contracts/:id/refresh` (#333); interface-history pagination
  (#343); CORS allowlist, body limits, request IDs and hardened shutdown
  (#311, #319, #328, #495, #496); contract-ID extractor, typed config and a
  hardened multi-network proxy (#499); SSE event streaming (*experimental*,
  #161, #495, #497); materialized swap/NFT/liquidity endpoints (*experimental*, no
  writer yet, #172).
- **Security and rate limiting:** security hardening (#101, #310, #312,
  #334); IP-based anonymous limits, separate RPC-route limits, per-IP
  concurrency caps and an LRU key cache (#305, #506); token-bucket and sharded
  rate limiters (#498); Redis rate-limit backend (*experimental*, #307);
  audit log (#309, #485); credential redaction in logs (#327, #334).
- **Webhooks:** delivery enhancements and atomic enqueue (#156, #182);
  encrypted secrets at rest and plaintext-secret removal (#189, #307, #308,
  #481, #485); SSRF protection and no redirects (#498, #500); auto-disable,
  tri-state `PATCH`, re-enable, `GET /webhooks/:id`, delivery details and test
  endpoint (#500, #501, #506); timestamped `t=…,v1=…` signatures, versioned
  envelopes and atomic delivery claiming (#503); topic/param filters (#515);
  delivery pruning (#485).
- **MCP server:** auth, event tools, structured output and protocol
  negotiation (#186, #334, #515, #516); integration guide (#185).
- **SDKs:** TypeScript SDK retries, codegen types, webhook verification,
  upgrade-watch endpoints, webhook lifecycle, analytics and authenticated SSE
  (#165, #317, #324, #513, #514); Python SDK (#300) and PyPI publishing (#483).
- **Database:** contract summaries with batched triggers (#314, #485); GIN
  index, enum constraints and redundant-index cleanup (#481); migration
  renumbering and per-test schema isolation (#483). Duplicate versions have
  since reappeared ([#535](https://github.com/Lumen-Scribe/Lumenqraph/issues/535)).
- **Operations and observability:** Grafana dashboard and Prometheus alerts
  (#158); queue-depth, enrichment and call-cache metrics (#311, #316,
  #324); build info in `/health` (#340); seed data and scripts (#171, #335,
  #342); Render and Fly configs (#321).
- **CI and tooling:** coverage, Dependabot, Docker/GHCR, npm and PyPI
  publishing (#164, #174, #181, #483); action SHA pinning (#480); scheduled
  E2E test (#164); OpenAPI drift check (#314).
- **Documentation:** README/API drift fixes, upgrade guide, migration
  workflow, multi-network, webhooks, schema and security docs (#168, #170,
  #306, #322, #339).

### Added (CI and process)
- **#475 — Migration lock-safety lint.** CI lints new and changed migrations
  with [squawk](https://squawkhq.com) (`migration-lint` job,
  `.squawk.toml`, `scripts/lint_migrations.sh`, `make migrations-lint`) and
  fails on table-locking DDL such as non-concurrent index builds and
  constraints without `NOT VALID`; `-- squawk-ignore <rule>` opts out.
  `docs/MIGRATIONS.md` documents lock-safe patterns, the `-- no-transaction`
  convention for concurrent indexes, and how to pre-build the indexes from
  `0010`/`0016` before upgrading a populated instance.
  ([#475](https://github.com/Lumen-Scribe/Lumenqraph/issues/475))
- **#476 — Definition of done.** `CONTRIBUTING.md` requires a behaviour-level
  test, green CI and updated docs before an issue is closed, and the PR
  template has a "Test proving the fix" field. `docs/KNOWN_GAPS.md` links each
  closed-but-unfixed issue to its open tracking issue.
  ([#476](https://github.com/Lumen-Scribe/Lumenqraph/issues/476))

### Fixed (CI and docs)
- **#488 — Scheduled E2E test never ran.** It pinned
  `docker/setup-buildx-action` to a SHA that doesn't exist, so every run failed
  at job setup. It now uses the verified v4 pin, with a timeout long enough for
  a cold release build. ([#488](https://github.com/Lumen-Scribe/Lumenqraph/issues/488))
- **#477 — README roadmap and features match the code.** The roadmap is split
  into shipped, experimental and planned, with tracking issues; the Features
  table covers webhooks signing, SSE, stats, the audit log, the Python SDK and
  monitoring; and Known limitations are current.
  ([#477](https://github.com/Lumen-Scribe/Lumenqraph/issues/477))

## [0.1.0] - Initial Release

### Migration notes

> Full step-by-step instructions: [docs/UPGRADING.md — Fresh install / v0.1.0](docs/UPGRADING.md#fresh-install--v010-initial-release)

**Fresh install** — no prior version to migrate from. The indexer applies
migrations `0001` through `0009` automatically on first startup.

**Required environment variables:** `DATABASE_URL`, `RPC_URL`.
All other variables have safe defaults. See [Configuration](README.md#configuration).

### Added
- **Core indexing**: Poll Soroban RPC `getEvents`, decode XDR to JSON, store in Postgres
- **Typed, self-describing decoding**: Parse contract's on-chain `contractspecv0` interface and enrich events with field names and types automatically (zero configuration)
- **Contract upgrade watch**: Track contract interface versions over time, compute semantic diffs, detect breaking changes, deliver `contract.upgraded` webhooks
- **Read layer** (`eth_call` for Soroban): Invoke contract view functions read-only via REST, with type-checked arguments and typed results
- **Transaction preview**: Dry-run state-changing calls via `/simulate`, returning typed results, emitted events (decoded), and resource fees
- **Contract state indexing**: Versioned snapshots of contract instance storage (admin, config, TVL, counters)
- **Per-key state indexing**: Track individual storage entries (e.g. per-holder token balances) discovered from events
- **Materialized token transfers**: SEP-41 `transfer` events projected into queryable `from`/`to`/`amount` table
- **TypeScript SDK generation**: Generate typed, zero-dependency clients on demand from on-chain contract specs at `/contracts/:id/sdk`
- **GraphQL API**: Relay-style cursor pagination for events and transfers alongside REST API
- **Signed webhooks**: HMAC-SHA256-signed event pushes with configurable filters, retries, and exponential backoff
- **API key authentication**: SHA-256-hashed API keys with per-key rate limits
- **Rate limiting**: Per-key and anonymous (IP-based) request throttling
- **Prometheus metrics**: `/metrics` endpoint with indexer lag, events ingested, API request counts, etc.
- **Health checks**: `/health` endpoint reporting network, last processed ledger, chain-tip lag
- **Backfill mode**: One-shot historical catch-up bounded by RPC retention
- **Retention policy**: Configurable ledger-based pruning of old events and state snapshots
- **Multi-network support**: Mainnet, testnet, futurenet, and custom network support
- **Instance mounts**: Serve multiple network deployments under path prefixes (e.g. `/testnet`)
- **Explorer UI**: Zero-build, single-page interface for browsing contracts, events, state, and interfaces with network auto-detection and switching
- **MCP server** (`lumenqraph-mcp`): Model Context Protocol server for AI agent access to indexed contracts
- **Docker support**: Multi-stage Dockerfile for all four binaries, docker-compose for full stack
- **Database migrations**: SQLx migrations for schema versioning (0001–0009)
- **Robust ingestion**: Idempotent writes, reorg tolerance, graceful shutdown, automatic retry with backoff
- **CLI tools**: API key generation, backfill script, database setup
- **CI/CD**: GitHub Actions workflow for `fmt`, `clippy`, and tests
- **Documentation**: Comprehensive README, API reference (docs/API.md), architecture guide (docs/ARCHITECTURE.md), deployment guide (docs/DEPLOYMENT.md)

### Architecture
- **Four service binaries** sharing one core library:
  - `lumenqraph-indexer`: Polls RPC, decodes events, enriches with contract specs, writes to Postgres
  - `lumenqraph-api`: Axum REST + GraphQL API with auth, rate limiting, metrics, and read layer
  - `lumenqraph-webhooks`: Subscription matching and signed webhook delivery
  - `lumenqraph-mcp`: Model Context Protocol server for AI agents
- **Shared coordination through Postgres**: Services scale, restart, and fail independently

### Supported Argument Types
- Primitives: `bool`, all sized integers, `i128`/`u128`, `u256`/`i256` (as decimal strings)
- Strings: `Symbol`, `String`
- Binary: `Address`, `Bytes`, `BytesN`
- Collections: `Option`, `Vec`, `Tuple`, symbol-keyed `Map`
- User-defined types: Structs, unit enums, and unions resolved from contract specs

[Unreleased]: https://github.com/Lumen-Scribe/Lumenqraph/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Lumen-Scribe/Lumenqraph/releases/tag/v0.1.0
