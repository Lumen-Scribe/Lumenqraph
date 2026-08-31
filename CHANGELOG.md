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

- **CORS is now same-origin only by default.** Set `CORS_ALLOWED_ORIGINS`
  if your frontend is on a different origin.

- **GraphQL introspection and GraphiQL are off by default.** Set
  `GRAPHQL_INTROSPECTION_ENABLED=true` in non-production environments if needed.

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

### Added
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

### Fixed
- `START_LEDGER` now clamped to RPC retention window on fresh start
- Documentation clarified `MAX_LOOKBACK_LEDGERS` vs `MAX_CATCHUP_LEDGERS`

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
