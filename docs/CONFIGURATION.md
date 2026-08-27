# Configuration Reference

This document describes every environment variable in Lumenqraph, its default value, valid range, which service(s) read it, and the performance/security tradeoff of changing it.

## Quick Start

```bash
# Minimal (.env.local)
DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph
RPC_URL=https://soroban-testnet.stellar.org

# Production (see Security Hardening section)
REQUIRE_API_KEY=true
ANON_RATE_LIMIT_PER_MIN=20
RPC_ROUTE_RATE_LIMIT_PER_MIN=5
GRAPHQL_INTROSPECTION_ENABLED=false
WEBHOOK_ENCRYPTION_KEY=<random-hex>
```

## Environment Variables by Service

### Database

All services read the database URL.

#### `DATABASE_URL`
| Property | Value |
|----------|-------|
| **Default** | None (required) |
| **Example** | `postgres://user:pass@localhost:5432/lumenqraph` |
| **Valid Range** | Valid PostgreSQL connection string |
| **Services** | Indexer, API, Webhooks |
| **Tradeoff** | None — must be set. Use SSL: `?sslmode=require` for production |

### Soroban RPC

#### `RPC_URL`
| Property | Value |
|----------|-------|
| **Default** | None (required) |
| **Example** | `https://soroban-testnet.stellar.org` (testnet) or `https://mainnet.sorobanrpc.com` (mainnet) |
| **Valid Range** | Valid HTTPS URL to a Soroban RPC endpoint |
| **Services** | Indexer, API |
| **Tradeoff** | **Trust**: Using an untrusted RPC corrupts the index. **Throughput**: Paid endpoints may offer lower latency |

#### `RPC_TIMEOUT_SECS`
| Property | Value |
|----------|-------|
| **Default** | 30 |
| **Example** | `10` (tight), `60` (generous) |
| **Valid Range** | 1 ≤ timeout ≤ 3600 |
| **Services** | Indexer, API |
| **Tradeoff** | **Liveness** vs **Reliability**: Lower timeout fails faster if RPC hangs; higher timeout tolerates slow endpoints but delays failure detection. Recommended: 30–60 for reliable production RPC |

---

## Indexer Configuration

The indexer is the core component that reads events from Soroban RPC and indexes them into PostgreSQL.

### Contract Filtering

#### `CONTRACT_IDS`
| Property | Value |
|----------|-------|
| **Default** | (empty) — index all contracts |
| **Example** | `CADQZ123...,CADQZ456...` (comma-separated) |
| **Valid Range** | Comma-separated list of valid Soroban contract addresses (C… strkeys) or empty |
| **Services** | Indexer |
| **Tradeoff** | **Scope** vs **Cost**: Empty = index all contract events (unbounded, high RPC cost). Bounded set (1–100 contracts) = predictable cost, essential for `UPGRADE_WATCH` and optional features. **Recommendation**: Always set a bounded list for production, even if broad |

**Example:**
```bash
# Index all contracts (high RPC cost, unbounded)
CONTRACT_IDS=

# Index specific contracts (predictable cost, UPGRADE_WATCH enabled for free)
CONTRACT_IDS=CADQZ123...,CADQZ456...,CADQZ789...
```

### Polling & Catchup

#### `POLL_INTERVAL_SECS`
| Property | Value |
|----------|-------|
| **Default** | 5 |
| **Example** | `3` (frequent), `15` (conservative) |
| **Valid Range** | 1 ≤ interval ≤ ∞ |
| **Services** | Indexer |
| **Tradeoff** | **Latency** vs **RPC Throughput**: Lower interval = near-real-time indexing but more RPC calls. Higher interval = batch processing, less RPC cost but stale data. Recommended: 3–10 seconds for production |

#### `MAX_CATCHUP_LEDGERS`
| Property | Value |
|----------|-------|
| **Default** | 4000 (≈5–6 hours at ~5 sec/ledger) |
| **Example** | `1000` (1–2 hours), `10000` (12–15 hours) |
| **Valid Range** | 1 ≤ catchup ≤ ∞ (clamped by RPC retention window) |
| **Services** | Indexer |
| **Tradeoff** | **Recovery Speed** vs **RPC Load**: Higher value allows catching up faster after downtime but risks hitting RPC processing limits (`-32001`). SDF public RPC retention is ~120k ledgers (7 days); paid endpoints may be higher. If you fall further behind, the indexer skips the gap and logs it. Recommended: 4000 for SDF public, higher (10000+) for paid endpoints |

#### `START_LEDGER`
| Property | Value |
|----------|-------|
| **Default** | 0 (start near chain tip) |
| **Example** | `1000000` (start from specific ledger) |
| **Valid Range** | 0 ≤ ledger ≤ current_tip |
| **Services** | Indexer (on first run only) |
| **Tradeoff** | **Historical Depth** vs **Sync Time**: 0 = start indexing from ~7 days ago (RPC retention). Higher value = skip early history, faster initial sync but shallower index. For new deployments, 0 is typical. Ignored on subsequent runs (cursor advances from DB) |

### Reorg Handling

#### `REORG_OVERLAP_LEDGERS`
| Property | Value |
|----------|-------|
| **Default** | 0 (disabled) |
| **Example** | `10` (conservative), `100` (aggressive) |
| **Valid Range** | 0 ≤ overlap ≤ 10000 |
| **Services** | Indexer |
| **Tradeoff** | **Reorg Safety** vs **RPC Cost**: Enabled = re-scan the last N ledgers each cycle, catching shallow reorgs. Cost: N extra RPC calls per cycle. Disabled = trust RPC (most reliable endpoints never reorg after finalization). Recommendation: Enable (10–50) for untrusted/public RPC, disable for trusted private RPC |

**Example:**
```bash
# For SDF public RPC (best effort)
REORG_OVERLAP_LEDGERS=20

# For trusted private RPC (skip re-scans, save cost)
REORG_OVERLAP_LEDGERS=0
```

### Data Fetching

#### `PAGE_SIZE`
| Property | Value |
|----------|-------|
| **Default** | 1000 |
| **Example** | `100` (conservative), `5000` (aggressive) |
| **Valid Range** | 1 ≤ size ≤ 10000 |
| **Services** | Indexer |
| **Tradeoff** | **Throughput** vs **Latency**: Larger pages = fewer RPC calls, higher throughput, but longer per-call latency. Smaller pages = finer-grained progress, faster recovery if a request hangs. Recommended: 1000–5000 for most setups |

### Contract Upgrade Watching

#### `UPGRADE_WATCH`
| Property | Value |
|----------|-------|
| **Default** | Automatic: ON if `CONTRACT_IDS` is set, OFF otherwise |
| **Example** | `true` or `false` |
| **Valid Range** | `true` \| `false` |
| **Services** | Indexer |
| **Tradeoff** | **Automatic Upgrade Detection** vs **RPC Cost**: When ON, costs 1 RPC call per tracked contract per cycle (cheap for bounded CONTRACT_IDS). When OFF, no upgrade detection. In index-all mode (empty CONTRACT_IDS), enabling this costs 1 call per *active* contract per cycle (potentially expensive). If STATE_INDEXING=true, UPGRADE_WATCH adds zero cost (already reading each contract). Recommended: Leave as default (auto-enabled when CONTRACT_IDS is set) |

### Contract State Indexing

#### `STATE_INDEXING`
| Property | Value |
|----------|-------|
| **Default** | `false` (disabled) |
| **Example** | `true` |
| **Valid Range** | `true` \| `false` |
| **Services** | Indexer |
| **Tradeoff** | **Contract State Snapshots** vs **RPC Cost**: When ON, snapshots each tracked contract's instance storage into `contract_state` table each cycle. Cost: 1 RPC call per tracked contract per cycle. Provides `/contracts/:id/state` endpoint. Recommended: Enable if using CONTRACT_IDS (bounded, predictable cost); optional for index-all (high variable cost) |

### Per-Key Storage Indexing

#### `KEY_INDEXING`
| Property | Value |
|----------|-------|
| **Default** | `false` (disabled) |
| **Example** | `true` |
| **Valid Range** | `true` \| `false` |
| **Services** | Indexer |
| **Tradeoff** | **Per-Holder Balances** vs **RPC Cost**: When ON, snapshots per-holder balances into `contract_data` for each holder mentioned in events. Cost: ~1 RPC call per *newly-active* holder per cycle (variable, can be high for popular tokens). Enables balance queries. Recommended: Pair with CONTRACT_IDS to bound the holder set; use sparingly for high-volume tokens |

#### `KEY_TEMPLATES`
| Property | Value |
|----------|-------|
| **Default** | `[]` (empty array) |
| **Example** | `[{"symbol":"Balance","events":["transfer","mint"],"params":[1,2],"durability":"persistent","label":"balance"}]` |
| **Valid Range** | JSON array of key template objects |
| **Services** | Indexer |
| **Tradeoff** | **Custom State Indexing** vs **RPC Cost**: Each template triggers state fetching for matching events. Advanced feature for custom data models (allowances, liquidity positions, etc.). Cost: ~1 RPC call per template per active key per cycle. Recommended: Use sparingly; reserved for specialized use cases |

**Template Schema:**
```json
{
  "symbol": "string (storage key variant)",
  "events": ["string (event names triggering this key)"],
  "params": [0, 1],  // topic indices to extract addresses from
  "durability": "persistent | temporary",
  "label": "string (optional, for grouping)"
}
```

#### `BALANCE_KEY_SYMBOL`
| Property | Value |
|----------|-------|
| **Default** | `Balance` |
| **Example** | `BalanceV2` (if token renamed the key) |
| **Valid Range** | Any valid symbol name |
| **Services** | Indexer (when KEY_INDEXING=true) |
| **Tradeoff** | None — cosmetic. Customize only if your token doesn't use standard `Balance` symbol |

#### `BALANCE_KEY_DURABILITY`
| Property | Value |
|----------|-------|
| **Default** | `persistent` |
| **Example** | `temporary` |
| **Valid Range** | `persistent` \| `temporary` |
| **Services** | Indexer (when KEY_INDEXING=true) |
| **Tradeoff** | None — must match the token's schema. Set to `temporary` only if the token stores balances as temporary entries (rare) |

### Data Retention

#### `RETENTION_LEDGERS`
| Property | Value |
|----------|-------|
| **Default** | 0 (disabled, keep all history) |
| **Example** | `120960` (7 days at ~5 sec/ledger), `525600` (30 days) |
| **Valid Range** | 0 ≤ ledgers ≤ ∞ |
| **Services** | Indexer |
| **Tradeoff** | **Historical Depth** vs **Database Size**: 0 = unbounded (disk is yours to manage). N > 0 = keep only last N ledgers, pruning older events and state snapshots (newest version per key always kept). Example: ~500 events/ledger at busy contracts, so 120960 ledgers ≈ 60M events ≈ ~50GB disk. Recommended: Tune to your database size cap; free-tier Postgres (~500MB) ≈ 0 ledgers or aggressive retention |

---

## API Configuration

The API service serves indexed data over HTTP, with rate limiting and optional API key authentication.

### Server Binding

#### `API_BIND_ADDR`
| Property | Value |
|----------|-------|
| **Default** | `0.0.0.0:8080` |
| **Example** | `127.0.0.1:3000` (localhost only) or `:8080` (all interfaces) |
| **Valid Range** | Valid `ip:port` |
| **Services** | API |
| **Tradeoff** | **Exposure** vs **Convenience**: `0.0.0.0` = accessible from anywhere (expect to be behind a reverse proxy in production with TLS). `127.0.0.1` = localhost only (for development or reverse proxy on same machine) |

### Authentication

#### `REQUIRE_API_KEY`
| Property | Value |
|----------|-------|
| **Default** | `false` (open access) |
| **Example** | `true` |
| **Valid Range** | `true` \| `false` |
| **Services** | API |
| **Tradeoff** | **Access Control** vs **Usability**: When ON, all requests require a valid API key header (`X-API-Key`). `/health` and `/metrics` remain public. Recommended: `true` for production to meter usage and prevent abuse |

#### `RPC_REQUIRE_API_KEY`
| Property | Value |
|----------|-------|
| **Default** | `false` |
| **Example** | `true` |
| **Valid Range** | `true` \| `false` |
| **Services** | API |
| **Tradeoff** | **RPC Quota Protection** vs **Public Simulation**: When ON, endpoints that hit RPC (like `/contracts/:id/call` and `/contracts/:id/simulate`) require API key even if REQUIRE_API_KEY=false. Recommended: `true` if you have a shared/expensive RPC quota |

### Rate Limiting

#### `ANON_RATE_LIMIT_PER_MIN`
| Property | Value |
|----------|-------|
| **Default** | 60 |
| **Example** | `10` (strict), `100` (generous) |
| **Valid Range** | 1 ≤ limit ≤ ∞ |
| **Services** | API |
| **Tradeoff** | **Protection** vs **Usability**: Lower limit = stricter rate limiting (prevents abuse, reduces RPC cost). Higher limit = easier for legitimate users but allows more abuse. Typical: 10–60 req/min per IP. Recommended: Start at 20–30 and tune based on observed legitimate traffic |

#### `RPC_ROUTE_RATE_LIMIT_PER_MIN`
| Property | Value |
|----------|-------|
| **Default** | 10 |
| **Example** | `5` (strict), `20` (generous) |
| **Valid Range** | 1 ≤ limit ≤ ∞ |
| **Services** | API |
| **Tradeoff** | **RPC Quota Protection** vs **User Experience**: Applies only to expensive routes (`/contracts/:id/call`, `/contracts/:id/simulate`). These hit upstream RPC directly. Lower limit = protects shared RPC quota. Recommended: 5–10 req/min for public RPC, 20–50 for paid/private RPC |

#### `RATE_LIMIT_TRUST_XFF`
| Property | Value |
|----------|-------|
| **Default** | `false` |
| **Example** | `true` |
| **Valid Range** | `true` \| `false` |
| **Services** | API |
| **Tradeoff** | **Proxy-behind Setup** vs **Security**: When ON, uses `X-Forwarded-For` header to identify client IP (necessary when behind nginx/HAProxy). When OFF, uses connection source IP. **Only enable if behind a trusted proxy**; otherwise attackers can forge IPs and bypass rate limits. Recommended: `true` if behind reverse proxy, `false` otherwise |

### CORS

#### `API_CORS_ALLOWED_ORIGINS`
| Property | Value |
|----------|-------|
| **Default** | `same_origin` |
| **Example** | `https://app.example.com,https://dashboard.example.com` or `*` |
| **Valid Range** | `same_origin` \| `*` \| comma-separated HTTPS URLs |
| **Services** | API |
| **Tradeoff** | **Security** vs **Flexibility**: `same_origin` = strict (recommended). `*` = permissive but leaks session cookies to all origins (risky). Specific list = balance. Recommended: Use specific origins for production |

### GraphQL

#### `GRAPHQL_INTROSPECTION_ENABLED`
| Property | Value |
|----------|-------|
| **Default** | `false` |
| **Example** | `true` |
| **Valid Range** | `true` \| `false` |
| **Services** | API |
| **Tradeoff** | **Developer Experience** vs **Security**: When ON, exposes GraphQL schema to clients (easier client code generation, but reveals all queries to attackers). When OFF, schema must be obtained separately. Recommended: `false` in production, `true` in development |

#### `GRAPHQL_MAX_DEPTH`
| Property | Value |
|----------|-------|
| **Default** | 12 |
| **Example** | `8` (strict), `20` (generous) |
| **Valid Range** | 1 ≤ depth ≤ ∞ |
| **Services** | API |
| **Tradeoff** | **DoS Prevention** vs **Query Flexibility**: Lower depth = prevents deeply-nested queries that could exhaust memory/CPU. Typical legitimate queries are 3–5 levels deep. Recommended: 8–12 |

#### `GRAPHQL_MAX_COMPLEXITY`
| Property | Value |
|----------|-------|
| **Default** | 1000 |
| **Example** | `500` (strict), `2000` (generous) |
| **Valid Range** | 1 ≤ complexity ≤ ∞ |
| **Services** | API |
| **Tradeoff** | **DoS Prevention** vs **Query Power**: Complexity is estimated per-field. Lower value = prevents expensive aggregations and bulk fetches. Recommended: 500–1000 for production |

### Body Limits

#### `MAX_BODY_SIZE_BYTES`
| Property | Value |
|----------|-------|
| **Default** | 10MB |
| **Example** | `1MB` or `100MB` |
| **Valid Range** | Any valid byte size |
| **Services** | API |
| **Tradeoff** | **Memory** vs **Flexibility**: Larger limit = accept larger payloads (e.g., GraphQL queries). Smaller = prevent memory exhaustion. Typical: 1–10MB |

---

## Webhook Configuration

The webhook service delivers indexed data to subscriber URLs with HMAC-SHA256 signatures.

### Service Behavior

#### `WEBHOOK_TICK_SECS`
| Property | Value |
|----------|-------|
| **Default** | 3 |
| **Example** | `1` (frequent), `10` (batched) |
| **Valid Range** | 1 ≤ ticks ≤ ∞ |
| **Services** | Webhooks |
| **Tradeoff** | **Latency** vs **Database Load**: Lower interval = near-real-time deliveries but more frequent DB polls. Higher interval = batched deliveries, less DB load but higher latency. Recommended: 3–5 seconds |

#### `WEBHOOK_BATCH_SIZE`
| Property | Value |
|----------|-------|
| **Default** | 100 |
| **Example** | `10` (small batches), `500` (large batches) |
| **Valid Range** | 1 ≤ batch ≤ ∞ |
| **Services** | Webhooks |
| **Tradeoff** | **Throughput** vs **Database Memory**: Larger batches = fetch more deliveries per query, higher throughput. Smaller batches = less memory/query cost. Recommended: 50–200 based on event volume |

#### `WEBHOOK_MAX_ATTEMPTS`
| Property | Value |
|----------|-------|
| **Default** | 6 |
| **Example** | `3` (aggressive), `10` (patient) |
| **Valid Range** | 1 ≤ attempts ≤ ∞ |
| **Services** | Webhooks |
| **Tradeoff** | **Reliability** vs **Database Churn**: More attempts = retry failed deliveries longer (recovers from temporary outages). Fewer attempts = fail fast, save database space. Retries use exponential backoff (2^N seconds, max 1 hour). Recommended: 6 for critical webhooks, 3 for best-effort |

### Timeouts

#### `WEBHOOK_CONNECT_TIMEOUT_SECS`
| Property | Value |
|----------|-------|
| **Default** | 5 |
| **Example** | `3` (tight), `10` (generous) |
| **Valid Range** | 1 ≤ timeout ≤ 3600 |
| **Services** | Webhooks |
| **Tradeoff** | **Liveness** vs **Reliability**: Lower timeout = fail faster if subscriber is down (frees up resources). Higher timeout = tolerates slow networks. Recommended: 3–5 seconds |

#### `WEBHOOK_TOTAL_TIMEOUT_SECS`
| Property | Value |
|----------|-------|
| **Default** | 10 |
| **Example** | `5` (tight), `30` (generous) |
| **Valid Range** | 1 ≤ timeout ≤ 3600 |
| **Services** | Webhooks |
| **Tradeoff** | **Liveness** vs **Reliability**: Total time for connect + request. Should be ≥ CONNECT_TIMEOUT. Recommended: 10–30 seconds |

### Concurrency

#### `WEBHOOK_MAX_CONCURRENT_PER_HOST`
| Property | Value |
|----------|-------|
| **Default** | 5 |
| **Example** | `1` (serialize), `20` (aggressive) |
| **Valid Range** | 1 ≤ concurrency ≤ ∞ |
| **Services** | Webhooks |
| **Tradeoff** | **Throughput** vs **Target Respect**: Limits concurrent deliveries to a single host (e.g., `app.example.com`). Higher = faster delivery but may overwhelm target. Recommended: 2–10 per host |

#### `WEBHOOK_MAX_CONCURRENT_DELIVERIES`
| Property | Value |
|----------|-------|
| **Default** | 100 |
| **Example** | `20` (conservative), `500` (aggressive) |
| **Valid Range** | 1 ≤ concurrency ≤ ∞ |
| **Services** | Webhooks |
| **Tradeoff** | **Throughput** vs **Memory/Connections**: Total concurrent deliveries across all hosts. Higher = faster throughput, more memory/TCP connections. Recommended: 50–200 depending on infrastructure |

### Failure Handling

#### `WEBHOOK_FAILURE_THRESHOLD`
| Property | Value |
|----------|-------|
| **Default** | 10 |
| **Example** | `5` (aggressive), `20` (patient) |
| **Valid Range** | 1 ≤ threshold ≤ ∞ |
| **Services** | Webhooks |
| **Tradeoff** | **Auto-Disable** vs **Tolerance**: If a subscription has N consecutive delivery failures, it's automatically disabled. Lower = faster disable (saves resources). Higher = tolerate intermittent outages. Recommended: 5–10 |

### Security

#### `WEBHOOK_ENCRYPTION_KEY`
| Property | Value |
|----------|-------|
| **Default** | `default-key-for-testing` (insecure!) |
| **Example** | `$(openssl rand -hex 32)` |
| **Valid Range** | Any string (recommend 32+ random hex chars) |
| **Services** | Webhooks |
| **Tradeoff** | **At-Rest Security** vs **Complexity**: When set, webhook secrets are encrypted in database using pgcrypto AES-128. Enables secure secret storage. **Strongly recommended for production**. If changed, previously-encrypted secrets cannot be decrypted; rotate keys with caution |

**Production Setup:**
```bash
WEBHOOK_ENCRYPTION_KEY="$(openssl rand -hex 32)"
```

---

## Security Hardening Configuration

Recommended settings for different deployment scenarios.

### Development (Local Testing)

```bash
# .env.development
DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph
RPC_URL=https://soroban-testnet.stellar.org

REQUIRE_API_KEY=false
ANON_RATE_LIMIT_PER_MIN=1000
GRAPHQL_INTROSPECTION_ENABLED=true
GRAPHQL_MAX_DEPTH=50
GRAPHQL_MAX_COMPLEXITY=5000

WEBHOOK_TICK_SECS=1
WEBHOOK_BATCH_SIZE=10
WEBHOOK_MAX_ATTEMPTS=2
```

### Staging (Internal Testing)

```bash
# .env.staging
DATABASE_URL=postgres://user:pass@db-staging:5432/lumenqraph?sslmode=require
RPC_URL=https://soroban-testnet.stellar.org

REQUIRE_API_KEY=true
ANON_RATE_LIMIT_PER_MIN=100
RPC_ROUTE_RATE_LIMIT_PER_MIN=20
RPC_REQUIRE_API_KEY=false
GRAPHQL_INTROSPECTION_ENABLED=true
GRAPHQL_MAX_DEPTH=15
GRAPHQL_MAX_COMPLEXITY=1000

WEBHOOK_ENCRYPTION_KEY="$(openssl rand -hex 32)"
WEBHOOK_TICK_SECS=3
WEBHOOK_MAX_ATTEMPTS=4
WEBHOOK_FAILURE_THRESHOLD=20
```

### Production (Hardened)

```bash
# .env.production
DATABASE_URL=postgres://user:$(openssl rand -hex 16)@db-private:5432/lumenqraph?sslmode=require
RPC_URL=https://your-trusted-soroban-rpc.example.com
RPC_TIMEOUT_SECS=30

# Security
REQUIRE_API_KEY=true
RPC_REQUIRE_API_KEY=true
ANON_RATE_LIMIT_PER_MIN=20
RPC_ROUTE_RATE_LIMIT_PER_MIN=5
RATE_LIMIT_TRUST_XFF=true  # Only if behind trusted reverse proxy

# Hardening
GRAPHQL_INTROSPECTION_ENABLED=false
GRAPHQL_MAX_DEPTH=10
GRAPHQL_MAX_COMPLEXITY=500
API_CORS_ALLOWED_ORIGINS="https://app.example.com,https://dashboard.example.com"

# Webhooks
WEBHOOK_ENCRYPTION_KEY="$(openssl rand -hex 32)"
WEBHOOK_TICK_SECS=3
WEBHOOK_BATCH_SIZE=100
WEBHOOK_MAX_ATTEMPTS=6
WEBHOOK_FAILURE_THRESHOLD=10
WEBHOOK_CONNECT_TIMEOUT_SECS=5
WEBHOOK_TOTAL_TIMEOUT_SECS=10
WEBHOOK_MAX_CONCURRENT_PER_HOST=5
WEBHOOK_MAX_CONCURRENT_DELIVERIES=100

# Indexing
CONTRACT_IDS="CADQZ...,CADQZ...,..."  # Bounded set (required for production)
POLL_INTERVAL_SECS=5
PAGE_SIZE=1000
MAX_CATCHUP_LEDGERS=4000
RETENTION_LEDGERS=120960  # 7 days

# Logging
RUST_LOG=info,lumenqraph_indexer=warn,lumenqraph_api=warn,lumenqraph_webhooks=warn
```

---

## Configuration Validation

Lumenqraph validates configuration at startup. Examples of validation errors:

```
❌ DATABASE_URL is required
❌ RPC_URL is required
❌ invalid CONTRACT_ID C1234: expected a C… strkey
❌ PAGE_SIZE 10001 out of range [1, 10000]; clamping to 10000
❌ POLL_INTERVAL_SECS cannot be zero; clamping to 1
❌ RETENTION_LEDGERS cannot be negative; clamping to 0
❌ BALANCE_KEY_DURABILITY "invalid" not recognized; expected "persistent" or "temporary"
❌ invalid origin in API_CORS_ALLOWED_ORIGINS: "not-a-url", skipping
```

Fix errors by adjusting `.env` and restarting.

---

## Configuration Tips

### Tuning for Different Workloads

**High-Volume Token (e.g., USDC)**
- `CONTRACT_IDS="CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"` (single token)
- `KEY_INDEXING=true`, `KEY_TEMPLATES=[{"symbol":"Balance","events":["mint","burn","transfer","transfer_from"],"params":[1,2],"durability":"persistent"}]`
- `ANON_RATE_LIMIT_PER_MIN=30`, `RPC_ROUTE_RATE_LIMIT_PER_MIN=10`

**Multi-Contract Ecosystem**
- `CONTRACT_IDS="C1...,C2...,..."` (10–50 contracts)
- `UPGRADE_WATCH=true`, `STATE_INDEXING=true`
- `POLL_INTERVAL_SECS=5`, `PAGE_SIZE=2000`
- `ANON_RATE_LIMIT_PER_MIN=50`, `RPC_ROUTE_RATE_LIMIT_PER_MIN=15`

**All-Contracts Indexing (Rare)**
- `CONTRACT_IDS=` (empty)
- `UPGRADE_WATCH=false`, `STATE_INDEXING=false`, `KEY_INDEXING=false`
- `POLL_INTERVAL_SECS=10`, `PAGE_SIZE=5000`
- `RETENTION_LEDGERS=43200` (3 days) — keep DB small
- `ANON_RATE_LIMIT_PER_MIN=100` (high, since serving many contracts)

### Monitoring Configuration Changes

Use `cargo audit` and `cargo deny` to check for dependency vulnerabilities after updating:

```bash
# After changing RUST_LOG or other config
cargo build --release

# Before deploying
cargo audit
cargo deny check
```

---

## See Also

- [WEBHOOKS.md](WEBHOOKS.md) — Webhook-specific configuration
- [SECURITY_MODEL.md](SECURITY_MODEL.md) — Security hardening checklist
- [SCHEMA.md](SCHEMA.md) — Database schema (affected by KEY_INDEXING, STATE_INDEXING, RETENTION_LEDGERS)
- [DEPLOYMENT.md](DEPLOYMENT.md) — Production deployment guide
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — Common configuration issues and fixes
