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
| **Tradeoff** | **Proxy-behind Setup** vs **Security**: When ON, uses the **right-most** `X-Forwarded-For` entry (the one appended by the proxy in front of this instance) to identify the client IP — i.e. exactly one trusted hop. Necessary when behind nginx/HAProxy or when this instance is mounted by a sibling via `INSTANCE_MOUNTS`. When OFF, uses connection source IP. **Only enable if behind a trusted proxy**; otherwise attackers can forge IPs and bypass rate limits. Recommended: `true` if behind reverse proxy, `false` otherwise |

#### `TRUSTED_PROXY_HOPS`
| Property | Value |
|----------|-------|
| **Default** | `0` |
| **Example** | `1` |
| **Valid Range** | Non-negative integer |
| **Services** | API |
| **Tradeoff** | **Proxy-behind Setup** vs **Security**: The number of trusted proxies appending to `X-Forwarded-For`. `0` ignores the header and uses the socket peer, so a directly exposed API cannot be spoofed. `1` uses the right-most entry. If the header has fewer entries than hops, it is ignored and the socket peer is used. Unparseable values fall back to `0`. The resolved mode is logged at startup. Recommended: `1` behind a single proxy, `0` otherwise |

### CORS

#### `CORS_ALLOWED_ORIGINS`
| Property | Value |
|----------|-------|
| **Default** | (unset — no CORS headers added) |
| **Example** | `https://app.example.com,https://dashboard.example.com` or `*` |
| **Valid Range** | (empty/unset) \| `*` \| comma-separated HTTPS URLs |
| **Services** | API |
| **Tradeoff** | **Security** vs **Flexibility**: (unset) = strict, browsers enforce same-origin by default (recommended). `*` = permissive but leaks session cookies to all origins (risky). Specific list = balance. Recommended: Use specific origins for production, leave unset for development |

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

### Instance Mounts (Proxy)

These apply only when `INSTANCE_MOUNTS` is set (see [MULTI_NETWORK.md](MULTI_NETWORK.md)).

#### `PROXY_CONNECT_TIMEOUT_SECS`
| Property | Value |
|----------|-------|
| **Default** | 5 |
| **Services** | API |
| **Tradeoff** | TCP connect timeout to a mounted upstream. An unreachable upstream returns `502`. |

#### `PROXY_TIMEOUT_SECS`
| Property | Value |
|----------|-------|
| **Default** | 30 |
| **Services** | API |
| **Tradeoff** | Total per-request timeout to a mounted upstream. A hung upstream returns `504` once it elapses. |

#### `PROXY_POOL_MAX_IDLE_PER_HOST`
| Property | Value |
|----------|-------|
| **Default** | 32 |
| **Services** | API |
| **Tradeoff** | Max idle pooled connections kept per mounted upstream. |

#### `PROXY_MAX_RESPONSE_BYTES`
| Property | Value |
|----------|-------|
| **Default** | 10485760 (10 MiB) |
| **Services** | API |
| **Tradeoff** | Max upstream response body size. Larger responses return `502`. |

#### `PROXY_RATE_LIMIT_PER_MIN`
| Property | Value |
|----------|-------|
| **Default** | 600 |
| **Services** | API |
| **Tradeoff** | Per-client-IP requests/min across all mounted routes, enforced by this instance before forwarding. `0` disables. The upstream still applies its own auth and limits. |

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
| **Default** | None — required; API and webhooks fail to start if missing or empty |
| **Example** | `$(openssl rand -hex 32)` |
| **Valid Range** | Non-empty string (recommend 32+ random hex chars) |
| **Services** | API, Webhooks |
| **Tradeoff** | **At-Rest Security** vs **Complexity**: Webhook secrets are encrypted in PostgreSQL using pgcrypto. The API reads this key once at startup and uses it for creation and rotation; the webhooks service uses the same value for decryption. If changed, previously-encrypted secrets cannot be decrypted; rotate keys with caution |

**Production Setup:**
```bash
WEBHOOK_ENCRYPTION_KEY="$(openssl rand -hex 32)"
```

---

## Complete Reference by Service

The tables below cover the remaining runtime settings, grouped by the service that reads them. A **Sensitive** setting affects authentication, network exposure, rate limiting or resource limits. Review these before deploying. Settings already described above are not repeated.

### Database connection pools

Every service reads the shared `DATABASE_*` settings. Each service also reads its own override (`INDEXER_DATABASE_*`, `MCP_DATABASE_*`). When both are set, the override wins.

| Variable | Default | Used by | Purpose |
|----------|---------|---------|---------|
| `DATABASE_MAX_CONNECTIONS` | 10 (API), 5 (webhooks) | API, webhooks, and the fallback for indexer and MCP | Maximum pooled connections. |
| `DATABASE_MIN_CONNECTIONS` | 1 (API, webhooks), 0 (indexer, MCP) | All | Minimum pooled connections. The indexer clamps this to the maximum. |
| `DATABASE_ACQUIRE_TIMEOUT_SECS` | 30 | All | Seconds to wait for a pooled connection before failing. |
| `DATABASE_IDLE_TIMEOUT_SECS` | 600 | All | Seconds before an idle connection is closed. |
| `DATABASE_CONNECT_RETRIES` | 30 | All | Connection attempts at startup, for when the database starts late. |
| `INDEXER_DATABASE_MAX_CONNECTIONS` | shared value (10 if unset) | Indexer | Indexer override of `DATABASE_MAX_CONNECTIONS`. |
| `INDEXER_DATABASE_MIN_CONNECTIONS` | shared value (0 if unset) | Indexer | Indexer override of `DATABASE_MIN_CONNECTIONS`. |
| `INDEXER_DATABASE_ACQUIRE_TIMEOUT_SECS` | shared value (30 if unset) | Indexer | Indexer override of `DATABASE_ACQUIRE_TIMEOUT_SECS`. |
| `INDEXER_DATABASE_IDLE_TIMEOUT_SECS` | shared value (600 if unset) | Indexer | Indexer override of `DATABASE_IDLE_TIMEOUT_SECS`. |
| `INDEXER_DATABASE_CONNECT_RETRIES` | shared value (30 if unset) | Indexer | Indexer override of `DATABASE_CONNECT_RETRIES`. |
| `MCP_DATABASE_MAX_CONNECTIONS` | shared value (5 if unset) | MCP | MCP override of `DATABASE_MAX_CONNECTIONS`. |
| `MCP_DATABASE_MIN_CONNECTIONS` | shared value (0 if unset) | MCP | MCP override of `DATABASE_MIN_CONNECTIONS`. |
| `MCP_DATABASE_ACQUIRE_TIMEOUT_SECS` | shared value (30 if unset) | MCP | MCP override of `DATABASE_ACQUIRE_TIMEOUT_SECS`. |
| `MCP_DATABASE_IDLE_TIMEOUT_SECS` | shared value (600 if unset) | MCP | MCP override of `DATABASE_IDLE_TIMEOUT_SECS`. |
| `MCP_DATABASE_CONNECT_RETRIES` | shared value (30 if unset) | MCP | MCP override of `DATABASE_CONNECT_RETRIES`. |

### Indexer

| Variable | Default | Purpose |
|----------|---------|---------|
| `INDEXER_HTTP_BIND_ADDR` | `127.0.0.1:9090` | Address for the indexer's `/health` and `/metrics`. Loopback by default, so they are not public (#605). **Sensitive:** binding to `0.0.0.0` exposes both. |
| `METRICS_BEARER_TOKEN` | unset | When set, `/metrics` requires `Authorization: Bearer <token>`. An empty value means no authentication. **Sensitive.** |
| `METRICS_API_KEY` | unset | Used for `/metrics` only when `METRICS_BEARER_TOKEN` is unset. Provided for parity with the API's `METRICS_REQUIRE_API_KEY`. **Sensitive.** |
| `RPC_URLS` | unset | Comma-separated Soroban RPC endpoints, tried in order. Takes precedence over `RPC_URL`. |
| `SKIP_MIGRATIONS` | unset | Set to `1`, `true`, `yes` or `on` to skip running migrations at startup. |
| `DEGRADED_POLL_INTERVAL_SECS` | 300 | Poll interval used while the circuit breaker is open. |
| `MAX_CONSECUTIVE_ERRORS` | 20 | Consecutive poll errors that open the circuit breaker, which switches polling to `DEGRADED_POLL_INTERVAL_SECS`. `0` disables the breaker. |
| `ENRICHMENT_WARN_THRESHOLD` | 0.5 | Logs a warning when the fraction of events that could not be enriched exceeds this value (0.0 to 1.0). Out-of-range values fall back to 0.5. |
| `SPEC_VERSION_RETENTION` | 0 | Number of contract spec versions to keep. `0` disables pruning. |
| `SPEC_CACHE_MAX_ENTRIES` | 2000 | Maximum contract specs cached in memory. Clamped to at least 1. |
| `SPEC_FETCH_CONCURRENCY` | 4 | Maximum concurrent contract spec fetches. Clamped to at least 1. |
| `HEALTH_MAX_LAG_LEDGERS` | 100 | `/health` reports unhealthy when the indexer is at least this many ledgers behind the chain tip. Also read by the API. |
| `HEALTH_MAX_STALE_SECS` | 120 | `/health` reports unhealthy when the last update is older than this. Also read by the API. |

### API

| Variable | Default | Purpose |
|----------|---------|---------|
| `MAX_REQUEST_BODY_BYTES` | 65536 | Maximum request body size. **Sensitive:** a resource limit. |
| `API_MAX_BODY_BYTES` | 65536 | Legacy alias for `MAX_REQUEST_BODY_BYTES`. Read only when that variable is unset. |
| `API_REQUEST_TIMEOUT_SECS` | 60 | Per-request timeout. |
| `API_SHUTDOWN_TIMEOUT_SECS` | 30 | How long in-flight requests may run during graceful shutdown. |
| `EXPLORER_DIR` | `explorer` | Directory of the static explorer UI served at `/`. The Docker image uses `/app/explorer`. |
| `CLIENT_IP_HEADER` | unset | Single-value header holding the real client IP, set by the platform (for example `Fly-Client-IP` or `CF-Connecting-IP`). It is preferred over `X-Forwarded-For`. **Sensitive:** set this only when the platform overwrites the header. A client-supplied value lets callers evade per-IP limits. |
| `MAX_CONCURRENT_PER_IP` | 100 | Maximum in-flight requests per client IP. **Sensitive:** a DoS limit. |
| `RATE_LIMIT_BACKEND` | `memory` | `memory` keeps counters per process. `redis` shares them across replicas and requires `REDIS_URL`. **Sensitive.** |
| `REDIS_URL` | unset | Redis connection URL. Required when `RATE_LIMIT_BACKEND=redis`. |
| `RATE_LIMIT_FAIL_MODE` | `open` | Behavior when the Redis backend errors. `open` allows the request. `closed` rejects it. **Sensitive.** |
| `READ_MAX_REQUEST_SIZE` | 262144 | Maximum size of a read-call request (256 KiB). |
| `READ_MAX_ARGS_SIZE` | 131072 | Maximum size of a read-call's arguments (128 KiB). |
| `CALL_CACHE_MAX_ENTRIES` | 1000 | Entries in the read-call result cache. |
| `CALL_CACHE_TTL_SECS` | 5 | Seconds a cached read-call result stays valid. |
| `API_KEY_CACHE_CAPACITY` | 2000 | Entries in the API-key lookup cache. Must be at least 1. |
| `API_KEY_CACHE_TTL_SECS` | 30 | Seconds a successful key lookup stays cached. A revoked key can keep working for up to this long. **Sensitive.** |
| `API_KEY_CACHE_NEG_TTL_SECS` | 5 | Seconds a failed key lookup stays cached. |
| `METRICS_REQUIRE_API_KEY` | `false` | When `true`, `/metrics` requires an API key. It is public by default. **Sensitive.** |
| `READYZ_LAG_THRESHOLD` | 100 | `/readyz` fails when the indexer is at least this many ledgers behind. |
| `READYZ_MAX_AGE_SECS` | 120 | `/readyz` fails when the indexer's last update is older than this. |
| `READYZ_REQUIRE_FRESH_INDEX` | `false` | When `true`, `/readyz` also requires a fresh index. It is off by default so that an indexer stall does not take every API replica out of rotation. |

### MCP server

| Variable | Default | Purpose |
|----------|---------|---------|
| `MCP_RPC_RATE_LIMIT_PER_MIN` | 60 | Requests per minute allowed for each MCP client (session or token), using a sliding window. A shared global bucket also caps total throughput. |
| `MCP_HTTP_BIND` | unset | Address for the streamable-HTTP transport. When unset, the server uses stdio only. |
| `MCP_AUTH_TOKEN` | unset | Bearer token required on the HTTP transport. The server refuses to start with `MCP_HTTP_BIND` set and no token, unless `MCP_HTTP_ALLOW_ANONYMOUS=true`. **Sensitive.** |
| `MCP_HTTP_ALLOW_ANONYMOUS` | `false` | Allows the HTTP transport without `MCP_AUTH_TOKEN`. Anyone who can reach the port can call tools and read the index, so bind to localhost. **Sensitive.** |
| `MCP_ALLOWED_ORIGINS` | unset | Comma-separated `Origin` allowlist for HTTP requests (DNS-rebinding protection). Browser requests from other origins get 403. **Sensitive.** |
| `MCP_ALLOW_ANY_ORIGIN` | `false` | Disables `Origin` checks. Only safe behind an authenticating reverse proxy. **Sensitive.** |

### Webhooks

| Variable | Default | Purpose |
|----------|---------|---------|
| `WEBHOOKS_METRICS_BIND_ADDR` | `127.0.0.1:9091` | Address for the webhook service's metrics endpoint. Loopback by default. **Sensitive:** binding to a public interface exposes it. |
| `WEBHOOK_DELIVERY_RETENTION_DAYS` | 14 | Days to keep delivery records before pruning them. |

### Telemetry (all services)

| Variable | Default | Purpose |
|----------|---------|---------|
| `LOG_FORMAT` | text | Set to `json` for structured JSON logs. `RUST_LOG` still controls the filter. |

### Test-only

| Variable | Default | Purpose |
|----------|---------|---------|
| `TEST_DATABASE_URL` | unset | Postgres URL for tests that need a real database. Those tests are skipped when it is unset. The service binaries never read it. |

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
CORS_ALLOWED_ORIGINS="https://app.example.com,https://dashboard.example.com"

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

Lumenqraph validates configuration at startup. The API and webhooks services
parse every setting once; a present but invalid numeric or boolean value
(e.g. `ANON_RATE_LIMIT_PER_MIN=6O`, `API_MAX_BODY_BYTES=1MB`,
`REQUIRE_API_KEY=ture`) stops the service with an error naming the variable
instead of silently using the default. Booleans accept
`1/true/yes/on` and `0/false/no/off`. The effective configuration (secrets
redacted) is logged at startup. Examples of validation errors:

```
❌ invalid ANON_RATE_LIMIT_PER_MIN="6O": invalid digit found in string
❌ DATABASE_URL is required
❌ RPC_URL is required
❌ invalid CONTRACT_ID C1234: expected a C… strkey
❌ PAGE_SIZE 10001 out of range [1, 10000]; clamping to 10000
❌ POLL_INTERVAL_SECS cannot be zero; clamping to 1
❌ RETENTION_LEDGERS cannot be negative; clamping to 0
❌ BALANCE_KEY_DURABILITY "invalid" not recognized; expected "persistent" or "temporary"
❌ invalid origin in CORS_ALLOWED_ORIGINS: "not-a-url", skipping
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
