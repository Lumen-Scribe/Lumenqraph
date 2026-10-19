# Multi-Network Deployment Guide

## Overview

Lumenqraph supports serving multiple Stellar networks (mainnet, testnet, futurenet) from a single logical deployment. This document describes the recommended architecture, implementation patterns, and trade-offs.

## Architecture Decision

**Recommended Pattern: One Instance Per Network + Federation**

After evaluating the complexity vs. benefit trade-offs, the recommended approach for serving multiple networks is:

- **One Lumenqraph deployment per network** (separate indexer, API, webhooks, and database per network)
- **Federation at the edge** using instance mounts and reverse proxying
- **No network dimension in the database schema** (keeps indexing and querying simple)

This pattern provides clean separation, independent scaling, and simple operations while still presenting as "one logical deployment" to clients.

## Why One-Instance-Per-Network?

### Benefits

1. **Clean separation**: Each network has its own database, cursor, and state — no risk of cross-contamination
2. **Independent scaling**: Scale mainnet (high traffic) and testnet (low traffic) independently
3. **Simple schema**: No network column proliferating through every table and index
4. **Isolated failures**: A testnet indexer crash doesn't affect mainnet
5. **Network-specific tuning**: Different `CONTRACT_IDS`, `RETENTION_LEDGERS`, `POLL_INTERVAL_SECS` per network
6. **Upgrade flexibility**: Test schema migrations on testnet before mainnet
7. **Existing tooling compatibility**: All observability, backfill, and admin tools work unchanged

### Trade-offs

The main trade-off is **operational overhead**: running N networks requires N×3 processes (indexer, API, webhooks) and N databases. However:

- **Shared infrastructure** mitigates this: all instances can run in one container/VM with one Postgres server (separate databases)
- **Free-tier feasibility**: The hosted demo serves mainnet + testnet from a single Render web service + Supabase project
- **Minimal duplication**: The code, config, and deployment logic are identical across instances — only the network-specific env vars differ

## Implementation: Instance Mounts

### How It Works

Instance mounts allow one Lumenqraph API to reverse-proxy to sibling instances under a path prefix:

```bash
# Primary instance (mainnet)
RPC_URL=https://mainnet.sorobanrpc.com
# ...mainnet config...

# Mount testnet instance
INSTANCE_MOUNTS=testnet=http://127.0.0.1:8081
```

With this config:
- `GET /contracts` → queries mainnet
- `GET /testnet/contracts` → proxied to testnet instance at `:8081`
- Same origin, no CORS, unified public URL

The primary instance's `/health` endpoint advertises mounts:

```json
{
  "network": "mainnet",
  "mounts": {
    "testnet": "/testnet"
  }
}
```

Clients (like the explorer) discover available networks automatically and switch with one click.

### Configuration

Set `INSTANCE_MOUNTS` as a comma-separated list of `name=url`:

```bash
INSTANCE_MOUNTS=testnet=http://127.0.0.1:8081,futurenet=http://127.0.0.1:8082
```

Rules:
- Mount names must be alphanumeric + hyphens (e.g., `testnet`, `my-testnet`)
- URLs are the internal base URL of the sibling instance (no trailing slash)
- Mount names must not collide with API route prefixes (`/contracts`, `/events`, etc.)
- Naming convention: use the network name (`testnet`, `futurenet`, `mainnet-staging`)

### How Requests Are Proxied

1. **Path prefix stripping**: `GET /testnet/contracts/:id/events` → proxied as `GET /contracts/:id/events` to the testnet instance
2. **Header forwarding**: Most headers (auth, accept, user-agent) are forwarded; hop-by-hop headers (connection, keep-alive, host) are dropped
3. **Auth delegation**: The mounted instance applies its own `REQUIRE_API_KEY` and rate limiting — the primary instance doesn't re-auth
4. **Body size limits**: Request bodies are capped at 10MB (more than enough for GraphQL queries or webhook subscriptions)
5. **Error handling**: If the mounted instance is unreachable, returns `502 Bad Gateway`

## Deployment Patterns

### Pattern 1: Single Container, Multiple Networks (Free Tier)

**Use case**: Serving mainnet + testnet from a single free-tier web service (Render, Fly.io Hobby)

**Setup**:
- One Postgres server with two databases: `lumenqraph_mainnet`, `lumenqraph_testnet`
- One container running multiple processes via `scripts/run-all-in-one.sh`:
  - Primary: indexer + API (mainnet, public port 8080)
  - Secondary: indexer + API (testnet, internal port 8081)
  - Optional: webhooks (mainnet only, to save memory)
- `INSTANCE_MOUNTS=testnet=http://127.0.0.1:8081`

**Example `run-all-in-one.sh`**:

```bash
#!/usr/bin/env bash
set -e

# Start testnet instance (internal only)
DATABASE_URL="$TESTNET_DATABASE_URL" \
RPC_URL="$TESTNET_RPC_URL" \
CONTRACT_IDS="$TESTNET_CONTRACT_IDS" \
API_BIND_ADDR="127.0.0.1:8081" \
lumenqraph-indexer &
lumenqraph-api &

# Start mainnet instance (public)
DATABASE_URL="$MAINNET_DATABASE_URL" \
RPC_URL="$MAINNET_RPC_URL" \
CONTRACT_IDS="$MAINNET_CONTRACT_IDS" \
INSTANCE_MOUNTS="testnet=http://127.0.0.1:8081" \
API_BIND_ADDR="0.0.0.0:8080" \
lumenqraph-indexer &
lumenqraph-api &

wait -n  # Exit if any process exits
```

**Pros**: Zero cost, one container, one public URL
**Cons**: All networks down if the container crashes; memory-constrained (requires aggressive `RETENTION_LEDGERS` and tight `CONTRACT_IDS`)

### Pattern 2: Separate Services, Shared Database

**Use case**: Production deployment with independent scaling, shared Postgres server

**Setup**:
- One managed Postgres with N databases (one per network)
- N×3 separate services (indexer, API, webhooks per network)
- Primary API has `INSTANCE_MOUNTS` pointing to secondary APIs' internal URLs
- Load balancer routes external traffic to primary API only

**Pros**: Independent scaling (scale mainnet API replicas without affecting testnet), isolated failures
**Cons**: More processes to manage, slightly higher ops overhead

### Pattern 3: Fully Isolated Deployments

**Use case**: Enterprise production with strict network isolation requirements

**Setup**:
- Completely separate Lumenqraph stacks per network (separate Postgres, separate infrastructure)
- Primary deployment has `INSTANCE_MOUNTS` pointing to secondary deployments' **external** URLs
- No shared infrastructure

**Pros**: Maximum isolation, independent infrastructure management, network-specific disaster recovery
**Cons**: Highest resource cost, most operational overhead

### Pattern 4: Edge Federation (Advanced)

**Use case**: Geographically distributed deployments

**Setup**:
- Separate Lumenqraph deployments per region (e.g., US mainnet, EU mainnet, Asia testnet)
- Global load balancer with geo-routing
- Each regional deployment optionally has `INSTANCE_MOUNTS` to other regions' networks

**Pros**: Low latency for global users, independent regional operations
**Cons**: Complex routing, potential cross-region consistency issues

## Network Discovery (Clients)

Clients should **always** call `GET /health` on startup to discover:
1. Which network the primary instance indexes (`network` field)
2. Whether additional networks are available (`mounts` field)

Example discovery flow:

```typescript
const health = await fetch('/health').then(r => r.json());
const primaryNetwork = health.network; // "mainnet"
const availableNetworks = {
  [primaryNetwork]: '',  // primary at root
  ...Object.fromEntries(
    Object.entries(health.mounts || {})
      .map(([net, path]) => [net, path])  // e.g., ["testnet", "/testnet"]
  )
};

// User selects "testnet"
const baseUrl = availableNetworks["testnet"];  // "/testnet"
const contracts = await fetch(`${baseUrl}/contracts`).then(r => r.json());
```

The explorer implements this pattern — see `explorer/index.html`.

## Configuration Reference

### Required Environment Variables Per Instance

Each instance (primary and mounted) needs:

- `DATABASE_URL`: Postgres connection string for this network's database
- `RPC_URL`: Soroban RPC endpoint for this network
- `API_BIND_ADDR`: Bind address (primary: `0.0.0.0:8080`, mounted: `127.0.0.1:808X`)

### Network-Specific Tuning

Different networks typically need different configs:

| Config | Mainnet | Testnet | Notes |
|--------|---------|---------|-------|
| `CONTRACT_IDS` | Curated allowlist | Empty or test contracts | Mainnet has high-volume SACs; testnet is scratch |
| `RETENTION_LEDGERS` | 120960 (~7 days) | 17280 (~1 day) | Testnet needs less history |
| `POLL_INTERVAL_SECS` | 5 | 10 | Testnet can poll slower |
| `STATE_INDEXING` | true (for tracked contracts) | false | Testnet usually doesn't need state snapshots |
| `KEY_INDEXING` | true | false | Per-holder balances matter more on mainnet |
| `UPGRADE_WATCH` | true | false | Mainnet contract upgrades are production events |

### Shared Configuration

These can be the same across instances:

- `REQUIRE_API_KEY`, `ANON_RATE_LIMIT_PER_MIN`: Auth/rate limiting strategy
- `GRAPHQL_MAX_DEPTH`, `GRAPHQL_MAX_COMPLEXITY`: Query safety limits
- `WEBHOOK_*`: Delivery settings (if running webhooks per network)

## Security Considerations

1. **Mounted instance auth**: Each mounted instance applies its own `REQUIRE_API_KEY` and rate limiting. The primary instance does **not** re-validate auth for proxied requests — the mounted instance is trusted.

2. **API key scope**: API keys are per-instance (per-database). A key issued on mainnet does **not** work on testnet through the mount — each network needs its own keys.

3. **Rate limiting**: Rate limits are also per-instance. A user hitting the testnet limit won't affect their mainnet quota.

4. **Internal vs external mounts**: 
   - **Internal** (`http://127.0.0.1:808X`): Sibling instances on the same host — no TLS needed
   - **External** (`https://testnet.example.com`): Sibling instances on different infrastructure — requires TLS and exposes the mounted instance to the internet (ensure it has auth enabled)

## Monitoring Multi-Network Deployments

### Prometheus Metrics

Each instance exports its own `/metrics` endpoint. For a multi-network deployment:

- **Primary instance**: `https://example.com/metrics` (mainnet)
- **Mounted instances**: Access their internal metrics directly (e.g., `http://127.0.0.1:8081/metrics` for testnet)

Configure Prometheus to scrape all instances:

```yaml
scrape_configs:
  - job_name: 'lumenqraph-mainnet'
    static_configs:
      - targets: ['lumenqraph-primary:8080']
        labels:
          network: 'mainnet'
  
  - job_name: 'lumenqraph-testnet'
    static_configs:
      - targets: ['lumenqraph-testnet:8081']
        labels:
          network: 'testnet'
```

### Alert Rules

Add a `network` label to all alert rules and dashboard queries to distinguish networks:

```yaml
- alert: IndexerLagHigh
  expr: lumenqraph_indexer_lag_ledgers{network="mainnet"} > 100
  labels:
    severity: warning
    network: mainnet
```

### Health Checks

- **Liveness** (`/livez`): Check per instance
- **Readiness** (`/readyz`): Check per instance — a testnet instance being "not ready" shouldn't mark mainnet as unhealthy
- **Overall health** (`/health`): The primary instance's `/health` reflects only the primary network; check mounted instances separately if needed

## Migration Path

### From Single-Network to Multi-Network

If you're running a single-network deployment and want to add a second network:

1. **Provision a second database**: Either a new Postgres server or a new database in the existing server
2. **Run the new instance** with its own env vars (`DATABASE_URL`, `RPC_URL`, `API_BIND_ADDR`)
3. **Add the mount** to the primary instance: `INSTANCE_MOUNTS=testnet=http://...`
4. **Update clients**: If clients are hardcoded to the primary network, no change needed — the new network is opt-in via discovery

### From Multi-Network to Single-Network

If you want to simplify back to a single network:

1. **Remove `INSTANCE_MOUNTS`** from the primary instance
2. **Stop the secondary instances**
3. **Update clients** to remove network selection UI (if they had it)

## FAQ

### Why not add a `network` column to every table?

**Complexity**: Every query, index, and foreign key would need a network dimension. Partitioning by network is effectively the same as separate databases, but with more footguns (accidentally querying across networks, schema migration complexity).

**Performance**: Network-scoped queries would need composite indexes `(network, ledger)`, `(network, contract_id, ledger)`, etc. — doubling index storage and slowing writes.

**Isolation**: A bug that corrupts one network's data would corrupt the whole table. Separate databases provide natural blast radius containment.

### Can I serve three or more networks?

Yes. Add as many `INSTANCE_MOUNTS` entries as you have sibling instances. Example:

```bash
INSTANCE_MOUNTS=testnet=http://127.0.0.1:8081,futurenet=http://127.0.0.1:8082,staging=http://127.0.0.1:8083
```

The explorer will show all four networks (primary + three mounts) in the network selector.

### Do mounted instances need the explorer?

No. Only the primary instance needs to serve the explorer UI (`EXPLORER_DIR`). Mounted instances are API-only — their HTML endpoints (explorer, GraphiQL) won't be accessible through the mount.

### Can I mount instances on different infrastructure?

Yes, but **use HTTPS and ensure auth is enabled** on the mounted instance. Example:

```bash
INSTANCE_MOUNTS=testnet=https://testnet-api.internal.example.com
```

The mounted instance is now exposed to network requests from the primary instance, so it must be secured.

### What if a mounted instance is down?

Requests to that mount return `502 Bad Gateway`. The primary instance and other mounts remain available. Clients should handle 502s gracefully (e.g., show an error banner: "Testnet instance unavailable").

## Summary

**Recommended: One instance per network, federated via instance mounts.**

This pattern provides:
- ✅ Clean separation and simple operations
- ✅ Independent scaling per network
- ✅ Unified client experience (one URL, network discovery)
- ✅ Feasible on free tiers (single container + multi-DB Postgres)

**Not recommended: Adding a network dimension to the schema.**

This would require:
- ❌ Network column on every table
- ❌ Composite indexes everywhere
- ❌ Partitioning complexity
- ❌ Cross-network query risks

For teams that need true first-class multi-network (e.g., a SaaS indexer serving 100+ custom networks), that complexity may be justified. For most deployments — including the hosted demo and typical self-hosted setups — one-instance-per-network is simpler, safer, and sufficient.
