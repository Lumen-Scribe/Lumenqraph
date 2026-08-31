# Upgrading Lumenqraph

This guide documents every breaking change, required migration step, new or
renamed environment variable, and any manual action needed when upgrading from
one Lumenqraph release to another.

**Always read this file before upgrading a running deployment.**

The indexer applies database migrations automatically on startup via
`sqlx::migrate!`. In most cases the upgrade procedure is:

1. Read the relevant section below.
2. Set any new required environment variables before restarting.
3. Stop the old binaries.
4. Deploy the new binaries — the indexer runs migrations first, then starts polling.
5. Start the API and webhooks workers once the indexer is healthy (`/health`).

For a full deployment reference see [docs/DEPLOYMENT.md](DEPLOYMENT.md).

---

## Table of Contents

- [Unreleased → next release](#unreleased--next-release)
- [Fresh install / v0.1.0 (initial release)](#fresh-install--v010-initial-release)
- [General notes](#general-notes)

---

## Unreleased → next release

> These changes are on `main` and will ship in the next versioned release.
> If you are tracking `main` directly, apply them now.

### Breaking changes

#### 1. Webhook secrets are now encrypted at rest — `WEBHOOK_ENCRYPTION_KEY` required

Migration `0017_webhook_enhancements.sql` introduced a `pgcrypto`-encrypted
`encrypted_secret` column on `webhook_subscriptions`. Migration
`0020_webhook_secret_encryption_backfill.sql` backfills it and replaces the
plaintext `secret` column value with the placeholder `[encrypted]`.

The backfill migration reads the key from the Postgres session setting
`app.webhook_encryption_key`, which the webhooks service sets at startup from
the `WEBHOOK_ENCRYPTION_KEY` environment variable. **If this variable is not
set before the indexer runs migrations, the backfill will silently skip
existing rows and they will be undeliverable until re-created.**

**Action required — before deploying:**

```bash
# Generate a 256-bit key and record it somewhere safe (password manager, secret store)
openssl rand -hex 32
```

Then set it in your environment:

```bash
# Fly.io
fly secrets set WEBHOOK_ENCRYPTION_KEY="<64-char hex string>"

# Docker / local
export WEBHOOK_ENCRYPTION_KEY="<64-char hex string>"

# render.yaml — set the value via the Render dashboard when prompted (sync: false)
```

> [!IMPORTANT]
> The default value in older `.env` files (`"default-key-for-testing"` or
> `"change-this-to-a-secure-random-key-in-production"`) is **not secure**.
> Do not use it in production. Any deployment that does not set this variable
> keeps webhook payloads unencrypted on disk.

Once running, verify the backfill succeeded:

```sql
SELECT COUNT(*) FROM webhook_subscriptions WHERE encrypted_secret IS NULL AND secret != '[encrypted]';
-- Should return 0
```

If any rows remain un-migrated, the webhooks service will log a warning and
skip delivery for those subscriptions. Re-create them via `POST /webhooks` to
generate a fresh encrypted secret.

---

#### 2. `token_transfers.kind` column added — API response shape change

Migration `0015_sep41_balance_deltas.sql` adds a `kind` column to
`token_transfers` (values: `transfer` | `mint` | `burn` | `clawback`).

**API change:** `GET /contracts/:id/transfers` now includes a `kind` field on
every row. Existing rows default to `"transfer"`. If your client reads
transfer payloads by index (rather than by key name), update it to account for
the new field.

No manual database action is required — the `DEFAULT 'transfer'` backfill runs
inside the migration.

---

#### 3. Webhook delivery table schema change — `upgrade` subscriptions

Migration `0008_spec_versions.sql` makes `webhook_deliveries.event_id`
nullable and adds an `upgrade_id` column, so deliveries can target either an
event or a contract-spec upgrade. A `CHECK` constraint enforces exactly one is
set.

**This is a schema change on a table the webhooks service writes to
continuously.** For zero-downtime upgrades, stop the webhooks service before
deploying; it will resume correctly after migration.

No env var change is required for existing `event`-kind subscriptions — the
`kind` column defaults to `'event'` for all existing rows.

---

#### 4. GraphQL introspection and GraphiQL are **off** by default in production

The API now defaults `GRAPHQL_INTROSPECTION_ENABLED=false`. If you relied on
the GraphiQL IDE at `GET /graphql` in a non-development environment, set this
explicitly:

```bash
GRAPHQL_INTROSPECTION_ENABLED=true  # development / staging only
```

---

#### 5. CORS is now **same-origin only** by default

`CORS_ALLOWED_ORIGINS` is unset by default, meaning no `Access-Control-*`
headers are added. Previously, CORS was permissive (`*`) by default.

If your frontend is on a different origin from the API, set this explicitly:

```bash
CORS_ALLOWED_ORIGINS=https://yourdapp.com,https://app.yourdapp.com
# or, for development only:
CORS_ALLOWED_ORIGINS=*
```

---

### New environment variables

All new variables have safe defaults and are **optional** unless marked
**Required**.

| Variable | Default | Notes |
| --- | --- | --- |
| `WEBHOOK_ENCRYPTION_KEY` | *(none)* | **Required for webhooks.** Generate with `openssl rand -hex 32`. See breaking change #1 above. |
| `CORS_ALLOWED_ORIGINS` | *(unset — same-origin)* | Comma-separated origins, `*`, or unset. See breaking change #5 above. |
| `GRAPHQL_MAX_DEPTH` | `12` | GraphQL query depth limit. Prevents deeply nested DoS queries. |
| `GRAPHQL_MAX_COMPLEXITY` | `1000` | GraphQL query complexity limit. |
| `GRAPHQL_INTROSPECTION_ENABLED` | `false` | Set `true` in dev/staging to enable GraphiQL IDE. See breaking change #4 above. |
| `RATE_LIMIT_TRUST_XFF` | `false` | Trust `X-Forwarded-For` for rate limiting. Enable only behind a trusted proxy. |
| `RATE_LIMIT_BACKEND` | `memory` | `memory` (per-instance) or `redis` (global across replicas). |
| `REDIS_URL` | *(none)* | Required when `RATE_LIMIT_BACKEND=redis`. |
| `RPC_ROUTE_RATE_LIMIT_PER_MIN` | `10` | Separate, tighter rate limit for `/call` and `/simulate` routes. |
| `RPC_REQUIRE_API_KEY` | `false` | Require auth on `/call` and `/simulate` even when `REQUIRE_API_KEY=false`. |
| `RPC_TIMEOUT_SECS` | `30` | HTTP timeout for all outbound RPC calls (indexer + API). |
| `READYZ_LAG_THRESHOLD` | `100` | Max ledger lag for `/readyz` to return `200`. |
| `READYZ_MAX_AGE_SECS` | `120` | Max cursor age (seconds) for `/readyz` to return `200`. |
| `HEALTH_MAX_LAG_LEDGERS` | `100` | Max ledger lag for `/health` to show `"ok"` status. |
| `HEALTH_MAX_STALE_SECS` | `120` | Max cursor age for `/health` to show `"ok"` status. |
| `ENRICHMENT_WARN_THRESHOLD` | `0.5` | Warn if >N fraction of events fail enrichment in a poll cycle. `0.0` disables. |
| `SPEC_CACHE_MAX_ENTRIES` | `2000` | In-memory spec cache size. Reduce if memory is constrained. |
| `SPEC_VERSION_RETENTION` | `0` | Min interface versions to keep per contract when pruning. `0` = follow `RETENTION_LEDGERS`. |
| `KEY_TEMPLATES` | *(empty)* | JSON array of per-key indexing templates beyond the built-in balance tracker. |
| `BALANCE_KEY_SYMBOL` | `Balance` | Storage-key symbol for per-holder balance entries. |
| `BALANCE_KEY_DURABILITY` | `persistent` | Durability of balance entries (`persistent` or `temporary`). |
| `DATABASE_MAX_CONNECTIONS` | `10` | SQLx pool ceiling. Set per Postgres tier — see [Connection Pool Sizing](DEPLOYMENT.md#connection-pool-sizing). |
| `DATABASE_MIN_CONNECTIONS` | `1` | Connections kept warm at idle. |
| `DATABASE_ACQUIRE_TIMEOUT_SECS` | `30` | Fail a request rather than queue indefinitely. |
| `DATABASE_IDLE_TIMEOUT_SECS` | `600` | Reclaim idle connections after N seconds. |

---

### Database migrations applied (0010–0021)

These run automatically on indexer startup. No manual SQL is required unless
noted.

| Migration | What it does |
| --- | --- |
| `0010_hot_query_indexes.sql` | Adds composite indexes for hot query paths (events, transfers, contract data). Index-only, safe to apply online. |
| `0011_observability_metrics.sql` | Adds enrichment and RPC observability counters to `indexer_cursor`. |
| `0012_amm_swaps.sql` | Creates `amm_swaps` table for materialized AMM swap events. |
| `0013_nft_events.sql` | Creates `nft_events` table for materialized NFT mint/transfer/burn events. |
| `0014_liquidity_events.sql` | Creates `liquidity_events` table for materialized AMM liquidity events. |
| `0015_sep41_balance_deltas.sql` | Adds `kind` column to `token_transfers` (see breaking change #2). |
| `0016_tx_hash_index.sql` | Adds index on `events.tx_hash` for `/transactions/:hash/events` queries. |
| `0017_spec_versions_ledger.sql` | Adds `observed_at_ledger` to `contract_spec_versions` for retention. |
| `0017_webhook_enhancements.sql` | Adds encryption, auto-disable tracking, and backfill columns to `webhook_subscriptions`. Requires `WEBHOOK_ENCRYPTION_KEY` (see breaking change #1). |
| `0018_audit_log.sql` | Creates `audit_log` table for API key and webhook management auditing. |
| `0019_enriched_gin_index.sql` | Replaces the existing GIN index on `events.enriched` with a `jsonb_path_ops` variant for faster containment queries. Safe online — uses `IF NOT EXISTS`. |
| `0020_webhook_secret_encryption_backfill.sql` | Backfills `encrypted_secret` and clears plaintext `secret` (see breaking change #1). Requires `WEBHOOK_ENCRYPTION_KEY`. |
| `0021_contract_summaries_delete.sql` | Replaces the `INSERT`-only `contract_summaries` trigger with one that handles `INSERT`, `UPDATE`, and `DELETE` correctly, so retention pruning keeps counts accurate. |

---

### Recommended upgrade procedure (Unreleased → next release)

```bash
# 1. Generate and store the encryption key
openssl rand -hex 32   # save the output

# 2. Set secrets before deploy (example: Fly.io)
fly secrets set WEBHOOK_ENCRYPTION_KEY="<output from step 1>"

# 3. Stop webhooks first (schema change on webhook_deliveries)
fly scale count webhooks=0   # or docker stop / systemctl stop

# 4. Deploy new binaries — the indexer runs all pending migrations on startup
fly deploy            # or docker compose up --build / cargo build --release

# 5. Verify migrations ran
fly logs -i <indexer-instance>   # look for "migrations applied"
# Or query directly:
# SELECT version FROM _sqlx_migrations ORDER BY version;

# 6. Verify encryption backfill
psql $DATABASE_URL -c "SELECT COUNT(*) FROM webhook_subscriptions WHERE encrypted_secret IS NULL AND secret != '[encrypted]';"
# Should be 0

# 7. Start webhooks
fly scale count webhooks=1
```

---

## Fresh install / v0.1.0 (initial release)

This section is for operators starting from scratch or from the initial
`v0.1.0` tag.

### Database migrations applied (0001–0009)

These are applied automatically by the indexer on first startup.

| Migration | What it creates |
| --- | --- |
| `0001_init.sql` | `events` table and `indexer_cursor` (ledger-tracking row). Core schema. |
| `0002_production.sql` | `decoded_topics`/`decoded_value` columns on `events`; monotonic `seq` for webhook ordering; `webhook_state` watermark; `api_keys` table; `webhook_subscriptions` and `webhook_deliveries` tables. |
| `0003_materialized.sql` | `token_transfers` table for materialized SEP-41 transfer events. |
| `0004_contract_specs.sql` | `contract_specs` table (typed, self-describing interface cache); `enriched` column on `events`. |
| `0005_contract_state.sql` | `contract_state` table (versioned instance-storage snapshots). |
| `0006_contract_data.sql` | `contract_data` table (versioned per-key storage snapshots, e.g. holder balances). |
| `0007_retention.sql` | `idx_events_ledger` index to support `RETENTION_LEDGERS` pruning. |
| `0008_spec_versions.sql` | `contract_spec_versions` table (append-only interface history + diffs); `kind` column on `webhook_subscriptions`; nullable `event_id` and new `upgrade_id` on `webhook_deliveries` for upgrade webhooks; `last_upgrade_id` watermark on `webhook_state`. |
| `0009_contract_summaries.sql` | `contract_summaries` table (denormalized event counts for fast `GET /contracts`); trigger and backfill. |

### Required environment variables for v0.1.0

| Variable | Example | Notes |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://user:pass@host/db?sslmode=require` | Postgres 14+. Must include `?sslmode=require` for managed instances. |
| `RPC_URL` | `https://mainnet.sorobanrpc.com` | Soroban RPC endpoint for your target network. |

All other variables have working defaults. See [Configuration](../README.md#configuration) for the full list.

---

## General notes

### How migrations work

Lumenqraph uses [SQLx offline migrations](https://docs.rs/sqlx/latest/sqlx/macro.migrate.html).
The indexer binary embeds and applies all migrations on startup before polling
begins. Migration state is tracked in the `_sqlx_migrations` table.

- Migrations are **forward-only**. There is no automatic rollback.
- For a rollback strategy (point-in-time restore, manual revert scripts), see
  [docs/MIGRATIONS.md](MIGRATIONS.md).
- If a migration fails mid-apply, the indexer will exit with a clear error.
  Fix the root cause (usually a missing env var or connection issue), then
  restart — SQLx will resume from the failed migration.

### Upgrade ordering

Always upgrade in this order to avoid downtime:

1. **Indexer** — runs migrations, resumes ingestion.
2. **API** — once the indexer is healthy at `/health`.
3. **Webhooks** — last, after schema migrations are confirmed complete.

For rolling deployments behind a load balancer, ensure the indexer has
finished all migrations before routing traffic to the new API instances.

### Checking which migrations have run

```sql
SELECT version, description, installed_on
FROM _sqlx_migrations
ORDER BY version;
```

### Verifying indexer health after upgrade

```bash
curl https://<your-host>/health
# Look for: "status": "ok", "lag_ledgers": < 100
```

### Rolling back a bad deploy

Lumenqraph does not ship automated down-migrations. Options:

1. **Restore a Postgres snapshot** taken before the upgrade (recommended for
   breaking schema changes).
2. **Redeploy the previous binary** — it will refuse to start if the database
   is ahead of its known migrations, so a snapshot is required if schema
   changes were applied.
3. See [docs/MIGRATIONS.md](MIGRATIONS.md) for detailed rollback strategies.
