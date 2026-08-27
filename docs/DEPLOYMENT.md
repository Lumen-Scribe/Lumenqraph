# Deployment

## Processes

Run three long-lived processes against one Postgres:

| Process | Command | Notes |
| --- | --- | --- |
| Indexer | `lumenqraph-indexer` | Must run 24/7 — a sleeping poller falls behind the chain. Applies migrations on startup. |
| API | `lumenqraph-api` | Stateless; scale horizontally behind a load balancer. |
| Webhooks | `lumenqraph-webhooks` | Single instance is fine; delivery is idempotent per (subscription, event). |

Only the indexer applies migrations (on startup, via `sqlx::migrate!`). Deploy
so the indexer starts first, or run `scripts/setup_db.sh` for an
API/webhooks-only deploy. The **API also serves the static explorer UI** at `/`
(same origin — no CORS, no configured API base); point `EXPLORER_DIR` at the
assets (the Docker image ships them at `/app/explorer`).

## Docker

```bash
docker compose -f docker-compose.full.yml up --build -d
```
One image holds all three binaries; each service overrides `command:`.
## Managed Deploy (Fly.io)

Fly.io is the recommended hosting platform for running Lumenqraph in production. The repository ships with a pre-configured [`fly.toml`](../fly.toml) that defines three distinct process groups:
1. `api`: Exposes the public REST/GraphQL API and serves the static explorer UI.
2. `indexer`: The worker that tails Soroban RPC and writes to Postgres.
3. `webhooks`: The worker that dispatches HTTP callback POSTs to subscribers.

### Step-by-Step Fly.io Walkthrough

#### 1. Initialize Fly App
Run the following command to initialize the application configuration. Fly.io will prompt you to choose an app name and default region:
```bash
fly launch --no-deploy --copy-config
```
Do not deploy immediately (`--no-deploy`) as you need to configure secrets and databases first.

#### 2. Set Up Managed Postgres
Provision a managed Postgres instance (e.g., via Neon, Supabase, or Fly Postgres). Ensure TLS is required and copy the connection string.
> [!IMPORTANT]
> The indexer must apply schema migrations on startup before the API and webhooks start. When using Fly.io, the `indexer` process group automatically runs migrations using built-in `sqlx::migrate!` on boot. 
> If you are deploying only the `api` or `webhooks` without the `indexer`, you must run database setup manually beforehand using `scripts/setup_db.sh`.

#### 3. Configure Secrets
Set the mandatory secret environment variables:
```bash
# Point to your managed Postgres (append ?sslmode=require)
fly secrets set DATABASE_URL="postgres://user:password@host:port/dbname?sslmode=require"

# (Optional) Comma-separated allowlist of contracts to index (empty = index all)
fly secrets set CONTRACT_IDS="CAS3J7GY...,CDZZWCAJ..."

# (Optional) Set an API key if REQUIRE_API_KEY=true is configured
fly secrets set API_KEY="your-secure-api-key"
```

#### 4. Configure Non-Secret Env Vars in `fly.toml`
Verify and adjust the variables in the `[env]` section of your `fly.toml`:
- `RPC_URL`: Set to a reliable Soroban RPC provider (e.g., `"https://mainnet.sorobanrpc.com"`).
- `MAX_CATCHUP_LEDGERS`: Set to `"120"` for public RPC rates, or higher for paid RPCs.

#### 5. Scaling and Availability Configuration
The indexer and webhooks workers must run as single instances to prevent database write conflicts and duplicate webhook dispatches. The API can scale horizontally.
In `fly.toml`, ensure the following under `[http_service]`:
```toml
auto_stop_machines = false
auto_start_machines = true
min_machines_running = 1
processes = ["api"]
```
To scale the API horizontally:
```bash
fly scale count api=2
```
Keep the indexer and webhooks at a single instance:
```bash
fly scale count indexer=1 webhooks=1
```
To adjust resources for individual processes:
```bash
fly scale vm shared-cpu-1x --memory 1024 --process-group indexer
fly scale vm shared-cpu-1x --memory 512 --process-group api
```

#### 6. Deploy
Deploy the configuration:
```bash
```
Once deployed, check health and access the explorer UI:
```bash
curl https://<your-app>.fly.dev/health
open https://<your-app>.fly.dev/
```

### Multi-Replica Rate Limiting

When running multiple API replicas behind a load balancer, the default in-memory rate limiter enforces limits **per instance** — a client can multiply their effective allowance by the number of replicas.

To enforce **global rate limits across all replicas**, use the Redis-backed rate limiter:

1. **Provision a Redis instance** (e.g., Fly Redis, Upstash, or any managed Redis)
2. **Set the backend configuration:**

```bash
fly secrets set RATE_LIMIT_BACKEND="redis"
fly secrets set REDIS_URL="redis://your-redis-host:6379"
```

3. **Deploy:**

```bash
fly deploy
```

The Redis backend uses a sliding window algorithm with atomic Lua scripts to ensure accurate, globally-consistent rate limiting. If Redis becomes unavailable, the rate limiter fails open (allows requests) to preserve API availability.

For single-instance deployments or when per-instance limits are acceptable, keep the default `RATE_LIMIT_BACKEND=memory` (no Redis required).

---
---

## Free-Tier Deploy (Render + Supabase)

For a zero-cost demonstration and quick evaluation, you can host Lumenqraph on Render's free tier paired with a free Supabase Postgres instance. 

### Free Tier Architecture Tradeoffs
Render's free tier has several limits that shape how we deploy:
- **Single Process Limit**: Render's free tier does not support background workers. The free account monthly allowance of 750 instance-hours covers exactly one running service (~730 hours/month).
- **All-in-One Container**: We bypass the single-service limit by using [`scripts/run-all-in-one.sh`](../scripts/run-all-in-one.sh). This script spins up the `indexer` and `api` together in the same container. Webhooks are omitted to conserve resources.
- **Auto Spin-Down**: Free Render web services are spun down after 15 minutes of inbound HTTP inactivity. This stops the indexer completely.
- **Database Storage Limits**: Supabase free Postgres caps database size at 500MB. Unrestricted indexing fills this up in hours.

### Step-by-Step Render Walkthrough

#### 1. Provision Supabase Database
1. Create a free account at [Supabase](https://supabase.com).
2. Create a new project.
3. Go to **Project Settings → Database → Connection string** and copy the URI. Remember to append `?sslmode=require`.

#### 2. Deploy Blueprint on Render
1. Fork the Lumenqraph repository on GitHub.
2. Go to your [Render Dashboard](https://dashboard.render.com).
3. Click **New → Blueprint**.
4. Select your fork of the Lumenqraph repository.
5. Render will automatically parse [`render.yaml`](../render.yaml). You will be prompted to input:
   - `DATABASE_URL`: The Supabase connection string.
   - `CONTRACT_IDS`: **Must** be a focused allowlist of contracts. **Do not** leave this empty or include high-frequency contracts (like the Stellar Asset Contract `CAS3J7GY...` which generates millions of events daily and will fill the 500MB cap in hours).

#### 3. Prevent Inactivity Spin-Down (Keep-Alive Cron)
Since Render will sleep the container if no HTTP requests are received, you must ping the health check endpoint.
1. Create a free account at an external cron provider (e.g., [cron-job.org](https://cron-job.org)).
2. Configure a cron job targeting `https://<your-render-subdomain>.onrender.com/health`.
3. Set the schedule to run **every 10 minutes**. This keeps the container awake and indexing continuously.

#### 4. Configure Testnet & Mainnet Dual-Indexing
You can index both Stellar Mainnet and Testnet using a single Render container:
1. In your Supabase SQL Editor, create a second database:
   ```sql
   CREATE DATABASE lumenqraph_testnet;
   ```
2. In Render environment settings, configure:
   - `TESTNET_DATABASE_URL`: The same connection string as before, but with the database name swapped to `lumenqraph_testnet`.
   - `TESTNET_CONTRACT_IDS`: A focused contract allowlist for testnet.
3. `scripts/run-all-in-one.sh` detects `TESTNET_DATABASE_URL` and starts a testnet API/indexer pair internally, proxying it via `INSTANCE_MOUNTS` under `/testnet`. The explorer UI will automatically detect the sibling network mount via `/health` and display a network switcher.

#### 5. Moving to a Paid Production Plan
When ready to move to separate, robust services:
1. Delete or disable the Render Blueprint setup on the free plan.
2. Provision a Render Web Service for the API, a Background Worker for the Indexer, and a Background Worker for Webhooks.
3. Remove the `run-all-in-one.sh` override and configure individual container start commands (`lumenqraph-api`, `lumenqraph-indexer`, `lumenqraph-webhooks`).
4. Upgrade to a paid Supabase plan or custom managed Postgres to lift the 500MB limit.

## Production Checklist

- [ ] `DATABASE_URL` → managed Postgres with TLS (`sslmode=require`).
- [ ] `RPC_URL` set (paid/retaining RPC if you need backfill or higher limits).
- [ ] `CONTRACT_IDS` = your allowlist, or intentionally empty to index all.
- [ ] `REQUIRE_API_KEY=true` to require `x-api-key` on data routes (`/health` +
      `/metrics` stay open); issue keys via the `api_keys` table. Leave `false`
      only if the read-only chain data is meant to be public.
- [ ] `ANON_RATE_LIMIT_PER_MIN` tuned (default 60/min/IP; per-instance — see below).
- [ ] Indexer pinned 24/7 (`auto_stop_machines=false`, `min_machines_running=1`).
- [ ] Scrape `/metrics`; alert on lag (`lumenqraph_indexer_lag_seconds` > 600s for warning,
      > 3600s for critical) and error rates. See the **Observability** section above
      for recommended thresholds and key metrics.

## Connection Pool Sizing

Each service opens its own pool. Four env vars control every pool (the defaults
below are per-service):

| Env var | Indexer default | API default | Webhooks default | Notes |
| --- | --- | --- | --- | --- |
| `DATABASE_MAX_CONNECTIONS` | 5 | 10 | 5 | Hard ceiling; capped by the DB tier. |
| `DATABASE_MIN_CONNECTIONS` | 1 | 1 | 1 | Connections kept warm at idle. |
| `DATABASE_ACQUIRE_TIMEOUT_SECS` | 30 | 30 | 30 | Fail a request rather than queue indefinitely. |
| `DATABASE_IDLE_TIMEOUT_SECS` | 600 | 600 | 600 | Reclaim idle connections after this many seconds. |

**Sizing guidance for managed tiers**

Managed databases have hard connection caps (Neon free: 25, Supabase free: 60,
Render free Postgres: 25). Running all three services plus the managed pooler
counts against the same cap. A safe starting point for free tiers:

```
DATABASE_MAX_CONNECTIONS=3   # indexer — writes only, low concurrency
DATABASE_MAX_CONNECTIONS=8   # api     — concurrent reads; scale up with API replicas
DATABASE_MAX_CONNECTIONS=2   # webhooks — delivery is serialised per subscription
```

On paid plans (Neon Standard 100 conn, Supabase Pro 60 direct / PgBouncer
unlimited): raise the API pool first; the indexer and webhooks are single-writer
processes and rarely benefit from more than 5–10 connections each.

## Postgres

Any Postgres 14+ works. For managed hosting, point `DATABASE_URL` at Supabase or
Neon (survives independently of the app host). Add read replicas for the API
before scaling the write path.

Two free tiers are traps for *this* workload, because a 24/7 indexer writes
constantly and so never idles:

- **Neon free** bills 100 CU-hours/project/month. A compute that never scales to
  zero burns ~182 CU-h (730h × 0.25 CU), so the database suspends itself partway
  through every month. Fine on a paid plan, or for the API alone.
- **Render free Postgres** is deleted 30 days after creation.

## Scaling notes

- **RPC** — the public SDF endpoint is rate-limited; move to a paid provider as
  event volume grows. Lower `POLL_INTERVAL_SECS` only alongside more RPC budget.
- **API rate limiting** is in-memory (per instance). Running multiple API
  instances means limits are per-instance; move the limiter to Redis for a
  global limit.
- **Caching** — put Redis in front of hot read paths (e.g. latest state) when
  traffic warrants; Postgres alone is fine to start.

## Observability

Scrape `GET /metrics`. The following are the key indexer health signals:

### Lag metrics

The indexer's position relative to the chain tip is exported as two metrics:

- `lumenqraph_indexer_lag_ledgers` (gauge) — ledgers behind the chain tip
  (computed as `chain_tip_ledger - last_processed_ledger`).
- `lumenqraph_indexer_lag_seconds` (gauge) — estimated time behind in seconds,
  derived as `lag_ledgers × ~5s/ledger`. Use this for human-friendly alerting.

**Recommended alert thresholds:**

- **Warning** (`lag_seconds > 600`, ~10 min): The indexer is falling behind at
  a visible rate. Check indexer logs for errors, RPC quota issues (`rpc_errors_32001_total`),
  or DB write latency spikes.
- **Critical** (`lag_seconds > 3600`, ~1 hour): The indexer is stalled or has hit
  an unrecoverable error. Immediate action needed; check error counts and logs.
- **Sev0** (`lag_seconds > 86400`, ~24 hours): The indexer is far behind the
  retention window; gaps in history are now unrecoverable via public RPC. This
  requires backfill from a retaining RPC or data-lake source.

### Error and enrichment metrics

- `lumenqraph_indexer_errors_total` — poll-cycle errors (check rate for spikes).
- `lumenqraph_events_enriched_total` / `lumenqraph_events_not_enriched_total` —
  enrichment coverage (calculate `enriched / (enriched + not_enriched)` as a
  percentage; a drop may indicate missing specs or spec-fetching issues).
- `lumenqraph_spec_fetch_failures_total` — failed contract spec fetches.
- `lumenqraph_rpc_errors_32001_total` — RPC quota-limit hits (indicates sustained
  load pressure; may need higher RPC plan or longer `POLL_INTERVAL_SECS`).

### Monitoring Setup

Ship-ready Prometheus alert rules and Grafana dashboards are included in the
[`monitoring/`](../monitoring/) directory:

- **`prometheus_alerts.yml`** — Pre-configured alerts for indexer lag, error
  rates, stalled state, and API health.
- **`grafana_dashboard.json`** — Ready-to-import dashboard covering lag, event
  ingestion rate, error rate, and API throughput.
- **`prometheus.yml`** — Sample Prometheus config for scraping the metrics
  endpoint.
- **`README.md`** — Full setup and customization guide.

#### Quick Start

1. **Deploy Prometheus** with the alert rules:
   ```bash
   docker run -d \
     -v $(pwd)/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml \
     -v $(pwd)/monitoring/prometheus_alerts.yml:/etc/prometheus/prometheus_alerts.yml \
     -p 9090:9090 \
     prom/prometheus
   ```

2. **Deploy Grafana**:
   ```bash
   docker run -d -p 3000:3000 grafana/grafana
   ```
   Default login: `admin` / `admin`.

3. **Add Prometheus data source** in Grafana:
   - Settings → Data Sources → Add Prometheus
   - URL: `http://prometheus:9090`

4. **Import the dashboard**:
   - Create → Import
   - Upload `monitoring/grafana_dashboard.json`

#### Metrics Overview

| Metric | Type | Description |
| --- | --- | --- |
| `lumenqraph_indexer_lag_ledgers` | Gauge | Ledgers behind chain tip |
| `lumenqraph_indexer_last_processed_ledger` | Gauge | Most recent ledger processed |
| `lumenqraph_indexer_chain_tip_ledger` | Gauge | Latest ledger on chain |
| `lumenqraph_indexer_ingested_total` | Counter | Total events ingested |
| `lumenqraph_indexer_errors_total` | Counter | Total poll-cycle errors |
| `lumenqraph_api_requests_total` | Counter | Total API requests served |
| `lumenqraph_events_total` | Gauge | Total events in database |

#### Alert Rules

| Alert | Threshold | Duration | Severity |
| --- | --- | --- | --- |
| IndexerLagHigh | lag > 100 ledgers | 5 min | warning |
| IndexerLagCritical | lag > 500 ledgers | 2 min | critical |
| IndexerStalled | No ingestion × lag exists | 5 min | critical |
| IndexerErrorRateHigh | error rate > 1% | 2 min | warning |
| LargeLagGrowth | lag growth > 1000 ledgers/hour | 5 min | warning |
| IngestRateLow | < 1 event/sec | 10 min | warning |
| APINoRequests | No requests | 5 min | warning |

Tune thresholds in `monitoring/prometheus_alerts.yml` to fit your SLA.

## Limits

`getEvents` serves only ~7 days of history, and public RPCs reject a request
whose `startLedger` is more than a few thousand ledgers behind the tip
(`-32001` "processing limit"). So the indexer caps each catch-up at
`MAX_CATCHUP_LEDGERS` (default 4000, ~5–6h): if the cursor falls further behind
(e.g. after downtime), it **skips ahead to that window and logs the
unrecoverable gap** rather than stalling forever on an impossible range. Deep
or gapless historical backfill requires a retaining/paid RPC or a
Galexie/captive-core data-lake source (not yet implemented); with one, raise
`MAX_CATCHUP_LEDGERS`.
