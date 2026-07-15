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

## Managed deploy (Fly.io)

The repo ships a ready [`fly.toml`](../fly.toml): one image, three process
groups (`api` serves HTTP + explorer; `indexer` and `webhooks` are always-on
workers), health-checked on `/health`.

```bash
fly launch --no-deploy --copy-config       # reuse fly.toml; pick your app name
fly secrets set DATABASE_URL='postgres://…?sslmode=require'
fly secrets set CONTRACT_IDS='CAS3J7GY…,CDZZWCAJ…'   # optional; empty = index all
fly deploy
curl https://<app>.fly.dev/health          # {"status":"ok","lag_ledgers":0,…}
open  https://<app>.fly.dev/                # explorer UI
```

Scale groups independently, e.g. `fly scale vm shared-cpu-2x --process-group indexer`.
Fly requires a card on file; machines on a trial org are force-stopped after
5 minutes, which the indexer cannot tolerate. For a card-free deploy, see below.

## Free-tier deploy (Render + Supabase)

For a standing demo at zero cost and no credit card. The repo ships a ready
[`render.yaml`](../render.yaml). Push to GitHub, then **Render Dashboard → New →
Blueprint →** pick the repo; you'll be prompted for `DATABASE_URL` and
`CONTRACT_IDS`.

Get `DATABASE_URL` from **Supabase → Project Settings → Database → Connection
string** (append `?sslmode=require`). Free Supabase pauses a project only after
7 days of *inactivity*, which a running indexer prevents.

This tier constrains the architecture in three ways, all handled by the
blueprint but worth understanding:

- **One service, not three.** Render's free plan has no background-worker type,
  and 750 instance-hours/month covers exactly one always-on service (a month is
  ~730h). So `dockerCommand: run-all-in-one.sh` runs the indexer *inside* the web
  service. Webhooks are dropped — add them on a paid plan or a second host.
- **A keep-alive ping is mandatory.** Free web services spin down after 15
  minutes without inbound traffic, which stops the indexer with them. Point a
  free external cron (e.g. cron-job.org) at `https://<app>.onrender.com/health`
  every 10 minutes. Without it the index silently stops at the first idle gap.
- **Data volume must be bounded.** Supabase free caps at 500MB, and writes start
  failing at the cap. Two settings keep the footprint flat:
  - `CONTRACT_IDS` **must** be an allowlist, and **must not** include
    `CAS3J7GY…` — that SAC emits ~550 events/ledger (~9.5M/day) and would fill
    500MB within hours.
  - `RETENTION_LEDGERS=34560` (~2 days) prunes older history as the tip advances.
    Watch actual disk use in Supabase before raising it; 120960 (~7 days) is the
    ceiling worth having, since public RPC can't backfill further back anyway.

Expect gaps after any restart: `MAX_CATCHUP_LEDGERS=120` makes the indexer skip
and log a gap rather than stall, because public RPC rejects a `getEvents` more
than a few thousand ledgers behind the tip. That's the honest trade for a free
demo — a continuous index needs an always-on worker and a retaining RPC.

## Production checklist

- [ ] `DATABASE_URL` → managed Postgres with TLS (`sslmode=require`).
- [ ] `RPC_URL` set (paid/retaining RPC if you need backfill or higher limits).
- [ ] `CONTRACT_IDS` = your allowlist, or intentionally empty to index all.
- [ ] `REQUIRE_API_KEY=true` to require `x-api-key` on data routes (`/health` +
      `/metrics` stay open); issue keys via the `api_keys` table. Leave `false`
      only if the read-only chain data is meant to be public.
- [ ] `ANON_RATE_LIMIT_PER_MIN` tuned (default 60/min/IP; per-instance — see below).
- [ ] Indexer pinned 24/7 (`auto_stop_machines=false`, `min_machines_running=1`).
- [ ] Scrape `/metrics`; alert on `lumenqraph_indexer_lag_ledgers`.

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

Scrape `GET /metrics`. Alert on `lumenqraph_indexer_lag_ledgers` climbing (the
indexer is falling behind) and on `lumenqraph_indexer_errors_total` rate.

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
