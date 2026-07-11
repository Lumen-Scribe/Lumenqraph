# Deployment

## Processes

Run three long-lived processes against one Postgres:

| Process | Command | Notes |
| --- | --- | --- |
| Indexer | `lumenqraph-indexer` | Must run 24/7 — a sleeping poller falls behind the chain. Applies migrations on startup. |
| API | `lumenqraph-api` | Stateless; scale horizontally behind a load balancer. |
| Webhooks | `lumenqraph-webhooks` | Single instance is fine; delivery is idempotent per (subscription, event). |

Only the indexer applies migrations. For an API/webhooks-only deploy, run
`scripts/setup_db.sh` first.

## Docker

```bash
docker compose -f docker-compose.full.yml up --build -d
```
One image holds all three binaries; each service overrides `command:`.

## Postgres

Any Postgres 14+ works. For managed hosting, point `DATABASE_URL` at Neon or
Supabase (survives independently of the app host). Add read replicas for the API
before scaling the write path.

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

`getEvents` serves only ~7 days of history, so backfill is bounded by RPC
retention. Deep historical backfill requires a captive-core / data-lake source
(not yet implemented).
