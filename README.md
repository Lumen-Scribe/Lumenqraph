# Lumenqraph

An open-source, self-hostable **Soroban event indexer** for Stellar. It tails
contract events from Soroban RPC, decodes their XDR to JSON, stores them in
Postgres, and serves them over a plain REST API + webhooks — *curl and get
JSON*, no VM or custom program to deploy.

> **Differentiation vs. Mercury/Allium:** simplicity + self-hostability. Register
> a contract, hit one HTTP endpoint (or a webhook). The code is inspectable and
> runs on your own infra.

## Features

- **Full XDR → JSON decoding** — ScVal decoded to friendly JSON: i128/u128 as
  decimal strings, addresses as `G…`/`C…` strkeys, bytes as hex, vecs/maps as
  arrays/objects. Raw base64 always retained (lossless).
- **Materialized token transfers** — SEP-41 `transfer` events projected into a
  queryable `from/to/amount` table.
- **REST API** — contracts, events (filterable), transfers, health, metrics.
- **Webhooks** — register a URL + filter, receive HMAC-signed event pushes with
  retry + exponential backoff.
- **Auth & rate limiting** — SHA-256-hashed API keys, per-key limits.
- **Observability** — Prometheus `/metrics`, `/health` with chain-tip lag.
- **Backfill mode** — one-shot historical catch-up (bounded by RPC retention).
- **Idempotent + reorg-tolerant** ingestion; graceful shutdown; Dockerized.

## Architecture

A Rust workspace: three service binaries sharing one core crate. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

| Crate | Role |
| --- | --- |
| `lumenqraph-core` | Shared models, errors, and the XDR→JSON / strkey decoder. |
| `lumenqraph-indexer` | Always-on: polls `getEvents`, decodes, writes Postgres. |
| `lumenqraph-api` | Axum read + management API (auth, rate limiting, metrics). |
| `lumenqraph-webhooks` | Matches events to subscriptions, delivers signed pushes. |

```
Soroban RPC ─poll─> [indexer] ─> Postgres <─ [api] ─REST─> dApps
                                     ▲
                                  [webhooks] ─signed POST─> subscribers
```

## Quick start

```bash
cp .env.example .env
docker compose up -d                    # local Postgres
cargo run -p lumenqraph-indexer         # polls + auto-migrates
cargo run -p lumenqraph-api             # REST on :8080
cargo run -p lumenqraph-webhooks        # optional: webhook delivery
```

```bash
curl localhost:8080/health
curl localhost:8080/contracts
curl 'localhost:8080/contracts/<CID>/events?event_name=transfer&limit=5'
curl 'localhost:8080/contracts/<CID>/transfers?limit=5'
curl localhost:8080/metrics
```

Full stack in Docker: `docker compose -f docker-compose.full.yml up --build`.
Common tasks: `make help`.

## Docs

- [API reference](docs/API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Deployment](docs/DEPLOYMENT.md)
- Minimal read UI: open [`explorer/index.html`](explorer/index.html).

## Configuration

See [`.env.example`](.env.example): `DATABASE_URL`, `RPC_URL`, `CONTRACT_IDS`
(empty = all), `POLL_INTERVAL_SECS`, `REQUIRE_API_KEY`, `ANON_RATE_LIMIT_PER_MIN`,
`WEBHOOK_*`.

Generate an API key: `DATABASE_URL=... ./scripts/gen_api_key.sh myapp pro 600`.
Backfill from a ledger: `cargo run -p lumenqraph-indexer -- backfill <ledger>`.

## Known limitations

- **History bounded by RPC retention** (~7 days). Deep backfill needs a
  captive-core / data-lake source (not yet built); `START_LEDGER` is clamped.
- **Rate limiter is per-instance** (in-memory) — use Redis for a global limit
  across multiple API replicas.

## Deployment note

The indexer must run 24/7 — a sleeping poller falls behind the chain. Use an
always-on host, not a free tier that idles.

## License

MIT — see [LICENSE](LICENSE).
