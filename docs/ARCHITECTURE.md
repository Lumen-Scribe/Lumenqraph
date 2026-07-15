# Architecture

Lumenqraph is a Rust workspace of four crates: three service binaries plus a
shared library.

```
                 ┌───────────────────────────────────────────┐
                 │                lumenqraph-core             │
                 │  models · XDR→JSON decode · strkey · errors│
                 └───────────────────────────────────────────┘
                     ▲              ▲                    ▲
                     │              │                    │
   Soroban RPC ──poll─┤   ┌─────────┴─────────┐   ┌──────┴────────┐
  (getEvents)         │   │  lumenqraph-api    │   │ lumenqraph-   │
        ┌─────────────┴─┐ │  (Axum, read+mgmt) │   │ webhooks      │
        │ lumenqraph-   │ └─────────┬──────────┘   │ (delivery)    │
        │ indexer       │           │              └──────┬────────┘
        │ (ingest+decode│           │                     │
        └───────┬───────┘           │                     │
                │  write            │ read                │ read/write
                ▼                   ▼                     ▼
             ┌──────────────────── Postgres ─────────────────────┐
             │ events · token_transfers · indexer_cursor         │
             │ api_keys · webhook_subscriptions · deliveries     │
             └───────────────────────────────────────────────────┘
```

## Why separate binaries

Each service scales, restarts, and fails independently:

- A spike in **API** traffic can't stall **ingestion**.
- A decode bug in the **indexer** can't take down the public read path.
- **Webhook** retries/backoff are isolated from request latency.

They coordinate only through Postgres — no direct RPC between services.

## Data flow

1. **Indexer** polls `getEvents` from its cursor to the chain tip, decodes each
   event's XDR (`core::xdr`), and writes `events` idempotently (`ON CONFLICT
   (event_id) DO NOTHING`). `transfer` events are projected into
   `token_transfers`. The cursor row also records the chain tip and counters.
2. **API** serves reads (`/contracts`, `/events`, `/transfers`), observability
   (`/health`, `/metrics`), and webhook management — behind API-key auth +
   rate limiting on data routes.
3. **Webhooks** streams two sources — new events by monotonic `events.seq`, and
   new contract upgrades by `contract_spec_versions.id` — matches each to active
   subscriptions of the corresponding `kind`, and delivers HMAC-signed payloads
   with exponential backoff. The two streams keep separate watermarks, so a quiet
   period in one can't stall the other.

Alongside (1), the indexer reads each tracked contract's instance entry when
`UPGRADE_WATCH` or `STATE_INDEXING` is on. That entry reveals the contract's
current executable hash: if it changed, the contract was upgraded in place, so
the interface is re-read and appended to `contract_spec_versions` with a semantic
diff against the previous version (`core::diff`). Both features read the same
entry, so enabling both costs one call per contract per cycle, not two.

## Decoding

`core::xdr` decodes the ScVal wire format directly (no `stellar-xdr` dep):
integers → JSON numbers or decimal strings (i128/u128 via native Rust 128-bit),
symbols/strings → strings, addresses → `G…`/`C…` strkeys (base32 +
CRC16-XModem), bytes → hex, vecs/maps → arrays/objects. Raw base64 is always
retained alongside the decoded JSON, so decoding is never lossy.

## Idempotency & reorgs

All writes key on the unique event `id`, so re-fetching a ledger (restart, retry,
shallow reorg) never double-counts. Stellar finality is fast, so deep reorgs are
not a practical concern.
