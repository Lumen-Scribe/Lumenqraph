# Architecture

Lumenqraph is a Rust workspace of five crates: four service binaries plus a
shared library.

```
                 ┌───────────────────────────────────────────┐
                 │                lumenqraph-core             │
                 │  models · XDR→JSON decode · strkey · errors│
                 └───────────────────────────────────────────┘
                     ▲              ▲          ▲              ▲
                     │              │          │              │
   Soroban RPC ──poll─┤   ┌─────────┴──┐ ┌────┴───────┐ ┌────┴────────┐
  (getEvents)         │   │lumenqraph- │ │lumenqraph- │ │lumenqraph-  │
        ┌─────────────┴─┐ │api         │ │webhooks    │ │mcp          │
        │ lumenqraph-   │ │(Axum,      │ │(delivery)  │ │(read-only   │
        │ indexer       │ │read+mgmt)  │ │            │ │MCP server)  │
        │ (ingest+decode│ └─────────┬──┘ └────┬───────┘ └────┬────────┘
        └───────┬───────┘           │         │             │
                │  write            │ read    │ read/write  │ read
                ▼                   ▼         ▼             ▼
             ┌──────────────────── Postgres ─────────────────────┐
             │ events · token_transfers · indexer_cursor         │
             │ contract_spec_versions · contract_state           │
             │ contract_data · contract_summaries                │
             │ api_keys · webhook_subscriptions · deliveries     │
             │ webhook_state                                     │
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

## Spec cache invalidation

Both the indexer and the API keep their own in-memory cache of a contract's
parsed interface (`indexer::specs::SpecCache` and `api::specs::SpecCache`
respectively) — the API needs it for `/functions`, `/call`, `/simulate`, and
diff endpoints; the indexer needs it to enrich events without re-parsing on
every one. They are separate processes with separate caches, so an upgrade
detected by the indexer is not automatically visible to the API.

Rather than rely on a TTL — which would leave a window where the API serves a
stale interface after the indexer has already recorded the new one — the API's
cache revalidates against Postgres on every lookup, keyed on both
`contract_specs.wasm_hash` and `contract_specs.fetched_at`. `wasm_hash`
changing is what actually signals an upgrade; `fetched_at` is compared
alongside it so any write to that row is caught even in cases hash equality
alone wouldn't cover (e.g. a hash collision, or the row being rewritten by a
future code path this cache doesn't know about). Together they mean any write
the indexer makes is visible to the API's cache on its very next lookup, with
no staleness window to tune.

This costs one small, indexed query per lookup (`wasm_hash`, `fetched_at`) —
the section itself (large, and requiring an XDR re-parse) is only re-fetched
when that comparison actually detects a change.

## Idempotency & reorgs

### Guarantee and limitations

All writes key on the unique event `id`, so re-fetching a ledger never double-counts.
However, **the idempotency guarantee only prevents double-counting; it does not handle
content changes** — if the RPC returns different event content for a ledger we already
stored, the stored copy will silently diverge from canonical.

**Deep reorgs** (the cursor falling behind the tip and needing to re-scan old ledgers)
are rare due to Stellar's finality. The public RPC typically retains ~120,000 ledgers
and rejects `getEvents` requests where `startLedger` is further behind.

**Shallow reorgs** (the RPC returning different events for a recently-closed ledger)
depend on the RPC's reorg exposure and whether `event_id` is stable across reorgs.
**Stellar RPC behavior here is not formally documented**; this implementation assumes
event content can change slightly if a reorg occurs within the last few ledgers.

### Mitigation: trailing re-scan

To handle shallow reorgs, enable `REORG_OVERLAP_LEDGERS` (default 0, disabled).
Each cycle, the indexer re-fetches the last N ledgers and upserts events,
updating mutable fields (`decoded_value`, `enriched`) if content changed.
The dedupe on `event_id` still prevents double-counting during this re-scan.

**Trade-offs:**
- Small values (10–100 ledgers) provide shallow reorg protection with minimal overhead.
- Larger values reduce reorg exposure but increase RPC requests and latency.
- Requires careful tuning based on observed RPC behavior and your SLA for event exactness.

If shallow reorgs cannot occur on your RPC, leave `REORG_OVERLAP_LEDGERS` at 0.
