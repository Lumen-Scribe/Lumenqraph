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

## Database Invariants

A handful of schema constructs carry load-bearing invariants that are not
obvious from the column definitions alone. A migration that changes any of them
without preserving the property described here will break a service at runtime,
usually silently. They are collected here so a schema change can be checked
against the list.

| Construct | Invariant | Why it exists | What breaks if violated |
|-----------|-----------|---------------|-------------------------|
| **`trg_update_contract_summary`** trigger on `events` | Every `INSERT` / `UPDATE` / `DELETE` on `events` adjusts the matching `contract_summaries` row in the same statement, so `contract_summaries` is always an exact aggregate of `events` — never a cache that can drift. | `GET /contracts` reads `contract_summaries` by primary key instead of running a `GROUP BY` over the whole `events` table (see the next section). | Disabling or narrowing the trigger, or bulk-loading `events` with the trigger off, makes `contract_summaries` diverge: `/contracts` then reports wrong `event_count` / ledger bounds, or lists contracts whose events were all pruned. Bulk loads must re-run the reconciliation in `migrations/0021_contract_summaries_delete.sql` or `DELETE FROM contract_summaries` and let it rebuild. |
| **`events.seq`** (`BIGSERIAL`, unique) | Assigned strictly increasing in insert order and never reused or reordered. It is *not* the ordering key of anything the indexer does — it exists purely so a downstream reader can stream new rows with a single high-water mark. `event_id` (the RPC id) is the dedupe key but is **not** monotonic, so it cannot be used for this. | The webhook enqueuer streams new events by `WHERE seq > last_seen` (see `webhook_state` below). One integer comparison replaces "diff the set of event ids I've seen". | Making `seq` nullable, resetting the sequence, backfilling rows with `seq` values below the current webhook watermark, or copying `events` without preserving `seq` all cause the webhook enqueuer to **skip** those rows permanently — subscribers silently miss deliveries. Reordering `seq` vs. insert order can also skip rows if the enqueuer reads a gap that later fills in. |
| **`webhook_state`** (single row, `CHECK (id = 1)`) | Exactly one row, holding `last_seq` (events stream watermark) and `last_upgrade_id` (contract-upgrade stream watermark). Each watermark only ever moves forward, and it is advanced **in the same transaction** that inserts the matching `webhook_deliveries` rows — so a crash between "enqueue" and "advance watermark" is impossible; the pair commit together or not at all. The two watermarks are independent: a quiet period in one stream cannot stall the other. | Gives at-least-once delivery with a bounded, crash-safe replay window, without a per-subscription cursor. The `ON CONFLICT (subscription_id, ...) DO NOTHING` dedupe on `webhook_deliveries` covers the "at-least" overlap. | Allowing a second row, resetting a watermark to 0 (re-enqueues and re-delivers the entire history), or advancing a watermark outside the enqueue transaction (a crash then skips deliveries). Manually editing `last_seq` forward to "skip a backlog" drops those deliveries for good. |
| **`indexer_cursor`** (single row, `CHECK (id = 1)`) | Exactly one row, id `1`. Holds `last_processed_ledger` plus denormalized status/counter columns (`chain_tip_ledger`, `events_ingested_total`, RPC/enrichment counters) that `/health` and `/metrics` read directly. The indexer resumes ingestion from `last_processed_ledger` on every startup. | A single well-known row is a cheap, race-free resume point for the one writer, and doubles as the status snapshot the API serves without querying the indexer process. | A second row, or `id <> 1`, makes the resume query ambiguous — the indexer can re-scan or skip ledgers. Deleting the row loses the resume point (it restarts from `START_LEDGER`). Two indexer processes writing this row concurrently is unsupported: run exactly one indexer. |

## contract_summaries trigger

`contract_summaries` is a denormalized table that keeps a running
`(event_count, first_seen_ledger, last_seen_ledger)` per contract, maintained
by a Postgres row trigger on `events` (`trg_update_contract_summary`).  It
exists so `GET /contracts` can be answered with a cheap primary-key scan instead
of a full `GROUP BY` on the (potentially very large) `events` table.

The trigger handles all three DML operations:

| DML | Behaviour |
|-----|-----------|
| `INSERT` | Upserts a row with `event_count = 1`, or increments an existing row by 1 and widens the `first_seen_ledger` / `last_seen_ledger` bounds. |
| `DELETE` | Decrements `event_count` and re-derives ledger bounds if the deleted row was at an edge (using an indexed `MIN`/`MAX` sub-select, so it is O(log n) not O(n)). When `event_count` reaches zero the summary row is **deleted** — `list_contracts` therefore never exposes contracts that have had all their events pruned. |
| `UPDATE` | Treated as a logical delete of the old row followed by an insert of the new one; only fires when `contract_id` or `ledger` actually changes, which is rare. |

**Retention interaction.** The retention pruner (`lumenqraph-indexer::retention`)
deletes from `events` in batches; each delete fires the trigger, so
`contract_summaries` stays exact across retention runs without any separate
reconciliation query.  This is covered by the Postgres-backed integration tests
in `crates/lumenqraph-indexer/src/retention.rs` (see the
`contract_summaries_*` test group).

**Migration history.**
- `migrations/0009_contract_summaries.sql` — initial table and `INSERT`-only trigger.
- `migrations/0021_contract_summaries_delete.sql` — replaces the trigger with the
  `INSERT | UPDATE | DELETE` variant described above.

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
