# Database Schema

This document describes the Lumenqraph PostgreSQL schema, including tables, relationships, indexes, and their purposes. It's essential for self-hosters who want to query the database directly or understand the data model.

## Overview

The schema is organized into several logical groups:

1. **Core Indexing** — Raw events and indexer state
2. **Decoded Data** — Typed, decoded representations of events
3. **Contract Metadata** — Interface versions and upgrades
4. **Per-Key Storage** — Contract instance and balance snapshots
5. **Derived Data** — Materialized views (token transfers, AMM swaps, NFTs, liquidity events)
6. **Webhooks** — Subscriptions and delivery queue
7. **API Management** — API keys and authentication

## Core Indexing Tables

### `events`

Raw Soroban contract events as received from Stellar RPC, with minimal processing.

```sql
CREATE TABLE events (
    event_id            TEXT PRIMARY KEY,           -- RPC `id` (dedupe key)
    contract_id         TEXT NOT NULL,              -- Soroban contract address (C…)
    ledger              BIGINT NOT NULL,            -- Stellar ledger number
    ledger_closed_at    TIMESTAMPTZ NOT NULL,       -- Ledger close timestamp
    event_type          TEXT NOT NULL,              -- 'contract' | 'system' | 'diagnostic'
    topics              JSONB NOT NULL,             -- Array of base64 XDR topics
    decoded_topics      JSONB NOT NULL DEFAULT '[]'::jsonb, -- Decoded topics as JSON
    event_name          TEXT,                       -- Best-effort symbol from topic[0]
    value               TEXT NOT NULL,              -- Base64 XDR event body
    decoded_value       JSONB NOT NULL DEFAULT 'null'::jsonb, -- Decoded body as JSON
    enriched            JSONB,                      -- Named, typed record from on-chain spec
    tx_hash             TEXT NOT NULL,              -- Transaction hash
    in_successful_call  BOOLEAN NOT NULL,           -- Whether tx succeeded
    paging_token        TEXT NOT NULL,              -- Soroban RPC paging token
    seq                 BIGINT UNIQUE,              -- Monotonic sequence (webhook watermark)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger DESC);
CREATE INDEX idx_events_name ON events (contract_id, event_name);
CREATE INDEX idx_events_seq ON events (seq);
CREATE INDEX idx_events_decoded_value ON events USING GIN (decoded_value);
CREATE INDEX idx_events_tx_hash ON events (tx_hash);
```

**Key Points:**
- `event_id` is RPC-assigned and unique; used for deduplication
- `seq` is database-assigned, monotonically increasing; used for webhook delivery ordering
- `topics` and `value` are preserved as base64 XDR (lossless)
- `decoded_*` fields are queryable JSON; `enriched` is typed when on-chain spec matches
- Events may be re-scanned for shallow reorg corrections (updated in place)

### `indexer_cursor`

Single-row table tracking the indexer's progress and health metrics.

```sql
CREATE TABLE indexer_cursor (
    id                      INTEGER PRIMARY KEY DEFAULT 1,
    last_processed_ledger   BIGINT NOT NULL,
    chain_tip_ledger        BIGINT NOT NULL DEFAULT 0,
    events_ingested_total   BIGINT NOT NULL DEFAULT 0,
    errors_total            BIGINT NOT NULL DEFAULT 0,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);
```

**Usage:**
- Always query with `WHERE id = 1` to fetch singleton
- `chain_tip_ledger` is updated by the indexer each cycle from RPC
- Lag = `chain_tip_ledger - last_processed_ledger`
- Used by `/health` and `/metrics` endpoints for operational visibility

## Contract Metadata Tables

### `contract_specs`

Latest interface (function/type/event schema) for each contract, extracted from the on-chain WASM.

```sql
CREATE TABLE contract_specs (
    contract_id         TEXT PRIMARY KEY,
    interface           JSONB NOT NULL,             -- Full on-chain spec (functions, types, events)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_specs_updated ON contract_specs (updated_at DESC);
```

**Purpose:**
- Used to decode events into typed, named records (`enriched` field)
- Updated when the contract's WASM changes (upgrade watch)

### `contract_spec_versions`

Historical record of interface changes, used to track upgrades and compute diffs.

```sql
CREATE TABLE contract_spec_versions (
    id                  BIGSERIAL PRIMARY KEY,
    contract_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,           -- Sequential version (1 = baseline, 2+ = upgrades)
    interface           JSONB NOT NULL,             -- Interface at this version
    wasm_hash           TEXT NOT NULL,              -- WASM executable hash
    previous_wasm_hash  TEXT,                       -- Hash of prior version (NULL for v1)
    breaking            BOOLEAN NOT NULL,           -- Whether diff breaks consumers
    diff                JSONB NOT NULL,             -- Semantic diff vs previous version
    observed_at         TIMESTAMPTZ NOT NULL,       -- When we detected this version
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (contract_id, version)
);

CREATE INDEX idx_spec_versions_contract ON contract_spec_versions (contract_id, version DESC);
CREATE INDEX idx_spec_versions_timestamp ON contract_spec_versions (observed_at DESC);
```

**Key Points:**
- Version 1 is the baseline; upgrades start at version 2
- `diff` includes `breaking: boolean` and `summary: string[]` of changes
- Used by webhook subscribers watching `kind = 'upgrade'`
- Only versions >= 2 trigger upgrade webhooks

## Per-Key Storage Tables

These tables snapshot contract state at each ledger, enabling historical queries and state reconstruction.

### `contract_state`

Instance storage snapshot for each tracked contract at each ledger.

```sql
CREATE TABLE contract_state (
    contract_id         TEXT NOT NULL,
    ledger              BIGINT NOT NULL,
    key                 TEXT NOT NULL,              -- Storage key variant
    value               JSONB NOT NULL,             -- Current value (decoded)
    raw_value           TEXT NOT NULL,              -- Base64 XDR (lossless)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, ledger, key)
);

CREATE INDEX idx_state_contract_ledger ON contract_state (contract_id, ledger DESC);
```

**Usage:**
- Enable via `STATE_INDEXING=true`
- One row per key per ledger snapshot
- `key` is the storage key variant name (e.g., `"Balance"`, `"Allowance"`)
- Query latest state: `WHERE ledger = (SELECT max(ledger) FROM contract_state WHERE contract_id = $1)`

### `contract_data`

Per-holder balance snapshots for SEP-41 tokens (and other indexed keys).

```sql
CREATE TABLE contract_data (
    contract_id         TEXT NOT NULL,
    holder              TEXT NOT NULL,              -- Account or contract address
    ledger              BIGINT NOT NULL,
    key                 TEXT NOT NULL,              -- Usually 'Balance'
    value               JSONB NOT NULL,             -- Current balance/value
    raw_value           TEXT NOT NULL,              -- Base64 XDR
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, holder, ledger, key)
);

CREATE INDEX idx_data_holder_ledger ON contract_data (holder, contract_id, ledger DESC);
CREATE INDEX idx_data_contract_ledger ON contract_data (contract_id, ledger DESC);
```

**Usage:**
- Enable via `KEY_INDEXING=true`
- Indexed when triggered by events in `KEY_TEMPLATES`
- Query latest balance: 
  ```sql
  SELECT value FROM contract_data 
  WHERE contract_id = $1 AND holder = $2 AND key = 'Balance'
  ORDER BY ledger DESC LIMIT 1;
  ```

## Event Derivation Tables

These tables materialize normalized event shapes for common patterns, enabling efficient queries without decoding or joins.

### `token_transfers`

Normalized SEP-41 transfer/mint/burn/clawback events.

```sql
CREATE TABLE token_transfers (
    event_id            TEXT PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id         TEXT NOT NULL,              -- Token contract
    from_addr           TEXT,                       -- Sender (NULL for mint)
    to_addr             TEXT,                       -- Recipient (NULL for burn/clawback)
    amount              TEXT NOT NULL,              -- Decimal string (handles i128)
    amount_numeric      NUMERIC(39, 0),             -- For aggregation/sorting
    kind                TEXT NOT NULL,              -- 'transfer' | 'mint' | 'burn' | 'clawback'
    ledger              BIGINT NOT NULL,
    ledger_closed_at    TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_transfers_contract ON token_transfers (contract_id, ledger DESC);
CREATE INDEX idx_transfers_from_to ON token_transfers (from_addr, contract_id, ledger DESC) WHERE from_addr IS NOT NULL;
CREATE INDEX idx_transfers_to ON token_transfers (to_addr, contract_id, ledger DESC) WHERE to_addr IS NOT NULL;
CREATE INDEX idx_transfers_ledger ON token_transfers (ledger DESC);
```

**Key Points:**
- `amount` is a decimal string to preserve precision (Soroban i128 exceeds SQL int64)
- `amount_numeric` is a PostgreSQL NUMERIC for sum/avg aggregations
- `kind` distinguishes the four SEP-41 event types
- Automatically derived from matching SEP-41 event signatures

### `amm_swaps`

Normalized AMM swap events (e.g., from Aquarius router).

```sql
CREATE TABLE amm_swaps (
    event_id            TEXT PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id         TEXT NOT NULL,              -- AMM/router contract
    sender              TEXT,                       -- Swap initiator
    sell_token          TEXT,                       -- Token being sold
    buy_token           TEXT,                       -- Token being bought
    sell_amount         TEXT NOT NULL,              -- Decimal string
    buy_amount          TEXT NOT NULL,              -- Decimal string
    raw_event_name      TEXT,                       -- Original event name
    ledger              BIGINT NOT NULL,
    ledger_closed_at    TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_swaps_contract ON amm_swaps (contract_id, ledger DESC);
CREATE INDEX idx_swaps_sender ON amm_swaps (sender, ledger DESC);
CREATE INDEX idx_swaps_tokens ON amm_swaps (sell_token, buy_token, ledger DESC);
```

### `nft_events`

Normalized NFT mint/transfer/burn events.

```sql
CREATE TABLE nft_events (
    event_id            TEXT PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id         TEXT NOT NULL,              -- NFT contract
    event_kind          TEXT NOT NULL,              -- 'mint' | 'transfer' | 'burn'
    from_addr           TEXT,                       -- Sender (NULL for mint)
    to_addr             TEXT,                       -- Recipient (NULL for burn)
    token_id            TEXT NOT NULL,              -- NFT token ID
    ledger              BIGINT NOT NULL,
    ledger_closed_at    TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nft_contract ON nft_events (contract_id, ledger DESC);
CREATE INDEX idx_nft_token ON nft_events (contract_id, token_id, ledger DESC);
CREATE INDEX idx_nft_holder ON nft_events (to_addr, contract_id) WHERE to_addr IS NOT NULL;
```

### `liquidity_events`

Normalized liquidity pool deposit/withdraw events.

```sql
CREATE TABLE liquidity_events (
    event_id            TEXT PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id         TEXT NOT NULL,              -- Pool contract
    event_kind          TEXT NOT NULL,              -- 'deposit' | 'withdraw'
    provider            TEXT,                       -- LP address
    amount_a            TEXT,                       -- Token A amount (decimal string)
    amount_b            TEXT,                       -- Token B amount (decimal string)
    shares              TEXT,                       -- LP shares minted/burned
    raw_event_name      TEXT,                       -- Original event name
    extra_amounts       JSONB,                      -- Additional amounts for >2-token pools
    ledger              BIGINT NOT NULL,
    ledger_closed_at    TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_liquidity_contract ON liquidity_events (contract_id, ledger DESC);
CREATE INDEX idx_liquidity_provider ON liquidity_events (provider, contract_id, ledger DESC);
```

## Webhook Tables

See [WEBHOOKS.md](WEBHOOKS.md) for detailed webhook documentation.

### `webhook_subscriptions`

Registered webhook endpoints with filters and secrets.

```sql
CREATE TABLE webhook_subscriptions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url                 TEXT NOT NULL,              -- Delivery URL
    kind                TEXT NOT NULL,              -- 'event' | 'upgrade'
    contract_id         TEXT,                       -- NULL = any
    event_name          TEXT,                       -- NULL = any (kind='event')
    secret              TEXT NOT NULL,              -- HMAC signing secret (plaintext fallback)
    encrypted_secret    TEXT,                       -- pgcrypto encrypted secret
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    starting_seq        BIGINT DEFAULT 0,           -- Optional backfill watermark
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    auto_disabled_at    TIMESTAMPTZ,
    auto_disabled_reason TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_subs_filter ON webhook_subscriptions (active, contract_id, event_name);
CREATE INDEX idx_subs_auto_disabled ON webhook_subscriptions (auto_disabled_at) WHERE auto_disabled_at IS NOT NULL;
```

### `webhook_deliveries`

Outgoing delivery queue with retry state and history.

```sql
CREATE TABLE webhook_deliveries (
    id                  BIGSERIAL PRIMARY KEY,
    subscription_id     UUID NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    event_id            TEXT REFERENCES events(event_id) ON DELETE CASCADE,
    upgrade_id          BIGINT REFERENCES contract_spec_versions(id) ON DELETE CASCADE,
    status              TEXT NOT NULL DEFAULT 'pending',  -- pending | delivered | failed
    attempts            INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subscription_id, event_id) WHERE event_id IS NOT NULL,
    UNIQUE (subscription_id, upgrade_id) WHERE upgrade_id IS NOT NULL
);

CREATE INDEX idx_deliveries_due ON webhook_deliveries (status, next_attempt_at);
CREATE INDEX idx_deliveries_subscription ON webhook_deliveries (subscription_id);
```

### `webhook_state`

Single-row table tracking enqueue watermarks (internal use only).

```sql
CREATE TABLE webhook_state (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    last_seq            BIGINT NOT NULL DEFAULT 0,      -- Highest seq enqueued
    last_upgrade_id     BIGINT NOT NULL DEFAULT 0,      -- Highest upgrade_id enqueued
    CONSTRAINT single_row_state CHECK (id = 1)
);
```

## API Management Tables

### `api_keys`

Registered API keys for rate-limited access.

```sql
CREATE TABLE api_keys (
    key_hash            TEXT PRIMARY KEY,           -- SHA-256 hash of the key (never store plaintext)
    name                TEXT NOT NULL,              -- Human-readable name
    tier                TEXT NOT NULL DEFAULT 'free', -- 'free' | 'pro' | 'enterprise'
    rate_limit_per_min  INTEGER NOT NULL DEFAULT 60,
    revoked             BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_active ON api_keys (revoked, created_at DESC) WHERE NOT revoked;
```

**Security:** Only the SHA-256 hash of the key is stored, never the plaintext. Raw keys are issued once during creation and never recoverable.

## Relationship Diagram (Entity-Relationship)

```
┌──────────────────┐
│   events         │◄─────────────────────────┐
├──────────────────┤                          │
│ event_id (PK)    │                          │
│ contract_id      │─────┐                    │
│ ledger           │     │                    │
│ seq (watermark)  │     │                    │
└──────────────────┘     │                    │
        │                 │                    │
        │ FK              │                    │
        ├─────────────┬───┘                    │
        │             │                        │
        ▼             ▼                        │
   ┌─────────────┐   ┌──────────────────┐     │
   │ contract_   │   │ contract_specs   │     │
   │ state       │   ├──────────────────┤     │
   └─────────────┘   │ contract_id (PK) │     │
        │             │ interface       │     │
        │             └──────────────────┘     │
        ▼                     ▲                │
   ┌──────────────┐          │                │
   │ contract_    │          │                │
   │ data         │     ┌────┴──────────────┐ │
   └──────────────┘     │ contract_spec_    │ │
        │               │ versions          │ │
        │               ├────────────────────┤ │
        │               │ id (PK)           │ │
        │               │ contract_id       │ │
        │               │ version           │ │
        │               │ wasm_hash         │ │
        │               │ diff              │ │
        │               └────────────────────┘ │
        │                        │             │
        │                        ▼             │
        │                  ┌──────────────────┤
        └─────────────────►│ webhook_         │
                           │ subscriptions    │
                           │ (has filters)    │
                           └──────────────────┘
                                  │
                                  │ FK
                                  ▼
                          ┌──────────────────┐
                          │ webhook_         │
                          │ deliveries       │
                          │ (queue + retry)  │
                          └──────────────────┘

Materialized Views (derived from events):
  events ──► token_transfers
  events ──► amm_swaps
  events ──► nft_events
  events ──► liquidity_events
```

## Foreign Key Cascades

Important cascade behaviors:

- Deleting an `event` cascades to:
  - `token_transfers`, `amm_swaps`, `nft_events`, `liquidity_events` (derived data)
  - `webhook_deliveries` (pending deliveries for that event)

- Deleting a `contract_spec_versions` cascades to:
  - `webhook_deliveries` (pending upgrade deliveries)

- Deleting a `webhook_subscriptions` cascades to:
  - `webhook_deliveries` (all pending and delivered records)

## Indexes at a Glance

| Table | Index | Purpose |
|-------|-------|---------|
| `events` | `idx_events_contract_ledger` | Efficient "recent events for a contract" queries |
| `events` | `idx_events_name` | Filter by event name within a contract |
| `events` | `idx_events_seq` | Webhook delivery ordering (monotonic) |
| `events` | `idx_events_decoded_value` | GIN index for JSON field queries |
| `contract_state` | `idx_state_contract_ledger` | Historical state snapshots per contract |
| `contract_data` | `idx_data_holder_ledger` | Per-holder balance history |
| `token_transfers` | `idx_transfers_contract` | Recent transfers for a token |
| `token_transfers` | `idx_transfers_from_to` | Sender/recipient queries (partial indexes) |
| `webhook_subscriptions` | `idx_subs_filter` | Match subscriptions to events |
| `webhook_deliveries` | `idx_deliveries_due` | Find deliveries ready for retry |

## Retention & Pruning

If `RETENTION_LEDGERS` is set, the indexer automatically prunes rows older than the specified window:

- **Events**: Deleted when `ledger < (current_tip - RETENTION_LEDGERS)`
- **Derived data** (token_transfers, etc.): Cascaded from events
- **State snapshots** (contract_state, contract_data): Deleted, but the **latest version is always kept**
- **Webhook deliveries**: Deleted when `created_at < (now - RETENTION_DAYS)` (soft limit via background task)

Example:
```sql
-- Keep only the last 7 days of history (at ~5 sec/ledger ≈ 120k ledgers)
RETENTION_LEDGERS=120960
```

## Querying Patterns

### Recent Events for a Contract

```sql
SELECT * FROM events 
WHERE contract_id = 'CADQZ...' 
ORDER BY ledger DESC 
LIMIT 100;
```

### Token Transfer History

```sql
SELECT * FROM token_transfers 
WHERE contract_id = 'CADQZ...' 
  AND (from_addr = 'GBVFX...' OR to_addr = 'GBVFX...')
ORDER BY ledger DESC;
```

### Current Balance for a Holder

```sql
SELECT value FROM contract_data 
WHERE contract_id = 'CADQZ...' 
  AND holder = 'GBVFX...' 
  AND key = 'Balance'
ORDER BY ledger DESC 
LIMIT 1;
```

### Contract Interface at a Point in Time

```sql
SELECT interface FROM contract_spec_versions 
WHERE contract_id = 'CADQZ...' 
  AND observed_at <= '2025-01-15'::timestamp
ORDER BY version DESC 
LIMIT 1;
```

### All Auto-Disabled Webhooks

```sql
SELECT id, url, auto_disabled_reason, auto_disabled_at 
FROM webhook_subscriptions 
WHERE auto_disabled_at IS NOT NULL
ORDER BY auto_disabled_at DESC;
```

## Maintenance

### Database Size

Monitor:
```sql
SELECT pg_size_pretty(pg_total_relation_size('events')) as events_size;
SELECT pg_size_pretty(pg_total_relation_size('contract_data')) as state_size;
SELECT pg_size_pretty(pg_database_size('lumenqraph')) as total_size;
```

### Vacuuming

PostgreSQL's autovacuum handles most pruning automatically. For high-throughput instances:

```sql
VACUUM ANALYZE events;
VACUUM ANALYZE token_transfers;
VACUUM ANALYZE contract_data;
```

### Reindexing

If indexes fragment after heavy retention pruning:

```sql
REINDEX INDEX CONCURRENTLY idx_events_contract_ledger;
REINDEX TABLE CONCURRENTLY events;
```

## See Also

- [MIGRATIONS.md](MIGRATIONS.md) — Migration history and rollback strategy
- [WEBHOOKS.md](WEBHOOKS.md) — Webhook subscriptions and delivery
- [CONFIGURATION.md](CONFIGURATION.md) — Indexer and API configuration
