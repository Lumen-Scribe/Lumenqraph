-- Typed, self-describing decoding driven by each contract's on-chain interface
-- (the `contractspecv0` WASM section). See lumenqraph-core::spec.

-- Named, typed record for an event, built from its contract's spec. NULL when
-- the contract publishes no matching spec (decoded_topics/decoded_value remain
-- the fallback, so nothing is ever lost).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS enriched JSONB;

-- Query enriched events by named field (e.g. an enriched transfer's amount).
CREATE INDEX IF NOT EXISTS idx_events_enriched
    ON events USING GIN (enriched);

-- Parsed contract interfaces, fetched once per contract from its deployed WASM
-- and served by GET /contracts/:id/interface. `interface` is the full functions
-- + events + user-defined types view; `wasm_hash` lets us refetch on upgrade.
CREATE TABLE IF NOT EXISTS contract_specs (
    contract_id  TEXT        PRIMARY KEY,
    wasm_hash    TEXT        NOT NULL,
    interface    JSONB       NOT NULL,
    -- Raw `contractspecv0` section (hex), re-parsed by the read layer to encode
    -- typed call arguments for POST /contracts/:id/call.
    spec_section TEXT        NOT NULL DEFAULT '',
    has_events   BOOLEAN     NOT NULL DEFAULT FALSE,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
