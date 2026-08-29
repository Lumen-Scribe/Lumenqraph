-- Materialized AMM swap vertical (#86)
--
-- Derived from Soroban AMM swap events (Aquarius, Soroswap, Phoenix, etc.).
-- Canonical swap event shape:
--   topics: [symbol "swap", sell_token Address, buy_token Address, (optional sender Address)]
--   value:  map or struct { sell_amount: i128, buy_amount: i128 }
--
-- Where AMMs differ in their exact event name or field names we normalise
-- to the common sell_amount / buy_amount / sell_token / buy_token columns.
-- Amounts are TEXT because i128 exceeds SQL integer range.
-- The `raw_event_name` column preserves the original name for filtering.

CREATE TABLE IF NOT EXISTS amm_swaps (
    event_id          TEXT        PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id       TEXT        NOT NULL,
    -- The address that initiated the swap (topic[1] when present, else NULL).
    sender            TEXT,
    -- Token sold by the swapper (input token).
    sell_token        TEXT,
    -- Token bought by the swapper (output token).
    buy_token         TEXT,
    -- Amount sold (i128 as decimal string).
    sell_amount       TEXT        NOT NULL,
    -- Amount bought (i128 as decimal string).
    buy_amount        TEXT        NOT NULL,
    -- Original event name preserved for protocol-specific queries.
    raw_event_name    TEXT,
    ledger            BIGINT      NOT NULL,
    ledger_closed_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_amm_swaps_contract_ledger
    ON amm_swaps (contract_id, ledger DESC);
CREATE INDEX IF NOT EXISTS idx_amm_swaps_sell_token ON amm_swaps (sell_token);
CREATE INDEX IF NOT EXISTS idx_amm_swaps_buy_token  ON amm_swaps (buy_token);
CREATE INDEX IF NOT EXISTS idx_amm_swaps_sender     ON amm_swaps (sender);
