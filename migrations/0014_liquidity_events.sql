-- Materialized liquidity events vertical (#88)
--
-- Derived from AMM deposit / withdraw (add/remove liquidity) events.
-- Canonical shapes emitted by major Soroban AMMs:
--   deposit  topics: [symbol "deposit",  provider Address]
--            value:  { amounts: [i128, i128], shares_minted: i128 }
--   withdraw topics: [symbol "withdraw", provider Address]
--            value:  { amounts: [i128, i128], shares_burned: i128 }
--
-- "add" and "remove" are normalised from their raw names ("deposit"/"withdraw"
-- or "add_liquidity"/"remove_liquidity") into the `event_kind` column.
-- LP-token delta (shares) is stored alongside the underlying token amounts.
-- Amounts are TEXT because i128 exceeds SQL integer range.
-- We store up to two underlying token amounts; additional amounts are in
-- `extra_amounts` as a JSON array for pools with 3+ assets.

CREATE TABLE IF NOT EXISTS liquidity_events (
    event_id          TEXT        PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id       TEXT        NOT NULL,
    -- "add" (deposit) or "remove" (withdraw).
    event_kind        TEXT        NOT NULL,
    -- Liquidity provider address.
    provider          TEXT,
    -- First underlying token amount (i128 as decimal string).
    amount_a          TEXT,
    -- Second underlying token amount (i128 as decimal string); NULL for single-asset ops.
    amount_b          TEXT,
    -- LP token delta: shares minted (add) or burned (remove).
    shares            TEXT,
    -- Original event name ("deposit" / "withdraw" / "add_liquidity" / …).
    raw_event_name    TEXT,
    -- JSON array of additional amounts for 3+ asset pools; NULL otherwise.
    extra_amounts     JSONB,
    ledger            BIGINT      NOT NULL,
    ledger_closed_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_liquidity_events_contract_ledger
    ON liquidity_events (contract_id, ledger DESC);
CREATE INDEX IF NOT EXISTS idx_liquidity_events_provider
    ON liquidity_events (provider);
CREATE INDEX IF NOT EXISTS idx_liquidity_events_kind
    ON liquidity_events (event_kind);
