-- Materialized per-vertical table: token transfers, derived from SEP-41 style
-- `transfer` events (topics: [symbol "transfer", from Address, to Address],
-- value: i128 amount). Amount is TEXT because i128 exceeds SQL numeric-in-i64.
--
-- This is the "vertical focus" surface: the best possible view for DeFi/token
-- flows, served without the caller decoding XDR themselves.

CREATE TABLE IF NOT EXISTS token_transfers (
    event_id          TEXT        PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id       TEXT        NOT NULL,
    from_addr         TEXT,
    to_addr           TEXT,
    amount            TEXT        NOT NULL,
    ledger            BIGINT      NOT NULL,
    ledger_closed_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transfers_contract_ledger
    ON token_transfers (contract_id, ledger DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_from ON token_transfers (from_addr);
CREATE INDEX IF NOT EXISTS idx_transfers_to   ON token_transfers (to_addr);
