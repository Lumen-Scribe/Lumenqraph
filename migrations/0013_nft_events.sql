-- Materialized NFT events vertical (#87)
--
-- Derived from Soroban NFT mint / transfer / burn events.
-- Canonical NFT event shapes:
--   mint     topics: [symbol "mint",     to Address,   token_id]
--   transfer topics: [symbol "transfer", from Address, to Address, token_id]
--   burn     topics: [symbol "burn",     from Address, token_id]
--
-- `token_id` may be an integer, a string, or a compound key depending on the
-- NFT contract; we store it as TEXT after JSON serialisation.
-- `event_kind` is one of "mint" | "transfer" | "burn".

CREATE TABLE IF NOT EXISTS nft_events (
    event_id          TEXT        PRIMARY KEY REFERENCES events(event_id) ON DELETE CASCADE,
    contract_id       TEXT        NOT NULL,
    -- "mint" | "transfer" | "burn"
    event_kind        TEXT        NOT NULL,
    -- Sender (NULL for mints).
    from_addr         TEXT,
    -- Recipient (NULL for burns).
    to_addr           TEXT,
    -- Token identifier, JSON-encoded for compound keys.
    token_id          TEXT        NOT NULL,
    ledger            BIGINT      NOT NULL,
    ledger_closed_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nft_events_contract_ledger
    ON nft_events (contract_id, ledger DESC);
CREATE INDEX IF NOT EXISTS idx_nft_events_token_id   ON nft_events (contract_id, token_id);
CREATE INDEX IF NOT EXISTS idx_nft_events_from_addr  ON nft_events (from_addr);
CREATE INDEX IF NOT EXISTS idx_nft_events_to_addr    ON nft_events (to_addr);
CREATE INDEX IF NOT EXISTS idx_nft_events_kind       ON nft_events (event_kind);
