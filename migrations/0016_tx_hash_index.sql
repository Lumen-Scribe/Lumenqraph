-- Add index on tx_hash for efficient by-transaction event lookup (#124)
-- Supports GET /transactions/:tx_hash/events

CREATE INDEX IF NOT EXISTS idx_events_tx_hash
    ON events (tx_hash, ledger ASC, event_id ASC);
