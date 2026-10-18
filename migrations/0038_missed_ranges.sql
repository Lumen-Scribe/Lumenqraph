-- Missed ranges tracking
-- Renumbered from 0023_missed_ranges.sql to resolve duplicate version 0023.
-- Idempotent so databases that already ran this under the old number are unaffected.

CREATE TABLE IF NOT EXISTS missed_ranges (
    id BIGSERIAL PRIMARY KEY,
    contract_id BIGINT NOT NULL,
    start_ledger BIGINT NOT NULL,
    end_ledger BIGINT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    recovered_at TIMESTAMPTZ,
    recovery_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_missed_ranges_unrecovered
    ON missed_ranges (contract_id, start_ledger)
    WHERE recovered_at IS NULL;