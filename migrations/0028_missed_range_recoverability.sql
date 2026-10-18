-- Distinguish gaps that can still be replayed from ranges skipped outside RPC retention.
ALTER TABLE missed_ranges
    ADD COLUMN IF NOT EXISTS recoverable BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_missed_ranges_recoverable
    ON missed_ranges (detected_at)
    WHERE recovered_at IS NULL AND recoverable = TRUE;
