-- Align `missed_ranges` with what the poller writes and reads (#707).
--
-- Migration 0038 created `start_ledger` / `end_ledger` with `contract_id NOT NULL`,
-- but the indexer inserts `from_ledger`, `to_ledger` and `reason` and selects
-- `from_ledger` / `to_ledger`. Every skip-ahead therefore failed, and because the
-- insert ran before the cursor moved, the poll cycle retried the same failure forever.
--
-- 0038 has shipped, so its shape is fixed forward here. Every statement is
-- idempotent: the renames check the catalog first, so a re-run is a no-op.
--
-- Squawk: `missed_ranges` is a small operational table written only by the indexer,
-- so the non-concurrent index rebuild below is acceptable and the lock is brief.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'missed_ranges' AND column_name = 'start_ledger'
    ) THEN
        ALTER TABLE missed_ranges RENAME COLUMN start_ledger TO from_ledger;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'missed_ranges' AND column_name = 'end_ledger'
    ) THEN
        ALTER TABLE missed_ranges RENAME COLUMN end_ledger TO to_ledger;
    END IF;
END $$;

-- Ranges are global (the poller records them without a contract), so the column stays but is optional.
ALTER TABLE missed_ranges ALTER COLUMN contract_id DROP NOT NULL;

ALTER TABLE missed_ranges ADD COLUMN IF NOT EXISTS reason TEXT;

-- Rebuild the partial index on the columns the recovery query actually uses.
DROP INDEX IF EXISTS idx_missed_ranges_unrecovered;
CREATE INDEX IF NOT EXISTS idx_missed_ranges_unrecovered
    ON missed_ranges (from_ledger)
    WHERE recovered_at IS NULL;
