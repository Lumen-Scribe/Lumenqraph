-- Align missed_ranges with the indexer's writer and gap-recovery reader (#715).
--
-- 0038 created the table with `contract_id` NOT NULL, but a gap belongs to the
-- indexer rather than to a contract, so the writer never supplies one. The
-- writer also records a human-readable `reason` (for example "outside RPC
-- retention; use deep-backfill"), which the table did not have. The writer and
-- reader use `start_ledger` / `end_ledger`, the columns 0038 created.
--
-- Additive and non-destructive: existing rows keep their values, and the
-- NOT NULL relaxation cannot fail.

ALTER TABLE missed_ranges
    ADD COLUMN IF NOT EXISTS reason TEXT;

ALTER TABLE missed_ranges
    ALTER COLUMN contract_id DROP NOT NULL;
