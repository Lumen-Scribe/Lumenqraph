-- Add ledger column to contract_spec_versions for ledger-based retention pruning.
--
-- The ledger value captures lastModifiedLedgerSeq of the contract instance when
-- the spec was observed. This allows `retention::prune` to delete versions older
-- than the retention window, while always preserving the newest N versions per
-- contract (so old but still-current interfaces are never lost).
--
-- For existing rows (if any), we set ledger to 0, indicating "unknown ledger".
-- New rows will capture the actual ledger when observed.

ALTER TABLE contract_spec_versions
    ADD COLUMN IF NOT EXISTS ledger BIGINT NOT NULL DEFAULT 0;
