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

-- Rename the semantics of `ledger` to `observed_ledger`.
--
-- `ledger` never actually recorded the ledger at which an upgrade happened: it
-- held either the ledger of the event that missed the spec cache (which may be
-- days after the upgrade, or before it for a backfill) or the contract
-- instance's `lastModifiedLedgerSeq` (which changes on every instance-storage
-- write, not just on executable updates). Rename it to `observed_ledger` to
-- make clear it is only "when we first saw this version", i.e. an upper bound
-- on the upgrade ledger.
--
-- We add `observed_ledger` and copy `ledger` into it, then drop the old
-- column. The copy is guarded so the migration is safe to re-run.

ALTER TABLE contract_spec_versions
    ADD COLUMN IF NOT EXISTS observed_ledger BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'contract_spec_versions'
          AND column_name = 'ledger'
    ) THEN
        EXECUTE 'UPDATE contract_spec_versions SET observed_ledger = ledger';
        EXECUTE 'ALTER TABLE contract_spec_versions DROP COLUMN ledger';
    END IF;
END
$$;

-- Add `upgrade_ledger`: the first ledger at which the new executable was live.
--
-- This is filled from an exact source when one is available (the executable
-- update system event or diagnostic, the transaction that called
-- `update_current_contract_wasm`, or a binary search over historical ledger
-- entries when an archive RPC is configured). It is NULL when the exact ledger
-- cannot be determined; in that case consumers should fall back to
-- `observed_ledger`, which is an upper bound on the upgrade ledger.

ALTER TABLE contract_spec_versions
    ADD COLUMN IF NOT EXISTS upgrade_ledger BIGINT;

-- Support version-at-ledger lookups: find the version that was live at a given
-- ledger for a contract.

CREATE INDEX IF NOT EXISTS idx_contract_spec_versions_contract_upgrade_ledger
    ON contract_spec_versions (contract_id, upgrade_ledger);