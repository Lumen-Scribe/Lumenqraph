-- Ensure contract data is unique per (contract_id, key).
-- Renumbered from 0023_contract_data_unique.sql to resolve duplicate version 0023.
-- Idempotent so databases that already ran this under the old number are unaffected.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'contract_data_contract_id_key_key'
          AND conrelid = 'contract_data'::regclass
    ) THEN
        ALTER TABLE contract_data
            ADD CONSTRAINT contract_data_contract_id_key_key
            UNIQUE (contract_id, key);
    END IF;
END
$$;