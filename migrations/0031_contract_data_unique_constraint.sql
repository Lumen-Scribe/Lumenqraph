-- Forward migration: add the uq_contract_data_contract_key_ledger UNIQUE constraint
-- to contract_data if it is not already present.
--
-- This constraint was previously edited into the already-released
-- 0006_contract_data.sql migration, which caused sqlx checksum failures on
-- existing databases and never applied to tables that already existed.
-- It now lives here as an idempotent forward migration.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_contract_data_contract_key_ledger'
    ) THEN
        ALTER TABLE contract_data
            ADD CONSTRAINT uq_contract_data_contract_key_ledger
            UNIQUE (contract_id, key, ledger);
    END IF;
END $$;