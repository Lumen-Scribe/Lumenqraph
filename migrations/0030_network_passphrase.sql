-- #593 Pin the Soroban network before ingesting data or switching RPC endpoints.
ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS network_passphrase TEXT;
