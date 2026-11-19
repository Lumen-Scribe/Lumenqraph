-- Renumbered from 0043 to resolve a duplicate version 0043 (see 0043_webhook_legacy_secret_nullable.sql).
-- Idempotent (ADD COLUMN IF NOT EXISTS).
-- #593 Pin the Soroban network before ingesting data or switching RPC endpoints.
ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS network_passphrase TEXT;
