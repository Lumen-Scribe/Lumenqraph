-- Forward migration: add API key expiry, last-used tracking and scopes.
--
-- These columns were previously added by editing the already-released
-- migration 0002_production.sql in place. That broke sqlx's checksum check
-- for existing databases and never applied the columns to tables that
-- already existed. 0002 has been reverted to its released bytes; the
-- intended schema changes live here instead.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS scopes TEXT[] NOT NULL DEFAULT '{read}';

CREATE INDEX IF NOT EXISTS idx_api_keys_last_used_at ON api_keys (last_used_at);