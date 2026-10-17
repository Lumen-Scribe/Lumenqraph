-- Webhook enhancements: auto-disable tracking, delivery sequencing, encrypted
-- secrets, and numeric token transfer amounts.
--
-- Renumbered from 0017 to 0034 to resolve a duplicate version prefix (see
-- docs/MIGRATIONS.md). Every statement is idempotent so databases that already
-- ran this migration under the old number are unaffected.

-- Needed for gen_random_uuid() used by encrypted secret storage.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Auto-disable bookkeeping for webhook endpoints.
ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0;

ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ;

ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS disabled_reason TEXT;

-- Monotonic sequence used to order deliveries per endpoint.
ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS starting_seq BIGINT NOT NULL DEFAULT 0;

-- Encrypted signing secret storage.
ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS encrypted_secret BYTEA;

-- Numeric representation of token transfer amounts for precise arithmetic.
ALTER TABLE token_transfers
    ADD COLUMN IF NOT EXISTS amount_numeric NUMERIC;