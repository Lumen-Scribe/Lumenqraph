-- Webhook delivery enhancements: auto-disable tracking, backfill, encryption, and improved deliverability

-- Track auto-disabled subscriptions with reason and timestamp
ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS auto_disabled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS auto_disabled_reason TEXT;

-- Index for listing auto-disabled subscriptions
CREATE INDEX IF NOT EXISTS idx_subs_auto_disabled
    ON webhook_subscriptions (auto_disabled_at) WHERE auto_disabled_at IS NOT NULL;

-- Add columns to track consecutive failures per subscription
ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0;

-- Task #116: Add per-subscription starting watermark for optional backfill
ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS starting_seq BIGINT DEFAULT 0;

-- Task #118: Encrypt secrets at rest with pgcrypto
-- Enable pgcrypto extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Add encrypted_secret column to store pgp_sym_encrypted secrets
ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS encrypted_secret TEXT;

-- Task #119: Add numeric column for token_transfers aggregation
ALTER TABLE token_transfers
    ADD COLUMN IF NOT EXISTS amount_numeric NUMERIC(39, 0);

-- Task #117: Webhook delivery retention uses created_at column (already exists)
-- No additional schema needed; pruning logic in retention.rs uses existing columns
