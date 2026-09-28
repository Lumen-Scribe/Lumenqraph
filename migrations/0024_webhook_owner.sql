-- #421: Add owner_key_hash to webhook_subscriptions so each subscription is
-- scoped to the API key that created it, preventing cross-tenant enumeration
-- and modification.
--
-- Existing rows get owner_key_hash = NULL; those rows remain manageable only
-- by admin keys (callers with elevated privileges). New rows always have their
-- creator's key_hash set.

ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS owner_key_hash TEXT REFERENCES api_keys(key_hash) ON DELETE SET NULL;

-- Index for the per-owner query pattern (WHERE owner_key_hash = $1).
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_owner
    ON webhook_subscriptions (owner_key_hash)
    WHERE owner_key_hash IS NOT NULL;

-- Also expand the audit_log table's action_type and resource_id to support
-- per-key webhook audit entries (previously hardcoded to 'webhook').
-- action_type already exists from migration 0018; resource_id may be new.
-- We use ALTER COLUMN … (no-op if already present) via ADD IF NOT EXISTS.
ALTER TABLE audit_log
    ADD COLUMN IF NOT EXISTS action_type TEXT,
    ADD COLUMN IF NOT EXISTS resource_id TEXT;
