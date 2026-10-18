-- Webhook subscription ownership (renumbered from 0024_webhook_owner.sql).
-- Idempotent so databases that already ran this under the old version are unaffected.

ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS owner_key_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_owner
    ON webhook_subscriptions (owner_key_hash);

ALTER TABLE audit_log
    ADD COLUMN IF NOT EXISTS action_type TEXT;

ALTER TABLE audit_log
    ADD COLUMN IF NOT EXISTS resource_id TEXT;