-- Columns used by POST /webhooks/:id/rotate-secret (#564).
--
-- The previous secret is kept encrypted, like encrypted_secret, so the
-- dispatcher can sign deliveries with it until previous_secret_expires_at.

ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS previous_encrypted_secret BYTEA,
    ADD COLUMN IF NOT EXISTS previous_secret_expires_at TIMESTAMPTZ;
