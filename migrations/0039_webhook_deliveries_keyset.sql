-- Renumbered from 0024_webhook_deliveries_keyset.sql (see docs/MIGRATIONS.md).
-- Idempotent so databases that already ran this under the old version are unaffected.

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_sub_id
    ON webhook_deliveries (subscription_id, id);