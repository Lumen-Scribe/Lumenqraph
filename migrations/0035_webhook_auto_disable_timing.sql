-- Webhook auto-disable timing
--
-- Renumbered from 0021_webhook_auto_disable_timing.sql to resolve a duplicate
-- version prefix. See docs/MIGRATIONS.md for the renumbering plan.
--
-- Every statement is idempotent so databases that already ran this migration
-- under its old version number are unaffected.

ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ;

ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS first_failure_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_subs_failure_timing
    ON webhook_subscriptions (first_failure_at)
    WHERE first_failure_at IS NOT NULL;