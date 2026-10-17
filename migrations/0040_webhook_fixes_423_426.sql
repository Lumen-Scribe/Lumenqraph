-- Webhook delivery fixes for issues #423 and #426.
--
-- Renumbered from 0024_webhook_fixes_423_426.sql to resolve the duplicate
-- 0024 version. See docs/MIGRATIONS.md for the renumbering plan.
--
-- Every statement is idempotent so databases that already ran this file
-- under the old version number are unaffected.

-- #423: monotonic sequence for ordering deliveries within a subscription.
ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS backfill_seq BIGINT;

-- #426: record the last delivery status code and a short response snippet
-- so operators can diagnose failures without the full payload.
ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS last_status_code INTEGER;

ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS last_response_snippet TEXT;

CREATE INDEX IF NOT EXISTS idx_deliveries_sub_status
    ON webhook_deliveries (subscription_id, last_status_code);