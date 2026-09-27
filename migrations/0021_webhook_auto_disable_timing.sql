-- Add timing columns for improved auto-disable logic (issue #451)
--
-- Auto-disable should be based on sustained outage duration, not just
-- concurrent failure counts. Track when the subscription last succeeded
-- and when the current failure streak started.

ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS first_failure_at TIMESTAMPTZ;

-- Index for finding subscriptions in sustained failure
CREATE INDEX IF NOT EXISTS idx_subs_failure_timing
    ON webhook_subscriptions (first_failure_at, last_success_at)
    WHERE active AND consecutive_failures > 0;
