-- 0044_webhook_deliveries_restrict.sql
-- Renumbered from 0025 to resolve a duplicate version 0025 (see 0025_webhook_safe_decrypt.sql).
-- Idempotent (DROP IF EXISTS before ADD), so databases that already ran it under the old number are unaffected.
--
-- Issue #601: Retention cascades into `webhook_deliveries`, silently deleting
-- pending deliveries and delivery history.
--
-- `webhook_deliveries.event_id` was declared `REFERENCES events(event_id)
-- ON DELETE CASCADE` in migration 0002. The indexer's retention pruner deletes
-- old `events` rows in batches, so any delivery for those events was deleted
-- too -- including pending ones (subscriber down, auto-disabled, or
-- backlogged) and failed ones a user may want to redrive or audit.
--
-- Change the foreign key to `ON DELETE RESTRICT` so retention pruning of
-- `events` can no longer silently delete pending or historical deliveries.
-- The pruner is expected to skip events that still have deliveries attached
-- (see `crates/lumenqraph-indexer/src/retention.rs`).

ALTER TABLE webhook_deliveries
    DROP CONSTRAINT IF EXISTS webhook_deliveries_event_id_fkey;

ALTER TABLE webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_event_id_fkey
    FOREIGN KEY (event_id)
    REFERENCES events (event_id)
    ON DELETE RESTRICT;