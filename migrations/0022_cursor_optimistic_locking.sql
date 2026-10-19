-- Add optimistic locking to indexer_cursor to prevent concurrent writer conflicts.
-- This ensures only one indexer instance can successfully advance the cursor at a time,
-- preventing lost updates or cursor rollbacks during rolling deployments.

ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
