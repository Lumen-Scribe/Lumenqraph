-- Track when the cursor last advanced, separately from `updated_at` (#757).
--
-- Counter helpers (incr_errors, track_rpc_call, ...) bump `updated_at` without
-- advancing the cursor, so a failing indexer looked fresh. Health checks now
-- measure freshness from `progress_at`, which only `write_progress` sets.
ALTER TABLE indexer_cursor ADD COLUMN IF NOT EXISTS progress_at TIMESTAMPTZ;
UPDATE indexer_cursor SET progress_at = updated_at WHERE progress_at IS NULL;
