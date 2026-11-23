-- Track when the cursor last ADVANCED (`last_processed_ledger` moved), separate
-- from `updated_at`, which every counter write (`incr_errors`, `set_consecutive_errors`,
-- `track_rpc_call`, `track_enrichment`, `track_timestamp_parse_error`, ...) bumps
-- without indexing anything (#756, #757).
--
-- Health freshness (`/health`, `/readyz`, the stalled alert) is computed from
-- `progress_at`: a poller whose every cycle fails refreshes `updated_at` but never
-- `progress_at`, and must be reported as degraded/stuck instead of fresh.
--
-- `progress_at` is NULL until the first progress write; NULL means "never
-- progressed" and is treated as maximally stale by the health endpoints.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS, and the backfill only fills rows that
-- already progressed (last_processed_ledger > 0). Rows that never progressed
-- stay NULL on purpose: counter-only writes must not fabricate freshness (#757).
ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS progress_at TIMESTAMPTZ;

UPDATE indexer_cursor
   SET progress_at = updated_at
 WHERE progress_at IS NULL
   AND last_processed_ledger > 0;
