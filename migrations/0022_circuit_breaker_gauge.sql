-- Add a circuit-breaker gauge column to indexer_cursor.
-- Tracks the current consecutive-error count so the Prometheus /metrics
-- endpoint can expose it as `lumenqraph_consecutive_errors`.
-- Reset to 0 by the poller on the first successful cycle after a run of errors.

ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS consecutive_errors BIGINT NOT NULL DEFAULT 0;
