-- Add enrichment coverage and RPC observability metrics to indexer_cursor.
-- These track enrichment success/failure and RPC call patterns for operational
-- visibility into the indexer's behavior.

ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS events_enriched_total        BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS events_not_enriched_total    BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS spec_fetch_failures_total    BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rpc_calls_total              BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rpc_errors_total             BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rpc_errors_32001_total       BIGINT NOT NULL DEFAULT 0;
