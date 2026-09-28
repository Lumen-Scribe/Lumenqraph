-- #398: Add rpc_failovers_total counter to indexer_cursor so the Prometheus
-- scrape endpoint can expose endpoint failovers as a metric.
ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS rpc_failovers_total BIGINT NOT NULL DEFAULT 0;
