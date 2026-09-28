-- #399: Track ledgerClosedAt parse failures so the /metrics endpoint can
-- expose lumenqraph_indexer_timestamp_parse_errors_total.
ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS timestamp_parse_errors_total BIGINT NOT NULL DEFAULT 0;
