-- Add GIN index on enriched column for efficient containment queries
-- The param filter uses enriched @> containment queries which require this index
-- jsonb_path_ops is more space-efficient than the default for @> queries

CREATE INDEX IF NOT EXISTS idx_events_enriched
    ON events USING GIN (enriched jsonb_path_ops);

-- Note: This migration is safe to run online. The index is created with IF NOT EXISTS
-- so it's idempotent. For large tables, consider using CREATE INDEX CONCURRENTLY instead:
--
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_enriched
--     ON events USING GIN (enriched jsonb_path_ops);
