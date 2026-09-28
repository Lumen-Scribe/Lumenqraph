-- #423: per-subscription backfill cursor.
-- A subscription created with `since` stores a starting_seq below the current
-- global watermark. The new backfill_seq column tracks how far the per-sub
-- backfill has progressed (NULL = fully caught up / no backfill requested).
ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS backfill_seq BIGINT;

-- #426: extend delivery history with the target event/upgrade id, the next
-- attempt time (already exists on the row but was not exposed by the API), the
-- HTTP status code returned by the subscriber, and the first 512 bytes of the
-- response body to aid debugging without storing arbitrarily large payloads.
ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS last_status_code     INTEGER,
    ADD COLUMN IF NOT EXISTS last_response_snippet TEXT;

-- Index to support the new ?status= filter on GET /webhooks/:id/deliveries
-- without scanning the whole table.
CREATE INDEX IF NOT EXISTS idx_deliveries_sub_status
    ON webhook_deliveries (subscription_id, status, next_attempt_at);
