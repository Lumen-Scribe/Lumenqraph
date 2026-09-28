-- Keyset pagination for GET /webhooks/:id/deliveries (#445): the handler pages
-- with `WHERE subscription_id = $1 AND id < $cursor ORDER BY id DESC`.
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_sub_id
    ON webhook_deliveries (subscription_id, id DESC);
