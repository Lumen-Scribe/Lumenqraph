ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS contract_ids TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS event_names TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS filter JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_contract_ids
    ON webhook_subscriptions USING GIN (contract_ids);
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_event_names
    ON webhook_subscriptions USING GIN (event_names);
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_filter
    ON webhook_subscriptions USING GIN (filter);

ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS last_status_code INTEGER,
    ADD COLUMN IF NOT EXISTS last_response_snippet VARCHAR(512);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_last_status_code
    ON webhook_deliveries (last_status_code);
