-- Production schema: decoded event JSON, richer indexer status, API keys,
-- and webhook subscriptions + delivery queue.

-- Decoded JSON alongside the raw base64 (raw stays lossless).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS decoded_topics JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS decoded_value  JSONB NOT NULL DEFAULT 'null'::jsonb;

-- Query events by decoded content (e.g. a transfer's `to` address).
CREATE INDEX IF NOT EXISTS idx_events_decoded_value
    ON events USING GIN (decoded_value);

-- Monotonic sequence so the webhook service can stream new events in order via
-- a single watermark (event_id is not monotonic).
ALTER TABLE events ADD COLUMN IF NOT EXISTS seq BIGSERIAL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_seq ON events (seq);

-- Webhook enqueue watermark (single row).
CREATE TABLE IF NOT EXISTS webhook_state (
    id       INTEGER PRIMARY KEY DEFAULT 1,
    last_seq BIGINT  NOT NULL DEFAULT 0,
    CONSTRAINT single_row_state CHECK (id = 1)
);

-- Extend the cursor into a full status row so /health and /metrics can report
-- lag (chain tip - processed) and lifetime counters.
ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS chain_tip_ledger       BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS events_ingested_total  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS errors_total           BIGINT NOT NULL DEFAULT 0;

-- API keys. Only the SHA-256 hash of the key is stored.
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash            TEXT        PRIMARY KEY,
    name                TEXT        NOT NULL,
    tier                TEXT        NOT NULL DEFAULT 'free',
    rate_limit_per_min  INTEGER     NOT NULL DEFAULT 60,
    revoked             BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Webhook subscriptions: register a URL + optional filters, get pushed events.
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    url          TEXT        NOT NULL,
    contract_id  TEXT,                       -- NULL = any contract
    event_name   TEXT,                       -- NULL = any event
    secret       TEXT        NOT NULL,       -- HMAC signing secret
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subs_filter
    ON webhook_subscriptions (active, contract_id, event_name);

-- Delivery queue: one row per (event, matching subscription), retried with
-- backoff until delivered or exhausted.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id               BIGSERIAL   PRIMARY KEY,
    subscription_id  UUID        NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    event_id         TEXT        NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    status           TEXT        NOT NULL DEFAULT 'pending',  -- pending | delivered | failed
    attempts         INTEGER     NOT NULL DEFAULT 0,
    last_error       TEXT,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_due
    ON webhook_deliveries (status, next_attempt_at);
