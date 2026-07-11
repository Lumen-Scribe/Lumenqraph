-- Core schema: raw events + the indexer's ledger cursor.
-- The /contracts view is derived from events at query time, so there is no
-- separate contracts table to keep in sync.

CREATE TABLE IF NOT EXISTS events (
    event_id            TEXT        PRIMARY KEY,   -- RPC `id`; dedupe key
    contract_id         TEXT        NOT NULL,
    ledger              BIGINT      NOT NULL,
    ledger_closed_at    TIMESTAMPTZ NOT NULL,
    event_type          TEXT        NOT NULL,      -- contract | system | diagnostic
    topics              JSONB       NOT NULL,      -- array of base64 XDR topics
    event_name          TEXT,                      -- best-effort topic[0] symbol
    value               TEXT        NOT NULL,      -- base64 XDR event body
    tx_hash             TEXT        NOT NULL,
    in_successful_call  BOOLEAN     NOT NULL,
    paging_token        TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serve "recent events for a contract" (ORDER BY ledger DESC) efficiently.
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger
    ON events (contract_id, ledger DESC);

-- Support filtering/aggregating by event name.
CREATE INDEX IF NOT EXISTS idx_events_name
    ON events (contract_id, event_name);

-- Single-row cursor tracking how far the indexer has processed.
CREATE TABLE IF NOT EXISTS indexer_cursor (
    id                     INTEGER     PRIMARY KEY DEFAULT 1,
    last_processed_ledger  BIGINT      NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);
