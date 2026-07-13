-- Contract state indexing: versioned snapshots of a contract's *instance*
-- storage (the enumerable key→value map Soroban keeps in the contract's
-- instance ledger entry — admin, config, metadata, global counters, …).
--
-- One row per (contract, ledger-at-which-the-instance-last-changed), so the
-- table is a time series of state: the latest row is current state, older rows
-- are history. Decoded to friendly JSON, same as events.

CREATE TABLE IF NOT EXISTS contract_state (
    contract_id  TEXT        NOT NULL,
    -- lastModifiedLedgerSeq of the instance entry when this snapshot was taken.
    ledger       BIGINT      NOT NULL,
    storage      JSONB       NOT NULL,
    captured_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, ledger)
);

-- "latest state for a contract" and "state history" both want newest-first.
CREATE INDEX IF NOT EXISTS idx_contract_state_latest
    ON contract_state (contract_id, ledger DESC);

-- Query state by decoded content (e.g. a contract whose admin is address X).
CREATE INDEX IF NOT EXISTS idx_contract_state_storage
    ON contract_state USING GIN (storage);
