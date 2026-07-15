-- Contract upgrade watch: an append-only history of each contract's on-chain
-- interface, plus the semantic diff between consecutive versions.
--
-- Soroban contracts are upgradable in place, so the interface behind a contract
-- ID is not a constant — it's a time series. `contract_specs` holds only the
-- *current* interface (it's upserted on upgrade); this table remembers every
-- version we ever observed, so a consumer can ask what changed and when.

CREATE TABLE IF NOT EXISTS contract_spec_versions (
    id                 BIGSERIAL   PRIMARY KEY,
    contract_id        TEXT        NOT NULL,
    -- 1 = the first interface we ever saw for this contract, which is not
    -- necessarily the contract's first deploy (we only see it once it's indexed).
    version            INTEGER     NOT NULL,
    wasm_hash          TEXT        NOT NULL,
    previous_wasm_hash TEXT,
    interface          JSONB       NOT NULL,
    -- Raw `contractspecv0` section (hex), kept per version so a later upgrade
    -- can be diffed against this exact interface without refetching WASM that
    -- the contract no longer runs (and that RPC may no longer serve).
    spec_section       TEXT        NOT NULL DEFAULT '',
    -- Diff against the previous version. NULL for version 1: nothing to compare
    -- against, which is distinct from "compared and found no changes" ('{}').
    diff               JSONB,
    breaking           BOOLEAN     NOT NULL DEFAULT FALSE,
    observed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Lets a concurrent writer lose the race harmlessly (ON CONFLICT DO NOTHING)
    -- rather than fork the history.
    UNIQUE (contract_id, version)
);

CREATE INDEX IF NOT EXISTS idx_spec_versions_contract
    ON contract_spec_versions (contract_id, version DESC);

-- Feed upgrades through the existing webhook pipeline ------------------------

-- Subscriptions are now typed. 'event' (the default) preserves exactly the
-- behaviour every existing row already has; 'upgrade' subscribes to interface
-- changes instead. Keeping them separate means an existing "all events from
-- contract X" subscription doesn't silently start receiving a new payload shape.
ALTER TABLE webhook_subscriptions
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'event';

-- A delivery now points at either an event or a spec version, never both and
-- never neither.
ALTER TABLE webhook_deliveries
    ALTER COLUMN event_id DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS upgrade_id BIGINT
        REFERENCES contract_spec_versions(id) ON DELETE CASCADE;

DO $$ BEGIN
    ALTER TABLE webhook_deliveries
        ADD CONSTRAINT delivery_targets_one_thing
        CHECK ((event_id IS NOT NULL) <> (upgrade_id IS NOT NULL));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Dedupe upgrade deliveries the way UNIQUE (subscription_id, event_id) dedupes
-- event ones. It has to be its own index because NULLs are distinct in a UNIQUE
-- constraint, so the existing one can't dedupe rows whose event_id is NULL.
CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_upgrade
    ON webhook_deliveries (subscription_id, upgrade_id)
    WHERE upgrade_id IS NOT NULL;

-- Second watermark, alongside last_seq: upgrades stream by their own monotonic
-- id, so a quiet period in one stream can't stall the other.
ALTER TABLE webhook_state
    ADD COLUMN IF NOT EXISTS last_upgrade_id BIGINT NOT NULL DEFAULT 0;
