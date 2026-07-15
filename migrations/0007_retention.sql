-- Retention support: prune old rows to keep the index inside a bounded disk
-- budget (a free-tier Postgres is typically capped at ~500MB, and a hyperactive
-- SAC can emit ~500 events/ledger — see RETENTION_LEDGERS in .env.example).
--
-- The existing event indexes are all contract-first ((contract_id, ledger DESC),
-- event_name), which can't serve retention's "every event older than ledger N"
-- scan. This index does, and it also speeds up the global newest-first event
-- feed the explorer opens with.

CREATE INDEX IF NOT EXISTS idx_events_ledger ON events (ledger);
