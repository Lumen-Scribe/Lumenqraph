-- Seed the single indexer cursor row so the first cycle always has a row to update (#708).
--
-- `last_processed_ledger = 0` means "not started". Stellar ledgers begin at 1, so 0 can
-- never be a real position, and START_LEDGER applies until the first progress write.
-- Earlier counter upserts could create this row with 0 as a side effect; that is the same
-- "not started" state, so existing rows need no repair.
--
-- Idempotent: ON CONFLICT DO NOTHING leaves an existing row (and its progress) alone.
INSERT INTO indexer_cursor (id, last_processed_ledger)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
