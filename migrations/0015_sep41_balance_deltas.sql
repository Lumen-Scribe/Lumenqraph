-- Project SEP-41 mint/burn/clawback events into token_transfers as balance deltas.
-- The kind column distinguishes the type of balance movement:
-- 'transfer' = transfer event (from/to both present)
-- 'mint' = mint event (to only, from is NULL)
-- 'burn' = burn event (from only, to is NULL)
-- 'clawback' = clawback event (from only, to is NULL) — administrative seizure
--
-- For mint/burn/clawback, the counterparty is NULL since these operations
-- don't involve a distinct sender/recipient in the traditional sense.

ALTER TABLE token_transfers
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'transfer';

-- Create index for filtering by balance delta kind
CREATE INDEX IF NOT EXISTS idx_transfers_kind ON token_transfers (kind);
