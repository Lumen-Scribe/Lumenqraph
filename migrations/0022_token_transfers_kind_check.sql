-- Add a database-level CHECK constraint on token_transfers.kind so that only
-- the four permitted values ('transfer', 'mint', 'burn', 'clawback') can ever
-- be stored. The application-level enforcement in store::extract_transfer is
-- correct, but an unconstrained TEXT column allows a bug, a migration, or a
-- direct database write to introduce rows with unexpected kind values that
-- would be silently served by the API.
--
-- Issue: #215

ALTER TABLE token_transfers
    ADD CONSTRAINT chk_transfer_kind
    CHECK (kind IN ('transfer', 'mint', 'burn', 'clawback'));
