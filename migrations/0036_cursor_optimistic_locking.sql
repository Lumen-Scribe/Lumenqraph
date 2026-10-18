-- Renumbered from 0022_cursor_optimistic_locking.sql to resolve duplicate
-- migration version 0022. See docs/MIGRATIONS.md for the renumbering plan.
-- Idempotent so databases that already ran this under the old number are
-- unaffected.

ALTER TABLE indexer_cursor
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;