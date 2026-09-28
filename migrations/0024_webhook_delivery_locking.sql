-- Add row-level locking to webhook_deliveries for safe horizontal scaling.
--
-- Without locking, multiple dispatcher instances fetch and deliver the same
-- rows, causing duplicate POSTs to subscribers. This migration adds columns
-- to claim deliveries atomically.

ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS locked_by TEXT;

-- Index for finding claimable rows: pending, due, and not locked.
CREATE INDEX IF NOT EXISTS idx_deliveries_claimable
    ON webhook_deliveries (status, next_attempt_at)
    WHERE status = 'pending' AND (locked_until IS NULL OR locked_until < now());

COMMENT ON COLUMN webhook_deliveries.locked_until IS 
    'Lease expiry time for delivery claim. Allows crashed workers'' claims to be retried.';
COMMENT ON COLUMN webhook_deliveries.locked_by IS 
    'Worker identifier that claimed this delivery (hostname, process ID, or instance ID).';
