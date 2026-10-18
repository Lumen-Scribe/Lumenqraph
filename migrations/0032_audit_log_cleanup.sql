-- Forward migration: apply post-release changes that were previously edited
-- in place into 0018_audit_log.sql. Released migrations are append-only, so
-- any intended schema changes must live in new forward migrations like this
-- one. All statements are idempotent so this is safe to run on both fresh and
-- upgraded databases.

-- Drop the legacy index that was removed from the released migration.
DROP INDEX IF EXISTS idx_audit_log_key_recent;

-- Add any columns that were introduced by editing the released migration.
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS actor TEXT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();