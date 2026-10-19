-- API-key lifecycle metadata and scheduled revocation for the management API.
-- Existing keys remain valid and receive the conservative `read` scope.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS scopes TEXT[] NOT NULL DEFAULT '{read}',
    ADD COLUMN IF NOT EXISTS revocation_scheduled_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_api_keys_last_used_at
    ON api_keys (last_used_at);

CREATE INDEX IF NOT EXISTS idx_api_keys_scheduled_revocation
    ON api_keys (revocation_scheduled_at)
    WHERE revocation_scheduled_at IS NOT NULL AND NOT revoked;
