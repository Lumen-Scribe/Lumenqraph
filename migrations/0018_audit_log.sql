-- Audit logging for API key authentication and webhook management actions.

-- Immutable audit log of authenticated requests and mutating actions.
-- Tracks which API key performed which action when, for incident response and abuse investigation.
CREATE TABLE IF NOT EXISTS audit_log (
    id                BIGSERIAL       PRIMARY KEY,
    -- Request-level audit: who, what, when
    key_hash_prefix   TEXT            NOT NULL,  -- First 8 chars of the key hash (identifying without exposing)
    route             TEXT            NOT NULL,  -- HTTP route accessed (e.g., /contracts, /webhooks)
    http_method       TEXT            NOT NULL,  -- GET, POST, PATCH, DELETE
    status_code       INT             NOT NULL,  -- HTTP response status code
    timestamp         TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Optional: mutating action details (for webhook/key operations)
    action_type       TEXT,           -- 'webhook_create', 'webhook_delete', 'webhook_update', 'key_revoke', etc.
    resource_id       TEXT,           -- webhook ID, key hash, etc.

    created_at        TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Index for recent audit trails (common query: "what happened in the last 24 hours?")
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp
    ON audit_log (timestamp DESC);

-- Index for per-key auditing (common query: "what did this key do?")
CREATE INDEX IF NOT EXISTS idx_audit_log_key
    ON audit_log (key_hash_prefix, timestamp DESC);

-- Index for action auditing (common query: "all webhook deletes")
CREATE INDEX IF NOT EXISTS idx_audit_log_action
    ON audit_log (action_type, timestamp DESC)
    WHERE action_type IS NOT NULL;

-- Index for finding a key's last-used timestamp
CREATE INDEX IF NOT EXISTS idx_audit_log_key_recent
    ON audit_log (key_hash_prefix, timestamp DESC)
    WHERE status_code < 400;
