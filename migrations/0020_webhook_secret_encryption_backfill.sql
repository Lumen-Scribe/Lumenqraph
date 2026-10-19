-- Backfill encrypted_secret for existing webhooks and clear plaintext column
-- Part of issue #194: complete the webhook secret encryption migration

-- Backfill encrypted_secret for any rows where it's NULL
-- Uses the same WEBHOOK_ENCRYPTION_KEY that dispatcher.rs reads
UPDATE webhook_subscriptions
SET encrypted_secret = pgp_sym_encrypt(secret, current_setting('app.webhook_encryption_key', true))
WHERE encrypted_secret IS NULL
  AND secret IS NOT NULL
  AND secret != '[encrypted]';

-- Clear the plaintext secret column (replace with placeholder)
UPDATE webhook_subscriptions
SET secret = '[encrypted]'
WHERE secret != '[encrypted]' AND encrypted_secret IS NOT NULL;

-- Future migration will drop the secret column once all instances are updated
-- For now, keep it to maintain backward compatibility during rolling deployments
