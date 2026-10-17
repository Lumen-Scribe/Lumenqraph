-- Backfill encrypted_secret for existing webhooks and clear plaintext column
-- Part of issue #194: complete the webhook secret encryption migration

-- Backfill encrypted_secret for rows that still have plaintext secrets.
-- Requires the app.webhook_encryption_key GUC to be set on the migration
-- connection; when it is not set, pgp_sym_encrypt returns NULL and the
-- backfill is a no-op.
UPDATE webhook_subscriptions
SET encrypted_secret = pgp_sym_encrypt(secret, current_setting('app.webhook_encryption_key', true)),
    secret = '[encrypted]'
WHERE encrypted_secret IS NULL
  AND secret IS NOT NULL
  AND secret <> '[encrypted]';