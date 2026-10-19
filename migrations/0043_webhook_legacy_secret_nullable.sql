-- The plaintext `secret` column was superseded by `encrypted_secret` (#194), but
-- 0002 created it NOT NULL and nothing relaxed it, so POST /webhooks failed (#565).
--
-- Inserts no longer write the legacy column. It is kept for now so existing rows
-- are not lost; drop it in a later migration once every row has an encrypted secret.

ALTER TABLE webhook_subscriptions
    ALTER COLUMN secret DROP NOT NULL;
