-- No-op (#362, #566). Legacy plaintext webhook secrets are encrypted at
-- webhooks service startup by `dispatcher::backfill_encrypted_secrets`, which
-- holds the encryption key. The column this migration once referenced,
-- `encrypted_secret`, is only added by 0034, so it cannot run here.
SELECT 1;
