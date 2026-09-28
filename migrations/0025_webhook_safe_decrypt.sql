-- Safe decryption function that returns NULL instead of raising an error.
--
-- pgp_sym_decrypt raises an exception when the key is wrong or data is corrupt,
-- aborting the entire query. This wrapper catches the exception and returns NULL,
-- allowing the dispatcher to handle decryption failures per-row.

CREATE OR REPLACE FUNCTION safe_decrypt_webhook_secret(
    encrypted_data BYTEA,
    encryption_key TEXT
) RETURNS TEXT AS $$
BEGIN
    RETURN pgp_sym_decrypt(encrypted_data, encryption_key);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION safe_decrypt_webhook_secret IS
    'Decrypt webhook secret with pgcrypto, returning NULL on failure instead of raising an error';
