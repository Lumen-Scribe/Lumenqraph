//! Cryptographic utilities for constant-time comparison of secrets and signatures.
//!
//! All functions in this module use constant-time comparison to prevent timing
//! attacks on sensitive data like HMAC signatures, API keys, and secrets.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Placeholder stored in the plaintext `secret` column once a secret has been
/// encrypted into `encrypted_secret`.
///
/// Migration `0020_webhook_secret_encryption_backfill.sql` attempted to encrypt
/// legacy plaintext secrets using `pgp_sym_encrypt(secret, current_setting('app.webhook_encryption_key', true))`,
/// but nothing ever sets that GUC, so `pgp_sym_encrypt(x, NULL)` returned `NULL`
/// and the backfill was a no-op. The backfill is therefore performed in
/// application code (where the key is available) using the SQL below.
pub const ENCRYPTED_SECRET_PLACEHOLDER: &str = "[encrypted]";

/// Idempotent SQL statement that encrypts legacy plaintext webhook secrets.
///
/// It encrypts every row whose `encrypted_secret` is still `NULL` and whose
/// plaintext `secret` is not already the `[encrypted]` placeholder, storing the
/// ciphertext in `encrypted_secret` and replacing the plaintext with the
/// placeholder. The `$1` parameter is the webhook encryption key.
///
/// Running this repeatedly is safe: once a row has been backfilled its
/// `encrypted_secret` is non-`NULL` and its `secret` is the placeholder, so it
/// no longer matches the `WHERE` clause.
pub const BACKFILL_WEBHOOK_SECRETS_SQL: &str = "UPDATE webhook_subscriptions \
     SET encrypted_secret = pgp_sym_encrypt(secret, $1), \
         secret = '[encrypted]' \
     WHERE encrypted_secret IS NULL \
       AND secret <> '[encrypted]'";

/// Errors returned by [`verify_webhook`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// The signature header did not contain a `t=` timestamp.
    MissingTimestamp,
    /// The `t=` value was not a valid Unix timestamp.
    InvalidTimestamp,
    /// The timestamp was outside the allowed tolerance (stale or replayed).
    StaleTimestamp,
    /// The header did not contain any `v1=` signature values.
    MissingSignature,
    /// A `v1=` value was not valid hex.
    InvalidHex,
    /// None of the provided signatures matched the computed HMAC.
    Mismatch,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            VerifyError::MissingTimestamp => "signature header is missing the timestamp",
            VerifyError::InvalidTimestamp => "signature header has an invalid timestamp",
            VerifyError::StaleTimestamp => "signature timestamp is outside the tolerance",
            VerifyError::MissingSignature => "signature header is missing a v1 signature",
            VerifyError::InvalidHex => "signature contains invalid hex",
            VerifyError::Mismatch => "signature does not match",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for VerifyError {}

/// Verify an `X-Lumenqraph-Signature` webhook header.
///
/// The header has the form `t=<unix_ts>,v1=<hex>[,v1=<hex>...]`. The signature
/// is an HMAC-SHA256 over `"{t}.{body}"`, where `{t}` is the timestamp string
/// exactly as it appeared in the header and `{body}` is the raw request body.
///
/// The timestamp must be within `tolerance` of the current time, which rejects
/// stale and replayed deliveries. Multiple `v1` values are supported (for
/// secret rotation); verification succeeds if any of them matches. Hex decoding
/// is case-insensitive and the comparison is constant time.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
/// use lumenqraph_core::crypto::verify_webhook;
///
/// let secret = "my-secret";
/// let header = "t=1700000000,v1=abc123...";
/// let body = b"webhook payload";
///
/// match verify_webhook(secret, header, body, Duration::from_secs(300)) {
///     Ok(()) => { /* signature is valid */ }
///     Err(_) => { /* signature is invalid */ }
/// }
/// ```
pub fn verify_webhook(
    secret: &str,
    header: &str,
    body: &[u8],
    tolerance: Duration,
) -> Result<(), VerifyError> {
    let mut timestamp: Option<&str> = None;
    let mut signatures: Vec<&str> = Vec::new();

    for part in header.split(',') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("t=") {
            timestamp = Some(value);
        } else if let Some(value) = part.strip_prefix("v1=") {
            signatures.push(value);
        }
    }

    let timestamp = timestamp.ok_or(VerifyError::MissingTimestamp)?;
    let ts: i64 = timestamp
        .parse()
        .map_err(|_| VerifyError::InvalidTimestamp)?;

    if signatures.is_empty() {
        return Err(VerifyError::MissingSignature);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    if now.abs_diff(ts) > tolerance.as_secs() {
        return Err(VerifyError::StaleTimestamp);
    }

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| VerifyError::Mismatch)?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    let expected = mac.finalize().into_bytes();

    for sig in signatures {
        let Ok(decoded) = hex::decode(sig) else {
            continue;
        };
        if decoded.len() == expected.len()
            && bool::from(expected.as_slice().ct_eq(decoded.as_slice()))
        {
            return Ok(());
        }
    }

    Err(VerifyError::Mismatch)
}

/// The placeholder shipped in older `.env.example` files. It is a public string,
/// so a key taken from it protects nothing.
pub const WEBHOOK_KEY_PLACEHOLDER_PREFIX: &str = "GENERATE_ME";

/// Minimum number of distinct characters a webhook encryption key must contain.
/// A random 32-byte key almost always has far more; this rejects repeated or
/// patterned values that still have the right length, such as `aaaa…`.
const MIN_DISTINCT_KEY_CHARS: usize = 8;

/// Check that `WEBHOOK_ENCRYPTION_KEY` is a strong key, not a placeholder.
///
/// Accepted forms are 64 hex characters (`openssl rand -hex 32`) or at least 43
/// base64 characters (32 bytes, `openssl rand -base64 32`). The value is used
/// exactly as given, so surrounding whitespace is rejected rather than trimmed.
/// Placeholders such as `GENERATE_ME_WITH_openssl_rand_hex_32` and short or
/// low-variety values are rejected with a message that says how to generate one.
pub fn validate_webhook_encryption_key(key: &str) -> anyhow::Result<()> {
    const HINT: &str = "generate a strong key with `openssl rand -hex 32`";

    if key
        .to_ascii_uppercase()
        .starts_with(WEBHOOK_KEY_PLACEHOLDER_PREFIX)
    {
        anyhow::bail!(
            "the value is the placeholder from .env.example, which is public and \
             gives no protection; {HINT}"
        );
    }

    let is_hex = key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit());
    let is_base64 = key.len() >= 43
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'-' | b'_' | b'='));
    if !is_hex && !is_base64 {
        anyhow::bail!(
            "must be 64 hex characters or at least 43 base64 characters (32 bytes of \
             entropy); {HINT}"
        );
    }

    let mut distinct = [false; 256];
    for b in key.bytes() {
        distinct[b as usize] = true;
    }
    let distinct_count = distinct.iter().filter(|&&seen| seen).count();
    if distinct_count < MIN_DISTINCT_KEY_CHARS {
        anyhow::bail!(
            "has only {distinct_count} distinct characters, which is too little entropy; {HINT}"
        );
    }

    Ok(())
}

/// Verify that two byte slices are equal using constant-time comparison.
///
/// This function is useful for comparing API keys, tokens, or other secrets
/// that have been hashed or encoded to bytes.
///
/// # Arguments
///
/// * `expected` - The expected bytes (e.g., from database)
/// * `provided` - The provided bytes (e.g., from request)
///
/// `true` if the bytes match (in constant time), `false` otherwise.
pub fn verify_bytes_equal(expected: &[u8], provided: &[u8]) -> bool {
    bool::from(expected.ct_eq(provided))
}

/// Compare two secrets (e.g. bearer tokens) in constant time.
///
/// Both values are hashed with SHA-256 first so that the comparison operates on
/// fixed-length digests. This avoids leaking the length of the expected secret
/// and lets callers compare arbitrary-length `&str` values without a
/// short-circuiting `==`.
pub fn verify_secret_equal(expected: &str, provided: &str) -> bool {
    let expected_digest = Sha256::digest(expected.as_bytes());
    let provided_digest = Sha256::digest(provided.as_bytes());
    verify_bytes_equal(expected_digest.as_slice(), provided_digest.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ts() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn sign(secret: &str, ts: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(ts.as_bytes());
        mac.update(b".");
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn test_verify_webhook_accepts_real_header() {
        let secret = "my-secret";
        let body = b"webhook payload";
        let ts = now_ts().to_string();
        let sig = sign(secret, &ts, body);
        let header = format!("t={ts},v1={sig}");
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(300)),
            Ok(())
        );
    }

    #[test]
    fn test_verify_webhook_accepts_uppercase_hex() {
        let secret = "my-secret";
        let body = b"webhook payload";
        let ts = now_ts().to_string();
        let sig = sign(secret, &ts, body).to_uppercase();
        let header = format!("t={ts},v1={sig}");
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(300)),
            Ok(())
        );
    }

    #[test]
    fn test_verify_webhook_accepts_any_of_multiple_v1() {
        let secret = "my-secret";
        let body = b"webhook payload";
        let ts = now_ts().to_string();
        let good = sign(secret, &ts, body);
        let bad = "0".repeat(64);
        let header = format!("t={ts},v1={bad},v1={good}");
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(300)),
            Ok(())
        );
    }

    /// The committed cross-language vector, shared with the Node, Python and Go
    /// examples in `docs/examples/webhooks` (all checked in CI).
    #[test]
    fn verifies_committed_cross_language_vector() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/examples/webhooks/test_vector.json"
        ))
        .unwrap();
        let secret = vector["secret"].as_str().unwrap();
        let header = vector["header"].as_str().unwrap();
        let body = vector["body"].as_str().unwrap().as_bytes();

        // The vector's timestamp is fixed in the past, so widen the tolerance
        // to keep this test independent of the wall clock.
        let wide = Duration::from_secs(i64::MAX as u64);
        assert_eq!(verify_webhook(secret, header, body, wide), Ok(()));

        let mut tampered = body.to_vec();
        tampered.push(b' ');
        assert_eq!(
            verify_webhook(secret, header, &tampered, wide),
            Err(VerifyError::Mismatch)
        );
        // With the default 300s tolerance the 2023 vector is stale.
        assert_eq!(
            verify_webhook(secret, header, body, Duration::from_secs(300)),
            Err(VerifyError::StaleTimestamp)
        );
    }

    #[test]
    fn test_verify_secret_equal_matches() {
        assert!(verify_secret_equal("s3cret-token", "s3cret-token"));
    }

    #[test]
    fn test_verify_secret_equal_rejects_mismatch() {
        assert!(!verify_secret_equal("s3cret-token", "s3cret-tokeN"));
        assert!(!verify_secret_equal("s3cret-token", "s3cret-token-longer"));
        assert!(!verify_secret_equal("s3cret-token", ""));
    }

    // A real key for accept cases; both forms are 32 bytes of entropy.
    const HEX_KEY: &str = "9f3a6c1e0b7d4a2f8e5c91b3d6a04f7e2c8b5d1a9e6f3c0b7a4d2e8f1c5b9a36";
    const B64_KEY: &str = "q7Zf3kP0mWc9Lx2RbT8yNn5VhJ1sDg4uEa6OiXrK0Mo";

    #[test]
    fn webhook_key_accepts_hex_and_base64_forms() {
        assert_eq!(HEX_KEY.len(), 64);
        assert!(validate_webhook_encryption_key(HEX_KEY).is_ok());
        // 43 characters is the minimum accepted base64 length (32 bytes).
        assert_eq!(B64_KEY.len(), 43);
        assert!(validate_webhook_encryption_key(B64_KEY).is_ok());
    }

    #[test]
    fn webhook_key_rejects_the_env_example_placeholder() {
        for placeholder in [
            "GENERATE_ME_WITH_openssl_rand_hex_32",
            "generate_me_with_openssl_rand_hex_32",
        ] {
            let err = validate_webhook_encryption_key(placeholder)
                .unwrap_err()
                .to_string();
            assert!(err.contains("placeholder"), "{err}");
            assert!(err.contains("openssl rand -hex 32"), "{err}");
        }
    }

    #[test]
    fn webhook_key_rejects_short_and_common_values() {
        for weak in ["changeme", "secret", "0123456789abcdef", "", " "] {
            let err = validate_webhook_encryption_key(weak)
                .unwrap_err()
                .to_string();
            assert!(err.contains("openssl rand -hex 32"), "{weak:?}: {err}");
        }
    }

    #[test]
    fn webhook_key_rejects_surrounding_whitespace() {
        let padded = format!(" {HEX_KEY}");
        assert!(validate_webhook_encryption_key(&padded).is_err());
        let trailing = format!("{HEX_KEY}\n");
        assert!(validate_webhook_encryption_key(&trailing).is_err());
    }

    #[test]
    fn webhook_key_rejects_repeated_characters() {
        let repeated = "a".repeat(64);
        let err = validate_webhook_encryption_key(&repeated)
            .unwrap_err()
            .to_string();
        assert!(err.contains("distinct characters"), "{err}");
    }
}
