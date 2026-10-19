//! Cryptographic utilities for constant-time comparison of secrets and signatures.
//!
//! All functions in this module use constant-time comparison to prevent timing
//! attacks on sensitive data like HMAC signatures, API keys, and secrets.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
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
    let tolerance_secs = tolerance.as_secs() as i64;
    if (now - ts).abs() > tolerance_secs {
        return Err(VerifyError::StaleTimestamp);
    }

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| VerifyError::Mismatch)?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    let expected = mac.finalize().into_bytes();

    let mut saw_candidate = false;
    for sig in signatures {
        let decoded = hex::decode(sig).map_err(|_| VerifyError::InvalidHex)?;
        saw_candidate = true;
        if bool::from(expected.as_slice().ct_eq(decoded.as_slice())) {
            return Ok(());
        }
    }

    if saw_candidate {
        Err(VerifyError::Mismatch)
    } else {
        Err(VerifyError::MissingSignature)
    }
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

    #[test]
    fn test_verify_webhook_rejects_mismatch() {
        let secret = "my-secret";
        let body = b"webhook payload";
        let ts = now_ts().to_string();
        let sig = sign("other-secret", &ts, body);
        let header = format!("t={ts},v1={sig}");
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(300)),
            Err(VerifyError::Mismatch)
        );
    }

    #[test]
    fn test_verify_webhook_rejects_stale_timestamp() {
        let secret = "my-secret";
        let body = b"webhook payload";
        let ts = (now_ts() - 3600).to_string();
        let sig = sign(secret, &ts, body);
        let header = format!("t={ts},v1={sig}");
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(300)),
            Err(VerifyError::StaleTimestamp)
        );
    }

    #[test]
    fn test_verify_webhook_rejects_replayed_old_timestamp() {
        let secret = "my-secret";
        let body = b"webhook payload";
        let ts = (now_ts() - 10).to_string();
        let sig = sign(secret, &ts, body);
        let header = format!("t={ts},v1={sig}");
        // Within a 300s tolerance it is accepted...
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(300)),
            Ok(())
        );
        // ...but a tighter tolerance treats it as a replay.
        assert_eq!(
            verify_webhook(secret, &header, body, Duration::from_secs(5)),
            Err(VerifyError::StaleTimestamp)
        );
    }

    #[test]
    fn test_verify_webhook_missing_timestamp() {
        let header = format!("v1={}", "0".repeat(64));
        assert_eq!(
            verify_webhook("secret", &header, b"body", Duration::from_secs(300)),
            Err(VerifyError::MissingTimestamp)
        );
    }

    #[test]
    fn test_verify_webhook_invalid_timestamp() {
        let header = format!("t=abc,v1={}", "0".repeat(64));
        assert_eq!(
            verify_webhook("secret", &header, b"body", Duration::from_secs(300)),
            Err(VerifyError::InvalidTimestamp)
        );
    }

    #[test]
    fn test_verify_webhook_invalid_hex() {
        let ts = now_ts().to_string();
        let header = format!("t={ts},v1=zzzz");
        assert_eq!(
            verify_webhook("secret", &header, b"body", Duration::from_secs(300)),
            Err(VerifyError::InvalidHex)
        );
    }

    #[test]
    fn test_verify_bytes_equal() {
        let expected = b"secret123";
        let provided = b"secret123";
        assert!(verify_bytes_equal(expected, provided));
    }

    #[test]
    fn test_verify_bytes_equal_mismatch() {
        let expected = b"secret123";
        let provided = b"secret124";
        assert!(!verify_bytes_equal(expected, provided));
    }

    #[test]
    fn test_verify_bytes_equal_different_lengths() {
        let expected = b"secret";
        let provided = b"secret123";
        assert!(!verify_bytes_equal(expected, provided));
    }

    #[test]
    fn test_backfill_sql_is_idempotent_and_encrypts() {
        // The backfill must only touch rows that still have a NULL
        // encrypted_secret and a non-placeholder plaintext secret, so that
        // re-running it is a no-op.
        assert!(BACKFILL_WEBHOOK_SECRETS_SQL.contains("encrypted_secret IS NULL"));
        assert!(BACKFILL_WEBHOOK_SECRETS_SQL.contains("secret <> '[encrypted]'"));
        assert!(BACKFILL_WEBHOOK_SECRETS_SQL.contains("pgp_sym_encrypt(secret, $1)"));
        assert_eq!(ENCRYPTED_SECRET_PLACEHOLDER, "[encrypted]");
    }
}
