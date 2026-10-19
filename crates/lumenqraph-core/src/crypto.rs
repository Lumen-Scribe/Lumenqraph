//! Cryptographic utilities for constant-time comparison of secrets and signatures.
//!
//! All functions in this module use constant-time comparison to prevent timing
//! attacks on sensitive data like HMAC signatures, API keys, and secrets.

use subtle::ConstantTimeEq;

/// Verify an HMAC-SHA256 signature using constant-time comparison.
///
/// This function compares the provided signature with the expected signature
/// using a constant-time algorithm, preventing timing attacks.
///
/// # Arguments
///
/// * `expected` - The expected signature (e.g., from HMAC calculation)
/// * `provided` - The provided signature (e.g., from HTTP header)
///
/// # Returns
///
/// `true` if the signatures match (in constant time), `false` otherwise.
///
/// # Example
///
/// ```ignore
/// use lumenqraph_core::crypto::verify_hmac_signature;
/// use hmac::{Hmac, Mac};
/// use sha2::Sha256;
///
/// type HmacSha256 = Hmac<Sha256>;
///
/// let secret = b"my-secret";
/// let body = b"webhook payload";
///
/// let mut mac = HmacSha256::new_from_slice(secret).unwrap();
/// mac.update(body);
/// let expected = hex::encode(mac.finalize().into_bytes());
///
/// let provided = "sha256=abc123...";
/// if verify_hmac_signature(&expected, provided) {
///     // Signature is valid
/// } else {
///     // Signature is invalid
/// }
/// ```
pub fn verify_hmac_signature(expected: &str, provided: &str) -> bool {
    // Extract the hex part after "sha256=" if present
    let provided_hex = if let Some(hex) = provided.strip_prefix("sha256=") {
        hex
    } else {
        provided
    };

    // Use constant-time comparison on the hex strings
    bool::from(expected.as_bytes().ct_eq(provided_hex.as_bytes()))
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
/// # Returns
///
/// `true` if the bytes match (in constant time), `false` otherwise.
pub fn verify_bytes_equal(expected: &[u8], provided: &[u8]) -> bool {
    bool::from(expected.ct_eq(provided))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_hmac_signature_with_prefix() {
        let expected = "abc123def456";
        let provided = "sha256=abc123def456";
        assert!(verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_hmac_signature_without_prefix() {
        let expected = "abc123def456";
        let provided = "abc123def456";
        assert!(verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_hmac_signature_mismatch() {
        let expected = "abc123def456";
        let provided = "sha256=abc123def457"; // Last digit differs
        assert!(!verify_hmac_signature(expected, provided));
    }

    #[test]
    fn test_verify_hmac_signature_missing_prefix() {
        let expected = "abc123def456";
        let provided = "sha256=different";
        assert!(!verify_hmac_signature(expected, provided));
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
}
