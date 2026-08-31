"""Tests for webhook signature verification."""

import hmac
import hashlib
import unittest

from lumenqraph import verify_webhook_signature


def _sign(body: str, secret: str) -> str:
    """Helper: compute the canonical ``sha256=<hex>`` signature."""
    digest = hmac.new(
        secret.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"


class TestWebhookSignature(unittest.TestCase):
    """Test webhook signature verification."""

    def test_valid_signature(self):
        """Valid body + secret → True."""
        body = '{"event": "test"}'
        secret = "test-secret"
        sig = _sign(body, secret)
        self.assertTrue(verify_webhook_signature(body, sig, secret))

    def test_invalid_signature(self):
        """Tampered hex value → False."""
        body = '{"event": "test"}'
        secret = "test-secret"
        self.assertFalse(verify_webhook_signature(body, "sha256=deadbeef", secret))

    def test_wrong_secret(self):
        """Correct format but wrong secret → False."""
        body = '{"event": "test"}'
        sig = _sign(body, "correct-secret")
        self.assertFalse(verify_webhook_signature(body, sig, "wrong-secret"))

    def test_missing_prefix(self):
        """Signature without ``sha256=`` prefix → False."""
        body = '{"event": "test"}'
        secret = "test-secret"
        bare_hex = hmac.new(
            secret.encode("utf-8"),
            body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        # No "sha256=" prefix → should be rejected
        self.assertFalse(verify_webhook_signature(body, bare_hex, secret))

    def test_empty_signature(self):
        """Empty signature header → False."""
        self.assertFalse(verify_webhook_signature('{"event": "test"}', "", "secret"))

    def test_empty_secret(self):
        """Empty secret → False (guard against misconfiguration)."""
        body = '{"event": "test"}'
        sig = _sign(body, "real-secret")
        self.assertFalse(verify_webhook_signature(body, sig, ""))

    def test_body_tampered(self):
        """Signature over original body does not verify against modified body."""
        original = '{"amount": "100"}'
        tampered = '{"amount": "999"}'
        secret = "s3cr3t"
        sig = _sign(original, secret)
        self.assertFalse(verify_webhook_signature(tampered, sig, secret))


if __name__ == "__main__":
    unittest.main()
