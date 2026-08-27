"""Tests for webhook signature verification."""

import unittest
from lumenqraph import verify_webhook_signature


class TestWebhookSignature(unittest.TestCase):
    """Test webhook signature verification."""

    def test_valid_signature(self):
        """Test verifying a valid signature."""
        body = '{"event": "test"}'
        secret = "test-secret"
        # Pre-computed HMAC-SHA256 of body with secret
        signature = "9b80d3e1d0b7d7d0b0e0e1e2e3e4e5e6e7e8e9e0e1e2e3e4e5e6e7e8e9e"

        # We'll compute it directly for testing
        import hmac
        import hashlib
        computed_sig = hmac.new(
            secret.encode("utf-8"),
            body.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        # Verify with the computed signature
        self.assertTrue(verify_webhook_signature(body, computed_sig, secret))

    def test_invalid_signature(self):
        """Test that invalid signature fails."""
        body = '{"event": "test"}'
        secret = "test-secret"
        invalid_sig = "invalid-signature-here"

        self.assertFalse(verify_webhook_signature(body, invalid_sig, secret))

    def test_wrong_secret(self):
        """Test that wrong secret fails verification."""
        body = '{"event": "test"}'
        secret = "test-secret"
        wrong_secret = "wrong-secret"

        import hmac
        import hashlib
        signature = hmac.new(
            secret.encode("utf-8"),
            body.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        self.assertFalse(verify_webhook_signature(body, signature, wrong_secret))

    def test_empty_signature(self):
        """Test that empty signature fails."""
        body = '{"event": "test"}'
        secret = "test-secret"

        self.assertFalse(verify_webhook_signature(body, "", secret))

    def test_empty_secret(self):
        """Test that empty secret fails."""
        body = '{"event": "test"}'
        signature = "some-signature"

        self.assertFalse(verify_webhook_signature(body, signature, ""))


if __name__ == "__main__":
    unittest.main()
