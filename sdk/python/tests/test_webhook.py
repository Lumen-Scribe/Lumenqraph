"""Tests for webhook signature verification."""

import hmac
import hashlib
import unittest
from unittest.mock import patch

from lumenqraph import LumenqraphClient, verify_webhook_signature


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


class TestWebhookLifecycleClient(unittest.TestCase):
    """Webhook lifecycle requests use the shared client request machinery."""

    def setUp(self):
        self.client = LumenqraphClient("http://test", retry={"max_retries": 0})

    def test_redrive_webhook_sends_optional_since_query(self):
        with patch.object(self.client, "_make_request", return_value={"redriven": 2}) as request:
            self.assertEqual(
                self.client.redrive_webhook("wh-1", "2026-01-01T00:00:00Z"),
                {"redriven": 2},
            )
        request.assert_called_once_with(
            "POST", "/webhooks/wh-1/redrive", {"since": "2026-01-01T00:00:00Z"}
        )

    def test_reenable_webhook_posts_to_lifecycle_endpoint(self):
        with patch.object(self.client, "_post", return_value={"reenabled": True}) as post:
            self.assertEqual(self.client.reenable_webhook("wh-1"), {"reenabled": True})
        post.assert_called_once_with("/webhooks/wh-1/reenable")

    def test_rotate_webhook_secret_sends_grace_period(self):
        result = {
            "id": "wh-1",
            "secret": "new-secret",
            "previous_secret_expires_at": "2026-01-02T00:00:00Z",
        }
        with patch.object(self.client, "_post", return_value=result) as post:
            self.assertEqual(self.client.rotate_webhook_secret("wh-1", 60), result)
        post.assert_called_once_with(
            "/webhooks/wh-1/rotate-secret", {"grace_seconds": 60}
        )


if __name__ == "__main__":
    unittest.main()
