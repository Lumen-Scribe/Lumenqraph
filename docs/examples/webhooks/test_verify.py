"""Checks verify.py against the committed cross-language test vector.

Run with: python3 -m unittest docs/examples/webhooks/test_verify.py
"""
import json
import os
import unittest

from verify import TOLERANCE_SECS, verify_webhook_signature

HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(HERE, "test_vector.json"), encoding="utf-8") as fh:
    VECTOR = json.load(fh)
BODY = VECTOR["body"].encode("utf-8")


class VerifyWebhookSignatureTests(unittest.TestCase):
    def test_accepts_committed_vector(self):
        self.assertTrue(
            verify_webhook_signature(BODY, VECTOR["header"], VECTOR["secret"], VECTOR["now"])
        )

    def test_rejects_tampered_body(self):
        self.assertFalse(
            verify_webhook_signature(BODY + b" ", VECTOR["header"], VECTOR["secret"], VECTOR["now"])
        )

    def test_rejects_wrong_secret(self):
        self.assertFalse(
            verify_webhook_signature(BODY, VECTOR["header"], "wrong-secret", VECTOR["now"])
        )

    def test_rejects_outside_tolerance(self):
        self.assertFalse(
            verify_webhook_signature(
                BODY,
                VECTOR["header"],
                VECTOR["secret"],
                VECTOR["timestamp"] + TOLERANCE_SECS + 1,
            )
        )

    def test_rejects_malformed_header(self):
        self.assertFalse(verify_webhook_signature(BODY, "v1=abc", VECTOR["secret"], VECTOR["now"]))
        self.assertFalse(
            verify_webhook_signature(BODY, f"t={VECTOR['timestamp']}", VECTOR["secret"], VECTOR["now"])
        )


if __name__ == "__main__":
    unittest.main()
