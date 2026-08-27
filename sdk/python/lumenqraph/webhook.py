"""Webhook signature verification utilities."""

import hmac
import hashlib
from typing import Optional


def verify_webhook_signature(body: str, signature: str, secret: str) -> bool:
    """Verify a webhook signature.

    Args:
        body: The raw webhook body as a string
        signature: The signature from the X-Webhook-Signature header
        secret: The webhook secret (from the webhook configuration)

    Returns:
        True if the signature is valid, False otherwise

    Example:
        def webhook_handler(request):
            signature = request.headers.get('X-Webhook-Signature')
            body = request.get_data(as_text=True)
            secret = "your-webhook-secret"

            if not verify_webhook_signature(body, signature, secret):
                return {"error": "Invalid signature"}, 401

            # Process webhook
            return {"ok": True}, 200
    """
    if not signature or not secret:
        return False

    # Compute HMAC-SHA256 of the body with the secret
    computed = hmac.new(
        secret.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    # Use constant-time comparison to prevent timing attacks
    return hmac.compare_digest(computed, signature)
