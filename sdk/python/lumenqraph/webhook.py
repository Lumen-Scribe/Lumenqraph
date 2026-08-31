"""Webhook signature verification utilities.

The Lumenqraph server signs the raw request body with the subscription secret
and sends the result as::

    X-Lumenqraph-Signature: sha256=<hex>

This module provides :func:`verify_webhook_signature` to validate that header
in constant time using :mod:`hmac` from the Python standard library.
"""

import hmac
import hashlib


def verify_webhook_signature(
    body: str,
    signature_header: str,
    secret: str,
) -> bool:
    """Verify a Lumenqraph webhook delivery signature.

    The server computes ``HMAC-SHA256(secret, raw_body)`` and sends the result
    as ``X-Lumenqraph-Signature: sha256=<hex>``.  Pass that full header value
    (including the ``sha256=`` prefix) as *signature_header*.

    Comparison is performed in constant time via :func:`hmac.compare_digest`
    so this function is safe to use in security-sensitive contexts.  It mirrors
    the server-side ``verify_hmac_signature()`` in
    ``lumenqraph-core/src/crypto.rs`` and the ``verifyWebhook`` helper in the
    TypeScript SDK.

    Args:
        body:              Raw HTTP request body as a string.
        signature_header:  Value of the ``X-Lumenqraph-Signature`` header,
                           e.g. ``"sha256=abcdef…"``.
        secret:            The subscription secret returned at creation time.

    Returns:
        ``True`` if the signature is valid, ``False`` otherwise.

    Example::

        from lumenqraph import verify_webhook_signature

        # Flask example
        @app.route("/hook", methods=["POST"])
        def webhook():
            sig = request.headers.get("X-Lumenqraph-Signature", "")
            body = request.get_data(as_text=True)
            if not verify_webhook_signature(body, sig, WEBHOOK_SECRET):
                return {"error": "invalid signature"}, 401
            # process payload …
            return {}, 200
    """
    if not signature_header or not secret:
        return False

    prefix = "sha256="
    if not signature_header.startswith(prefix):
        return False

    provided_hex = signature_header[len(prefix):]

    computed_hex = hmac.new(
        secret.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    # Constant-time comparison prevents timing-oracle attacks.
    return hmac.compare_digest(computed_hex, provided_hex)
