"""Webhook signature verification utilities.

The Lumenqraph server signs `"{timestamp}.{raw_body}"` with the subscription secret
and sends the result as::

    X-Lumenqraph-Signature: t=<timestamp>,v1=<hex>

This module provides :func:`verify_webhook_signature` to validate that header
in constant time using :mod:`hmac` from the Python standard library, with
replay protection via timestamp validation.
"""

import hmac
import hashlib
import time


def verify_webhook_signature(
    body: str,
    signature_header: str,
    secret: str,
    tolerance_secs: int = 300,
) -> bool:
    """Verify a Lumenqraph webhook delivery signature with replay protection.

    The server computes ``HMAC-SHA256(secret, "{timestamp}.{raw_body}")`` and sends
    the result as ``X-Lumenqraph-Signature: t=<timestamp>,v1=<hex>``.  Pass that
    full header value as *signature_header*.

    This function enforces timestamp freshness to prevent replay attacks. By default,
    signatures older than 5 minutes are rejected. You can customize this via
    *tolerance_secs*.

    Comparison is performed in constant time via :func:`hmac.compare_digest`
    so this function is safe to use in security-sensitive contexts.  It mirrors
    the server-side ``verify_hmac_signature()`` in
    ``lumenqraph-core/src/crypto.rs`` and the ``verifyWebhook`` helper in the
    TypeScript SDK.

    Args:
        body:              Raw HTTP request body as a string.
        signature_header:  Value of the ``X-Lumenqraph-Signature`` header,
                           e.g. ``"t=1727090000,v1=abcdef…"`` or legacy ``"sha256=abcdef…"``.
        secret:            The subscription secret returned at creation time.
        tolerance_secs:    Maximum age of the timestamp in seconds (default: 300 = 5 minutes).
                           Set to 0 to disable timestamp validation (not recommended).

    Returns:
        ``True`` if the signature is valid and fresh, ``False`` otherwise.

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

    # Try new timestamped format first: "t=<timestamp>,v1=<hex>"
    if "t=" in signature_header and "v1=" in signature_header:
        return _verify_timestamped_signature(body, signature_header, secret, tolerance_secs)

    # Fall back to legacy format: "sha256=<hex>"
    # This path will be removed in a future release (deprecated)
    return _verify_legacy_signature(body, signature_header, secret)


def _verify_timestamped_signature(
    body: str,
    signature_header: str,
    secret: str,
    tolerance_secs: int,
) -> bool:
    """Verify timestamped signature format: t=<timestamp>,v1=<hex>"""
    # Parse header
    parts = signature_header.split(",")
    timestamp = None
    signatures = []

    for part in parts:
        key_value = part.split("=", 1)
        if len(key_value) != 2:
            continue
        key, value = key_value
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError:
                return False
        elif key == "v1":
            signatures.append(value)

    if timestamp is None or not signatures:
        return False

    # Check timestamp freshness to prevent replay attacks
    if tolerance_secs > 0:
        now = int(time.time())
        age = abs(now - timestamp)
        if age > tolerance_secs:
            return False

    # Construct signed payload: "{timestamp}.{body}"
    signed_payload = f"{timestamp}.{body}"

    # Compute expected signature
    computed_hex = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    # Check against all provided v1 signatures (supports secret rotation)
    for provided_hex in signatures:
        if hmac.compare_digest(computed_hex, provided_hex):
            return True

    return False


def _verify_legacy_signature(
    body: str,
    signature_header: str,
    secret: str,
) -> bool:
    """Verify legacy signature format: sha256=<hex>"""
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
