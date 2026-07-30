"""Webhook signature verification helper."""

import hmac
import hashlib


async def verify_webhook(
    raw_body: str | bytes,
    signature_header: str,
    secret: str,
) -> bool:
    """
    Verify a Lumenqraph webhook delivery using its HMAC-SHA256 signature.

    The server signs the raw request body with the subscription secret and sends
    the result as `X-Lumenqraph-Signature: sha256=<hex>`. Pass that header value
    as `signature_header` and the raw (un-parsed) request body.

    Comparison is performed in constant time via hmac.compare_digest so this
    helper is safe to use in security-sensitive contexts. It mirrors the
    server-side `verify_hmac_signature()` in `lumenqraph-core/src/crypto.rs`.

    Args:
        raw_body: Raw HTTP request body (string or bytes)
        signature_header: Value of the X-Lumenqraph-Signature header (e.g., "sha256=abcdef...")
        secret: The subscription secret returned at creation time

    Returns:
        True if the signature is valid, False otherwise

    Example:
        >>> # FastAPI / Starlette
        >>> from fastapi import FastAPI, Request
        >>> from lumenqraph import verify_webhook
        >>>
        >>> app = FastAPI()
        >>>
        >>> @app.post("/hook")
        >>> async def handle_webhook(request: Request):
        ...     raw_body = await request.body()
        ...     signature = request.headers.get("x-lumenqraph-signature", "")
        ...     secret = os.environ["WEBHOOK_SECRET"]
        ...
        ...     if not await verify_webhook(raw_body, signature, secret):
        ...         return {"error": "invalid signature"}, 401
        ...
        ...     # process webhook...
        ...     return {"status": "ok"}
    """
    # Parse off the "sha256=" prefix
    prefix = "sha256="
    if not signature_header.startswith(prefix):
        return False

    provided_hex = signature_header[len(prefix) :]

    # Convert body to bytes if needed
    body_bytes = raw_body.encode("utf-8") if isinstance(raw_body, str) else raw_body

    # Compute expected signature
    expected_mac = hmac.new(
        secret.encode("utf-8"), body_bytes, hashlib.sha256
    ).hexdigest()

    # Constant-time comparison
    return hmac.compare_digest(expected_mac, provided_hex)
