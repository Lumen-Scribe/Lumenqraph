# --- begin doc example ---
import hashlib
import hmac

TOLERANCE_SECS = 300


def verify_webhook_signature(
    raw_body: bytes,
    signature_header: str,
    secret: str,
    now: int,
) -> bool:
    """Verify an X-Lumenqraph-Signature header: "t=<unix>,v1=<hex>[,v1=<hex>...]".

    `raw_body` must be the exact bytes of the HTTP body. Pass `now` as the
    current Unix time in seconds (for example, int(time.time())).
    """
    timestamp_text = None
    signatures = []
    for part in signature_header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t":
            timestamp_text = value
        elif key == "v1":
            signatures.append(value)

    if timestamp_text is None or not signatures:
        return False
    try:
        timestamp = int(timestamp_text)
    except ValueError:
        return False

    # Reject stale or future-dated deliveries (replay protection)
    if abs(now - timestamp) > TOLERANCE_SECS:
        return False

    # HMAC-SHA256 over "{t}.{body}", using the timestamp text exactly as sent
    expected = hmac.new(
        secret.encode("utf-8"),
        timestamp_text.encode("ascii") + b"." + raw_body,
        hashlib.sha256,
    ).digest()

    # Accept if any v1 matches (supports secret rotation); constant-time compare
    for sig in signatures:
        try:
            candidate = bytes.fromhex(sig)
        except ValueError:
            continue
        if hmac.compare_digest(candidate, expected):
            return True
    return False
# --- end doc example ---


if __name__ == "__main__":
    import json
    import sys

    with open(sys.argv[1], encoding="utf-8") as fh:
        vector = json.load(fh)
    ok = verify_webhook_signature(
        vector["body"].encode("utf-8"), vector["header"], vector["secret"], vector["now"]
    )
    print("vector verifies:", ok)
    sys.exit(0 if ok else 1)
