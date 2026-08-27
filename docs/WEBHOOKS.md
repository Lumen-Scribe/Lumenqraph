# Webhooks

Lumenqraph provides webhook support to push indexed events and contract upgrades to your application in real-time, with cryptographic signature verification for authenticity.

## Overview

The webhook service continuously monitors indexed data and delivers notifications when:

1. **New events** are indexed from tracked contracts
2. **Contract upgrades** are detected (WASM executable changes)

Each webhook is signed with HMAC-SHA256 using your subscription's secret, preventing replay attacks and ensuring integrity.

## Subscription Management

### Creating a Subscription

Webhooks are managed through the `webhook_subscriptions` table:

```sql
INSERT INTO webhook_subscriptions (url, kind, contract_id, event_name, secret)
VALUES (
  'https://your-app.example.com/webhooks/lumenqraph',
  'event',                    -- 'event' or 'upgrade'
  NULL,                        -- NULL = any contract
  'transfer',                  -- NULL = any event name
  'your-webhook-secret'
);
```

### Filtering

- **kind**: `'event'` (contract events) or `'upgrade'` (contract upgrades)
- **contract_id**: Filter by contract ID (NULL = all contracts)
- **event_name**: Filter by event name (NULL = all events)
- **starting_seq**: Optional watermark for backfill/recovery (defaults to 0)

### Managing Subscriptions

Enable/disable:
```sql
UPDATE webhook_subscriptions SET active = false WHERE id = '...';
```

Auto-disabled subscriptions track failures:
```sql
SELECT url, auto_disabled_reason, consecutive_failures 
FROM webhook_subscriptions 
WHERE auto_disabled_at IS NOT NULL;
```

## Payload Shapes

### Event Payload

When a contract event matches a subscription with `kind = 'event'`, the webhook body contains the raw event row:

```json
{
  "event_id": "...",
  "contract_id": "CADQZ...",
  "ledger": 12345,
  "ledger_closed_at": "2025-01-15T10:30:00Z",
  "event_type": "contract",
  "topics": [
    "AAAADwAAAAd0cmFuc2Zlcg==",
    "..."
  ],
  "decoded_topics": [...],
  "event_name": "transfer",
  "value": "AAAADwAA...",
  "decoded_value": {
    "from": "GBVFX...",
    "to": "GBVFY...",
    "amount": "1000000"
  },
  "enriched": {
    "from": "GBVFX...",
    "to": "GBVFY...",
    "amount": 1000000
  },
  "tx_hash": "...",
  "in_successful_call": true,
  "paging_token": "...",
  "created_at": "2025-01-15T10:30:01Z"
}
```

**Note:** The `seq` field is present in the database but removed from the webhook payload to avoid exposing internal sequencing.

### Upgrade Payload

When a contract's WASM executable changes and matches an `kind = 'upgrade'` subscription, the webhook body contains:

```json
{
  "type": "contract.upgraded",
  "contract_id": "CADQZ...",
  "version": 2,
  "wasm_hash": "abc123...",
  "previous_wasm_hash": "abc122...",
  "breaking": true,
  "diff": {
    "breaking": true,
    "summary": [
      "removed function withdraw() -> void",
      "added function emergencyWithdraw() -> void"
    ]
  },
  "observed_at": "2025-01-15T10:30:00Z"
}
```

**Version 1 is never delivered** — it's the baseline interface when we first see a contract, not an upgrade. Only version 2+ trigger upgrade webhooks.

## Signature Verification

### The Signature Header

Each webhook includes a cryptographic signature in the `X-Lumenqraph-Signature` header:

```
X-Lumenqraph-Signature: sha256=abc123def456...
```

The signature is computed as HMAC-SHA256 over the **raw request body** (before any JSON parsing) using your subscription's secret.

### Bytes Signed

The HMAC is computed over the exact bytes sent in the HTTP POST body. No whitespace normalization or canonicalization occurs — sign the raw bytes as-is.

### Verification Examples

#### Node.js / JavaScript

Use the built-in `crypto.timingSafeEqual()` for constant-time comparison to prevent timing attacks:

```javascript
const crypto = require("crypto");

function verifyWebhookSignature(rawBody, signatureHeader, secret) {
  // Compute the expected signature
  const expected = "sha256=" + 
    crypto.createHmac("sha256", secret)
      .update(rawBody)
      .digest("hex");

  // Use timing-safe comparison to prevent timing attacks
  try {
    return crypto.timingSafeEqual(
      Buffer.from(signatureHeader),
      Buffer.from(expected)
    );
  } catch {
    // Buffers have different lengths
    return false;
  }
}

// Usage in Express.js
app.post("/webhooks/lumenqraph", express.raw({ type: "application/json" }), (req, res) => {
  const signature = req.headers["x-lumenqraph-signature"];
  const secret = process.env.LUMENQRAPH_WEBHOOK_SECRET;

  if (!verifyWebhookSignature(req.body, signature, secret)) {
    return res.status(401).json({ error: "Invalid signature" });
  }

  // Process the webhook
  const payload = JSON.parse(req.body);
  console.log("Event:", payload.event_id);
  res.json({ status: "ok" });
});
```

#### Rust

Use the `lumenqraph_core::crypto::verify_hmac_signature()` function (constant-time by default):

```rust
use hmac::{Hmac, Mac};
use sha2::Sha256;
use lumenqraph_core::crypto::verify_hmac_signature;

type HmacSha256 = Hmac<Sha256>;

fn verify_webhook(raw_body: &[u8], signature_header: &str, secret: &str) -> bool {
    // Compute expected signature
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("invalid secret");
    mac.update(raw_body);
    let expected = hex::encode(mac.finalize().into_bytes());

    // Use constant-time comparison
    verify_hmac_signature(&expected, signature_header)
}
```

For production Rust servers (e.g., Axum), capture the raw body before JSON parsing:

```rust
use axum::extract::RawBody;
use bytes::Bytes;

async fn webhook_handler(
    body: Bytes,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let signature = headers
        .get("x-lumenqraph-signature")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("missing signature header"))?;

    let secret = std::env::var("LUMENQRAPH_WEBHOOK_SECRET")
        .expect("LUMENQRAPH_WEBHOOK_SECRET");

    if !verify_webhook(&body, signature, &secret) {
        return Err(anyhow::anyhow!("invalid signature"));
    }

    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    // Process webhook...
    Ok(StatusCode::OK)
}
```

## HTTP Headers

Every webhook delivery includes these headers:

| Header | Example | Purpose |
|--------|---------|---------|
| `X-Lumenqraph-Signature` | `sha256=abc123...` | HMAC-SHA256 signature over the request body |
| `X-Lumenqraph-Delivery-Id` | `12345` | Unique delivery attempt ID (deduplication key) |
| `X-Lumenqraph-Timestamp` | `2025-01-15T10:30:00Z` | RFC3339 UTC timestamp of delivery |
| `X-Lumenqraph-Attempt` | `1` | Attempt number (1 = first, incremented on retry) |
| `X-Lumenqraph-Event` | `contract.event` or `contract.upgraded` | Event type for routing |
| `User-Agent` | `lumenqraph-webhooks/0.1` | Identifies Lumenqraph as the sender |
| `Content-Type` | `application/json` | Always application/json |

## Retry Behavior

Failed deliveries (non-2xx response or timeout) are retried with exponential backoff + jitter:

- **Attempt 1**: Immediate
- **Attempt 2**: 0–2 seconds delay
- **Attempt 3**: 0–4 seconds delay
- **Attempt 4**: 0–8 seconds delay
- **Attempt 5**: 0–16 seconds delay
- **Attempt 6**: 0–3600 seconds delay (max 1 hour)

After 6 failed attempts, the delivery is marked as `failed` in the `webhook_deliveries` table.

### Auto-Disable Threshold

If a subscription exceeds consecutive failures, it's automatically disabled and marked with:

- `auto_disabled_at`: Timestamp of auto-disable
- `auto_disabled_reason`: Human-readable reason
- `active`: Set to `false`

To re-enable, manually set `active = true`:

```sql
UPDATE webhook_subscriptions SET active = true WHERE id = '...';
```

## Configuration

Webhook service behavior is controlled by environment variables (see [`docs/CONFIGURATION.md`](CONFIGURATION.md)):

| Variable | Default | Purpose |
|----------|---------|---------|
| `WEBHOOK_TICK_SECS` | 3 | Poll database every N seconds for deliveries |
| `WEBHOOK_BATCH_SIZE` | 100 | Enqueue up to N deliveries per cycle |
| `WEBHOOK_MAX_ATTEMPTS` | 6 | Maximum retry attempts before marking failed |
| `WEBHOOK_CONNECT_TIMEOUT_SECS` | 5 | TCP connection timeout per delivery |
| `WEBHOOK_TOTAL_TIMEOUT_SECS` | 10 | Total time (connect + request) per delivery |
| `WEBHOOK_MAX_CONCURRENT_PER_HOST` | 5 | Max concurrent deliveries to a single host |
| `WEBHOOK_MAX_CONCURRENT_DELIVERIES` | 100 | Max total concurrent deliveries |
| `WEBHOOK_FAILURE_THRESHOLD` | 10 | Consecutive failures before auto-disable |
| `WEBHOOK_ENCRYPTION_KEY` | `default-key-for-testing` | Symmetric key for encrypting secrets at rest (pgcrypto) |

## Database Schema

Webhooks are managed through three tables:

### `webhook_subscriptions`

Registered webhook endpoints and their filters:

```sql
CREATE TABLE webhook_subscriptions (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  url                   TEXT NOT NULL,
  kind                  TEXT NOT NULL,         -- 'event' or 'upgrade'
  contract_id           TEXT,                  -- NULL = any
  event_name            TEXT,                  -- NULL = any
  secret                TEXT NOT NULL,         -- plaintext fallback
  encrypted_secret      TEXT,                  -- pgcrypto encrypted
  active                BOOLEAN NOT NULL DEFAULT TRUE,
  starting_seq          BIGINT DEFAULT 0,      -- backfill watermark
  consecutive_failures  INTEGER NOT NULL DEFAULT 0,
  auto_disabled_at      TIMESTAMPTZ,
  auto_disabled_reason  TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### `webhook_deliveries`

Outgoing delivery queue with retry state:

```sql
CREATE TABLE webhook_deliveries (
  id                BIGSERIAL PRIMARY KEY,
  subscription_id   UUID NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
  event_id          TEXT REFERENCES events(event_id) ON DELETE CASCADE,
  upgrade_id        BIGINT REFERENCES contract_spec_versions(id) ON DELETE CASCADE,
  status            TEXT NOT NULL DEFAULT 'pending',  -- pending | delivered | failed
  attempts          INTEGER NOT NULL DEFAULT 0,
  last_error        TEXT,
  next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  delivered_at      TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (subscription_id, event_id) WHERE event_id IS NOT NULL,
  UNIQUE (subscription_id, upgrade_id) WHERE upgrade_id IS NOT NULL
);
```

### `webhook_state`

Single-row table tracking the delivery watermark (internal use only):

```sql
CREATE TABLE webhook_state (
  id                INTEGER PRIMARY KEY DEFAULT 1,
  last_seq          BIGINT NOT NULL DEFAULT 0,
  last_upgrade_id   BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT single_row_state CHECK (id = 1)
);
```

## Encryption at Rest

Webhook secrets can be encrypted in the database using pgcrypto's symmetric encryption:

- **Plaintext**: Stored in the `secret` column (default fallback)
- **Encrypted**: Stored in the `encrypted_secret` column using `pgp_sym_encrypt()`

The encryption key is configured via `WEBHOOK_ENCRYPTION_KEY` and should be a strong random value. The dispatcher automatically decrypts on delivery:

```sql
COALESCE(pgp_sym_decrypt(s.encrypted_secret, $1), s.secret)
```

## Delivery State Diagram

```
                       ┌──────────────┐
                       │    Pending   │
                       └──────────────┘
                              │
                              ↓
        ┌─────────────────────────────────────────┐
        │    POST to webhook URL                  │
        │  (with signature & retry headers)       │
        └─────────────────────────────────────────┘
                        │         │
                ✓       │         │      ✗ (non-2xx or timeout)
              ┌─────────┴─────────┴─────────┐
              ↓                             ↓
          ┌─────────┐          ┌───────────────────┐
          │Delivered│          │  Retry Scheduled  │
          └─────────┘          └───────────────────┘
                                       │
                        ┌──────────────┴──────────────┐
                        │                             │
                        ↓                             ↓
                  Attempts < Max         Attempts >= Max
                        │                             │
                        ↓                             ↓
                   Exponential Backoff        ┌──────────────┐
                        │                     │    Failed    │
                        └────────────────────→└──────────────┘
```

## Testing Webhooks Locally

Use a tool like [RequestBin](https://requestbin.com/) or [Webhook.cool](https://webhook.cool/) to capture and inspect webhook payloads:

1. Create a temporary endpoint at one of these services
2. Insert a subscription pointing to that endpoint:
   ```sql
   INSERT INTO webhook_subscriptions (url, kind, secret)
   VALUES ('https://your-requestbin.example.com/...', 'event', 'test-secret');
   ```
3. Index an event that matches your filters
4. Check the requestbin dashboard to see the HTTP POST, headers, and signature

## Troubleshooting

### Webhooks Not Firing

1. Check `webhook_subscriptions.active = true`
2. Verify filters match (`contract_id`, `event_name`)
3. Check `webhook_state.last_seq` is advancing
4. Look for errors in `webhook_deliveries.last_error`

### Signature Verification Fails

1. Ensure you're signing the **raw request body bytes**, not a normalized JSON string
2. Extract the signature from the `X-Lumenqraph-Signature` header (not from the body)
3. Use constant-time comparison (`crypto.timingSafeEqual` in Node, `verify_hmac_signature()` in Rust)
4. Verify the secret matches the one in your subscription

### Deliveries Stuck in Pending

1. Check webhook service logs: `RUST_LOG=lumenqraph_webhooks=debug`
2. Verify the webhook URL is reachable
3. Check for network/firewall issues
4. Monitor `webhook_deliveries` for timeout or DNS errors in `last_error`

## See Also

- [CONFIGURATION.md](CONFIGURATION.md) — Webhook environment variables
- [SECURITY.md](../SECURITY.md) — Cryptographic details and best practices
- [SCHEMA.md](SCHEMA.md) — Webhook tables and schema details
