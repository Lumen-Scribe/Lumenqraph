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

## Payload Envelope

Every delivery body is a JSON envelope. The `X-Lumenqraph-Event` header carries the same `type` value, so you can route on either.

```json
{
  "id": "12345",
  "type": "contract.event",
  "version": 1,
  "created_at": "2025-01-15T10:30:01Z",
  "data": {
    "event_id": "0000000000012345-0000000001",
    "contract_id": "CADQZ...",
    "ledger": 12345,
    "ledger_closed_at": "2025-01-15T10:30:00Z",
    "event_name": "transfer",
    "topics": ["AAAADwAAAAd0cmFuc2Zlcg==", "..."],
    "decoded_topics": ["..."],
    "value": "AAAADwAA...",
    "decoded_value": { "from": "GBVFX...", "to": "GBVFY...", "amount": "1000000" },
    "enriched": { "from": "GBVFX...", "to": "GBVFY...", "amount": 1000000 },
    "tx_hash": "...",
    "in_successful_call": true
  }
}
```

### Envelope fields

| Field | Type | Meaning |
|-------|------|---------|
| `id` | string | Delivery ID. Same value as `X-Lumenqraph-Delivery-Id`; use it to deduplicate. |
| `type` | string | `contract.event` or `contract.upgraded`. Determines the shape of `data`. |
| `version` | integer | Envelope schema version. Currently `1`. It changes only for breaking envelope changes. |
| `created_at` | string | RFC 3339 UTC time the delivery was created. |
| `data` | object | The payload for `type`, described below. |

Within a version, new optional fields may be added. Ignore fields you do not recognise rather than rejecting the delivery.

### `contract.event` data

| Field | Type | Meaning |
|-------|------|---------|
| `event_id` | string | Identifier of the indexed event. |
| `contract_id` | string | Emitting contract (`C...` strkey). |
| `ledger` | integer | Ledger sequence containing the event. |
| `ledger_closed_at` | string | RFC 3339 UTC close time of that ledger. |
| `event_name` | string \| null | Name taken from the first topic, when it is a symbol. |
| `topics` | string[] | Base64 XDR topics, as emitted. |
| `value` | string | Base64 XDR event value. |
| `decoded_topics` | any | Topics decoded to JSON. |
| `decoded_value` | any | Value decoded to JSON. |
| `enriched` | object \| null | Named, typed record from the contract spec; `null` when no spec matched. |
| `tx_hash` | string | Hash of the transaction that emitted the event. |
| `in_successful_call` | boolean | Whether the emitting call succeeded. |

The internal `seq` column is not included in the payload.

### `contract.upgraded` data

```json
{
  "id": "12346",
  "type": "contract.upgraded",
  "version": 1,
  "created_at": "2025-01-15T10:30:00Z",
  "data": {
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
}
```

| Field | Type | Meaning |
|-------|------|---------|
| `contract_id` | string | Upgraded contract. |
| `version` | integer | Interface version after the upgrade. |
| `wasm_hash` | string | Hash of the new WASM. |
| `previous_wasm_hash` | string \| null | Hash of the WASM it replaced. |
| `breaking` | boolean | Whether the interface diff contains breaking changes. |
| `diff` | object | `breaking` flag and human-readable `summary` lines. |
| `observed_at` | string | RFC 3339 UTC time the upgrade was observed. |

**Version 1 is never delivered** — it's the baseline interface when we first see a contract, not an upgrade. Only version 2+ trigger upgrade webhooks.

## Signature Verification

### The Signature Header

Each webhook includes a cryptographic signature in the `X-Lumenqraph-Signature` header:

```
X-Lumenqraph-Signature: t=1700000000,v1=abc123def456...
```

The header is a comma-separated list of `key=value` pairs:

- `t` — Unix timestamp (seconds) at which the delivery was signed.
- `v1` — HMAC-SHA256 signature, hex-encoded. There may be **more than one** `v1` value (e.g. during secret rotation); the delivery is valid if **any** `v1` matches.

### Bytes Signed

The HMAC is computed over the string `"{t}.{body}"`, where `{t}` is the timestamp **exactly as it appears in the header** and `{body}` is the exact bytes sent in the HTTP POST body. No whitespace normalization or canonicalization occurs — sign the raw bytes as-is. Use the raw body, not a re-serialized JSON object.

### Replay Protection

Reject deliveries whose `t` is too far from the current time. A tolerance of 300 seconds (5 minutes) is recommended; the examples below use it. This prevents an attacker from replaying a previously captured, validly signed request. You should also deduplicate on the `X-Lumenqraph-Delivery-Id` header (or the envelope `id`).

### Verification Examples

The Node.js, Python and Go examples below are the files in [`docs/examples/webhooks/`](examples/webhooks/). CI runs their test suites against the committed [cross-language test vector](#cross-language-test-vector), so these snippets are known to verify correctly.

#### Node.js / JavaScript

Uses the built-in `crypto.timingSafeEqual()` for constant-time comparison. Pass the current Unix time as `nowSecs` only when testing; the default reads the clock.

```javascript

```

#### Python

```python

```

#### Go

```go

```

#### Rust

Use `lumenqraph_core::crypto::verify_webhook()`, which parses `t=`/`v1=`, enforces the timestamp tolerance, computes the HMAC over `{t}.{body}`, decodes hex case-insensitively, and compares in constant time:

```rust
use std::time::Duration;
use lumenqraph_core::crypto::{verify_webhook, VerifyError};

fn check_webhook(raw_body: &[u8], signature_header: &str, secret: &str) -> bool {
    match verify_webhook(secret, signature_header, raw_body, Duration::from_secs(300)) {
        Ok(()) => true,
        Err(VerifyError::StaleTimestamp) => {
            // Replayed or delayed delivery — reject
            false
        }
        Err(_) => false,
    }
}
```

For production Rust servers (e.g., Axum), capture the raw body before JSON parsing:

```rust
use std::time::Duration;
use axum::body::Bytes;
use axum::http::HeaderMap;
use lumenqraph_core::crypto::verify_webhook;

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

    verify_webhook(&secret, signature, &body, Duration::from_secs(300))
        .map_err(|e| anyhow::anyhow!("invalid signature: {e}"))?;

    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    // Process webhook...
    Ok(StatusCode::OK)
}
```

### Cross-language test vector

[`docs/examples/webhooks/test_vector.json`](examples/webhooks/test_vector.json) holds a fixed secret, timestamp, raw body and header (with one rotated, non-matching `v1` value). Every verifier must accept it at `now` and reject a modified body, a wrong secret, and a delivery outside the tolerance. `scripts/check_webhook_examples.py` runs the Node, Python, Go and Rust checks, and confirms that each snippet above matches its file. The Rust check runs as part of `lumenqraph-core`'s tests.

## HTTP Headers

Every webhook delivery includes these headers:

| Header | Example | Purpose |
|--------|---------|---------|
| `X-Lumenqraph-Signature` | `t=1700000000,v1=abc123...` | HMAC-SHA256 signature over `{t}.{body}` (see [Signature Verification](#signature-verification)) |
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
| `WEBHOOK_ENCRYPTION_KEY` | **REQUIRED** | Symmetric key for encrypting secrets at rest (pgcrypto). Generate with `openssl rand -hex 32`. Must be 64 hex characters or at least 43 base64 characters with enough variety. Both services refuse to start if it is missing, blank, the `.env.example` placeholder, or weak. |

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
  event_id          TEXT REFERENCES events(event_id) ON DELETE RESTRICT,
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

`event_id` uses `ON DELETE RESTRICT` rather than `ON DELETE CASCADE` so that deleting an
event can never silently remove a delivery row. See [Retention and Event Pruning](#retention-and-event-pruning).

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

## Retention and Event Pruning

When `RETENTION_LEDGERS` is configured, the indexer periodically prunes old rows from the
`events` table in batches. Webhook deliveries reference events, so pruning interacts with the
delivery queue as follows:

- **Active pending deliveries are never pruned away.** The pruner skips an event while an active,
  non-auto-disabled subscription has a `pending` delivery for it, including retry backoff. Pending
  deliveries for paused or auto-disabled subscriptions become `failed` with reason `expired` after
  `WEBHOOK_PENDING_MAX_AGE` days (7 by default), so abandoned subscriptions cannot pin events forever.
- **Delivered/failed history is pruned with the event.** Once a delivery is no longer pending
  (`delivered` or `failed`), it no longer blocks pruning. For each batch of out-of-window
  events, the pruner deletes the `delivered`/`failed` delivery rows for those events and then
  deletes the events themselves, in the same transaction. The deleted delivery rows are counted
  in the `lumenqraph_indexer_webhook_deliveries_dropped_by_retention_total` metric. If you need to audit
  or redrive failed deliveries, do so before the event falls outside the retention window, or
  increase `RETENTION_LEDGERS`.
- **`ON DELETE RESTRICT` is a safety net.** Because `webhook_deliveries.event_id` is declared
  `ON DELETE RESTRICT`, any attempt to delete an event that still has a delivery row fails
  rather than cascading. This guarantees that a bug in the pruner cannot silently drop delivery
  history: the pruner must remove the delivery rows explicitly before the event can be deleted.
- **Each table's prune is independent.** A failure while pruning one table (for example
  `events`) does not skip the others (`contract_state`, `contract_data`, spec versions) in the
  same pass.

> **Note:** Retention is opt-in. With `RETENTION_LEDGERS` unset, events and delivery history are
> kept indefinitely.

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
2. Sign the string `"{t}.{body}"` (timestamp from the header, then a literal `.`, then the raw body)
3. Extract the signature from the `X-Lumenqraph-Signature` header (not from the body) and accept any `v1` value
4. Check the `t` timestamp is within your tolerance (e.g. 300 seconds) — stale deliveries are rejected
5. Use constant-time comparison (`crypto.timingSafeEqual` in Node, `verify_webhook()` in Rust)
6. Verify the secret matches the one in your subscription

### Deliveries Stuck in Pending

1. Check webhook service logs: `RUST_LOG=lumenqraph_webhooks=debug`
2. Verify the webhook URL is reachable
3. Check for network/firewall issues
4. Monitor `webhook_deliveries` for timeout or DNS errors in `last_error`
5. If a subscription is down for a long time, pause or auto-disable it. Its pending deliveries
   expire after `WEBHOOK_PENDING_MAX_AGE` days, allowing source-event retention to proceed (see
   [Retention and Event Pruning](#retention-and-event-pruning)).

## See Also

- [CONFIGURATION.md](CONFIGURATION.md) — Webhook environment variables
- [SECURITY.md](../SECURITY.md) — Cryptographic details and best practices
- [SCHEMA.md](SCHEMA.md) — Webhook tables and schema details
