# API reference

Base URL defaults to `http://localhost:8080`.

Auth: data routes accept an API key via `Authorization: Bearer <key>` or
`x-api-key: <key>`. When `REQUIRE_API_KEY=false` (default), unauthenticated
callers are allowed up to `ANON_RATE_LIMIT_PER_MIN`. `/health` and `/metrics`
are always public. Rate-limit breaches return `429`; bad/revoked keys `401`.

## Public

### `GET /health`
```json
{ "status": "ok", "last_processed_ledger": 3550886, "chain_tip_ledger": 3550886,
  "lag_ledgers": 0, "seconds_since_cursor_update": 1,
  "events_ingested_total": 4895, "errors_total": 0 }
```

### `GET /metrics`
Prometheus text: `lumenqraph_indexer_lag_ledgers`, `lumenqraph_events_total`,
`lumenqraph_indexer_ingested_total`, `lumenqraph_indexer_errors_total`,
`lumenqraph_api_requests_total`, …

## Data (authenticated / rate-limited)

### `GET /contracts`
Contracts seen, with `event_count`, `first_seen_ledger`, `last_seen_ledger`.

### `GET /contracts/:id/events`
Query: `limit` (1–1000, default 50), `offset`, `event_name` (e.g. `transfer`).
Each row has raw base64 (`topics`, `value`) **and** decoded JSON
(`decoded_topics`, `decoded_value`), plus `event_name`, `tx_hash`, `ledger`, …

### `GET /contracts/:id/transfers`
Materialized SEP-41 transfers. Query: `limit`, `offset`, `from`, `to`.
```json
[{ "from_addr": "G...", "to_addr": "G...", "amount": "100000000000",
   "ledger": 3550885, "event_id": "..." }]
```

## Webhooks (authenticated)

### `POST /webhooks`
Body: `{ "url": "https://...", "contract_id": null, "event_name": "transfer" }`
(filters optional). Returns the subscription including a one-time `secret` used
to verify the `X-Lumenqraph-Signature: sha256=<hmac>` header on deliveries.

### `GET /webhooks`
Lists subscriptions (secrets omitted).

### `DELETE /webhooks/:id`
Removes a subscription (and cascades its deliveries).

### Verifying a delivery
`HMAC-SHA256(secret, raw_request_body)` hex must equal the value after
`sha256=` in `X-Lumenqraph-Signature`.
