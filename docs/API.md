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

## Contract interface & upgrades

A Soroban contract can be upgraded in place, so its interface is a time series.
Version 1 is the first interface the indexer ever saw (a baseline: `diff` is
`null` and it fires no webhook); each later version is an upgrade. Requires the
indexer's `UPGRADE_WATCH` (on by default when `CONTRACT_IDS` is set).

### `GET /contracts/:id/interface`
The decoded on-chain interface: `functions`, `events`, `structs`, `unions`,
`enums`. Query: `version` (a historical version; default is the current one).

### `GET /contracts/:id/interface/history`
Every version observed, newest first. Query: `limit` (1–200, default 50).
```json
{ "contract_id": "CB...", "count": 2, "versions": [
  { "version": 2, "wasm_hash": "...", "previous_wasm_hash": "...",
    "breaking": true, "observed_at": "2026-07-15T...Z",
    "diff": { "breaking": true, "summary": ["removed function withdraw() -> void"],
              "functions": { "added": [], "removed": ["withdraw() -> void"], "changed": [] },
              "events": { "added": [], "removed": [], "changed": [] },
              "types":  { "added": [], "removed": [], "changed": [] } } },
  { "version": 1, "previous_wasm_hash": null, "breaking": false, "diff": null }
] }
```

### `GET /contracts/:id/interface/diff`
Diff any two versions. Query: `from`, `to` (default: the latest upgrade, i.e.
`to` = newest, `from` = the one before). `400` if the contract has only a
baseline version, or if `from` == `to`; `404` for an unknown version.

`breaking` is true when anything was removed or changed — an integration built
against the old interface may no longer work. Additions alone are not breaking.

## Webhooks (authenticated)

### `POST /webhooks`
Body: `{ "url": "https://...", "kind": "event", "contract_id": null, "event_name": "transfer" }`
(all but `url` optional). Returns the subscription including a one-time `secret`
used to verify the `X-Lumenqraph-Signature: sha256=<hmac>` header on deliveries.

`kind` is `event` (default: a contract emitted an event; the payload is the event
row) or `upgrade` (a contract's interface changed). `event_name` doesn't apply to
`upgrade` subscriptions — scope them with `contract_id`, or omit it for all
contracts. An `upgrade` delivery is signed identically, and carries:
```json
{ "type": "contract.upgraded", "contract_id": "CB...", "version": 2,
  "wasm_hash": "...", "previous_wasm_hash": "...", "breaking": true,
  "diff": { "...": "as in /interface/diff" }, "observed_at": "2026-07-15T...Z" }
```

### `GET /webhooks`
Lists subscriptions (secrets omitted).

### `DELETE /webhooks/:id`
Removes a subscription (and cascades its deliveries).

### Verifying a delivery
`HMAC-SHA256(secret, raw_request_body)` hex must equal the value after
`sha256=` in `X-Lumenqraph-Signature`.
