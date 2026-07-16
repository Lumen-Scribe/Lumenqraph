# API reference

Base URL defaults to `http://localhost:8080`.

Auth: data routes accept an API key via `Authorization: Bearer <key>` or
`x-api-key: <key>`. When `REQUIRE_API_KEY=false` (default), unauthenticated
callers are allowed up to `ANON_RATE_LIMIT_PER_MIN`. `/health` and `/metrics`
are always public. Rate-limit breaches return `429`; bad/revoked keys `401`.

## Public

### `GET /health`
```json
{ "status": "ok", "network": "mainnet",
  "network_passphrase": "Public Global Stellar Network ; September 2015",
  "last_processed_ledger": 3550886, "chain_tip_ledger": 3550886,
  "lag_ledgers": 0, "seconds_since_cursor_update": 1,
  "events_ingested_total": 4895, "errors_total": 0 }
```
`network` is which Stellar network this deployment indexes (`mainnet` /
`testnet` / `futurenet` / `custom`), asked of the RPC itself and cached — so
clients (like the explorer) can adapt instead of asking the user. `null` while
the RPC is unreachable.

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

### `GET /contracts/:id/state`
Versioned snapshots of the contract's **instance storage** (admin, config,
counters…), newest first. Query: `limit` (1–200, default 1 = current state).
Requires the indexer's `STATE_INDEXING`; `404` if there are no snapshots.
```json
{ "contract_id": "CB...", "count": 1, "versions": [
  { "ledger": 3550880, "storage": { "TotalSupply": "1000", "IsPaused": false },
    "captured_at": "2026-07-15T..." }] }
```

### `GET /contracts/:id/data`
The current value of every **per-key** entry snapshotted for this contract —
e.g. each tracked holder's `Balance(Address)`. One row per key (its latest
snapshot). Query: `label` (e.g. `balance`), `limit` (1–1000, default 100).
Requires the indexer's `KEY_INDEXING`.
```json
{ "contract_id": "CB...", "count": 2, "keys": [
  { "key_hash": "9f2c…", "key": ["Balance", "G..."], "durability": "persistent",
    "ledger": 3550881, "value": "500", "label": "balance",
    "captured_at": "2026-07-15T..." }] }
```

### `GET /contracts/:id/data/:key_hash`
The version history of a single per-key entry (one holder's balance over time),
newest first. Query: `limit` (1–500).

## Read layer (authenticated)

Invoke contracts through RPC simulation. Arguments are type-checked against the
contract's on-chain spec *before* the network call, so mistakes come back as a
`400` with a precise message rather than an opaque simulation failure. Nothing
is ever signed or submitted.

### `GET /contracts/:id/functions`
The contract's callable functions with typed inputs/outputs.
```json
{ "contract_id": "CB...", "functions": [
  { "name": "balance", "inputs": [{ "name": "id", "type": "Address" }],
    "outputs": ["i128"] }] }
```

### `POST /contracts/:id/call`
Invoke a **view** function read-only and return a typed result.
Body: `{ "function": "balance", "args": { "id": "G..." }, "source_account": null }`
— `args` takes an object keyed by parameter name, or a positional array.
```json
{ "contract_id": "CB...", "function": "balance",
  "result": "500", "simulated_at_ledger": 3550886 }
```

### `POST /contracts/:id/simulate`
Dry-run **any** call, including state-changing ones, and get the typed result,
the events it would emit (decoded + enriched), and its estimated resource fee.
Same body as `/call`.
```json
{ "contract_id": "CB...", "function": "transfer", "result": null,
  "events": [
    { "contract_id": "CB...", "type": "contract", "event": "transfer",
      "topics": ["transfer", "G...", "G..."], "data": "500",
      "enriched": { "event": "transfer", "params": { "amount": { "type": "i128", "value": "500" } } } }],
  "min_resource_fee": "34561", "simulated_at_ledger": 3550886 }
```
`fn_call`/`fn_return` diagnostic noise is dropped; `enriched` is non-null only
for events emitted by the contract being simulated, since that's the only spec
in hand.

Errors are client-facing: a wrong-typed argument gives
`400 {"error": "argument \"id\": invalid address strkey"}`, an unknown function
`400 {"error": "contract has no function named \"nope\""}`, and a contract whose
interface isn't indexed (or a Stellar Asset Contract, which has no spec) gives
`404`. A contract trap is the caller's mistake, not a server fault, so it comes
back as `400 {"error": "simulation failed: ..."}`.

## GraphQL

### `POST /graphql`
Executes queries; `GET /graphql` serves the GraphiQL IDE in a browser. Behind
the same auth and rate limiting as the REST data routes.

REST stays the primary, zero-dependency interface; GraphQL is for clients that
want to select fields and page with cursors. High-volume lists (`events`,
`transfers`) are Relay-style cursor connections; naturally bounded ones
(`contracts`, `contractState`, `contractData`) are plain lists.

```graphql
query {
  events(contractId: "CB...", first: 20) {
    edges { cursor node { ledger eventName enriched } }
    pageInfo { hasNextPage endCursor }
  }
}
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

## Generated typed clients

### `GET /contracts/:id/sdk`
A ready-to-use, typed TypeScript client for the contract, generated on demand
from its on-chain interface — the codegen equivalent of everything above. Save
it and call the contract with full type safety and zero dependencies:

```bash
curl -o contract.ts "$BASE/contracts/CB.../sdk?lang=ts"
```
```ts
import { ContractClient } from "./contract";
const c = new ContractClient({ baseUrl: "https://lumenqraph.onrender.com" });
const pool = await c.get_pool_info(); // typed from the chain's own schema
```

Query: `lang` (`ts`, the default and only target so far; anything else is a
`400`) and `version` (generate from a historical interface version — the client
your integration was built against *before* an upgrade). Structs become
interfaces, unit enums become case-name literal types, unions become
`"Case" | { Case: [...] }` shapes — and because `/call` results are named with
the same spec, what a call returns is exactly what the next call accepts.
Generation is deterministic: same interface version, same file.

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
