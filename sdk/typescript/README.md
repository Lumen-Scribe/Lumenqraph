# @lumenqraph/sdk

Typed TypeScript client for the [Lumenqraph](https://github.com/Lumen-Scribe/Lumenqraph)
Soroban event-indexer API — REST and GraphQL, zero runtime dependencies (uses
the platform `fetch`; Node 18+ or the browser).

## Install

```bash
npm install @lumenqraph/sdk
```

## Quick start

```ts
import { LumenqraphClient } from "@lumenqraph/sdk";

const lq = new LumenqraphClient({
  baseUrl: "http://localhost:8080",
  apiKey: process.env.LUMENQRAPH_API_KEY, // only if REQUIRE_API_KEY is on
});

// Which contracts are indexed?
const contracts = await lq.listContracts();

// Recent decoded + typed events (REST, limit/offset).
const events = await lq.listEvents(contracts[0].contract_id, { limit: 20 });

// Current on-chain state (instance storage) and per-holder balances.
const state = await lq.getState(contracts[0].contract_id);
const balances = await lq.getData(contracts[0].contract_id, { label: "balance" });

// A read-only view call, type-checked against the on-chain spec.
const dec = await lq.call(contracts[0].contract_id, { function: "decimals" });
console.log(dec.result);

// Dry-run any call and preview its result, events, and cost.
const preview = await lq.simulate(contracts[0].contract_id, {
  function: "transfer",
  args: { from: "G...", to: "G...", amount: "100" },
});
```

## Cursor pagination

The GraphQL endpoint exposes Relay-style cursor connections. The SDK wraps them
as an async iterator that fetches page after page for you:

```ts
for await (const ev of lq.paginateEvents(contractId, { pageSize: 100 })) {
  console.log(ev.ledger, ev.event_name, ev.enriched ?? ev.decoded_value);
}
```

Or drive it a page at a time:

```ts
let page = await lq.eventsPage(contractId, { first: 50 });
while (page.hasNextPage) {
  page = await lq.eventsPage(contractId, { first: 50, after: page.endCursor! });
}
```

## Raw GraphQL

```ts
const data = await lq.graphql<{ transfers: { edges: { node: unknown }[] } }>(`
  query($id: String!) {
    transfers(contractId: $id, first: 10) {
      edges { node { fromAddr toAddr amount ledger } }
      pageInfo { hasNextPage endCursor }
    }
  }`,
  { id: contractId },
);
```

## API surface

| Method | Endpoint |
| --- | --- |
| `health()` | `GET /health` |
| `listContracts()` | `GET /contracts` |
| `getInterface(id)` | `GET /contracts/:id/interface` |
| `getState(id, { limit })` | `GET /contracts/:id/state` |
| `getData(id, { label, limit })` | `GET /contracts/:id/data` |
| `getDataKey(id, keyHash, { limit })` | `GET /contracts/:id/data/:keyHash` |
| `listEvents(id, { limit, offset, eventName })` | `GET /contracts/:id/events` |
| `listTransfers(id?, { limit, offset })` | `GET /contracts/:id/transfers` |
| `listFunctions(id)` | `GET /contracts/:id/functions` |
| `call(id, { function, args, sourceAccount })` | `POST /contracts/:id/call` |
| `simulate(id, { function, args, sourceAccount })` | `POST /contracts/:id/simulate` |
| `graphql(query, variables)` | `POST /graphql` |
| `eventsPage` / `paginateEvents` | `POST /graphql` (cursor) |

Errors for non-2xx responses are thrown as `LumenqraphError` (`.status`, `.body`).

## Build from source

```bash
cd sdk/typescript
npm install
npm run build   # emits dist/ (ESM + .d.ts)
```

## License

MIT
