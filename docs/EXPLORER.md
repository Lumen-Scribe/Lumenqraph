# Explorer UI and Configuration

The Lumenqraph Explorer is a lightweight, zero-build, responsive Single Page Application (SPA) served directly by the API. It provides a visual dashboard for self-hosters to monitor ingestion health, browse indexed contracts, analyze state/transfers, review interface upgrades, and simulate read-only contract calls.

---

## Features

The explorer page (`explorer/index.html`) is structured into a real-time KPI monitoring header, an indexed contract sidebar, and a detailed contract tab panel.

### 1. Ingestion Health KPIs
At the top of the dashboard, the UI displays real-time statistics fetched from the API's `/health` endpoint:
- **Indexer Status**: A visual dot indicators showing the health of the tailing process (Green/Healthy if caught up, Yellow/Warning if lagging, Red/Critical if far behind or unreachable).
- **Lag (ledgers)**: The number of ledgers behind the chain tip.
- **Processed Ledger**: The height of the last indexed ledger.
- **Chain Tip**: The current height of the Stellar network.
- **Events Indexed**: Total events persisted in the database.
- **Errors**: Cumulative error count recorded during ingestion.

### 2. Contract Detail Tabs
When a contract is selected, the explorer fetches data from the API's endpoints to populate six dynamic tabs:

- **Events**: Lists the latest decoded events with columns for Ledger, Event Name/Type, Decoded Value, and transaction links to Stellar.Expert.
- **Transfers**: Projects SEP-41 token transfer operations (`from`, `to`, `amount`).
- **State**: Displays the raw JSON snapshot of the contract's instance storage (`GET /contracts/:id/state`).
- **Holders**: Displays per-key storage snapshots (e.g., token balances) with key hash mappings (`GET /contracts/:id/data`).
- **Interface**: Parses and renders the contract's callable functions, inputs, outputs, and custom data structures directly from the on-chain WASM metadata.
- **Upgrades**: Renders a chronological timeline of contract upgrades, comparing WASM hashes and outputting a semantic diff of added, modified, or deleted functions/events.

### 3. Read Simulator & View Call
The Interface tab includes an interactive simulation form. Users can specify view function parameters, and the UI will dispatch a request to `/contracts/:id/call` or `/contracts/:id/simulate` to run the execution using `simulateTransaction` on the Soroban RPC. It renders the typed result directly.

---

## Configuration and Serving

The explorer is a single, self-contained HTML file located in [`explorer/index.html`](../explorer/index.html). It can be served in two ways:

### 1. Same-Origin Serving (Recommended)
By default, the `lumenqraph-api` serves the explorer directory at its root fallback URL.
- **`EXPLORER_DIR`**: Set this environment variable to the path containing `index.html` (e.g., `EXPLORER_DIR="/app/explorer"`).
- **CORS-Free**: Running the UI same-origin eliminates the need to configure CORS or manage cross-origin credentials.
- **Revalidation Cache Headers**: The API serves these assets with a `Cache-Control: no-cache` header. This forces browsers to perform a lightweight conditional request (checking `Last-Modified` tags). If the assets haven't changed, the server answers with a fast `304 Not Modified`, but new deployments are loaded immediately instead of waiting for long browser cache expirations.

### 2. Remote Static Hosting
You can host `explorer/index.html` on any static provider (GitHub Pages, Vercel, S3, Netlify, or local filesystem).
- In the top right header of the UI, fill in the **API base** field with your remote Lumenqraph API URL (e.g. `https://api.lumenqraph.dev`).
- Input your optional `x-api-key` in the API Key field if your endpoint has authentication enabled (`REQUIRE_API_KEY=true`).
- The API base URL is remembered in `localStorage`. The API key is **not** persisted by default: it is kept only for the current tab and is cleared when the tab closes.
- To keep the key across browser restarts, tick **Remember on this device**. Use **Forget key** to remove it from the page and from the browser.
- A key saved by an older version of the explorer in `localStorage` is removed on the next load; re-enter it and tick **Remember on this device** if you want it kept.
- Use a **read-only-scoped** key in the explorer where possible. API keys can carry write access (for example, to webhooks), and anyone using a shared machine can read a remembered key.

---

## Network Discovery & Sibling Instances

A single Lumenqraph instance indexes exactly one network (Stellar Mainnet or Testnet). However, you can front multiple instances using a single origin to allow seamless switching.

### Sibling Proxying via `INSTANCE_MOUNTS`
To index multiple networks on the same domain, deploy separate instances for each network and configure the primary instance to mount the other as a sibling proxy:
```env
INSTANCE_MOUNTS="testnet=http://127.0.0.1:8081"
```
The primary API will capture all incoming requests under `/testnet/*` and proxy them directly to the testnet instance (stripping the prefix).

### Auto-Discovery and One-Click Switching
1. **Advertisement**: Sibling mounts are declared in the `/health` endpoint output under the `mounts` field.
2. **Discovery**: The explorer UI loads the `/health` endpoint of the primary base URL. It discovers the declared mounts (`{ "testnet": "/testnet" }`).
3. **Switching**: The Network dropdown in the header lists "mainnet" and "testnet". 
   - When the user selects "testnet", the UI automatically updates its API Base to the mounted subpath (e.g. `/testnet`), updates its deep links pointing to Stellar.Expert, and queries the testnet database.
   - The UI remembers the API base URLs for each network in `localStorage` (`lq.base.mainnet`, `lq.base.testnet`) to ensure seamless switching back and forth.
