<div align="center">

# Lumenqraph

**An open-source, self-hostable event indexer for Soroban smart contracts on Stellar.**

Tail contract events from Soroban RPC, decode their XDR to clean JSON, store them in Postgres, and serve them over a plain REST API and signed webhooks — *curl and get JSON*, no VM or custom program to deploy.

[![CI](https://github.com/Lumen-Scribe/Lumenqraph/actions/workflows/ci.yml/badge.svg)](https://github.com/Lumen-Scribe/Lumenqraph/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![Built for Stellar](https://img.shields.io/badge/built%20for-Stellar%20Soroban-black.svg)](https://stellar.org/soroban)

[Quick start](#quick-start) · [API](#api) · [Architecture](#architecture) · [Docs](docs/) · [Roadmap](#roadmap) · [Contributing](#contributing)

</div>

---

## Table of contents

- [Why Lumenqraph](#why-lumenqraph)
- [Features](#features)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API](#api)
- [Typed, self-describing decoding](#typed-self-describing-decoding)
- [Read layer — `eth_call` for Soroban](#read-layer--eth_call-for-soroban)
- [Contract state indexing](#contract-state-indexing)
- [AI-agent access — the MCP server](#ai-agent-access--the-mcp-server)
- [Webhooks](#webhooks)
- [Running in production](#running-in-production)
- [Project structure](#project-structure)
- [Development](#development)
- [Roadmap](#roadmap)
- [Known limitations](#known-limitations)
- [Contributing](#contributing)
- [License](#license)

## Why Lumenqraph

Soroban contracts emit events, but raw on-chain data is nearly unusable for building a frontend or dashboard directly — you'd have to replay the ledger, decode XDR, and reconstruct state yourself. An indexer sits between the chain and your application: it watches events, decodes them, stores them queryably, and serves them over an API so your dApp makes a normal HTTP call instead of talking to the chain.

Lumenqraph's angle is **simplicity, self-hostability, and typed decoding that needs zero configuration**:

- **Typed, self-describing events — automatically.** Soroban contracts publish their full interface (function, type, and event schemas) *on-chain*, embedded in the deployed WASM. Lumenqraph reads that schema and turns a raw event into a **named, typed record** (`{ from: Address, to: Address, amount: i128 }`) with no ABI upload and no manual mapping. This is a Soroban-native advantage — on EVM chains the equivalent ABI lives off-chain and has to be verified or uploaded by hand. See [Typed, self-describing decoding](#typed-self-describing-decoding).
- **Zero learning curve** — a plain REST API and JSON webhooks. No custom VM, no programs to write and deploy.
- **Self-hostable and inspectable** — run it on your own infrastructure; the code is open and auditable.
- **Decoded, not raw** — XDR is decoded to friendly JSON (addresses as `G…`/`C…` strkeys, amounts as decimal strings), with the raw base64 always retained losslessly.

## Features

| | |
| --- | --- |
| 🧩 **Full XDR → JSON decoding** | ScVal decoded to friendly JSON: `i128`/`u128` as decimal strings, addresses as `G…`/`C…` strkeys, bytes as hex, vecs/maps as arrays/objects. Raw base64 always retained. |
| 🏷️ **Typed, spec-driven decoding** | Reads each contract's on-chain `contractspecv0` interface to emit **named, typed** events (`{from, to, amount: i128}`) — zero ABI upload. Serves the full decoded interface at `/contracts/:id/interface`. |
| 📖 **Read layer (`eth_call` for Soroban)** | Invoke any contract view function read-only over REST and get a **typed** result. Arguments are type-checked against the on-chain spec before simulation. |
| 🤖 **MCP server (AI-agent access)** | A [Model Context Protocol](https://modelcontextprotocol.io) server that lets Claude (or any MCP agent) discover, query, and call any indexed Soroban contract — typed and self-describing, zero hand-written schema. |
| 🗂️ **Contract state indexing** | Optional versioned snapshots of each contract's instance storage (admin, config, TVL, counters…), decoded to JSON — historical *state*, not just events. |
| 💸 **Materialized token transfers** | SEP-41 `transfer` events projected into a queryable `from`/`to`/`amount` table. |
| 🔌 **REST API** | Contracts, events (filterable by name), transfers, health, and Prometheus metrics. |
| 📣 **Signed webhooks** | Register a URL + filter, receive HMAC-SHA256-signed event pushes with retries and exponential backoff. |
| 🔑 **Auth & rate limiting** | SHA-256-hashed API keys with per-key request limits. |
| 📊 **Observability** | Prometheus `/metrics` and a `/health` endpoint reporting chain-tip lag. |
| ⏪ **Backfill mode** | One-shot historical catch-up (bounded by RPC retention). |
| 🛡️ **Robust ingestion** | Idempotent, reorg-tolerant writes; graceful shutdown; automatic retry with backoff. |
| 🐳 **Production-ready** | Dockerized, CI-gated (fmt + clippy + tests), fully documented. |

## Architecture

Lumenqraph is a Rust workspace of three service binaries sharing one core library. The services coordinate only through Postgres, so each can scale, restart, and fail independently — API traffic can never stall ingestion, and a decode bug can't take down the read path.

```
  Soroban RPC ──poll getEvents──▶ ┌───────────┐ ──write──▶ ┌────────────┐ ◀──read── ┌─────────┐ ──REST──▶ dApps
                                  │  indexer  │            │  Postgres  │           │   api   │
                                  └───────────┘            └────────────┘           └─────────┘
                                                                 ▲
                                                          read   │  write
                                                            ┌────────────┐
                                                            │  webhooks  │ ──signed POST──▶ subscribers
                                                            └────────────┘
```

| Crate | Role |
| --- | --- |
| [`lumenqraph-core`](crates/lumenqraph-core) | Shared models, error types, the self-contained XDR→JSON / strkey decoder, the on-chain contract-spec parser + event enricher, and the read-layer call encoder. |
| [`lumenqraph-indexer`](crates/lumenqraph-indexer) | Always-on process: polls `getEvents`, decodes, enriches against each contract's interface, writes to Postgres. |
| [`lumenqraph-api`](crates/lumenqraph-api) | Axum read + management API (auth, rate limiting, metrics) and the read layer (typed view-function calls via RPC). |
| [`lumenqraph-webhooks`](crates/lumenqraph-webhooks) | Matches events to subscriptions and delivers signed pushes. |
| [`lumenqraph-mcp`](crates/lumenqraph-mcp) | Model Context Protocol server: typed, self-describing contract access for AI agents. |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Quick start

**Prerequisites:** [Rust](https://rustup.rs/) (stable) and [Docker](https://docs.docker.com/get-docker/).

```bash
git clone https://github.com/Lumen-Scribe/Lumenqraph.git
cd Lumenqraph
cp .env.example .env                 # defaults target Stellar testnet

docker compose up -d                 # local Postgres
cargo run -p lumenqraph-indexer      # polls RPC + auto-applies migrations
cargo run -p lumenqraph-api          # REST API on :8080 (separate shell)
cargo run -p lumenqraph-webhooks     # optional: webhook delivery (separate shell)
```

Query it:

```bash
curl localhost:8080/health
curl localhost:8080/contracts
curl 'localhost:8080/contracts/<CONTRACT_ID>/events?event_name=transfer&limit=5'
curl 'localhost:8080/contracts/<CONTRACT_ID>/transfers?limit=5'
curl localhost:8080/metrics
```

To run the entire stack (Postgres + all three services) in Docker:

```bash
docker compose -f docker-compose.full.yml up --build
```

Common tasks are wrapped in the [`Makefile`](Makefile) — run `make help`.

## Configuration

All configuration is via environment variables (see [`.env.example`](.env.example)).

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph` | Postgres connection string. |
| `RPC_URL` | `https://soroban-testnet.stellar.org` | Soroban RPC endpoint. Used by the indexer (polling) and the API (read-layer simulation). |
| `CONTRACT_IDS` | *(empty)* | Comma-separated contract IDs to index. Empty = **all** contract events. |
| `POLL_INTERVAL_SECS` | `5` | How often the indexer polls for new events. |
| `PAGE_SIZE` | `1000` | Events requested per `getEvents` page (1–10000). |
| `START_LEDGER` | `0` | Ledger to start a fresh index from. `0` = near the tip. Clamped to RPC retention. |
| `STATE_INDEXING` | `false` | Snapshot contract instance storage into `contract_state` (versioned). One extra RPC call per tracked contract per cycle; best paired with `CONTRACT_IDS`. |
| `API_BIND_ADDR` | `0.0.0.0:8080` | API listen address. |
| `REQUIRE_API_KEY` | `false` | Require a valid API key on data routes. |
| `ANON_RATE_LIMIT_PER_MIN` | `60` | Requests/min for unauthenticated callers. |
| `WEBHOOK_TICK_SECS` | `3` | Webhook dispatcher poll interval. |
| `WEBHOOK_BATCH_SIZE` | `100` | Deliveries processed per tick. |
| `WEBHOOK_MAX_ATTEMPTS` | `6` | Delivery attempts before a webhook is marked failed. |
| `RUST_LOG` | `info` | Log filter (`tracing` syntax). |

## API

Base URL defaults to `http://localhost:8080`. Full reference: [docs/API.md](docs/API.md).

**Authentication.** Data routes accept an API key via `Authorization: Bearer <key>` or `x-api-key: <key>`. When `REQUIRE_API_KEY=false` (default), unauthenticated callers are allowed up to `ANON_RATE_LIMIT_PER_MIN`. `/health` and `/metrics` are always public. Rate-limit breaches return `429`; invalid or revoked keys return `401`.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Indexing status and chain-tip lag. *(public)* |
| `GET` | `/metrics` | Prometheus metrics. *(public)* |
| `GET` | `/contracts` | Contracts seen, with per-contract counts. |
| `GET` | `/contracts/:id/interface` | The contract's decoded on-chain interface: functions, events, and user-defined types. |
| `GET` | `/contracts/:id/state` | Versioned snapshots of the contract's instance storage, newest first. Query: `limit` (1 = current state). |
| `GET` | `/contracts/:id/functions` | The contract's callable functions with typed inputs/outputs. |
| `POST` | `/contracts/:id/call` | Invoke a view function read-only and return a typed result. Body: `{ function, args, source_account? }`. |
| `GET` | `/contracts/:id/events` | Events for a contract. Query: `limit`, `offset`, `event_name`. |
| `GET` | `/contracts/:id/transfers` | Materialized token transfers. Query: `limit`, `offset`, `from`, `to`. |
| `POST` | `/webhooks` | Create a subscription. |
| `GET` | `/webhooks` | List subscriptions (secrets omitted). |
| `DELETE` | `/webhooks/:id` | Delete a subscription. |

<details>
<summary><b>Example: a decoded transfer event</b></summary>

```jsonc
// GET /contracts/<CID>/events?event_name=transfer&limit=1
{
  "event_id": "0015250934946869248-0000000000",
  "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
  "ledger": 3550885,
  "event_name": "transfer",
  "decoded_topics": ["transfer", "GAIH3ULL...ZNSR", "GDN4OHYR...YQZ3", "native"],
  "decoded_value": "100000000000",
  // Named + typed, from the contract's on-chain spec (null if the contract
  // publishes no matching event schema):
  "enriched": {
    "event": "transfer",
    "params": {
      "from":   { "type": "Address", "value": "GAIH3ULL...ZNSR" },
      "to":     { "type": "Address", "value": "GDN4OHYR...YQZ3" },
      "amount": { "type": "i128",    "value": "100000000000" }
    }
  },
  "topics": ["AAAADwAA...", "..."],   // raw base64 XDR, always retained
  "value": "AAAACv//...",
  "tx_hash": "3664562a...",
  "in_successful_call": true
}
```
</details>

Generate an API key:

```bash
DATABASE_URL=... ./scripts/gen_api_key.sh myapp pro 600   # name, tier, requests/min
```

## Typed, self-describing decoding

Every generic decoder can tell you a value is an `i128` or an address. Lumenqraph goes further: it reads each contract's **on-chain interface** — the `contractspecv0` schema that Soroban embeds directly in the deployed WASM — and uses it to attach real **field names and types** to every event, automatically.

The first time the indexer sees a contract, it fetches the contract's WASM (two `getLedgerEntries` hops: instance → WASM hash → code), parses the interface once, caches it, and persists it. Every later event from that contract is enriched into a named record and stored in the `enriched` column. If a contract publishes no matching schema, `enriched` is simply `null` and the always-present `decoded_*` fields remain — nothing is ever lost.

Inspect any deployed contract's interface straight from the CLI (no database required):

```bash
cargo run -p lumenqraph-indexer -- inspect <CONTRACT_ID>
```

```jsonc
// GET /contracts/<CID>/interface  (or the `inspect` command above)
{
  "contract_id": "CB...",
  "has_events": true,
  "interface": {
    "events": [
      { "name": "transfer", "data_format": "single", "params": [
        { "name": "from",   "type": "Address", "location": "topic" },
        { "name": "to",     "type": "Address", "location": "topic" },
        { "name": "amount", "type": "i128",    "location": "data"  }
      ] }
    ],
    "functions": [ { "name": "transfer", "inputs": [ /* … */ ], "outputs": [] } ],
    "structs": [], "unions": [], "enums": []
  }
}
```

**Why this is a Soroban advantage.** On EVM chains, the ABI that names an event's fields lives *off-chain* — an indexer only produces human-readable data if someone verifies the contract or uploads its ABI. Soroban ships that schema *with the code*, so Lumenqraph delivers typed, self-describing data for any contract with **zero configuration**. Implementation: [`lumenqraph-core::spec`](crates/lumenqraph-core/src/spec.rs).

## Read layer — `eth_call` for Soroban

History tells you what *happened*; the read layer tells you the current *state*. `GET /contracts/:id/events` serves indexed events; `POST /contracts/:id/call` invokes a contract's **view functions** read-only — the Soroban equivalent of EVM's `eth_call`, a primitive no other Stellar indexer exposes as a service.

Under the hood it uses Soroban RPC's `simulateTransaction`. The friction that usually makes this hard — hand-building a transaction envelope and encoding/decoding XDR — is gone: Lumenqraph reads the arguments straight from JSON, **type-checks and encodes them against the contract's on-chain spec** (so a bad argument is rejected *before* the network call), simulates, and decodes the result into typed JSON.

```bash
# Discover what you can call:
curl localhost:8080/contracts/<CID>/functions
# → [{ "name": "balance", "inputs": [{ "name": "account", "type": "Address" }], "outputs": ["i128"] }, …]

# Call a view function (args by name or as a positional array):
curl -X POST localhost:8080/contracts/<CID>/call \
  -H 'Content-Type: application/json' \
  -d '{"function":"balance","args":{"account":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"}}'
```

```jsonc
{
  "contract_id": "CDTLXP6K…HIZA",
  "function": "balance",
  "result": { "type": "i128", "value": "0" },
  "simulated_at_ledger": 3585685
}
```

Errors are precise and client-facing: unknown function, missing/extra argument, or a wrong-typed argument all return `400` with a message (`argument "account": invalid address strkey`) rather than a failed simulation. Reads need the contract's interface, which the indexer captures on first sighting — Stellar Asset Contracts (no WASM spec) aren't callable this way. Supported argument types today: bool, all sized integers, `i128`/`u128`, `Symbol`, `String`, `Address`, `Bytes`/`BytesN`, `Option`, `Vec`, `Tuple`, and symbol-keyed `Map`; big-int (256-bit) and user-defined-type arguments are on the roadmap. Implementation: [`lumenqraph-core::read`](crates/lumenqraph-core/src/read.rs).

## Contract state indexing

Events are *history* (what happened); the read layer is current state *on demand* (one call, right now). Contract state indexing is the third leg: **versioned snapshots of a contract's on-chain state**, stored so you can query current state cheaply and see how it changed over time — something even `eth_call` can't do without an archive node.

Enable it with `STATE_INDEXING=true` (ideally alongside `CONTRACT_IDS`). Each cycle the indexer reads a tracked contract's **instance storage** — the enumerable key→value map Soroban keeps in the contract's instance entry (admin, config, global counters, TVL, …) — decodes it to JSON (the same shape as events), and writes a new `contract_state` row **only when the instance actually changed** (detected via the entry's `lastModifiedLedgerSeq`). The newest row is current state; older rows are history.

```jsonc
// GET /contracts/<CID>/state          → current state (limit=1, the default)
// GET /contracts/<CID>/state?limit=50 → the last 50 versions, newest first
{
  "contract_id": "CDTLXP6K…HIZA",
  "count": 1,
  "versions": [
    {
      "ledger": 3584379,
      "captured_at": "2026-07-13T13:11:59Z",
      "storage": [
        { "key": "METADATA", "val": { "name": "Stellars LP", "symbol": "sLP", "decimals": 13 } },
        { "key": ["TotalSupply"],  "val": "1624887782285314838" },
        { "key": ["ReservedUsdc"], "val": "768829008937" },
        { "key": ["IsPaused"],     "val": false }
        // …
      ]
    }
  ]
}
```

Reading the instance also reveals the contract's current WASM hash, so an **upgraded contract is detected automatically** — its cached interface is evicted and re-parsed on the next event, keeping enrichment and the read/MCP layers correct across upgrades. Instance storage is what's universally enumerable via RPC; per-key persistent storage (e.g. individual balances) is on the roadmap. Implementation: [`lumenqraph-indexer::state`](crates/lumenqraph-indexer/src/state.rs).

## AI-agent access — the MCP server

Everything above — typed events, decoded interfaces, typed read calls — is exactly the structured, self-describing metadata an AI agent needs to work with a chain. [`lumenqraph-mcp`](crates/lumenqraph-mcp) exposes it as a [Model Context Protocol](https://modelcontextprotocol.io) server, so **Claude (Desktop or Code) or any MCP client can discover, query, and call any Soroban contract** — with no hand-written tool schemas, because the schemas come from each contract's on-chain interface.

It's a standard stdio JSON-RPC server that reuses the same Postgres and read-layer encoder as the API, and offers four tools:

| Tool | What the agent can do |
| --- | --- |
| `list_contracts` | See which contracts are indexed, with event counts. |
| `get_contract_interface` | Discover a contract's functions (typed inputs/outputs), events, and user-defined types. |
| `get_contract_state` | Read a contract's current (and historical) instance storage. |
| `query_events` | Read a contract's recent events, decoded and enriched. |
| `call_contract` | Invoke a view function read-only and get a typed result (args type-checked against the spec). |

### Quick start

**1. Build the server binary.**

```bash
cargo build --release -p lumenqraph-mcp
# → target/release/lumenqraph-mcp   (install it on your PATH, or use the full path below)
```

**2. Make sure the indexer has populated the database.** The MCP server is a *read* surface over the same Postgres the indexer writes — it needs data (and, for `call_contract`, the contract's interface, which the indexer captures on first sighting). If you followed the [Quick start](#quick-start) above, you already have this.

**3. Smoke-test it without an MCP client** by piping newline-delimited JSON-RPC to its stdin (protocol replies come back on stdout, one per line):

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_contracts","arguments":{}}}' \
| DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph' \
  RPC_URL='https://soroban-testnet.stellar.org' \
  ./target/release/lumenqraph-mcp
# → an initialize result, the four tool definitions, then your indexed contracts.
```

**4. Register it with your MCP client** (e.g. Claude Desktop's `claude_desktop_config.json`, or Claude Code via `claude mcp add`):

```jsonc
{
  "mcpServers": {
    "lumenqraph": {
      "command": "lumenqraph-mcp",   // or the absolute path to the built binary
      "env": {
        "DATABASE_URL": "postgres://…",   // the same DB the indexer writes
        "RPC_URL": "https://soroban-testnet.stellar.org"
      }
    }
  }
}
```

**5. Restart the client and just ask** — in plain language:

> *"Using lumenqraph, what functions does contract `C…` expose? What's the balance of `G…`? Show me its last few `transfer` events."*

The agent calls `get_contract_interface` to discover the typed signatures, then `call_contract` and `query_events` to answer — all typed, all validated against the on-chain spec, with no schema you had to write.

## Webhooks

Register a URL (with optional contract/event filters) and receive event pushes as they're indexed. Each delivery carries an `X-Lumenqraph-Signature: sha256=<hmac>` header — an HMAC-SHA256 of the raw request body, keyed by the `secret` returned once at creation.

```bash
curl -X POST localhost:8080/webhooks \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/hook","event_name":"transfer"}'
```

Verifying a delivery (Node.js):

```js
const crypto = require("crypto");

function verify(rawBody, signatureHeader, secret) {
  const expected =
    "sha256=" + crypto.createHmac("sha256", secret).update(rawBody).digest("hex");
  return crypto.timingSafeEqual(
    Buffer.from(signatureHeader),
    Buffer.from(expected)
  );
}
```

Deliveries retry with exponential backoff up to `WEBHOOK_MAX_ATTEMPTS`.

## Running in production

Run three long-lived processes against one Postgres. Only the indexer applies migrations.

| Process | Notes |
| --- | --- |
| `lumenqraph-indexer` | Must run **24/7** — a sleeping poller falls behind the chain. |
| `lumenqraph-api` | Stateless; scale horizontally behind a load balancer. |
| `lumenqraph-webhooks` | A single instance suffices; delivery is idempotent per (subscription, event). |

Scrape `GET /metrics` and alert on `lumenqraph_indexer_lag_ledgers` climbing. For managed Postgres, point `DATABASE_URL` at Neon or Supabase. See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for scaling notes (RPC providers, Redis-backed rate limiting, caching).

## Project structure

```
Lumenqraph/
├── crates/
│   ├── lumenqraph-core/       # shared models, errors, XDR↔JSON, spec parser, read encoder
│   ├── lumenqraph-indexer/    # polling, decoding, spec enrichment, state snapshots, backfill
│   ├── lumenqraph-api/        # Axum REST API, auth, rate limiting, metrics, read layer
│   ├── lumenqraph-webhooks/   # subscription matching + signed delivery
│   └── lumenqraph-mcp/        # Model Context Protocol server for AI agents
├── migrations/                # ordered sqlx SQL migrations (0001–0005)
├── docs/                      # ARCHITECTURE, API, DEPLOYMENT
├── explorer/                  # minimal zero-build read UI
├── scripts/                   # gen_api_key, backfill, setup_db
├── Dockerfile                 # multi-stage build (all four binaries)
├── docker-compose.yml         # local Postgres for dev
├── docker-compose.full.yml    # full stack
└── Makefile                   # common tasks (make help)
```

## Development

```bash
make db          # start local Postgres
make build       # cargo build --workspace
make test        # cargo test --workspace
make fmt         # cargo fmt --all
make lint        # cargo clippy -- -D warnings
```

CI runs formatting, Clippy (warnings denied), tests, and a release build against a Postgres service on every push and pull request. Please run `make fmt lint test` before opening a PR.

## Roadmap

- [x] Typed, self-describing decoding from each contract's on-chain interface (`contractspecv0`)
- [x] Read layer: typed, read-only view-function calls via `simulateTransaction` (`eth_call` for Soroban)
- [x] MCP server: typed, self-describing Soroban access for AI agents (Model Context Protocol)
- [x] Contract state indexing: versioned instance-storage snapshots + automatic spec refresh on upgrade
- [ ] State indexing: per-key persistent storage (e.g. individual balances), not just instance storage
- [ ] Read layer: user-defined-type and 256-bit-integer arguments; in-memory spec cache in the API
- [ ] Enrichment for user-defined struct/enum/union values (naming nested UDT fields, not just event params)
- [ ] Deep historical backfill via a captive-core / data-lake source (beyond RPC's ~7-day window)
- [ ] Additional materialized verticals (AMM swaps, liquidity, NFT mints/transfers)
- [ ] GraphQL endpoint alongside REST; cursor-based pagination
- [ ] Redis-backed rate limiting and read caching for multi-instance deployments
- [ ] Client SDKs (TypeScript, Python)
- [ ] Grafana dashboards and alert rules

Contributions toward any of these are very welcome — see [Contributing](#contributing).

## Known limitations

- **History is bounded by RPC retention (~7 days).** Deep backfill needs a captive-core / data-lake source (on the roadmap); `START_LEDGER` is clamped to the retention window.
- **The rate limiter is per-instance** (in-memory). Running multiple API replicas means limits apply per replica — move the limiter to Redis for a global limit.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the dev setup and conventions. Good first issues are labelled in the [issue tracker](https://github.com/Lumen-Scribe/Lumenqraph/issues).

## License

Licensed under the [MIT License](LICENSE).

---

<div align="center">
Built for the <a href="https://stellar.org/soroban">Stellar / Soroban</a> ecosystem.
</div>
