# Lumenqraph MCP Server

**[Model Context Protocol](https://modelcontextprotocol.io)** (MCP) is a standard that lets AI agents like Claude discover and call tools. The Lumenqraph MCP server gives any MCP client **typed, self-describing access to Soroban contracts** — without hand-written schemas, because the types come from each contract's on-chain interface.

## Quick Start

### 1. Build the MCP server

```bash
# From the repo root
cargo build --release -p lumenqraph-mcp
```

The binary is at `./target/release/lumenqraph-mcp`.

### 2. Prepare the database

The MCP server is a **read-only** surface over the same Postgres that the indexer writes to. Make sure:
- The indexer has populated the database with contracts and events.
- You have a valid `DATABASE_URL` (e.g., `postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph`).

If you're just trying this out, run the indexer for a few minutes so it has some contracts to query.

### 3. Test it locally (no MCP client needed)

Pipe newline-delimited JSON-RPC messages to stdin and read responses from stdout:

#### stdio

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_contracts","arguments":{}}}' \
| DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph' \
  RPC_URL='https://soroban-testnet.stellar.org' \
  ./target/release/lumenqraph-mcp
```

You should see three responses: an `initialize` result, the tool list, and your indexed contracts.

#### streamable HTTP

```bash
MCP_AUTH_TOKEN=secret \
DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph' \
RPC_URL='https://soroban-testnet.stellar.org' \
MCP_HTTP_BIND='127.0.0.1:8090' \
./target/release/lumenqraph-mcp
```

Then connect to `http://127.0.0.1:8090/mcp` with an `Authorization: Bearer secret` header and JSON-RPC requests.

For remote (non-localhost) deployments, add `MCP_ALLOWED_ORIGINS` so only your front-end's origin is accepted:

## Transport & Environment

### Transport

The server supports two transports:

- **stdio**: newline-delimited JSON-RPC 2.0 over stdin/stdout for local agents and desktop clients.
- **streamable HTTP**: JSON-RPC over HTTPS for shared, remote deployments. The server listens on a bind address such as `0.0.0.0:8090` and exposes the MCP endpoint at `/mcp`.

For stdio mode, log messages go to stderr so stdout stays a clean protocol channel. For HTTP mode, send `Authorization: Bearer <token>` when `MCP_AUTH_TOKEN` is configured and use `Mcp-Session-Id` for session continuity.

### HTTP transport security

The Streamable HTTP transport is hardened against the DNS-rebinding and open-proxy attacks described in the [MCP spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports):

1. **Authentication is required by default.** The HTTP transport refuses to start when `MCP_HTTP_BIND` is set and `MCP_AUTH_TOKEN` is unset, because anyone who can reach the port can call `call_contract` / `simulate_call` (spending upstream RPC quota) and read the whole index. Set `MCP_AUTH_TOKEN`, or explicitly opt into anonymous access with `MCP_HTTP_ALLOW_ANONYMOUS=true` if you accept that risk (e.g. behind an authenticating reverse proxy).

2. **Binds to `127.0.0.1` by default.** A bare port (`MCP_HTTP_BIND=:8090` or `MCP_HTTP_BIND=8090`) is rewritten to `127.0.0.1:8090` so a misconfigured bind cannot expose the server to the network. Pass an explicit `0.0.0.0:8090` only if you intend to expose it and have set `MCP_AUTH_TOKEN` plus a network firewall.

3. **Origin header validation (DNS-rebinding protection).** Browsers send an `Origin` header on cross-origin requests; the server rejects any `Origin` that is not in `MCP_ALLOWED_ORIGINS` with HTTP 403 before the request reaches the JSON-RPC handler. Requests with no `Origin` header (CLI clients like `curl` and most MCP clients) are always allowed. Set `MCP_ALLOWED_ORIGINS=https://app.example.com,https://staging.example.com` (comma-separated). If unset, every browser Origin is rejected. `MCP_ALLOW_ANY_ORIGIN=true` disables the check entirely — only do this behind an authenticating reverse proxy that strips Origin.

4. **Session management.** A successful `initialize` issues an `Mcp-Session-Id` response header (a UUID). All subsequent requests from the client MUST include that `Mcp-Session-Id` header; the server rejects requests without it with `400 Bad Request` and requests with an unknown/expired id with `404 Not Found`. `DELETE /mcp` with the session id tears down the session.

5. **`MCP-Protocol-Version` header.** After `initialize`, requests should carry the negotiated `MCP-Protocol-Version` header. An unsupported version is rejected with `400 Bad Request`.

6. **`GET /mcp` returns `405 Method Not Allowed`.** This server does not implement a server-initiated SSE stream, so per the spec it returns 405 (with an `Allow: POST, DELETE` header) rather than opening a stream. Clients use POST for all JSON-RPC messages.

### Remote deployment model

For a shared, remote deployment (e.g. a team sharing one indexer's MCP endpoint):

```bash
MCP_AUTH_TOKEN=$(openssl rand -hex 32) \
MCP_ALLOWED_ORIGINS=https://your-app.example.com \
MCP_HTTP_BIND=0.0.0.0:8090 \
DATABASE_URL='postgres://...' RPC_URL='https://...' \
./target/release/lumenqraph-mcp
```

Put it behind TLS (a reverse proxy or a load balancer) — the transport itself is plain HTTP. The reverse proxy should forward `Authorization`, `Origin`, `Mcp-Session-Id`, and `MCP-Protocol-Version` headers unchanged. If the proxy authenticates callers itself and you want to skip Origin validation, set `MCP_ALLOW_ANY_ORIGIN=true` (do NOT unset `MCP_AUTH_TOKEN` — the proxy should inject it).

### Environment Variables

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `DATABASE_URL` | ✅ | — | Postgres connection string, e.g. `postgres://user:pass@host:5432/db` |
| `RPC_URL` | ❌ | Detected from the indexed network | Stellar RPC endpoint for `call_contract` and `simulate_call`; overrides automatic detection when set |
| `RPC_TIMEOUT_SECS` | ❌ | `30` | Timeout for RPC calls in seconds |
| `MCP_AUTH_TOKEN` | ❌ (stdio) / ✅ (HTTP) | — | Bearer token required in `Authorization` field during `initialize`. **Required to start the HTTP transport** unless `MCP_HTTP_ALLOW_ANONYMOUS=true`. |
| `MCP_HTTP_BIND` | ❌ | unset (stdio) | Address to bind the HTTP transport. Bare ports (`:8090` or `8090`) default to `127.0.0.1`. Use `0.0.0.0:8090` to expose on all interfaces (requires `MCP_AUTH_TOKEN`). |
| `MCP_HTTP_ALLOW_ANONYMOUS` | ❌ | `false` | Explicit opt-in to start the HTTP transport without `MCP_AUTH_TOKEN`. Use only behind an authenticating reverse proxy. |
| `MCP_ALLOWED_ORIGINS` | ❌ | unset (deny all browser Origins) | Comma-separated allowlist of `Origin` headers accepted by the HTTP transport (DNS-rebinding protection). Requests with no `Origin` header are always allowed. |
| `MCP_ALLOW_ANY_ORIGIN` | ❌ | `false` | Disable Origin validation entirely. Only safe behind an authenticating reverse proxy that strips `Origin`. |
| `MCP_RPC_RATE_LIMIT_PER_MIN` | ❌ | `60` | Requests/min allowed for MCP tools that call the RPC |

Example `.env` file for local testing:

```bash
DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph
RPC_URL=https://soroban-testnet.stellar.org
RPC_TIMEOUT_SECS=30
MCP_AUTH_TOKEN=your-secret-token
# Optional: set a bind address for the HTTP transport (default: stdio)
# MCP_HTTP_BIND=127.0.0.1:8090
# Optional: restrict which browser Origins may call the HTTP transport
# MCP_ALLOWED_ORIGINS=https://your-app.example.com
```

When `RPC_URL` is unset or blank, the MCP server reads
`indexer_cursor.network_passphrase` and selects the matching public RPC endpoint
for Stellar mainnet, testnet, or futurenet. The indexer must have stored its
network passphrase before the MCP server starts. For custom networks or an
uninitialized indexer, set `RPC_URL` explicitly; startup fails rather than
silently sending RPC calls to the wrong network.

## Tools

The MCP server exposes 17 tools for agents to discover and query Soroban contracts. The definitions live in `crates/lumenqraph-mcp/src/tools.rs` (`definitions()`); a client can list them with `tools/list`.

| Tool | Purpose |
| --- | --- |
| `list_contracts` | List all indexed contracts with event counts and ledger ranges |
| `get_contract_interface` | Get a contract's decoded on-chain interface (functions, events, types) |
| `list_contract_functions` | List a contract's callable functions with typed inputs and outputs, without the full interface payload |
| `get_contract_upgrades` | View a contract's interface history with semantic diffs and breaking-change detection |
| `diff_contract_interface` | Compute what changed between any two versions of a contract's interface |
| `get_contract_state` | Read a contract's current (and historical) instance storage |
| `get_contract_data` | Read a contract's per-key state — individual entries like token balances |
| `get_contract_stats` | Summarize a contract's event activity by event name |
| `query_events` | Query recent indexed events for a contract, newest first |
| `get_event` | Fetch a single indexed event by its event id |
| `get_transaction_events` | List every indexed event of a transaction in emission order |
| `query_transfers` | Query materialized token transfers for a contract, newest first |
| `query_swaps` | Query materialized AMM swap events for a contract, newest first |
| `query_nft_events` | Query materialized NFT mint/transfer/burn events for a contract, newest first |
| `query_liquidity_events` | Query materialized liquidity add/remove events for a contract, newest first |
| `call_contract` | Invoke a contract's view function read-only via RPC simulation, with type-checking |
| `simulate_call` | Dry-run ANY contract call (including state-changing ones) without submitting it |

### Tool parameters

Every tool that takes a `contract_id` expects a contract id (`C...`). `limit` values outside the stated range are clamped to it. Parameters marked optional can be omitted.

| Tool | Parameters | Returns |
| --- | --- | --- |
| `list_contracts` | none | Indexed contracts with event counts and ledger ranges |
| `get_contract_interface` | `contract_id` | The decoded interface |
| `list_contract_functions` | `contract_id` | `contract_id`, `count`, `functions` |
| `get_contract_upgrades` | `contract_id`; `limit` (optional, 1-200, default 20) | Interface versions, newest first, with diffs |
| `diff_contract_interface` | `contract_id`; `from` and `to` (both required, interface version numbers >= 1, must differ) | `contract_id`, `from`, `to`, `diff` |
| `get_contract_state` | `contract_id`; `limit` (optional, 1-200, default 1 = current state) | Instance storage; requires state indexing |
| `get_contract_data` | `contract_id`; `label` (optional, e.g. `balance`); `limit` (optional, 1-1000, default 100) | Latest value per key; requires key indexing |
| `get_contract_stats` | `contract_id`; `limit` (optional, 1-50, default 10) | `contract_id`, `total_events`, `counts` (`event_name`, `count`, most frequent first) |
| `query_events` | `contract_id`; `event_name` (optional); `limit` (optional, 1-200, default 20); `include_raw` (optional, default `false`) | Events, newest first |
| `get_event` | `event_id`; `include_raw` (optional, default `false`) | `event`; errors if no event has that id |
| `get_transaction_events` | `tx_hash`; `limit` (optional, 1-1000, default 100); `successful_only` (optional, only events from successful calls); `include_raw` (optional, default `false`) | `tx_hash`, `count`, `events` in emission order |
| `query_transfers` | `contract_id`; `from` (optional sender); `to` (optional recipient); `limit` (optional, 1-200, default 20) | `contract_id`, `count`, `transfers` (`from_addr`, `to_addr`, `amount`, `kind`, `ledger`, ...) |
| `query_swaps` | `contract_id`; `sender`, `sell_token`, `buy_token` (all optional); `limit` (optional, 1-200, default 20) | `contract_id`, `count`, `swaps` (sender, sell/buy token and amount, ...) |
| `query_nft_events` | `contract_id`; `kind` (optional: `mint`, `transfer` or `burn`); `from`, `to`, `token_id` (optional); `limit` (optional, 1-200, default 20) | `contract_id`, `count`, `nft_events` |
| `query_liquidity_events` | `contract_id`; `kind` (optional: `add` or `remove`); `provider` (optional); `limit` (optional, 1-200, default 20) | `contract_id`, `count`, `liquidity_events` |
| `call_contract` | `contract_id`; `function`; `args` (object keyed by parameter name, or a positional array) | Typed result of the read-only call |
| `simulate_call` | `contract_id`; `function`; `args` (as above); `source_account` (optional `G...`) | Typed result, the events the call would emit, and its resource cost |

`query_events`, `get_event` and `get_transaction_events` return a compact event shape (`event_id`, `contract_id`, `ledger`, `closed_at`, `event_type`, `name`, `tx_hash`, `params`, `in_successful_call`). `params` is the enriched record when available, otherwise the decoded value. With `include_raw=true` they return the complete event rows, including raw XDR.

`query_nft_events` and `query_liquidity_events` reject any other `kind` value with an error.

## Connect to Claude Desktop

### Step 1: Get the server binary path

```bash
# From the repo root, after building
realpath ./target/release/lumenqraph-mcp
# Output: /path/to/lumenqraph-mcp
```

### Step 2: Edit Claude Desktop config

**macOS/Linux:** `~/.config/Claude/claude_desktop_config.json`

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

Add the `lumenqraph` server to the `mcpServers` object. Replace `/path/to/lumenqraph-mcp` with the actual path from Step 1:

```json
{
  "mcpServers": {
    "lumenqraph": {
      "command": "/path/to/lumenqraph-mcp",
      "env": {
        "DATABASE_URL": "postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph",
        "RPC_URL": "https://soroban-testnet.stellar.org"
      }
    }
  }
}
```

### Step 3: Restart Claude Desktop

Close and reopen Claude Desktop. The MCP server will be available in new conversations.

### Step 4: Verify in Claude

In a chat, you should see a hammer icon 🔨 indicating connected tools. You can now ask Claude to list contracts, explore their interfaces, or call functions.

## Connect to Claude Code (CLI)

### One-time setup

```bash
claude mcp add lumenqraph /path/to/lumenqraph-mcp \
  --env DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph' \
  --env RPC_URL='https://soroban-testnet.stellar.org'
```

Verify it was added:

```bash
claude mcp list
```

### In Claude Code

The MCP server is now available in all new `claude` sessions. Use it like any MCP tool:

```bash
claude "List the indexed contracts and tell me about the first one's interface"
```

## Example: Agent Session

Here's a walkthrough of what an agent (Claude) can do with the MCP server.

### Agent: "List the indexed contracts"

**Agent prompt:**
```
What contracts are indexed? List the first 5 with their event counts.
```

**Agent uses:** `list_contracts`

**Result:**
```json
{
  "contracts": [
    {
      "id": "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
      "events_count": 1543,
      "min_ledger": 521234,
      "max_ledger": 789456
    },
    {
      "id": "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBSC4",
      "events_count": 234,
      "min_ledger": 654321,
      "max_ledger": 789000
    }
    // ... more contracts
  ]
}
```

### Agent: "What can the first contract do?"

**Agent prompt:**
```
Contract CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4 — what functions does it expose? Show me the full interface.
```

**Agent uses:** `get_contract_interface` with `contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"`

**Result:**
```json
{
  "contract_id": "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
  "functions": [
    {
      "name": "transfer",
      "kind": "external",
      "input": [
        {
          "name": "from",
          "type": "Address"
        },
        {
          "name": "to",
          "type": "Address"
        },
        {
          "name": "amount",
          "type": "i128"
        }
      ],
      "output": "bool"
    },
    {
      "name": "balance_of",
      "kind": "readonly",
      "input": [
        {
          "name": "account",
          "type": "Address"
        }
      ],
      "output": "i128"
    }
    // ... more functions
  ],
  "events": [
    {
      "name": "transfer",
      "topics": ["from", "to"],
      "data": "amount"
    }
  ]
}
```

### Agent: "Call the `balance_of` function"

**Agent prompt:**
```
What's the balance of account GBRPYHIL2CI3WHZDTOOQFC6EB4KJJGUJEEN4S7JJLR42CPWX2BKXBQZ?
```

**Agent uses:** `call_contract` with:
- `contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"`
- `function: "balance_of"`
- `args: { "account": "GBRPYHIL2CI3WHZDTOOQFC6EB4KJJGUJEEN4S7JJLR42CPWX2BKXBQZ" }`

**Result:**
```json
{
  "result": "1000000",
  "type": "i128"
}
```

### Agent: "Simulate a transfer"

**Agent prompt:**
```
What would happen if I transferred 500000 from my account (GBRPYHIL2CI3WHZDTOOQFC6EB4KJJGUJEEN4S7JJLR42CPWX2BKXBQZ) to GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF? Show me the events it would emit.
```

**Agent uses:** `simulate_call` with:
- `contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"`
- `function: "transfer"`
- `args: { "from": "GBRPYHIL2CI3WHZDTOOQFC6EB4KJJGUJEEN4S7JJLR42CPWX2BKXBQZ", "to": "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF", "amount": "500000" }`

**Result:**
```json
{
  "result": "true",
  "result_type": "bool",
  "events": [
    {
      "type": "transfer",
      "from": "GBRPYHIL2CI3WHZDTOOQFC6EB4KJJGUJEEN4S7JJLR42CPWX2BKXBQZ",
      "to": "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
      "amount": "500000"
    }
  ],
  "cost": {
    "cpu_insn": 1234,
    "mem_bytes": 5678
  }
}
```

## Troubleshooting

### "Server did not respond" or "Tool not found"

**Problem:** Claude Desktop or Claude Code can't connect to the MCP server.

**Checklist:**
- [ ] The binary path in the config is correct and the file exists
- [ ] The database is running and `DATABASE_URL` is valid
- [ ] The indexer has populated at least one contract
- [ ] Logs: check Claude Desktop's logs (`~/.config/Claude/logs`) or Claude Code's terminal output

### "Contract not found" or "No such contract"

**Problem:** You're trying to query a contract that isn't in the database yet.

**Solution:** Make sure the indexer is running and has seen events from that contract. The MCP server only knows about contracts the indexer has indexed.

### RPC call timeouts

**Problem:** `call_contract` or `simulate_call` times out.

**Solution:** Increase `RPC_TIMEOUT_SECS` (default 30) in the config or `.env`:

```json
{
  "mcpServers": {
    "lumenqraph": {
      "env": {
        "RPC_TIMEOUT_SECS": "60"
      }
    }
  }
}
```

### Slow tool responses

**Problem:** `list_contracts`, `get_contract_interface`, etc. are slow.

**Cause:** Large database or slow Postgres connection.

**Solutions:**
- Ensure Postgres has indexes on `contracts`, `contract_versions`, and `events` (the indexer creates these).
- Check Postgres disk space and memory.
- Verify network latency to the Postgres host.
- Increase the connection pool size in `main.rs` if needed (`max_connections`).

## Architecture

The MCP server is a single binary (`crates/lumenqraph-mcp`) that:
1. **Reads from the same Postgres the indexer writes to** — zero extra data pipeline.
2. **Reuses the same XDR decoder/encoder the REST API uses** — consistent decoding across all access methods.
3. **Runs as a subprocess under the MCP client** — no separate daemon or network endpoint needed.

### JSON-RPC Handshake

When a client connects:

1. Client sends `initialize` with its protocol version.
2. Server responds with its capabilities and server info.
3. Client sends `tools/list` to discover available tools.
4. Client sends `tools/call` to invoke a tool, passing the tool name and arguments.

The server runs a loop on stdin/stdout, handling one message per line.

## Development

### Building from source

```bash
cargo build --release -p lumenqraph-mcp
```

### Environment for testing

```bash
# Set up for local testing
export DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph'
export RPC_URL='https://soroban-testnet.stellar.org'

# Run the server
./target/release/lumenqraph-mcp
```

### Running tests

```bash
# Unit tests (no DB required)
cargo test -p lumenqraph-mcp

# Integration tests (requires TEST_DATABASE_URL)
export TEST_DATABASE_URL='postgres://lumenqraph:lumenqraph@localhost:5432/test'
cargo test -p lumenqraph-mcp --test '*' -- --ignored
```

## See Also

- [Model Context Protocol](https://modelcontextprotocol.io) — the specification
- [Lumenqraph README](../README.md#ai-agent-access--the-mcp-server) — quick reference
- [Architecture](ARCHITECTURE.md) — how the indexer, API, and MCP layers fit together

### Agent: "What changed between two versions?"

**Agent prompt:**
```
Contract <contract_id> was upgraded. What changed between version 1 and version 3, and is it breaking?
```

**Agent uses:** `get_contract_upgrades` with `contract_id` to see which versions exist, then `diff_contract_interface` with:
- `contract_id: "<contract_id>"`
- `from: 1`
- `to: 3`

**Result:** an object with `contract_id`, `from`, `to` and a `diff` describing the functions, events and types that were added, removed or changed between the two versions. `diff_contract_interface` compares any two versions directly, so the agent does not have to chain the per-upgrade diffs from `get_contract_upgrades`.

### Agent: "Show a wallet's transfer history"

**Agent prompt:**
```
Show the 10 most recent transfers of token <contract_id> sent by <address>.
```

**Agent uses:** `query_transfers` with:
- `contract_id: "<contract_id>"`
- `from: "<address>"`
- `limit: 10`

**Result:** an object with `contract_id`, `count` and a `transfers` array, newest first. Each transfer carries `event_id`, `from_addr`, `to_addr`, `amount`, `kind`, `ledger` and `ledger_closed_at`. Pass `to` instead of (or together with) `from` to filter by recipient.
