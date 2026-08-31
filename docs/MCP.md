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

## Transport & Environment

### Transport

The server uses **newline-delimited JSON-RPC 2.0 over stdio**, the standard MCP transport:
- Read JSON-RPC requests from stdin, one per line.
- Write JSON-RPC responses to stdout, one per line.
- Log messages go to stderr (so stdout stays a clean protocol channel).

### Environment Variables

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `DATABASE_URL` | ✅ | — | Postgres connection string, e.g. `postgres://user:pass@host:5432/db` |
| `RPC_URL` | ❌ | `https://soroban-testnet.stellar.org` | Stellar RPC endpoint for `call_contract` and `simulate_call` |
| `RPC_TIMEOUT_SECS` | ❌ | `30` | Timeout for RPC calls in seconds |
| `MCP_AUTH_TOKEN` | ❌ | — | Optional auth token required in `Authorization` field during `initialize` |

Example `.env` file for local testing:

```bash
DATABASE_URL=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph
RPC_URL=https://soroban-testnet.stellar.org
RPC_TIMEOUT_SECS=30
MCP_AUTH_TOKEN=your-secret-token
```

## Tools

The MCP server exposes eight tools for agents to discover and query Soroban contracts:

| Tool | Purpose |
| --- | --- |
| `list_contracts` | List all indexed contracts with event counts and ledger ranges |
| `get_contract_interface` | Get a contract's decoded on-chain interface (functions, events, types) |
| `get_contract_upgrades` | View a contract's interface history with semantic diffs and breaking-change detection |
| `get_contract_state` | Read a contract's current (and historical) instance storage |
| `get_contract_data` | Read a contract's per-key state — individual entries like token balances |
| `query_events` | Query recent indexed events for a contract, newest first |
| `call_contract` | Invoke a contract's view function read-only via RPC simulation, with type-checking |
| `simulate_call` | Dry-run ANY contract call (including state-changing ones) without submitting it |

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
