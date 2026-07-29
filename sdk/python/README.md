# Lumenqraph Python SDK

A typed Python client for the Lumenqraph API - Stellar smart contract indexing and analytics.

## Installation

```bash
pip install lumenqraph
```

## Quick Start

```python
from lumenqraph import LumenqraphClient

# Initialize the client
async with LumenqraphClient(base_url="http://localhost:8080") as client:
    # List all contracts
    contracts = await client.list_contracts()
    
    # Get events for a contract
    contract_id = contracts[0]["contract_id"]
    events = await client.list_events(contract_id, limit=10)
    
    # Paginate through all events
    async for event in client.paginate_events(contract_id):
        print(event["event_name"], event.get("enriched") or event["decoded_value"])
```

## Features

- **Typed**: Full type hints for better IDE support and type checking
- **Async**: Built on `httpx` for efficient async/await patterns
- **Retry Logic**: Automatic retries with exponential backoff for failed requests
- **Cursor Pagination**: Async generators for seamless pagination through large datasets
- **Webhook Verification**: Helper function for verifying webhook signatures

## API Methods

### Contracts

```python
# List all tracked contracts
contracts = await client.list_contracts()

# Get contract interface
interface = await client.get_interface(contract_id)

# List callable functions
functions = await client.list_functions(contract_id)
```

### Events

```python
# Get recent events (with pagination)
events = await client.list_events(
    contract_id,
    limit=50,
    offset=0,
    event_name="transfer"  # optional filter
)

# Paginate through all events
async for event in client.paginate_events(contract_id, page_size=100):
    process_event(event)

# Get a single page via GraphQL
page = await client.events_page(contract_id, first=50, after=cursor)
```

### State & Data

```python
# Get contract state snapshots
state = await client.get_state(contract_id, limit=10)

# Get all data keys (e.g., token balances)
data = await client.get_data(contract_id, label="Balance", limit=100)

# Get history of a specific key
key_history = await client.get_data_key(contract_id, key_hash, limit=10)
```

### Transfers

```python
# Get all transfers
transfers = await client.list_transfers(limit=50)

# Get transfers for a specific contract
contract_transfers = await client.list_transfers(contract_id, limit=50)
```

### Contract Calls

```python
# Call a read-only function
result = await client.call(
    contract_id,
    {
        "function": "balance",
        "args": {"address": "G..."},
    }
)

# Simulate a transaction
simulation = await client.simulate(
    contract_id,
    {
        "function": "transfer",
        "args": {"from": "G...", "to": "G...", "amount": 1000},
        "source_account": "G..."
    }
)
print(simulation["events"])  # Preview emitted events
print(simulation["min_resource_fee"])  # Estimated cost
```

### GraphQL

```python
# Execute raw GraphQL queries
query = """
    query GetContract($id: String!) {
        contract(id: $id) {
            contractId
            eventCount
        }
    }
"""
result = await client.graphql(query, {"id": contract_id})
```

## Retry Configuration

Customize retry behavior:

```python
client = LumenqraphClient(
    base_url="http://localhost:8080",
    retry={
        "max_retries": 5,        # Maximum retry attempts (default: 3)
        "base_delay_ms": 500,    # Initial retry delay (default: 250ms)
        "max_delay_ms": 60000,   # Maximum delay cap (default: 30000ms)
        "timeout_ms": 15000,     # Request timeout (default: 10000ms)
    }
)
```

## Webhook Verification

Verify webhook signatures for secure webhook handling:

```python
from fastapi import FastAPI, Request, HTTPException
from lumenqraph import verify_webhook
import os

app = FastAPI()

@app.post("/webhook")
async def handle_webhook(request: Request):
    raw_body = await request.body()
    signature = request.headers.get("x-lumenqraph-signature", "")
    secret = os.environ["WEBHOOK_SECRET"]
    
    if not await verify_webhook(raw_body, signature, secret):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Process webhook payload
    data = await request.json()
    return {"status": "received"}
```

## Authentication

If your Lumenqraph instance requires an API key:

```python
client = LumenqraphClient(
    base_url="http://localhost:8080",
    api_key="your-api-key"
)
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/lumenqraph.git
cd lumenqraph/sdk/python

# Install dependencies
pip install -e ".[dev]"
```

### Testing

```bash
# Run tests
pytest

# Run tests with coverage
pytest --cov=lumenqraph --cov-report=html

# Type checking
mypy lumenqraph

# Linting
ruff check lumenqraph
```

## License

MIT License - see the LICENSE file for details.
