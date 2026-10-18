# Lumenqraph Python SDK

A typed Python client over the Lumenqraph REST + GraphQL API. HTTP uses only the standard library; `pydantic` backs the optional generated models.

## Installation

```bash
pip install lumenqraph
```

## Quick Start

```python
from lumenqraph import LumenqraphClient

# Initialize the client
lq = LumenqraphClient(base_url="http://localhost:8080")

# Or with an API key
lq = LumenqraphClient(
    base_url="http://localhost:8080",
    api_key="your-api-key"
)

# Get contracts
contracts = lq.list_contracts()
print(f"Found {len(contracts['data'])} contracts")

# Get events for a contract (with pagination)
for event in lq.paginate_events(contracts['data'][0]['contract_id']):
    print(f"Event: {event['event_name']}")
    print(f"  Value: {event.get('enriched') or event.get('decoded_value')}")
```

## Features

### API coverage

Every REST resource the API serves is covered by both clients. Method names are
the snake_case form of the [TypeScript SDK](../typescript)'s; the async client
(`AsyncLumenqraphClient`) has the same methods as coroutines, and its
`paginate_*` methods return async iterators.

| Resource | Endpoint | Python method | TypeScript |
| --- | --- | --- | --- |
| Health | `GET /health` | `health()` | `health` |
| Liveness / readiness | `GET /livez`, `GET /readyz` | `livez()`, `readyz()` | — |
| Contracts | `GET /contracts` | `list_contracts(limit, after)`, `paginate_contracts()` | `listContracts` |
| Interface | `GET /contracts/{id}/interface` | `get_interface(id, version)` | `getInterface`, `getInterfaceAtVersion` |
| Interface history | `GET /contracts/{id}/interface/history` | `get_interface_history(id, limit, after)` | `getInterfaceHistory` |
| Interface diff | `GET /contracts/{id}/interface/diff` | `get_interface_diff(id, from_version, to_version)` | `getInterfaceDiff` |
| Refresh interface | `POST /contracts/{id}/refresh` | `refresh_contract(id)` | — |
| Generated SDK | `GET /contracts/{id}/sdk` | `generate_sdk(id, lang, version)` | `generateSdk` |
| Stats | `GET /contracts/{id}/stats` | `get_stats(id, bucket, resolution, window, group_by, …)` | `getStats` |
| Instance state | `GET /contracts/{id}/state` | `get_state(id, limit)` | `getState` |
| Per-key data | `GET /contracts/{id}/data` | `get_data(id, label, limit)` | `getData` |
| Per-key history | `GET /contracts/{id}/data/{key_hash}` | `get_data_key(id, key_hash, limit)` | `getDataKey` |
| Events | `GET /contracts/{id}/events` | `list_events(id, …filters)`, `paginate_events(id, …)` | `listEvents`, `paginateEventsRest` |
| Event stream (SSE) | `GET /contracts/{id}/events/stream` | `stream_events(id, event_name, cursor)` *(sync client)* | `streamEvents` |
| Single event | `GET /events/{event_id}` | `get_event(event_id)` | `getEvent` |
| Transaction events | `GET /transactions/{tx_hash}/events` | `get_transaction_events(tx_hash, limit)` | `getTransactionEvents` |
| Transfers | `GET /contracts/{id}/transfers` | `list_transfers(id, …)`, `paginate_transfers(id, …)` | `listTransfers`, `paginateTransfers` |
| Swaps | `GET /contracts/{id}/swaps` | `list_swaps(id, …)`, `paginate_swaps(id, …)` | `listSwaps`, `paginateSwaps` |
| NFTs | `GET /contracts/{id}/nfts` | `list_nft_events(id, …)`, `paginate_nft_events(id, …)` | `listNftEvents`, `paginateNftEvents` |
| Liquidity | `GET /contracts/{id}/liquidity` | `list_liquidity_events(id, …)`, `paginate_liquidity_events(id, …)` | `listLiquidityEvents`, `paginateLiquidityEvents` |
| Functions | `GET /contracts/{id}/functions` | `list_functions(id)` | `listFunctions` |
| Call | `POST /contracts/{id}/call` | `call(id, function, args, source_account)` | `call` |
| Simulate | `POST /contracts/{id}/simulate` | `simulate(id, function, args, source_account)` | `simulate` |
| Create webhook | `POST /webhooks` | `create_webhook(url, kind, contract_id(s), event_name(s), filter, since)` | `createWebhook` |
| List webhooks | `GET /webhooks` | `list_webhooks()` | `listWebhooks` |
| Update webhook | `PATCH /webhooks/{id}` | `update_webhook(id, active, …filters)` | `updateWebhook` |
| Delete webhook | `DELETE /webhooks/{id}` | `delete_webhook(id)` | `deleteWebhook` |
| Deliveries | `GET /webhooks/{id}/deliveries` | `list_deliveries(id, …, include_summary)`, `paginate_deliveries(id)` | `listDeliveries`, `paginateDeliveries` |
| Redrive | `POST /webhooks/{id}/redrive` | `redrive_webhook(id, since)` | `redriveWebhook` |
| Re-enable | `POST /webhooks/{id}/reenable` | `reenable_webhook(id)` | `reenableWebhook` |
| Rotate secret | `POST /webhooks/{id}/rotate-secret` | `rotate_webhook_secret(id, grace_seconds)` | `rotateWebhookSecret` |
| GraphQL | `POST /graphql` | `graphql(query, variables)`, `events_page(id, …)`, `paginate_events_graphql(id, …)` | `graphql`, `eventsPage`, `paginateEvents` |
| Webhook signatures | — | `verify_webhook_signature(body, header, secret)` | `verifyWebhook` |

Transfers, swaps, NFTs and liquidity are per-contract resources, so
`contract_id` is a required argument for each of them.

### Typed models

`lumenqraph.models` contains pydantic v2 models generated from the repository's
[`openapi.yaml`](../../openapi.yaml) (`Contract`, `EventRecord`, `EventsPage`,
`WebhookSubscription`, `DeliveriesPage`, …). The clients return plain dicts, and
you can validate a response when you want typed access:

```python
from lumenqraph import LumenqraphClient, models

lq = LumenqraphClient(base_url="http://localhost:8080")
page = models.EventsPage.model_validate(lq.list_events("CABC..."))
for event in page.data:
    print(event.ledger, event.event_name)
```

Do not edit `models.py` by hand; regenerate it (see [Development](#development)).
CI fails when it drifts from `openapi.yaml`.

### Pagination

The SDK supports cursor-based pagination for efficient paging through large result sets:

```python
# Cursor pagination (recommended for large datasets)
for event in lq.paginate_events(contract_id):
    print(event)

# Manual cursor pagination
response = lq.list_events(contract_id, limit=100)
while True:
    handle(response["data"])
    if not response["has_more"]:
        break
    response = lq.list_events(contract_id, limit=100, after=response["next_cursor"])
```

### Retry Policy

The client retries 429/502/503/504 responses and network errors with exponential backoff, honouring `Retry-After` on 429:

```python
lq = LumenqraphClient(
    base_url="http://localhost:8080",
    retry={
        "max_retries": 3,           # Maximum number of retries (default 3)
        "base_delay_ms": 250,       # Initial delay in milliseconds (default 250)
        "max_delay_ms": 30_000,     # Maximum delay cap (default 30s)
        "timeout_ms": 10_000,       # Per-request timeout (default 10s)
    }
)
```

### Webhook Signature Verification

Verify incoming webhook signatures for security:

```python
from lumenqraph import verify_webhook_signature

def handle_webhook(request):
    signature = request.headers.get('X-Lumenqraph-Signature')
    body = request.get_data(as_text=True)
    secret = "your-webhook-secret"

    if not verify_webhook_signature(body, signature, secret):
        return {"error": "Invalid signature"}, 401

    # Process the webhook
    data = json.loads(body)
    return {"ok": True}, 200
```

## Type Hints

The SDK is fully typed for IDE autocompletion and type checking:

The package ships a `py.typed` marker and is checked with `mypy --strict` in CI.
Responses are typed as `dict[str, Any]`; use `lumenqraph.models` for validated,
attribute-style access. `call`/`simulate`, `get_state` and `get_data` on the sync
client return small dataclasses (`CallResult`, `ContractState`, `ContractData`).

## Error Handling

The SDK raises `LumenqraphError` for API errors:

```python
from lumenqraph import LumenqraphClient, LumenqraphError

lq = LumenqraphClient(base_url="http://localhost:8080")

try:
    result = lq.call("CABC...", function="get_balance")
except LumenqraphError as e:
    print(f"API error: {e}")
    print(f"Status: {e.status}")
    print(f"Response body: {e.body}")
```

## Examples

### List all events for a contract

```python
lq = LumenqraphClient(base_url="http://localhost:8080")

# Get all events using cursor pagination
count = 0
for event in lq.paginate_events("CABC..."):
    print(f"{event['event_name']}: {event['decoded_value']}")
    count += 1

print(f"Total events: {count}")
```

### Find a specific event type

```python
for event in lq.paginate_events("CABC...", event_name="transfer"):
    print(f"Transfer: {event['enriched']}")
```

### Get contract interface and call a function

```python
interface = lq.get_interface("CABC...")
print(f"Functions: {interface['interface']['functions']}")

# Call a view function
result = lq.call("CABC...", function="get_balance", args=["GXYZ..."])
print(f"Balance: {result.result}")
```

### Simulate a transaction

```python
simulation = lq.simulate(
    "CABC...",
    function="transfer",
    args=["from", "to", "1000"]
)
print(f"Events: {simulation.events}")
print(f"Cost: {simulation.min_resource_fee} stroops")
```

## Development

Supported Python versions: 3.10 – 3.14.

### Running Tests and Type Checks

```bash
pip install -e ".[dev]"
python -m pytest tests/
mypy --strict lumenqraph
```

### Regenerating the models

```bash
pip install -e ".[codegen]"
python scripts/codegen.py          # rewrite lumenqraph/models.py from ../../openapi.yaml
python scripts/codegen.py --check  # what CI runs: fail if models.py is stale
```

### Building the Package

```bash
python -m build
```

### Publishing to PyPI

Releases are published automatically by the `pypi-publish` GitHub Actions workflow
when a `v*` tag is pushed. Pre-release tags (for example `v0.2.0-rc.1`) are published
to TestPyPI first; stable tags go to PyPI via Trusted Publishing (OIDC, no API token).
The workflow fails if the version in `pyproject.toml` does not match the tag.

To publish manually:

```bash
python -m build
python -m twine upload dist/*
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.
