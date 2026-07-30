"""
Lumenqraph Python SDK — a typed client over the Lumenqraph REST + GraphQL API.

Minimal dependencies: uses httpx for HTTP with retry support.

Example:
    >>> from lumenqraph import LumenqraphClient
    >>> client = LumenqraphClient(base_url="http://localhost:8080")
    >>> contracts = await client.list_contracts()
    >>> async for event in client.paginate_events(contracts[0]["contract_id"]):
    ...     print(event["event_name"], event.get("enriched") or event["decoded_value"])
"""

from .client import LumenqraphClient, LumenqraphError
from .types import (
    Contract,
    EventRecord,
    Transfer,
    StateVersion,
    ContractState,
    DataKey,
    ContractData,
    DataKeyHistory,
    CallResult,
    CallOptions,
    Page,
    RetryOptions,
    ClientOptions,
)
from .webhook import verify_webhook

__version__ = "0.1.0"

__all__ = [
    "LumenqraphClient",
    "LumenqraphError",
    "Contract",
    "EventRecord",
    "Transfer",
    "StateVersion",
    "ContractState",
    "DataKey",
    "ContractData",
    "DataKeyHistory",
    "CallResult",
    "CallOptions",
    "Page",
    "RetryOptions",
    "ClientOptions",
    "verify_webhook",
]
