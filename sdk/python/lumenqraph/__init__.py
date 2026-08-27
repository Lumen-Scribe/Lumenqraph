"""
Lumenqraph Python SDK — a typed client over the Lumenqraph REST + GraphQL API.

Example usage:

    from lumenqraph import LumenqraphClient

    lq = LumenqraphClient(base_url="http://localhost:8080")
    contracts = lq.list_contracts()
    for event in lq.paginate_events(contracts[0]["contract_id"]):
        print(event["event_name"], event.get("enriched") or event.get("decoded_value"))
"""

from .client import (
    LumenqraphClient,
    LumenqraphError,
    ClientOptions,
    RetryOptions,
)
from .webhook import verify_webhook_signature

__all__ = [
    "LumenqraphClient",
    "LumenqraphError",
    "ClientOptions",
    "RetryOptions",
    "verify_webhook_signature",
]

__version__ = "0.1.0"
