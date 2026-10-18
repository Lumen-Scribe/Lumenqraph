"""
Lumenqraph Python SDK — a typed client over the Lumenqraph REST + GraphQL API.

Synchronous example::

    from lumenqraph import LumenqraphClient

    lq = LumenqraphClient(base_url="http://localhost:8080")
    contracts = lq.list_contracts()
    for event in lq.paginate_events(contracts["data"][0]["contract_id"]):
        print(event["event_name"], event.get("enriched") or event.get("decoded_value"))

Typed response models generated from ``openapi.yaml`` are available in
:mod:`lumenqraph.models`, e.g. ``models.EventsPage.model_validate(page)``.

Async example::

    import asyncio
    from lumenqraph import AsyncLumenqraphClient

    async def main():
        async with AsyncLumenqraphClient(base_url="http://localhost:8080") as lq:
            contracts = await lq.list_contracts()
            async for event in lq.paginate_events(contracts["data"][0]["contract_id"]):
                print(event["event_name"], event.get("enriched") or event.get("decoded_value"))

    asyncio.run(main())
"""

from . import models
from .async_client import AsyncLumenqraphClient
from .client import (
    ClientOptions,
    LumenqraphClient,
    LumenqraphError,
    RetryOptions,
)
from .webhook import verify_webhook_signature

__all__ = [
    "LumenqraphClient",
    "AsyncLumenqraphClient",
    "LumenqraphError",
    "ClientOptions",
    "RetryOptions",
    "models",
    "verify_webhook_signature",
]

__version__ = "0.1.0"
