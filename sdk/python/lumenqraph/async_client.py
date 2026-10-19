"""Async (asyncio) Lumenqraph API client.

This module mirrors the surface of :class:`lumenqraph.client.LumenqraphClient`
but uses :mod:`asyncio` and :class:`urllib.request` via a thread-pool executor
(so no third-party dependency is needed) for all HTTP I/O.

For high-concurrency use-cases you may prefer to install ``aiohttp`` or
``httpx`` and wrap them instead — this implementation favours zero dependencies
over raw throughput.

Example::

    import asyncio
    from lumenqraph.async_client import AsyncLumenqraphClient

    async def main():
        async with AsyncLumenqraphClient(base_url="http://localhost:8080") as lq:
            contracts = await lq.list_contracts()
            async for event in lq.paginate_events(contracts["data"][0]["contract_id"]):
                print(event["event_name"], event.get("enriched") or event.get("decoded_value"))

    asyncio.run(main())
"""

import asyncio
import json
import time
import urllib.error
import urllib.request
from typing import Any, AsyncGenerator, Dict, Optional
from urllib.parse import urlencode

from .client import LumenqraphError, RetryOptions


class AsyncLumenqraphClient:
    """Async Lumenqraph API client.

    All methods are coroutines.  HTTP I/O is dispatched to a thread-pool
    executor so the event loop is never blocked.

    The client can be used as an async context manager::

        async with AsyncLumenqraphClient(base_url="http://localhost:8080") as lq:
            health = await lq.health()

    It can also be instantiated directly without the context-manager protocol;
    call :meth:`aclose` explicitly when done if you need deterministic cleanup.
    """

    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BASE_DELAY_MS = 250
    DEFAULT_MAX_DELAY_MS = 30_000
    DEFAULT_TIMEOUT_MS = 10_000
    RETRYABLE_STATUSES = {429, 502, 503, 504}

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        retry: Optional[RetryOptions] = None,
    ) -> None:
        """Initialise the async Lumenqraph client.

        Args:
            base_url:  Base URL of the Lumenqraph API (e.g. ``http://localhost:8080``).
            api_key:   Optional API key for authenticated requests.
            retry:     Optional retry / timeout policy.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._retry = self._merge_retry(retry or {})

    def _merge_retry(self, opts: RetryOptions) -> Dict[str, int]:
        return {
            "max_retries": opts.get("max_retries", self.DEFAULT_MAX_RETRIES),
            "base_delay_ms": opts.get("base_delay_ms", self.DEFAULT_BASE_DELAY_MS),
            "max_delay_ms": opts.get("max_delay_ms", self.DEFAULT_MAX_DELAY_MS),
            "timeout_ms": opts.get("timeout_ms", self.DEFAULT_TIMEOUT_MS),
        }

    async def __aenter__(self) -> "AsyncLumenqraphClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Release any resources held by the client (currently a no-op)."""

    # ---- Core request machinery ----

    def _make_url(self, path: str, query: Optional[Dict[str, Any]] = None) -> str:
        url = self.base_url + path
        if query:
            filtered = {k: v for k, v in query.items() if v is not None}
            if filtered:
                url = f"{url}?{urlencode(filtered)}"
        return url

    def _sync_request(
        self,
        method: str,
        url: str,
        body: Optional[bytes],
        headers: Dict[str, str],
        timeout: float,
    ) -> Any:
        """Blocking HTTP call — run in a thread-pool executor."""
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        attempt = 0
        max_retries = self._retry["max_retries"]

        while True:
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    text = resp.read().decode("utf-8")
                    return json.loads(text) if text else None
            except urllib.error.HTTPError as exc:
                status = exc.code
                text = exc.read().decode("utf-8")
                try:
                    body_obj = json.loads(text)
                except json.JSONDecodeError:
                    body_obj = text

                if status not in self.RETRYABLE_STATUSES or attempt >= max_retries:
                    raise LumenqraphError(f"HTTP {status}: {text}", status, body_obj)

                delay_s = min(
                    self._retry["base_delay_ms"] * (2 ** attempt),
                    self._retry["max_delay_ms"],
                ) / 1000.0
                time.sleep(delay_s)
                attempt += 1

            except urllib.error.URLError as exc:
                if attempt >= max_retries:
                    raise LumenqraphError(f"Network error: {exc}", 0, None)
                delay_s = min(
                    self._retry["base_delay_ms"] * (2 ** attempt),
                    self._retry["max_delay_ms"],
                ) / 1000.0
                time.sleep(delay_s)
                attempt += 1

    async def _request(
        self,
        method: str,
        path: str,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = self._make_url(path, query)
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key

        encoded_body: Optional[bytes] = None
        if body is not None:
            encoded_body = json.dumps(body).encode("utf-8")

        timeout = self._retry["timeout_ms"] / 1000.0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._sync_request,
            method,
            url,
            encoded_body,
            headers,
            timeout,
        )

    async def _get(self, path: str, query: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("GET", path, query)

    async def _post(self, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request("POST", path, body=body)

    # ---- Public API surface (mirrors LumenqraphClient) ----

    async def health(self) -> Dict[str, Any]:
        """Get health and indexing-lag report."""
        return await self._get("/health")

    async def list_contracts(
        self, limit: int = 200, offset: int = 0
    ) -> Dict[str, Any]:
        """Get contracts with pagination."""
        return await self._get("/contracts", {"limit": limit, "offset": offset})

    async def get_interface(
        self, contract_id: str, version: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get a contract's decoded on-chain interface."""
        query: Dict[str, Any] = {}
        if version is not None:
            query["version"] = version
        return await self._get(f"/contracts/{contract_id}/interface", query)

    async def get_state(
        self, contract_id: str, limit: int = 1
    ) -> Dict[str, Any]:
        """Get versioned instance-storage snapshots, newest first."""
        return await self._get(
            f"/contracts/{contract_id}/state", {"limit": limit}
        )

    async def get_data(
        self,
        contract_id: str,
        label: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Get latest value of every per-key entry."""
        query: Dict[str, Any] = {"limit": limit}
        if label:
            query["label"] = label
        return await self._get(f"/contracts/{contract_id}/data", query)

    async def get_data_key(
        self, contract_id: str, key_hash: str, limit: int = 50
    ) -> Dict[str, Any]:
        """Get version history of a single per-key entry."""
        return await self._get(
            f"/contracts/{contract_id}/data/{key_hash}", {"limit": limit}
        )

    async def list_events(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        event_name: Optional[str] = None,
        after: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get recent events for a contract, newest first."""
        query: Dict[str, Any] = {"limit": limit}
        if offset:
            query["offset"] = offset
        if event_name:
            query["event_name"] = event_name
        if after:
            query["after"] = after
        return await self._get(f"/contracts/{contract_id}/events", query)

    async def list_transfers(
        self,
        contract_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get materialized SEP-41 transfers."""
        path = f"/contracts/{contract_id}/transfers" if contract_id else "/transfers"
        return await self._get(path, {"limit": limit, "offset": offset})

    async def list_functions(self, contract_id: str) -> Dict[str, Any]:
        """Get a contract's callable view functions and their typed signatures."""
        return await self._get(f"/contracts/{contract_id}/functions")

    async def call(
        self,
        contract_id: str,
        function: str,
        args: Optional[Any] = None,
        source_account: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Invoke a view function read-only and get a typed result."""
        return await self._post(
            f"/contracts/{contract_id}/call",
            {"function": function, "args": args, "source_account": source_account},
        )

    async def simulate(
        self,
        contract_id: str,
        function: str,
        args: Optional[Any] = None,
        source_account: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dry-run any call and preview its result, emitted events, and cost."""
        return await self._post(
            f"/contracts/{contract_id}/simulate",
            {"function": function, "args": args, "source_account": source_account},
        )

    async def graphql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Execute a raw GraphQL query against ``/graphql``."""
        body = await self._post("/graphql", {"query": query, "variables": variables or {}})
        errors = (body or {}).get("errors")
        if errors:
            messages = "; ".join(e.get("message", str(e)) for e in errors)
            raise LumenqraphError(f"GraphQL error: {messages}", 200, errors)
        return (body or {}).get("data")

    async def paginate_events(
        self,
        contract_id: str,
        event_name: Optional[str] = None,
        page_size: int = 100,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Async generator over all events for a contract via cursor pagination.

        Transparently fetches page after page until all events have been
        yielded.

        Args:
            contract_id:  Contract to fetch events for.
            event_name:   Optional event-name filter.
            page_size:    Number of events to request per page (default 100).

        Yields:
            Event record dicts.

        Example::

            async for event in lq.paginate_events(contract_id, event_name="transfer"):
                print(event["ledger"], event.get("enriched"))
        """
        cursor: Optional[str] = None
        while True:
            response = await self.list_events(
                contract_id,
                limit=page_size,
                event_name=event_name,
                after=cursor,
            )
            data = response.get("data", [])
            for event in data:
                yield event
            if not response.get("has_more", False):
                break
            cursor = response.get("next_cursor")
            if not cursor:
                break

    async def paginate_transfers(
        self,
        contract_id: Optional[str] = None,
        page_size: int = 100,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Async generator over all materialized transfers.

        Args:
            contract_id:  Optional contract to scope the query to.
            page_size:    Transfers per page (default 100).

        Yields:
            Transfer record dicts.
        """
        offset = 0
        while True:
            response = await self.list_transfers(
                contract_id, limit=page_size, offset=offset
            )
            items = response if isinstance(response, list) else response.get("data", [])
            for item in items:
                yield item
            if not items or len(items) < page_size:
                break
            offset += len(items)
