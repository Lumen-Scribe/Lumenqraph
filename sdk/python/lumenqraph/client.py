"""Main Lumenqraph client implementation."""

import asyncio
import random
import time
from typing import Any, AsyncGenerator
from urllib.parse import urlencode, quote

import httpx

from .types import (
    CallOptions,
    CallResult,
    Contract,
    ContractData,
    ContractState,
    DataKeyHistory,
    EventRecord,
    Json,
    Page,
    RetryOptions,
    Transfer,
)

# Default retry/timeout constants
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY_MS = 250
DEFAULT_MAX_DELAY_MS = 30_000
DEFAULT_TIMEOUT_MS = 10_000

# HTTP status codes that merit a retry
RETRYABLE_STATUSES = {429, 502, 503, 504}


class LumenqraphError(Exception):
    """Error raised for any non-2xx API response."""

    def __init__(self, message: str, status: int, body: Any):
        super().__init__(message)
        self.status = status
        self.body = body


class LumenqraphClient:
    """
    A typed client over the Lumenqraph REST + GraphQL API.

    Example:
        >>> client = LumenqraphClient(base_url="http://localhost:8080")
        >>> contracts = await client.list_contracts()
        >>> async for event in client.paginate_events(contracts[0]["contract_id"]):
        ...     print(event)
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        retry: RetryOptions | None = None,
    ):
        """
        Initialize the Lumenqraph client.

        Args:
            base_url: Base URL of the Lumenqraph API (e.g., http://localhost:8080)
            api_key: Optional API key for authentication
            retry: Optional retry/timeout policy
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.retry = {
            "max_retries": retry.get("max_retries", DEFAULT_MAX_RETRIES) if retry else DEFAULT_MAX_RETRIES,
            "base_delay_ms": retry.get("base_delay_ms", DEFAULT_BASE_DELAY_MS) if retry else DEFAULT_BASE_DELAY_MS,
            "max_delay_ms": retry.get("max_delay_ms", DEFAULT_MAX_DELAY_MS) if retry else DEFAULT_MAX_DELAY_MS,
            "timeout_ms": retry.get("timeout_ms", DEFAULT_TIMEOUT_MS) if retry else DEFAULT_TIMEOUT_MS,
        }
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def close(self):
        """Close the underlying HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            timeout = httpx.Timeout(self.retry["timeout_ms"] / 1000.0)
            self._client = httpx.AsyncClient(timeout=timeout)
        return self._client

    # ---- REST API Methods ----

    async def health(self) -> Json:
        """Get liveness + indexing-lag report."""
        return await self._get("/health")

    async def list_contracts(self) -> list[Contract]:
        """Get contracts the indexer has seen, with per-contract event counts."""
        return await self._get("/contracts")

    async def get_interface(self, contract_id: str) -> Json:
        """Get a contract's decoded on-chain interface (functions, events, types)."""
        return await self._get(f"/contracts/{_enc(contract_id)}/interface")

    async def get_state(
        self, contract_id: str, limit: int | None = None
    ) -> ContractState:
        """
        Get versioned instance-storage snapshots, newest first.

        Args:
            contract_id: Contract ID
            limit: Maximum number of versions to return (default: all)
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        return await self._get(f"/contracts/{_enc(contract_id)}/state", params)

    async def get_data(
        self,
        contract_id: str,
        label: str | None = None,
        limit: int | None = None,
    ) -> ContractData:
        """
        Get latest value of every per-key entry (e.g., holder balances).

        Args:
            contract_id: Contract ID
            label: Optional label filter
            limit: Maximum number of keys to return
        """
        params = {}
        if label is not None:
            params["label"] = label
        if limit is not None:
            params["limit"] = limit
        return await self._get(f"/contracts/{_enc(contract_id)}/data", params)

    async def get_data_key(
        self,
        contract_id: str,
        key_hash: str,
        limit: int | None = None,
    ) -> DataKeyHistory:
        """
        Get the version history of a single per-key entry.

        Args:
            contract_id: Contract ID
            key_hash: Key hash
            limit: Maximum number of versions to return
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        return await self._get(
            f"/contracts/{_enc(contract_id)}/data/{_enc(key_hash)}", params
        )

    async def list_events(
        self,
        contract_id: str,
        limit: int | None = None,
        offset: int | None = None,
        event_name: str | None = None,
    ) -> list[EventRecord]:
        """
        Get recent events for a contract, newest first.

        Args:
            contract_id: Contract ID
            limit: Maximum number of events
            offset: Pagination offset
            event_name: Filter by event name
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if event_name is not None:
            params["event_name"] = event_name
        return await self._get(f"/contracts/{_enc(contract_id)}/events", params)

    async def list_transfers(
        self,
        contract_id: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Transfer]:
        """
        Get materialized SEP-41 transfers, newest first.

        Args:
            contract_id: Optional contract ID to filter by
            limit: Maximum number of transfers
            offset: Pagination offset
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        path = (
            f"/contracts/{_enc(contract_id)}/transfers"
            if contract_id
            else "/transfers"
        )
        return await self._get(path, params)

    async def list_functions(self, contract_id: str) -> Json:
        """Get a contract's callable view functions and their typed signatures."""
        return await self._get(f"/contracts/{_enc(contract_id)}/functions")

    async def call(self, contract_id: str, options: CallOptions) -> CallResult:
        """
        Invoke a view function read-only and get a typed result.

        Args:
            contract_id: Contract ID
            options: Call options (function name, args, source account)
        """
        body = {
            "function": options["function"],
            "args": options.get("args"),
            "source_account": options.get("source_account"),
        }
        return await self._post(f"/contracts/{_enc(contract_id)}/call", body)

    async def simulate(self, contract_id: str, options: CallOptions) -> CallResult:
        """
        Dry-run any call and preview its result, emitted events, and cost.

        Args:
            contract_id: Contract ID
            options: Call options (function name, args, source account)
        """
        body = {
            "function": options["function"],
            "args": options.get("args"),
            "source_account": options.get("source_account"),
        }
        return await self._post(f"/contracts/{_enc(contract_id)}/simulate", body)

    # ---- GraphQL Methods ----

    async def graphql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> Json:
        """
        Execute a raw GraphQL query against /graphql.

        Args:
            query: GraphQL query string
            variables: Optional query variables
        """
        body = {"query": query, "variables": variables or {}}
        response = await self._post("/graphql", body)

        if isinstance(response, dict) and "errors" in response:
            errors = response["errors"]
            if errors:
                messages = "; ".join(e.get("message", str(e)) for e in errors)
                raise LumenqraphError(f"GraphQL error: {messages}", 200, errors)

        return response.get("data") if isinstance(response, dict) else response

    async def events_page(
        self,
        contract_id: str,
        first: int = 50,
        after: str | None = None,
        event_name: str | None = None,
    ) -> Page[EventRecord]:
        """
        Get one cursor page of events via GraphQL.

        Args:
            contract_id: Contract ID
            first: Number of events per page
            after: Cursor for pagination
            event_name: Filter by event name
        """
        query = """
            query Events($id: String!, $name: String, $first: Int, $after: String) {
                events(contractId: $id, eventName: $name, first: $first, after: $after) {
                    edges {
                        cursor
                        node {
                            eventId contractId ledger ledgerClosedAt eventType eventName
                            decodedTopics decodedValue enriched txHash inSuccessfulCall
                        }
                    }
                    pageInfo { hasNextPage endCursor }
                }
            }
        """
        variables = {
            "id": contract_id,
            "name": event_name,
            "first": first,
            "after": after,
        }
        data = await self.graphql(query, variables)

        events = data["events"]
        nodes = [edge["node"] for edge in events["edges"]]
        page_info = events["pageInfo"]

        return {
            "nodes": nodes,
            "end_cursor": page_info["endCursor"],
            "has_next_page": page_info["hasNextPage"],
        }

    async def paginate_events(
        self,
        contract_id: str,
        page_size: int = 100,
        event_name: str | None = None,
    ) -> AsyncGenerator[EventRecord, None]:
        """
        Async iterator over all of a contract's events via GraphQL cursor pagination.

        Args:
            contract_id: Contract ID
            page_size: Number of events per page
            event_name: Filter by event name

        Yields:
            Event records one at a time
        """
        after: str | None = None
        while True:
            page = await self.events_page(
                contract_id, first=page_size, after=after, event_name=event_name
            )
            for node in page["nodes"]:
                yield node

            if not page["has_next_page"] or not page["end_cursor"]:
                break

            after = page["end_cursor"]

    # ---- Internal HTTP Methods ----

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Internal GET request."""
        url = self.base_url + path
        if params:
            # Filter out None values
            filtered = {k: v for k, v in params.items() if v is not None}
            if filtered:
                url += "?" + urlencode(filtered)
        return await self._request("GET", url)

    async def _post(self, path: str, body: Any) -> Any:
        """Internal POST request."""
        return await self._request("POST", self.base_url + path, json=body)

    async def _request(self, method: str, url: str, **kwargs) -> Any:
        """
        Core request wrapper with retry + timeout.

        Retry policy:
        - Network errors are always retried
        - HTTP 429: honors Retry-After before retrying
        - HTTP 502/503/504: retried with exponential backoff + jitter
        - Any other non-2xx: thrown immediately as LumenqraphError
        """
        max_retries = self.retry["max_retries"]
        base_delay_ms = self.retry["base_delay_ms"]
        max_delay_ms = self.retry["max_delay_ms"]
        attempt = 0

        headers = kwargs.get("headers", {})
        if self.api_key:
            headers["x-api-key"] = self.api_key
        kwargs["headers"] = headers

        while True:
            client = self._get_client()
            try:
                response = await client.request(method, url, **kwargs)
                text = response.text

                # Parse JSON if present
                try:
                    parsed = response.json() if text else None
                except Exception:
                    parsed = text

                if not response.is_success:
                    # Check if retryable
                    if response.status_code in RETRYABLE_STATUSES and attempt < max_retries:
                        wait_ms = self._get_retry_delay(
                            response, attempt, base_delay_ms, max_delay_ms
                        )
                        await asyncio.sleep(wait_ms / 1000.0)
                        attempt += 1
                        continue

                    # Non-retryable error
                    message = (
                        parsed.get("error", f"{response.status_code} {response.reason_phrase}")
                        if isinstance(parsed, dict)
                        else f"{response.status_code} {response.reason_phrase}"
                    )
                    raise LumenqraphError(message, response.status_code, parsed or text)

                return parsed

            except (httpx.RequestError, httpx.TimeoutException) as e:
                # Network error or timeout
                if attempt < max_retries:
                    wait_ms = _jittered_delay(attempt, base_delay_ms, max_delay_ms)
                    await asyncio.sleep(wait_ms / 1000.0)
                    attempt += 1
                    continue
                raise

    def _get_retry_delay(
        self,
        response: httpx.Response,
        attempt: int,
        base_delay_ms: int,
        max_delay_ms: int,
    ) -> float:
        """Get retry delay in milliseconds, honoring Retry-After header."""
        retry_after = response.headers.get("retry-after")
        if retry_after:
            # Try parsing as seconds
            try:
                seconds = int(retry_after)
                if seconds >= 0:
                    return seconds * 1000.0
            except ValueError:
                # Try parsing as HTTP date
                try:
                    from email.utils import parsedate_to_datetime

                    date = parsedate_to_datetime(retry_after)
                    delta = (date - parsedate_to_datetime(time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()))).total_seconds()
                    if delta > 0:
                        return delta * 1000.0
                except Exception:
                    pass

        return _jittered_delay(attempt, base_delay_ms, max_delay_ms)


def _jittered_delay(attempt: int, base_ms: int, max_ms: int) -> float:
    """Exponential backoff with full jitter."""
    cap = min(max_ms, base_ms * (2**attempt))
    return random.random() * cap


def _enc(segment: str) -> str:
    """URL-encode a path segment."""
    return quote(segment, safe="")
