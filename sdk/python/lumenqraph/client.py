"""Lumenqraph REST API client."""

import time
import json
from typing import Any, Dict, List, Optional, Iterator, TypedDict
from dataclasses import dataclass, field, asdict
from urllib.parse import urljoin, urlencode
import urllib.request
import urllib.error


class RetryOptions(TypedDict, total=False):
    """Retry policy configuration."""
    max_retries: int
    base_delay_ms: int
    max_delay_ms: int
    timeout_ms: int


class ClientOptions(TypedDict, total=False):
    """Client configuration options."""
    base_url: str
    api_key: Optional[str]
    retry: Optional[RetryOptions]


@dataclass
class Contract:
    """A contract indexed by Lumenqraph."""
    contract_id: str
    event_count: int
    first_seen_ledger: Optional[int] = None
    last_seen_ledger: Optional[int] = None


@dataclass
class EventRecord:
    """A single event emitted by a contract."""
    event_id: str
    contract_id: str
    ledger: int
    ledger_closed_at: str
    event_type: str
    topics: List[str]
    decoded_topics: Any
    event_name: Optional[str]
    value: str
    decoded_value: Any
    enriched: Optional[Any] = None
    tx_hash: str = ""
    in_successful_call: bool = False
    paging_token: str = ""
    created_at: str = ""


@dataclass
class Transfer:
    """A SEP-41 token transfer."""
    event_id: str
    contract_id: str
    from_addr: Optional[str]
    to_addr: Optional[str]
    amount: str
    ledger: int
    ledger_closed_at: str
    kind: str = ""


@dataclass
class ContractState:
    """Contract instance storage snapshots."""
    contract_id: str
    count: int
    versions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ContractData:
    """Contract per-key data snapshots."""
    contract_id: str
    count: int
    keys: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CallResult:
    """Result of a contract call or simulation."""
    contract_id: str
    function: str
    result: Any
    simulated_at_ledger: int
    events: Optional[List[Any]] = None
    min_resource_fee: Optional[str] = None


class LumenqraphError(Exception):
    """Error thrown for any non-2xx API response."""
    def __init__(self, message: str, status: int, body: Any):
        super().__init__(message)
        self.status = status
        self.body = body


class LumenqraphClient:
    """Lumenqraph API client with retry and timeout logic."""

    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BASE_DELAY_MS = 250
    DEFAULT_MAX_DELAY_MS = 30_000
    DEFAULT_TIMEOUT_MS = 10_000
    RETRYABLE_STATUSES = {429, 502, 503, 504}

    def __init__(self, base_url: str, api_key: Optional[str] = None,
                 retry: Optional[RetryOptions] = None):
        """Initialize the Lumenqraph client.

        Args:
            base_url: Base URL of the Lumenqraph API (e.g. http://localhost:8080)
            api_key: Optional API key for authenticated requests
            retry: Optional retry configuration
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.retry = self._merge_retry_options(retry or {})

    def _merge_retry_options(self, opts: RetryOptions) -> Dict[str, int]:
        """Merge provided options with defaults."""
        return {
            "max_retries": opts.get("max_retries", self.DEFAULT_MAX_RETRIES),
            "base_delay_ms": opts.get("base_delay_ms", self.DEFAULT_BASE_DELAY_MS),
            "max_delay_ms": opts.get("max_delay_ms", self.DEFAULT_MAX_DELAY_MS),
            "timeout_ms": opts.get("timeout_ms", self.DEFAULT_TIMEOUT_MS),
        }

    def _make_request(self, method: str, path: str, query: Optional[Dict[str, Any]] = None,
                      body: Optional[Dict[str, Any]] = None) -> Any:
        """Make an HTTP request with retry logic."""
        url = urljoin(self.base_url, path)
        if query:
            # Filter out None values and encode
            query = {k: v for k, v in query.items() if v is not None}
            if query:
                url = f"{url}?{urlencode(query)}"

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key

        data = None
        if body:
            data = json.dumps(body).encode("utf-8")

        attempt = 0
        max_retries = self.retry["max_retries"]

        while True:
            try:
                req = urllib.request.Request(url, data=data, headers=headers, method=method)
                timeout = self.retry["timeout_ms"] / 1000.0

                try:
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        response_data = response.read().decode("utf-8")
                        return json.loads(response_data) if response_data else None
                except urllib.error.HTTPError as e:
                    status = e.code
                    response_text = e.read().decode("utf-8")
                    try:
                        body_obj = json.loads(response_text)
                    except json.JSONDecodeError:
                        body_obj = response_text

                    if status not in self.RETRYABLE_STATUSES or attempt >= max_retries:
                        raise LumenqraphError(
                            f"HTTP {status}: {response_text}",
                            status,
                            body_obj
                        )

                    # Retry with exponential backoff
                    delay_ms = min(
                        self.retry["base_delay_ms"] * (2 ** attempt),
                        self.retry["max_delay_ms"]
                    )
                    time.sleep(delay_ms / 1000.0)
                    attempt += 1

            except urllib.error.URLError as e:
                if attempt >= max_retries:
                    raise LumenqraphError(f"Network error: {e}", 0, None)
                delay_ms = min(
                    self.retry["base_delay_ms"] * (2 ** attempt),
                    self.retry["max_delay_ms"]
                )
                time.sleep(delay_ms / 1000.0)
                attempt += 1

    def _get(self, path: str, query: Optional[Dict[str, Any]] = None) -> Any:
        """Make a GET request."""
        return self._make_request("GET", path, query)

    def _post(self, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        """Make a POST request."""
        return self._make_request("POST", path, body=body)

    def health(self) -> Dict[str, Any]:
        """Get health and indexing-lag report."""
        return self._get("/health")

    def list_contracts(self, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        """Get contracts with pagination.

        Args:
            limit: Maximum number of contracts to return (default 200, max 500)
            offset: Number of contracts to skip (default 0)

        Returns:
            Response with data (list of contracts) and has_more flag
        """
        return self._get("/contracts", {"limit": limit, "offset": offset})

    def get_interface(self, contract_id: str, version: Optional[int] = None) -> Dict[str, Any]:
        """Get a contract's decoded on-chain interface."""
        query = {}
        if version is not None:
            query["version"] = version
        return self._get(f"/contracts/{contract_id}/interface", query)

    def get_state(self, contract_id: str, limit: int = 1) -> ContractState:
        """Get versioned instance-storage snapshots."""
        result = self._get(f"/contracts/{contract_id}/state", {"limit": limit})
        return ContractState(**result)

    def get_data(self, contract_id: str, label: Optional[str] = None,
                 limit: int = 100) -> ContractData:
        """Get latest value of every per-key entry."""
        query = {}
        if label:
            query["label"] = label
        query["limit"] = limit
        result = self._get(f"/contracts/{contract_id}/data", query)
        return ContractData(**result)

    def get_data_key(self, contract_id: str, key_hash: str,
                     limit: int = 50) -> Dict[str, Any]:
        """Get version history of a single per-key entry."""
        return self._get(f"/contracts/{contract_id}/data/{key_hash}", {"limit": limit})

    def list_events(self, contract_id: str, limit: int = 50, offset: int = 0,
                    event_name: Optional[str] = None, after: Optional[str] = None) -> Dict[str, Any]:
        """Get recent events for a contract."""
        query = {"limit": limit}
        if offset:
            query["offset"] = offset
        if event_name:
            query["event_name"] = event_name
        if after:
            query["after"] = after
        return self._get(f"/contracts/{contract_id}/events", query)

    def list_transfers(self, contract_id: Optional[str] = None, limit: int = 50,
                       offset: int = 0) -> Dict[str, Any]:
        """Get materialized SEP-41 transfers."""
        path = f"/contracts/{contract_id}/transfers" if contract_id else "/transfers"
        return self._get(path, {"limit": limit, "offset": offset})

    def list_functions(self, contract_id: str) -> Dict[str, Any]:
        """Get a contract's callable view functions."""
        return self._get(f"/contracts/{contract_id}/functions")

    def call(self, contract_id: str, function: str, args: Optional[Any] = None,
             source_account: Optional[str] = None) -> CallResult:
        """Invoke a view function read-only."""
        body = {
            "function": function,
            "args": args,
            "source_account": source_account,
        }
        result = self._post(f"/contracts/{contract_id}/call", body)
        return CallResult(**result)

    def simulate(self, contract_id: str, function: str, args: Optional[Any] = None,
                 source_account: Optional[str] = None) -> CallResult:
        """Dry-run any call and preview its result, events, and cost."""
        body = {
            "function": function,
            "args": args,
            "source_account": source_account,
        }
        result = self._post(f"/contracts/{contract_id}/simulate", body)
        return CallResult(**result)

    def paginate_events(self, contract_id: str, event_name: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """Paginate through all events for a contract using cursor pagination.

        Args:
            contract_id: The contract ID to fetch events for
            event_name: Optional event name filter

        Yields:
            EventRecord dictionaries
        """
        cursor = None
        while True:
            response = self.list_events(contract_id, limit=1000, event_name=event_name, after=cursor)
            data = response.get("data", [])

            for event in data:
                yield event

            if not response.get("has_more", False):
                break

            # Use the last event as the cursor for the next page
            if data:
                last_event = data[-1]
                # For cursor pagination, we'd use the next_cursor if available
                # For now, use offset-based pagination fallback
                cursor = response.get("next_cursor")
                if not cursor:
                    break
