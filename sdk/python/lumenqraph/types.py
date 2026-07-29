"""Type definitions mirroring the Lumenqraph REST + GraphQL API responses."""

from typing import Any, Generic, TypeVar, TypedDict
from typing_extensions import NotRequired

Json = Any  # Type alias for arbitrary JSON-serializable data

T = TypeVar("T")


class Contract(TypedDict):
    """A contract tracked by the indexer."""

    contract_id: str
    event_count: int
    first_seen_ledger: NotRequired[int | None]
    last_seen_ledger: NotRequired[int | None]


class EventRecord(TypedDict):
    """A decoded contract event."""

    event_id: str
    contract_id: str
    ledger: int
    ledger_closed_at: str
    event_type: str
    topics: list[str]
    decoded_topics: Json
    event_name: str | None
    value: str
    decoded_value: Json
    enriched: Json | None  # Named, typed record from the contract's on-chain spec
    tx_hash: str
    in_successful_call: bool
    paging_token: str
    created_at: str


class Transfer(TypedDict):
    """A materialized SEP-41 transfer."""

    event_id: str
    contract_id: str
    from_addr: str | None
    to_addr: str | None
    amount: str
    ledger: int
    ledger_closed_at: str


class StateVersion(TypedDict):
    """A versioned instance-storage snapshot."""

    ledger: int
    storage: Json
    captured_at: str


class ContractState(TypedDict):
    """Versioned instance-storage snapshots for a contract."""

    contract_id: str
    count: int
    versions: list[StateVersion]


class DataKey(TypedDict):
    """A per-key entry (e.g., holder balance)."""

    key_hash: str
    key: Json
    durability: str
    ledger: int
    value: Json
    label: str | None
    captured_at: str


class ContractData(TypedDict):
    """Latest value of every per-key entry for a contract."""

    contract_id: str
    count: int
    keys: list[DataKey]


class DataKeyVersion(TypedDict):
    """A single version of a data key."""

    ledger: int
    value: Json
    captured_at: str


class DataKeyHistory(TypedDict):
    """Version history of a single per-key entry."""

    contract_id: str
    key_hash: str
    key: Json
    durability: str
    label: str | None
    count: int
    versions: list[DataKeyVersion]


class CallResult(TypedDict):
    """Result of a contract call or simulation."""

    contract_id: str
    function: str
    result: Json
    simulated_at_ledger: int
    events: NotRequired[list[Json]]  # Present for simulate
    min_resource_fee: NotRequired[str]  # Present for simulate


class CallOptions(TypedDict, total=False):
    """Options for calling or simulating a contract function."""

    function: str  # Required
    args: Json  # Arguments: object keyed by parameter name, or positional array
    source_account: str  # Optional G… source account for simulation


class Page(TypedDict, Generic[T]):
    """A Relay-style cursor page."""

    nodes: list[T]
    end_cursor: str | None
    has_next_page: bool


class RetryOptions(TypedDict, total=False):
    """Retry policy for API requests."""

    max_retries: int  # Maximum number of retry attempts after first failure (default: 3)
    base_delay_ms: int  # Base delay in ms for first retry (default: 250)
    max_delay_ms: int  # Hard cap on computed delay in ms (default: 30000)
    timeout_ms: int  # Per-request timeout in ms (default: 10000)


class ClientOptions(TypedDict, total=False):
    """Configuration options for LumenqraphClient."""

    base_url: str  # Required: Base URL of the Lumenqraph API
    api_key: str  # API key sent as x-api-key header
    retry: RetryOptions  # Retry/timeout policy
