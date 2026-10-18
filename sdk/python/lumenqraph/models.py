# Generated from openapi.yaml by scripts/codegen.py. DO NOT EDIT.
# Regenerate with `python scripts/codegen.py`; CI checks for drift.

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import AnyUrl, AwareDatetime, BaseModel, Field, RootModel


class Contract(BaseModel):
    contract_id: str
    """
    Bech32m `C…` contract address.
    """
    event_count: int
    first_seen_ledger: int | None = None
    last_seen_ledger: int | None = None


class EventRecord(BaseModel):
    event_id: str
    contract_id: str
    ledger: int
    ledger_closed_at: AwareDatetime
    event_type: Literal['contract', 'system', 'diagnostic']
    topics: list[str]
    """
    Raw base64 XDR topics.
    """
    decoded_topics: Any
    """
    Decoded topics as JSON values.
    """
    event_name: str | None = None
    """
    Best-effort event name (first topic decoded as a symbol).
    """
    value: str
    """
    Raw base64 XDR event body.
    """
    decoded_value: Any
    """
    Decoded event body as a JSON value.
    """
    enriched: Any | None = None
    """
    Named, typed record built from the contract's on-chain spec.
    `null` when no spec is available for this event.

    """
    tx_hash: str
    in_successful_call: bool
    paging_token: str
    created_at: AwareDatetime


class Transfer(BaseModel):
    event_id: str
    contract_id: str
    from_addr: str | None = None
    to_addr: str | None = None
    amount: str
    """
    Token amount as a decimal string (i128 exceeds JSON integer range).
    """
    ledger: int
    ledger_closed_at: AwareDatetime


class StateVersion(BaseModel):
    ledger: int
    storage: Any
    """
    Decoded contract instance storage as a JSON value.
    """
    captured_at: AwareDatetime


class ContractState(BaseModel):
    contract_id: str
    count: int
    versions: list[StateVersion]


class DataKey(BaseModel):
    key_hash: str
    key: Any
    """
    The storage key as a JSON value.
    """
    durability: Literal['persistent', 'temporary']
    ledger: int
    value: Any
    """
    The decoded value as a JSON value.
    """
    label: str | None = None
    captured_at: AwareDatetime


class ContractData(BaseModel):
    contract_id: str
    count: int
    keys: list[DataKey]


class DataKeyVersionEntry(BaseModel):
    ledger: int
    value: Any
    """
    The decoded value at this ledger.
    """
    captured_at: AwareDatetime


class DataKeyHistory(BaseModel):
    contract_id: str
    key_hash: str
    key: Any
    """
    The storage key as a JSON value.
    """
    durability: Literal['persistent', 'temporary']
    label: str | None = None
    count: int
    versions: list[DataKeyVersionEntry]


class CallRequest(BaseModel):
    function: str
    args: Any | None = None
    """
    Arguments by name (object) or as a positional array.
    """
    source_account: str | None = None
    """
    Optional `G…` source account for the simulation.
    """


class CallResult(BaseModel):
    contract_id: str
    function: str
    result: Any
    """
    Typed result decoded from the contract's on-chain spec.
    """
    simulated_at_ledger: int
    events: list[Any] | None = None
    """
    Present for `simulate` — the events the call would emit.
    """
    min_resource_fee: str | None = None
    """
    Present for `simulate` — minimum resource fee in stroops.
    """


class WebhookKind(RootModel[Literal['event', 'upgrade']]):
    root: Literal['event', 'upgrade']
    """
    `event` — fires when a matching contract event is indexed.
    `upgrade` — fires when the contract's on-chain interface changes.

    """


class CreateWebhookRequest(BaseModel):
    url: AnyUrl
    kind: WebhookKind | None = None
    contract_id: str | None = None
    event_name: str | None = None


class WebhookSubscription(BaseModel):
    id: UUID
    url: AnyUrl
    kind: WebhookKind
    contract_id: str | None = None
    event_name: str | None = None
    active: bool
    created_at: AwareDatetime


class WebhookCreated(WebhookSubscription):
    secret: str
    """
    HMAC-SHA256 signing secret. Returned only at creation — store it
    securely; it cannot be retrieved later. Use `verifyWebhook()` in
    the SDK to verify deliveries.

    """


class HealthResponse(BaseModel):
    status: Literal['ok', 'lagging']
    last_processed_ledger: int
    chain_tip_ledger: int
    lag_ledgers: int
    events_ingested_total: int | None = None
    errors_total: int | None = None
    updated_at: AwareDatetime | None = None


class ErrorResponse(BaseModel):
    error: str


class PageMeta(BaseModel):
    has_more: bool
    """
    Whether more rows are available after this page.
    """
    next_cursor: str | None = Field(...)
    """
    Opaque cursor to pass as `after` for the next page. Null on the last page.
    """


class EventsPage(PageMeta):
    data: list[EventRecord]


class TransfersPage(PageMeta):
    data: list[Transfer]


class MaterializedPage(PageMeta):
    """
    Page of materialized swap, NFT or liquidity rows.
    """

    data: list[dict[str, Any]]


class WebhookDelivery(BaseModel):
    id: int
    status: Literal['pending', 'delivered', 'failed']
    attempts: int
    last_error: str | None = None
    delivered_at: AwareDatetime | None = None
    created_at: AwareDatetime


class DeliverySummary(BaseModel):
    total: int
    delivered: int
    failed: int
    pending: int


class DeliveriesPage(PageMeta):
    data: list[WebhookDelivery]
    summary: DeliverySummary | None = None
