"""Lumenqraph REST + GraphQL API client (synchronous).

Every REST resource the API exposes has a method here; see the parity table in
``sdk/python/README.md``. Request/response models generated from
``openapi.yaml`` live in :mod:`lumenqraph.models`.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict, TypeVar, cast

from ._http import (
    EVENTS_PAGE_QUERY,
    LumenqraphError,
    RetryOptions,
    Transport,
    build_headers,
    build_url,
    drop_none,
    enc,
    encode_json,
    events_page_from_graphql,
    graphql_data,
    page_query,
    parse_sse,
    resolve_retry,
)

__all__ = [
    "LumenqraphClient",
    "LumenqraphError",
    "ClientOptions",
    "RetryOptions",
    "Contract",
    "EventRecord",
    "Transfer",
    "ContractState",
    "ContractData",
    "CallResult",
    "ledger_key_hash",
]

JsonDict = dict[str, Any]
WebhookKind = Literal["event", "upgrade"]
SdkLang = Literal["ts", "python", "rust"]
Durability = Literal["persistent", "temporary"]

# XDR discriminants for the LedgerKey union (Stellar XDR, LedgerEntryType).
_LEDGER_ENTRY_TYPE_CONTRACT_DATA = 6
# XDR discriminants for the ContractDataDurability union.
_DURABILITY_CODES: dict[str, int] = {"temporary": 0, "persistent": 1}
# XDR discriminants for the ScVal union (ScValType).
_SCVAL_CODES: dict[str, int] = {
    "scv_bool": 0,
    "scv_void": 1,
    "scv_error": 2,
    "scv_u32": 3,
    "scv_i32": 4,
    "scv_u64": 5,
    "scv_i64": 6,
    "scv_timepoint": 7,
    "scv_duration": 8,
    "scv_u128": 9,
    "scv_i128": 10,
    "scv_u256": 11,
    "scv_i256": 12,
    "scv_bytes": 13,
    "scv_string": 14,
    "scv_symbol": 15,
    "scv_vec": 16,
    "scv_map": 17,
    "scv_address": 18,
    "scv_contract_instance": 19,
    "scv_ledger_key_contract_instance": 20,
    "scv_ledger_key_nonce": 21,
    "scv_ledger_key_ttl": 22,
}
# XDR discriminants for the SCAddress union.
_SCADDRESS_CODES: dict[str, int] = {"sc_address_account": 0, "sc_address_contract": 1}
# XDR discriminants for the PublicKey union.
_PUBLIC_KEY_CODES: dict[str, int] = {"public_key_type_ed25519": 0}
# XDR discriminants for the CryptoKeyType union.
_CRYPTO_KEY_TYPE_ED25519 = 0
# XDR discriminants for the ScErrorType / ScErrorCode unions.
_SCERROR_TYPE_CODES: dict[str, int] = {
    "sce_contract": 0,
    "sce_wASM_vm": 1,
    "sce_context": 2,
    "sce_storage": 3,
    "sce_object": 4,
    "sce_crypto": 5,
    "sce_events": 6,
    "sce_budget": 7,
    "sce_value": 8,
    "sce_auth": 9,
}
_SCERROR_CODE_CODES: dict[str, int] = {
    "scec_arith_domain": 0,
    "scec_index_bounds": 1,
    "scec_invalid_input": 2,
    "scec_missing_value": 3,
    "scec_existing_value": 4,
    "scec_exceeded_limit": 5,
    "scec_invalid_action": 6,
    "scec_internal_error": 7,
    "scec_unexpected_type": 8,
    "scec_unexpected_size": 9,
    "scec_out_of_memory": 10,
}
# XDR discriminants for the ScSpecType union (used by ScSpecTypeDef).
_SCSPEC_TYPE_CODES: dict[str, int] = {
    "scspec_type_val": 0,
    "scspec_type_bool": 1,
    "scspec_type_void": 2,
    "scspec_type_error": 3,
    "scspec_type_u32": 4,
    "scspec_type_i32": 5,
    "scspec_type_u64": 6,
    "scspec_type_i64": 7,
    "scspec_type_timepoint": 8,
    "scspec_type_duration": 9,
    "scspec_type_u128": 10,
    "scspec_type_i128": 11,
    "scspec_type_u256": 12,
    "scspec_type_i256": 13,
    "scspec_type_bytes": 14,
    "scspec_type_string": 15,
    "scspec_type_symbol": 16,
    "scspec_type_address": 17,
    "scspec_type_option": 18,
    "scspec_type_result": 19,
    "scspec_type_vec": 20,
    "scspec_type_map": 21,
    "scspec_type_tuple": 22,
    "scspec_type_bytes_n": 23,
    "scspec_type_udt": 24,
}


def _xdr_uint(value: int, size: int) -> bytes:
    return int(value).to_bytes(size, "big", signed=False)


def _xdr_int(value: int, size: int) -> bytes:
    return int(value).to_bytes(size, "big", signed=True)


def _xdr_var_opaque(data: bytes) -> bytes:
    """Variable-length opaque: 4-byte length prefix, bytes, zero pad to 4."""
    pad = (-len(data)) % 4
    return _xdr_uint(len(data), 4) + data + b"\x00" * pad


def _xdr_string(text: str) -> bytes:
    return _xdr_var_opaque(text.encode("utf-8"))


def _xdr_scaddress(address: str) -> bytes:
    """Encode an SCAddress from its strkey form (``G...`` account or ``C...`` contract)."""
    if not isinstance(address, str) or len(address) < 2:
        raise ValueError(f"invalid Stellar address: {address!r}")
    try:
        raw = base64.b32decode(address + "=" * ((-len(address)) % 8), casefold=True)
    except Exception as exc:  # noqa: BLE001 - surface as ValueError
        raise ValueError(f"invalid Stellar address: {address!r}") from exc
    if len(raw) != 35:
        raise ValueError(f"invalid Stellar address: {address!r}")
    version, payload, checksum = raw[0], raw[1:33], raw[33:]
    expected = hashlib.crc32(version.to_bytes(1, "big") + payload) if False else None
    # CRC16-XModem over version+payload, little-endian, as used by strkey.
    crc = 0
    for byte in version.to_bytes(1, "big") + payload:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    if checksum != crc.to_bytes(2, "little"):
        raise ValueError(f"invalid Stellar address checksum: {address!r}")
    del expected
    if version == 6 << 3:  # 'G' account
        return _xdr_uint(_SCADDRESS_CODES["sc_address_account"], 4) + _xdr_uint(
            _PUBLIC_KEY_CODES["public_key_type_ed25519"], 4
        ) + payload
    if version == 2 << 3:  # 'C' contract
        return _xdr_uint(_SCADDRESS_CODES["sc_address_contract"], 4) + payload
    raise ValueError(f"unsupported Stellar address version: {address!r}")


def _xdr_scval(value: Any) -> bytes:
    """Encode an ScVal from its JSON form (the shape the Lumenqraph API returns).

    Accepts either a tagged object (``{"type": "u32", "value": 7}``) or a
    base64 XDR string (``"AAAA..."``) which is decoded and re-encoded verbatim.
    """
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        # Assume a base64-encoded ScVal XDR blob.
        try:
            return base64.b64decode(value, validate=True)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"invalid base64 ScVal XDR: {value!r}") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"unsupported ScVal: {value!r}")
    tag = value.get("type") or value.get("kind")
    if tag is None:
        raise ValueError(f"ScVal object missing 'type': {value!r}")
    tag = str(tag).lower()
    if tag.startswith("scv_"):
        code = _SCVAL_CODES[tag]
    else:
        code = _SCVAL_CODES[f"scv_{tag}"]
    inner = value.get("value")
    out = _xdr_uint(code, 4)
    if tag in ("scv_bool", "bool"):
        return out + _xdr_uint(1 if inner else 0, 4)
    if tag in ("scv_void", "void"):
        return out
    if tag in ("scv_u32", "u32"):
        return out + _xdr_uint(int(inner), 4)
    if tag in ("scv_i32", "i32"):
        return out + _xdr_int(int(inner), 4)
    if tag in ("scv_u64", "u64", "scv_timepoint", "timepoint", "scv_duration", "duration"):
        return out + _xdr_uint(int(inner), 8)
    if tag in ("scv_i64", "i64"):
        return out + _xdr_int(int(inner), 8)
    if tag in ("scv_u128", "u128", "scv_u256", "u256"):
        size = 16 if "128" in tag else 32
        return out + _xdr_uint(int(inner), size)
    if tag in ("scv_i128", "i128", "scv_i256", "i256"):
        size = 16 if "128" in tag else 32
        return out + _xdr_int(int(inner), size)
    if tag in ("scv_bytes", "bytes"):
        return out + _xdr_var_opaque(base64.b64decode(inner) if isinstance(inner, str) else bytes(inner))
    if tag in ("scv_string", "string", "scv_symbol", "symbol"):
        return out + _xdr_string(str(inner))
    if tag in ("scv_address", "address"):
        return out + _xdr_scaddress(str(inner))
    if tag in ("scv_vec", "vec"):
        items = inner or []
        return out + _xdr_uint(len(items), 4) + b"".join(_xdr_scval(i) for i in items)
    if tag in ("scv_map", "map"):
        entries = inner or []
        encoded = b""
        for entry in entries:
            if isinstance(entry, Mapping):
                k, v = entry.get("key"), entry.get("value")
            else:
                k, v = entry[0], entry[1]
            encoded += _xdr_scval(k) + _xdr_scval(v)
        return out + _xdr_uint(len(entries), 4) + encoded
    if tag in ("scv_error", "error"):
        if isinstance(inner, Mapping):
            etype = str(inner.get("type", "sce_context")).lower()
            ecode = str(inner.get("code", "scec_internal_error")).lower()
        else:
            etype, ecode = "sce_context", "scec_internal_error"
        return (
            out
            + _xdr_uint(_SCERROR_TYPE_CODES[etype], 4)
            + _xdr_uint(_SCERROR_CODE_CODES[ecode], 4)
        )
    raise ValueError(f"unsupported ScVal type: {tag!r}")


def _xdr_ledger_key_contract_data(contract_id: str, key_sc_val: Any, durability: str) -> bytes:
    """Encode ``LedgerKey::ContractData { contract, key, durability }`` as XDR."""
    if durability not in _DURABILITY_CODES:
        raise ValueError(f"durability must be 'persistent' or 'temporary', got {durability!r}")
    return (
        _xdr_uint(_LEDGER_ENTRY_TYPE_CONTRACT_DATA, 4)
        + _xdr_scaddress(contract_id)
        + _xdr_scval(key_sc_val)
        + _xdr_uint(_DURABILITY_CODES[durability], 4)
    )


def ledger_key_hash(contract_id: str, key_sc_val: Any, durability: Durability) -> str:
    """Compute the canonical ledger-key hash for a contract-data entry.

    Returns ``hex(sha256(LedgerKey::ContractData{contract, key, durability}.to_xdr()))``,
    matching the value the Lumenqraph server stores as ``ledger_key_hash`` and
    exposes as the ``:key_hash`` path parameter on ``/contracts/:id/data/:key_hash``.

    Args:
        contract_id: The contract's strkey (``C...``).
        key_sc_val: The storage key as an ScVal — either a tagged JSON object
            (``{"type": "symbol", "value": "Balance"}``) or a base64 XDR string.
        durability: ``"persistent"`` or ``"temporary"``.

    Returns:
        Lowercase hex SHA-256 digest (64 characters).
    """
    xdr = _xdr_ledger_key_contract_data(contract_id, key_sc_val, durability)
    return hashlib.sha256(xdr).hexdigest()


class ClientOptions(TypedDict, total=False):
    """Client configuration options."""

    base_url: str
    api_key: str | None
    retry: RetryOptions | None


@dataclass
class Contract:
    """A contract indexed by Lumenqraph."""

    contract_id: str
    event_count: int
    first_seen_ledger: int | None = None
    last_seen_ledger: int | None = None


@dataclass
class EventRecord:
    """A single event emitted by a contract."""

    event_id: str
    contract_id: str
    ledger: int
    ledger_closed_at: str
    event_type: str
    topics: list[str]
    decoded_topics: Any
    event_name: str | None
    value: str
    decoded_value: Any
    enriched: Any | None = None
    tx_hash: str = ""
    in_successful_call: bool = False
    paging_token: str = ""
    created_at: str = ""


@dataclass
class Transfer:
    """A SEP-41 token transfer."""

    event_id: str
    contract_id: str
    from_addr: str | None
    to_addr: str | None
    amount: str
    ledger: int
    ledger_closed_at: str
    kind: str = ""


@dataclass
class ContractState:
    """Contract instance storage snapshots."""

    contract_id: str
    count: int
    versions: list[JsonDict] = field(default_factory=list)


@dataclass
class ContractData:
    """Contract per-key data snapshots.

    Each entry in ``keys`` carries a ``ledger_key_hash`` (the canonical
    ``hex(sha256(LedgerKey XDR))``) and, for one release, a deprecated
    ``key_hash`` alias with the same value.
    """

    contract_id: str
    count: int
    keys: list[JsonDict] = field(default_factory=list)


@dataclass
class CallResult:
    """Result of a contract call or simulation."""

    contract_id: str
    function: str
    result: Any
    simulated_at_ledger: int
    events: list[Any] | None = None
    min_resource_fee: str | None = None


_D = TypeVar("_D")


def _from_dict(cls: type[_D], data: Mapping[str, Any]) -> _D:
    """Build a dataclass from a response, ignoring fields it doesn't declare
    so a server adding a field never breaks older SDK versions."""
    names = {f.name for f in dataclasses.fields(cast(Any, cls))}
    return cls(**{k: v for k, v in data.items() if k in names})


def _call_body(function: str, args: Any, source_account: str | None) -> JsonDict:
    return {"function": function, "args": args, "source_account": source_account}


def _stream_query(event_name: str | None, cursor: str | int | None) -> tuple[JsonDict, str | None]:
    """Map a stream cursor to query params; a ``ledger:event_id`` cursor
    resumes within a ledger and doubles as the initial ``Last-Event-ID``."""
    query: JsonDict = {"event_name": event_name}
    last_event_id: str | None = None
    if cursor is not None:
        text = str(cursor)
        ledger, sep, event_id = text.partition(":")
        if sep and ledger.isdigit():
            query["cursor"] = ledger
            query["cursor_event_id"] = event_id
            last_event_id = text
        else:
            query["cursor"] = text
    return query, last_event_id


class LumenqraphClient:
    """Lumenqraph API client with retry and timeout logic."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        retry: RetryOptions | None = None,
    ) -> None:
        """Initialize the Lumenqraph client.

        Args:
            base_url: Base URL of the Lumenqraph API (e.g. http://localhost:8080)
            api_key: Optional API key, sent as ``x-api-key``
            retry: Optional retry / timeout policy
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.retry = resolve_retry(retry)
        self._transport = Transport(self.retry)

    # ---- Core request machinery ----

    def _request(
        self,
        method: str,
        path: str,
        query: Mapping[str, Any] | None = None,
        body: Any = None,
        raw: bool = False,
    ) -> Any:
        url = build_url(self.base_url, path, query)
        accept = "text/plain, */*" if raw else "application/json"
        return self._transport.send(
            method, url, encode_json(body), build_headers(self.api_key, accept), raw=raw
        )

    def _get(self, path: str, query: Mapping[str, Any] | None = None) -> Any:
        return self._request("GET", path, query)

    def _post(self, path: str, body: Any = None, query: Mapping[str, Any] | None = None) -> Any:
        return self._request("POST", path, query, body)

    def _paginate(self, path: str, query: Mapping[str, Any]) -> Iterator[JsonDict]:
        """Follow ``next_cursor`` until ``has_more`` is false."""
        after: str | None = None
        while True:
            response = self._get(path, {**query, "after": after})
            yield from response.get("data", [])
            after = response.get("next_cursor")
            if not response.get("has_more", False) or not after:
                break

    # ---- Health ----

    def health(self) -> JsonDict:
        """Get health and indexing-lag report."""
        result: JsonDict = self._get("/health")
        return result

    def livez(self) -> Any:
        """Liveness probe: the process is up."""
        return self._get("/livez")

    def readyz(self) -> Any:
        """Readiness probe: the API can reach Postgres."""
        return self._get("/readyz")

    # ---- Contracts & interfaces ----

    def list_contracts(self, limit: int | None = None, after: str | None = None) -> JsonDict:
        """Contracts the indexer has seen, with per-contract event counts.

        Pass the response's ``next_cursor`` as ``after`` for the next page.
        """
        result: JsonDict = self._get("/contracts", {"limit": limit, "after": after})
        return result

    def paginate_contracts(self, page_size: int = 100) -> Iterator[JsonDict]:
        """Iterate over every indexed contract via cursor pagination."""
        return self._paginate("/contracts", {"limit": page_size})

    def get_interface(self, contract_id: str, version: int | None = None) -> JsonDict:
        """A contract's decoded on-chain interface, optionally at a historical version."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/interface", {"version": version})
        return result

    def get_interface_history(
        self, contract_id: str, limit: int | None = None, after: str | None = None
    ) -> JsonDict:
        """The version history of a contract's interface, with semantic diffs."""
        result: JsonDict = self._get(
            f"/contracts/{enc(contract_id)}/interface/history", {"limit": limit, "after": after}
        )
        return result

    def get_interface_diff(
        self, contract_id: str, from_version: int | None = None, to_version: int | None = None
    ) -> JsonDict:
        """The semantic diff between two interface versions (defaults: previous → newest)."""
        result: JsonDict = self._get(
            f"/contracts/{enc(contract_id)}/interface/diff", {"from": from_version, "to": to_version}
        )
        return result

    def refresh_contract(self, contract_id: str) -> JsonDict:
        """Re-read a contract's on-chain interface now instead of waiting for the indexer."""
        result: JsonDict = self._post(f"/contracts/{enc(contract_id)}/refresh")
        return result

    def generate_sdk(
        self, contract_id: str, lang: SdkLang | None = None, version: int | None = None
    ) -> str:
        """Generated client source code for a contract (``ts`` by default)."""
        result: str = self._request(
            "GET", f"/contracts/{enc(contract_id)}/sdk", {"lang": lang, "version": version}, raw=True
        )
        return result

    def get_stats(
        self,
        contract_id: str,
        bucket: str | None = None,
        resolution: str | None = None,
        window: str | None = None,
        group_by: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
        from_ledger: int | None = None,
        to_ledger: int | None = None,
    ) -> JsonDict:
        """Time-bucketed event statistics for a contract."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/stats", {
            "bucket": bucket,
            "resolution": resolution,
            "window": window,
            "group_by": group_by,
            "from": from_time,
            "to": to_time,
            "from_ledger": from_ledger,
            "to_ledger": to_ledger,
        })
        return result

    # ---- State & data ----

    def get_state(self, contract_id: str, limit: int = 1) -> ContractState:
        """Versioned instance-storage snapshots, newest first (``limit=1`` = current)."""
        result = self._get(f"/contracts/{enc(contract_id)}/state", {"limit": limit})
        return _from_dict(ContractState, result)

    def get_data(self, contract_id: str, label: str | None = None, limit: int = 100) -> ContractData:
        """Latest value of every per-key entry (e.g. holder balances)."""
        result = self._get(f"/contracts/{enc(contract_id)}/data", {"label": label, "limit": limit})
        return _from_dict(ContractData, result)

    def get_data_key(self, contract_id: str, ledger_key_hash: str, limit: int = 50) -> JsonDict:
        """Version history of a single per-key entry.

        ``ledger_key_hash`` is the canonical ``hex(sha256(LedgerKey XDR))`` value
        — compute it with :func:`ledger_key_hash`. The old ``key_hash`` name is
        accepted as a deprecated alias.
        """
        result: JsonDict = self._get(
            f"/contracts/{enc(contract_id)}/data/{enc(ledger_key_hash)}", {"limit": limit}
        )
        return result

    def get_data_key_by_scval(
        self,
        contract_id: str,
        key_sc_val: Any,
        durability: Durability,
        limit: int = 50,
    ) -> JsonDict:
        """Version history of a per-key entry identified by its ScVal + durability.

        Convenience wrapper that computes the ledger-key hash for you.
        """
        return self.get_data_key(
            contract_id, ledger_key_hash(contract_id, key_sc_val, durability), limit=limit
        )

    # ---- Events ----

    def list_events(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        event_name: str | None = None,
        after: str | None = None,
        from_ledger: int | None = None,
        to_ledger: int | None = None,
        since: str | None = None,
        until: str | None = None,
        topic0: str | None = None,
        topic1: str | None = None,
        topic2: str | None = None,
        topic3: str | None = None,
        param: str | None = None,
        successful_only: bool | None = None,
    ) -> JsonDict:
        """Recent events for a contract, newest first (cursor via ``after``)."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/events", page_query(
            limit, offset, after, {
                "event_name": event_name,
                "from_ledger": from_ledger,
                "to_ledger": to_ledger,
                "since": since,
                "until": until,
                "topic0": topic0,
                "topic1": topic1,
                "topic2": topic2,
                "topic3": topic3,
                "param": param,
                "successful_only": successful_only,
            }))
        return result

    def paginate_events(
        self, contract_id: str, event_name: str | None = None, page_size: int = 100,
        **filters: Any,
    ) -> Iterator[JsonDict]:
        """Iterate over all of a contract's events via REST cursor pagination.

        ``filters`` accepts the extra :meth:`list_events` filters (``from_ledger``,
        ``since``, ``topic0``, ``param``, …).
        """
        return self._paginate(
            f"/contracts/{enc(contract_id)}/events",
            {"limit": page_size, "event_name": event_name, **filters},
        )

    def get_event(self, event_id: str) -> JsonDict:
        """A single event by its unique id."""
        result: JsonDict = self._get(f"/events/{enc(event_id)}")
        return result

    def get_transaction_events(self, tx_hash: str, limit: int | None = None) -> JsonDict:
        """All indexed events emitted by a transaction, in emission order."""
        result: JsonDict = self._get(f"/transactions/{enc(tx_hash)}/events", {"limit": limit})
        return result

    def stream_events(
        self,
        contract_id: str,
        event_name: str | None = None,
        cursor: str | int | None = None,
        reconnect: bool = True,
    ) -> Iterator[JsonDict]:
        """Stream new events over Server-Sent Events, resuming from the last one seen.

        ``cursor`` is a ledger number or a ``ledger:event_id`` composite. On a
        dropped connection the stream reconnects with ``Last-Event-ID`` unless
        ``reconnect`` is false. Break out of the loop to stop.
        """
        query, last_event_id = _stream_query(event_name, cursor)
        url = build_url(self.base_url, f"/contracts/{enc(contract_id)}/events/stream", query)
        attempt = 0
        while True:
            headers = build_headers(self.api_key, "text/event-stream")
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            try:
                for frame in parse_sse(self._transport.stream_lines(url, headers)):
                    attempt = 0
                    if "id" in frame:
                        last_event_id = frame["id"] or None
                    if "data" not in frame:
                        continue
                    payload = json.loads(frame["data"])
                    event = payload.get("data", payload) if isinstance(payload, dict) else payload
                    if event:
                        yield event
            except LumenqraphError as exc:
                if not reconnect or exc.status not in (0, 429, 502, 503, 504):
                    raise
            if not reconnect:
                return
            time.sleep(min(1.0 * (2 ** attempt), 30.0))
            attempt += 1

    # ---- Materialized projections ----

    def list_transfers(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        from_addr: str | None = None,
        to_addr: str | None = None,
    ) -> JsonDict:
        """Materialized SEP-41 transfers for a contract, newest first.

        Returns ``{"data": [...], "has_more": bool, "next_cursor": str | None}``.
        Pass ``next_cursor`` as ``after`` for the next page; ``offset`` is
        deprecated and capped at 10,000 by the server.
        """
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/transfers", page_query(
            limit, offset, after, {"from": from_addr, "to": to_addr}))
        return result

    def list_swaps(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        sender: str | None = None,
        sell_token: str | None = None,
        buy_token: str | None = None,
    ) -> JsonDict:
        """Materialized AMM swaps, newest first (cursor-paginated)."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/swaps", page_query(
            limit, offset, after,
            {"sender": sender, "sell_token": sell_token, "buy_token": buy_token}))
        return result

    def list_nft_events(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        kind: Literal["mint", "transfer", "burn"] | None = None,
        from_addr: str | None = None,
        to_addr: str | None = None,
        token_id: str | None = None,
    ) -> JsonDict:
        """Materialized NFT mint/transfer/burn events, newest first (cursor-paginated)."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/nfts", page_query(
            limit, offset, after,
            {"kind": kind, "from": from_addr, "to": to_addr, "token_id": token_id}))
        return result

    def list_liquidity_events(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        kind: Literal["add", "remove"] | None = None,
        provider: str | None = None,
    ) -> JsonDict:
        """Materialized liquidity add/remove events, newest first (cursor-paginated)."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/liquidity", page_query(
            limit, offset, after, {"kind": kind, "provider": provider}))
        return result

    def paginate_transfers(
        self, contract_id: str, page_size: int = 100,
        from_addr: str | None = None, to_addr: str | None = None,
    ) -> Iterator[JsonDict]:
        """Iterate over all of a contract's transfers via cursor pagination."""
        return self._paginate(f"/contracts/{enc(contract_id)}/transfers",
                              {"limit": page_size, "from": from_addr, "to": to_addr})

    def paginate_swaps(
        self, contract_id: str, page_size: int = 100, **filters: str | None
    ) -> Iterator[JsonDict]:
        """Iterate over all of a contract's AMM swaps (filters: sender, sell_token, buy_token)."""
        return self._paginate(f"/contracts/{enc(contract_id)}/swaps",
                              {"limit": page_size, **filters})

    def paginate_nft_events(
        self, contract_id: str, page_size: int = 100, **filters: str | None
    ) -> Iterator[JsonDict]:
        """Iterate over all of a contract's NFT events (filters: kind, from, to, token_id)."""
        return self._paginate(f"/contracts/{enc(contract_id)}/nfts",
                              {"limit": page_size, **filters})

    def paginate_liquidity_events(
        self, contract_id: str, page_size: int = 100, **filters: str | None
    ) -> Iterator[JsonDict]:
        """Iterate over all of a contract's liquidity events (filters: kind, provider)."""
        return self._paginate(f"/contracts/{enc(contract_id)}/liquidity",
                              {"limit": page_size, **filters})

    # ---- Read layer ----

    def list_functions(self, contract_id: str) -> JsonDict:
        """A contract's callable view functions and their typed signatures."""
        result: JsonDict = self._get(f"/contracts/{enc(contract_id)}/functions")
        return result

    def call(
        self, contract_id: str, function: str, args: Any | None = None,
        source_account: str | None = None,
    ) -> CallResult:
        """Invoke a view function read-only and get a typed result."""
        result = self._post(f"/contracts/{enc(contract_id)}/call",
                            _call_body(function, args, source_account))
        return _from_dict(CallResult, result)

    def simulate(
        self, contract_id: str, function: str, args: Any | None = None,
        source_account: str | None = None,
    ) -> CallResult:
        """Dry-run any call and preview its result, emitted events, and cost."""
        result = self._post(f"/contracts/{enc(contract_id)}/simulate",
                            _call_body(function, args, source_account))
        return _from_dict(CallResult, result)

    # ---- Webhooks ----

    def create_webhook(
        self,
        url: str,
        kind: WebhookKind | None = None,
        contract_id: str | None = None,
        contract_ids: list[str] | None = None,
        event_name: str | None = None,
        event_names: list[str] | None = None,
        filter: JsonDict | None = None,
        since: str | int | None = None,
    ) -> JsonDict:
        """Create a webhook subscription. The response carries the one-time ``secret``.

        ``since`` optionally backfills: ``"last N"``, a ledger number, or an
        ISO-8601 timestamp.
        """
        result: JsonDict = self._post("/webhooks", drop_none({
            "url": url,
            "kind": kind,
            "contract_id": contract_id,
            "contract_ids": contract_ids,
            "event_name": event_name,
            "event_names": event_names,
            "filter": filter,
            "since": None if since is None else str(since),
        }))
        return result

    def list_webhooks(self) -> list[JsonDict]:
        """List all webhook subscriptions."""
        result: list[JsonDict] = self._get("/webhooks")
        return result

    def update_webhook(
        self,
        webhook_id: str,
        active: bool | None = None,
        contract_id: str | None = None,
        contract_ids: list[str] | None = None,
        event_name: str | None = None,
        event_names: list[str] | None = None,
        filter: JsonDict | None = None,
    ) -> JsonDict:
        """Pause/resume a subscription or change its filters (``PATCH``)."""
        result: JsonDict = self._request("PATCH", f"/webhooks/{enc(webhook_id)}", body=drop_none({
            "active": active,
            "contract_id": contract_id,
            "contract_ids": contract_ids,
            "event_name": event_name,
            "event_names": event_names,
            "filter": filter,
        }))
        return result

    def delete_webhook(self, webhook_id: str) -> Any:
        """Delete a webhook subscription."""
        return self._request("DELETE", f"/webhooks/{enc(webhook_id)}")

    def list_deliveries(
        self,
        webhook_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        include_summary: bool = False,
    ) -> JsonDict:
        """A webhook's delivery attempts, newest first (cursor-paginated).

        Per-status counts are only returned (under ``summary``) when
        ``include_summary`` is true.
        """
        result: JsonDict = self._get(f"/webhooks/{enc(webhook_id)}/deliveries", page_query(
            limit, offset, after, {"include_summary": True if include_summary else None}))
        return result

    def paginate_deliveries(self, webhook_id: str, page_size: int = 100) -> Iterator[JsonDict]:
        """Iterate over all delivery attempts of a webhook."""
        return self._paginate(f"/webhooks/{enc(webhook_id)}/deliveries", {"limit": page_size})

    def redrive_webhook(self, webhook_id: str, since: str | None = None) -> JsonDict:
        """Reset failed deliveries to pending, optionally only those created at/after ``since`` (RFC 3339)."""
        result: JsonDict = self._post(f"/webhooks/{enc(webhook_id)}/redrive", query={"since": since})
        return result

    def reenable_webhook(self, webhook_id: str) -> JsonDict:
        """Clear an auto-disable and reactivate a webhook subscription."""
        result: JsonDict = self._post(f"/webhooks/{enc(webhook_id)}/reenable")
        return result

    def rotate_webhook_secret(self, webhook_id: str, grace_seconds: int | None = None) -> JsonDict:
        """Rotate a webhook secret; the old one stays valid for ``grace_seconds`` (default 24h)."""
        body = {"grace_seconds": grace_seconds} if grace_seconds is not None else None
        result: JsonDict = self._post(f"/webhooks/{enc(webhook_id)}/rotate-secret", body)
        return result

    # ---- GraphQL ----

    def graphql(self, query: str, variables: Mapping[str, Any] | None = None) -> Any:
        """Execute a raw GraphQL query against ``/graphql`` and return its ``data``."""
        body = self._post("/graphql", {"query": query, "variables": dict(variables or {})})
        return graphql_data(body)

    def events_page(
        self,
        contract_id: str,
        first: int = 50,
        after: str | None = None,
        event_name: str | None = None,
    ) -> JsonDict:
        """One cursor page of events via GraphQL: ``{nodes, end_cursor, has_next_page}``."""
        data = self.graphql(EVENTS_PAGE_QUERY, {
            "id": contract_id, "name": event_name, "first": first, "after": after,
        })
        return events_page_from_graphql(data)

    def paginate_events_graphql(
        self, contract_id: str, event_name: str | None = None, page_size: int = 100
    ) -> Iterator[JsonDict]:
        """Iterate over all of a contract's events via GraphQL cursor pagination."""
        after: str | None = None
        while True:
            page = self.events_page(contract_id, first=page_size, after=after, event_name=event_name)
            yield from page["nodes"]
            after = page["end_cursor"]
            if not page["has_next_page"] or not after:
                break
