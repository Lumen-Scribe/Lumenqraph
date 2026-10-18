"""Async (asyncio) Lumenqraph API client.

This module mirrors the surface of :class:`lumenqraph.client.LumenqraphClient`
method for method, but every call is a coroutine. HTTP I/O uses the same
zero-dependency :mod:`urllib` transport, dispatched to a thread-pool executor so
the event loop is never blocked.

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

from __future__ import annotations

import asyncio
import functools
from collections.abc import AsyncIterator, Mapping
from typing import Any, Literal

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
    resolve_retry,
)
from .client import JsonDict, SdkLang, WebhookKind, _call_body

__all__ = ["AsyncLumenqraphClient", "LumenqraphError"]


class AsyncLumenqraphClient:
    """Async Lumenqraph API client.

    All methods are coroutines (``paginate_*`` return async iterators).

    The client can be used as an async context manager::

        async with AsyncLumenqraphClient(base_url="http://localhost:8080") as lq:
            health = await lq.health()

    It can also be instantiated directly without the context-manager protocol;
    call :meth:`aclose` explicitly when done if you need deterministic cleanup.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        retry: RetryOptions | None = None,
    ) -> None:
        """Initialise the async Lumenqraph client.

        Args:
            base_url:  Base URL of the Lumenqraph API (e.g. ``http://localhost:8080``).
            api_key:   Optional API key, sent as ``x-api-key``.
            retry:     Optional retry / timeout policy.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._retry = resolve_retry(retry)
        self._transport = Transport(self._retry)

    async def __aenter__(self) -> AsyncLumenqraphClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Release any resources held by the client (currently a no-op)."""

    # ---- Core request machinery ----

    async def _request(
        self,
        method: str,
        path: str,
        query: Mapping[str, Any] | None = None,
        body: Any = None,
        raw: bool = False,
    ) -> Any:
        url = build_url(self.base_url, path, query)
        accept = "text/plain, */*" if raw else "application/json"
        send = functools.partial(
            self._transport.send,
            method,
            url,
            encode_json(body),
            build_headers(self.api_key, accept),
            raw,
        )
        return await asyncio.get_running_loop().run_in_executor(None, send)

    async def _get(self, path: str, query: Mapping[str, Any] | None = None) -> Any:
        return await self._request("GET", path, query)

    async def _post(self, path: str, body: Any = None) -> Any:
        return await self._request("POST", path, None, body)

    async def _paginate(self, path: str, query: Mapping[str, Any]) -> AsyncIterator[JsonDict]:
        """Follow ``next_cursor`` until ``has_more`` is false."""
        after: str | None = None
        while True:
            response = await self._get(path, {**query, "after": after})
            for item in response.get("data", []):
                yield item
            after = response.get("next_cursor")
            if not response.get("has_more", False) or not after:
                break

    # ---- Health ----

    async def health(self) -> JsonDict:
        """Get health and indexing-lag report."""
        result: JsonDict = await self._get("/health")
        return result

    async def livez(self) -> Any:
        """Liveness probe: the process is up."""
        return await self._get("/livez")

    async def readyz(self) -> Any:
        """Readiness probe: the API can reach Postgres."""
        return await self._get("/readyz")

    # ---- Contracts & interfaces ----

    async def list_contracts(self, limit: int | None = None, after: str | None = None) -> JsonDict:
        """Contracts the indexer has seen (cursor via ``after``)."""
        result: JsonDict = await self._get("/contracts", {"limit": limit, "after": after})
        return result

    def paginate_contracts(self, page_size: int = 100) -> AsyncIterator[JsonDict]:
        """Every indexed contract via cursor pagination."""
        return self._paginate("/contracts", {"limit": page_size})

    async def get_interface(self, contract_id: str, version: int | None = None) -> JsonDict:
        """A contract's decoded on-chain interface, optionally at a historical version."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/interface", {"version": version}
        )
        return result

    async def get_interface_history(
        self, contract_id: str, limit: int | None = None, after: str | None = None
    ) -> JsonDict:
        """The version history of a contract's interface, with semantic diffs."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/interface/history", {"limit": limit, "after": after}
        )
        return result

    async def get_interface_diff(
        self, contract_id: str, from_version: int | None = None, to_version: int | None = None
    ) -> JsonDict:
        """The semantic diff between two interface versions (defaults: previous → newest)."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/interface/diff", {"from": from_version, "to": to_version}
        )
        return result

    async def refresh_contract(self, contract_id: str) -> JsonDict:
        """Re-read a contract's on-chain interface now."""
        result: JsonDict = await self._post(f"/contracts/{enc(contract_id)}/refresh")
        return result

    async def generate_sdk(
        self, contract_id: str, lang: SdkLang | None = None, version: int | None = None
    ) -> str:
        """Generated client source code for a contract (``ts`` by default)."""
        result: str = await self._request(
            "GET", f"/contracts/{enc(contract_id)}/sdk", {"lang": lang, "version": version}, raw=True
        )
        return result

    async def get_stats(
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
        result: JsonDict = await self._get(f"/contracts/{enc(contract_id)}/stats", {
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

    async def get_state(self, contract_id: str, limit: int = 1) -> JsonDict:
        """Versioned instance-storage snapshots, newest first."""
        result: JsonDict = await self._get(f"/contracts/{enc(contract_id)}/state", {"limit": limit})
        return result

    async def get_data(
        self, contract_id: str, label: str | None = None, limit: int = 100
    ) -> JsonDict:
        """Latest value of every per-key entry."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/data", {"label": label, "limit": limit}
        )
        return result

    async def get_data_key(self, contract_id: str, key_hash: str, limit: int = 50) -> JsonDict:
        """Version history of a single per-key entry."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/data/{enc(key_hash)}", {"limit": limit}
        )
        return result

    # ---- Events ----

    async def list_events(
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
        result: JsonDict = await self._get(f"/contracts/{enc(contract_id)}/events", page_query(
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
    ) -> AsyncIterator[JsonDict]:
        """All of a contract's events via REST cursor pagination.

        Example::

            async for event in lq.paginate_events(contract_id, event_name="transfer"):
                print(event["ledger"], event.get("enriched"))
        """
        return self._paginate(
            f"/contracts/{enc(contract_id)}/events",
            {"limit": page_size, "event_name": event_name, **filters},
        )

    async def get_event(self, event_id: str) -> JsonDict:
        """A single event by its unique id."""
        result: JsonDict = await self._get(f"/events/{enc(event_id)}")
        return result

    async def get_transaction_events(self, tx_hash: str, limit: int | None = None) -> JsonDict:
        """All indexed events emitted by a transaction, in emission order."""
        result: JsonDict = await self._get(
            f"/transactions/{enc(tx_hash)}/events", {"limit": limit}
        )
        return result

    # ---- Materialized projections ----

    async def list_transfers(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        from_addr: str | None = None,
        to_addr: str | None = None,
    ) -> JsonDict:
        """Materialized SEP-41 transfers for a contract, newest first (cursor-paginated)."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/transfers",
            page_query(limit, offset, after, {"from": from_addr, "to": to_addr}),
        )
        return result

    async def list_swaps(
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
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/swaps",
            page_query(limit, offset, after,
                       {"sender": sender, "sell_token": sell_token, "buy_token": buy_token}),
        )
        return result

    async def list_nft_events(
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
        """Materialized NFT events, newest first (cursor-paginated)."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/nfts",
            page_query(limit, offset, after,
                       {"kind": kind, "from": from_addr, "to": to_addr, "token_id": token_id}),
        )
        return result

    async def list_liquidity_events(
        self,
        contract_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        kind: Literal["add", "remove"] | None = None,
        provider: str | None = None,
    ) -> JsonDict:
        """Materialized liquidity events, newest first (cursor-paginated)."""
        result: JsonDict = await self._get(
            f"/contracts/{enc(contract_id)}/liquidity",
            page_query(limit, offset, after, {"kind": kind, "provider": provider}),
        )
        return result

    def paginate_transfers(
        self, contract_id: str, page_size: int = 100,
        from_addr: str | None = None, to_addr: str | None = None,
    ) -> AsyncIterator[JsonDict]:
        """All of a contract's transfers via cursor pagination."""
        return self._paginate(
            f"/contracts/{enc(contract_id)}/transfers",
            {"limit": page_size, "from": from_addr, "to": to_addr},
        )

    def paginate_swaps(
        self, contract_id: str, page_size: int = 100, **filters: str | None
    ) -> AsyncIterator[JsonDict]:
        """All AMM swaps for a contract (filters: sender, sell_token, buy_token)."""
        return self._paginate(f"/contracts/{enc(contract_id)}/swaps", {"limit": page_size, **filters})

    def paginate_nft_events(
        self, contract_id: str, page_size: int = 100, **filters: str | None
    ) -> AsyncIterator[JsonDict]:
        """All NFT events for a contract (filters: kind, from, to, token_id)."""
        return self._paginate(f"/contracts/{enc(contract_id)}/nfts", {"limit": page_size, **filters})

    def paginate_liquidity_events(
        self, contract_id: str, page_size: int = 100, **filters: str | None
    ) -> AsyncIterator[JsonDict]:
        """All liquidity events for a contract (filters: kind, provider)."""
        return self._paginate(
            f"/contracts/{enc(contract_id)}/liquidity", {"limit": page_size, **filters}
        )

    # ---- Read layer ----

    async def list_functions(self, contract_id: str) -> JsonDict:
        """A contract's callable view functions and their typed signatures."""
        result: JsonDict = await self._get(f"/contracts/{enc(contract_id)}/functions")
        return result

    async def call(
        self, contract_id: str, function: str, args: Any | None = None,
        source_account: str | None = None,
    ) -> JsonDict:
        """Invoke a view function read-only and get a typed result."""
        result: JsonDict = await self._post(
            f"/contracts/{enc(contract_id)}/call", _call_body(function, args, source_account)
        )
        return result

    async def simulate(
        self, contract_id: str, function: str, args: Any | None = None,
        source_account: str | None = None,
    ) -> JsonDict:
        """Dry-run any call and preview its result, emitted events, and cost."""
        result: JsonDict = await self._post(
            f"/contracts/{enc(contract_id)}/simulate", _call_body(function, args, source_account)
        )
        return result

    # ---- Webhooks ----

    async def create_webhook(
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
        """Create a webhook subscription. The response carries the one-time ``secret``."""
        result: JsonDict = await self._post("/webhooks", drop_none({
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

    async def list_webhooks(self) -> list[JsonDict]:
        """List all webhook subscriptions."""
        result: list[JsonDict] = await self._get("/webhooks")
        return result

    async def update_webhook(
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
        result: JsonDict = await self._request("PATCH", f"/webhooks/{enc(webhook_id)}", None, drop_none({
            "active": active,
            "contract_id": contract_id,
            "contract_ids": contract_ids,
            "event_name": event_name,
            "event_names": event_names,
            "filter": filter,
        }))
        return result

    async def delete_webhook(self, webhook_id: str) -> Any:
        """Delete a webhook subscription."""
        return await self._request("DELETE", f"/webhooks/{enc(webhook_id)}")

    async def list_deliveries(
        self,
        webhook_id: str,
        limit: int = 50,
        offset: int = 0,
        after: str | None = None,
        include_summary: bool = False,
    ) -> JsonDict:
        """A webhook's delivery attempts, newest first (cursor-paginated)."""
        result: JsonDict = await self._get(
            f"/webhooks/{enc(webhook_id)}/deliveries",
            page_query(limit, offset, after,
                       {"include_summary": True if include_summary else None}),
        )
        return result

    def paginate_deliveries(self, webhook_id: str, page_size: int = 100) -> AsyncIterator[JsonDict]:
        """All delivery attempts of a webhook."""
        return self._paginate(f"/webhooks/{enc(webhook_id)}/deliveries", {"limit": page_size})

    async def redrive_webhook(self, webhook_id: str, since: str | None = None) -> JsonDict:
        """Reset failed deliveries to pending, optionally only those at/after ``since`` (RFC 3339)."""
        result: JsonDict = await self._request(
            "POST", f"/webhooks/{enc(webhook_id)}/redrive", {"since": since}, None
        )
        return result

    async def reenable_webhook(self, webhook_id: str) -> JsonDict:
        """Clear an auto-disable and reactivate a webhook subscription."""
        result: JsonDict = await self._post(f"/webhooks/{enc(webhook_id)}/reenable")
        return result

    async def rotate_webhook_secret(
        self, webhook_id: str, grace_seconds: int | None = None
    ) -> JsonDict:
        """Rotate a webhook secret; the old one stays valid for ``grace_seconds`` (default 24h)."""
        body = {"grace_seconds": grace_seconds} if grace_seconds is not None else None
        result: JsonDict = await self._post(f"/webhooks/{enc(webhook_id)}/rotate-secret", body)
        return result

    # ---- GraphQL ----

    async def graphql(self, query: str, variables: Mapping[str, Any] | None = None) -> Any:
        """Execute a raw GraphQL query against ``/graphql`` and return its ``data``."""
        body = await self._post("/graphql", {"query": query, "variables": dict(variables or {})})
        return graphql_data(body)

    async def events_page(
        self,
        contract_id: str,
        first: int = 50,
        after: str | None = None,
        event_name: str | None = None,
    ) -> JsonDict:
        """One cursor page of events via GraphQL: ``{nodes, end_cursor, has_next_page}``."""
        data = await self.graphql(EVENTS_PAGE_QUERY, {
            "id": contract_id, "name": event_name, "first": first, "after": after,
        })
        return events_page_from_graphql(data)

    async def paginate_events_graphql(
        self, contract_id: str, event_name: str | None = None, page_size: int = 100
    ) -> AsyncIterator[JsonDict]:
        """All of a contract's events via GraphQL cursor pagination."""
        after: str | None = None
        while True:
            page = await self.events_page(
                contract_id, first=page_size, after=after, event_name=event_name
            )
            for node in page["nodes"]:
                yield node
            after = page["end_cursor"]
            if not page["has_next_page"] or not after:
                break
