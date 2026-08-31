"""Unit tests for the async Lumenqraph client.

These tests exercise the client's URL-building and pagination logic without
making real network calls by monkey-patching the internal ``_sync_request``
method.
"""

import asyncio
import unittest
from typing import Any, Dict
from unittest.mock import patch, MagicMock

from lumenqraph import AsyncLumenqraphClient, LumenqraphError


class TestAsyncClientUrlBuilding(unittest.IsolatedAsyncioTestCase):
    """Verify that query parameters are assembled correctly."""

    async def test_list_contracts_default_params(self):
        captured: Dict[str, Any] = {}

        async def mock_request(method, path, query=None, body=None):
            captured["method"] = method
            captured["path"] = path
            captured["query"] = query
            return {"data": [], "has_more": False}

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            await lq.list_contracts()

        self.assertEqual(captured["method"], "GET")
        self.assertEqual(captured["path"], "/contracts")
        self.assertEqual(captured["query"], {"limit": 200, "offset": 0})

    async def test_list_events_with_event_name(self):
        captured: Dict[str, Any] = {}

        async def mock_request(method, path, query=None, body=None):
            captured["query"] = query
            return {"data": [], "has_more": False, "next_cursor": None}

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            await lq.list_events("C1", event_name="transfer")

        self.assertIn("event_name", captured["query"])
        self.assertEqual(captured["query"]["event_name"], "transfer")

    async def test_call_sends_correct_body(self):
        captured: Dict[str, Any] = {}

        async def mock_request(method, path, query=None, body=None):
            captured["body"] = body
            return {"contract_id": "C1", "function": "balance",
                    "result": {"type": "i128", "value": "0"},
                    "simulated_at_ledger": 1}

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            await lq.call("C1", function="balance", args={"id": "G1"})

        self.assertEqual(captured["body"]["function"], "balance")
        self.assertEqual(captured["body"]["args"], {"id": "G1"})


class TestAsyncClientPagination(unittest.IsolatedAsyncioTestCase):
    """Verify cursor-based pagination exhausts all pages."""

    async def test_paginate_events_follows_cursors(self):
        pages = [
            {"data": [{"event_id": "e1"}, {"event_id": "e2"}],
             "has_more": True, "next_cursor": "cur1"},
            {"data": [{"event_id": "e3"}],
             "has_more": False, "next_cursor": None},
        ]
        call_count = 0

        async def mock_request(method, path, query=None, body=None):
            nonlocal call_count
            result = pages[call_count]
            call_count += 1
            return result

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            events = []
            async for ev in lq.paginate_events("C1"):
                events.append(ev)

        self.assertEqual(len(events), 3)
        self.assertEqual(events[0]["event_id"], "e1")
        self.assertEqual(events[2]["event_id"], "e3")
        self.assertEqual(call_count, 2)

    async def test_paginate_events_single_page(self):
        async def mock_request(method, path, query=None, body=None):
            return {"data": [{"event_id": "e1"}], "has_more": False, "next_cursor": None}

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            events = [ev async for ev in lq.paginate_events("C1")]

        self.assertEqual(len(events), 1)


class TestAsyncClientErrors(unittest.IsolatedAsyncioTestCase):
    """Verify error propagation from the underlying sync request."""

    async def test_raises_lumenqraph_error_on_404(self):
        async def mock_request(method, path, query=None, body=None):
            raise LumenqraphError("not found", 404, {"error": "not found"})

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            with self.assertRaises(LumenqraphError) as ctx:
                await lq.get_interface("UNKNOWN")
        self.assertEqual(ctx.exception.status, 404)

    async def test_graphql_raises_on_errors_field(self):
        async def mock_request(method, path, query=None, body=None):
            return {"errors": [{"message": "field not found"}], "data": None}

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(lq, "_request", side_effect=mock_request):
            with self.assertRaises(LumenqraphError):
                await lq.graphql("{ bad }")


class TestAsyncClientContextManager(unittest.IsolatedAsyncioTestCase):
    """Async context manager protocol."""

    async def test_async_with(self):
        async with AsyncLumenqraphClient(base_url="http://test") as lq:
            self.assertIsInstance(lq, AsyncLumenqraphClient)
        # No exception — aclose is a no-op but must not raise.


if __name__ == "__main__":
    unittest.main()
