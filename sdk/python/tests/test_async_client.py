"""Unit tests for the async Lumenqraph client.

These tests exercise the client's URL-building and pagination logic without
making real network calls by monkey-patching the internal ``_sync_request``
method.
"""

import asyncio
import io
import unittest
import urllib.error
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

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
        # /contracts is cursor-paginated; unset params are dropped when the
        # URL is built.
        self.assertEqual(captured["query"], {"limit": None, "after": None})

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


class TestAsyncWebhookLifecycle(unittest.IsolatedAsyncioTestCase):
    """Webhook lifecycle methods issue the expected async requests."""

    async def test_webhook_lifecycle_calls(self):
        calls = []

        async def mock_request(method, path, query=None, body=None):
            calls.append((method, path, query, body))
            if path.endswith("/redrive"):
                return {"redriven": 1}
            if path.endswith("/reenable"):
                return {"reenabled": True}
            return {
                "id": "wh-1",
                "secret": "new-secret",
                "previous_secret_expires_at": "2026-01-02T00:00:00Z",
            }

        client = AsyncLumenqraphClient(base_url="http://test")
        with patch.object(client, "_request", side_effect=mock_request):
            self.assertEqual(
                await client.redrive_webhook("wh-1", "2026-01-01T00:00:00Z"),
                {"redriven": 1},
            )
            self.assertEqual(await client.reenable_webhook("wh-1"), {"reenabled": True})
            rotated = await client.rotate_webhook_secret("wh-1", 60)

        self.assertEqual(rotated["secret"], "new-secret")
        self.assertEqual(calls[0], (
            "POST", "/webhooks/wh-1/redrive", {"since": "2026-01-01T00:00:00Z"}, None
        ))
        self.assertEqual(calls[1][1], "/webhooks/wh-1/reenable")
        self.assertEqual(calls[2][3], {"grace_seconds": 60})


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


class FakeOk(io.BytesIO):
    """Stand-in for a successful ``urlopen`` response."""

    def __enter__(self) -> "FakeOk":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def _http_503(headers: Optional[Dict[str, str]] = None) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://test", 503, "unavailable", headers or {}, io.BytesIO(b"{}"))


class TestAsyncRetries(unittest.IsolatedAsyncioTestCase):
    """Retries run in the coroutine: backoff is an asyncio sleep, and cancellation stops them."""

    async def test_retry_after_is_awaited_before_the_next_attempt(self):
        attempts = []

        def fake_urlopen(req, timeout=None):
            attempts.append(req)
            if len(attempts) == 1:
                raise _http_503({"Retry-After": "2"})
            return FakeOk(b'{"status": "ok"}')

        lq = AsyncLumenqraphClient(base_url="http://test")
        sleep = AsyncMock()
        with patch("lumenqraph._http.urllib.request.urlopen", side_effect=fake_urlopen), \
                patch("lumenqraph.async_client.asyncio.sleep", sleep), \
                patch("lumenqraph._http._full_jitter", return_value=0):
            result = await lq.health()

        self.assertEqual(result, {"status": "ok"})
        self.assertEqual(len(attempts), 2)
        sleep.assert_awaited_once_with(2.0)

    async def test_cancelling_the_task_stops_further_attempts(self):
        attempts = []

        def always_503(req, timeout=None):
            attempts.append(req)
            raise _http_503()

        # A long base delay keeps the task parked in asyncio.sleep, so the
        # cancel lands between attempts.
        lq = AsyncLumenqraphClient(base_url="http://test", retry={"base_delay_ms": 60_000})
        with patch("lumenqraph._http.urllib.request.urlopen", side_effect=always_503), \
                patch("lumenqraph._http._full_jitter", side_effect=lambda ceiling: ceiling):
            task = asyncio.create_task(lq.health())
            for _ in range(200):
                if attempts:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(len(attempts), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0.1)

        self.assertEqual(len(attempts), 1, "no attempt may run after cancellation")

    async def test_gives_up_after_max_retries(self):
        attempts = []

        def always_503(req, timeout=None):
            attempts.append(req)
            raise _http_503()

        lq = AsyncLumenqraphClient(base_url="http://test", retry={"max_retries": 2, "base_delay_ms": 1})
        with patch("lumenqraph._http.urllib.request.urlopen", side_effect=always_503), \
                patch("lumenqraph.async_client.asyncio.sleep", AsyncMock()):
            with self.assertRaises(LumenqraphError) as ctx:
                await lq.health()

        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(len(attempts), 3)

    async def test_client_errors_are_not_retried(self):
        attempts = []

        def bad_request(req, timeout=None):
            attempts.append(req)
            raise urllib.error.HTTPError("http://test", 400, "bad", {}, io.BytesIO(b'{"error": "bad"}'))

        lq = AsyncLumenqraphClient(base_url="http://test")
        with patch("lumenqraph._http.urllib.request.urlopen", side_effect=bad_request):
            with self.assertRaises(LumenqraphError):
                await lq.health()

        self.assertEqual(len(attempts), 1)


if __name__ == "__main__":
    unittest.main()
