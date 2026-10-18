"""Unit tests for the synchronous Lumenqraph client.

``urllib.request.urlopen`` is patched in the shared transport so the tests
exercise the real request building, auth headers, retry/backoff, error mapping
and pagination without any network access.
"""

import io
import json
import unittest
import urllib.error
from email.message import Message
from typing import Any, Dict, List, Optional
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from lumenqraph import LumenqraphClient, LumenqraphError
from lumenqraph.client import CallResult, ContractState

URLOPEN = "lumenqraph._http.urllib.request.urlopen"
SLEEP = "lumenqraph._http.Transport._sleep"


class FakeResponse(io.BytesIO):
    """Minimal stand-in for ``http.client.HTTPResponse``."""

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def ok(body: Any) -> FakeResponse:
    text = body if isinstance(body, str) else json.dumps(body)
    return FakeResponse(text.encode("utf-8"))


def http_error(status: int, body: Any = None, headers: Optional[Dict[str, str]] = None):
    text = "" if body is None else (body if isinstance(body, str) else json.dumps(body))
    msg = Message()
    for k, v in (headers or {}).items():
        msg[k] = v
    return urllib.error.HTTPError(
        "http://test", status, "error", msg, io.BytesIO(text.encode("utf-8"))
    )


class Recorder:
    """Records each request and replays queued responses/exceptions."""

    def __init__(self, *responses: Any) -> None:
        self.responses: List[Any] = list(responses)
        self.requests: List[Any] = []

    def __call__(self, req: Any, timeout: Optional[float] = None) -> Any:
        self.requests.append(req)
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def url(self, i: int = -1) -> str:
        return self.requests[i].full_url

    def path(self, i: int = -1) -> str:
        return urlsplit(self.url(i)).path

    def query(self, i: int = -1) -> Dict[str, str]:
        return {k: v[0] for k, v in parse_qs(urlsplit(self.url(i)).query).items()}

    def body(self, i: int = -1) -> Any:
        data = self.requests[i].data
        return None if data is None else json.loads(data)

    def method(self, i: int = -1) -> str:
        return self.requests[i].get_method()

    def header(self, name: str, i: int = -1) -> Optional[str]:
        # urllib normalises header names with str.capitalize().
        return self.requests[i].get_header(name.capitalize())


def client(**kwargs: Any) -> LumenqraphClient:
    return LumenqraphClient(base_url="http://test/", **kwargs)


class TestRequests(unittest.TestCase):
    def test_trailing_slash_is_stripped(self):
        rec = Recorder(ok({"status": "ok"}))
        with patch(URLOPEN, rec):
            client().health()
        self.assertEqual(rec.url(), "http://test/health")

    def test_list_contracts_uses_cursor_params(self):
        rec = Recorder(ok({"data": [], "has_more": False, "next_cursor": None}))
        with patch(URLOPEN, rec):
            client().list_contracts(limit=10, after="C1")
        self.assertEqual(rec.path(), "/contracts")
        self.assertEqual(rec.query(), {"limit": "10", "after": "C1"})

    def test_none_query_params_are_omitted(self):
        rec = Recorder(ok({"data": []}))
        with patch(URLOPEN, rec):
            client().list_events("C1")
        self.assertEqual(rec.query(), {"limit": "50"})

    def test_list_events_filters(self):
        rec = Recorder(ok({"data": []}))
        with patch(URLOPEN, rec):
            client().list_events("C1", event_name="transfer", from_ledger=5,
                                 topic0="transfer", successful_only=True)
        self.assertEqual(rec.query(), {
            "limit": "50", "event_name": "transfer", "from_ledger": "5",
            "topic0": "transfer", "successful_only": "true",
        })

    def test_offset_never_sent_with_cursor(self):
        rec = Recorder(ok({"data": []}))
        with patch(URLOPEN, rec):
            client().list_swaps("C1", offset=20, after="cur")
        self.assertNotIn("offset", rec.query())

    def test_path_segments_are_encoded(self):
        rec = Recorder(ok({}))
        with patch(URLOPEN, rec):
            client().get_data_key("C1", "a/b")
        self.assertEqual(rec.url(), "http://test/contracts/C1/data/a%2Fb?limit=50")

    def test_transfers_require_contract(self):
        rec = Recorder(ok({"data": []}))
        with patch(URLOPEN, rec):
            client().list_transfers("C1", from_addr="GA", to_addr="GB")
        self.assertEqual(rec.path(), "/contracts/C1/transfers")
        self.assertEqual(rec.query(), {"limit": "50", "from": "GA", "to": "GB"})

    def test_interface_history_and_diff(self):
        rec = Recorder(ok({}), ok({}))
        with patch(URLOPEN, rec):
            lq = client()
            lq.get_interface_history("C1", limit=5)
            lq.get_interface_diff("C1", from_version=1, to_version=3)
        self.assertEqual(rec.path(0), "/contracts/C1/interface/history")
        self.assertEqual(rec.query(0), {"limit": "5"})
        self.assertEqual(rec.path(1), "/contracts/C1/interface/diff")
        self.assertEqual(rec.query(1), {"from": "1", "to": "3"})

    def test_event_and_transaction_lookups(self):
        rec = Recorder(ok({"event_id": "e1"}), ok({"tx_hash": "abc", "data": []}))
        with patch(URLOPEN, rec):
            lq = client()
            lq.get_event("e1")
            lq.get_transaction_events("abc", limit=10)
        self.assertEqual(rec.path(0), "/events/e1")
        self.assertEqual(rec.path(1), "/transactions/abc/events")
        self.assertEqual(rec.query(1), {"limit": "10"})

    def test_stats_maps_time_range(self):
        rec = Recorder(ok({"data": [], "total": 0}))
        with patch(URLOPEN, rec):
            client().get_stats("C1", bucket="hour", group_by="event_name",
                               from_time="2026-01-01T00:00:00Z", to_ledger=9)
        self.assertEqual(rec.path(), "/contracts/C1/stats")
        self.assertEqual(rec.query(), {
            "bucket": "hour", "group_by": "event_name",
            "from": "2026-01-01T00:00:00Z", "to_ledger": "9",
        })

    def test_generate_sdk_returns_text(self):
        rec = Recorder(ok("export class Client {}"))
        with patch(URLOPEN, rec):
            src = client().generate_sdk("C1", lang="python")
        self.assertEqual(src, "export class Client {}")
        self.assertEqual(rec.query(), {"lang": "python"})

    def test_call_posts_body_and_returns_dataclass(self):
        rec = Recorder(ok({
            "contract_id": "C1", "function": "balance", "result": "0",
            "simulated_at_ledger": 1, "unknown_new_field": True,
        }))
        with patch(URLOPEN, rec):
            res = client().call("C1", "balance", {"id": "G1"})
        self.assertIsInstance(res, CallResult)
        self.assertEqual(res.result, "0")
        self.assertEqual(rec.method(), "POST")
        self.assertEqual(rec.body(), {"function": "balance", "args": {"id": "G1"},
                                      "source_account": None})

    def test_get_state_returns_dataclass(self):
        rec = Recorder(ok({"contract_id": "C1", "count": 0, "versions": []}))
        with patch(URLOPEN, rec):
            state = client().get_state("C1")
        self.assertIsInstance(state, ContractState)


class TestWebhooks(unittest.TestCase):
    def test_lifecycle_endpoints(self):
        rec = Recorder(
            ok({"id": "w1", "secret": "s"}),
            ok([{"id": "w1"}]),
            ok({"id": "w1", "active": False}),
            ok({"redriven": 2}),
            ok({"reenabled": True}),
            ok({"secret": "s2"}),
            ok({"deleted": True}),
        )
        with patch(URLOPEN, rec):
            lq = client(api_key="k")
            lq.create_webhook("https://hook", contract_id="C1", event_names=["transfer"])
            lq.list_webhooks()
            lq.update_webhook("w1", active=False)
            lq.redrive_webhook("w1", since="2026-01-01T00:00:00Z")
            lq.reenable_webhook("w1")
            lq.rotate_webhook_secret("w1", grace_seconds=60)
            lq.delete_webhook("w1")

        self.assertEqual((rec.method(0), rec.path(0)), ("POST", "/webhooks"))
        self.assertEqual(rec.body(0), {"url": "https://hook", "contract_id": "C1",
                                       "event_names": ["transfer"]})
        self.assertEqual((rec.method(1), rec.path(1)), ("GET", "/webhooks"))
        self.assertEqual((rec.method(2), rec.path(2)), ("PATCH", "/webhooks/w1"))
        self.assertEqual(rec.body(2), {"active": False})
        self.assertEqual(rec.path(3), "/webhooks/w1/redrive")
        self.assertEqual(rec.query(3), {"since": "2026-01-01T00:00:00Z"})
        self.assertIsNone(rec.body(3))
        self.assertEqual(rec.path(4), "/webhooks/w1/reenable")
        self.assertEqual(rec.body(5), {"grace_seconds": 60})
        self.assertEqual((rec.method(6), rec.path(6)), ("DELETE", "/webhooks/w1"))

    def test_deliveries_summary_flag(self):
        rec = Recorder(ok({"data": []}))
        with patch(URLOPEN, rec):
            client().list_deliveries("w1", include_summary=True)
        self.assertEqual(rec.query(), {"limit": "50", "include_summary": "true"})


class TestAuthHeaders(unittest.TestCase):
    def test_api_key_sent_when_configured(self):
        rec = Recorder(ok({}), ok({}))
        with patch(URLOPEN, rec):
            lq = client(api_key="secret")
            lq.health()
            lq.call("C1", "f")
        self.assertEqual(rec.header("x-api-key", 0), "secret")
        self.assertEqual(rec.header("x-api-key", 1), "secret")

    def test_api_key_absent_by_default(self):
        rec = Recorder(ok({}))
        with patch(URLOPEN, rec):
            client().health()
        self.assertIsNone(rec.header("x-api-key"))


class TestRetries(unittest.TestCase):
    def test_retries_503_then_succeeds(self):
        rec = Recorder(http_error(503), ok({"status": "ok"}))
        with patch(URLOPEN, rec), patch(SLEEP) as sleep:
            self.assertEqual(client().health(), {"status": "ok"})
        self.assertEqual(len(rec.requests), 2)
        sleep.assert_called_once_with(250)

    def test_exponential_backoff_is_capped(self):
        rec = Recorder(http_error(502), http_error(502), http_error(502), ok({}))
        retry = {"base_delay_ms": 100, "max_delay_ms": 300}
        with patch(URLOPEN, rec), patch(SLEEP) as sleep:
            client(retry=retry).health()
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [100, 200, 300])

    def test_honors_retry_after_seconds(self):
        rec = Recorder(http_error(429, headers={"Retry-After": "2"}), ok({}))
        with patch(URLOPEN, rec), patch(SLEEP) as sleep:
            client().health()
        sleep.assert_called_once_with(2000)

    def test_does_not_retry_client_errors(self):
        rec = Recorder(http_error(400, {"error": "bad"}))
        with patch(URLOPEN, rec), patch(SLEEP) as sleep:
            with self.assertRaises(LumenqraphError):
                client().health()
        self.assertEqual(len(rec.requests), 1)
        sleep.assert_not_called()

    def test_gives_up_after_max_retries(self):
        rec = Recorder(*[http_error(503) for _ in range(3)])
        with patch(URLOPEN, rec), patch(SLEEP):
            with self.assertRaises(LumenqraphError) as ctx:
                client(retry={"max_retries": 2}).health()
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(len(rec.requests), 3)

    def test_retries_network_errors(self):
        rec = Recorder(urllib.error.URLError("refused"), ok({"status": "ok"}))
        with patch(URLOPEN, rec), patch(SLEEP):
            self.assertEqual(client().health(), {"status": "ok"})

    def test_network_error_maps_to_status_zero(self):
        rec = Recorder(urllib.error.URLError("refused"))
        with patch(URLOPEN, rec):
            with self.assertRaises(LumenqraphError) as ctx:
                client(retry={"max_retries": 0}).health()
        self.assertEqual(ctx.exception.status, 0)
        self.assertIsNone(ctx.exception.body)


class TestErrorMapping(unittest.TestCase):
    def test_json_error_body(self):
        rec = Recorder(http_error(404, {"error": "contract not found"}))
        with patch(URLOPEN, rec):
            with self.assertRaises(LumenqraphError) as ctx:
                client().get_interface("C404")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.body, {"error": "contract not found"})
        self.assertIn("contract not found", str(ctx.exception))

    def test_non_json_error_body(self):
        rec = Recorder(http_error(401, "unauthorized"))
        with patch(URLOPEN, rec):
            with self.assertRaises(LumenqraphError) as ctx:
                client().health()
        self.assertEqual(ctx.exception.body, "unauthorized")

    def test_graphql_errors_raise(self):
        rec = Recorder(ok({"data": None, "errors": [{"message": "a"}, {"message": "b"}]}))
        with patch(URLOPEN, rec):
            with self.assertRaises(LumenqraphError) as ctx:
                client().graphql("{ bad }")
        self.assertEqual(ctx.exception.status, 200)
        self.assertIn("a; b", str(ctx.exception))


class TestPagination(unittest.TestCase):
    def test_paginate_events_follows_cursor(self):
        rec = Recorder(
            ok({"data": [{"event_id": "e1"}, {"event_id": "e2"}],
                "has_more": True, "next_cursor": "c1"}),
            ok({"data": [{"event_id": "e3"}], "has_more": False, "next_cursor": None}),
        )
        with patch(URLOPEN, rec):
            ids = [e["event_id"] for e in client().paginate_events("C1", page_size=2)]
        self.assertEqual(ids, ["e1", "e2", "e3"])
        self.assertNotIn("after", rec.query(0))
        self.assertEqual(rec.query(1)["after"], "c1")

    def test_paginate_stops_without_cursor(self):
        rec = Recorder(ok({"data": [{"id": 1}], "has_more": True, "next_cursor": None}))
        with patch(URLOPEN, rec):
            items = list(client().paginate_deliveries("w1"))
        self.assertEqual(len(items), 1)
        self.assertEqual(len(rec.requests), 1)

    def test_paginate_contracts(self):
        rec = Recorder(
            ok({"data": [{"contract_id": "C1"}], "has_more": True, "next_cursor": "C1"}),
            ok({"data": [{"contract_id": "C2"}], "has_more": False, "next_cursor": None}),
        )
        with patch(URLOPEN, rec):
            ids = [c["contract_id"] for c in client().paginate_contracts()]
        self.assertEqual(ids, ["C1", "C2"])

    def test_paginate_events_graphql(self):
        def page(nodes, has_next, cursor):
            return ok({"data": {"events": {
                "edges": [{"cursor": n, "node": {"eventId": n}} for n in nodes],
                "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
            }}})

        rec = Recorder(page(["e1"], True, "c1"), page(["e2"], False, None))
        with patch(URLOPEN, rec):
            ids = [e["eventId"] for e in client().paginate_events_graphql("C1")]
        self.assertEqual(ids, ["e1", "e2"])
        self.assertEqual(rec.body(1)["variables"]["after"], "c1")


class TestStreamEvents(unittest.TestCase):
    def test_parses_sse_frames(self):
        sse = (
            ": keep-alive\n\n"
            "id: 10:e1\ndata: {\"data\": {\"event_id\": \"e1\"}}\n\n"
            "id: 11:e2\ndata: {\"event_id\": \"e2\"}\n\n"
        )
        rec = Recorder(FakeResponse(sse.encode("utf-8")))
        with patch(URLOPEN, rec):
            events = list(client().stream_events("C1", cursor="9:e0", reconnect=False))
        self.assertEqual([e["event_id"] for e in events], ["e1", "e2"])
        self.assertEqual(rec.path(), "/contracts/C1/events/stream")
        self.assertEqual(rec.query(), {"cursor": "9", "cursor_event_id": "e0"})
        self.assertEqual(rec.header("last-event-id"), "9:e0")
        self.assertEqual(rec.header("accept"), "text/event-stream")


if __name__ == "__main__":
    unittest.main()
