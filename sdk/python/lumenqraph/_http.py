"""Blocking HTTP transport shared by the sync and async clients.

Uses only :mod:`urllib` so the SDK has no runtime HTTP dependency. Both clients
build requests with :func:`build_url` / :func:`build_headers` and send them with
:meth:`Transport.send`, so retry, timeout and error-mapping behaviour is
identical across them.
"""

from __future__ import annotations

import email.utils
import json
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from typing import Any, TypedDict
from urllib.parse import quote, urlencode


class RetryOptions(TypedDict, total=False):
    """Retry policy configuration."""

    max_retries: int
    base_delay_ms: int
    max_delay_ms: int
    timeout_ms: int


class ResolvedRetry(TypedDict):
    """A :class:`RetryOptions` with every field filled in."""

    max_retries: int
    base_delay_ms: int
    max_delay_ms: int
    timeout_ms: int


class LumenqraphError(Exception):
    """Error raised for any non-2xx API response (``status`` 0 for network errors)."""

    def __init__(self, message: str, status: int, body: Any):
        super().__init__(message)
        self.status = status
        self.body = body


DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY_MS = 250
DEFAULT_MAX_DELAY_MS = 30_000
DEFAULT_TIMEOUT_MS = 10_000
RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})


def resolve_retry(opts: RetryOptions | None) -> ResolvedRetry:
    """Merge provided retry options with the defaults."""
    opts = opts or {}
    return {
        "max_retries": opts.get("max_retries", DEFAULT_MAX_RETRIES),
        "base_delay_ms": opts.get("base_delay_ms", DEFAULT_BASE_DELAY_MS),
        "max_delay_ms": opts.get("max_delay_ms", DEFAULT_MAX_DELAY_MS),
        "timeout_ms": opts.get("timeout_ms", DEFAULT_TIMEOUT_MS),
    }


def enc(segment: str) -> str:
    """Percent-encode a path segment (contract ids, hashes, webhook ids)."""
    return quote(str(segment), safe="")


def _query_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def build_url(base_url: str, path: str, query: Mapping[str, Any] | None = None) -> str:
    """Join ``base_url`` and ``path`` and append non-``None`` query params."""
    url = base_url + path
    if query:
        filtered = {k: _query_value(v) for k, v in query.items() if v is not None}
        if filtered:
            url = f"{url}?{urlencode(filtered)}"
    return url


def build_headers(api_key: str | None, accept: str = "application/json") -> dict[str, str]:
    headers = {"Content-Type": "application/json", "Accept": accept}
    if api_key:
        headers["x-api-key"] = api_key
    return headers


def page_query(
    limit: int | None,
    offset: int,
    after: str | None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a list-endpoint query. ``offset`` is only sent when non-zero
    (it is deprecated server-side) and never alongside a cursor."""
    query: dict[str, Any] = {"limit": limit, "after": after}
    if offset and not after:
        query["offset"] = offset
    if extra:
        query.update(extra)
    return query


def _parse_body(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _error_message(status: int, body: Any, text: str) -> str:
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return f"HTTP {status}: {body['error']}"
    return f"HTTP {status}: {text}"


def _retry_after_ms(header: str | None) -> int | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) to ms."""
    if not header:
        return None
    header = header.strip()
    if header.isdigit():
        return int(header) * 1000
    try:
        when = email.utils.parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    return max(0, int((when.timestamp() - time.time()) * 1000))


class Transport:
    """Sends one request with retry/backoff and maps failures to :class:`LumenqraphError`."""

    def __init__(self, retry: ResolvedRetry):
        self.retry = retry

    def _delay_ms(self, attempt: int, retry_after: int | None = None) -> int:
        backoff = self.retry["base_delay_ms"] * (2 ** attempt)
        if retry_after is not None:
            backoff = max(backoff, retry_after)
        return min(backoff, self.retry["max_delay_ms"])

    def _sleep(self, ms: int) -> None:
        time.sleep(ms / 1000.0)

    def send(
        self,
        method: str,
        url: str,
        body: bytes | None,
        headers: Mapping[str, str],
        raw: bool = False,
    ) -> Any:
        """Send a request, retrying 429/502/503/504 and network errors.

        Returns the decoded JSON body (``None`` for an empty body), or the raw
        text when ``raw`` is true.
        """
        timeout = self.retry["timeout_ms"] / 1000.0
        max_retries = self.retry["max_retries"]
        attempt = 0
        while True:
            req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    text: str = resp.read().decode("utf-8")
                    if raw:
                        return text
                    return json.loads(text) if text else None
            except urllib.error.HTTPError as exc:
                status = exc.code
                text = exc.read().decode("utf-8")
                parsed = _parse_body(text)
                if status not in RETRYABLE_STATUSES or attempt >= max_retries:
                    raise LumenqraphError(_error_message(status, parsed, text), status, parsed) from None
                retry_after = _retry_after_ms(exc.headers.get("Retry-After") if exc.headers else None)
                self._sleep(self._delay_ms(attempt, retry_after))
                attempt += 1
            except urllib.error.URLError as exc:
                if attempt >= max_retries:
                    raise LumenqraphError(f"Network error: {exc.reason}", 0, None) from None
                self._sleep(self._delay_ms(attempt))
                attempt += 1

    def stream_lines(self, url: str, headers: Mapping[str, str]) -> Iterator[str]:
        """Open a long-lived GET and yield decoded lines (for SSE)."""
        req = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            resp = urllib.request.urlopen(req, timeout=None)
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8")
            parsed = _parse_body(text)
            raise LumenqraphError(_error_message(exc.code, parsed, text), exc.code, parsed) from None
        except urllib.error.URLError as exc:
            raise LumenqraphError(f"Network error: {exc.reason}", 0, None) from None
        with resp:
            for raw_line in resp:
                yield raw_line.decode("utf-8").rstrip("\r\n")


def encode_json(body: Any) -> bytes | None:
    return None if body is None else json.dumps(body).encode("utf-8")


def drop_none(data: Mapping[str, Any]) -> dict[str, Any]:
    """Drop ``None`` values so optional fields are omitted from request bodies."""
    return {k: v for k, v in data.items() if v is not None}


EVENTS_PAGE_QUERY = """
query Events($id: String!, $name: String, $first: Int, $after: String) {
  events(contractId: $id, eventName: $name, first: $first, after: $after) {
    edges { cursor node {
      eventId contractId ledger ledgerClosedAt eventType eventName
      decodedTopics decodedValue enriched txHash inSuccessfulCall
    } }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def graphql_data(body: Any) -> Any:
    """Return ``data`` from a GraphQL response, raising on ``errors``."""
    if not isinstance(body, dict):
        return None
    errors = body.get("errors")
    if errors:
        messages = "; ".join(
            e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in errors
        )
        raise LumenqraphError(f"GraphQL error: {messages}", 200, errors)
    return body.get("data")


def events_page_from_graphql(data: Any) -> dict[str, Any]:
    """Convert a GraphQL ``events`` connection to ``{nodes, end_cursor, has_next_page}``."""
    events = (data or {}).get("events") or {}
    info = events.get("pageInfo") or {}
    return {
        "nodes": [edge["node"] for edge in events.get("edges", [])],
        "end_cursor": info.get("endCursor"),
        "has_next_page": bool(info.get("hasNextPage", False)),
    }


class SseFrame(TypedDict, total=False):
    id: str
    event: str
    data: str


def parse_sse(lines: Iterator[str]) -> Iterator[SseFrame]:
    """Group SSE lines into frames (``id``/``event``/``data``)."""
    frame: SseFrame = {}
    data_lines: list[str] = []
    for line in lines:
        if line == "":
            if data_lines:
                frame["data"] = "\n".join(data_lines)
            if frame:
                yield frame
            frame, data_lines = {}, []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data_lines.append(value)
        elif field == "id":
            frame["id"] = value
        elif field == "event":
            frame["event"] = value
    if data_lines:
        frame["data"] = "\n".join(data_lines)
    if frame:
        yield frame
