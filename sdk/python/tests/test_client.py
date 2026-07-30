"""Tests for the Lumenqraph client."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

from lumenqraph import LumenqraphClient, LumenqraphError


@pytest.fixture
def mock_response():
    """Create a mock HTTP response."""
    def _make_response(status_code: int = 200, json_data: dict | None = None, text: str = ""):
        response = MagicMock(spec=httpx.Response)
        response.status_code = status_code
        response.is_success = 200 <= status_code < 300
        response.reason_phrase = "OK" if response.is_success else "Error"
        response.text = text or (str(json_data) if json_data else "")
        response.json.return_value = json_data
        response.headers = httpx.Headers({})
        return response
    return _make_response


@pytest.mark.asyncio
async def test_list_contracts_success(mock_response):
    """Test successful contract listing."""
    expected = [
        {"contract_id": "C123", "event_count": 10, "first_seen_ledger": 1, "last_seen_ledger": 100}
    ]
    
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, expected)
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            contracts = await client.list_contracts()
            
        assert contracts == expected
        mock_request.assert_called_once()


@pytest.mark.asyncio
async def test_get_interface(mock_response):
    """Test getting contract interface."""
    expected = {"functions": [{"name": "transfer"}]}
    
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, expected)
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            interface = await client.get_interface("C123")
            
        assert interface == expected


@pytest.mark.asyncio
async def test_list_events_with_filters(mock_response):
    """Test listing events with filters."""
    expected = [{"event_id": "E1", "event_name": "transfer"}]
    
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, expected)
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            events = await client.list_events(
                "C123",
                limit=10,
                offset=0,
                event_name="transfer"
            )
            
        assert events == expected
        # Verify query parameters were included
        call_args = mock_request.call_args
        assert "limit=10" in call_args[0][1]
        assert "event_name=transfer" in call_args[0][1]


@pytest.mark.asyncio
async def test_call_function(mock_response):
    """Test calling a contract function."""
    expected = {
        "contract_id": "C123",
        "function": "balance",
        "result": 1000,
        "simulated_at_ledger": 100
    }
    
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, expected)
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            result = await client.call("C123", {
                "function": "balance",
                "args": {"address": "G123"}
            })
            
        assert result == expected


@pytest.mark.asyncio
async def test_error_handling(mock_response):
    """Test error handling for non-2xx responses."""
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(404, {"error": "Contract not found"})
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            with pytest.raises(LumenqraphError) as exc_info:
                await client.get_interface("C999")
            
        assert exc_info.value.status == 404
        assert "Contract not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_retry_on_503(mock_response):
    """Test retry logic for 503 errors."""
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        # First call fails with 503, second succeeds
        mock_request.side_effect = [
            mock_response(503, {"error": "Service Unavailable"}),
            mock_response(200, [{"contract_id": "C123"}])
        ]
        
        async with LumenqraphClient(
            base_url="http://localhost:8080",
            retry={"max_retries": 3, "base_delay_ms": 10}
        ) as client:
            contracts = await client.list_contracts()
            
        assert len(contracts) == 1
        assert mock_request.call_count == 2


@pytest.mark.asyncio
async def test_graphql_query(mock_response):
    """Test GraphQL query execution."""
    expected_data = {"contract": {"contractId": "C123", "eventCount": 10}}
    
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, {"data": expected_data})
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            result = await client.graphql(
                "query { contract(id: $id) { contractId eventCount } }",
                {"id": "C123"}
            )
            
        assert result == expected_data


@pytest.mark.asyncio
async def test_graphql_error(mock_response):
    """Test GraphQL error handling."""
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, {
            "errors": [{"message": "Contract not found"}]
        })
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            with pytest.raises(LumenqraphError) as exc_info:
                await client.graphql("query { contract(id: $id) }", {"id": "C999"})
            
        assert "Contract not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_paginate_events(mock_response):
    """Test event pagination."""
    page1 = {
        "nodes": [{"eventId": "E1"}, {"eventId": "E2"}],
        "has_next_page": True,
        "end_cursor": "cursor1"
    }
    page2 = {
        "nodes": [{"eventId": "E3"}],
        "has_next_page": False,
        "end_cursor": None
    }
    
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.side_effect = [
            mock_response(200, {"data": {"events": {
                "edges": [
                    {"cursor": "c1", "node": {"eventId": "E1"}},
                    {"cursor": "c2", "node": {"eventId": "E2"}}
                ],
                "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"}
            }}}),
            mock_response(200, {"data": {"events": {
                "edges": [{"cursor": "c3", "node": {"eventId": "E3"}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}
            }}})
        ]
        
        async with LumenqraphClient(base_url="http://localhost:8080") as client:
            events = []
            async for event in client.paginate_events("C123", page_size=2):
                events.append(event)
            
        assert len(events) == 3
        assert events[0]["eventId"] == "E1"
        assert events[2]["eventId"] == "E3"


@pytest.mark.asyncio
async def test_api_key_header(mock_response):
    """Test that API key is included in headers."""
    with patch("httpx.AsyncClient.request", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response(200, [])
        
        async with LumenqraphClient(
            base_url="http://localhost:8080",
            api_key="test-key-123"
        ) as client:
            await client.list_contracts()
            
        call_kwargs = mock_request.call_args[1]
        assert call_kwargs["headers"]["x-api-key"] == "test-key-123"


@pytest.mark.asyncio
async def test_context_manager():
    """Test client as async context manager."""
    client = LumenqraphClient(base_url="http://localhost:8080")
    
    async with client:
        assert client._get_client() is not None
    
    # Client should be closed after context exit
    # Verify by checking if a new client is created on next access
    client2 = client._get_client()
    assert client2 is not None
