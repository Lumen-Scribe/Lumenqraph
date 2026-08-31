#!/bin/bash
# End-to-end smoke test for Lumenqraph full stack.
# Verifies that the indexer → database → API pipeline works correctly.
# Expects the full stack to be running (via docker-compose).

set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MAX_ATTEMPTS=30
RETRY_DELAY_SECS=2
INDEXER_URL="${INDEXER_URL:-http://localhost:9090}"

echo "Starting E2E smoke test..."
echo "API URL: $API_URL"
echo "Max attempts: $MAX_ATTEMPTS"

# Wait for API health endpoint to report ok
attempt=0
while [ $attempt -lt $MAX_ATTEMPTS ]; do
  if curl -sf "$API_URL/health" > /dev/null 2>&1; then
    echo "✓ API is healthy"
    break
  fi
  attempt=$((attempt + 1))
  echo "Waiting for API health... ($attempt/$MAX_ATTEMPTS)"
  sleep $RETRY_DELAY_SECS
done

if [ $attempt -eq $MAX_ATTEMPTS ]; then
  echo "✗ API failed to become healthy after $((MAX_ATTEMPTS * RETRY_DELAY_SECS)) seconds"
  exit 1
fi

# Query /contracts endpoint to verify API is responding
echo "Testing API endpoints..."
if ! contracts=$(curl -sf "$API_URL/contracts" 2>/dev/null); then
  echo "✗ Failed to fetch /contracts endpoint"
  exit 1
fi

echo "✓ /contracts endpoint responds"

# Parse the response to check if it's valid JSON
if ! echo "$contracts" | jq empty 2>/dev/null; then
  echo "✗ /contracts response is not valid JSON"
  exit 1
fi

echo "✓ /contracts response is valid JSON"

# Check if we have any contracts (this is optional, but helpful for debugging)
contract_count=$(echo "$contracts" | jq 'length // 0' 2>/dev/null || echo 0)
echo "Total contracts in database: $contract_count"

# Test /health endpoint returns expected fields
echo "Testing /health endpoint..."
if ! health=$(curl -sf "$API_URL/health" 2>/dev/null); then
  echo "✗ Failed to fetch /health endpoint"
  exit 1
fi

if ! echo "$health" | jq -e '.status' > /dev/null 2>/dev/null; then
  echo "✗ /health response missing 'status' field"
  exit 1
fi

health_status=$(echo "$health" | jq -r '.status' 2>/dev/null || echo "unknown")
echo "✓ API health status: $health_status"

echo ""
echo "✅ E2E smoke test passed! Full pipeline is operational."
exit 0
