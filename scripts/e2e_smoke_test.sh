#!/bin/bash
# End-to-end smoke test for Lumenqraph full stack.
# Verifies that the indexer → database → API pipeline works correctly.
# Expects the full stack to be running (via docker-compose).

set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MAX_ATTEMPTS=30
RETRY_DELAY_SECS=2
INDEXER_URL="${INDEXER_URL:-http://localhost:9090}"
# Deadline (in seconds) for the indexer to advance last_processed_ledger.
LEDGER_ADVANCE_TIMEOUT="${LEDGER_ADVANCE_TIMEOUT:-120}"

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

# Assert /contracts is a non-empty array of indexed contracts.
if ! echo "$contracts" | jq -e 'type == "array"' > /dev/null 2>&1; then
  echo "✗ /contracts response is not a JSON array"
  exit 1
fi

contract_count=$(echo "$contracts" | jq 'length')
echo "Total contracts in database: $contract_count"

if [ "$contract_count" -eq 0 ]; then
  echo "✗ /contracts returned no indexed contracts"
  exit 1
fi

echo "✓ /contracts returned $contract_count indexed contract(s)"

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

health_status=$(echo "$health" | jq -r '.status')
echo "✓ API health status: $health_status"

# Wait until the indexer advances last_processed_ledger, proving the
# indexer → database pipeline is actually making progress.
echo "Waiting for indexer to advance last_processed_ledger (timeout: ${LEDGER_ADVANCE_TIMEOUT}s)..."
if ! echo "$health" | jq -e '.last_processed_ledger != null' > /dev/null 2>&1; then
  echo "✗ /health response missing 'last_processed_ledger' field"
  exit 1
fi

initial_ledger=$(echo "$health" | jq -r '.last_processed_ledger')
echo "Initial last_processed_ledger: $initial_ledger"

ledger_deadline=$((SECONDS + LEDGER_ADVANCE_TIMEOUT))
advanced=false
while [ $SECONDS -lt $ledger_deadline ]; do
  if ! health=$(curl -sf "$API_URL/health" 2>/dev/null); then
    echo "Waiting for /health... (retrying)"
    sleep $RETRY_DELAY_SECS
    continue
  fi
  current_ledger=$(echo "$health" | jq -r '.last_processed_ledger // empty')
  if [ -n "$current_ledger" ] && [ "$current_ledger" -gt "$initial_ledger" ]; then
    echo "✓ last_processed_ledger advanced: $initial_ledger -> $current_ledger"
    advanced=true
    break
  fi
  echo "Waiting for last_processed_ledger to advance... (current: ${current_ledger:-unknown})"
  sleep $RETRY_DELAY_SECS
done

if [ "$advanced" != "true" ]; then
  echo "✗ last_processed_ledger did not advance within ${LEDGER_ADVANCE_TIMEOUT}s"
  exit 1
fi

# Assert the first indexed contract has at least one event.
first_contract_id=$(echo "$contracts" | jq -r '.[0].id // .[0].contract_id // empty')
if [ -z "$first_contract_id" ]; then
  echo "✗ Could not determine contract id from /contracts response"
  exit 1
fi

echo "Testing /contracts/$first_contract_id/events?limit=1 ..."
events_url="$API_URL/contracts/$first_contract_id/events?limit=1"
http_code=$(curl -s -o /tmp/e2e_events.json -w '%{http_code}' "$events_url")
if [ "$http_code" != "200" ]; then
  echo "✗ $events_url returned HTTP $http_code (expected 200)"
  exit 1
fi

if ! jq -e 'type == "array"' /tmp/e2e_events.json > /dev/null 2>&1; then
  echo "✗ events response is not a JSON array"
  exit 1
fi

event_count=$(jq 'length' /tmp/e2e_events.json)
if [ "$event_count" -lt 1 ]; then
  echo "✗ events response contained no rows for contract $first_contract_id"
  exit 1
fi

echo "✓ /contracts/$first_contract_id/events returned $event_count event(s)"

echo ""
echo "✅ E2E smoke test passed! Full pipeline is operational."
exit 0
