#!/usr/bin/env bash
# Run the indexer and the API together in ONE container.
#
# Lumenqraph normally runs these as separate services (see docker-compose.full.yml
# and fly.toml), which is what you want when you can scale them independently.
# This script exists for hosts that give you a single always-on process — notably
# Render's free tier, which has no background-worker type. See docs/DEPLOYMENT.md.
#
# Both processes share one DB pool budget and one CPU here, so this trades
# isolation for fitting in a free slot. If either process exits, the container
# exits, and the platform restarts the pair — never leave the API up serving a
# silently frozen index, which would look healthy while going stale.
#
# Optionally runs a SECOND indexer+API pair against another network in the same
# container: set TESTNET_DATABASE_URL (plus TESTNET_CONTRACT_IDS etc.) and the
# testnet pair starts on an internal port, with the public API reverse-proxying
# it under /testnet (INSTANCE_MOUNTS). One container, one URL, two networks.

set -uo pipefail

# Render (and most PaaS) inject the port to listen on as $PORT; the API reads
# $API_BIND_ADDR. Bridge them, without overriding an explicit setting.
if [[ -n "${PORT:-}" && -z "${API_BIND_ADDR:-}" ]]; then
  export API_BIND_ADDR="0.0.0.0:${PORT}"
fi

pids=()

# ---- Optional testnet pair (must start before the public API so it can be
# told about the mount) -------------------------------------------------------
if [[ -n "${TESTNET_DATABASE_URL:-}" ]]; then
  testnet_port="${TESTNET_API_PORT:-8081}"
  echo "run-all-in-one: starting testnet pair (internal port ${testnet_port})" >&2

  # START_LEDGER=0 (start at the tip): a ledger number configured for the
  # primary network would be meaningless on this one.
  DATABASE_URL="${TESTNET_DATABASE_URL}" \
  RPC_URL="${TESTNET_RPC_URL:-https://soroban-testnet.stellar.org}" \
  CONTRACT_IDS="${TESTNET_CONTRACT_IDS:-}" \
  RETENTION_LEDGERS="${TESTNET_RETENTION_LEDGERS:-${RETENTION_LEDGERS:-0}}" \
  START_LEDGER=0 \
    lumenqraph-indexer &
  pids+=($!)

  # No explorer on the inner API: the outer one serves the UI for both.
  DATABASE_URL="${TESTNET_DATABASE_URL}" \
  RPC_URL="${TESTNET_RPC_URL:-https://soroban-testnet.stellar.org}" \
  API_BIND_ADDR="127.0.0.1:${testnet_port}" \
  EXPLORER_DIR="/nonexistent" \
    lumenqraph-api &
  pids+=($!)

  export INSTANCE_MOUNTS="${INSTANCE_MOUNTS:-testnet=http://127.0.0.1:${testnet_port}}"
fi

# ---- Primary pair ------------------------------------------------------------
lumenqraph-indexer &
pids+=($!)
lumenqraph-api &
pids+=($!)

shutdown() {
  trap - TERM INT
  # All handle SIGTERM and stop cleanly (the indexers commit their cursors first).
  kill -TERM "${pids[@]}" 2>/dev/null || true
  wait
}
trap shutdown TERM INT

# Wake on whichever process exits first; its status becomes the container's.
wait -n
status=$?
echo "run-all-in-one: a process exited (status ${status}); stopping the container" >&2
kill -TERM "${pids[@]}" 2>/dev/null || true
wait
exit "${status}"
