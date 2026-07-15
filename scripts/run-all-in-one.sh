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

set -uo pipefail

# Render (and most PaaS) inject the port to listen on as $PORT; the API reads
# $API_BIND_ADDR. Bridge them, without overriding an explicit setting.
if [[ -n "${PORT:-}" && -z "${API_BIND_ADDR:-}" ]]; then
  export API_BIND_ADDR="0.0.0.0:${PORT}"
fi

lumenqraph-indexer &
indexer=$!
lumenqraph-api &
api=$!

shutdown() {
  trap - TERM INT
  # Both handle SIGTERM and stop cleanly (the indexer commits its cursor first).
  kill -TERM "$indexer" "$api" 2>/dev/null || true
  wait
}
trap shutdown TERM INT

# Wake on whichever process exits first; its status becomes the container's.
wait -n
status=$?
echo "run-all-in-one: a process exited (status ${status}); stopping the container" >&2
kill -TERM "$indexer" "$api" 2>/dev/null || true
wait
exit "${status}"
