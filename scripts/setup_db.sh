#!/usr/bin/env bash
# Apply migrations manually via psql. Normally the indexer applies them on
# startup; use this for an API/webhooks-only deploy where no indexer runs.
# Usage: DATABASE_URL=... ./scripts/setup_db.sh
set -euo pipefail
: "${DATABASE_URL:?set DATABASE_URL}"
DIR="$(cd "$(dirname "$0")/../migrations" && pwd)"
for f in "$DIR"/*.sql; do
  echo "applying $(basename "$f")"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done
echo "done"
