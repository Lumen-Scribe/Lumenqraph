#!/usr/bin/env bash
# Apply migrations manually via psql.
#
# Prefer `lumenqraph-indexer migrate` (or `make migrate`): it records each
# migration in _sqlx_migrations, which every service checks at startup. This
# script does NOT record them, so the API, webhooks and MCP server will refuse
# to start against a database set up only with it. Keep it for inspecting or
# hand-applying SQL.
# Usage: DATABASE_URL=... ./scripts/setup_db.sh
set -euo pipefail
: "${DATABASE_URL:?set DATABASE_URL}"
DIR="$(cd "$(dirname "$0")/../migrations" && pwd)"
for f in "$DIR"/*.sql; do
  echo "applying $(basename "$f")"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done
echo "done"
