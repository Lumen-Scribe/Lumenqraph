#!/usr/bin/env bash
# Generate an API key, store only its SHA-256 hash, and print the key once.
# Usage: DATABASE_URL=... ./scripts/gen_api_key.sh [name] [tier] [rate_per_min]
set -euo pipefail

NAME="${1:-default}"
TIER="${2:-free}"
LIMIT="${3:-60}"
: "${DATABASE_URL:?set DATABASE_URL}"

KEY="lqk_$(head -c 24 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 32)"
HASH=$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO api_keys (key_hash, name, tier, rate_limit_per_min)
   VALUES ('$HASH', '$NAME', '$TIER', $LIMIT)"

echo "API key (store it now — only shown once):"
echo "  $KEY"
