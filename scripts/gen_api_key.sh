#!/usr/bin/env bash
# Generate an API key, store only its SHA-256 hash, and print the key once.
# Usage: ./scripts/gen_api_key.sh [name] [tier] [rate_per_min]
# Environment: DATABASE_URL must be set (recommended: via .pgpass for secure password handling)
set -euo pipefail

NAME="${1:-default}"
TIER="${2:-free}"
LIMIT="${3:-60}"
: "${DATABASE_URL:?set DATABASE_URL}"

KEY="lqk_$(head -c 24 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 32)"
HASH=$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)

# Use PGPASSWORD for password handling instead of embedding in DATABASE_URL.
# This keeps credentials out of process listings (visible via ps/proc).
# For production, use a .pgpass file: ~/.pgpass with mode 0600 containing:
#   hostname:port:database:username:password
export PGPASSWORD="${PGPASSWORD:-}"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO api_keys (key_hash, name, tier, rate_limit_per_min)
   VALUES ('$HASH', '$NAME', '$TIER', $LIMIT)"

echo "API key (store it now — only shown once):"
echo "  $KEY"
