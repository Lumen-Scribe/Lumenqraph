#!/usr/bin/env bash
# Generate an API key, store only its SHA-256 hash, and print the key once.
#
# Usage:
#   ./scripts/gen_api_key.sh [--expires-in DURATION] [--scopes LIST] [name] [tier] [rate_per_min]
#
#   name           Label stored with the key (default: default)
#   tier           Tier label stored with the key (default: free)
#   rate_per_min   Requests per minute, a positive integer (default: 60)
#   --expires-in   Expire the key after N days or N hours, e.g. 90d or 12h (default: never)
#   --scopes       Comma-separated scopes: read, rpc, webhooks:write, admin (default: read)
#
# Examples:
#   ./scripts/gen_api_key.sh "ci bot" internal 600 --scopes read,rpc --expires-in 90d
#
# Every value is passed to psql as a variable and quoted there, never spliced
# into the SQL text, so a name containing quotes is stored literally.
#
# Environment: DATABASE_URL must be set (recommended: via .pgpass for secure password handling)
set -euo pipefail

usage() {
  sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
}

EXPIRES_IN=""
SCOPES="read"
POSITIONAL=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --expires-in)   EXPIRES_IN="${2:-}"; shift 2 ;;
    --expires-in=*) EXPIRES_IN="${1#*=}"; shift ;;
    --scopes)       SCOPES="${2:-}"; shift 2 ;;
    --scopes=*)     SCOPES="${1#*=}"; shift ;;
    -h|--help)      usage; exit 0 ;;
    --)             shift; POSITIONAL+=("$@"); break ;;
    -*)             echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)              POSITIONAL+=("$1"); shift ;;
  esac
done

NAME="${POSITIONAL[0]-default}"
TIER="${POSITIONAL[1]-free}"
LIMIT="${POSITIONAL[2]-60}"
: "${DATABASE_URL:?set DATABASE_URL}"

fail() { echo "error: $*" >&2; exit 2; }

# --- validation -------------------------------------------------------------
[ -n "$NAME" ] && [ "${#NAME}" -le 200 ] || fail "name must be 1-200 characters"
case "$NAME" in *$'\n'*|*$'\r'*|*$'\t'*) fail "name must not contain newlines or tabs" ;; esac

[[ "$TIER" =~ ^[A-Za-z0-9_.-]{1,64}$ ]] || fail "tier must be 1-64 characters of letters, digits, '_', '.' or '-'"

# Integer, not a leading zero, and within a 32-bit column.
[[ "$LIMIT" =~ ^[1-9][0-9]{0,8}$ ]] || fail "rate_per_min must be a positive integer, got '$LIMIT'"

INTERVAL=""
if [ -n "$EXPIRES_IN" ]; then
  if [[ "$EXPIRES_IN" =~ ^([1-9][0-9]{0,3})([dh])$ ]]; then
    case "${BASH_REMATCH[2]}" in
      d) INTERVAL="${BASH_REMATCH[1]} days" ;;
      h) INTERVAL="${BASH_REMATCH[1]} hours" ;;
    esac
  else
    fail "--expires-in must look like 90d or 12h, got '$EXPIRES_IN'"
  fi
fi

ALLOWED_SCOPES=" read rpc webhooks:write admin "
[[ "$SCOPES" =~ ^[a-z:_,]+$ ]] || fail "--scopes must be a comma-separated list, got '$SCOPES'"
IFS=',' read -r -a SCOPE_LIST <<< "$SCOPES"
for scope in "${SCOPE_LIST[@]}"; do
  case "$ALLOWED_SCOPES" in
    *" $scope "*) ;;
    *) fail "unknown scope '$scope' (allowed: read, rpc, webhooks:write, admin)" ;;
  esac
done

# --- key material -----------------------------------------------------------
# 24 random bytes as 48 hex characters (192 bits), with no filtering.
KEY="lqk_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
HASH=$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)
# Prefix that identifies this key in audit logs without revealing it.
PREFIX="${KEY:0:12}"

# Use PGPASSWORD for password handling instead of embedding in DATABASE_URL.
# This keeps credentials out of process listings (visible via ps/proc).
# For production, use a .pgpass file: ~/.pgpass with mode 0600 containing:
#   hostname:port:database:username:password
export PGPASSWORD="${PGPASSWORD:-}"

# SQL is read from stdin so psql interpolates the :'name' variables. Passing it
# with -c would not interpolate them.
psql "$DATABASE_URL" -q -v ON_ERROR_STOP=1 \
  -v key_hash="$HASH" \
  -v name="$NAME" \
  -v tier="$TIER" \
  -v rate_limit="$LIMIT" \
  -v scopes="$SCOPES" \
  -v expires_in="$INTERVAL" <<'SQL'
INSERT INTO api_keys (key_hash, name, tier, rate_limit_per_min, scopes, expires_at)
VALUES (
  :'key_hash',
  :'name',
  :'tier',
  :'rate_limit'::integer,
  string_to_array(:'scopes', ','),
  now() + NULLIF(:'expires_in', '')::interval
);
SQL

echo "API key (store it now — only shown once):"
echo "  $KEY"
echo "key prefix (for audit logs): $PREFIX"
echo "name=$NAME tier=$TIER rate_per_min=$LIMIT scopes=$SCOPES expires_in=${EXPIRES_IN:-never}"
