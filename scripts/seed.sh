#!/usr/bin/env bash
# Populate a realistic demo dataset for local exploration without a live RPC.
#
# Usage:
#   DATABASE_URL=... ./scripts/seed.sh
#
# Override the base ledger (defaults to an estimate of the current Stellar
# mainnet tip so that /health reports a near-zero lag_ledgers value):
#   SEED_LEDGER=55000000 DATABASE_URL=... ./scripts/seed.sh
set -euo pipefail

: "${DATABASE_URL:=postgres://lumenqraph:lumenqraph@localhost:5432/lumenqraph}"

# ---------------------------------------------------------------------------
# Determine SEED_LEDGER.
#
# Stellar mainnet produces ~1 ledger every 5 seconds.  We approximate the
# current tip so that the seeded indexer_cursor is close to reality and
# /health does not report a misleadingly large lag_ledgers value.
#
# Genesis ledger (seq 2) closed at 2015-09-30 18:00:00 UTC.
# tip ≈ 2 + (now_unix - genesis_unix) / 5
# ---------------------------------------------------------------------------
if [[ -z "${SEED_LEDGER:-}" ]]; then
  GENESIS_UNIX=1443636000  # 2015-09-30 18:00:00 UTC
  NOW_UNIX=$(date +%s)
  SEED_LEDGER=$(( 2 + (NOW_UNIX - GENESIS_UNIX) / 5 ))
fi

echo "Using SEED_LEDGER=${SEED_LEDGER}"
echo "Applying seed data to ${DATABASE_URL}..."

# Pass the ledger value into PostgreSQL as a session-level GUC so that
# seed.sql can read it via current_setting('seed.ledger', true).
psql "$DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -c "SET seed.ledger = ${SEED_LEDGER};" \
  -f "$(dirname "$0")/seed.sql"

# ---------------------------------------------------------------------------
# Advance the indexer cursor to the tip ledger so the /health endpoint
# reports near-zero lag immediately after seeding.
# ---------------------------------------------------------------------------
echo "Setting indexer_cursor to ledger ${SEED_LEDGER}..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO indexer_cursor (id, last_processed_ledger, updated_at)
VALUES (1, ${SEED_LEDGER}, NOW())
ON CONFLICT (id) DO UPDATE
    SET last_processed_ledger = ${SEED_LEDGER},
        updated_at             = NOW();
SQL

echo "Done! The explorer and API should now show meaningful data."
echo "Seed ledger: ${SEED_LEDGER}  (lag_ledgers in /health should be near zero)"
