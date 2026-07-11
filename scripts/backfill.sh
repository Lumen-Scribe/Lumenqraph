#!/usr/bin/env bash
# One-shot historical catch-up from a start ledger to the current tip.
# Usage: ./scripts/backfill.sh <start_ledger>
# Note: bounded by RPC retention (~7 days); older ledgers are clamped.
set -euo pipefail
START="${1:?usage: backfill.sh <start_ledger>}"
cargo run -p lumenqraph-indexer --release -- backfill "$START"
