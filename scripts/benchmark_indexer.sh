#!/bin/bash

# Lumenqraph Indexer Benchmark Runner
#
# Runs the three isolated benchmark phases for the indexer pipeline using a
# mock Soroban RPC server instead of a live network endpoint. This eliminates
# network-latency variance so results are stable and comparable across runs.
#
# Phases
# ------
#   1. xdr_decode     — XDR → JSON decode throughput (CPU-only, no I/O)
#   2. db_insert      — database write throughput (requires Postgres)
#   3. enrichment     — spec-driven event enrichment throughput (CPU-only)
#
# Usage
# -----
#   ./scripts/benchmark_indexer.sh [OPTIONS]
#
# Options
#   --phase PHASE        Run a single phase (xdr_decode | db_insert | enrichment).
#                        Default: run all three phases.
#   --events N           Number of synthetic events to bench per phase.
#                        Default: 10000
#   --baseline FILE      Compare against a saved baseline JSON file.
#   --save-baseline FILE Write this run's results to FILE for future comparison.
#   --db-url URL         Postgres connection URL (required for db_insert phase).
#                        Falls back to DATABASE_URL env var or .env file.
#   -h | --help          Print this message.
#
# Requirements
# ------------
#   • Rust toolchain (stable)
#   • Postgres (only for the db_insert phase)
#   • jq (for baseline comparison)
#
# Examples
# --------
#   # Run all phases, saving a baseline
#   ./scripts/benchmark_indexer.sh --save-baseline benchmarks/baseline.json
#
#   # Only benchmark the decode step against a previous baseline
#   ./scripts/benchmark_indexer.sh --phase xdr_decode --baseline benchmarks/baseline.json
#
#   # CI mode: fail if any phase is >10% slower than the baseline
#   ./scripts/benchmark_indexer.sh --baseline benchmarks/baseline.json

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
PHASE="all"
EVENTS=10000
BASELINE_FILE=""
SAVE_BASELINE=""
DB_URL="${DATABASE_URL:-}"
BENCHMARK_DIR="benchmarks"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# ── colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ── argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --phase)      PHASE="$2";          shift 2 ;;
        --events)     EVENTS="$2";         shift 2 ;;
        --baseline)   BASELINE_FILE="$2";  shift 2 ;;
        --save-baseline) SAVE_BASELINE="$2"; shift 2 ;;
        --db-url)     DB_URL="$2";         shift 2 ;;
        -h|--help)
            sed -n '3,40p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; echo "Run $0 --help for usage."; exit 1 ;;
    esac
done

# ── load .env if DATABASE_URL is still unset ─────────────────────────────────
if [[ -z "$DB_URL" && -f ".env" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^DATABASE_URL= ]] && DB_URL="${line#DATABASE_URL=}" && break
    done < ".env"
fi

# ── helpers ───────────────────────────────────────────────────────────────────
header()  { echo -e "\n${CYAN}══ $* ══${NC}"; }
ok()      { echo -e "  ${GREEN}✓${NC} $*"; }
warn()    { echo -e "  ${YELLOW}⚠${NC}  $*"; }
fail()    { echo -e "  ${RED}✗${NC} $*"; }
section() { echo -e "  ${YELLOW}→${NC} $*"; }

# ── print banner ──────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Lumenqraph Indexer Benchmark (mock-RPC mode)   ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════╝${NC}"
echo "  Timestamp : $TIMESTAMP"
echo "  Phase     : $PHASE"
echo "  Events    : $EVENTS"
echo ""

mkdir -p "$BENCHMARK_DIR"
RESULTS_FILE="${BENCHMARK_DIR}/results_${TIMESTAMP}.json"

# ── build in release mode (criterion benches need release) ───────────────────
header "Building (release)"
if cargo build --release -p lumenqraph-indexer 2>&1 | grep -E "^error" ; then
    fail "Build failed — aborting"
    exit 1
fi
ok "Build succeeded"

# ── phase: xdr_decode ─────────────────────────────────────────────────────────
run_xdr_decode() {
    header "Phase 1 / 3 — XDR decode throughput"
    section "Running criterion benchmark: xdr_decode"
    echo ""

    # criterion writes machine-readable output to target/criterion/<name>/
    cargo bench \
        --bench bench_indexer \
        -- xdr_decode \
        2>&1 | grep -E "(xdr_decode|time|thrpt|ns/iter)" | head -20

    # Extract the mean from criterion's estimates.json if available
    local est
    est=$(find target/criterion -name "estimates.json" -path "*/xdr_decode/*" 2>/dev/null | head -1)
    if [[ -n "$est" ]]; then
        local mean_ns
        mean_ns=$(python3 -c "import json,sys; d=json.load(open('$est')); print(d['mean']['point_estimate'])" 2>/dev/null || echo "N/A")
        ok "Mean latency per event: ${mean_ns} ns"
    fi
}

# ── phase: enrichment ─────────────────────────────────────────────────────────
run_enrichment() {
    header "Phase 2 / 3 — Event enrichment throughput"
    section "Running criterion benchmark: enrichment"
    echo ""

    cargo bench \
        --bench bench_indexer \
        -- enrichment \
        2>&1 | grep -E "(enrichment|time|thrpt|ns/iter)" | head -20

    local est
    est=$(find target/criterion -name "estimates.json" -path "*/enrichment/*" 2>/dev/null | head -1)
    if [[ -n "$est" ]]; then
        local mean_ns
        mean_ns=$(python3 -c "import json,sys; d=json.load(open('$est')); print(d['mean']['point_estimate'])" 2>/dev/null || echo "N/A")
        ok "Mean latency per event: ${mean_ns} ns"
    fi
}

# ── phase: db_insert ─────────────────────────────────────────────────────────
run_db_insert() {
    header "Phase 3 / 3 — Database insert throughput (mock-RPC end-to-end)"
    if [[ -z "$DB_URL" ]]; then
        warn "DATABASE_URL not set — skipping db_insert phase"
        warn "Pass --db-url or set DATABASE_URL to run this phase"
        return
    fi

    section "Checking database connectivity"
    if ! psql "$DB_URL" -c "SELECT 1" > /dev/null 2>&1; then
        fail "Cannot connect to database: $DB_URL"
        warn "Skipping db_insert phase"
        return
    fi
    ok "Database connected"

    section "Running end-to-end mock-RPC benchmark"
    echo ""

    # The Rust integration test suite contains the mock RPC + fetch_and_store
    # path. We run it with a timing wrapper here.
    # The criterion bench_indexer::db_insert benchmark handles its own
    # pool setup and teardown — it uses TEST_DATABASE_URL.
    TEST_DATABASE_URL="$DB_URL" cargo bench \
        --bench bench_indexer \
        -- db_insert \
        2>&1 | grep -E "(db_insert|time|thrpt|ns/iter|events/s)" | head -20

    ok "DB insert phase complete"
}

# ── collect results ───────────────────────────────────────────────────────────
collect_results() {
    header "Collecting results"

    local results="{}"
    local date_str
    date_str=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    # Parse criterion's estimates.json for each benchmark if present
    for bench_name in xdr_decode enrichment db_insert; do
        local est
        est=$(find target/criterion -name "estimates.json" -path "*/${bench_name}/*" 2>/dev/null | head -1)
        if [[ -n "$est" ]] && command -v python3 &>/dev/null; then
            local mean_ns median_ns
            mean_ns=$(python3 -c "
import json
d = json.load(open('$est'))
print(d['mean']['point_estimate'])
" 2>/dev/null || echo "null")
            results=$(echo "$results" | python3 -c "
import json, sys
d = json.load(sys.stdin)
d['$bench_name'] = {'mean_ns': $mean_ns, 'timestamp': '$date_str', 'events': $EVENTS}
print(json.dumps(d, indent=2))
" 2>/dev/null || echo "$results")
        fi
    done

    echo "$results" > "$RESULTS_FILE"
    ok "Results saved to $RESULTS_FILE"
    echo "$results"
}

# ── baseline comparison ───────────────────────────────────────────────────────
compare_baseline() {
    local current_file="$1"
    local baseline_file="$2"

    if ! command -v python3 &>/dev/null; then
        warn "python3 not found — skipping baseline comparison"
        return
    fi

    header "Comparing against baseline: $baseline_file"

    python3 - "$current_file" "$baseline_file" <<'PYEOF'
import json, sys

current  = json.load(open(sys.argv[1]))
baseline = json.load(open(sys.argv[2]))

REGRESSION_THRESHOLD = 0.10  # 10%
any_regression = False

for bench in ("xdr_decode", "enrichment", "db_insert"):
    if bench not in current or bench not in baseline:
        print(f"  ⚠  {bench}: not present in both runs — skipping")
        continue

    cur_ns  = current[bench]["mean_ns"]
    base_ns = baseline[bench]["mean_ns"]

    if cur_ns is None or base_ns is None:
        print(f"  ⚠  {bench}: missing data — skipping")
        continue

    delta_pct = (cur_ns - base_ns) / base_ns * 100
    direction = "slower" if delta_pct > 0 else "faster"
    symbol    = "✗" if delta_pct > REGRESSION_THRESHOLD * 100 else "✓"

    print(f"  {symbol}  {bench}: {cur_ns/1e6:.3f} ms  (baseline {base_ns/1e6:.3f} ms, "
          f"{abs(delta_pct):.1f}% {direction})")

    if delta_pct > REGRESSION_THRESHOLD * 100:
        any_regression = True
        print(f"       REGRESSION: >{REGRESSION_THRESHOLD*100:.0f}% slower than baseline")

if any_regression:
    print("\nOne or more benchmarks regressed by more than 10%.")
    sys.exit(1)
else:
    print("\nAll benchmarks within acceptable bounds (< 10% regression).")
PYEOF
}

# ── main ──────────────────────────────────────────────────────────────────────
case "$PHASE" in
    all)
        run_xdr_decode
        run_enrichment
        run_db_insert
        ;;
    xdr_decode) run_xdr_decode ;;
    enrichment) run_enrichment ;;
    db_insert)  run_db_insert  ;;
    *)
        fail "Unknown phase: $PHASE (must be all | xdr_decode | enrichment | db_insert)"
        exit 1
        ;;
esac

RESULTS=$(collect_results)

if [[ -n "$SAVE_BASELINE" ]]; then
    cp "$RESULTS_FILE" "$SAVE_BASELINE"
    ok "Baseline saved to $SAVE_BASELINE"
fi

if [[ -n "$BASELINE_FILE" ]]; then
    compare_baseline "$RESULTS_FILE" "$BASELINE_FILE"
fi

echo ""
echo -e "${GREEN}Benchmark complete.${NC} Results: $RESULTS_FILE"
