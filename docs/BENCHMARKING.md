# Indexer Throughput Benchmarking

## Overview

This document describes how to benchmark the Lumenqraph indexer's event
ingestion pipeline.  The benchmarks are designed for **regression detection**:
they must be stable, reproducible, and comparable across machines and time.

### The core constraint: eliminate network latency

The original script timed an end-to-end run against a live Soroban RPC
endpoint.  That approach conflates three very different things:

| Source of time | Typical magnitude | Varies with |
|----------------|------------------|-------------|
| XDR decode (CPU) | < 1 µs / event | CPU speed |
| DB write latency | 1–5 ms / batch | Postgres tier, index count |
| RPC latency | 10–300 ms / page | Network, RPC load, time of day |

RPC latency dominates the total and varies wildly — two runs 10 minutes apart
from different networks can differ by 10×.  The benchmarks below strip the
network out entirely so each phase measures exactly one component.

---

## Benchmark structure

Three phases are isolated in `crates/lumenqraph-indexer/benches/bench_indexer.rs`
using [Criterion](https://github.com/bheisler/criterion.rs) for statistical
rigour (multiple iterations, outlier rejection, confidence intervals):

| Phase | What is measured | Needs DB |
|-------|-----------------|----------|
| `xdr_decode` | Base64 XDR → decoded JSON (topics + value) per event | No |
| `enrichment` | Spec-driven named/typed enrichment per event | No |
| `enrichment_nested_udt` | Enrichment of an event whose value nests a UDT | No |
| `db_insert` | UNNEST batch INSERT into Postgres | Yes |

The `xdr_decode`, `enrichment`, and `enrichment_nested_udt` phases are pure-CPU
and run anywhere. The `db_insert` phase gates on `TEST_DATABASE_URL`; if the
variable is absent the phase is silently skipped.

---

## Running benchmarks

### Prerequisites

- Rust stable (1.75+)
- For the `db_insert` phase: Postgres with migrations applied

### Run all CPU phases (no Postgres required)

```bash
cargo bench --bench bench_indexer
```

Criterion writes HTML reports to `target/criterion/`.

### Run a single phase

```bash
cargo bench --bench bench_indexer -- xdr_decode
cargo bench --bench bench_indexer -- enrichment
cargo bench --bench bench_indexer -- enrichment_nested_udt
```

### Run all three phases (including DB)

```bash
TEST_DATABASE_URL=postgres://user:pass@localhost/lumenqraph_bench \
  cargo bench --bench bench_indexer
```

### Use the wrapper script

`scripts/benchmark_indexer.sh` wraps the above with baseline save/compare:

```bash
# Run all phases and save a baseline
./scripts/benchmark_indexer.sh --save-baseline benchmarks/baseline.json

# Later: compare against the baseline (fails if any phase regressed > 10%)
./scripts/benchmark_indexer.sh --baseline benchmarks/baseline.json

# Only run the decode phase
./scripts/benchmark_indexer.sh --phase xdr_decode

# DB phase only, with an explicit URL
./scripts/benchmark_indexer.sh \
    --phase db_insert \
    --db-url postgres://user:pass@localhost/lumenqraph_bench
```

---

## Expected results

Reference figures on a commodity developer laptop (M-series, 16 GB RAM, SSD
Postgres). Actual numbers will differ; what matters is **consistency across
runs on the same machine**.

### `xdr_decode` (1 000 events per iteration)

| Metric | Value |
|--------|-------|
| Mean time | ~2 ms |
| Throughput | ~500 000 events / s |

### `enrichment` (1 000 events per iteration)

| Metric | Value |
|--------|-------|
| Mean time | ~1.5 ms |
| Throughput | ~650 000 events / s |

### `enrichment_nested_udt`

Enriches `PositionChanged { pos: Position }`, where `Position` is a struct whose
`status` field is itself a `Status` enum, against a spec that declares 100
unrelated UDTs ahead of them. This is the case that used to pay a linear scan
over the whole type list — once per nested value, recursively — before UDTs were
indexed by name in `ContractSpec::reindex`.

Run it against a saved baseline to see the delta:

```bash
# On the commit before the indexing change:
cargo bench --bench bench_indexer -- enrichment_nested_udt --save-baseline before

# On the change itself:
cargo bench --bench bench_indexer -- enrichment_nested_udt --baseline before
```

Criterion prints the per-batch `change:` line; the improvement grows with the
number of declared types and with how deeply the event nests them.

### `db_insert` (100 events per batch, local Postgres)

| Metric | Value |
|--------|-------|
| Mean time per batch | ~5 ms |
| Throughput | ~20 000 events / s |

---

## `events` storage and index footprint

Ingestion throughput and disk footprint on the `events` table are dominated by
index maintenance.  Each insert touches up to nine indexes, two or three of
them GIN, which are the most expensive to maintain.  The GIN index on
`decoded_value` is only useful for ad-hoc containment queries that no shipped
API route issues (routes filter on `enriched` and `decoded_topics`), so it is
**not created by default**.

### Measuring index usage and size

Run the following against a mainnet sample (or any representative database) to
see which indexes are actually used and how much space each consumes:

```sql
-- Index usage: number of scans per index on the events table.
SELECT indexrelname AS index_name,
       idx_scan,
       idx_tup_read,
       idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname = 'events'
ORDER BY idx_scan DESC;

-- Index size in bytes (and human-readable).
SELECT indexrelname AS index_name,
       pg_relation_size(indexrelid) AS size_bytes,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE relname = 'events'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Table size and total relation size (table + indexes + toast).
SELECT pg_size_pretty(pg_relation_size('events')) AS table_size,
       pg_size_pretty(pg_total_relation_size('events')) AS total_size;

-- Bytes per event (table + indexes).
SELECT pg_total_relation_size('events')::numeric / NULLIF(count(*), 0) AS bytes_per_event
FROM events;
```

### Reference numbers (mainnet sample)

Measured on a mainnet sample with the default (minimal) index set.  Absolute
values depend on the sample window and Postgres tier; the **before/after**
delta is what matters.

| Metric | Before (full indexes) | After (minimal, default) |
|--------|----------------------|--------------------------|
| Bytes / event (table + indexes) | ~1 450 B | ~1 050 B |
| `db_insert` throughput | ~20 000 events / s | ~28 000 events / s |
| GIN indexes on `events` | 3 | 1 |

Dropping the `decoded_value` GIN index removes the most expensive index to
maintain on every insert while leaving all shipped API routes unaffected.

### Enabling the extra GIN indexes for ad-hoc querying

By default only the indexes used by shipped API routes are created.  To enable
the optional GIN indexes (for ad-hoc containment queries on `decoded_value`),
set `LUMENQRAPH_JSONB_INDEXES=full` before starting the indexer; a startup task
applies the extra indexes.  The default is `minimal`.

```bash
# Default: only indexes used by shipped API routes.
LUMENQRAPH_JSONB_INDEXES=minimal lumenqraph-indexer

# Opt in to the extra GIN indexes for ad-hoc querying.
LUMENQRAPH_JSONB_INDEXES=full lumenqraph-indexer
```

Alternatively, apply the snippet directly:

```sql
-- Enable the optional GIN index on decoded_value for ad-hoc containment queries.
CREATE INDEX IF NOT EXISTS idx_events_decoded_value
    ON events USING gin (decoded_value);

-- To revert to the default (minimal) configuration:
DROP INDEX IF EXISTS idx_events_decoded_value;
```

---

## Regression detection

A regression is a **> 10% increase in mean latency** for the same batch size
compared to a saved baseline.  The wrapper script enforces this automatically
when `--baseline` is passed.

### Establishing a baseline

Run once on a known-good commit:

```bash
./scripts/benchmark_indexer.sh --save-baseline benchmarks/baseline.json
git add benchmarks/baseline.json
git commit -m "bench: establish baseline"
```

### Checking a PR

```bash
./scripts/benchmark_indexer.sh --baseline benchmarks/baseline.json
```

The script exits with a non-zero status if any phase regressed, making it
suitable as a CI step (see `.github/workflows/ci.yml`).

---

## Interpreting criterion output

```
xdr_decode/1000         time:   [1.9841 ms 1.9985 ms 2.0148 ms]
                        thrpt:  [496.33 Kelem/s 500.38 Kelem/s 503.75 Kelem/s]
                        change: [-0.5124% +0.1234% +0.7781%] (p = 0.73 > 0.05)
                        No change in performance detected.
```

| Column | Meaning |
|--------|---------|
| `[lo  mid  hi]` | 95% confidence interval for the mean |
| `change` | % change vs. previous run of this benchmark |
| `p =` | p-value from a two-sample t-test; p > 0.05 means "no detected change" |

HTML reports with plots are written to `target/criterion/<phase>/` after each
run.

---

## Adding new benchmark cases

Add new `bench_with_input` calls inside the relevant `bench_*` function in
`benches/bench_indexer.rs`.  Keeping the three-phase structure ensures each
new case measures exactly one component.

To measure a genuinely end-to-end scenario (mock RPC → decode → enrich →
insert), build on the `backfill` integration tests in
`crates/lumenqraph-indexer/src/backfill.rs` which already use the in-process
mock RPC server (`spawn_mock_rpc`).

---

## References

- Criterion user guide: <https://bheisler.github.io/criterion.rs/book/>
- PostgreSQL `EXPLAIN ANALYZE`: <https://www.postgresql.org/docs/current/sql-explain.html>
- PostgreSQL `pg_stat_user_indexes`: <https://www.postgresql.org/docs/current/monitoring-stats.html>
- Soroban event pagination: <https://developers.stellar.org/network/soroban-rpc/api-reference/methods/getEvents>
