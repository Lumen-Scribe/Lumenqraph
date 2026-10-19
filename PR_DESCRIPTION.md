# Infrastructure improvements and testing enhancements

## Summary

Implements three infrastructure improvements and testing enhancements to support local development, production safety, and codec correctness.

## Changes

### #93: Demo dataset / seed script for local exploration

- Added `scripts/seed.sql` with realistic demo data (SEP-41 token events, interface, state, and per-holder balances)
- Added `scripts/seed.sh` wrapper script for easy execution
- Added `make seed` target to Makefile
- Zero-data first-run is now solved: `make seed` populates a working dataset in seconds
- Explorer and API show meaningful data immediately after seeding

**Usage:**
```bash
make db
make seed
cargo run -p lumenqraph-api
# Visit localhost:8080/contracts and see demo data
```

### #94: Concurrency guard for multiple indexer instances

- Added Postgres advisory lock (`pg_advisory_lock`) to prevent concurrent indexer instances from racing
- Lock ID: `0x6c756d656e717261` ("lumenqra" as i64)
- Only one indexer runs as leader; others block and become hot standbys
- Guards both migrations and polling to prevent: duplicate migration runs, redundant RPC calls, racing cursor updates
- Lock is automatically released on shutdown (normal exit, backfill, or deep-backfill)
- Documented single-writer guarantee in code comments

**Behavior:**
- First instance acquires lock immediately and becomes active
- Second instance logs "blocking until it releases" and waits
- On leader failure, a standby takes over automatically

### #95: Config validation with range clamping at startup

- Added `clamp_with_warning()` helper to validate and clamp numeric config at load time
- `PAGE_SIZE`: clamped to RPC bounds (1–10000) with clear log message
- `POLL_INTERVAL_SECS`: minimum 1 second
- `MAX_CATCHUP_LEDGERS`: minimum 1
- `RETENTION_LEDGERS`: rejects negatives, clamps to 0
- `REORG_OVERLAP_LEDGERS`: rejects negatives, clamps to 0
- Out-of-range values log warnings at startup (before any work happens)
- Added unit tests for all boundary conditions

**Why:** `PAGE_SIZE=0` or `100000` now fails fast with "PAGE_SIZE is below minimum; clamping to 1" instead of a confusing RPC error at runtime.

### #96: Comprehensive strkey (base32 + CRC16) negative/edge tests

- Added 7 new edge-case tests for the strkey encoder/decoder:
  - `strkey_bad_crc`: Flip a payload byte to corrupt checksum → rejected
  - `strkey_truncated_input`: Various truncated lengths → rejected
  - `strkey_overlength_input`: Extra characters → rejected
  - `strkey_wrong_version_byte`: G-strkey vs C-strkey mismatch → rejected
  - `strkey_roundtrip_g_and_c`: Valid G/C keys round-trip correctly
  - `strkey_invalid_base32_chars`: Invalid chars ('0', '1', '8', '!') → rejected
  - Extended `invalid_contract_ids_rejected` with explicit CRC corruption

**Coverage:** Bad CRC, wrong length, wrong version byte, invalid alphabet all return typed errors (never panic or wrong-but-accepted keys).

## Testing

- All new tests pass: `cargo test --workspace`
- Config clamping tests verify boundary behavior
- Strkey tests cover all documented failure modes from #96
- Seed script tested locally: populates 4 events, 1 spec, 1 state row, 2 balance rows

## Related Issues

Closes #93  
Closes #94  
Closes #95  
Closes #96
