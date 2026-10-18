# Migration Policy

This document covers how migrations are versioned, the append-only rule, and the rollback strategy.

## Versioning

Migrations live in `migrations/` and are applied by `sqlx::migrate!`. sqlx keys each migration by the integer prefix of its filename and stores it in `_sqlx_migrations.version`, which is the primary key. **Two files with the same prefix are a hard error**: on a fresh database `sqlx::migrate!()` fails outright, and on an existing database one of the files is silently treated as already applied, so its schema changes never run.

### Duplicate-version cleanup

Several parallel PRs each added their own `0017`, `0021`, `0022`, `0023` and `0024`, so `main` briefly contained duplicate versions. The cleanup rule is:

- **Keep** the version a file had when it was first applied in a release. Released databases already recorded that version, and changing it would make sqlx either re-run the file or refuse to start.
- **Move** every other file to a new number in the `0030+` range, in a strictly increasing sequence.

| Old filename | New filename | Action |
| --- | --- | --- |
| `0017_spec_versions_ledger.sql` | `0017_spec_versions_ledger.sql` | keep (released) |
| `0017_webhook_enhancements.sql` | `0030_webhook_enhancements.sql` | move |
| `0021_contract_summaries_delete.sql` | `0021_contract_summaries_delete.sql` | keep (released) |
| `0021_webhook_auto_disable_timing.sql` | `0031_webhook_auto_disable_timing.sql` | move |
| `0022_cursor_optimistic_locking.sql` | `0022_cursor_optimistic_locking.sql` | keep (released) |
| `0022_token_transfers_kind_check.sql` | `0032_token_transfers_kind_check.sql` | move |
| `0023_contract_data_unique.sql` | `0023_contract_data_unique.sql` | keep (released) |
| `0023_missed_ranges.sql` | `0033_missed_ranges.sql` | move |
| `0024_webhook_deliveries_keyset.sql` | `0024_webhook_deliveries_keyset.sql` | keep (released) |
| `0024_webhook_delivery_locking.sql` | `0034_webhook_delivery_locking.sql` | move |
| `0024_webhook_fixes_423_426.sql` | `0035_webhook_fixes_423_426.sql` | move |
| `0024_webhook_owner.sql` | `0036_webhook_owner.sql` | move |

**Moved files must be idempotent.** A database that already ran the file under its old number will run it again under the new number, so every statement in a moved migration must be safe to re-apply: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, `DROP … IF EXISTS`, and so on. Do not assume the migration runs exactly once.

### Timestamp-based versions

Integer prefixes collide whenever two PRs are open at the same time. New migrations should therefore use timestamp-based versions:

```
YYYYMMDDHHMM_short_description.sql
```

For example `202610051430_webhook_retry_backoff.sql`. Timestamps are unique per author-minute and sort correctly, so parallel PRs cannot collide. The `0030+` numbers above exist only to resolve the historical duplicates; new work should not continue that sequence.

## Append-only rule

**Released migrations are never edited, renamed or deleted.** sqlx stores a checksum per applied migration and refuses to start when a file's contents change:

```
migration 2 was previously applied but has been modified
```

Editing a released migration also causes silent schema drift: adding a column to a `CREATE TABLE IF NOT EXISTS` never touches a table that already exists, so fresh and upgraded databases end up with different schemas.

If a released migration needs to change:

1. Leave the original file byte-for-byte as it shipped.
2. Add a new forward migration that applies the change idempotently:

```sql
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS scopes TEXT[];
```

### CI enforcement

Two required checks protect this policy:

- **`migration-versions`** — fails when two files under `migrations/` share a version prefix (`ls migrations | cut -d_ -f1 | sort | uniq -d` must print nothing), and migrates both a fresh database and a database at the last release.
- **`migrations-append-only`** — compares `migrations/` against the merge base and fails when a PR modifies, renames or deletes an existing file. Genuine emergencies can bypass it with the `allow-migration-edit` pull-request label, which must be removed again before the next release.

## Rollback Strategy

### Context

Lumenqraph uses **forward-only migrations** applied via `sqlx::migrate!`. There are no automated down-migrations or reversible migration pairs (`up`/`down`). This document explains the rationale for that choice and provides guidance for operators who need to revert a bad schema change.

For how to write new migrations that don't lock large tables, and how CI enforces it, see [Writing lock-safe migrations](#writing-lock-safe-migrations).

## Why Forward-Only?

1. **Additive-by-design schema evolution**: Most migrations add new tables, columns, or indexes without modifying existing data paths. Removing a newly added column or index is always safe (the application ignores columns it doesn't query) — reverting such a migration is a no-op from the application's perspective.

2. **Data-loss risk in automated down migrations**: A migration that transforms or deletes data (e.g., changing a column type, dropping a table, or backfilling values) cannot be mechanically reversed without losing information. Pretending otherwise — by shipping a `down` migration that truncates or restores from a snapshot — creates a false sense of safety and encourages untested rollback paths.

3. **Deploy-then-migrate pattern**: In a zero-downtime deployment, the old application version coexists with the new schema for a brief window. Down migrations that remove columns or tables the old version still queries will break that old version. Forward-only migrations enforce additive changes, making staged rollouts safer.

4. **Operational clarity**: Explicitly documenting rollback procedures (below) makes the cost and risk visible, rather than hiding it behind an untested `down.sql` script that operators discover under pressure.

## Rollback Procedures

### If the migration added only indexes or new tables/columns (most cases)

**No action required.** Deploy the previous application version. It will ignore any new columns and tables it doesn't reference. The schema is forward-compatible by design.

**Optional cleanup** (if disk space is a concern): Manually drop the new index, column, or table after confirming the old version is stable:

```sql
-- Example: migration 0014 added idx_events_tx_hash
DROP INDEX IF EXISTS idx_events_tx_hash;

-- Example: migration 0005 added contract_state table
DROP TABLE IF EXISTS contract_state CASCADE;
```

Document any manual cleanup SQL for your specific migration and test it on a non-production instance first.

### If the migration modified existing data or constraints

**1. Assess the damage**: Determine what was changed:
   - Column type changed? Check if the old application can still read it.
   - Constraint tightened (e.g., `NOT NULL` added)? The old version may insert NULLs and fail.
   - Data backfilled or transformed? Reverting may require restoring from backup.

**2. Restore from backup** (if a recent point-in-time backup exists):
   - Stop all services writing to the database.
   - Restore the database to a snapshot taken before the migration.
   - Deploy the previous application version.
   - Resume ingestion. The indexer will catch up from its stored `last_processed_ledger`.

**3. Write a forward-fix migration** (if backup restoration is not feasible):
   - Create a new migration (e.g., `0021_fix_0018.sql`) that reverses the problematic change additively:
     - If a column type was changed incorrectly, add a new column with the correct type and backfill it.
     - If a constraint was too strict, relax it.
   - Deploy the fix migration and a new application version that uses the corrected schema.
   - This is slower but avoids data loss and works even if the bad migration has already been in production for days.

### If the migration ran but the application failed to deploy

This is the safest rollback scenario: the schema changed, but no production traffic ran against it.

1. Stop the indexer and API processes (prevent partial writes).
2. Manually revert the schema changes (see "added only indexes…" above).
3. Deploy the previous application version.
4. Resume services.

## Writing lock-safe migrations

`events` is by far the largest table, and `token_transfers` and `contract_data` grow with it. DDL that takes a strong lock on one of them blocks the indexer's writes (and can queue API reads behind it) for as long as the statement runs, which for an index build or a validating scan over millions of rows means minutes to hours. Every new migration must therefore be **lock-safe**, or explicitly justified.

### The CI lint

CI runs [squawk](https://squawkhq.com) over every migration a pull request adds or changes (the `migration-lint` job in `.github/workflows/ci.yml`, configured by [`.squawk.toml`](../.squawk.toml)). It fails on patterns such as:

- `CREATE INDEX` / `DROP INDEX` without `CONCURRENTLY`
- `ADD CONSTRAINT … CHECK/FOREIGN KEY` without `NOT VALID`
- `ALTER COLUMN … TYPE` (a full table rewrite)
- `SET NOT NULL`, adding a required column, renaming or dropping columns and tables
- `CREATE INDEX CONCURRENTLY` inside a transaction (sqlx would fail at runtime)

Run the same check locally (with [squawk installed](https://squawkhq.com/docs/)):

```bash
make migrations-lint                         # changed vs origin/main
bash scripts/lint_migrations.sh <base-ref>   # changed vs another ref
bash scripts/lint_migrations.sh -- migrations/0030_example.sql
```

**Intentional exceptions.** If a flagged statement is safe in context (for example, the table is new and empty in this same release, or is known to stay tiny), put a `squawk-ignore` comment directly above it, with the reason:

```sql
-- squawk-ignore constraint-missing-not-valid
-- Safe: webhook_subscriptions is created in this release and is empty.
ALTER TABLE webhook_subscriptions ADD CONSTRAINT chk_kind CHECK (kind IN ('event', 'upgrade'));
```

Reviewers should treat every `squawk-ignore` as a claim that needs justifying.

### Safe patterns

| Instead of | Write |
| --- | --- |
| `CREATE INDEX idx ON events (…)` | `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON events (…)` in its own `-- no-transaction` file (below) |
| `DROP INDEX idx` | `DROP INDEX CONCURRENTLY IF EXISTS idx` in its own `-- no-transaction` file |
| `ADD CONSTRAINT c CHECK (…)` | Migration N: `ADD CONSTRAINT c CHECK (…) NOT VALID` (brief lock, no scan). Migration N+1: `VALIDATE CONSTRAINT c` (scans under `SHARE UPDATE EXCLUSIVE`, so writes continue) |
| `ADD CONSTRAINT … FOREIGN KEY` | Same two steps: `NOT VALID`, then `VALIDATE CONSTRAINT` |
| `ALTER COLUMN x SET NOT NULL` | Add `CHECK (x IS NOT NULL) NOT VALID`, validate it, then `SET NOT NULL` (Postgres 12+ reuses the validated check and skips the scan), then drop the check |
| `ALTER COLUMN x TYPE …` | Add a new column, backfill it in batches from application code or a maintenance command, switch reads over, then drop the old column in a later release |
| `ADD COLUMN x … DEFAULT volatile_fn()` | Add the column without a default (or with a constant default, which is metadata-only on Postgres 11+), then backfill in batches |

### Concurrent index migrations with sqlx

`CREATE INDEX CONCURRENTLY` cannot run inside a transaction, and sqlx wraps every migration in one unless the file's **first line** is `-- no-transaction`. The convention is:

1. **One concurrent statement per file.** Put nothing else in the file, so a failure can't leave the migration half-applied with other statements committed.
2. **First line is exactly `-- no-transaction`.** The lint script reads this header to lint the file without assuming a transaction. A `CONCURRENTLY` statement in a file *without* the header fails the lint, because it would fail at runtime.
3. **Always `IF NOT EXISTS` / `IF EXISTS`**, so a retried migration is idempotent.

```sql
-- no-transaction
-- Speed up by-transaction lookups (#NNN). Built concurrently: events is large.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_example
    ON events (contract_id, ledger DESC);
```

**If a concurrent build fails** (deadlock, cancellation, a unique violation), Postgres leaves an `INVALID` index behind, and `IF NOT EXISTS` will then silently skip it on retry. Check for and drop invalid indexes before re-running:

```sql
SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
DROP INDEX CONCURRENTLY IF EXISTS idx_events_example;
```

### Bounding lock waits during an upgrade

Even a brief `ACCESS EXCLUSIVE` lock queues behind long-running queries, and every later query queues behind *it*. Before an upgrade on a busy instance, cap how long DDL may wait for a lock, so a blocked migration fails fast and can be retried, instead of stalling ingestion:

```sql
ALTER ROLE lumenqraph SET lock_timeout = '5s';   -- before starting the new indexer
-- … upgrade; the indexer applies migrations on startup …
ALTER ROLE lumenqraph RESET lock_timeout;        -- after migrations complete
```

### Historical migrations that lock large tables

These released migrations predate the lint. They can't be edited (sqlx checksums applied migrations), so on a **populated** deployment, do the expensive work ahead of time:

| Migration | What it does | Before upgrading a populated instance |
| --- | --- | --- |
| `0010_hot_query_indexes.sql` | Non-concurrent indexes on `events`, `token_transfers`, `contract_data`, `contract_spec_versions` | Pre-create them concurrently (below); the migration's `IF NOT EXISTS` then makes it a no-op |
| `0016_tx_hash_index.sql` | Non-concurrent `idx_events_tx_hash` on `events` | Pre-create it concurrently (below) |
| `0019_enriched_gin_index.sql` | Intended GIN index on `events.enriched` | Currently a no-op: `idx_events_enriched` already exists from `0004` (see #364, #476) |
| `0022_token_transfers_kind_check.sql` | `ADD CONSTRAINT chk_transfer_kind CHECK …` validated immediately, scanning `token_transfers` under `ACCESS EXCLUSIVE` | Run in a maintenance window, or with `lock_timeout` set as above. First confirm it will succeed: `SELECT count(*) FROM token_transfers WHERE kind NOT IN ('transfer','mint','burn','clawback');` must return 0 |

Pre-creating the `0010` and `0016` indexes (run each statement separately, outside a transaction, e.g. in `psql`):

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_contract_event_ledger
    ON events (contract_id, event_name, ledger DESC, event_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_contract_from_ledger
    ON token_transfers (contract_id, from_addr, ledger DESC, event_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_contract_to_ledger
    ON token_transfers (contract_id, to_addr, ledger DESC, event_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_contract_from_to_ledger
    ON token_transfers (contract_id, from_addr, to_addr, ledger DESC, event_id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_data_contract_label_ledger
    ON contract_data (contract_id, label, ledger DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_spec_versions_contract
    ON contract_spec_versions (contract_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_spec_versions_contract_version_desc
    ON contract_spec_versions (contract_id, version DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_tx_hash
    ON events (tx_hash, ledger ASC, event_id ASC);
```

Then check that none are `INVALID` (query above) before starting the new indexer.

## Migration Testing Checklist

To minimize rollback risk, test new migrations on a staging environment:

- [ ] `make migrations-lint` passes, or each `squawk-ignore` carries a justification.
- [ ] Apply the migration on a copy of production data.
- [ ] Verify the indexer can process events and the API can serve requests.
- [ ] Run the application's integration tests against the new schema.
- [ ] If the migration transforms data, spot-check a sample of rows.
- [ ] Document the rollback procedure (if non-trivial) before merging.

## Examples: Migration-Specific Rollback Notes

### 0001 → 0013 (baseline through spec versioning, contract state, etc.)

These migrations are **cumulative and foundational**. Rolling back any of them requires restoring from backup, as they define the core schema the application depends on. If you discover a critical bug in one of these migrations:

1. Stop all services.
2. Restore the database from a pre-migration backup.
3. Fix the migration SQL.
4. Re-run migrations from a clean state.

### 0016: tx_hash index (issue #124)

**Rollback**: Drop the index. The application will still work; queries by transaction hash will just be slower.

```sql
DROP INDEX IF EXISTS idx_events_tx_hash;
```

**Forward-fix**: If the index was incorrectly defined (wrong columns, wrong order), create a new migration adding the corrected index and dropping the old one:

```sql
-- 0021_fix_tx_hash_index.sql
DROP INDEX IF EXISTS idx_events_tx_hash;
CREATE INDEX idx_events_tx_hash_v2 ON events (tx_hash, ledger ASC);
```

## When to Introduce Down Migrations

If Lumenqraph's deployment model changes — for example, if it becomes a SaaS product where schema rollback must be instant and automated — revisit this decision. Until then, forward-only migrations with explicit rollback documentation are the safest choice for a self-hosted, rapidly evolving indexer.

## References

- [Migrations directory](../migrations/)
- [sqlx migration docs](https://docs.rs/sqlx/latest/sqlx/macro.migrate.html)
- [Why Yelp doesn't do down migrations](https://engineeringblog.yelp.com/2017/11/no-downtime-database-migrations.html)
