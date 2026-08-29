# Migration Rollback Strategy

## Context

Lumenqraph uses **forward-only migrations** applied via `sqlx::migrate!`. There are no automated down-migrations or reversible migration pairs (`up`/`down`). This document explains the rationale for that choice and provides guidance for operators who need to revert a bad schema change.

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

## Migration Testing Checklist

To minimize rollback risk, test new migrations on a staging environment:

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
