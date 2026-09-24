-- Fix the contract_summaries trigger to handle DELETE correctly.
--
-- The original trigger (0009_contract_summaries.sql) only handled INSERT, so
-- retention pruning (which deletes events) would leave contract_summaries with
-- stale, over-inflated event counts. This migration replaces it with a trigger
-- that handles INSERT, UPDATE, and DELETE correctly.
--
-- On DELETE: decrement event_count and, when it reaches zero, remove the row
-- entirely (matching the `WHERE event_count > 0` filter in list_contracts).
-- On INSERT: same upsert as before.
-- ON UPDATE: handles the unlikely case that ledger changes.
--
-- The DELETE and INSERT paths use statement-level triggers with transition
-- tables so that a batch of N rows (e.g. a 5,000-row retention prune or an
-- UNNEST batch insert) results in a single summary UPDATE per affected
-- contract instead of N row-level probes and updates. This avoids the
-- O(rows x index probes) blow-up and the lock contention on the hot summary
-- rows that the row-level trigger caused.

-- Row-level function: handles INSERT and UPDATE (single-row semantics).
CREATE OR REPLACE FUNCTION update_contract_summary()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO contract_summaries (
            contract_id,
            event_count,
            first_seen_ledger,
            last_seen_ledger,
            updated_at
        ) VALUES (
            NEW.contract_id,
            1,
            NEW.ledger,
            NEW.ledger,
            now()
        )
        ON CONFLICT (contract_id) DO UPDATE SET
            event_count       = contract_summaries.event_count + 1,
            first_seen_ledger = LEAST(contract_summaries.first_seen_ledger,
                                      EXCLUDED.first_seen_ledger),
            last_seen_ledger  = GREATEST(contract_summaries.last_seen_ledger,
                                         EXCLUDED.last_seen_ledger),
            updated_at        = now();
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        -- Events rarely change their ledger, but guard it anyway.
        IF OLD.ledger IS DISTINCT FROM NEW.ledger
            OR OLD.contract_id IS DISTINCT FROM NEW.contract_id THEN
            -- Treat as a delete of the old row followed by an insert of the new.
            UPDATE contract_summaries
               SET event_count       = GREATEST(event_count - 1, 0),
                   first_seen_ledger = CASE
                       WHEN first_seen_ledger = OLD.ledger
                       THEN (SELECT MIN(ledger) FROM events
                              WHERE contract_id = OLD.contract_id
                                AND event_id <> OLD.event_id)
                       ELSE first_seen_ledger
                   END,
                   last_seen_ledger  = CASE
                       WHEN last_seen_ledger = OLD.ledger
                       THEN (SELECT MAX(ledger) FROM events
                              WHERE contract_id = OLD.contract_id
                                AND event_id <> OLD.event_id)
                       ELSE last_seen_ledger
                   END,
                   updated_at        = now()
             WHERE contract_id = OLD.contract_id;

            DELETE FROM contract_summaries
             WHERE contract_id = OLD.contract_id
               AND event_count = 0;

            INSERT INTO contract_summaries (
                contract_id,
                event_count,
                first_seen_ledger,
                last_seen_ledger,
                updated_at
            ) VALUES (
                NEW.contract_id,
                1,
                NEW.ledger,
                NEW.ledger,
                now()
            )
            ON CONFLICT (contract_id) DO UPDATE SET
                event_count       = contract_summaries.event_count + 1,
                first_seen_ledger = LEAST(contract_summaries.first_seen_ledger,
                                          EXCLUDED.first_seen_ledger),
                last_seen_ledger  = GREATEST(contract_summaries.last_seen_ledger,
                                             EXCLUDED.last_seen_ledger),
                updated_at        = now();
        END IF;
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Statement-level function for batch INSERTs (e.g. UNNEST batch insert).
-- Aggregates the transition table by contract_id once and applies a single
-- upsert per affected contract.
CREATE OR REPLACE FUNCTION contract_summary_after_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO contract_summaries (
        contract_id,
        event_count,
        first_seen_ledger,
        last_seen_ledger,
        updated_at
    )
    SELECT
        contract_id,
        count(*),
        MIN(ledger),
        MAX(ledger),
        now()
      FROM new_rows
     GROUP BY contract_id
    ON CONFLICT (contract_id) DO UPDATE SET
        event_count       = contract_summaries.event_count + EXCLUDED.event_count,
        first_seen_ledger = LEAST(contract_summaries.first_seen_ledger,
                                  EXCLUDED.first_seen_ledger),
        last_seen_ledger  = GREATEST(contract_summaries.last_seen_ledger,
                                     EXCLUDED.last_seen_ledger),
        updated_at        = now();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Statement-level function for batch DELETEs (e.g. retention pruning).
-- Aggregates the transition table by contract_id once, decrements counts in a
-- single UPDATE, and recomputes first_seen_ledger / last_seen_ledger once per
-- affected contract rather than once per deleted row.
CREATE OR REPLACE FUNCTION contract_summary_after_delete()
RETURNS TRIGGER AS $$
BEGIN
    -- Decrement counts for every affected contract in one statement.
    UPDATE contract_summaries cs
       SET event_count = GREATEST(cs.event_count - d.deleted_count, 0),
           updated_at  = now()
      FROM (
          SELECT contract_id, count(*) AS deleted_count
            FROM old_rows
           GROUP BY contract_id
      ) d
     WHERE cs.contract_id = d.contract_id;

    -- Recompute ledger bounds once per affected contract. The deleted rows are
    -- the oldest events, so first_seen_ledger is the value that usually needs
    -- to move; last_seen_ledger is recomputed too for correctness.
    UPDATE contract_summaries cs
       SET first_seen_ledger = bounds.min_ledger,
           last_seen_ledger  = bounds.max_ledger,
           updated_at        = now()
      FROM (
          SELECT d.contract_id,
                 (SELECT MIN(ledger) FROM events
                   WHERE contract_id = d.contract_id) AS min_ledger,
                 (SELECT MAX(ledger) FROM events
                   WHERE contract_id = d.contract_id) AS max_ledger
            FROM (
                SELECT DISTINCT contract_id FROM old_rows
            ) d
      ) bounds
     WHERE cs.contract_id = bounds.contract_id
       AND cs.event_count > 0;

    -- Remove summary rows for contracts whose events are all gone.
    DELETE FROM contract_summaries
     WHERE event_count = 0;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Replace the row-level trigger with statement-level triggers that use
-- transition tables for the batch INSERT and DELETE paths. UPDATE keeps the
-- row-level trigger since ledger changes are rare and single-row.
DROP TRIGGER IF EXISTS trg_update_contract_summary ON events;
DROP TRIGGER IF EXISTS trg_contract_summary_ins ON events;
DROP TRIGGER IF EXISTS trg_contract_summary_del ON events;

CREATE TRIGGER trg_update_contract_summary
AFTER UPDATE ON events
FOR EACH ROW
EXECUTE FUNCTION update_contract_summary();

CREATE TRIGGER trg_contract_summary_ins
AFTER INSERT ON events
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION contract_summary_after_insert();

CREATE TRIGGER trg_contract_summary_del
AFTER DELETE ON events
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION contract_summary_after_delete();
