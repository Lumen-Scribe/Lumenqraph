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

    ELSIF TG_OP = 'DELETE' THEN
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

        RETURN OLD;

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

DROP TRIGGER IF EXISTS trg_update_contract_summary ON events;

CREATE TRIGGER trg_update_contract_summary
AFTER INSERT OR UPDATE OR DELETE ON events
FOR EACH ROW
EXECUTE FUNCTION update_contract_summary();
</｜｜DSML｜｜ parameter>