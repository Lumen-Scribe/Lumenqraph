-- Forward migration: re-apply the statement-level contract summary trigger rewrite
-- that was previously edited in place into the already-released
-- 0021_contract_summaries_delete.sql. Released migrations are append-only, so the
-- intended change is applied here idempotently instead.

CREATE OR REPLACE FUNCTION update_contract_summary() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO contract_summaries (contract_id, event_count, first_seen_ledger, last_seen_ledger)
        SELECT contract_id, COUNT(*), MIN(ledger), MAX(ledger)
        FROM new_events
        GROUP BY contract_id
        ON CONFLICT (contract_id) DO UPDATE SET
            event_count = contract_summaries.event_count + EXCLUDED.event_count,
            first_seen_ledger = LEAST(contract_summaries.first_seen_ledger, EXCLUDED.first_seen_ledger),
            last_seen_ledger = GREATEST(contract_summaries.last_seen_ledger, EXCLUDED.last_seen_ledger);
        RETURN NULL;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE contract_summaries cs
        SET event_count = GREATEST(cs.event_count - d.deleted_count, 0),
            last_seen_ledger = COALESCE(
                (SELECT MAX(ledger) FROM events e WHERE e.contract_id = cs.contract_id),
                cs.first_seen_ledger
            )
        FROM (
            SELECT contract_id, COUNT(*) AS deleted_count
            FROM old_events
            GROUP BY contract_id
        ) d
        WHERE cs.contract_id = d.contract_id;
        RETURN NULL;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION contract_summary_after_insert() RETURNS TRIGGER AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION contract_summary_after_delete() RETURNS TRIGGER AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_contract_summary ON events;
DROP TRIGGER IF EXISTS trg_contract_summary_ins ON events;
DROP TRIGGER IF EXISTS trg_contract_summary_del ON events;

CREATE TRIGGER trg_update_contract_summary
    AFTER INSERT ON events
    REFERENCING NEW TABLE AS new_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION update_contract_summary();

CREATE TRIGGER trg_contract_summary_ins
    AFTER INSERT ON events
    REFERENCING NEW TABLE AS new_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION contract_summary_after_insert();

CREATE TRIGGER trg_contract_summary_del
    AFTER DELETE ON events
    REFERENCING OLD TABLE AS old_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION update_contract_summary();