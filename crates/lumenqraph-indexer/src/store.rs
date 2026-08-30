//! Persistence. Writes are idempotent (`ON CONFLICT DO NOTHING` on the unique
//! `event_id`) so re-fetching a ledger never double-counts. For shallow reorgs,
//! `upsert_events` updates mutable fields (decoded values, enrichment) when the
//! RPC returns different content for a recently-passed ledger.
//! Transfer events are additionally projected into the materialized `token_transfers` table.

use lumenqraph_core::NewEvent;
use serde_json::Value;
use sqlx::{PgPool, Row};
use tracing::debug;

/// Insert a batch of events (+ derived transfers) in one transaction using a
/// single multi-row UNNEST statement. Returns the number of events newly inserted.
///
/// Each call issues at most two SQL statements regardless of batch size:
/// one UNNEST INSERT for events (RETURNING newly-inserted IDs) and one UNNEST
/// INSERT for their derived token_transfers. This avoids the per-row round-trip
/// overhead that dominates at high PAGE_SIZE values.
pub async fn insert_events(pool: &PgPool, events: &[NewEvent]) -> anyhow::Result<u64> {
    if events.is_empty() {
        return Ok(0);
    }

    // Collect column arrays for the UNNEST batch insert.
    let mut event_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut contract_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut ledgers: Vec<i64> = Vec::with_capacity(events.len());
    let mut closed_ats: Vec<chrono::DateTime<chrono::Utc>> = Vec::with_capacity(events.len());
    let mut event_types: Vec<String> = Vec::with_capacity(events.len());
    let mut topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut event_names: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut values: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_values_json: Vec<String> = Vec::with_capacity(events.len());
    let mut enriched_json: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut tx_hashes: Vec<String> = Vec::with_capacity(events.len());
    let mut in_successful_calls: Vec<bool> = Vec::with_capacity(events.len());
    let mut paging_tokens: Vec<String> = Vec::with_capacity(events.len());

    for e in events {
        event_ids.push(e.event_id.clone());
        contract_ids.push(e.contract_id.clone());
        ledgers.push(e.ledger);
        closed_ats.push(e.ledger_closed_at);
        event_types.push(e.event_type.clone());
        topics_json.push(serde_json::to_string(&e.topics)?);
        decoded_topics_json.push(serde_json::to_string(&e.decoded_topics)?);
        event_names.push(e.event_name.clone());
        values.push(e.value.clone());
        decoded_values_json.push(serde_json::to_string(&e.decoded_value)?);
        enriched_json.push(
            e.enriched
                .as_ref()
                .map(|v| serde_json::to_string(v))
                .transpose()?,
        );
        tx_hashes.push(e.tx_hash.clone());
        in_successful_calls.push(e.in_successful_call);
        paging_tokens.push(e.paging_token.clone());
    }

    let mut tx = pool.begin().await?;

    // Single INSERT for the whole batch. JSON columns are passed as text and
    // cast to jsonb in the SELECT so the driver only needs to encode plain arrays.
    // RETURNING gives us only the newly-inserted IDs (conflicts are excluded).
    let inserted_rows = sqlx::query(
        "INSERT INTO events (
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         SELECT
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics::jsonb, decoded_topics::jsonb, event_name, value,
            decoded_value::jsonb, enriched::jsonb, tx_hash, in_successful_call, paging_token
         FROM UNNEST(
            $1::text[], $2::text[], $3::bigint[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::text[], $10::text[],
            $11::text[], $12::text[], $13::bool[], $14::text[]
         ) AS t(
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id",
    )
    .bind(&event_ids)
    .bind(&contract_ids)
    .bind(&ledgers)
    .bind(&closed_ats)
    .bind(&event_types)
    .bind(&topics_json)
    .bind(&decoded_topics_json)
    .bind(&event_names)
    .bind(&values)
    .bind(&decoded_values_json)
    .bind(&enriched_json)
    .bind(&tx_hashes)
    .bind(&in_successful_calls)
    .bind(&paging_tokens)
    .fetch_all(&mut *tx)
    .await?;

    let inserted_count = inserted_rows.len() as u64;

    // Project token_transfers only for events that were newly inserted.
    if inserted_count > 0 {
        let inserted_ids: std::collections::HashSet<String> = inserted_rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>("event_id").ok())
            .collect();

        let transfers: Vec<Transfer> = events
            .iter()
            .filter(|e| inserted_ids.contains(&e.event_id))
            .filter_map(|e| extract_transfer(e))
            .collect();

        if !transfers.is_empty() {
            let t_event_ids: Vec<String> =
                transfers.iter().map(|t| t.event_id.clone()).collect();
            let t_contract_ids: Vec<String> =
                transfers.iter().map(|t| t.contract_id.clone()).collect();
            let t_from_addrs: Vec<Option<String>> =
                transfers.iter().map(|t| t.from_addr.clone()).collect();
            let t_to_addrs: Vec<Option<String>> =
                transfers.iter().map(|t| t.to_addr.clone()).collect();
            let t_amounts: Vec<String> = transfers.iter().map(|t| t.amount.clone()).collect();
            let t_kinds: Vec<String> = transfers.iter().map(|t| t.kind.clone()).collect();
            let t_amounts_numeric: Vec<Option<String>> = transfers.iter().map(|t| {
                Some(t.amount.clone())
            }).collect();
            let t_ledgers: Vec<i64> = transfers.iter().map(|t| t.ledger).collect();
            let t_closed_ats: Vec<chrono::DateTime<chrono::Utc>> =
                transfers.iter().map(|t| t.ledger_closed_at).collect();

            sqlx::query(
                "INSERT INTO token_transfers
                    (event_id, contract_id, from_addr, to_addr, amount, kind, amount_numeric, ledger, ledger_closed_at)
                 SELECT * FROM UNNEST(
                    $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::numeric[], $8::bigint[], $9::timestamptz[]
                 ) AS t(event_id, contract_id, from_addr, to_addr, amount, kind, amount_numeric, ledger, ledger_closed_at)
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(&t_event_ids)
            .bind(&t_contract_ids)
            .bind(&t_from_addrs)
            .bind(&t_to_addrs)
            .bind(&t_amounts)
            .bind(&t_kinds)
            .bind(&t_amounts_numeric)
            .bind(&t_ledgers)
            .bind(&t_closed_ats)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(inserted_count)
}

/// Insert a batch of events with upsert semantics for reorg handling.
/// If an event already exists (same event_id), update mutable fields (decoded values, enrichment)
/// in case the RPC returned different content for a recently-passed ledger (shallow reorg).
/// Returns (inserted, updated) counts.
pub async fn upsert_events(
    pool: &PgPool,
    events: &[NewEvent],
) -> anyhow::Result<(u64, u64)> {
    if events.is_empty() {
        return Ok((0, 0));
    }

    // Collect column arrays for the UNNEST batch upsert
    let mut event_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut contract_ids: Vec<String> = Vec::with_capacity(events.len());
    let mut ledgers: Vec<i64> = Vec::with_capacity(events.len());
    let mut closed_ats: Vec<chrono::DateTime<chrono::Utc>> = Vec::with_capacity(events.len());
    let mut event_types: Vec<String> = Vec::with_capacity(events.len());
    let mut topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_topics_json: Vec<String> = Vec::with_capacity(events.len());
    let mut event_names: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut values: Vec<String> = Vec::with_capacity(events.len());
    let mut decoded_values_json: Vec<String> = Vec::with_capacity(events.len());
    let mut enriched_json: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut tx_hashes: Vec<String> = Vec::with_capacity(events.len());
    let mut in_successful_calls: Vec<bool> = Vec::with_capacity(events.len());
    let mut paging_tokens: Vec<String> = Vec::with_capacity(events.len());

    for e in events {
        event_ids.push(e.event_id.clone());
        contract_ids.push(e.contract_id.clone());
        ledgers.push(e.ledger);
        closed_ats.push(e.ledger_closed_at);
        event_types.push(e.event_type.clone());
        topics_json.push(serde_json::to_string(&e.topics)?);
        decoded_topics_json.push(serde_json::to_string(&e.decoded_topics)?);
        event_names.push(e.event_name.clone());
        values.push(e.value.clone());
        decoded_values_json.push(serde_json::to_string(&e.decoded_value)?);
        enriched_json.push(
            e.enriched
                .as_ref()
                .map(|v| serde_json::to_string(v))
                .transpose()?,
        );
        tx_hashes.push(e.tx_hash.clone());
        in_successful_calls.push(e.in_successful_call);
        paging_tokens.push(e.paging_token.clone());
    }

    let mut tx = pool.begin().await?;

    // Single batch upsert with RETURNING to distinguish inserts from updates
    let rows = sqlx::query(
        "INSERT INTO events (
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         SELECT
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics::jsonb, decoded_topics::jsonb, event_name, value,
            decoded_value::jsonb, enriched::jsonb, tx_hash, in_successful_call, paging_token
         FROM UNNEST(
            $1::text[], $2::text[], $3::bigint[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::text[], $10::text[],
            $11::text[], $12::text[], $13::bool[], $14::text[]
         ) AS t(
            event_id, contract_id, ledger, ledger_closed_at, event_type,
            topics, decoded_topics, event_name, value, decoded_value,
            enriched, tx_hash, in_successful_call, paging_token
         )
         ON CONFLICT (event_id) DO UPDATE SET
            decoded_topics = EXCLUDED.decoded_topics,
            decoded_value = EXCLUDED.decoded_value,
            enriched = EXCLUDED.enriched
         RETURNING event_id, (xmax = 0) AS inserted",
    )
    .bind(&event_ids)
    .bind(&contract_ids)
    .bind(&ledgers)
    .bind(&closed_ats)
    .bind(&event_types)
    .bind(&topics_json)
    .bind(&decoded_topics_json)
    .bind(&event_names)
    .bind(&values)
    .bind(&decoded_values_json)
    .bind(&enriched_json)
    .bind(&tx_hashes)
    .bind(&in_successful_calls)
    .bind(&paging_tokens)
    .fetch_all(&mut *tx)
    .await?;

    let mut inserted = 0u64;
    let mut updated = 0u64;
    let mut inserted_ids = std::collections::HashSet::new();

    for row in rows {
        let event_id: String = row.try_get("event_id")?;
        let is_insert: bool = row.try_get("inserted")?;
        if is_insert {
            inserted += 1;
            inserted_ids.insert(event_id);
        } else {
            updated += 1;
        }
    }

    if updated > 0 {
        debug!(count = updated, "events updated due to shallow reorg");
    }

    // Project token_transfers only for newly inserted events
    if inserted > 0 {
        let transfers: Vec<Transfer> = events
            .iter()
            .filter(|e| inserted_ids.contains(&e.event_id))
            .filter_map(|e| extract_transfer(e))
            .collect();

        if !transfers.is_empty() {
            let t_event_ids: Vec<String> =
                transfers.iter().map(|t| t.event_id.clone()).collect();
            let t_contract_ids: Vec<String> =
                transfers.iter().map(|t| t.contract_id.clone()).collect();
            let t_from_addrs: Vec<Option<String>> =
                transfers.iter().map(|t| t.from_addr.clone()).collect();
            let t_to_addrs: Vec<Option<String>> =
                transfers.iter().map(|t| t.to_addr.clone()).collect();
            let t_amounts: Vec<String> = transfers.iter().map(|t| t.amount.clone()).collect();
            let t_kinds: Vec<String> = transfers.iter().map(|t| t.kind.clone()).collect();
            let t_amounts_numeric: Vec<Option<String>> = transfers.iter().map(|t| {
                Some(t.amount.clone())
            }).collect();
            let t_ledgers: Vec<i64> = transfers.iter().map(|t| t.ledger).collect();
            let t_closed_ats: Vec<chrono::DateTime<chrono::Utc>> =
                transfers.iter().map(|t| t.ledger_closed_at).collect();

            sqlx::query(
                "INSERT INTO token_transfers
                    (event_id, contract_id, from_addr, to_addr, amount, kind, amount_numeric, ledger, ledger_closed_at)
                 SELECT * FROM UNNEST(
                    $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::numeric[], $8::bigint[], $9::timestamptz[]
                 ) AS t(event_id, contract_id, from_addr, to_addr, amount, kind, amount_numeric, ledger, ledger_closed_at)
                 ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(&t_event_ids)
            .bind(&t_contract_ids)
            .bind(&t_from_addrs)
            .bind(&t_to_addrs)
            .bind(&t_amounts)
            .bind(&t_kinds)
            .bind(&t_amounts_numeric)
            .bind(&t_ledgers)
            .bind(&t_closed_ats)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok((inserted, updated))
}

struct Transfer {
    event_id: String,
    contract_id: String,
    from_addr: Option<String>,
    to_addr: Option<String>,
    amount: String,
    kind: String,
    ledger: i64,
    ledger_closed_at: chrono::DateTime<chrono::Utc>,
}

/// Recognize SEP-41 style balance-delta events: transfer, mint, burn, clawback.
/// Returns None for other event types.
///
/// transfer: Topics: [symbol "transfer", from Address, to Address]
/// mint: Topics: [symbol "mint", to Address]
/// burn: Topics: [symbol "burn", from Address]
/// clawback: Topics: [symbol "clawback", from Address]
/// All have Value: i128 amount (decoded as a decimal string).
fn extract_transfer(e: &NewEvent) -> Option<Transfer> {
    let event_name = e.event_name.as_deref()?;
    let kind = match event_name {
        "transfer" => "transfer",
        "mint" => "mint",
        "burn" => "burn",
        "clawback" => "clawback",
        _ => return None,
    };

    let as_addr = |v: Option<&Value>| match v {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    };

    let amount = match &e.decoded_value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };

    let (from_addr, to_addr) = match kind {
        "transfer" => (as_addr(e.decoded_topics.get(1)), as_addr(e.decoded_topics.get(2))),
        "mint" => (None, as_addr(e.decoded_topics.get(1))),
        "burn" | "clawback" => (as_addr(e.decoded_topics.get(1)), None),
        _ => return None,
    };

    Some(Transfer {
        event_id: e.event_id.clone(),
        contract_id: e.contract_id.clone(),
        from_addr,
        to_addr,
        amount,
        kind: kind.to_string(),
        ledger: e.ledger,
        ledger_closed_at: e.ledger_closed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(name: Option<&str>, topics: Vec<Value>, value: Value) -> NewEvent {
        NewEvent {
            event_id: "e1".into(),
            contract_id: "C1".into(),
            ledger: 42,
            ledger_closed_at: chrono::Utc::now(),
            event_type: "contract".into(),
            topics: vec![],
            decoded_topics: topics,
            event_name: name.map(|s| s.to_string()),
            value: String::new(),
            decoded_value: value,
            enriched: None,
            tx_hash: "tx".into(),
            in_successful_call: true,
            paging_token: "e1".into(),
        }
    }

    #[test]
    fn extracts_transfer_from_to_amount() {
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!("300"),
        );
        let t = extract_transfer(&e).expect("transfer should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GFROM"));
        assert_eq!(t.to_addr.as_deref(), Some("GTO"));
        assert_eq!(t.amount, "300");
        assert_eq!(t.kind, "transfer");
    }

    #[test]
    fn extracts_mint_event() {
        let e = event(
            Some("mint"),
            vec![json!("mint"), json!("GRECIPIENT")],
            json!("1000"),
        );
        let t = extract_transfer(&e).expect("mint should be recognised");
        assert_eq!(t.from_addr, None);
        assert_eq!(t.to_addr.as_deref(), Some("GRECIPIENT"));
        assert_eq!(t.amount, "1000");
        assert_eq!(t.kind, "mint");
    }

    #[test]
    fn extracts_burn_event() {
        let e = event(
            Some("burn"),
            vec![json!("burn"), json!("GBURNER")],
            json!("500"),
        );
        let t = extract_transfer(&e).expect("burn should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GBURNER"));
        assert_eq!(t.to_addr, None);
        assert_eq!(t.amount, "500");
        assert_eq!(t.kind, "burn");
    }

    #[test]
    fn extracts_clawback_event() {
        let e = event(
            Some("clawback"),
            vec![json!("clawback"), json!("GVICTIM")],
            json!("750"),
        );
        let t = extract_transfer(&e).expect("clawback should be recognised");
        assert_eq!(t.from_addr.as_deref(), Some("GVICTIM"));
        assert_eq!(t.to_addr, None);
        assert_eq!(t.amount, "750");
        assert_eq!(t.kind, "clawback");
    }

    #[test]
    fn ignores_other_events() {
        let e = event(Some("approval"), vec![json!("approval")], json!("1"));
        assert!(extract_transfer(&e).is_none());
    }

    #[test]
    fn non_string_amount_is_stringified() {
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!("GFROM"), json!("GTO")],
            json!(300),
        );
        let t = extract_transfer(&e).unwrap();
        assert_eq!(t.amount, "300");
    }

    #[test]
    fn missing_address_topics_are_none() {
        let e = event(Some("transfer"), vec![json!("transfer")], json!("5"));
        let t = extract_transfer(&e).unwrap();
        assert!(t.from_addr.is_none());
        assert!(t.to_addr.is_none());
        assert_eq!(t.amount, "5");
    }
}
