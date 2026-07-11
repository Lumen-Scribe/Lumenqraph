//! Persistence. Writes are idempotent (`ON CONFLICT DO NOTHING` on the unique
//! `event_id`) so re-fetching a ledger never double-counts. Transfer events are
//! additionally projected into the materialized `token_transfers` table.

use lumenqraph_core::NewEvent;
use serde_json::Value;
use sqlx::PgPool;

/// Insert a batch of events (+ derived transfers) in one transaction. Returns
/// the number of events newly inserted.
pub async fn insert_events(pool: &PgPool, events: &[NewEvent]) -> anyhow::Result<u64> {
    if events.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    let mut inserted = 0u64;
    for e in events {
        let topics = serde_json::to_value(&e.topics)?;
        let decoded_topics = serde_json::to_value(&e.decoded_topics)?;
        let result = sqlx::query(
            "INSERT INTO events (
                event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                tx_hash, in_successful_call, paging_token
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&e.event_id)
        .bind(&e.contract_id)
        .bind(e.ledger)
        .bind(e.ledger_closed_at)
        .bind(&e.event_type)
        .bind(topics)
        .bind(decoded_topics)
        .bind(&e.event_name)
        .bind(&e.value)
        .bind(&e.decoded_value)
        .bind(&e.tx_hash)
        .bind(e.in_successful_call)
        .bind(&e.paging_token)
        .execute(&mut *tx)
        .await?;

        let newly = result.rows_affected();
        inserted += newly;

        // Only project transfers for rows we actually inserted.
        if newly > 0 {
            if let Some(t) = extract_transfer(e) {
                sqlx::query(
                    "INSERT INTO token_transfers
                        (event_id, contract_id, from_addr, to_addr, amount, ledger, ledger_closed_at)
                     VALUES ($1,$2,$3,$4,$5,$6,$7)
                     ON CONFLICT (event_id) DO NOTHING",
                )
                .bind(&t.event_id)
                .bind(&t.contract_id)
                .bind(&t.from_addr)
                .bind(&t.to_addr)
                .bind(&t.amount)
                .bind(t.ledger)
                .bind(t.ledger_closed_at)
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(inserted)
}

struct Transfer {
    event_id: String,
    contract_id: String,
    from_addr: Option<String>,
    to_addr: Option<String>,
    amount: String,
    ledger: i64,
    ledger_closed_at: chrono::DateTime<chrono::Utc>,
}

/// Recognise a SEP-41 style transfer event and pull out from/to/amount.
/// Topics: [symbol "transfer", from Address, to Address, (optional asset)].
/// Value: i128 amount (decoded as a decimal string).
fn extract_transfer(e: &NewEvent) -> Option<Transfer> {
    if e.event_name.as_deref() != Some("transfer") {
        return None;
    }
    let as_addr = |v: Option<&Value>| match v {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    };
    let amount = match &e.decoded_value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    Some(Transfer {
        event_id: e.event_id.clone(),
        contract_id: e.contract_id.clone(),
        from_addr: as_addr(e.decoded_topics.get(1)),
        to_addr: as_addr(e.decoded_topics.get(2)),
        amount,
        ledger: e.ledger,
        ledger_closed_at: e.ledger_closed_at,
    })
}
