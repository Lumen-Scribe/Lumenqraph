//! Map an RPC event into our storage model, decoding XDR along the way. When
//! the contract's on-chain interface spec is available, the generically-decoded
//! event is additionally enriched into a named, typed record.
//!
//! ## Timestamp handling (#399)
//!
//! `ledgerClosedAt` has historically been emitted by RPC as both RFC 3339
//! (`"2024-01-01T00:00:00Z"`) and as a Unix-epoch integer string (`"1704067200"`).
//! We accept both. On parse failure we log the raw value, count the error, and
//! return `None` so the caller can decide the fallback — we never silently store
//! `Utc::now()`.

use chrono::{DateTime, TimeZone, Utc};
use lumenqraph_core::{xdr, ContractSpec, EventType, NewEvent};
use tracing::error;

use crate::rpc_client::EventInfo;

/// Parse `ledgerClosedAt` from either RFC 3339 or Unix-epoch-seconds format.
///
/// Returns `Some(DateTime<Utc>)` on success, `None` on failure.
/// On failure, logs the raw value at ERROR level — callers must **not** fall
/// back to `Utc::now()`.
pub fn parse_ledger_closed_at(raw: &str) -> Option<DateTime<Utc>> {
    // Try RFC 3339 first (the current RPC format).
    if let Ok(dt) = raw.parse::<DateTime<Utc>>() {
        return Some(dt);
    }
    // Try Unix epoch seconds (older RPC versions emitted a plain integer).
    if let Ok(secs) = raw.trim().parse::<i64>() {
        if let Some(dt) = Utc.timestamp_opt(secs, 0).single() {
            return Some(dt);
        }
    }
    // Neither format matched — log and count; caller decides what to do.
    error!(
        raw_timestamp = raw,
        "ledgerClosedAt could not be parsed as RFC 3339 or Unix seconds; \
         event will be dropped rather than stored with a fabricated timestamp"
    );
    None
}

/// Convert an [`EventInfo`] from the RPC into a [`NewEvent`] ready for storage.
///
/// Returns `None` when the `ledgerClosedAt` timestamp cannot be parsed — the
/// event is skipped entirely rather than stored with a fabricated timestamp.
/// All other missing/malformed fields are handled gracefully with defaults.
pub fn to_new_event(e: &EventInfo, spec: Option<&ContractSpec>) -> Option<NewEvent> {
    // Parse the timestamp. On failure, return None — never fall back to Utc::now().
    let parsed_ts = parse_ledger_closed_at(&e.ledger_closed_at)?;
    // Unknown event types cannot satisfy the events.event_type CHECK; skip them.
    let event_type = EventType::from_rpc_str(&e.event_type)?;
    // The ledger timestamp is authoritative. Backfill and catch-up routinely
    // ingest ledgers older than wall-clock tolerance, so never clamp it.
    let ledger_closed_at = parsed_ts;

    let decoded_topics = xdr::decode_topics(&e.topic);
    let decoded_value = xdr::decode_scval_base64(&e.value);
    let event_name = e.topic.first().and_then(|t| xdr::event_name_from_topic(t));

    // Enrich against the spec when we have both a name and a matching schema.
    // `spec` must be the version in force at `e.ledger` (see `SpecCache::get`),
    // not necessarily the contract's current interface.
    let enriched = match (spec, &event_name) {
        (Some(spec), Some(name)) => spec.enrich_event(name, &decoded_topics, &decoded_value),
        _ => None,
    };

    Some(NewEvent {
        event_id: e.id.clone(),
        contract_id: e.contract_id.clone(),
        ledger: e.ledger,
        ledger_closed_at,
        // The RPC only emits contract|system|diagnostic, the values the
        // `events.event_type` CHECK constraint accepts. Anything else cannot be
        // stored, so the event is skipped.
        event_type: e.event_type.parse::<EventType>().ok()?,
        topics: e.topic.clone(),
        decoded_topics,
        event_name,
        value: e.value.clone(),
        decoded_value,
        enriched,
        tx_hash: e.tx_hash.clone(),
        in_successful_call: e.in_successful_contract_call,
        paging_token: if e.paging_token.is_empty() {
            e.id.clone()
        } else {
            e.paging_token.clone()
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_ledger_closed_at ────────────────────────────────────────────

    #[test]
    fn accepts_rfc3339_format() {
        let dt = parse_ledger_closed_at("2024-01-01T00:00:00Z");
        assert!(dt.is_some());
        assert_eq!(dt.unwrap().timestamp(), 1_704_067_200);
    }

    #[test]
    fn accepts_unix_seconds_format() {
        let dt = parse_ledger_closed_at("1704067200");
        assert!(dt.is_some());
        assert_eq!(dt.unwrap().timestamp(), 1_704_067_200);
    }

    #[test]
    fn rejects_garbage_string() {
        // Must return None — never Utc::now().
        let result = parse_ledger_closed_at("not-a-timestamp");
        assert!(result.is_none(), "expected None for unparseable timestamp");
    }

    #[test]
    fn rejects_empty_string() {
        assert!(parse_ledger_closed_at("").is_none());
    }

    #[test]
    fn rejects_partial_rfc3339() {
        // A date without time component is not valid RFC 3339.
        assert!(parse_ledger_closed_at("2024-01-01").is_none());
    }

    // ── version-at-ledger enrichment ──────────────────────────────────────

    /// Build a minimal `EventInfo` for a contract event at `ledger`.
    fn event_info(ledger: u32, topic: &str, value: &str) -> EventInfo {
        EventInfo {
            id: format!("ev-{ledger}"),
            contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM".to_string(),
            ledger,
            ledger_closed_at: "2024-01-01T00:00:00Z".to_string(),
            event_type: "contract".to_string(),
            topic: vec![topic.to_string()],
            value: value.to_string(),
            tx_hash: "deadbeef".to_string(),
            in_successful_contract_call: true,
            paging_token: String::new(),
        }
    }

    /// DB test: versions v1@100 and v2@200. An event at ledger 150 must be
    /// enriched with v1's names and an event at 250 with v2's.
    #[tokio::test]
    async fn enrichment_uses_spec_version_in_force_at_event_ledger() {
        use crate::specs::SpecCache;
        use lumenqraph_core::ContractSpec;

        let Some(pool) = crate::test_support::test_pool().await else {
            return;
        };
        let contract_id = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";

        // v1: event `old_name` with a single i64 field.
        let v1 = ContractSpec::from_json(
            r#"{
                "name": "v1",
                "events": [{
                    "name": "old_name",
                    "params": [{ "name": "amount", "type": "i64" }]
                }]
            }"#,
        )
        .expect("v1 spec parses");

        // v2: the event is renamed and its field retyped.
        let v2 = ContractSpec::from_json(
            r#"{
                "name": "v2",
                "events": [{
                    "name": "new_name",
                    "params": [{ "name": "total", "type": "i64" }]
                }]
            }"#,
        )
        .expect("v2 spec parses");

        // Persist both versions with their upgrade ledgers.
        crate::specs::store_spec_version(&pool, contract_id, 100, &v1)
            .await
            .expect("store v1");
        crate::specs::store_spec_version(&pool, contract_id, 200, &v2)
            .await
            .expect("store v2");

        let cache = SpecCache::new();
        cache.load_from_db(&pool).await.expect("load cache");

        // Event at ledger 150: v1 is in force, so `old_name`/`amount` apply.
        let spec_150 = cache.get(&pool, None, contract_id, 150).await;
        let ev_150 = event_info(150, "old_name", "AAAAAA==");
        let new_150 = to_new_event(&ev_150, spec_150.as_deref()).expect("event 150 converts");
        let enriched_150 = new_150.enriched.expect("event 150 is enriched");
        assert_eq!(enriched_150.name, "old_name");
        assert_eq!(enriched_150.params[0].name, "amount");

        // Event at ledger 250: v2 is in force, so `new_name`/`total` apply.
        let spec_250 = cache.get(&pool, None, contract_id, 250).await;
        let ev_250 = event_info(250, "new_name", "AAAAAA==");
        let new_250 = to_new_event(&ev_250, spec_250.as_deref()).expect("event 250 converts");
        let enriched_250 = new_250.enriched.expect("event 250 is enriched");
        assert_eq!(enriched_250.name, "new_name");
        assert_eq!(enriched_250.params[0].name, "total");
    }
}
