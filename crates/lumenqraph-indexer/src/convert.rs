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
use lumenqraph_core::{xdr, ContractSpec, NewEvent};
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
    // The ledger timestamp is authoritative. Backfill and catch-up routinely
    // ingest ledgers older than wall-clock tolerance, so never clamp it.
    let ledger_closed_at = parsed_ts;

    let decoded_topics = xdr::decode_topics(&e.topic);
    let decoded_value = xdr::decode_scval_base64(&e.value);
    let event_name = e.topic.first().and_then(|t| xdr::event_name_from_topic(t));

    // Enrich against the spec when we have both a name and a matching schema.
    let enriched = match (spec, &event_name) {
        (Some(spec), Some(name)) => spec.enrich_event(name, &decoded_topics, &decoded_value),
        _ => None,
    };

    Some(NewEvent {
        event_id: e.id.clone(),
        contract_id: e.contract_id.clone(),
        ledger: e.ledger,
        ledger_closed_at,
        event_type: e.event_type.clone(),
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

}
