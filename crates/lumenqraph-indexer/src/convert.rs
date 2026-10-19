//! Map an RPC event into our storage model, decoding XDR along the way. When
//! the contract's on-chain interface spec is available, the generically-decoded
//! event is additionally enriched into a named, typed record.

use chrono::{DateTime, Duration, Utc};
use lumenqraph_core::{xdr, ContractSpec, NewEvent};
use tracing::warn;

use crate::rpc_client::EventInfo;

/// How far a `ledgerClosedAt` timestamp from the RPC may drift from wall-clock
/// time before it's treated as implausible and clamped. A misbehaving or
/// malicious RPC could otherwise report a time-travelled timestamp, which
/// would corrupt time-range filters and time-series aggregation downstream.
const TIMESTAMP_TOLERANCE: Duration = Duration::hours(1);

/// Validate `ledger_closed_at` against wall-clock time, clamping (rather than
/// rejecting) values outside the tolerance window. Clamping is preferred over
/// rejection because dropping the event entirely would leave a gap in the
/// index; a clamped timestamp is still close enough to be useful for
/// time-range queries.
fn validate_ledger_closed_at(raw: &str, parsed: DateTime<Utc>) -> DateTime<Utc> {
    let now = Utc::now();
    let lower = now - TIMESTAMP_TOLERANCE;
    let upper = now + TIMESTAMP_TOLERANCE;

    if parsed < lower {
        warn!(raw, %parsed, %now, "ledger_closed_at is implausibly far in the past; clamping");
        lower
    } else if parsed > upper {
        warn!(raw, %parsed, %now, "ledger_closed_at is implausibly far in the future; clamping");
        upper
    } else {
        parsed
    }
}

pub fn to_new_event(e: &EventInfo, spec: Option<&ContractSpec>) -> NewEvent {
    let ledger_closed_at = e
        .ledger_closed_at
        .parse::<DateTime<Utc>>()
        .unwrap_or_else(|_| Utc::now());
    let ledger_closed_at = validate_ledger_closed_at(&e.ledger_closed_at, ledger_closed_at);

    let decoded_topics = xdr::decode_topics(&e.topic);
    let decoded_value = xdr::decode_scval_base64(&e.value);
    let event_name = e.topic.first().and_then(|t| xdr::event_name_from_topic(t));

    // Enrich against the spec when we have both a name and a matching schema.
    let enriched = match (spec, &event_name) {
        (Some(spec), Some(name)) => spec.enrich_event(name, &decoded_topics, &decoded_value),
        _ => None,
    };

    NewEvent {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plausible_timestamp_passes_through_unchanged() {
        let now = Utc::now();
        assert_eq!(validate_ledger_closed_at("irrelevant", now), now);
    }

    #[test]
    fn a_future_timestamp_is_clamped_to_the_upper_bound() {
        let far_future = Utc::now() + Duration::hours(5);
        let clamped = validate_ledger_closed_at("irrelevant", far_future);
        assert!(clamped < far_future);
        assert!(clamped <= Utc::now() + TIMESTAMP_TOLERANCE);
    }

    #[test]
    fn a_past_timestamp_is_clamped_to_the_lower_bound() {
        let far_past = Utc::now() - Duration::hours(5);
        let clamped = validate_ledger_closed_at("irrelevant", far_past);
        assert!(clamped > far_past);
        assert!(clamped >= Utc::now() - TIMESTAMP_TOLERANCE);
    }
}
