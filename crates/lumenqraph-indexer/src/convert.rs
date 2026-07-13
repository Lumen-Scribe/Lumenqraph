//! Map an RPC event into our storage model, decoding XDR along the way. When
//! the contract's on-chain interface spec is available, the generically-decoded
//! event is additionally enriched into a named, typed record.

use chrono::{DateTime, Utc};
use lumenqraph_core::{xdr, ContractSpec, NewEvent};

use crate::rpc_client::EventInfo;

pub fn to_new_event(e: &EventInfo, spec: Option<&ContractSpec>) -> NewEvent {
    let ledger_closed_at = e
        .ledger_closed_at
        .parse::<DateTime<Utc>>()
        .unwrap_or_else(|_| Utc::now());

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
