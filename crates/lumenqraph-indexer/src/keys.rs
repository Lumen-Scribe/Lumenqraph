//! Per-key state discovery: turning the events we already index into the set of
//! *individual* storage keys worth snapshotting.
//!
//! Instance storage (see [`crate::state`]) is one enumerable map. Per-holder
//! state — a token's `Balance(Address)`, say — lives in separate, non-enumerable
//! ledger entries: you can only read one if you know its exact key. But a
//! token's own events name its holders (a `transfer` carries `from` and `to`),
//! so we derive the keys to track from the events flowing through the indexer.

use std::str::FromStr;

use lumenqraph_core::NewEvent;
use stellar_xdr::curr::{ContractDataDurability, ScAddress, ScSymbol, ScVal, ScVec};

/// A configurable template for building storage keys from event parameters.
#[derive(Debug, Clone)]
pub struct KeyTemplate {
    /// The symbol for the storage key variant (e.g., "Balance", "Allowance").
    pub symbol: String,
    /// Event names that should trigger key discovery (e.g., ["transfer", "mint"]).
    pub event_names: Vec<String>,
    /// Parameter indices from decoded_topics to use as key arguments.
    pub param_indices: Vec<usize>,
    /// Storage durability: persistent or temporary.
    pub durability: ContractDataDurability,
    /// Optional label for grouping in contract_data table (e.g., "allowance").
    pub label: Option<String>,
}

/// Token events that name a holder address in their topics.
const HOLDER_EVENTS: [&str; 4] = ["transfer", "mint", "burn", "clawback"];

impl KeyTemplate {
    /// Extract keys from an event according to this template.
    pub fn keys_from_event(&self, e: &NewEvent) -> Vec<Vec<String>> {
        // Check if event name matches
        let event_name = match &e.event_name {
            Some(n) => n,
            None => return Vec::new(),
        };
        
        if !self.event_names.iter().any(|en| en == event_name) {
            return Vec::new();
        }

        // Extract parameters at specified indices
        let mut params: Vec<Vec<String>> = vec![Vec::new()];
        
        for &idx in &self.param_indices {
            if idx >= e.decoded_topics.len() {
                return Vec::new();
            }
            
            let topic = &e.decoded_topics[idx];
            let addresses = extract_addresses(topic);
            
            if addresses.is_empty() {
                return Vec::new();
            }
            
            // Cartesian product for multi-param keys
            let mut next_params = Vec::new();
            for base in params {
                for addr in &addresses {
                    let mut combo = base.clone();
                    combo.push(addr.clone());
                    next_params.push(combo);
                }
            }
            params = next_params;
        }
        
        params
    }

    /// Build a storage key from parameters.
    pub fn build_key(&self, params: &[String]) -> anyhow::Result<ScVal> {
        if params.len() != self.param_indices.len() {
            anyhow::bail!("param count mismatch");
        }

        let mut entries: Vec<ScVal> = vec![ScVal::Symbol(ScSymbol(self.symbol.as_str().try_into()?))];
        
        for param in params {
            let addr = ScAddress::from_str(param)
                .map_err(|_| anyhow::anyhow!("invalid address strkey {param:?}"))?;
            entries.push(ScVal::Address(addr));
        }
        
        Ok(ScVal::Vec(Some(ScVec(entries.try_into()?))))
    }
}

/// Extract address strkeys from a JSON value (may be a single string or nested).
fn extract_addresses(val: &serde_json::Value) -> Vec<String> {
    match val {
        serde_json::Value::String(s) if is_address(s) => vec![s.clone()],
        serde_json::Value::Array(arr) => {
            arr.iter().flat_map(extract_addresses).collect()
        }
        _ => Vec::new(),
    }
}

/// Build a SEP-41-style balance storage key: `Vec[Symbol(symbol), Address]`
/// (the shape the soroban token reference and most SEP-41 tokens use for
/// `DataKey::Balance(addr)`). `symbol` is configurable because some contracts
/// name the variant differently.
pub fn balance_key(symbol: &str, address: &str) -> anyhow::Result<ScVal> {
    let addr = ScAddress::from_str(address)
        .map_err(|_| anyhow::anyhow!("invalid address strkey {address:?}"))?;
    let entries: Vec<ScVal> = vec![
        ScVal::Symbol(ScSymbol(symbol.try_into()?)),
        ScVal::Address(addr),
    ];
    Ok(ScVal::Vec(Some(ScVec(entries.try_into()?))))
}

/// The holder addresses named in a token event's decoded topics. Any topic that
/// is a valid Stellar address strkey (`G…` account or `C…` contract) counts, so
/// this is robust to argument-order differences between token implementations.
/// Returns empty for events that don't concern a holder.
pub fn holders_in_event(e: &NewEvent) -> Vec<String> {
    let is_holder_event = e
        .event_name
        .as_deref()
        .map(|n| HOLDER_EVENTS.contains(&n))
        .unwrap_or(false);
    if !is_holder_event {
        return Vec::new();
    }
    e.decoded_topics
        .iter()
        .filter_map(|t| t.as_str())
        .filter(|s| is_address(s))
        .map(|s| s.to_string())
        .collect()
}

/// Whether a string is a valid Stellar address strkey.
fn is_address(s: &str) -> bool {
    (s.starts_with('G') || s.starts_with('C')) && ScAddress::from_str(s).is_ok()
}

/// Parse a durability config string. Anything other than "temporary" (case
/// insensitive) is treated as persistent — the common case for balances.
pub fn parse_durability(s: &str) -> ContractDataDurability {
    if s.trim().eq_ignore_ascii_case("temporary") {
        ContractDataDurability::Temporary
    } else {
        ContractDataDurability::Persistent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use stellar_xdr::curr::{
        AccountId, ContractId, Hash, Limits, PublicKey as XdrPublicKey, Uint256, WriteXdr,
    };

    // Build guaranteed-valid strkeys straight from XDR types (checksums correct
    // by construction), so the test never depends on a hand-copied address.
    fn account_strkey(seed: u8) -> String {
        ScAddress::Account(AccountId(XdrPublicKey::PublicKeyTypeEd25519(Uint256(
            [seed; 32],
        ))))
        .to_string()
    }
    fn contract_strkey(seed: u8) -> String {
        ScAddress::Contract(ContractId(Hash([seed; 32]))).to_string()
    }

    fn event(name: Option<&str>, topics: Vec<Value>) -> NewEvent {
        NewEvent {
            event_id: "e1".into(),
            contract_id: "C1".into(),
            ledger: 1,
            ledger_closed_at: chrono::Utc::now(),
            event_type: "contract".into(),
            topics: vec![],
            decoded_topics: topics,
            event_name: name.map(String::from),
            value: String::new(),
            decoded_value: Value::Null,
            enriched: None,
            tx_hash: "tx".into(),
            in_successful_call: true,
            paging_token: "e1".into(),
        }
    }

    #[test]
    fn balance_key_is_symbol_then_address_vec() {
        let key = balance_key("Balance", &account_strkey(1)).expect("valid key");
        // Round-trips through XDR (proves it's a well-formed ScVal).
        assert!(key.to_xdr_base64(Limits::none()).is_ok());
        match key {
            ScVal::Vec(Some(v)) => {
                assert_eq!(v.len(), 2);
                assert!(matches!(v[0], ScVal::Symbol(_)));
                assert!(matches!(v[1], ScVal::Address(_)));
            }
            _ => panic!("expected a 2-element vec key"),
        }
    }

    #[test]
    fn balance_key_rejects_bad_address() {
        assert!(balance_key("Balance", "not-an-address").is_err());
    }

    #[test]
    fn extracts_holders_from_transfer() {
        let from = account_strkey(1);
        let to = contract_strkey(2);
        let e = event(
            Some("transfer"),
            vec![json!("transfer"), json!(from), json!(to)],
        );
        let holders = holders_in_event(&e);
        assert_eq!(holders, vec![from, to]);
    }

    #[test]
    fn ignores_non_holder_events_and_non_addresses() {
        // A non-holder event yields nothing even if it carries an address.
        assert!(
            holders_in_event(&event(Some("set_admin"), vec![json!(account_strkey(3))])).is_empty()
        );
        // A holder event with no address topics yields nothing.
        assert!(
            holders_in_event(&event(Some("mint"), vec![json!("mint"), json!("100")])).is_empty()
        );
    }
}
