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
use stellar_xdr::curr::{
    AccountId, Asset, ContractDataDurability, LedgerKey, ScAddress, ScSymbol, ScVal, ScVec,
};

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

/// Upper bound on the number of topic segments a Soroban event can carry. A
/// `param_indices` entry at or beyond this can never address a real topic, so
/// it is rejected at config load rather than silently matching nothing.
const MAX_TOPIC_SEGMENTS: usize = 4;

impl KeyTemplate {
    /// Validate this template at config load. Every mistake that would
    /// otherwise fail silently per event at runtime (an unparseable symbol, an
    /// empty event list, an empty or nonsensical index list, an index that can
    /// never address a topic) is surfaced here as a startup error naming the
    /// `KEY_TEMPLATES` variable and the offending template.
    pub fn validate(&self) -> anyhow::Result<()> {
        let ident = self.identifier();

        // Soroban symbols are 1-32 characters from [a-zA-Z0-9_].
        let symbol_ok = !self.symbol.is_empty()
            && self.symbol.len() <= 32
            && self
                .symbol
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if !symbol_ok || ScSymbol::try_from(self.symbol.as_str()).is_err() {
            anyhow::bail!(
                "KEY_TEMPLATES: template {ident:?} has invalid symbol {:?}: \
                 must be 1-32 characters from the ScSymbol charset",
                self.symbol
            );
        }

        if self.event_names.is_empty() {
            anyhow::bail!(
                "KEY_TEMPLATES: template {ident:?} has an empty event_names list; \
                 it would never match any event"
            );
        }

        if self.param_indices.is_empty() {
            anyhow::bail!(
                "KEY_TEMPLATES: template {ident:?} has an empty param_indices list; \
                 specify at least one topic index"
            );
        }

        for &idx in &self.param_indices {
            if idx == 0 {
                anyhow::bail!(
                    "KEY_TEMPLATES: template {ident:?} uses param_indices entry 0, which is the \
                     event-name topic; use indices >= 1"
                );
            }
            if idx >= MAX_TOPIC_SEGMENTS {
                anyhow::bail!(
                    "KEY_TEMPLATES: template {ident:?} uses param_indices entry {idx}, which is \
                     out of range (topics have at most {MAX_TOPIC_SEGMENTS} segments)"
                );
            }
        }

        Ok(())
    }

    /// A human-readable identifier for this template in error messages: the
    /// label when set, otherwise the symbol.
    fn identifier(&self) -> String {
        self.label.clone().unwrap_or_else(|| self.symbol.clone())
    }

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

        let mut entries: Vec<ScVal> =
            vec![ScVal::Symbol(ScSymbol(self.symbol.as_str().try_into()?))];

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
        serde_json::Value::Array(arr) => arr.iter().flat_map(extract_addresses).collect(),
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

/// Whether a strkey names a classic account (`G…`) rather than a contract (`C…`).
pub fn is_account_address(s: &str) -> bool {
    s.starts_with('G') && ScAddress::from_str(s).is_ok()
}

/// Whether a strkey names a contract (`C…`).
pub fn is_contract_address(s: &str) -> bool {
    s.starts_with('C') && ScAddress::from_str(s).is_ok()
}

/// Parse a SEP-11 asset string (`native`, `CODE:ISSUER`, or `CODE:ISSUER:CONTRACT`)
/// into an [`Asset`]. Returns `None` for anything that isn't a valid asset.
pub fn asset_from_sep11(s: &str) -> Option<Asset> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("native") || s.eq_ignore_ascii_case("xlm") {
        return Some(Asset::Native);
    }
    let mut parts = s.split(':');
    let code = parts.next()?;
    let issuer = parts.next()?;
    // A third component (the SAC contract id) is allowed and ignored here.
    let _ = parts.next();
    if parts.next().is_some() {
        return None;
    }
    let issuer = AccountId::from_str(issuer).ok()?;
    if code.len() <= 4 {
        let code: [u8; 4] = {
            let mut buf = [0u8; 4];
            let bytes = code.as_bytes();
            buf[..bytes.len()].copy_from_slice(bytes);
            buf
        };
        Some(Asset::CreditAlphanum4(stellar_xdr::curr::AlphaNum4 {
            asset_code: stellar_xdr::curr::AssetCode4(code),
            issuer,
        }))
    } else if code.len() <= 12 {
        let code: [u8; 12] = {
            let mut buf = [0u8; 12];
            let bytes = code.as_bytes();
            buf[..bytes.len()].copy_from_slice(bytes);
            buf
        };
        Some(Asset::CreditAlphanum12(stellar_xdr::curr::AlphaNum12 {
            asset_code: stellar_xdr::curr::AssetCode12(code),
            issuer,
        }))
    } else {
        None
    }
}

/// Derive the wrapped [`Asset`] from a SAC's `name` instance-storage value.
///
/// The SAC stores its asset name as a string: `"native"` for XLM, or the
/// SEP-11 `CODE:ISSUER` form for issued assets. Returns `None` if the value is
/// not a string or does not parse as an asset.
pub fn asset_from_sac_name(name: &ScVal) -> Option<Asset> {
    match name {
        ScVal::String(s) => {
            let text = s.to_utf8_string().ok()?;
            asset_from_sep11(&text)
        }
        _ => None,
    }
}

/// The ledger key to read for a holder's balance.
///
/// For a SAC, a classic account (`G…`) holds its balance in a trustline (or the
/// native account entry for XLM), not in contract data — so we build the
/// matching [`LedgerKey`]. For contract holders (`C…`) and for non-SAC
/// contracts, balances live in contract data under `Vec[Symbol(symbol), Address]`.
///
/// `asset` is the asset derived from the SAC (see [`asset_from_sac_name`] /
/// [`asset_from_sep11`]); it is only consulted when `is_sac` is true and the
/// holder is a `G…` account.
pub fn balance_ledger_key(
    is_sac: bool,
    asset: Option<&Asset>,
    holder: &str,
    symbol: &str,
) -> anyhow::Result<LedgerKey> {
    if is_sac && is_account_address(holder) {
        let asset = asset.ok_or_else(|| {
            anyhow::anyhow!("SAC account holder {holder:?} requires a derived asset")
        })?;
        let account_id = AccountId::from_str(holder)
            .map_err(|_| anyhow::anyhow!("invalid account strkey {holder:?}"))?;
        return Ok(match asset {
            Asset::Native => LedgerKey::Account(stellar_xdr::curr::LedgerKeyAccount { account_id }),
            Asset::CreditAlphanum4(a) => {
                LedgerKey::Trustline(stellar_xdr::curr::LedgerKeyTrustLine {
                    account_id,
                    asset: stellar_xdr::curr::TrustLineAsset::CreditAlphanum4(a.clone()),
                })
            }
            Asset::CreditAlphanum12(a) => {
                LedgerKey::Trustline(stellar_xdr::curr::LedgerKeyTrustLine {
                    account_id,
                    asset: stellar_xdr::curr::TrustLineAsset::CreditAlphanum12(a.clone()),
                })
            }
        });
    }

    // Contract holder, or non-SAC contract: contract-data Balance key.
    let contract = ScAddress::from_str(holder)
        .map_err(|_| anyhow::anyhow!("invalid address strkey {holder:?}"))?;
    Ok(LedgerKey::ContractData(
        stellar_xdr::curr::LedgerKeyContractData {
            contract,
            key: balance_key(symbol, holder)?,
            durability: ContractDataDurability::Persistent,
        },
    ))
}

/// Parse a durability config string strictly. Only `persistent` or `temporary`
/// (case-insensitive, surrounding whitespace ignored) are accepted; anything
/// else — including typos like `instance` or `temp` — is an error, so a
/// misconfigured key template fails startup instead of silently reading the
/// wrong storage.
pub fn parse_durability(s: &str) -> anyhow::Result<ContractDataDurability> {
    match s.trim().to_ascii_lowercase().as_str() {
        "persistent" => Ok(ContractDataDurability::Persistent),
        "temporary" => Ok(ContractDataDurability::Temporary),
        _ => anyhow::bail!("invalid durability {s:?}: expected \"persistent\" or \"temporary\""),
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
            event_type: lumenqraph_core::EventType::Contract,
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
    fn parse_durability_accepts_valid_values() {
        assert_eq!(
            parse_durability("persistent").expect("valid"),
            ContractDataDurability::Persistent
        );
        assert_eq!(
            parse_durability("temporary").expect("valid"),
            ContractDataDurability::Temporary
        );
        // Case-insensitive and trimmed.
        assert_eq!(
            parse_durability("  Persistent ").expect("valid"),
            ContractDataDurability::Persistent
        );
        assert_eq!(
            parse_durability("TEMPORARY").expect("valid"),
            ContractDataDurability::Temporary
        );
    }

    #[test]
    fn parse_durability_rejects_invalid_values() {
        for bad in ["instance", "temp", "", "  ", "persistant", "temporarily"] {
            let err = parse_durability(bad).expect_err("should reject");
            let msg = err.to_string();
            assert!(msg.contains("persistent"), "message: {msg}");
            assert!(msg.contains("temporary"), "message: {msg}");
        }
    }

    fn valid_template() -> KeyTemplate {
        KeyTemplate {
            symbol: "Balance".into(),
            event_names: vec!["transfer".into()],
            param_indices: vec![1],
            durability: ContractDataDurability::Persistent,
            label: Some("balance".into()),
        }
    }

    #[test]
    fn validate_accepts_valid_template() {
        valid_template().validate().expect("valid template");
    }

    #[test]
    fn validate_rejects_invalid_symbol() {
        // Too long for an ScSymbol (max 32 chars).
        let mut t = valid_template();
        t.symbol = "X".repeat(33);
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("balance"), "message: {msg}");

        // Invalid character for an ScSymbol.
        let mut t = valid_template();
        t.symbol = "Bad Symbol!".into();
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("balance"), "message: {msg}");
    }

    #[test]
    fn validate_rejects_empty_event_names() {
        let mut t = valid_template();
        t.event_names.clear();
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("balance"), "message: {msg}");
    }

    #[test]
    fn validate_rejects_empty_param_indices() {
        let mut t = valid_template();
        t.param_indices.clear();
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("balance"), "message: {msg}");
    }

    #[test]
    fn validate_rejects_zero_param_index() {
        let mut t = valid_template();
        t.param_indices = vec![0];
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("balance"), "message: {msg}");
    }

    #[test]
    fn validate_rejects_out_of_range_param_index() {
        let mut t = valid_template();
        t.param_indices = vec![MAX_TOPIC_SEGMENTS];
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("balance"), "message: {msg}");
    }

    #[test]
    fn validate_error_names_template_by_symbol_without_label() {
        let mut t = valid_template();
        t.label = None;
        t.event_names.clear();
        let msg = t.validate().expect_err("should reject").to_string();
        assert!(msg.contains("KEY_TEMPLATES"), "message: {msg}");
        assert!(msg.contains("Balance"), "message: {msg}");
    }

    #[test]
    fn sac_account_holder_produces_trustline_key() {
        // USDC-like issued asset wrapped by a SAC.
        let issuer = AccountId::from_str(&account_strkey(9)).expect("valid issuer");
        let asset = Asset::CreditAlphanum4(stellar_xdr::curr::AlphaNum4 {
            asset_code: stellar_xdr::curr::AssetCode4(*b"USDC"),
            issuer: issuer.clone(),
        });
        let holder = account_strkey(1);
        let key = balance_ledger_key(true, Some(&asset), &holder, "Balance").expect("key");
        match key {
            LedgerKey::Trustline(tl) => {
                assert_eq!(tl.account_id, AccountId::from_str(&holder).unwrap());
                assert_eq!(
                    tl.asset,
                    stellar_xdr::curr::TrustLineAsset::CreditAlphanum4(match &asset {
                        Asset::CreditAlphanum4(a) => a.clone(),
                        _ => unreachable!(),
                    })
                );
            }
            other => panic!("expected trustline key, got {other:?}"),
        }
    }

    #[test]
    fn sac_native_account_holder_produces_account_key() {
        let key = balance_ledger_key(true, Some(&Asset::Native), &account_strkey(2), "Balance")
            .expect("key");
        assert!(matches!(key, LedgerKey::Account(_)));
    }

    #[test]
    fn sac_contract_holder_produces_contract_data_key() {
        let asset = Asset::Native;
        let holder = contract_strkey(3);
        let key = balance_ledger_key(true, Some(&asset), &holder, "Balance").expect("key");
        match key {
            LedgerKey::ContractData(cd) => {
                assert_eq!(cd.contract, ScAddress::from_str(&holder).unwrap());
                assert!(matches!(cd.key, ScVal::Vec(Some(_))));
            }
            other => panic!("expected contract-data key, got {other:?}"),
        }
    }

    #[test]
    fn non_sac_account_holder_uses_contract_data_key() {
        // Non-SAC contract: even a G… holder is read from contract data.
        let holder = account_strkey(4);
        let key = balance_ledger_key(false, None, &holder, "Balance").expect("key");
        assert!(matches!(key, LedgerKey::ContractData(_)));
    }

    #[test]
    fn asset_from_sep11_parses_forms() {
        assert_eq!(asset_from_sep11("native"), Some(Asset::Native));
        let issuer = account_strkey(7);
        assert!(matches!(
            asset_from_sep11(&format!("USDC:{issuer}")),
            Some(Asset::CreditAlphanum4(_))
        ));
        assert!(matches!(
            asset_from_sep11(&format!("LONGCODE:{issuer}")),
            Some(Asset::CreditAlphanum12(_))
        ));
        assert!(asset_from_sep11("not-an-asset").is_none());
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
