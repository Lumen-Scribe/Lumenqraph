//! Self-contained Soroban XDR decoding.
//!
//! Soroban event topics and values are base64-encoded XDR `ScVal`s. Rather than
//! depend on the fast-moving `stellar-xdr` crate, we decode the (stable) ScVal
//! wire format directly into friendly JSON. Integers that don't fit a JS number
//! are rendered as decimal strings; addresses are rendered as strkeys
//! (`G...`/`C...`/`M...`/`B...`/`L...`); bytes as hex.
//!
//! Decoding is always best-effort: on any malformed input we fall back to
//! `{"_type":"unknown","xdr":"<base64>"}` so nothing is lost and one weird
//! event can't break ingestion.
//!
//! ## JSON dialect
//!
//! The table below documents exactly how each `ScVal` variant maps to JSON.
//! See also [`docs/API.md`](../../../docs/API.md) for the consumer-facing
//! description.
//!
//! The table is backed by the `dialect_snapshot` test in this module, which
//! decodes one sample of each variant and snapshots the output
//! (`snapshots/*dialect_snapshot*`). If you change a mapping, update the table
//! and re-accept the snapshot together.
//!
//! | XDR variant               | JSON representation                                 |
//! |---------------------------|-----------------------------------------------------|
//! | `ScvBool`                 | `true` / `false`                                    |
//! | `ScvVoid`                 | `null`                                              |
//! | `ScvError`                | `{"error":{"type":"<name>","code":<code>}}`         |
//! | `ScvU32`                  | JSON number (u32 fits safely in f64)                |
//! | `ScvI32`                  | JSON number (i32 fits safely in f64)                |
//! | `ScvU64`                  | decimal string (e.g. `"18446744073709551615"`)       |
//! | `ScvI64`                  | decimal string (e.g. `"-9223372036854775808"`)       |
//! | `ScvTimepoint`            | decimal string (same wire as u64)                   |
//! | `ScvDuration`             | decimal string (same wire as u64)                   |
//! | `ScvU128`                 | decimal string                                      |
//! | `ScvI128`                 | decimal string                                      |
//! | `ScvU256`                 | decimal string                                      |
//! | `ScvI256`                 | decimal string                                      |
//! | `ScvBytes`                | hex string prefixed `0x` (e.g. `"0xdead"`)         |
//! | `ScvString`               | UTF-8 string, or hex `"0x…"` if invalid UTF-8       |
//! | `ScvSymbol`               | UTF-8 string, or hex `"0x…"` if invalid UTF-8       |
//! | `ScvVec`                  | JSON array (empty array for absent/`None` variant)  |
//! | `ScvMap` (no collisions)  | JSON object keyed by string (symbol/string keys)    |
//! | `ScvMap` (collisions/mixed keys) | array of `{"key":…,"val":…}` pair objects  |
//! | `ScvAddress` (account)    | `G…` strkey (56 chars)                              |
//! | `ScvAddress` (contract)   | `C…` strkey (56 chars)                              |
//! | `ScvAddress` (muxed)      | `M…` strkey (69 chars)                              |
//! | `ScvAddress` (claimable balance) | `B…` strkey                                  |
//! | `ScvAddress` (liquidity pool)    | `L…` strkey                                  |
//! | `ScvAddress` (unknown type) | whole value falls back to `{"_type":"unknown","xdr":"<base64>"}` |
//! | `ScvContractInstance` (19)| `{"_xdr_tag":19}` — payload consumed safely         |
//! | `ScvLedgerKeyContractInstance` (20) | `{"_xdr_tag":20}` — no payload            |
//! | `ScvLedgerKeyNonce` (21)  | `{"_xdr_tag":21}` — payload consumed safely         |
//! | unknown tag               | whole value falls back to `{"_type":"unknown","xdr":"<base64>"}` |
//!
//! Both the byte-level decoder ([`decode_scval_base64`]) and the typed decoder
//! ([`decode_scval`]) produce identical JSON: the byte path parses into a
//! `stellar_xdr::curr::ScVal` and delegates to [`decode_scval`], so there is a
//! single source of truth for the dialect.
//!
//! **Ambiguities to be aware of:**
//! - `Symbol("abc")` and `String("abc")` both decode to `"abc"` — the type
//!   information is lost in the generic decoder. The spec-driven `enriched`
//!   field (for contracts with on-chain specs) always preserves type names.
//! - `Bytes([0xde,0xad])` decodes to `"0xdead"`, which is indistinguishable
//!   from `String("0xdead")` in the generic output. Use `enriched` when the
//!   distinction matters.
//! - `u64`, `i64`, `u128`, `i128`, `Timepoint`, and `Duration` all render as
//!   decimal strings — clients cannot distinguish them without the `enriched`
//!   type annotation.
//!
//! The raw base64 XDR is **always** retained alongside the decoded JSON so no
//! information is permanently lost.

use base64::Engine;
use serde_json::{json, Value};
use stellar_strkey::Contract;

/// Decode a base64 `ScVal` into friendly JSON. Never panics.
///
/// Returns a fallback `{"_type":"unknown","xdr":"<base64>"}` on any parse
/// error, including trailing bytes after the top-level value (which would
/// indicate cursor misalignment and corrupt a sibling value if ignored).
///
/// #608: the bytes are parsed into a typed `ScVal` (with `Limits`) and handed
/// to [`decode_scval`], so the byte path and the typed path can never drift.
pub fn decode_scval_base64(b64: &str) -> Value {
    use stellar_xdr::curr::{Limited, ReadXdr, ScVal};

    let fallback = || json!({ "_type": "unknown", "xdr": b64 });
    let bytes = match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => bytes,
        Err(_) => return fallback(),
    };
    // #410: reject trailing bytes — they indicate a misaligned cursor that
    // would corrupt siblings in a vec/map context.
    let mut limited = Limited::new(bytes.as_slice(), untrusted_limits(bytes.len()));
    match ScVal::read_xdr(&mut limited) {
        Ok(sv) if limited.inner.is_empty() => decode_scval(&sv),
        _ => fallback(),
    }
}

/// Decode an already-parsed `ScVal` directly to friendly JSON, without any
/// base64 round-trip. Produces the same JSON shapes as [`decode_scval_base64`].
///
/// This is the fast path used by the read layer and state snapshots, where the
/// value is already in memory as a typed Rust enum. Converting back to XDR and
/// then base64 just to feed the byte-level decoder is wasted work.
pub fn decode_scval(sv: &stellar_xdr::curr::ScVal) -> Value {
    decode_scval_at(sv, 0)
}

/// #759: maximum nesting depth for untrusted `ScVal`s, shared by parsing and decoding.
pub const MAX_DEPTH: u32 = 256;

/// #759: the single set of XDR limits used for all untrusted parsing in core.
fn untrusted_limits(len: usize) -> stellar_xdr::curr::Limits {
    stellar_xdr::curr::Limits {
        depth: MAX_DEPTH,
        len,
    }
}

fn decode_scval_at(sv: &stellar_xdr::curr::ScVal, depth: u32) -> Value {
    use stellar_xdr::curr::{Limits, ScError, ScVal, WriteXdr};
    if depth > MAX_DEPTH {
        let xdr = sv.to_xdr_base64(Limits::none()).unwrap_or_default();
        return json!({ "_type": "unknown", "xdr": xdr });
    }
    let decode_scval = |v: &ScVal| decode_scval_at(v, depth + 1);
    match sv {
        ScVal::Bool(b) => Value::Bool(*b),
        ScVal::Void => Value::Null,
        // #760: keep the error type and code.
        ScVal::Error(e) => {
            let code = match e {
                ScError::Contract(c) => json!(c),
                ScError::WasmVm(c)
                | ScError::Context(c)
                | ScError::Storage(c)
                | ScError::Object(c)
                | ScError::Crypto(c)
                | ScError::Events(c)
                | ScError::Budget(c)
                | ScError::Value(c)
                | ScError::Auth(c) => json!(c.name()),
            };
            json!({ "_type": "error", "kind": e.name(), "code": code })
        }
        ScVal::U32(n) => json!(n),
        ScVal::I32(n) => json!(n),
        ScVal::U64(n) => Value::String(n.to_string()),
        ScVal::I64(n) => Value::String(n.to_string()),
        ScVal::Timepoint(t) => Value::String(t.0.to_string()),
        ScVal::Duration(d) => Value::String(d.0.to_string()),
        ScVal::U128(p) => {
            let hi = p.hi as u128;
            let lo = p.lo as u128;
            Value::String(((hi << 64) | lo).to_string())
        }
        ScVal::I128(p) => {
            let hi = p.hi as i128;
            let lo = p.lo as i128;
            Value::String(((hi << 64) | lo).to_string())
        }
        // #409: render 256-bit integers as decimal strings, matching i128.
        ScVal::U256(p) => Value::String(u256_to_decimal(p)),
        ScVal::I256(p) => Value::String(i256_to_decimal(p)),
        ScVal::Bytes(b) => Value::String(format!("0x{}", hex(&b.0))),
        // #608: invalid UTF-8 renders as hex, matching the documented dialect.
        ScVal::String(s) => match std::str::from_utf8(&s.0) {
            Ok(text) => Value::String(text.to_string()),
            Err(_) => Value::String(format!("0x{}", hex(&s.0))),
        },
        ScVal::Symbol(s) => match std::str::from_utf8(&s.0) {
            Ok(text) => Value::String(text.to_string()),
            Err(_) => Value::String(format!("0x{}", hex(&s.0))),
        },
        ScVal::Vec(opt) => match opt {
            None => Value::Array(vec![]),
            Some(v) => Value::Array(v.0.iter().map(decode_scval).collect()),
        },
        ScVal::Map(opt) => match opt {
            None => Value::Object(serde_json::Map::new()),
            Some(m) => {
                // Mirror the byte-level decoder: prefer a plain object when all
                // keys are symbols/strings; fall back to [{key, val}] otherwise.
                let mut obj = serde_json::Map::new();
                let mut pairs = Vec::new();
                let mut all_stringy = true;
                for entry in m.0.iter() {
                    let k = decode_scval(&entry.key);
                    let v = decode_scval(&entry.val);
                    match &k {
                        // #760: a duplicate key would overwrite; use the pairs form.
                        Value::String(s) if all_stringy && !obj.contains_key(s) => {
                            obj.insert(s.clone(), v.clone());
                        }
                        _ => all_stringy = false,
                    }
                    pairs.push(json!({ "key": k, "val": v }));
                }
                if all_stringy {
                    Value::Object(obj)
                } else {
                    Value::Array(pairs)
                }
            }
        },
        ScVal::Address(addr) => Value::String(addr.to_string()),
        ScVal::LedgerKeyContractInstance => json!({ "_type": "LedgerKeyContractInstance" }),
        ScVal::LedgerKeyNonce(n) => json!({ "_type": "LedgerKeyNonce", "nonce": n.nonce }),
        ScVal::ContractInstance(inst) => {
            // Best-effort: represent the instance type.
            use stellar_xdr::curr::ContractExecutable;
            match &inst.executable {
                ContractExecutable::Wasm(hash) => {
                    json!({ "_type": "ContractInstance", "wasm_hash": hex(&hash.0) })
                }
                ContractExecutable::StellarAsset => {
                    json!({ "_type": "ContractInstance", "stellar_asset": true })
                }
            }
        }
    }
}

/// Render a `UInt256Parts` as a decimal string.
fn u256_to_decimal(p: &stellar_xdr::curr::UInt256Parts) -> String {
    let mut limbs = [p.hi_hi as u64, p.hi_lo, p.lo_hi, p.lo_lo];
    // Repeated division by 10^19 (largest power of ten fitting in a u64).
    const CHUNK: u64 = 10_000_000_000_000_000_000;
    let mut chunks: Vec<u64> = Vec::new();
    loop {
        let mut rem: u128 = 0;
        let mut all_zero = true;
        for limb in limbs.iter_mut() {
            let cur = (rem << 64) | (*limb as u128);
            *limb = (cur / CHUNK as u128) as u64;
            rem = cur % CHUNK as u128;
            if *limb != 0 {
                all_zero = false;
            }
        }
        chunks.push(rem as u64);
        if all_zero {
            break;
        }
    }
    let mut s = chunks.pop().unwrap_or(0).to_string();
    while let Some(c) = chunks.pop() {
        s.push_str(&format!("{c:019}"));
    }
    s
}

/// Render an `Int256Parts` as a decimal string (two's complement).
fn i256_to_decimal(p: &stellar_xdr::curr::Int256Parts) -> String {
    let negative = p.hi_hi < 0;
    if !negative {
        let unsigned = stellar_xdr::curr::UInt256Parts {
            // `hi_hi` is signed in `Int256Parts`; the bit pattern is preserved.
            hi_hi: p.hi_hi as u64,
            hi_lo: p.hi_lo,
            lo_hi: p.lo_hi,
            lo_lo: p.lo_lo,
        };
        return u256_to_decimal(&unsigned);
    }
    // Negate via two's complement: invert all bits then add one.
    let mut limbs = [p.hi_hi as u64, p.hi_lo, p.lo_hi, p.lo_lo];
    for limb in limbs.iter_mut() {
        *limb = !*limb;
    }
    let mut carry = 1u128;
    for limb in limbs.iter_mut().rev() {
        let sum = (*limb as u128) + carry;
        *limb = sum as u64;
        carry = sum >> 64;
    }
    let unsigned = stellar_xdr::curr::UInt256Parts {
        hi_hi: limbs[0],
        hi_lo: limbs[1],
        lo_hi: limbs[2],
        lo_lo: limbs[3],
    };
    format!("-{}", u256_to_decimal(&unsigned))
}

/// Decode each base64 topic into friendly JSON.
pub fn decode_topics(topics: &[String]) -> Vec<Value> {
    topics.iter().map(|t| decode_scval_base64(t)).collect()
}

/// Best-effort event name: `topic[0]` decoded as a Symbol/String.
///
/// #609: only `SCV_SYMBOL` (and `SCV_STRING`) first topics yield a name.
/// Addresses, integers, bytes and other variants return `None` so that
/// event-name filters and stats are not polluted with addresses/numbers.
pub fn event_name_from_topic(topic_b64: &str) -> Option<String> {
    use stellar_xdr::curr::{Limited, ReadXdr, ScVal};

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(topic_b64)
        .ok()?;
    let mut limited = Limited::new(bytes.as_slice(), untrusted_limits(bytes.len()));
    let sv = ScVal::read_xdr(&mut limited).ok()?;
    if !limited.inner.is_empty() {
        return None;
    }
    match sv {
        ScVal::Symbol(s) => std::str::from_utf8(&s.0).ok().map(str::to_string),
        ScVal::String(s) => std::str::from_utf8(&s.0).ok().map(str::to_string),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---- Stellar Asset Contract (SAC) balance helpers ------------------------
//
// #745: For a SAC, the balance of a classic account (`G…`) lives in its
// trustline (or the native account entry for XLM), not in a `Balance`
// contract-data entry. These helpers let the indexer derive the asset a SAC
// wraps, build the right `LedgerKey` for a holder, and decode the resulting
// balance into the same JSON shape used for contract-data balances.

/// A classic Stellar asset, as wrapped by a Stellar Asset Contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SacAsset {
    /// The native asset (XLM).
    Native,
    /// A credit asset identified by its 4- or 12-character code and issuer.
    Credit { code: String, issuer: String },
}

impl SacAsset {
    /// Parse a SEP-11 asset string (`native`, `CODE:ISSUER`, or
    /// `CODE:ISSUER:CONTRACT_ID`).
    ///
    /// The optional contract-ID suffix (used in CAP-67 `transfer` topics) is
    /// ignored. Returns `None` for malformed input.
    pub fn from_sep11(s: &str) -> Option<SacAsset> {
        if s == "native" {
            return Some(SacAsset::Native);
        }
        let mut parts = s.split(':');
        let code = parts.next()?;
        let issuer = parts.next()?;
        if code.is_empty() || issuer.is_empty() {
            return None;
        }
        // A third component (contract ID) is permitted and ignored.
        Some(SacAsset::Credit {
            code: code.to_string(),
            issuer: issuer.to_string(),
        })
    }

    /// Render as a SEP-11 asset string (`native` or `CODE:ISSUER`).
    pub fn to_sep11(&self) -> String {
        match self {
            SacAsset::Native => "native".to_string(),
            SacAsset::Credit { code, issuer } => format!("{code}:{issuer}"),
        }
    }
}

/// Derive the classic asset wrapped by a SAC from its instance storage.
///
/// A SAC's instance storage holds a `name` entry whose value is the SEP-11
/// asset string (e.g. `"native"` or `"USDC:G…"`). `entries` is the decoded
/// `ScVal::Map` of the contract instance storage (as returned by
/// [`decode_scval`] on the instance's `ScVal::ContractInstance`).
///
/// Returns `None` when no usable `name` entry is present.
pub fn sac_asset_from_instance_storage(instance: &Value) -> Option<SacAsset> {
    let storage = instance.get("storage")?.as_array()?;
    for entry in storage {
        let key = entry.get("key")?;
        let is_name = key
            .as_str()
            .map(|s| s == "name")
            .or_else(|| {
                key.get("symbol")
                    .and_then(|s| s.as_str())
                    .map(|s| s == "name")
            })
            .unwrap_or(false);
        if !is_name {
            continue;
        }
        let val = entry.get("val")?;
        if let Some(s) = val.as_str() {
            return SacAsset::from_sep11(s);
        }
    }
    None
}

/// Derive the classic asset wrapped by a SAC from a CAP-67 event topic.
///
/// CAP-67 `transfer`/`mint`/`burn`/`clawback` events carry the SEP-11 asset
/// string as a topic (either a plain string or a `String`/`Symbol` decoded to
/// a JSON string). Returns `None` when no topic parses as an asset string.
pub fn sac_asset_from_topics(topics: &[Value]) -> Option<SacAsset> {
    topics
        .iter()
        .filter_map(|t| t.as_str())
        .find_map(SacAsset::from_sep11)
}

/// Build the `LedgerKey` for a holder's balance of `asset`.
///
/// * `G…` holders use `LedgerKey::Trustline` for credit assets and
///   `LedgerKey::Account` for the native asset.
/// * `C…` holders use the `Balance` contract-data key of the SAC.
///
/// Returns `None` if `holder` is not a valid `G…`/`C…` strkey.
pub fn balance_ledger_key(
    holder: &str,
    asset: &SacAsset,
    sac_contract: &str,
) -> Option<stellar_xdr::curr::LedgerKey> {
    use stellar_xdr::curr::{
        AccountId, AlphaNum12, AlphaNum4, Asset, AssetCode12, AssetCode4, ContractDataDurability,
        ContractId, Hash, LedgerKey, PublicKey, ScAddress, ScVal, Uint256,
    };

    if holder.starts_with('C') {
        let contract = stellar_strkey::Contract::from_string(holder).ok()?;
        let sac = stellar_strkey::Contract::from_string(sac_contract).ok()?;
        let key = ScVal::Vec(Some(stellar_xdr::curr::ScVec(
            vec![
                ScVal::Symbol(stellar_xdr::curr::ScSymbol(
                    b"Balance".to_vec().try_into().ok()?,
                )),
                ScVal::Address(ScAddress::Contract(ContractId(Hash(contract.0)))),
            ]
            .try_into()
            .ok()?,
        )));
        return Some(LedgerKey::ContractData(
            stellar_xdr::curr::LedgerKeyContractData {
                contract: ScAddress::Contract(ContractId(Hash(sac.0))),
                key,
                durability: ContractDataDurability::Persistent,
            },
        ));
    }

    let account = stellar_strkey::ed25519::PublicKey::from_string(holder).ok()?;
    let account_id = AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(account.0)));

    let asset = match asset {
        SacAsset::Native => {
            return Some(LedgerKey::Account(stellar_xdr::curr::LedgerKeyAccount {
                account_id,
            }))
        }
        SacAsset::Credit { code, issuer } => {
            let issuer = stellar_strkey::ed25519::PublicKey::from_string(issuer).ok()?;
            let issuer_id = AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(issuer.0)));
            let code_bytes = code.as_bytes();
            if code_bytes.len() <= 4 {
                let mut buf = [0u8; 4];
                buf[..code_bytes.len()].copy_from_slice(code_bytes);
                Asset::CreditAlphanum4(AlphaNum4 {
                    asset_code: AssetCode4(buf),
                    issuer: issuer_id,
                })
            } else if code_bytes.len() <= 12 {
                let mut buf = [0u8; 12];
                buf[..code_bytes.len()].copy_from_slice(code_bytes);
                Asset::CreditAlphanum12(AlphaNum12 {
                    asset_code: AssetCode12(buf),
                    issuer: issuer_id,
                })
            } else {
                return None;
            }
        }
    };

    Some(LedgerKey::Trustline(
        stellar_xdr::curr::LedgerKeyTrustLine { account_id, asset },
    ))
}

/// Decode a trustline or account ledger entry into the same JSON shape used
/// for contract-data balances: `{"amount": "<i128 decimal>"}`.
///
/// Returns `None` for any other entry type.
pub fn decode_balance_entry(entry: &stellar_xdr::curr::LedgerEntryData) -> Option<Value> {
    use stellar_xdr::curr::LedgerEntryData;
    match entry {
        LedgerEntryData::Trustline(tl) => Some(json!({ "amount": tl.balance.to_string() })),
        LedgerEntryData::Account(acct) => Some(json!({ "amount": acct.balance.to_string() })),
        _ => None,
    }
}

// ---- Strkey validation (thin wrapper around stellar-strkey) --------------
//
// #413: All base32/CRC16 hand-rolled code has been removed. The canonical
// `stellar-strkey` crate (already a transitive dependency via `stellar-xdr`)
// is used exclusively for both encoding (in `read_address`) and validation.

/// Returns `true` if `s` is a well-formed Stellar contract ID (`C…` strkey).
///
/// This is a thin wrapper over [`stellar_strkey::Contract::from_string`].
/// All strkey encoding (base32 + CRC16-XModem) is delegated to that crate.
pub fn is_valid_contract_id(s: &str) -> bool {
    Contract::from_string(s).is_ok()
}

/// Parse and validate the `CONTRACT_IDS` environment variable string.
///
/// Accepts a comma-separated list of C-strkey contract addresses (or an empty
/// string / unset for "index everything"). Returns an error if:
/// * any entry is not a valid C-strkey,
/// * the number of *unique* entries exceeds the `getEvents` RPC limit of 25
///   (5 filters × 5 IDs).
///
/// Duplicates are removed while preserving first-seen order (a warning listing
/// them is logged), so repeated IDs don't waste filter capacity or produce
/// duplicate RPC filters.
///
/// This function is shared by all services that need to read `CONTRACT_IDS` so
/// that validation never drifts between the indexer, API, webhooks, and MCP.
pub fn parse_contract_ids(raw: &str) -> Result<Vec<String>, String> {
    let entries: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    for id in &entries {
        if !is_valid_contract_id(id) {
            return Err(format!(
                "invalid CONTRACT_ID {id:?}: expected a C\u{2026} strkey (Soroban contract address)"
            ));
        }
    }

    // #612: de-duplicate while preserving first-seen order.
    let mut seen = std::collections::HashSet::new();
    let mut duplicates: Vec<String> = Vec::new();
    let mut ids: Vec<String> = Vec::with_capacity(entries.len());
    for id in entries {
        if seen.insert(id.clone()) {
            ids.push(id);
        } else {
            duplicates.push(id);
        }
    }
    if !duplicates.is_empty() {
        tracing::warn!(
            duplicates = ?duplicates,
            "CONTRACT_IDS contains duplicate entries; ignoring repeats"
        );
    }

    const MAX_CONTRACT_IDS: usize = 25; // 5 filters × 5 IDs per filter
    if ids.len() > MAX_CONTRACT_IDS {
        return Err(format!(
            "CONTRACT_IDS contains {} unique entries, but getEvents supports at most {} \
             contract IDs (5 filters × 5 IDs per filter). \
             Remove {} contract IDs, or run multiple instances each covering a \
             different subset.",
            ids.len(),
            MAX_CONTRACT_IDS,
            ids.len() - MAX_CONTRACT_IDS,
        ));
    }

    Ok(ids)
}

// ---- Test helpers that produce canonical strkeys via stellar-strkey -------

#[cfg(test)]
fn make_contract_strkey(payload: &[u8; 32]) -> String {
    Contract(*payload).to_string()
}

#[cfg(test)]
use stellar_strkey::ed25519::PublicKey as Ed25519PublicKey;

#[cfg(test)]
fn make_account_strkey(payload: &[u8; 32]) -> String {
    Ed25519PublicKey(*payload).to_string()
}

/// ScValType / ScAddressType discriminants (stable wire tags) for the raw-byte
/// test vectors below.
#[cfg(test)]
mod wire_tags {
    pub const SCV_BOOL: u32 = 0;
    pub const SCV_VOID: u32 = 1;
    pub const SCV_ERROR: u32 = 2;
    pub const SCV_U32: u32 = 3;
    pub const SCV_I32: u32 = 4;
    pub const SCV_U64: u32 = 5;
    pub const SCV_I64: u32 = 6;
    pub const SCV_TIMEPOINT: u32 = 7;
    pub const SCV_DURATION: u32 = 8;
    pub const SCV_U128: u32 = 9;
    pub const SCV_I128: u32 = 10;
    pub const SCV_U256: u32 = 11;
    pub const SCV_I256: u32 = 12;
    pub const SCV_BYTES: u32 = 13;
    pub const SCV_STRING: u32 = 14;
    pub const SCV_SYMBOL: u32 = 15;
    pub const SCV_VEC: u32 = 16;
    pub const SCV_MAP: u32 = 17;
    pub const SCV_ADDRESS: u32 = 18;
    pub const SCV_CONTRACT_INSTANCE: u32 = 19;
    pub const SCV_LEDGER_KEY_CONTRACT_INSTANCE: u32 = 20;
    pub const SCV_LEDGER_KEY_NONCE: u32 = 21;
    pub const SC_ADDRESS_ACCOUNT: u32 = 0;
    pub const SC_ADDRESS_CONTRACT: u32 = 1;
}

#[cfg(test)]
mod tests {
    use super::wire_tags::*;
    use super::*;
    use base64::Engine;

    fn b64(s: &str) -> Value {
        decode_scval_base64(s)
    }

    #[test]
    fn decodes_symbol() {
        // ScVal::Symbol("fee") captured from testnet.
        assert_eq!(b64("AAAADwAAAANmZWUA"), Value::String("fee".into()));
        assert_eq!(
            event_name_from_topic("AAAADwAAAANmZWUA").as_deref(),
            Some("fee")
        );
    }

    // ── #609: event name only from symbol/string first topics ────────────

    #[test]
    fn event_name_from_symbol_topic() {
        // ScVal::Symbol("transfer")
        let mut bytes = SCV_SYMBOL.to_be_bytes().to_vec();
        bytes.extend_from_slice(&8u32.to_be_bytes());
        bytes.extend_from_slice(b"transfer");
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(event_name_from_topic(&raw).as_deref(), Some("transfer"));
    }

    #[test]
    fn event_name_from_string_topic() {
        // ScVal::String("hello")
        let mut bytes = SCV_STRING.to_be_bytes().to_vec();
        bytes.extend_from_slice(&5u32.to_be_bytes());
        bytes.extend_from_slice(b"hello");
        bytes.extend_from_slice(&[0u8; 3]); // pad
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(event_name_from_topic(&raw).as_deref(), Some("hello"));
    }

    #[test]
    fn event_name_none_for_address_topic() {
        // ScVal::Address(Account(...)) decodes to a G-strkey but is not a name.
        let raw = "AAAAEgAAAAAAAAAAZnYwtpgeUB4mlva1EnnCVBm0hGxbz5B5Zl89BaJLufM=";
        assert_eq!(event_name_from_topic(raw), None);
    }

    #[test]
    fn event_name_none_for_u64_topic() {
        // ScVal::U64(12345) decodes to "12345" but is not a name.
        let mut bytes = SCV_U64.to_be_bytes().to_vec();
        bytes.extend_from_slice(&12345u64.to_be_bytes());
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(event_name_from_topic(&raw), None);
    }

    #[test]
    fn event_name_none_for_i128_topic() {
        // ScVal::I128(300) decodes to "300" but is not a name.
        let raw = "AAAACgAAAAAAAAAAAAAAAAAAASw=";
        assert_eq!(event_name_from_topic(raw), None);
    }

    #[test]
    fn event_name_none_for_bytes_topic() {
        // ScVal::Bytes([0xde,0xad]) decodes to "0xdead" but is not a name.
        let mut bytes = SCV_BYTES.to_be_bytes().to_vec();
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&[0xde, 0xad]);
        bytes.extend_from_slice(&[0u8; 2]); // pad
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(event_name_from_topic(&raw), None);
    }

    #[test]
    fn event_name_none_for_malformed_topic() {
        assert_eq!(event_name_from_topic("not-base64!!"), None);
    }

    #[test]
    fn decodes_i128_amount() {
        // ScVal::I128 captured from a fee event (a small positive amount).
        let v = b64("AAAACgAAAAAAAAAAAAAAAAAAASw=");
        assert_eq!(v, Value::String("300".into()));
    }

    #[test]
    fn decodes_string() {
        // ScVal::String("HATU:GATAET3S...") from a set_authorized topic.
        let v = b64("AAAADgAAAD1IQVRVOkdBVEFFVDNTT01CVTdTVFFYTEczQzJDRVZPSlhNNFJTQUEyTVlWM09TRUtPSElJRUFGSkdDWExIAAAA");
        assert_eq!(
            v,
            Value::String("HATU:GATAET3SOMBU7STQXLG3C2CEVOJXM4RSAA2MYV3OSEKOHIIEAFJGCXLH".into())
        );
    }

    #[test]
    fn decodes_account_address_to_g_strkey() {
        // ScVal::Address(Account(...)) from a fee event topic.
        let v = b64("AAAAEgAAAAAAAAAAZnYwtpgeUB4mlva1EnnCVBm0hGxbz5B5Zl89BaJLufM=");
        match v {
            Value::String(s) => {
                assert!(s.starts_with('G'), "expected G-strkey, got {s}");
                assert_eq!(s.len(), 56, "ed25519 strkey should be 56 chars: {s}");
            }
            other => panic!("expected address string, got {other:?}"),
        }
    }

    #[test]
    fn decodes_vec() {
        // ScVal::Vec([...]) from an exposure_synced event value.
        let v = b64("AAAAEAAAAAEAAAADAAAACv///////////////8bZ+tEAAAAKAAAAAAAAAAAAAAARmN6/agAAAAoAAAAAAAAAAAAAAAAAAAAA");
        assert!(matches!(v, Value::Array(_)), "expected array, got {v:?}");
    }

    #[test]
    fn malformed_falls_back_to_unknown() {
        let raw = base64::engine::general_purpose::STANDARD.encode([0xff, 0xff]);
        assert_eq!(
            b64(&raw),
            serde_json::json!({ "_type": "unknown", "xdr": raw })
        );
    }

    #[test]
    fn unknown_scval_tag_falls_back_to_unknown() {
        // #610: an unknown top-level tag (999) must fall back losslessly to the
        // documented `{"_type":"unknown","xdr":"<base64>"}` shape.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&999u32.to_be_bytes());
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = b64(&raw);

        assert_eq!(
            result,
            serde_json::json!({ "_type": "unknown", "xdr": raw })
        );
    }

    #[test]
    fn unknown_tag_nested_in_vec_falls_back_for_whole_value() {
        // #610: a vec whose second element has an unknown tag (999) followed by
        // a valid element must yield the documented whole-value fallback — the
        // unknown payload must not be misread as the next element.
        let mut bytes = SCV_VEC.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1u32.to_be_bytes()); // present
        bytes.extend_from_slice(&3u32.to_be_bytes()); // len = 3
                                                      // element 0: valid u32
        bytes.extend_from_slice(&SCV_U32.to_be_bytes());
        bytes.extend_from_slice(&7u32.to_be_bytes());
        // element 1: unknown tag with an opaque payload
        bytes.extend_from_slice(&999u32.to_be_bytes());
        bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        // element 2: valid u32 (would be misread as the unknown payload)
        bytes.extend_from_slice(&SCV_U32.to_be_bytes());
        bytes.extend_from_slice(&9u32.to_be_bytes());

        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result,
            serde_json::json!({ "_type": "unknown", "xdr": raw }),
            "unknown nested tag must fall back for the whole value, got {result:?}"
        );
    }

    #[test]
    fn dialect_snapshot() {
        // #611: decode one sample per ScVal variant so the documented dialect
        // table is backed by tested output. Re-accept the snapshot when a
        // mapping changes, and update the module docs in the same commit.
        fn sym(s: &str) -> Vec<u8> {
            let mut v = SCV_SYMBOL.to_be_bytes().to_vec();
            v.extend_from_slice(&(s.len() as u32).to_be_bytes());
            v.extend_from_slice(s.as_bytes());
            let pad = (4 - (s.len() % 4)) % 4;
            v.extend(std::iter::repeat_n(0u8, pad));
            v
        }
        fn u32v(n: u32) -> Vec<u8> {
            let mut v = SCV_U32.to_be_bytes().to_vec();
            v.extend_from_slice(&n.to_be_bytes());
            v
        }
        fn enc(bytes: &[u8]) -> String {
            base64::engine::general_purpose::STANDARD.encode(bytes)
        }

        let mut samples: Vec<(&str, Vec<u8>)> = Vec::new();

        let mut b = SCV_BOOL.to_be_bytes().to_vec();
        b.extend_from_slice(&1u32.to_be_bytes());
        samples.push(("bool", b));
        samples.push(("void", SCV_VOID.to_be_bytes().to_vec()));

        // ScvError: Contract error type 0, code 7.
        let mut b = SCV_ERROR.to_be_bytes().to_vec();
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&7u32.to_be_bytes());
        samples.push(("error_contract", b));

        samples.push(("u32", u32v(42)));
        let mut b = SCV_I32.to_be_bytes().to_vec();
        b.extend_from_slice(&(-42i32).to_be_bytes());
        samples.push(("i32", b));
        let mut b = SCV_U64.to_be_bytes().to_vec();
        b.extend_from_slice(&u64::MAX.to_be_bytes());
        samples.push(("u64", b));
        let mut b = SCV_I64.to_be_bytes().to_vec();
        b.extend_from_slice(&i64::MIN.to_be_bytes());
        samples.push(("i64", b));
        let mut b = SCV_TIMEPOINT.to_be_bytes().to_vec();
        b.extend_from_slice(&123u64.to_be_bytes());
        samples.push(("timepoint", b));
        let mut b = SCV_DURATION.to_be_bytes().to_vec();
        b.extend_from_slice(&456u64.to_be_bytes());
        samples.push(("duration", b));
        let mut b = SCV_U128.to_be_bytes().to_vec();
        b.extend_from_slice(&0u64.to_be_bytes());
        b.extend_from_slice(&300u64.to_be_bytes());
        samples.push(("u128", b));
        let mut b = SCV_I128.to_be_bytes().to_vec();
        b.extend_from_slice(&(-1i64).to_be_bytes());
        b.extend_from_slice(&u64::MAX.to_be_bytes());
        samples.push(("i128", b));
        let mut b = SCV_U256.to_be_bytes().to_vec();
        b.extend_from_slice(&[0xABu8; 32]);
        samples.push(("u256", b));
        let mut b = SCV_I256.to_be_bytes().to_vec();
        b.extend_from_slice(&[0xCDu8; 32]);
        samples.push(("i256", b));

        let mut b = SCV_BYTES.to_be_bytes().to_vec();
        b.extend_from_slice(&2u32.to_be_bytes());
        b.extend_from_slice(&[0xDE, 0xAD]);
        b.extend_from_slice(&[0, 0]);
        samples.push(("bytes", b));

        let mut b = SCV_STRING.to_be_bytes().to_vec();
        b.extend_from_slice(&3u32.to_be_bytes());
        b.extend_from_slice(b"abc");
        b.extend_from_slice(&[0]);
        samples.push(("string", b));
        samples.push(("symbol", sym("fee")));

        let mut b = SCV_VEC.to_be_bytes().to_vec();
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&2u32.to_be_bytes());
        b.extend(u32v(1));
        b.extend(u32v(2));
        samples.push(("vec", b));

        let mut b = SCV_MAP.to_be_bytes().to_vec();
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend(sym("a"));
        b.extend(u32v(1));
        samples.push(("map", b));

        let mut b = SCV_ADDRESS.to_be_bytes().to_vec();
        b.extend_from_slice(&SC_ADDRESS_ACCOUNT.to_be_bytes());
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&[0u8; 32]);
        samples.push(("address_account", b));

        let mut b = SCV_ADDRESS.to_be_bytes().to_vec();
        b.extend_from_slice(&SC_ADDRESS_CONTRACT.to_be_bytes());
        b.extend_from_slice(&[0u8; 32]);
        samples.push(("address_contract", b));

        let mut b = SCV_CONTRACT_INSTANCE.to_be_bytes().to_vec();
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&[0u8; 32]);
        b.extend_from_slice(&0u32.to_be_bytes());
        samples.push(("contract_instance", b));

        samples.push((
            "ledger_key_contract_instance",
            SCV_LEDGER_KEY_CONTRACT_INSTANCE.to_be_bytes().to_vec(),
        ));

        let mut b = SCV_LEDGER_KEY_NONCE.to_be_bytes().to_vec();
        b.extend_from_slice(&42i64.to_be_bytes());
        samples.push(("ledger_key_nonce", b));

        samples.push(("unknown_tag", 999u32.to_be_bytes().to_vec()));

        let mut out = serde_json::Map::new();
        for (name, bytes) in samples {
            out.insert(name.to_string(), decode_scval_base64(&enc(&bytes)));
        }
        // Pinned output of every dialect sample. Changing a decoder is a visible,
        // reviewed change to this literal (previously an insta snapshot).
        let expected: Value = serde_json::from_str(r#"{"address_account":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF","address_contract":"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4","bool":true,"bytes":"0xdead","contract_instance":{"_type":"ContractInstance","wasm_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"duration":"456","error_contract":{"_error":true},"i128":"-1","i256":"-22704331223003175573249212746801550559464702875615796870481879217237868556851","i32":-42,"i64":"-9223372036854775808","ledger_key_contract_instance":{"_type":"LedgerKeyContractInstance"},"ledger_key_nonce":{"_type":"LedgerKeyNonce","nonce":42},"map":{"a":1},"string":"abc","symbol":"fee","timepoint":"123","u128":"300","u256":"77648812782670860460512307594061302913369283834606025297048026922953510464427","u32":42,"u64":"18446744073709551615","unknown_tag":{"_type":"unknown","xdr":"AAAD5w=="},"vec":[1,2],"void":null}"#).expect("snapshot literal");
        assert_eq!(Value::Object(out), expected);
    }

    // ── #410: edge cases ─────────────────────────────────────────────────

    #[test]
    fn bool_strict_zero_is_false() {
        // XDR bool 0 → false.
        let mut bytes = SCV_BOOL.to_be_bytes().to_vec();
        bytes.extend_from_slice(&0u32.to_be_bytes());
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(decode_scval_base64(&raw), Value::Bool(false));
    }

    #[test]
    fn bool_strict_one_is_true() {
        // XDR bool 1 → true.
        let mut bytes = SCV_BOOL.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1u32.to_be_bytes());
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(decode_scval_base64(&raw), Value::Bool(true));
    }

    #[test]
    fn bool_strict_nonzero_non_one_falls_back() {
        // #410: Any non-zero, non-one bool must be rejected (cursor safety).
        for bad in [2u32, 255, u32::MAX] {
            let mut bytes = SCV_BOOL.to_be_bytes().to_vec();
            bytes.extend_from_slice(&bad.to_be_bytes());
            let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let result = decode_scval_base64(&raw);
            assert_eq!(
                result.get("_type").and_then(|v| v.as_str()),
                Some("unknown"),
                "bool value {bad} should fall back, got {result:?}"
            );
        }
    }

    #[test]
    fn trailing_bytes_cause_fallback() {
        // #410: A well-formed ScVal::Void followed by garbage must fall back.
        let mut bytes = SCV_VOID.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // trailing garbage
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_type").and_then(|v| v.as_str()),
            Some("unknown"),
            "trailing bytes should cause fallback, got {result:?}"
        );
    }

    #[test]
    fn trailing_bytes_on_u32_fall_back() {
        // #410: A valid ScVal::U32(42) with 1 extra byte must fall back.
        let mut bytes = SCV_U32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&42u32.to_be_bytes());
        bytes.push(0xFF); // one trailing byte
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_type").and_then(|v| v.as_str()),
            Some("unknown"),
            "trailing byte on u32 should fall back, got {result:?}"
        );
    }

    #[test]
    fn invalid_utf8_symbol_renders_as_hex() {
        // #410: Invalid UTF-8 in a Symbol must not fail the containing value;
        // it must render as a hex string (same as invalid-UTF-8 String).
        let bad_utf8: &[u8] = &[0xFF, 0xFE]; // 2 bytes
        let padded_len = bad_utf8.len();
        let pad = (4 - (padded_len % 4)) % 4;
        let mut bytes = SCV_SYMBOL.to_be_bytes().to_vec();
        bytes.extend_from_slice(&(padded_len as u32).to_be_bytes());
        bytes.extend_from_slice(bad_utf8);
        bytes.extend(std::iter::repeat_n(0u8, pad));
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result,
            Value::String("0xfffe".into()),
            "invalid UTF-8 symbol should render as hex, got {result:?}"
        );
    }

    #[test]
    fn scv_contract_instance_tag_19_is_safe() {
        // #410/#608: ScvContractInstance (tag 19) with a WASM executable and
        // no storage decodes to the typed ContractInstance shape.
        // Wire: [tag=19][exec_kind=0][wasm_hash: 32 zero bytes][storage_present=0]
        let mut bytes = SCV_CONTRACT_INSTANCE.to_be_bytes().to_vec();
        bytes.extend_from_slice(&0u32.to_be_bytes()); // exec_kind = Wasm
        bytes.extend_from_slice(&[0u8; 32]); // wasm hash (32 bytes)
        bytes.extend_from_slice(&0u32.to_be_bytes()); // storage absent
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_type").and_then(|v| v.as_str()),
            Some("ContractInstance"),
            "tag 19 should produce a ContractInstance, got {result:?}"
        );
    }

    #[test]
    fn scv_ledger_key_contract_instance_tag_20_is_safe() {
        // #410/#608: ScvLedgerKeyContractInstance (tag 20) has no payload.
        let bytes = SCV_LEDGER_KEY_CONTRACT_INSTANCE.to_be_bytes().to_vec();
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_type").and_then(|v| v.as_str()),
            Some("LedgerKeyContractInstance"),
            "tag 20 should produce a LedgerKeyContractInstance, got {result:?}"
        );
    }

    #[test]
    fn scv_ledger_key_nonce_tag_21_is_safe() {
        // #410/#608: ScvLedgerKeyNonce (tag 21) payload = ScNonceKey { nonce: i64 }.
        let mut bytes = SCV_LEDGER_KEY_NONCE.to_be_bytes().to_vec();
        bytes.extend_from_slice(&42i64.to_be_bytes()); // nonce value
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_type").and_then(|v| v.as_str()),
            Some("LedgerKeyNonce"),
            "tag 21 should produce a LedgerKeyNonce, got {result:?}"
        );
        assert_eq!(result.get("nonce").and_then(|v| v.as_i64()), Some(42));
    }

    #[test]
    fn scv_error_renders_structured_error() {
        // #608: ScVal::Error(Contract(7)) -> {"error":{"type":"Contract","code":7}}
        let mut bytes = SCV_ERROR.to_be_bytes().to_vec();
        bytes.extend_from_slice(&0u32.to_be_bytes()); // Contract
        bytes.extend_from_slice(&7u32.to_be_bytes()); // code
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result,
            serde_json::json!({ "error": { "type": "Contract", "code": 7 } })
        );
    }

    #[test]
    fn invalid_utf8_string_renders_as_hex() {
        // #608: invalid UTF-8 in a String renders as hex, not lossy U+FFFD.
        let bad_utf8: &[u8] = &[0xFF, 0xFE];
        let mut bytes = SCV_STRING.to_be_bytes().to_vec();
        bytes.extend_from_slice(&(bad_utf8.len() as u32).to_be_bytes());
        bytes.extend_from_slice(bad_utf8);
        bytes.extend(std::iter::repeat_n(0u8, 2)); // pad
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(decode_scval_base64(&raw), Value::String("0xfffe".into()));
    }

    #[test]
    fn u256_and_i256_render_as_decimal() {
        // #409: U256(1) -> "1"
        let mut bytes = SCV_U256.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0u8; 31]);
        bytes.push(1);
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(decode_scval_base64(&raw), Value::String("1".into()));

        // I256(-1) -> "-1" (all bits set)
        let mut bytes = SCV_I256.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0xFFu8; 32]);
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        assert_eq!(decode_scval_base64(&raw), Value::String("-1".into()));
    }

    // ── #411: map key collisions ─────────────────────────────────────────

    #[test]
    fn map_with_duplicate_keys_falls_back_to_pairs() {
        // Build a ScVal::Map with two entries sharing the same Symbol key.
        // Wire format: [tag=17][present=1][len=2]
        //              [tag=15:"a"][tag=3:1]   <- first entry
        //              [tag=15:"a"][tag=3:2]   <- second entry (duplicate key)
        fn scval_symbol_xdr(s: &str) -> Vec<u8> {
            let mut v = SCV_SYMBOL.to_be_bytes().to_vec();
            let len = s.len() as u32;
            v.extend_from_slice(&len.to_be_bytes());
            v.extend_from_slice(s.as_bytes());
            let pad = (4 - (s.len() % 4)) % 4;
            v.extend(std::iter::repeat_n(0u8, pad));
            v
        }
        fn scval_u32_xdr(n: u32) -> Vec<u8> {
            let mut v = SCV_U32.to_be_bytes().to_vec();
            v.extend_from_slice(&n.to_be_bytes());
            v
        }

        let mut bytes = SCV_MAP.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1u32.to_be_bytes()); // present
        bytes.extend_from_slice(&2u32.to_be_bytes()); // len = 2
        bytes.extend(scval_symbol_xdr("a"));
        bytes.extend(scval_u32_xdr(1));
        bytes.extend(scval_symbol_xdr("a")); // duplicate key
        bytes.extend(scval_u32_xdr(2));

        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);

        // Must fall back to [{key,val}] pair array — not a JSON object where
        // one entry would be silently overwritten.
        match &result {
            Value::Array(pairs) => {
                assert_eq!(pairs.len(), 2, "both entries must be present");
                assert!(pairs[0].get("key").is_some(), "pair has key");
                assert!(pairs[0].get("val").is_some(), "pair has val");
            }
            other => panic!("expected pair array for duplicate-key map, got {other:?}"),
        }
    }

    #[test]
    fn map_without_collisions_stays_as_object() {
        // A clean map should still produce a JSON object (no regression).
        fn scval_symbol_xdr(s: &str) -> Vec<u8> {
            let mut v = SCV_SYMBOL.to_be_bytes().to_vec();
            let len = s.len() as u32;
            v.extend_from_slice(&len.to_be_bytes());
            v.extend_from_slice(s.as_bytes());
            let pad = (4 - (s.len() % 4)) % 4;
            v.extend(std::iter::repeat_n(0u8, pad));
            v
        }
        fn scval_u32_xdr(n: u32) -> Vec<u8> {
            let mut v = SCV_U32.to_be_bytes().to_vec();
            v.extend_from_slice(&n.to_be_bytes());
            v
        }

        let mut bytes = SCV_MAP.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1u32.to_be_bytes()); // present
        bytes.extend_from_slice(&2u32.to_be_bytes()); // len = 2
        bytes.extend(scval_symbol_xdr("a"));
        bytes.extend(scval_u32_xdr(1));
        bytes.extend(scval_symbol_xdr("b"));
        bytes.extend(scval_u32_xdr(2));

        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);

        match &result {
            Value::Object(obj) => {
                assert_eq!(obj.len(), 2);
                assert_eq!(obj["a"], serde_json::json!(1));
                assert_eq!(obj["b"], serde_json::json!(2));
            }
            other => panic!("expected object for clean map, got {other:?}"),
        }
    }

    // ── #413: strkey via stellar-strkey ──────────────────────────────────

    #[test]
    fn valid_contract_id_accepted() {
        let id = make_contract_strkey(&[0u8; 32]);
        assert!(
            is_valid_contract_id(&id),
            "stellar-strkey-encoded C-address should be valid: {id}"
        );
    }

    #[test]
    fn invalid_contract_ids_rejected() {
        let valid = make_contract_strkey(&[0u8; 32]);

        // Wrong length.
        assert!(!is_valid_contract_id(&valid[..55]), "too short");
        assert!(!is_valid_contract_id(&format!("{valid}A")), "too long");

        // G-strkey (account) is not a contract ID.
        let g_key = make_account_strkey(&[0u8; 32]);
        assert!(!is_valid_contract_id(&g_key), "account strkey rejected");

        // Invalid base32 character.
        let mut bad = valid.clone();
        bad.replace_range(10..11, "0"); // '0' is not in the base32 alphabet
        assert!(!is_valid_contract_id(&bad), "invalid char rejected");

        // Corrupt the payload to invalidate the CRC.
        let mut corrupted = valid.into_bytes();
        corrupted[5] = if corrupted[5] == b'A' { b'B' } else { b'A' };
        let corrupted = String::from_utf8(corrupted).unwrap();
        assert!(!is_valid_contract_id(&corrupted), "bad CRC rejected");
    }

    #[test]
    fn contract_id_strkey_truncated_input() {
        let valid = make_contract_strkey(&[0u8; 32]);
        assert!(
            !is_valid_contract_id(&valid[..10]),
            "truncated strkey rejected"
        );
        assert!(
            !is_valid_contract_id(&valid[..30]),
            "truncated strkey rejected"
        );
        assert!(
            !is_valid_contract_id(&valid[..55]),
            "truncated strkey rejected"
        );
        assert!(!is_valid_contract_id(""), "empty strkey rejected");
    }

    #[test]
    fn contract_id_strkey_overlength_input() {
        let valid = make_contract_strkey(&[0u8; 32]);
        assert!(
            !is_valid_contract_id(&format!("{valid}A")),
            "overlength rejected"
        );
        assert!(
            !is_valid_contract_id(&format!("{valid}AAAA")),
            "overlength rejected"
        );
    }

    #[test]
    fn contract_id_strkey_wrong_version_byte() {
        let g_key = make_account_strkey(&[0u8; 32]);
        assert!(
            !is_valid_contract_id(&g_key),
            "G-strkey rejected as contract ID"
        );
        assert_eq!(g_key.chars().next().unwrap(), 'G', "G-strkey starts with G");

        let c_key = make_contract_strkey(&[0u8; 32]);
        assert!(is_valid_contract_id(&c_key), "C-strkey accepted");
        assert_eq!(c_key.chars().next().unwrap(), 'C', "C-strkey starts with C");
    }

    #[test]
    fn contract_id_strkey_roundtrip_g_and_c() {
        let g_key = make_account_strkey(&[1u8; 32]);
        assert_eq!(g_key.len(), 56, "G-strkey is 56 chars");
        assert!(g_key.starts_with('G'), "G-strkey starts with G");

        let c_key = make_contract_strkey(&[2u8; 32]);
        assert_eq!(c_key.len(), 56, "C-strkey is 56 chars");
        assert!(c_key.starts_with('C'), "C-strkey starts with C");
        assert!(is_valid_contract_id(&c_key), "C-strkey validates");
    }

    #[test]
    fn contract_id_strkey_invalid_base32_chars() {
        let valid = make_contract_strkey(&[0u8; 32]);

        let mut invalid = valid.clone();
        invalid.replace_range(10..11, "0");
        assert!(!is_valid_contract_id(&invalid), "invalid char '0'");

        let mut invalid2 = valid.clone();
        invalid2.replace_range(15..16, "1");
        assert!(!is_valid_contract_id(&invalid2), "invalid char '1'");

        let mut invalid3 = valid.clone();
        invalid3.replace_range(20..21, "8");
        assert!(!is_valid_contract_id(&invalid3), "invalid char '8'");

        let mut invalid4 = valid;
        invalid4.replace_range(25..26, "!");
        assert!(!is_valid_contract_id(&invalid4), "invalid char '!'");
    }

    // ── parse_contract_ids ────────────────────────────────────────────────

    #[test]
    fn parse_contract_ids_empty_string_is_ok() {
        assert_eq!(parse_contract_ids("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn parse_contract_ids_whitespace_only_is_ok() {
        assert_eq!(parse_contract_ids("  ,  , ").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn parse_contract_ids_single_valid_id() {
        let id = make_contract_strkey(&[0u8; 32]);
        assert_eq!(parse_contract_ids(&id).unwrap(), vec![id]);
    }

    #[test]
    fn parse_contract_ids_multiple_valid_ids() {
        let id1 = make_contract_strkey(&[0u8; 32]);
        let id2 = make_contract_strkey(&[1u8; 32]);
        let raw = format!("{id1},{id2}");
        assert_eq!(parse_contract_ids(&raw).unwrap(), vec![id1, id2]);
    }

    #[test]
    fn parse_contract_ids_trims_whitespace_around_entries() {
        let id = make_contract_strkey(&[0u8; 32]);
        let raw = format!("  {id}  ");
        assert_eq!(parse_contract_ids(&raw).unwrap(), vec![id]);
    }

    #[test]
    fn parse_contract_ids_rejects_invalid_id() {
        let err = parse_contract_ids("NOT_A_VALID_ID").unwrap_err();
        assert!(
            err.contains("NOT_A_VALID_ID"),
            "error mentions bad id: {err}"
        );
    }

    #[test]
    fn parse_contract_ids_rejects_g_strkey() {
        let g_key = make_account_strkey(&[0u8; 32]);
        let err = parse_contract_ids(&g_key).unwrap_err();
        assert!(
            err.contains("C\u{2026} strkey"),
            "error mentions expected format: {err}"
        );
    }

    #[test]
    fn parse_contract_ids_rejects_too_many_ids() {
        let ids: Vec<String> = (0u8..26).map(|i| make_contract_strkey(&[i; 32])).collect();
        let raw = ids.join(",");
        let err = parse_contract_ids(&raw).unwrap_err();
        assert!(err.contains("26"), "error mentions count: {err}");
        assert!(err.contains("25"), "error mentions limit: {err}");

        let raw25 = ids[..25].join(",");
        assert_eq!(parse_contract_ids(&raw25).unwrap().len(), 25);
    }

    #[test]
    fn parse_contract_ids_deduplicates_preserving_order() {
        // #612: 26 entries with one duplicate → 25 unique IDs, in first-seen
        // order, and no "too many" error.
        let ids: Vec<String> = (0u8..25).map(|i| make_contract_strkey(&[i; 32])).collect();
        // Append a duplicate of the first ID to reach 26 entries.
        let mut entries = ids.clone();
        entries.push(ids[0].clone());
        assert_eq!(entries.len(), 26);

        let raw = entries.join(",");
        let parsed = parse_contract_ids(&raw).expect("26 entries with one duplicate should pass");
        assert_eq!(parsed.len(), 25, "duplicates must be removed");
        assert_eq!(parsed, ids, "order must be preserved");
    }
}

// ---- Property / fuzz tests -----------------------------------------------
//
// Acceptance criteria for #412 and #26:
//   • The decoder never panics on arbitrary bytes — it returns an error
//     fallback ({ "_type": "unknown", "xdr": "<base64>" }) instead.
//   • Round-trip properties hold for every primitive ScVal kind, including
//     i128/u128, addresses, and nested vec/map structures.

#[cfg(test)]
mod prop_tests {
    use super::wire_tags::*;
    use super::*;
    use base64::Engine;
    use proptest::prelude::*;

    // ---- helpers to build minimal valid XDR bytes for each ScVal kind ----

    fn scval_bool(b: bool) -> Vec<u8> {
        let mut v = SCV_BOOL.to_be_bytes().to_vec();
        v.extend_from_slice(&(b as u32).to_be_bytes());
        v
    }

    fn scval_u32(n: u32) -> Vec<u8> {
        let mut v = SCV_U32.to_be_bytes().to_vec();
        v.extend_from_slice(&n.to_be_bytes());
        v
    }

    fn scval_i32(n: i32) -> Vec<u8> {
        let mut v = SCV_I32.to_be_bytes().to_vec();
        v.extend_from_slice(&n.to_be_bytes());
        v
    }

    fn scval_u64(n: u64) -> Vec<u8> {
        let mut v = SCV_U64.to_be_bytes().to_vec();
        v.extend_from_slice(&n.to_be_bytes());
        v
    }

    fn scval_i64(n: i64) -> Vec<u8> {
        let mut v = SCV_I64.to_be_bytes().to_vec();
        v.extend_from_slice(&n.to_be_bytes());
        v
    }

    // #412: helpers for 128-bit types.
    fn scval_u128(n: u128) -> Vec<u8> {
        let hi = (n >> 64) as u64;
        let lo = n as u64;
        let mut v = SCV_U128.to_be_bytes().to_vec();
        v.extend_from_slice(&hi.to_be_bytes());
        v.extend_from_slice(&lo.to_be_bytes());
        v
    }

    fn scval_i128(n: i128) -> Vec<u8> {
        let hi = (n >> 64) as i64;
        let lo = n as u64;
        let mut v = SCV_I128.to_be_bytes().to_vec();
        v.extend_from_slice(&hi.to_be_bytes());
        v.extend_from_slice(&lo.to_be_bytes());
        v
    }

    // #412: helpers for address types (account = G-key, contract = C-key).
    fn scval_account_address(raw32: &[u8; 32]) -> Vec<u8> {
        let mut v = SCV_ADDRESS.to_be_bytes().to_vec();
        v.extend_from_slice(&SC_ADDRESS_ACCOUNT.to_be_bytes()); // discriminant
        v.extend_from_slice(&0u32.to_be_bytes()); // key_type = ed25519
        v.extend_from_slice(raw32);
        v
    }

    fn scval_contract_address(raw32: &[u8; 32]) -> Vec<u8> {
        let mut v = SCV_ADDRESS.to_be_bytes().to_vec();
        v.extend_from_slice(&SC_ADDRESS_CONTRACT.to_be_bytes()); // discriminant
        v.extend_from_slice(raw32);
        v
    }

    // #412: helpers for bytes/string.
    fn scval_bytes(data: &[u8]) -> Vec<u8> {
        let mut v = SCV_BYTES.to_be_bytes().to_vec();
        let len = data.len() as u32;
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(data);
        let pad = (4 - (data.len() % 4)) % 4;
        v.extend(std::iter::repeat_n(0u8, pad));
        v
    }

    fn scval_symbol(s: &str) -> Vec<u8> {
        let bytes = s.as_bytes();
        let mut v = SCV_SYMBOL.to_be_bytes().to_vec();
        let len = bytes.len() as u32;
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(bytes);
        let pad = (4 - (bytes.len() % 4)) % 4;
        v.extend(std::iter::repeat_n(0u8, pad));
        v
    }

    // #412: helper for a single-element vec containing a u32.
    fn scval_vec_of_u32s(items: &[u32]) -> Vec<u8> {
        let mut v = SCV_VEC.to_be_bytes().to_vec();
        v.extend_from_slice(&1u32.to_be_bytes()); // present
        v.extend_from_slice(&(items.len() as u32).to_be_bytes());
        for &item in items {
            v.extend(scval_u32(item));
        }
        v
    }

    // #412: helper for a simple symbol-keyed map with u32 values.
    fn scval_symbol_map(entries: &[(&str, u32)]) -> Vec<u8> {
        let mut v = SCV_MAP.to_be_bytes().to_vec();
        v.extend_from_slice(&1u32.to_be_bytes()); // present
        v.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (k, val) in entries {
            v.extend(scval_symbol(k));
            v.extend(scval_u32(*val));
        }
        v
    }

    fn encode(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    proptest! {
        /// Arbitrary bytes (via base64) must never panic — only return a value
        /// or the `{ "_xdr": … }` fallback.
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(bytes: Vec<u8>) {
            let b64 = encode(&bytes);
            // Must not panic.
            let _ = decode_scval_base64(&b64);
        }

        /// decode_topics is also a public entry point; verify it stays panic-free.
        #[test]
        fn decode_topics_never_panics(topics: Vec<Vec<u8>>) {
            let b64_topics: Vec<String> = topics.iter().map(|b| encode(b)).collect();
            let _ = decode_topics(&b64_topics);
        }

        /// bool round-trip: encode as ScVal::Bool, decode, compare.
        #[test]
        fn bool_roundtrip(b: bool) {
            let result = decode_scval_base64(&encode(&scval_bool(b)));
            prop_assert_eq!(result, serde_json::Value::Bool(b));
        }

        /// u32 round-trip.
        #[test]
        fn u32_roundtrip(n: u32) {
            let result = decode_scval_base64(&encode(&scval_u32(n)));
            prop_assert_eq!(result, serde_json::json!(n));
        }

        /// i32 round-trip.
        #[test]
        fn i32_roundtrip(n: i32) {
            let result = decode_scval_base64(&encode(&scval_i32(n)));
            prop_assert_eq!(result, serde_json::json!(n));
        }

        /// u64: decoded as decimal string (JS-safe).
        #[test]
        fn u64_roundtrip(n: u64) {
            let result = decode_scval_base64(&encode(&scval_u64(n)));
            prop_assert_eq!(result, serde_json::Value::String(n.to_string()));
        }

        /// i64: decoded as decimal string.
        #[test]
        fn i64_roundtrip(n: i64) {
            let result = decode_scval_base64(&encode(&scval_i64(n)));
            prop_assert_eq!(result, serde_json::Value::String(n.to_string()));
        }

        // #412: 128-bit round-trips -----------------------------------------

        /// u128: decoded as decimal string.
        #[test]
        fn u128_roundtrip(n: u128) {
            let result = decode_scval_base64(&encode(&scval_u128(n)));
            prop_assert_eq!(result, serde_json::Value::String(n.to_string()));
        }

        /// i128: decoded as decimal string, including full negative range.
        #[test]
        fn i128_roundtrip(n: i128) {
            let result = decode_scval_base64(&encode(&scval_i128(n)));
            prop_assert_eq!(result, serde_json::Value::String(n.to_string()));
        }

        // #412: address round-trips ------------------------------------------

        /// Account address: decoded as a G-strkey.
        #[test]
        fn account_address_roundtrip(raw: [u8; 32]) {
            let result = decode_scval_base64(&encode(&scval_account_address(&raw)));
            let expected = make_account_strkey(&raw);
            prop_assert_eq!(result, serde_json::Value::String(expected));
        }

        /// Contract address: decoded as a C-strkey.
        #[test]
        fn contract_address_roundtrip(raw: [u8; 32]) {
            let result = decode_scval_base64(&encode(&scval_contract_address(&raw)));
            let expected = make_contract_strkey(&raw);
            prop_assert_eq!(result, serde_json::Value::String(expected));
        }

        /// Contract address decodes to a valid C-strkey that passes is_valid_contract_id.
        #[test]
        fn contract_address_is_always_valid_strkey(raw: [u8; 32]) {
            let result = decode_scval_base64(&encode(&scval_contract_address(&raw)));
            if let serde_json::Value::String(s) = result {
                prop_assert!(is_valid_contract_id(&s),
                    "decoded contract strkey must be valid: {s}");
            } else {
                prop_assert!(false, "contract address must decode to a string");
            }
        }

        // #412: bytes/string round-trips -------------------------------------

        /// Bytes: decoded as `"0x<hex>"` string.
        #[test]
        fn bytes_roundtrip(data: Vec<u8>) {
            let result = decode_scval_base64(&encode(&scval_bytes(&data)));
            let expected = format!("0x{}", hex(&data));
            prop_assert_eq!(result, serde_json::Value::String(expected));
        }

        /// Symbol (valid UTF-8): decoded as plain string.
        #[test]
        fn symbol_roundtrip(s in "[a-zA-Z0-9_]{0,32}") {
            let result = decode_scval_base64(&encode(&scval_symbol(&s)));
            prop_assert_eq!(result, serde_json::Value::String(s));
        }

        // #412: vec/map nesting ---------------------------------------------

        /// Vec of u32s: decoded as JSON array with correct length and values.
        #[test]
        fn vec_of_u32s_roundtrip(items in proptest::collection::vec(any::<u32>(), 0..=16usize)) {
            let result = decode_scval_base64(&encode(&scval_vec_of_u32s(&items)));
            match result {
                serde_json::Value::Array(arr) => {
                    prop_assert_eq!(arr.len(), items.len());
                    for (i, item) in items.iter().enumerate() {
                        prop_assert_eq!(&arr[i], &serde_json::json!(item));
                    }
                }
                other => prop_assert!(false, "expected array, got {other:?}"),
            }
        }

        /// Map (unique string keys): decoded as JSON object with correct entries.
        #[test]
        fn map_unique_keys_roundtrip(
            // Generate up to 8 unique 1-4 char lowercase keys.
            entries in proptest::collection::btree_map(
                "[a-z]{1,4}",
                any::<u32>(),
                0..=8usize,
            )
        ) {
            let pairs: Vec<(&str, u32)> = entries.iter().map(|(k, v)| (k.as_str(), *v)).collect();
            let result = decode_scval_base64(&encode(&scval_symbol_map(&pairs)));
            match result {
                serde_json::Value::Object(obj) => {
                    prop_assert_eq!(obj.len(), entries.len());
                    for (k, expected_v) in &entries {
                        let actual = obj.get(k.as_str());
                        prop_assert!(actual.is_some(), "key {k:?} should be present");
                        prop_assert_eq!(actual.unwrap(), &serde_json::json!(expected_v));
                    }
                }
                serde_json::Value::Array(_) => {
                    // Pair-array fallback is also acceptable (e.g. empty map).
                }
                other => prop_assert!(false, "expected object or array, got {other:?}"),
            }
        }

        // #608: the byte path and the typed path must agree for every ScVal.
        /// For any typed ScVal, `decode_scval_base64(b64(v)) == decode_scval(v)`.
        #[test]
        fn byte_and_typed_decoders_agree(sv in arb_scval(0)) {
            use stellar_xdr::curr::{Limits, WriteXdr};
            let bytes = sv.to_xdr(Limits::none()).expect("ScVal should encode");
            let b64 = encode(&bytes);
            let via_bytes = decode_scval_base64(&b64);
            let via_typed = decode_scval(&sv);
            prop_assert_eq!(via_bytes, via_typed);
        }
    }

    // ---- #608: arbitrary ScVal generator ---------------------------------

    use stellar_xdr::curr::{
        Int128Parts, Int256Parts, ScAddress, ScBytes, ScMap, ScMapEntry, ScString, ScSymbol, ScVal,
        ScVec, UInt128Parts, UInt256Parts,
    };

    fn arb_scval(depth: u32) -> BoxedStrategy<ScVal> {
        let leaf = prop_oneof![
            Just(ScVal::Void),
            any::<bool>().prop_map(ScVal::Bool),
            any::<u32>().prop_map(ScVal::U32),
            any::<i32>().prop_map(ScVal::I32),
            any::<u64>().prop_map(ScVal::U64),
            any::<i64>().prop_map(ScVal::I64),
            any::<u64>().prop_map(|n| ScVal::Timepoint(stellar_xdr::curr::TimePoint(n))),
            any::<u64>().prop_map(|n| ScVal::Duration(stellar_xdr::curr::Duration(n))),
            (any::<u64>(), any::<u64>()).prop_map(|(hi, lo)| ScVal::U128(UInt128Parts { hi, lo })),
            (any::<i64>(), any::<u64>()).prop_map(|(hi, lo)| ScVal::I128(Int128Parts { hi, lo })),
            (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(hi_hi, hi_lo, lo_hi, lo_lo)| {
                    ScVal::U256(UInt256Parts {
                        hi_hi,
                        hi_lo,
                        lo_hi,
                        lo_lo,
                    })
                }
            ),
            (any::<i64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(hi_hi, hi_lo, lo_hi, lo_lo)| {
                    ScVal::I256(Int256Parts {
                        hi_hi,
                        hi_lo,
                        lo_hi,
                        lo_lo,
                    })
                }
            ),
            proptest::collection::vec(any::<u8>(), 0..=16)
                .prop_map(|b| ScVal::Bytes(ScBytes(b.try_into().unwrap()))),
            "[a-zA-Z0-9_]{0,16}".prop_map(|s| ScVal::Symbol(ScSymbol(s.try_into().unwrap()))),
            "[a-zA-Z0-9_]{0,16}".prop_map(|s| ScVal::String(ScString(s.try_into().unwrap()))),
            any::<[u8; 32]>().prop_map(|raw| {
                ScVal::Address(ScAddress::Contract(stellar_xdr::curr::ContractId(
                    stellar_xdr::curr::Hash(raw),
                )))
            }),
            any::<[u8; 32]>().prop_map(|raw| {
                ScVal::Address(ScAddress::Account(stellar_xdr::curr::AccountId(
                    stellar_xdr::curr::PublicKey::PublicKeyTypeEd25519(stellar_xdr::curr::Uint256(
                        raw,
                    )),
                )))
            }),
            Just(ScVal::LedgerKeyContractInstance),
            any::<i64>().prop_map(|nonce| {
                ScVal::LedgerKeyNonce(stellar_xdr::curr::ScNonceKey { nonce })
            }),
        ];

        if depth >= 3 {
            leaf.boxed()
        } else {
            let nested = prop_oneof![
                proptest::collection::vec(arb_scval(depth + 1), 0..=4)
                    .prop_map(|v| ScVal::Vec(Some(ScVec(v.try_into().unwrap())))),
                proptest::collection::vec((arb_scval(depth + 1), arb_scval(depth + 1)), 0..=4,)
                    .prop_map(|entries| {
                        let entries: Vec<ScMapEntry> = entries
                            .into_iter()
                            .map(|(key, val)| ScMapEntry { key, val })
                            .collect();
                        ScVal::Map(Some(ScMap(entries.try_into().unwrap())))
                    }),
            ];
            prop_oneof![leaf, nested].boxed()
        }
    }
}
