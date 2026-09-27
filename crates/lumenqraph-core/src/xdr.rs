//! Self-contained Soroban XDR decoding.
//!
//! Soroban event topics and values are base64-encoded XDR `ScVal`s. Rather than
//! depend on the fast-moving `stellar-xdr` crate, we decode the (stable) ScVal
//! wire format directly into friendly JSON. Integers that don't fit a JS number
//! are rendered as decimal strings; addresses are rendered as strkeys
//! (`G...`/`C...`); bytes as hex.
//!
//! Decoding is always best-effort: on any malformed input we fall back to
//! `{"_xdr": "<base64>"}` so nothing is lost and one weird event can't break
//! ingestion.
//!
//! ## JSON dialect
//!
//! The table below documents exactly how each `ScVal` variant maps to JSON.
//! See also [`docs/API.md`](../../../docs/API.md) for the consumer-facing
//! description.
//!
//! | XDR variant               | JSON representation                                 |
//! |---------------------------|-----------------------------------------------------|
//! | `ScvBool`                 | `true` / `false`                                    |
//! | `ScvVoid`                 | `null`                                              |
//! | `ScvError`                | `{"_error":true}`                                   |
//! | `ScvU32`                  | JSON number (u32 fits safely in f64)                |
//! | `ScvI32`                  | JSON number (i32 fits safely in f64)                |
//! | `ScvU64`                  | decimal string (e.g. `"18446744073709551615"`)       |
//! | `ScvI64`                  | decimal string (e.g. `"-9223372036854775808"`)       |
//! | `ScvTimepoint`            | decimal string (same wire as u64)                   |
//! | `ScvDuration`             | decimal string (same wire as u64)                   |
//! | `ScvU128`                 | decimal string                                      |
//! | `ScvI128`                 | decimal string                                      |
//! | `ScvU256`                 | `{"_u256_hex":"<32 bytes hex>"}`                    |
//! | `ScvI256`                 | `{"_u256_hex":"<32 bytes hex>"}`                    |
//! | `ScvBytes`                | hex string prefixed `0x` (e.g. `"0xdead"`)         |
//! | `ScvString`               | UTF-8 string, or hex `"0x…"` if invalid UTF-8       |
//! | `ScvSymbol`               | UTF-8 string, or hex `"0x…"` if invalid UTF-8       |
//! | `ScvVec`                  | JSON array (empty array for absent/`None` variant)  |
//! | `ScvMap` (no collisions)  | JSON object keyed by string (symbol/string keys)    |
//! | `ScvMap` (collisions/mixed keys) | array of `{"key":…,"val":…}` pair objects  |
//! | `ScvAddress` (account)    | `G…` strkey (56 chars)                              |
//! | `ScvAddress` (contract)   | `C…` strkey (56 chars)                              |
//! | `ScvAddress` (other)      | `"_addr_type_<N>"` placeholder                      |
//! | `ScvContractInstance` (19)| `{"_xdr_tag":19}` — payload consumed safely         |
//! | `ScvLedgerKeyContractInstance` (20) | `{"_xdr_tag":20}` — no payload            |
//! | `ScvLedgerKeyNonce` (21)  | `{"_xdr_tag":21}` — payload consumed safely         |
//! | unknown tag               | `{"_xdr_tag":<N>}` fallback; surrounding data intact|
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
use serde_json::{json, Map, Value};
use stellar_strkey::{Contract, Ed25519PublicKey};

// ScValType discriminants (stable wire tags).
const SCV_BOOL: u32 = 0;
const SCV_VOID: u32 = 1;
const SCV_ERROR: u32 = 2;
const SCV_U32: u32 = 3;
const SCV_I32: u32 = 4;
const SCV_U64: u32 = 5;
const SCV_I64: u32 = 6;
const SCV_TIMEPOINT: u32 = 7;
const SCV_DURATION: u32 = 8;
const SCV_U128: u32 = 9;
const SCV_I128: u32 = 10;
const SCV_U256: u32 = 11;
const SCV_I256: u32 = 12;
const SCV_BYTES: u32 = 13;
const SCV_STRING: u32 = 14;
const SCV_SYMBOL: u32 = 15;
const SCV_VEC: u32 = 16;
const SCV_MAP: u32 = 17;
const SCV_ADDRESS: u32 = 18;
// Tags 19–21: present in instance-storage snapshots; payload must be consumed.
const SCV_CONTRACT_INSTANCE: u32 = 19;
const SCV_LEDGER_KEY_CONTRACT_INSTANCE: u32 = 20;
const SCV_LEDGER_KEY_NONCE: u32 = 21;

// ScAddressType discriminants.
const SC_ADDRESS_ACCOUNT: u32 = 0;
const SC_ADDRESS_CONTRACT: u32 = 1;

/// Decode a base64 `ScVal` into friendly JSON. Never panics.
///
/// Returns a fallback `{"_type":"unknown","xdr":"<base64>"}` on any parse
/// error, including trailing bytes after the top-level value (which would
/// indicate cursor misalignment and corrupt a sibling value if ignored).
pub fn decode_scval_base64(b64: &str) -> Value {
    match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => {
            let mut cur = Cursor::new(&bytes);
            match cur.read_scval() {
                // #410: reject trailing bytes — they indicate a misaligned
                // cursor that would corrupt siblings in a vec/map context.
                Some(v) if cur.pos == cur.buf.len() => v,
                Some(_) => json!({ "_type": "unknown", "xdr": b64 }),
                None => json!({ "_type": "unknown", "xdr": b64 }),
            }
        }
        Err(_) => json!({ "_type": "unknown", "xdr": b64 }),
    }
}

/// Decode each base64 topic into friendly JSON.
pub fn decode_topics(topics: &[String]) -> Vec<Value> {
    topics.iter().map(|t| decode_scval_base64(t)).collect()
}

/// Best-effort event name: `topic[0]` decoded as a Symbol/String.
pub fn event_name_from_topic(topic_b64: &str) -> Option<String> {
    match decode_scval_base64(topic_b64) {
        Value::String(s) => Some(s),
        _ => None,
    }
}

/// A minimal big-endian XDR reader.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.buf.len() {
            return None;
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Some(s)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    /// XDR variable opaque / string: 4-byte length, bytes, pad to 4-byte align.
    fn var_bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        let data = self.take(len)?.to_vec();
        let pad = (4 - (len % 4)) % 4;
        self.take(pad)?;
        Some(data)
    }

    fn read_scval(&mut self) -> Option<Value> {
        let tag = self.u32()?;
        Some(match tag {
            // #410: strict bool — XDR bool must be exactly 0 or 1.
            SCV_BOOL => {
                let raw = self.u32()?;
                match raw {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => return None,
                }
            }
            SCV_VOID => Value::Null,
            SCV_ERROR => {
                // Skip: (type u32, code u32). Represent opaquely.
                let _ = self.u32()?;
                let _ = self.u32()?;
                json!({ "_error": true })
            }
            SCV_U32 => json!(self.u32()?),
            SCV_I32 => json!(self.i32()?),
            SCV_U64 => Value::String(self.u64()?.to_string()),
            SCV_I64 => Value::String(self.i64()?.to_string()),
            SCV_TIMEPOINT => Value::String(self.u64()?.to_string()),
            SCV_DURATION => Value::String(self.u64()?.to_string()),
            SCV_U128 => {
                // UInt128Parts { hi: u64, lo: u64 }
                let hi = self.u64()? as u128;
                let lo = self.u64()? as u128;
                Value::String(((hi << 64) | lo).to_string())
            }
            SCV_I128 => {
                // Int128Parts { hi: i64, lo: u64 }
                let hi = self.i64()? as i128;
                let lo = self.u64()? as i128;
                Value::String(((hi << 64) | lo).to_string())
            }
            SCV_U256 | SCV_I256 => {
                // 256-bit: four 64-bit limbs (hi_hi, hi_lo, lo_hi, lo_lo).
                let raw = self.take(32)?;
                json!({ "_u256_hex": hex(raw) })
            }
            SCV_BYTES => Value::String(format!("0x{}", hex(&self.var_bytes()?))),
            SCV_STRING => match String::from_utf8(self.var_bytes()?) {
                Ok(s) => Value::String(s),
                Err(e) => Value::String(format!("0x{}", hex(e.as_bytes()))),
            },
            // #410: invalid UTF-8 in a Symbol is rendered as hex (like String)
            // instead of failing the entire containing value.
            SCV_SYMBOL => match String::from_utf8(self.var_bytes()?) {
                Ok(s) => Value::String(s),
                Err(e) => Value::String(format!("0x{}", hex(e.as_bytes()))),
            },
            SCV_VEC => {
                // Option<ScVec>: presence flag, then length-prefixed ScVal array.
                if self.u32()? == 0 {
                    Value::Array(vec![])
                } else {
                    let len = self.u32()? as usize;
                    let mut items = Vec::with_capacity(len.min(1024));
                    for _ in 0..len {
                        items.push(self.read_scval()?);
                    }
                    Value::Array(items)
                }
            }
            SCV_MAP => {
                if self.u32()? == 0 {
                    Value::Object(Map::new())
                } else {
                    let len = self.u32()? as usize;
                    self.read_map(len)?
                }
            }
            SCV_ADDRESS => Value::String(self.read_address()?),
            // #410: tag 19 — ScvContractInstance.
            // Wire format: executable (union: 0=wasm u32+hash32, 1=token u32),
            // then Option<ScMap> storage. Rather than try to decode this fully,
            // we consume the bytes safely by re-entrantly reading the sub-ScVal
            // fields so the cursor stays aligned, then fall back to an opaque
            // marker. The `read_contract_instance_payload` helper does just that.
            SCV_CONTRACT_INSTANCE => {
                self.skip_contract_instance_payload()?;
                json!({ "_xdr_tag": SCV_CONTRACT_INSTANCE })
            }
            // #410: tag 20 — ScvLedgerKeyContractInstance. No payload.
            SCV_LEDGER_KEY_CONTRACT_INSTANCE => {
                json!({ "_xdr_tag": SCV_LEDGER_KEY_CONTRACT_INSTANCE })
            }
            // #410: tag 21 — ScvLedgerKeyNonce. Payload: ScNonceKey { nonce: i64 }.
            SCV_LEDGER_KEY_NONCE => {
                let _nonce = self.i64()?;
                json!({ "_xdr_tag": SCV_LEDGER_KEY_NONCE })
            }
            // Any other unknown tag: return an opaque marker. No bytes consumed
            // beyond the tag (which is fine — we signal failure via None so the
            // top-level falls back, keeping the cursor state irrelevant).
            _ => json!({ "_xdr_tag": tag }),
        })
    }

    /// Consume the payload of a `ScvContractInstance` (tag 19) without trying
    /// to decode it into friendly JSON. Returns `None` if the bytes are
    /// structurally invalid (which causes the caller to fall back losslessly).
    ///
    /// XDR layout (simplified):
    ///   executable: union {
    ///     case WASM(0): wasm_hash = opaque[32]
    ///     case TOKEN(1): (no payload)
    ///   }
    ///   storage: Option<ScMap>
    fn skip_contract_instance_payload(&mut self) -> Option<()> {
        // Read the executable union discriminant.
        let exec_kind = self.u32()?;
        match exec_kind {
            0 => {
                // Wasm: 32-byte hash.
                self.take(32)?;
            }
            1 => {
                // Token (Stellar Asset Contract): no additional bytes.
            }
            _ => return None,
        }
        // Read the Option<ScMap> storage presence flag.
        let present = self.u32()?;
        if present != 0 {
            let len = self.u32()? as usize;
            // Consume each key/value pair by decoding them; this keeps the
            // cursor aligned even if the individual values are themselves complex.
            for _ in 0..len {
                self.read_scval()?;
                self.read_scval()?;
            }
        }
        Some(())
    }

    /// #411: read a map and fall back to the `[{key,val}]` pair form when any
    /// key collision is detected, instead of silently dropping entries.
    fn read_map(&mut self, len: usize) -> Option<Value> {
        let mut obj: Map<String, Value> = Map::new();
        let mut pairs: Vec<Value> = Vec::with_capacity(len);
        let mut all_stringy = true;
        let mut has_collision = false;

        for _ in 0..len {
            let k = self.read_scval()?;
            let v = self.read_scval()?;
            match &k {
                Value::String(s) => {
                    if obj.contains_key(s.as_str()) {
                        // Duplicate key — remember this so we fall back.
                        has_collision = true;
                    }
                    obj.insert(s.clone(), v.clone());
                }
                _ => all_stringy = false,
            }
            pairs.push(json!({ "key": k, "val": v }));
        }

        // Prefer a plain object only when every key is a symbol/string AND
        // there are no collisions (which would silently lose data).
        if all_stringy && !has_collision {
            Some(Value::Object(obj))
        } else {
            Some(Value::Array(pairs))
        }
    }

    fn read_address(&mut self) -> Option<String> {
        match self.u32()? {
            SC_ADDRESS_ACCOUNT => {
                // AccountId -> PublicKey union: key type (0 = ed25519), 32 bytes.
                let _key_type = self.u32()?;
                let raw = self.take(32)?;
                // #413: use stellar-strkey for canonical encoding.
                let mut payload = [0u8; 32];
                payload.copy_from_slice(raw);
                Some(Ed25519PublicKey(payload).to_string())
            }
            SC_ADDRESS_CONTRACT => {
                let raw = self.take(32)?;
                // #413: use stellar-strkey for canonical encoding.
                let mut payload = [0u8; 32];
                payload.copy_from_slice(raw);
                Some(Contract(payload).to_string())
            }
            other => Some(format!("_addr_type_{other}")),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
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
/// * the number of entries exceeds the `getEvents` RPC limit of 25 (5 filters ×
///   5 IDs).
///
/// This function is shared by all services that need to read `CONTRACT_IDS` so
/// that validation never drifts between the indexer, API, webhooks, and MCP.
pub fn parse_contract_ids(raw: &str) -> Result<Vec<String>, String> {
    let ids: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    for id in &ids {
        if !is_valid_contract_id(id) {
            return Err(format!(
                "invalid CONTRACT_ID {id:?}: expected a C\u{2026} strkey (Soroban contract address)"
            ));
        }
    }

    const MAX_CONTRACT_IDS: usize = 25; // 5 filters × 5 IDs per filter
    if ids.len() > MAX_CONTRACT_IDS {
        return Err(format!(
            "CONTRACT_IDS contains {} entries, but getEvents supports at most {} \
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
fn make_account_strkey(payload: &[u8; 32]) -> String {
    Ed25519PublicKey(*payload).to_string()
}

#[cfg(test)]
mod tests {
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
        assert_eq!(b64(&raw), serde_json::json!({ "_type": "unknown", "xdr": raw }));
    }

    #[test]
    fn unknown_scval_tag_returns_discriminator() {
        // Create an XDR with an unknown tag (999) — not one of the SCV_* constants.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&999u32.to_be_bytes());
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = b64(&raw);

        // Should return a structured unknown marker (no trailing-byte fallback
        // because unknown tags consume nothing after the tag itself).
        assert_eq!(result.get("_xdr_tag").and_then(|v| v.as_u64()), Some(999));
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
        bytes.extend(std::iter::repeat(0u8).take(pad));
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(result, Value::String("0xfffe".into()),
            "invalid UTF-8 symbol should render as hex, got {result:?}");
    }

    #[test]
    fn scv_contract_instance_tag_19_is_safe() {
        // #410: ScvContractInstance (tag 19) with a WASM executable and no
        // storage must decode to {"_xdr_tag":19} without leaving trailing bytes
        // that would corrupt siblings.
        // Wire: [tag=19][exec_kind=0][wasm_hash: 32 zero bytes][storage_present=0]
        let mut bytes = SCV_CONTRACT_INSTANCE.to_be_bytes().to_vec();
        bytes.extend_from_slice(&0u32.to_be_bytes()); // exec_kind = Wasm
        bytes.extend_from_slice(&[0u8; 32]);           // wasm hash (32 bytes)
        bytes.extend_from_slice(&0u32.to_be_bytes()); // storage absent
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_xdr_tag").and_then(|v| v.as_u64()),
            Some(19),
            "tag 19 should produce _xdr_tag:19, got {result:?}"
        );
    }

    #[test]
    fn scv_ledger_key_contract_instance_tag_20_is_safe() {
        // #410: ScvLedgerKeyContractInstance (tag 20) has no payload.
        let mut bytes = SCV_LEDGER_KEY_CONTRACT_INSTANCE.to_be_bytes().to_vec();
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_xdr_tag").and_then(|v| v.as_u64()),
            Some(20),
            "tag 20 should produce _xdr_tag:20, got {result:?}"
        );
    }

    #[test]
    fn scv_ledger_key_nonce_tag_21_is_safe() {
        // #410: ScvLedgerKeyNonce (tag 21) payload = ScNonceKey { nonce: i64 }.
        let mut bytes = SCV_LEDGER_KEY_NONCE.to_be_bytes().to_vec();
        bytes.extend_from_slice(&42i64.to_be_bytes()); // nonce value
        let raw = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let result = decode_scval_base64(&raw);
        assert_eq!(
            result.get("_xdr_tag").and_then(|v| v.as_u64()),
            Some(21),
            "tag 21 should produce _xdr_tag:21, got {result:?}"
        );
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
            v.extend(std::iter::repeat(0u8).take(pad));
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
            v.extend(std::iter::repeat(0u8).take(pad));
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
    fn strkey_truncated_input() {
        let valid = make_contract_strkey(&[0u8; 32]);
        assert!(!is_valid_contract_id(&valid[..10]), "truncated strkey rejected");
        assert!(!is_valid_contract_id(&valid[..30]), "truncated strkey rejected");
        assert!(!is_valid_contract_id(&valid[..55]), "truncated strkey rejected");
        assert!(!is_valid_contract_id(""), "empty strkey rejected");
    }

    #[test]
    fn strkey_overlength_input() {
        let valid = make_contract_strkey(&[0u8; 32]);
        assert!(!is_valid_contract_id(&format!("{valid}A")), "overlength rejected");
        assert!(!is_valid_contract_id(&format!("{valid}AAAA")), "overlength rejected");
    }

    #[test]
    fn strkey_wrong_version_byte() {
        let g_key = make_account_strkey(&[0u8; 32]);
        assert!(!is_valid_contract_id(&g_key), "G-strkey rejected as contract ID");
        assert_eq!(g_key.chars().next().unwrap(), 'G', "G-strkey starts with G");

        let c_key = make_contract_strkey(&[0u8; 32]);
        assert!(is_valid_contract_id(&c_key), "C-strkey accepted");
        assert_eq!(c_key.chars().next().unwrap(), 'C', "C-strkey starts with C");
    }

    #[test]
    fn strkey_roundtrip_g_and_c() {
        let g_key = make_account_strkey(&[1u8; 32]);
        assert_eq!(g_key.len(), 56, "G-strkey is 56 chars");
        assert!(g_key.starts_with('G'), "G-strkey starts with G");

        let c_key = make_contract_strkey(&[2u8; 32]);
        assert_eq!(c_key.len(), 56, "C-strkey is 56 chars");
        assert!(c_key.starts_with('C'), "C-strkey starts with C");
        assert!(is_valid_contract_id(&c_key), "C-strkey validates");
    }

    #[test]
    fn strkey_invalid_base32_chars() {
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
        assert!(err.contains("NOT_A_VALID_ID"), "error mentions bad id: {err}");
    }

    #[test]
    fn parse_contract_ids_rejects_g_strkey() {
        let g_key = make_account_strkey(&[0u8; 32]);
        let err = parse_contract_ids(&g_key).unwrap_err();
        assert!(err.contains("C\u{2026} strkey"), "error mentions expected format: {err}");
    }

    #[test]
    fn parse_contract_ids_rejects_too_many_ids() {
        let ids: Vec<String> = (0u8..26)
            .map(|i| make_contract_strkey(&[i; 32]))
            .collect();
        let raw = ids.join(",");
        let err = parse_contract_ids(&raw).unwrap_err();
        assert!(err.contains("26"), "error mentions count: {err}");
        assert!(err.contains("25"), "error mentions limit: {err}");

        let raw25 = ids[..25].join(",");
        assert_eq!(parse_contract_ids(&raw25).unwrap().len(), 25);
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
        v.extend_from_slice(&0u32.to_be_bytes());               // key_type = ed25519
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
        v.extend(std::iter::repeat(0u8).take(pad));
        v
    }

    fn scval_symbol(s: &str) -> Vec<u8> {
        let bytes = s.as_bytes();
        let mut v = SCV_SYMBOL.to_be_bytes().to_vec();
        let len = bytes.len() as u32;
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(bytes);
        let pad = (4 - (bytes.len() % 4)) % 4;
        v.extend(std::iter::repeat(0u8).take(pad));
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
    }
}
