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

use base64::Engine;
use serde_json::{json, Map, Value};

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

// ScAddressType discriminants.
const SC_ADDRESS_ACCOUNT: u32 = 0;
const SC_ADDRESS_CONTRACT: u32 = 1;

/// Decode a base64 `ScVal` into friendly JSON. Never panics.
pub fn decode_scval_base64(b64: &str) -> Value {
    match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => {
            let mut cur = Cursor::new(&bytes);
            match cur.read_scval() {
                Some(v) => v,
                None => json!({ "_xdr": b64 }),
            }
        }
        Err(_) => json!({ "_xdr": b64 }),
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
            SCV_BOOL => Value::Bool(self.u32()? != 0),
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
                // 256-bit: no native type; render the 32 bytes as hex.
                let raw = self.take(32)?;
                json!({ "_u256_hex": hex(raw) })
            }
            SCV_BYTES => Value::String(format!("0x{}", hex(&self.var_bytes()?))),
            SCV_STRING => match String::from_utf8(self.var_bytes()?) {
                Ok(s) => Value::String(s),
                Err(e) => Value::String(format!("0x{}", hex(e.as_bytes()))),
            },
            SCV_SYMBOL => match String::from_utf8(self.var_bytes()?) {
                Ok(s) => Value::String(s),
                Err(_) => return None,
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
            _ => json!({ "_xdr_tag": tag }),
        })
    }

    fn read_map(&mut self, len: usize) -> Option<Value> {
        let mut obj = Map::new();
        let mut pairs = Vec::new();
        let mut all_stringy = true;
        for _ in 0..len {
            let k = self.read_scval()?;
            let v = self.read_scval()?;
            match &k {
                Value::String(s) => {
                    obj.insert(s.clone(), v.clone());
                }
                _ => all_stringy = false,
            }
            pairs.push(json!({ "key": k, "val": v }));
        }
        // Prefer a plain object when every key is a symbol/string.
        if all_stringy {
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
                Some(strkey(VERSION_ACCOUNT, raw))
            }
            SC_ADDRESS_CONTRACT => {
                let raw = self.take(32)?;
                Some(strkey(VERSION_CONTRACT, raw))
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

// ---- Strkey encoding (base32 of version || payload || crc16-xmodem LE) ----

const VERSION_ACCOUNT: u8 = 6 << 3; // 'G'
const VERSION_CONTRACT: u8 = 2 << 3; // 'C'

/// Returns `true` if `s` is a well-formed Stellar contract ID (`C…` strkey).
///
/// Checks: 56-character length, base32 alphabet (A–Z, 2–7), version byte
/// `0x10` (`C`), and a valid CRC16-XModem checksum over the version + payload.
pub fn is_valid_contract_id(s: &str) -> bool {
    // A contract strkey encodes version(1) + payload(32) + crc(2) = 35 bytes.
    // 35 × 8 bits / 5 bits-per-char = 56 characters exactly.
    if s.len() != 56 {
        return false;
    }
    let Some(bytes) = base32_decode(s) else {
        return false;
    };
    if bytes.len() != 35 {
        return false;
    }
    if bytes[0] != VERSION_CONTRACT {
        return false;
    }
    crc16_xmodem(&bytes[..33]) == u16::from_le_bytes([bytes[33], bytes[34]])
}

fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::with_capacity(35);
    for b in s.bytes() {
        let idx = ALPHABET.iter().position(|&a| a == b)? as u32;
        buffer = (buffer << 5) | idx;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

fn strkey(version: u8, payload: &[u8]) -> String {
    let mut data = Vec::with_capacity(1 + payload.len() + 2);
    data.push(version);
    data.extend_from_slice(payload);
    let crc = crc16_xmodem(&data);
    data.extend_from_slice(&crc.to_le_bytes());
    base32_encode(&data)
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn base32_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::new();
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for &b in data {
        buffer = (buffer << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1f) as usize;
            out.push(ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1f) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
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
    fn malformed_falls_back_to_raw() {
        let raw = base64::engine::general_purpose::STANDARD.encode([0xff, 0xff]);
        assert_eq!(b64(&raw), serde_json::json!({ "_xdr": raw }));
    }

    #[test]
    fn valid_contract_id_accepted() {
        let id = strkey(VERSION_CONTRACT, &[0u8; 32]);
        assert!(
            is_valid_contract_id(&id),
            "strkey-encoded C-address should be valid: {id}"
        );
    }

    #[test]
    fn invalid_contract_ids_rejected() {
        let valid = strkey(VERSION_CONTRACT, &[0u8; 32]);

        // Wrong length.
        assert!(!is_valid_contract_id(&valid[..55]), "too short");
        assert!(!is_valid_contract_id(&format!("{valid}A")), "too long");

        // G-strkey (account) is not a contract ID.
        let g_key = strkey(VERSION_ACCOUNT, &[0u8; 32]);
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
    fn strkey_bad_crc() {
        // Flip a byte in the payload to corrupt the checksum.
        let mut bytes = vec![VERSION_CONTRACT];
        bytes.extend_from_slice(&[0u8; 32]);
        let crc = crc16_xmodem(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        let valid = base32_encode(&bytes);

        // Now corrupt a payload byte.
        let mut corrupted_bytes = vec![VERSION_CONTRACT];
        let mut payload = [0u8; 32];
        payload[0] = 0xFF; // flip first payload byte
        corrupted_bytes.extend_from_slice(&payload);
        corrupted_bytes.extend_from_slice(&crc.to_le_bytes()); // keep old CRC
        let corrupted = base32_encode(&corrupted_bytes);

        assert!(is_valid_contract_id(&valid), "valid key should pass");
        assert!(!is_valid_contract_id(&corrupted), "corrupted CRC should fail");
    }

    #[test]
    fn strkey_truncated_input() {
        let valid = strkey(VERSION_CONTRACT, &[0u8; 32]);
        // Truncate to various lengths.
        assert!(!is_valid_contract_id(&valid[..10]), "truncated strkey rejected");
        assert!(!is_valid_contract_id(&valid[..30]), "truncated strkey rejected");
        assert!(!is_valid_contract_id(&valid[..55]), "truncated strkey rejected");
        assert!(!is_valid_contract_id(""), "empty strkey rejected");
    }

    #[test]
    fn strkey_overlength_input() {
        let valid = strkey(VERSION_CONTRACT, &[0u8; 32]);
        // Add extra characters.
        assert!(!is_valid_contract_id(&format!("{valid}A")), "overlength rejected");
        assert!(!is_valid_contract_id(&format!("{valid}AAAA")), "overlength rejected");
    }

    #[test]
    fn strkey_wrong_version_byte() {
        // G-strkey (account, version 0x30) with C payload should fail.
        let g_key = strkey(VERSION_ACCOUNT, &[0u8; 32]);
        assert!(!is_valid_contract_id(&g_key), "G-strkey rejected as contract ID");
        assert_eq!(g_key.chars().next().unwrap(), 'G', "G-strkey starts with G");

        // C-strkey (contract, version 0x10) should pass.
        let c_key = strkey(VERSION_CONTRACT, &[0u8; 32]);
        assert!(is_valid_contract_id(&c_key), "C-strkey accepted");
        assert_eq!(c_key.chars().next().unwrap(), 'C', "C-strkey starts with C");
    }

    #[test]
    fn strkey_roundtrip_g_and_c() {
        // Valid G-strkey (ed25519 public key).
        let g_payload = [1u8; 32];
        let g_key = strkey(VERSION_ACCOUNT, &g_payload);
        assert_eq!(g_key.len(), 56, "G-strkey is 56 chars");
        assert!(g_key.starts_with('G'), "G-strkey starts with G");

        // Valid C-strkey (contract ID).
        let c_payload = [2u8; 32];
        let c_key = strkey(VERSION_CONTRACT, &c_payload);
        assert_eq!(c_key.len(), 56, "C-strkey is 56 chars");
        assert!(c_key.starts_with('C'), "C-strkey starts with C");
        assert!(is_valid_contract_id(&c_key), "C-strkey validates");
    }

    #[test]
    fn strkey_invalid_base32_chars() {
        let valid = strkey(VERSION_CONTRACT, &[0u8; 32]);
        // Replace chars with invalid base32 characters.
        let mut invalid = valid.clone();
        invalid.replace_range(10..11, "0"); // '0' not in base32 alphabet
        assert!(!is_valid_contract_id(&invalid), "invalid char '0'");

        let mut invalid2 = valid.clone();
        invalid2.replace_range(15..16, "1"); // '1' not in base32 alphabet
        assert!(!is_valid_contract_id(&invalid2), "invalid char '1'");

        let mut invalid3 = valid.clone();
        invalid3.replace_range(20..21, "8"); // '8' not in base32 alphabet
        assert!(!is_valid_contract_id(&invalid3), "invalid char '8'");

        let mut invalid4 = valid;
        invalid4.replace_range(25..26, "!"); // '!' not in base32 alphabet
        assert!(!is_valid_contract_id(&invalid4), "invalid char '!'");
    }

    // ── parse_contract_ids ────────────────────────────────────────────────

    fn valid_c_strkey() -> String {
        strkey(VERSION_CONTRACT, &[0u8; 32])
    }

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
        let id = valid_c_strkey();
        assert_eq!(parse_contract_ids(&id).unwrap(), vec![id]);
    }

    #[test]
    fn parse_contract_ids_multiple_valid_ids() {
        let id1 = strkey(VERSION_CONTRACT, &[0u8; 32]);
        let id2 = strkey(VERSION_CONTRACT, &[1u8; 32]);
        let raw = format!("{id1},{id2}");
        assert_eq!(parse_contract_ids(&raw).unwrap(), vec![id1, id2]);
    }

    #[test]
    fn parse_contract_ids_trims_whitespace_around_entries() {
        let id = valid_c_strkey();
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
        let g_key = strkey(VERSION_ACCOUNT, &[0u8; 32]);
        let err = parse_contract_ids(&g_key).unwrap_err();
        assert!(err.contains("C\u{2026} strkey"), "error mentions expected format: {err}");
    }

    #[test]
    fn parse_contract_ids_rejects_too_many_ids() {
        // Build 26 valid contract IDs (one over the limit of 25).
        let mut ids: Vec<String> = (0u8..26)
            .map(|i| strkey(VERSION_CONTRACT, &[i; 32]))
            .collect();
        // Make each one unique by varying its payload byte.
        let raw = ids.join(",");
        let err = parse_contract_ids(&raw).unwrap_err();
        assert!(err.contains("26"), "error mentions count: {err}");
        assert!(err.contains("25"), "error mentions limit: {err}");
        // 25 IDs (at the limit) should be accepted.
        ids.truncate(25);
        let raw25 = ids.join(",");
        assert_eq!(parse_contract_ids(&raw25).unwrap().len(), 25);
    }
}

// ---- Property / fuzz tests -----------------------------------------------
//
// Acceptance criteria for #26:
//   • The decoder never panics on arbitrary bytes — it returns an error
//     fallback ({ "_xdr": "<base64>" }) instead.
//   • Round-trip properties hold for well-formed values of each primitive
//     ScVal kind.

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
    }
}
