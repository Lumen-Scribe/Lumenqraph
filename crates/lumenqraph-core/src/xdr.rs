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
}
