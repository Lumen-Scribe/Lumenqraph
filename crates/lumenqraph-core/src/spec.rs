//! Typed, self-describing decoding from a contract's **on-chain interface**.
//!
//! Soroban contracts embed their full interface schema inside the deployed WASM,
//! in a custom section named `contractspecv0`: an XDR-encoded list of
//! [`ScSpecEntry`] describing every function, user-defined type, and (as of
//! Protocol 23) every event — names, field names, and types included.
//!
//! This is a Soroban-native advantage: on EVM chains the equivalent ABI lives
//! off-chain and has to be uploaded or verified by hand. Here the schema ships
//! with the code, so we can turn a generically-decoded event
//! (`["transfer", "G…", "G…"], "105000000"`) into a fully named, typed record
//! (`{ from: Address, to: Address, amount: i128 }`) with **zero configuration**.
//!
//! Everything here is best-effort: a contract with no spec section, an
//! unrecognised event, or a length mismatch simply yields `None`, and the caller
//! falls back to the always-present generic decoding.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::{json, Value};
use stellar_xdr::curr::{
    Limited, Limits, ReadXdr, ScSpecEntry, ScSpecEventDataFormat, ScSpecEventParamLocationV0,
    ScSpecTypeDef,
};

/// A contract's parsed interface: the queryable form of `contractspecv0`.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ContractSpec {
    pub functions: Vec<FunctionSpec>,
    pub events: Vec<EventSpec>,
    pub structs: Vec<UdtStruct>,
    pub unions: Vec<UdtUnion>,
    pub enums: Vec<UdtEnum>,
    /// event name -> index into `events`, for O(1) enrichment lookups.
    #[serde(skip)]
    events_by_name: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionSpec {
    pub name: String,
    pub doc: String,
    pub inputs: Vec<Field>,
    pub outputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventSpec {
    pub name: String,
    pub doc: String,
    pub params: Vec<EventParam>,
    /// How the non-topic data is laid out: "single" | "vec" | "map".
    pub data_format: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventParam {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    /// "topic" (indexed) or "data" (in the event body).
    pub location: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UdtStruct {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UdtUnion {
    pub name: String,
    pub cases: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UdtEnum {
    pub name: String,
    pub cases: Vec<(String, u32)>,
}

impl ContractSpec {
    /// Parse a contract's interface out of its deployed WASM. Returns `None` if
    /// the module carries no `contractspecv0` section (e.g. a Stellar Asset
    /// Contract) or the section can't be parsed.
    pub fn from_wasm(wasm: &[u8]) -> Option<Self> {
        let section = spec_section_of(wasm)?;
        Self::from_spec_xdr(section)
    }

    /// Parse a concatenated stream of XDR `ScSpecEntry` (the raw section body).
    pub fn from_spec_xdr(bytes: &[u8]) -> Option<Self> {
        let mut spec = ContractSpec::default();
        // The section is a back-to-back sequence of ScSpecEntry with no outer
        // length prefix; the iterator reads entries until the stream is drained.
        let mut limited = Limited::new(bytes, Limits::none());
        for entry in ScSpecEntry::read_xdr_iter(&mut limited) {
            match entry {
                Ok(e) => spec.push_entry(e),
                // A trailing partial / unrecognised entry ends parsing; keep what we have.
                Err(_) => break,
            }
        }
        if spec.is_empty() {
            None
        } else {
            spec.reindex();
            Some(spec)
        }
    }

    fn is_empty(&self) -> bool {
        self.functions.is_empty()
            && self.events.is_empty()
            && self.structs.is_empty()
            && self.unions.is_empty()
            && self.enums.is_empty()
    }

    fn reindex(&mut self) {
        self.events_by_name = self
            .events
            .iter()
            .enumerate()
            .map(|(i, e)| (e.name.clone(), i))
            .collect();
    }

    fn push_entry(&mut self, entry: ScSpecEntry) {
        match entry {
            ScSpecEntry::FunctionV0(f) => self.functions.push(FunctionSpec {
                name: f.name.to_utf8_string_lossy(),
                doc: string_of(&f.doc),
                inputs: f
                    .inputs
                    .iter()
                    .map(|i| Field {
                        name: string_of(&i.name),
                        type_name: type_name(&i.type_),
                    })
                    .collect(),
                outputs: f.outputs.iter().map(type_name).collect(),
            }),
            ScSpecEntry::UdtStructV0(s) => self.structs.push(UdtStruct {
                name: string_of(&s.name),
                fields: s
                    .fields
                    .iter()
                    .map(|f| Field {
                        name: string_of(&f.name),
                        type_name: type_name(&f.type_),
                    })
                    .collect(),
            }),
            ScSpecEntry::UdtUnionV0(u) => self.unions.push(UdtUnion {
                name: string_of(&u.name),
                cases: u
                    .cases
                    .iter()
                    .map(|c| match c {
                        stellar_xdr::curr::ScSpecUdtUnionCaseV0::VoidV0(v) => string_of(&v.name),
                        stellar_xdr::curr::ScSpecUdtUnionCaseV0::TupleV0(t) => string_of(&t.name),
                    })
                    .collect(),
            }),
            ScSpecEntry::UdtEnumV0(e) => self.enums.push(UdtEnum {
                name: string_of(&e.name),
                cases: e
                    .cases
                    .iter()
                    .map(|c| (string_of(&c.name), c.value))
                    .collect(),
            }),
            ScSpecEntry::EventV0(ev) => {
                let params = ev
                    .params
                    .iter()
                    .map(|p| EventParam {
                        name: string_of(&p.name),
                        type_name: type_name(&p.type_),
                        location: match p.location {
                            ScSpecEventParamLocationV0::TopicList => "topic",
                            ScSpecEventParamLocationV0::Data => "data",
                        },
                    })
                    .collect();
                self.events.push(EventSpec {
                    name: ev.name.to_utf8_string_lossy(),
                    doc: string_of(&ev.doc),
                    params,
                    data_format: match ev.data_format {
                        ScSpecEventDataFormat::SingleValue => "single",
                        ScSpecEventDataFormat::Vec => "vec",
                        ScSpecEventDataFormat::Map => "map",
                    },
                });
            }
            // Error enums carry no data useful for event enrichment.
            ScSpecEntry::UdtErrorEnumV0(_) => {}
        }
    }

    /// True if this contract publishes at least one typed event schema.
    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    /// Enrich a generically-decoded event into a named, typed record using the
    /// matching event spec. `decoded_topics[0]` is expected to be the event
    /// name symbol; the remaining topics and `decoded_value` are already-decoded
    /// JSON from the generic decoder. Returns `None` when no spec matches.
    pub fn enrich_event(
        &self,
        event_name: &str,
        decoded_topics: &[Value],
        decoded_value: &Value,
    ) -> Option<Value> {
        let spec = self.events.get(*self.events_by_name.get(event_name)?)?;

        // Topic params bind to topics after the name symbol (index 0); data
        // params bind to the event body according to the declared data format.
        let topic_vals = decoded_topics.get(1..).unwrap_or(&[]);
        let mut topic_iter = topic_vals.iter();

        // Pre-split the data side so each data param can be pulled by position
        // (Vec) or by name (Map); SingleValue feeds the lone data param directly.
        let data_array = match (spec.data_format, decoded_value) {
            ("vec", Value::Array(a)) => Some(a.clone()),
            _ => None,
        };
        let mut data_iter = data_array.iter().flatten();

        let mut params = serde_json::Map::new();
        for p in &spec.params {
            let value = match p.location {
                "topic" => topic_iter.next().cloned().unwrap_or(Value::Null),
                _ => match spec.data_format {
                    "map" => decoded_value.get(&p.name).cloned().unwrap_or(Value::Null),
                    "vec" => data_iter.next().cloned().unwrap_or(Value::Null),
                    // single value: the body is the one data param.
                    _ => decoded_value.clone(),
                },
            };
            params.insert(
                p.name.clone(),
                json!({ "type": p.type_name, "value": value }),
            );
        }

        Some(json!({
            "event": spec.name,
            "params": Value::Object(params),
        }))
    }

    /// A stable JSON view of the whole interface, for `GET /contracts/:id/interface`.
    pub fn to_interface_json(&self) -> Value {
        json!({
            "functions": self.functions,
            "events": self.events,
            "structs": self.structs,
            "unions": self.unions,
            "enums": self.enums,
        })
    }
}

/// The raw `contractspecv0` custom-section bytes of a deployed WASM module, if
/// present. This is the XDR the read layer re-parses to encode typed call args.
pub fn spec_section_of(wasm: &[u8]) -> Option<&[u8]> {
    wasm_custom_section(wasm, "contractspecv0")
}

/// Render an `ScSpecTypeDef` as a compact, human-readable type name, e.g.
/// `i128`, `Address`, `Option<u64>`, `Vec<Address>`, `Map<Symbol, i128>`,
/// `BytesN<32>`, or a UDT's own name.
pub(crate) fn type_name(t: &ScSpecTypeDef) -> String {
    use ScSpecTypeDef as T;
    match t {
        T::Val => "Val".into(),
        T::Bool => "bool".into(),
        T::Void => "void".into(),
        T::Error => "Error".into(),
        T::U32 => "u32".into(),
        T::I32 => "i32".into(),
        T::U64 => "u64".into(),
        T::I64 => "i64".into(),
        T::Timepoint => "Timepoint".into(),
        T::Duration => "Duration".into(),
        T::U128 => "u128".into(),
        T::I128 => "i128".into(),
        T::U256 => "u256".into(),
        T::I256 => "i256".into(),
        T::Bytes => "Bytes".into(),
        T::String => "String".into(),
        T::Symbol => "Symbol".into(),
        T::Address => "Address".into(),
        T::MuxedAddress => "MuxedAddress".into(),
        T::Option(o) => format!("Option<{}>", type_name(&o.value_type)),
        T::Result(r) => format!(
            "Result<{}, {}>",
            type_name(&r.ok_type),
            type_name(&r.error_type)
        ),
        T::Vec(v) => format!("Vec<{}>", type_name(&v.element_type)),
        T::Map(m) => format!(
            "Map<{}, {}>",
            type_name(&m.key_type),
            type_name(&m.value_type)
        ),
        T::Tuple(t) => {
            let inner: Vec<String> = t.value_types.iter().map(type_name).collect();
            format!("({})", inner.join(", "))
        }
        T::BytesN(b) => format!("BytesN<{}>", b.n),
        T::Udt(u) => u.name.to_utf8_string_lossy(),
    }
}

/// XDR `string`s in the spec are UTF-8 with a length bound; render lossily.
fn string_of<const N: u32>(s: &stellar_xdr::curr::StringM<N>) -> String {
    s.to_utf8_string_lossy()
}

/// Locate a WASM custom section by name and return its content bytes.
///
/// WASM module layout: an 8-byte header (`\0asm` + version), then a sequence of
/// sections, each `section_id: u8`, `size: leb128-u32`, `payload[size]`. Custom
/// sections have id 0 and a payload that begins with a `name` (leb128 length +
/// UTF-8), followed by the section content.
fn wasm_custom_section<'a>(wasm: &'a [u8], want: &str) -> Option<&'a [u8]> {
    let mut c = Reader { buf: wasm, pos: 0 };
    // Header: magic + version.
    if c.take(4)? != b"\0asm" {
        return None;
    }
    let _version = c.take(4)?;

    while c.pos < wasm.len() {
        let id = c.byte()?;
        let size = c.leb_u32()? as usize;
        let body_start = c.pos;
        let body_end = body_start.checked_add(size)?;
        if body_end > wasm.len() {
            return None;
        }
        if id == 0 {
            // Custom section: name then content, all within [body_start, body_end).
            let name_len = c.leb_u32()? as usize;
            let name_bytes = c.take(name_len)?;
            let content_start = c.pos;
            if name_bytes == want.as_bytes() {
                return wasm.get(content_start..body_end);
            }
        }
        // Skip to the next section regardless of what we read inside.
        c.pos = body_end;
    }
    None
}

/// Minimal byte reader with just the LEB128 + slice ops the WASM walk needs.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    /// Unsigned LEB128, bounded to 5 bytes (u32).
    fn leb_u32(&mut self) -> Option<u32> {
        let mut result: u32 = 0;
        let mut shift = 0;
        for _ in 0..5 {
            let byte = self.byte()?;
            result |= ((byte & 0x7f) as u32) << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::{
        ScSpecEventParamV0, ScSpecEventV0, ScSpecUdtStructFieldV0, ScSpecUdtStructV0, ScSymbol,
        VecM, WriteXdr,
    };

    // Build a `transfer(from: Address, to: Address, amount: i128)` event spec,
    // with from/to as topics and amount as single-value data.
    fn transfer_event_entry() -> ScSpecEntry {
        let param =
            |name: &str, ty: ScSpecTypeDef, loc: ScSpecEventParamLocationV0| ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: name.try_into().unwrap(),
                type_: ty,
                location: loc,
            };
        let params: VecM<ScSpecEventParamV0, 50> = vec![
            param(
                "from",
                ScSpecTypeDef::Address,
                ScSpecEventParamLocationV0::TopicList,
            ),
            param(
                "to",
                ScSpecTypeDef::Address,
                ScSpecEventParamLocationV0::TopicList,
            ),
            param(
                "amount",
                ScSpecTypeDef::I128,
                ScSpecEventParamLocationV0::Data,
            ),
        ]
        .try_into()
        .unwrap();
        ScSpecEntry::EventV0(ScSpecEventV0 {
            doc: "A token transfer.".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: ScSymbol("transfer".try_into().unwrap()),
            prefix_topics: vec![ScSymbol("transfer".try_into().unwrap())]
                .try_into()
                .unwrap(),
            params,
            data_format: ScSpecEventDataFormat::SingleValue,
        })
    }

    fn position_struct_entry() -> ScSpecEntry {
        let field = |name: &str, ty: ScSpecTypeDef| ScSpecUdtStructFieldV0 {
            doc: "".try_into().unwrap(),
            name: name.try_into().unwrap(),
            type_: ty,
        };
        ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
            doc: "".try_into().unwrap(),
            lib: "".try_into().unwrap(),
            name: "Position".try_into().unwrap(),
            fields: vec![
                field("borrower", ScSpecTypeDef::Address),
                field("debt", ScSpecTypeDef::I128),
            ]
            .try_into()
            .unwrap(),
        })
    }

    /// Concatenate entries into a section body, exactly as the WASM stores them.
    fn spec_section(entries: &[ScSpecEntry]) -> Vec<u8> {
        let mut out = Vec::new();
        for e in entries {
            out.extend(e.to_xdr(Limits::none()).unwrap());
        }
        out
    }

    #[test]
    fn parses_event_and_struct_from_spec_xdr() {
        let body = spec_section(&[transfer_event_entry(), position_struct_entry()]);
        let spec = ContractSpec::from_spec_xdr(&body).expect("should parse");
        assert_eq!(spec.events.len(), 1);
        assert_eq!(spec.events[0].name, "transfer");
        assert_eq!(spec.events[0].params.len(), 3);
        assert_eq!(spec.structs.len(), 1);
        assert_eq!(spec.structs[0].name, "Position");
        assert_eq!(spec.structs[0].fields[1].type_name, "i128");
    }

    #[test]
    fn enriches_a_transfer_event() {
        let body = spec_section(&[transfer_event_entry()]);
        let spec = ContractSpec::from_spec_xdr(&body).unwrap();

        let topics = vec![json!("transfer"), json!("GFROM"), json!("GTO")];
        let value = json!("105000000");
        let enriched = spec.enrich_event("transfer", &topics, &value).unwrap();

        assert_eq!(enriched["event"], "transfer");
        assert_eq!(enriched["params"]["from"]["value"], "GFROM");
        assert_eq!(enriched["params"]["from"]["type"], "Address");
        assert_eq!(enriched["params"]["to"]["value"], "GTO");
        assert_eq!(enriched["params"]["amount"]["value"], "105000000");
        assert_eq!(enriched["params"]["amount"]["type"], "i128");
    }

    #[test]
    fn unknown_event_is_not_enriched() {
        let body = spec_section(&[transfer_event_entry()]);
        let spec = ContractSpec::from_spec_xdr(&body).unwrap();
        assert!(spec
            .enrich_event("mint", &[json!("mint")], &json!(1))
            .is_none());
    }

    fn push_leb(out: &mut Vec<u8>, mut v: u32) {
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if v == 0 {
                break;
            }
        }
    }

    #[test]
    fn finds_and_parses_a_real_wasm_custom_section() {
        // Minimal WASM module: header + one custom section "contractspecv0".
        // The section body easily exceeds 127 bytes, so sizes need real LEB128.
        let body = spec_section(&[transfer_event_entry()]);
        let name = b"contractspecv0";
        let mut section = Vec::new();
        push_leb(&mut section, name.len() as u32);
        section.extend(name);
        section.extend(&body);

        let mut wasm = Vec::new();
        wasm.extend(b"\0asm");
        wasm.extend([1, 0, 0, 0]); // version
        wasm.push(0); // custom section id
        push_leb(&mut wasm, section.len() as u32);
        wasm.extend(section);

        let spec = ContractSpec::from_wasm(&wasm).expect("should find + parse section");
        assert_eq!(spec.events[0].name, "transfer");
    }

    #[test]
    fn wasm_without_spec_section_yields_none() {
        let mut wasm = Vec::new();
        wasm.extend(b"\0asm");
        wasm.extend([1, 0, 0, 0]);
        assert!(ContractSpec::from_wasm(&wasm).is_none());
    }

    #[test]
    fn renders_nested_type_names() {
        let opt_u64 = ScSpecTypeDef::Option(Box::new(stellar_xdr::curr::ScSpecTypeOption {
            value_type: Box::new(ScSpecTypeDef::U64),
        }));
        assert_eq!(type_name(&opt_u64), "Option<u64>");
    }
}
