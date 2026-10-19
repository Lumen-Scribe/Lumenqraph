//! Golden-file regression corpus for XDR decoding, spec parsing, and
//! UDT-enriched event output.
//!
//! Every test in this file is self-contained and deterministic: no network, no
//! database, no environment variables required. Each case encodes the
//! *expectation inline* as `serde_json::json!{…}` so a diff between the
//! committed value and today's output is immediately visible in CI.
//!
//! # How to regenerate fixtures
//!
//! If a decode or enrich behaviour is intentionally changed (e.g. a new integer
//! rendering), update the expected `json!{…}` values in this file to match the
//! new output and commit both the code change and the updated expectations
//! together. The test names are stable identifiers for the specific behaviour
//! being pinned.
//!
//! # Corpus structure
//!
//! 1. `xdr_*`   — raw base64 ScVal → decoded JSON (lumenqraph_core::xdr)
//! 2. `spec_*`  — raw spec XDR bytes → parsed ContractSpec shape
//! 3. `enrich_*`— ContractSpec::enrich_event on decoded topics/value
//! 4. `udt_*`   — ContractSpec::relabel_value for struct/enum/union results

use lumenqraph_core::xdr::decode_scval_base64;
use lumenqraph_core::ContractSpec;
use serde_json::json;
use stellar_xdr::curr::{
    Limits, ScSpecEntry, ScSpecEventDataFormat, ScSpecEventParamLocationV0, ScSpecEventV0,
    ScSpecFunctionInputV0, ScSpecFunctionV0, ScSpecTypeDef, ScSpecUdtEnumCaseV0, ScSpecUdtEnumV0,
    ScSpecUdtStructFieldV0, ScSpecUdtStructV0, ScSpecUdtUnionCaseV0, ScSpecUdtUnionCaseTupleV0,
    ScSpecUdtUnionCaseVoidV0, ScSpecUdtUnionV0, ScSymbol, WriteXdr,
};

// ── helpers ────────────────────────────────────────────────────────────────

/// Concatenate raw XDR bytes for a list of ScSpecEntry items into the flat
/// stream that is the `contractspecv0` section body.
fn spec_bytes(entries: &[ScSpecEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        out.extend_from_slice(&e.to_xdr(Limits::none()).unwrap());
    }
    out
}

fn sym(s: &str) -> ScSymbol {
    ScSymbol(s.try_into().unwrap())
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. XDR DECODING GOLDEN TESTS
// ═══════════════════════════════════════════════════════════════════════════

/// Symbol "fee" captured from a real Soroban event topic.
#[test]
fn xdr_symbol_fee() {
    assert_eq!(
        decode_scval_base64("AAAADwAAAANmZWUA"),
        json!("fee")
    );
}

/// Symbol "transfer" — the canonical SEP-41 event name.
#[test]
fn xdr_symbol_transfer() {
    // ScVal::Symbol("transfer") — 8 chars, padded to 8 (already aligned).
    assert_eq!(
        decode_scval_base64("AAAADwAAAAh0cmFuc2Zlcg=="),
        json!("transfer")
    );
}

/// i128 small positive value (300) captured from a fee event.
#[test]
fn xdr_i128_small_positive() {
    // hi=0, lo=300 → "300"
    assert_eq!(
        decode_scval_base64("AAAACgAAAAAAAAAAAAAAAAAAASw="),
        json!("300")
    );
}

/// i128 zero.
#[test]
fn xdr_i128_zero() {
    // hi=0, lo=0
    assert_eq!(
        decode_scval_base64("AAAACgAAAAAAAAAAAAAAAAAAAAA="),
        json!("0")
    );
}

/// u32 value.
#[test]
fn xdr_u32() {
    // ScVal::U32(7) — tag=3, value=7
    assert_eq!(
        decode_scval_base64("AAAAAwAAAAc="),
        json!(7)
    );
}

/// bool true.
#[test]
fn xdr_bool_true() {
    // ScVal::Bool(true) — tag=0, value=1
    assert_eq!(
        decode_scval_base64("AAAAAAAAAAE="),
        json!(true)
    );
}

/// bool false.
#[test]
fn xdr_bool_false() {
    // ScVal::Bool(false) — tag=0, value=0
    assert_eq!(
        decode_scval_base64("AAAAAAAAAAA="),
        json!(false)
    );
}

/// void / null.
#[test]
fn xdr_void_is_null() {
    // ScVal::Void — tag=1
    assert_eq!(
        decode_scval_base64("AAAAAQ=="),
        json!(null)
    );
}

/// Account address decodes to a G… strkey.
#[test]
fn xdr_account_address_is_g_strkey() {
    let v = decode_scval_base64(
        "AAAAEgAAAAAAAAAAZnYwtpgeUB4mlva1EnnCVBm0hGxbz5B5Zl89BaJLufM=",
    );
    let s = v.as_str().expect("should be a string");
    assert!(s.starts_with('G'), "account address must start with G, got {s}");
    assert_eq!(s.len(), 56, "ed25519 strkey must be 56 chars, got {s}");
}

/// Malformed base64 falls back to `{_xdr: "<input>"}` rather than panicking.
#[test]
fn xdr_malformed_falls_back_gracefully() {
    let raw = "not!valid!base64!!";
    assert_eq!(decode_scval_base64(raw), json!({ "_xdr": raw }));
}

/// A Vec of three i128s decodes to a JSON array of decimal strings.
#[test]
fn xdr_vec_of_i128s() {
    let v = decode_scval_base64(
        "AAAAEAAAAAEAAAADAAAACv///////////////8bZ+tEAAAAKAAAAAAAAAAAAAAARmN6/agAAAAoAAAAAAAAAAAAAAAAAAAAA",
    );
    assert!(v.is_array(), "expected array, got {v:?}");
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Each element should be a decimal-string i128.
    for el in arr {
        assert!(el.is_string(), "each vec element should be a decimal string, got {el:?}");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. SPEC PARSING GOLDEN TESTS
// ═══════════════════════════════════════════════════════════════════════════

/// A SEP-41 token contract spec parses to the expected function + event list.
#[test]
fn spec_sep41_transfer_function_and_event() {
    // Build a minimal SEP-41-like spec: one `transfer` function and one
    // `transfer` event with `from`, `to` (topics) and `amount` (data).
    let transfer_fn = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: sym("transfer"),
        inputs: vec![
            ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "from".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            },
            ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "to".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            },
            ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            },
        ]
        .try_into()
        .unwrap(),
        outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
    });

    use stellar_xdr::curr::ScSpecEventParamV0;
    let transfer_event = ScSpecEntry::EventV0(ScSpecEventV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: sym("transfer"),
        prefix_topics: vec![sym("transfer")].try_into().unwrap(),
        params: vec![
            ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: "from".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
                location: ScSpecEventParamLocationV0::TopicList,
            },
            ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: "to".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
                location: ScSpecEventParamLocationV0::TopicList,
            },
            ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
                location: ScSpecEventParamLocationV0::Data,
            },
        ]
        .try_into()
        .unwrap(),
        data_format: ScSpecEventDataFormat::SingleValue,
    });

    let bytes = spec_bytes(&[transfer_fn, transfer_event]);
    let spec = ContractSpec::from_spec_xdr(&bytes).expect("spec must parse");

    // Functions
    assert_eq!(spec.functions.len(), 1, "expected one function");
    let f = &spec.functions[0];
    assert_eq!(f.name, "transfer");
    assert_eq!(f.inputs.len(), 3);
    assert_eq!(f.inputs[0].name, "from");
    assert_eq!(f.inputs[0].type_name, "Address");
    assert_eq!(f.inputs[1].name, "to");
    assert_eq!(f.inputs[1].type_name, "Address");
    assert_eq!(f.inputs[2].name, "amount");
    assert_eq!(f.inputs[2].type_name, "i128");
    assert_eq!(f.outputs, vec!["void"]);

    // Events
    assert_eq!(spec.events.len(), 1, "expected one event");
    let e = &spec.events[0];
    assert_eq!(e.name, "transfer");
    assert_eq!(e.data_format, "single");
    assert_eq!(e.prefix_topics, vec!["transfer"]);
    assert_eq!(e.params.len(), 3);
    assert_eq!(e.params[0].name, "from");
    assert_eq!(e.params[0].location, "topic");
    assert_eq!(e.params[1].name, "to");
    assert_eq!(e.params[1].location, "topic");
    assert_eq!(e.params[2].name, "amount");
    assert_eq!(e.params[2].location, "data");
}

/// A spec with a UDT struct parses its fields correctly.
#[test]
fn spec_udt_struct_fields_parsed() {
    let order_struct = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Order".try_into().unwrap(),
        fields: vec![
            ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            },
            ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "buyer".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            },
        ]
        .try_into()
        .unwrap(),
    });
    // We need at least one function for from_spec_xdr to return Some.
    let dummy_fn = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: sym("get"),
        inputs: vec![].try_into().unwrap(),
        outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
    });

    let bytes = spec_bytes(&[dummy_fn, order_struct]);
    let spec = ContractSpec::from_spec_xdr(&bytes).expect("spec must parse");

    assert_eq!(spec.structs.len(), 1);
    let s = &spec.structs[0];
    assert_eq!(s.name, "Order");
    assert_eq!(s.fields.len(), 2);
    assert_eq!(s.fields[0].name, "amount");
    assert_eq!(s.fields[0].type_name, "i128");
    assert_eq!(s.fields[1].name, "buyer");
    assert_eq!(s.fields[1].type_name, "Address");
}

/// A spec with a unit enum parses its cases correctly.
#[test]
fn spec_udt_enum_cases_parsed() {
    let status_enum = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Status".try_into().unwrap(),
        cases: vec![
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Open".try_into().unwrap(),
                value: 0,
            },
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Filled".try_into().unwrap(),
                value: 1,
            },
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Cancelled".try_into().unwrap(),
                value: 2,
            },
        ]
        .try_into()
        .unwrap(),
    });
    let dummy_fn = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: sym("get"),
        inputs: vec![].try_into().unwrap(),
        outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
    });

    let bytes = spec_bytes(&[dummy_fn, status_enum]);
    let spec = ContractSpec::from_spec_xdr(&bytes).expect("spec must parse");

    assert_eq!(spec.enums.len(), 1);
    let e = &spec.enums[0];
    assert_eq!(e.name, "Status");
    assert_eq!(e.cases, vec![
        ("Open".to_string(), 0u32),
        ("Filled".to_string(), 1),
        ("Cancelled".to_string(), 2),
    ]);
}

/// A spec with a union parses void and tuple cases.
#[test]
fn spec_udt_union_cases_parsed() {
    let action_union = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Action".try_into().unwrap(),
        cases: vec![
            ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                doc: "".try_into().unwrap(),
                name: "Cancel".try_into().unwrap(),
            }),
            ScSpecUdtUnionCaseV0::TupleV0(ScSpecUdtUnionCaseTupleV0 {
                doc: "".try_into().unwrap(),
                name: "Bid".try_into().unwrap(),
                type_: vec![ScSpecTypeDef::Address, ScSpecTypeDef::I128]
                    .try_into()
                    .unwrap(),
            }),
        ]
        .try_into()
        .unwrap(),
    });
    let dummy_fn = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: sym("act"),
        inputs: vec![].try_into().unwrap(),
        outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
    });

    let bytes = spec_bytes(&[dummy_fn, action_union]);
    let spec = ContractSpec::from_spec_xdr(&bytes).expect("spec must parse");

    assert_eq!(spec.unions.len(), 1);
    let u = &spec.unions[0];
    assert_eq!(u.name, "Action");
    assert_eq!(u.cases.len(), 2);
    assert_eq!(u.cases[0].name, "Cancel");
    assert!(u.cases[0].type_names.is_empty(), "void case has no types");
    assert_eq!(u.cases[1].name, "Bid");
    assert_eq!(u.cases[1].type_names, vec!["Address", "i128"]);
}

/// An empty / no-spec WASM section returns None cleanly.
#[test]
fn spec_empty_bytes_returns_none() {
    assert!(ContractSpec::from_spec_xdr(&[]).is_none());
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. EVENT ENRICHMENT GOLDEN TESTS
// ═══════════════════════════════════════════════════════════════════════════

/// Helper: build a SEP-41 ContractSpec (transfer function + event).
fn sep41_spec() -> ContractSpec {
    use stellar_xdr::curr::ScSpecEventParamV0;

    let transfer_fn = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: sym("transfer"),
        inputs: vec![
            ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "from".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            },
            ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "to".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            },
            ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            },
        ]
        .try_into()
        .unwrap(),
        outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
    });

    let transfer_event = ScSpecEntry::EventV0(ScSpecEventV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: sym("transfer"),
        prefix_topics: vec![sym("transfer")].try_into().unwrap(),
        params: vec![
            ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: "from".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
                location: ScSpecEventParamLocationV0::TopicList,
            },
            ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: "to".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
                location: ScSpecEventParamLocationV0::TopicList,
            },
            ScSpecEventParamV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
                location: ScSpecEventParamLocationV0::Data,
            },
        ]
        .try_into()
        .unwrap(),
        data_format: ScSpecEventDataFormat::SingleValue,
    });

    ContractSpec::from_spec_xdr(&spec_bytes(&[transfer_fn, transfer_event]))
        .expect("sep41 spec must parse")
}

/// A SEP-41 transfer event enriches to the canonical named record.
/// This pins the exact output shape the API/MCP serves to clients.
#[test]
fn enrich_sep41_transfer_golden() {
    let spec = sep41_spec();

    // Decoded topics: [event_name_symbol, from_address, to_address]
    let decoded_topics = vec![
        json!("transfer"),
        json!("GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M96ENTMLX2R7G9S"),
        json!("GDN4OHYR3TBP6GYOFP4RWHSJ6GTVQ7QKLPYHIKQC7Y4QIJZRJX7GQXN"),
    ];
    // Decoded value: the amount as a decimal string i128
    let decoded_value = json!("100000000000");

    let enriched = spec
        .enrich_event("transfer", &decoded_topics, &decoded_value)
        .expect("transfer event must enrich");

    assert_eq!(enriched["event"], "transfer");

    let params = &enriched["params"];
    assert_eq!(params["from"]["type"], "Address");
    assert_eq!(
        params["from"]["value"],
        "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M96ENTMLX2R7G9S"
    );
    assert_eq!(params["to"]["type"], "Address");
    assert_eq!(
        params["to"]["value"],
        "GDN4OHYR3TBP6GYOFP4RWHSJ6GTVQ7QKLPYHIKQC7Y4QIJZRJX7GQXN"
    );
    assert_eq!(params["amount"]["type"], "i128");
    assert_eq!(params["amount"]["value"], "100000000000");
}

/// An event with no matching spec returns None (not an error).
#[test]
fn enrich_unknown_event_returns_none() {
    let spec = sep41_spec();
    let result = spec.enrich_event("unknown_event", &[json!("unknown_event")], &json!(null));
    assert!(result.is_none(), "unknown event must return None, not panic");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. UDT RELABELING GOLDEN TESTS
// ═══════════════════════════════════════════════════════════════════════════

/// Helper: spec with Status enum + Action union + Order struct.
fn udt_spec() -> ContractSpec {
    let status_enum = ScSpecEntry::UdtEnumV0(ScSpecUdtEnumV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Status".try_into().unwrap(),
        cases: vec![
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Open".try_into().unwrap(),
                value: 0,
            },
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Filled".try_into().unwrap(),
                value: 1,
            },
            ScSpecUdtEnumCaseV0 {
                doc: "".try_into().unwrap(),
                name: "Cancelled".try_into().unwrap(),
                value: 2,
            },
        ]
        .try_into()
        .unwrap(),
    });

    let action_union = ScSpecEntry::UdtUnionV0(ScSpecUdtUnionV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Action".try_into().unwrap(),
        cases: vec![
            ScSpecUdtUnionCaseV0::VoidV0(ScSpecUdtUnionCaseVoidV0 {
                doc: "".try_into().unwrap(),
                name: "Cancel".try_into().unwrap(),
            }),
            ScSpecUdtUnionCaseV0::TupleV0(ScSpecUdtUnionCaseTupleV0 {
                doc: "".try_into().unwrap(),
                name: "Bid".try_into().unwrap(),
                type_: vec![ScSpecTypeDef::Address, ScSpecTypeDef::I128]
                    .try_into()
                    .unwrap(),
            }),
        ]
        .try_into()
        .unwrap(),
    });

    let order_struct = ScSpecEntry::UdtStructV0(ScSpecUdtStructV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: "Order".try_into().unwrap(),
        fields: vec![
            ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "amount".try_into().unwrap(),
                type_: ScSpecTypeDef::I128,
            },
            ScSpecUdtStructFieldV0 {
                doc: "".try_into().unwrap(),
                name: "buyer".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            },
        ]
        .try_into()
        .unwrap(),
    });

    // Need at least one function so from_spec_xdr returns Some.
    let dummy_fn = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: sym("noop"),
        inputs: vec![].try_into().unwrap(),
        outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
    });

    ContractSpec::from_spec_xdr(&spec_bytes(&[dummy_fn, status_enum, action_union, order_struct]))
        .expect("udt spec must parse")
}

/// A unit enum discriminant is relabeled to the case name string.
#[test]
fn udt_enum_discriminant_relabeled_to_case_name() {
    use stellar_xdr::curr::ScSpecTypeUdt;
    let spec = udt_spec();

    let ty = ScSpecTypeDef::Udt(ScSpecTypeUdt {
        name: "Status".try_into().unwrap(),
    });

    assert_eq!(spec.relabel_value(&json!(0), &ty), json!("Open"));
    assert_eq!(spec.relabel_value(&json!(1), &ty), json!("Filled"));
    assert_eq!(spec.relabel_value(&json!(2), &ty), json!("Cancelled"));
}

/// A void union case is relabeled to its case name string.
#[test]
fn udt_union_void_case_relabeled_to_name() {
    use stellar_xdr::curr::ScSpecTypeUdt;
    let spec = udt_spec();

    let ty = ScSpecTypeDef::Udt(ScSpecTypeUdt {
        name: "Action".try_into().unwrap(),
    });

    // The generic decoder produces ["Cancel"] for a void case.
    let raw = json!(["Cancel"]);
    assert_eq!(spec.relabel_value(&raw, &ty), json!("Cancel"));
}

/// A tuple union case is relabeled to `{CaseName: [..values]}`.
#[test]
fn udt_union_tuple_case_relabeled_to_named_map() {
    use stellar_xdr::curr::ScSpecTypeUdt;
    let spec = udt_spec();

    let ty = ScSpecTypeDef::Udt(ScSpecTypeUdt {
        name: "Action".try_into().unwrap(),
    });

    // ["Bid", "GABC...", "250"] from the generic decoder.
    let raw = json!(["Bid", "GABC1111111111111111111111111111111111111111111111111111", "250"]);
    let relabeled = spec.relabel_value(&raw, &ty);
    assert!(relabeled.get("Bid").is_some(), "expected {{Bid: [..]}}, got {relabeled}");
    let vals = relabeled["Bid"].as_array().unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0], "GABC1111111111111111111111111111111111111111111111111111");
    assert_eq!(vals[1], "250");
}

/// A struct decoded as an object is passed through with its field values
/// recursively relabeled (no-op for primitive fields, but the shape is stable).
#[test]
fn udt_struct_object_fields_are_stable() {
    use stellar_xdr::curr::ScSpecTypeUdt;
    let spec = udt_spec();

    let ty = ScSpecTypeDef::Udt(ScSpecTypeUdt {
        name: "Order".try_into().unwrap(),
    });

    let raw = json!({ "amount": "500", "buyer": "GABC1111111111111111111111111111111111111111111111111111" });
    let relabeled = spec.relabel_value(&raw, &ty);
    // Primitive fields pass through unchanged.
    assert_eq!(relabeled["amount"], "500");
    assert_eq!(relabeled["buyer"], "GABC1111111111111111111111111111111111111111111111111111");
}

/// Unknown UDT name: the value is returned unchanged (best-effort, no panic).
#[test]
fn udt_unknown_type_returns_value_unchanged() {
    use stellar_xdr::curr::ScSpecTypeUdt;
    let spec = udt_spec();

    let ty = ScSpecTypeDef::Udt(ScSpecTypeUdt {
        name: "NoSuchType".try_into().unwrap(),
    });
    let raw = json!(42);
    assert_eq!(spec.relabel_value(&raw, &ty), raw);
}
