//! The read layer: invoke a contract's **view functions** read-only and get a
//! typed result back — Soroban's answer to EVM `eth_call`.
//!
//! Soroban RPC's `simulateTransaction` can execute a contract invocation without
//! submitting it, returning the function's result. The friction is that you have
//! to hand-build a transaction envelope and encode/decode XDR. This module does
//! both, driven by the contract's on-chain spec (see [`crate::spec`]): given a
//! function name and JSON arguments, it type-checks and encodes the arguments
//! into `ScVal`s, wraps them in a simulation transaction, and hands back the
//! base64 XDR to simulate. Decoding the result reuses the event decoder.
//!
//! The network round-trip itself lives in the API service; everything here is
//! pure and unit-tested.

use std::str::FromStr;

use serde_json::Value;
use stellar_xdr::curr::{
    ContractEventBody, ContractEventType, DiagnosticEvent, HostFunction, Int128Parts,
    InvokeContractArgs, InvokeHostFunctionOp, Limited, Limits, Memo, MuxedAccount, Operation,
    OperationBody, Preconditions, PublicKey, ReadXdr, ScAddress, ScBytes, ScMap, ScMapEntry,
    ScSpecEntry, ScSpecFunctionV0, ScSpecTypeDef, ScString, ScSymbol, ScVal, ScVec, SequenceNumber,
    Transaction, TransactionEnvelope, TransactionExt, TransactionV1Envelope, UInt128Parts, Uint256,
    VecM, WriteXdr,
};

use crate::spec::type_name;
use crate::ContractSpec;

/// The canonical all-zero account, used as the (never-signed, never-charged)
/// source of a simulation transaction when the caller supplies none.
const ZERO_ACCOUNT: Uint256 = Uint256([0u8; 32]);

/// An error encoding a read call — all client-fixable (bad/missing args, wrong
/// type, unknown function), so the API maps these to `400`.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("contract has no function named {0:?}")]
    FunctionNotFound(String),
    #[error("missing argument {0:?}")]
    MissingArgument(String),
    #[error("argument {name:?}: {msg}")]
    BadArgument { name: String, msg: String },
    #[error("argument {name:?}: type {ty} is not yet supported by the read layer")]
    UnsupportedType { name: String, ty: String },
    #[error("could not build simulation transaction: {0}")]
    Build(String),
}

/// A ready-to-simulate call: the base64 transaction envelope plus the declared
/// return type, so the result can be labelled once decoded.
#[derive(Debug)]
pub struct EncodedCall {
    pub tx_xdr: String,
    pub output_type: String,
}

/// Encode a typed contract read into a simulation transaction.
///
/// `spec_section` is the raw `contractspecv0` XDR (as captured at index time).
/// `args` is either a JSON object keyed by parameter name, or a positional JSON
/// array. `source_account` is an optional `G…` account to use as the tx source
/// (defaults to the zero account, which simulation accepts for read-only calls).
pub fn encode_call(
    spec_section: &[u8],
    contract_id: &str,
    function: &str,
    args: &Value,
    source_account: Option<&str>,
) -> Result<EncodedCall, EncodeError> {
    let func = find_function(spec_section, function)
        .ok_or_else(|| EncodeError::FunctionNotFound(function.to_string()))?;

    let mut scvals: Vec<ScVal> = Vec::with_capacity(func.inputs.len());
    for (i, input) in func.inputs.iter().enumerate() {
        let name = input.name.to_utf8_string_lossy();
        let jv = match args {
            Value::Object(m) => m.get(&name),
            Value::Array(a) => a.get(i),
            _ => None,
        }
        .ok_or_else(|| EncodeError::MissingArgument(name.clone()))?;
        scvals.push(json_to_scval(jv, &input.type_, &name)?);
    }

    let output_type = func
        .outputs
        .first()
        .map(type_name)
        .unwrap_or_else(|| "void".to_string());

    let tx_xdr = build_read_tx(contract_id, function, scvals, source_account)
        .map_err(|e| EncodeError::Build(e.to_string()))?;
    Ok(EncodedCall {
        tx_xdr,
        output_type,
    })
}

/// List a contract's callable functions (name, typed inputs, output type),
/// derived from the raw spec section. Handy for a `/functions` endpoint.
pub fn functions(spec_section: &[u8]) -> Vec<Value> {
    parse_entries(spec_section)
        .into_iter()
        .filter_map(|e| match e {
            ScSpecEntry::FunctionV0(f) => Some(serde_json::json!({
                "name": f.name.to_utf8_string_lossy(),
                "inputs": f.inputs.iter().map(|i| serde_json::json!({
                    "name": i.name.to_utf8_string_lossy(),
                    "type": type_name(&i.type_),
                })).collect::<Vec<_>>(),
                "outputs": f.outputs.iter().map(type_name).collect::<Vec<_>>(),
            })),
            _ => None,
        })
        .collect()
}

fn parse_entries(spec_section: &[u8]) -> Vec<ScSpecEntry> {
    let mut limited = Limited::new(spec_section, Limits::none());
    ScSpecEntry::read_xdr_iter(&mut limited)
        .filter_map(Result::ok)
        .collect()
}

fn find_function(spec_section: &[u8], function: &str) -> Option<ScSpecFunctionV0> {
    parse_entries(spec_section)
        .into_iter()
        .find_map(|e| match e {
            ScSpecEntry::FunctionV0(f) if f.name.to_utf8_string_lossy() == function => Some(f),
            _ => None,
        })
}

/// Convert one JSON argument into an `ScVal` according to its declared type.
fn json_to_scval(v: &Value, ty: &ScSpecTypeDef, name: &str) -> Result<ScVal, EncodeError> {
    use ScSpecTypeDef as T;
    let bad = |msg: &str| EncodeError::BadArgument {
        name: name.to_string(),
        msg: msg.to_string(),
    };
    let unsupported = || EncodeError::UnsupportedType {
        name: name.to_string(),
        ty: type_name(ty),
    };

    Ok(match ty {
        T::Bool => ScVal::Bool(v.as_bool().ok_or_else(|| bad("expected a boolean"))?),
        T::U32 => ScVal::U32(int::<u32>(v, name)?),
        T::I32 => ScVal::I32(int::<i32>(v, name)?),
        T::U64 => ScVal::U64(int::<u64>(v, name)?),
        T::I64 => ScVal::I64(int::<i64>(v, name)?),
        T::Timepoint => ScVal::Timepoint(int::<u64>(v, name)?.into()),
        T::Duration => ScVal::Duration(int::<u64>(v, name)?.into()),
        T::U128 => ScVal::U128(u128_parts(int::<u128>(v, name)?)),
        T::I128 => ScVal::I128(i128_parts(int::<i128>(v, name)?)),
        T::Symbol => ScVal::Symbol(ScSymbol(
            str_of(v, name)?
                .try_into()
                .map_err(|_| bad("symbol too long or invalid"))?,
        )),
        T::String => ScVal::String(ScString(
            str_of(v, name)?
                .try_into()
                .map_err(|_| bad("string too long"))?,
        )),
        T::Address => ScVal::Address(
            ScAddress::from_str(str_of(v, name)?).map_err(|_| bad("invalid address strkey"))?,
        ),
        T::Bytes => ScVal::Bytes(ScBytes(
            decode_hex(v, name)?
                .try_into()
                .map_err(|_| bad("byte string too long"))?,
        )),
        T::BytesN(n) => {
            let bytes = decode_hex(v, name)?;
            if bytes.len() != n.n as usize {
                return Err(bad(&format!("expected {} bytes", n.n)));
            }
            ScVal::Bytes(ScBytes(
                bytes.try_into().map_err(|_| bad("byte string too long"))?,
            ))
        }
        T::Option(inner) => {
            if v.is_null() {
                ScVal::Void
            } else {
                json_to_scval(v, &inner.value_type, name)?
            }
        }
        T::Vec(inner) => {
            let arr = v.as_array().ok_or_else(|| bad("expected an array"))?;
            let items: Result<Vec<ScVal>, _> = arr
                .iter()
                .map(|el| json_to_scval(el, &inner.element_type, name))
                .collect();
            ScVal::Vec(Some(ScVec(vecm(items?, name)?)))
        }
        T::Tuple(t) => {
            let arr = v.as_array().ok_or_else(|| bad("expected a tuple array"))?;
            if arr.len() != t.value_types.len() {
                return Err(bad(&format!(
                    "expected {} tuple elements",
                    t.value_types.len()
                )));
            }
            let items: Result<Vec<ScVal>, _> = arr
                .iter()
                .zip(t.value_types.iter())
                .map(|(el, et)| json_to_scval(el, et, name))
                .collect();
            ScVal::Vec(Some(ScVec(vecm(items?, name)?)))
        }
        T::Map(m) => {
            // Only symbol/string-keyed maps map cleanly from a JSON object.
            let obj = v.as_object().ok_or_else(|| bad("expected an object"))?;
            let mut entries = Vec::with_capacity(obj.len());
            for (k, val) in obj {
                let key = json_to_scval(&Value::String(k.clone()), &m.key_type, name)?;
                let val = json_to_scval(val, &m.value_type, name)?;
                entries.push(ScMapEntry { key, val });
            }
            ScVal::Map(Some(ScMap(vecm(entries, name)?)))
        }
        // Big integers and user-defined types need more context than the first
        // cut of the read layer carries; surfaced as a clear client error.
        T::U256
        | T::I256
        | T::Udt(_)
        | T::Val
        | T::Result(_)
        | T::Error
        | T::Void
        | T::MuxedAddress => {
            return Err(unsupported());
        }
    })
}

fn build_read_tx(
    contract_id: &str,
    function: &str,
    args: Vec<ScVal>,
    source_account: Option<&str>,
) -> Result<String, stellar_xdr::curr::Error> {
    let source = match source_account {
        Some(g) => match PublicKey::from_str(g)? {
            PublicKey::PublicKeyTypeEd25519(k) => MuxedAccount::Ed25519(k),
        },
        None => MuxedAccount::Ed25519(ZERO_ACCOUNT),
    };

    let invoke = InvokeContractArgs {
        contract_address: ScAddress::from_str(contract_id)?,
        function_name: ScSymbol(function.try_into()?),
        args: args.try_into()?,
    };
    let op = Operation {
        source_account: None,
        body: OperationBody::InvokeHostFunction(InvokeHostFunctionOp {
            host_function: HostFunction::InvokeContract(invoke),
            auth: VecM::default(),
        }),
    };
    let tx = Transaction {
        source_account: source,
        fee: 0,
        seq_num: SequenceNumber(0),
        cond: Preconditions::None,
        memo: Memo::None,
        operations: vec![op].try_into()?,
        // V0: no Soroban footprint attached — RPC's preflight computes it.
        ext: TransactionExt::V0,
    };
    let envelope = TransactionEnvelope::Tx(TransactionV1Envelope {
        tx,
        signatures: VecM::default(),
    });
    envelope.to_xdr_base64(Limits::none())
}

/// Decode a simulation's base64 `ScVal` result and label it with its type.
pub fn decode_result(result_xdr: &str, output_type: &str) -> Value {
    serde_json::json!({
        "type": output_type,
        "value": crate::xdr::decode_scval_base64(result_xdr),
    })
}

/// Decode the events a simulation reported. The low-level diagnostic trace
/// (`fn_call`/`fn_return`) is filtered out; the remaining contract and system
/// events are decoded like indexed events, and those emitted by
/// `target_contract` are enriched with its spec when one is supplied. This is
/// what makes `POST /contracts/:id/simulate` show "the events this call emits".
pub fn decode_events(
    events_xdr: &[String],
    target_contract: &str,
    spec: Option<&ContractSpec>,
) -> Vec<Value> {
    let mut out = Vec::new();
    for b64 in events_xdr {
        let Ok(diag) = DiagnosticEvent::from_xdr_base64(b64, Limits::none()) else {
            continue;
        };
        let event = diag.event;
        // Skip the fn_call/fn_return diagnostic trace — keep meaningful events.
        let kind = match event.type_ {
            ContractEventType::Contract => "contract",
            ContractEventType::System => "system",
            ContractEventType::Diagnostic => continue,
        };
        let ContractEventBody::V0(body) = event.body;
        let contract_id = event
            .contract_id
            .map(|c| ScAddress::Contract(c).to_string());
        let topics: Vec<Value> = body.topics.iter().map(scval_to_json).collect();
        let data = scval_to_json(&body.data);
        let event_name = topics.first().and_then(|t| t.as_str().map(String::from));

        // Enrich events emitted by the contract we're simulating.
        let enriched = match (spec, &event_name, &contract_id) {
            (Some(spec), Some(name), Some(cid)) if cid == target_contract => {
                spec.enrich_event(name, &topics, &data)
            }
            _ => None,
        };

        out.push(serde_json::json!({
            "contract_id": contract_id,
            "type": kind,
            "event": event_name,
            "topics": topics,
            "data": data,
            "enriched": enriched,
        }));
    }
    out
}

/// Decode an already-parsed `ScVal` to JSON by re-encoding it through the same
/// decoder events use (keeps one JSON shape across the whole system).
fn scval_to_json(sv: &ScVal) -> Value {
    match sv.to_xdr_base64(Limits::none()) {
        Ok(b64) => crate::xdr::decode_scval_base64(&b64),
        Err(_) => Value::Null,
    }
}

// ---- small helpers ----

fn str_of<'a>(v: &'a Value, name: &str) -> Result<&'a str, EncodeError> {
    v.as_str().ok_or_else(|| EncodeError::BadArgument {
        name: name.to_string(),
        msg: "expected a string".to_string(),
    })
}

/// Parse an integer argument from a JSON string or integral number, into any
/// integer type. Strings are required for values that exceed a JS-safe number.
fn int<T: FromStr>(v: &Value, name: &str) -> Result<T, EncodeError> {
    let s = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),
        _ => {
            return Err(EncodeError::BadArgument {
                name: name.to_string(),
                msg: "expected an integer (as a number or decimal string)".to_string(),
            })
        }
    };
    s.parse::<T>().map_err(|_| EncodeError::BadArgument {
        name: name.to_string(),
        msg: "integer out of range for this type".to_string(),
    })
}

fn decode_hex(v: &Value, name: &str) -> Result<Vec<u8>, EncodeError> {
    let s = str_of(v, name)?
        .strip_prefix("0x")
        .unwrap_or(str_of(v, name)?);
    hex::decode(s).map_err(|_| EncodeError::BadArgument {
        name: name.to_string(),
        msg: "expected a hex string".to_string(),
    })
}

fn vecm<U>(items: Vec<U>, name: &str) -> Result<VecM<U>, EncodeError> {
    items.try_into().map_err(|_| EncodeError::BadArgument {
        name: name.to_string(),
        msg: "too many elements".to_string(),
    })
}

fn i128_parts(n: i128) -> Int128Parts {
    let b = n.to_be_bytes();
    Int128Parts {
        hi: i64::from_be_bytes(b[0..8].try_into().unwrap()),
        lo: u64::from_be_bytes(b[8..16].try_into().unwrap()),
    }
}

fn u128_parts(n: u128) -> UInt128Parts {
    UInt128Parts {
        hi: (n >> 64) as u64,
        lo: n as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::{
        ScSpecFunctionInputV0, ScSpecFunctionV0, ScSpecTypeDef, ScSymbol, WriteXdr,
    };

    // balance(id: Address) -> i128
    fn balance_spec() -> Vec<u8> {
        let entry = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
            doc: "".try_into().unwrap(),
            name: ScSymbol("balance".try_into().unwrap()),
            inputs: vec![ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: "id".try_into().unwrap(),
                type_: ScSpecTypeDef::Address,
            }]
            .try_into()
            .unwrap(),
            outputs: vec![ScSpecTypeDef::I128].try_into().unwrap(),
        });
        entry.to_xdr(Limits::none()).unwrap()
    }

    const G: &str = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF";
    const C: &str = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4";

    #[test]
    fn encodes_a_typed_call_and_round_trips() {
        let spec = balance_spec();
        let args = serde_json::json!({ "id": G });
        let call = encode_call(&spec, C, "balance", &args, None).expect("should encode");
        assert_eq!(call.output_type, "i128");

        // Decode the envelope back and check the invocation we built.
        let env = TransactionEnvelope::from_xdr_base64(&call.tx_xdr, Limits::none()).unwrap();
        let TransactionEnvelope::Tx(v1) = env else {
            panic!("expected v1 envelope")
        };
        let OperationBody::InvokeHostFunction(op) = &v1.tx.operations[0].body else {
            panic!("expected invoke host function")
        };
        let HostFunction::InvokeContract(ic) = &op.host_function else {
            panic!("expected invoke contract")
        };
        assert_eq!(ic.function_name.to_utf8_string_lossy(), "balance");
        assert_eq!(ic.args.len(), 1);
        assert!(matches!(ic.args[0], ScVal::Address(_)));
    }

    #[test]
    fn positional_args_work_too() {
        let spec = balance_spec();
        let call = encode_call(&spec, C, "balance", &serde_json::json!([G]), None);
        assert!(call.is_ok());
    }

    #[test]
    fn unknown_function_is_an_error() {
        let spec = balance_spec();
        let err = encode_call(&spec, C, "nope", &serde_json::json!({}), None).unwrap_err();
        assert!(matches!(err, EncodeError::FunctionNotFound(_)));
    }

    #[test]
    fn missing_argument_is_an_error() {
        let spec = balance_spec();
        let err = encode_call(&spec, C, "balance", &serde_json::json!({}), None).unwrap_err();
        assert!(matches!(err, EncodeError::MissingArgument(_)));
    }

    #[test]
    fn wrong_argument_type_is_an_error() {
        let spec = balance_spec();
        // an address arg given a non-strkey string
        let err = encode_call(
            &spec,
            C,
            "balance",
            &serde_json::json!({ "id": "nope" }),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, EncodeError::BadArgument { .. }));
    }

    #[test]
    fn i128_splits_into_correct_parts() {
        // (hi << 64) | lo == value
        let p = i128_parts(105_000_000);
        assert_eq!(((p.hi as i128) << 64) | (p.lo as i128), 105_000_000);
        let neg = i128_parts(-5);
        assert_eq!(((neg.hi as i128) << 64) | (neg.lo as i128), -5);
    }

    #[test]
    fn functions_lists_the_interface() {
        let fns = functions(&balance_spec());
        assert_eq!(fns.len(), 1);
        assert_eq!(fns[0]["name"], "balance");
        assert_eq!(fns[0]["inputs"][0]["type"], "Address");
        assert_eq!(fns[0]["outputs"][0], "i128");
    }

    #[test]
    fn decodes_simulation_events_and_skips_diagnostics() {
        use stellar_xdr::curr::{
            ContractEvent, ContractEventBody, ContractEventType, ContractEventV0, ContractId,
            DiagnosticEvent, ExtensionPoint, Hash,
        };
        let sym = |s: &str| ScVal::Symbol(ScSymbol(s.try_into().unwrap()));
        let mk = |ty: ContractEventType, topic0: &str| {
            let ev = ContractEvent {
                ext: ExtensionPoint::V0,
                contract_id: Some(ContractId(Hash([7u8; 32]))),
                type_: ty,
                body: ContractEventBody::V0(ContractEventV0 {
                    topics: vec![sym(topic0)].try_into().unwrap(),
                    data: ScVal::I128(i128_parts(42)),
                }),
            };
            DiagnosticEvent {
                in_successful_contract_call: true,
                event: ev,
            }
            .to_xdr_base64(Limits::none())
            .unwrap()
        };

        let events = vec![
            mk(ContractEventType::Contract, "transfer"),
            mk(ContractEventType::Diagnostic, "fn_call"),
        ];
        let decoded = decode_events(&events, "CUNKNOWN", None);
        // The diagnostic trace event is filtered out.
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0]["type"], "contract");
        assert_eq!(decoded[0]["event"], "transfer");
        assert_eq!(decoded[0]["data"], "42");
        assert!(decoded[0]["contract_id"].as_str().unwrap().starts_with('C'));
    }
}
