//! Live end-to-end check of the read layer against Soroban testnet.
//! Ignored by default (needs network + `curl`); run with:
//!   cargo test -p lumenqraph-core --test live_simulate -- --ignored --nocapture

use std::process::Command;

use lumenqraph_core::read::{decode_result, encode_call};
use serde_json::{json, Value};
use stellar_xdr::curr::{Limits, ScSpecEntry, ScSpecFunctionV0, ScSpecTypeDef, ScSymbol, WriteXdr};

const RPC: &str = "https://soroban-testnet.stellar.org";
// Testnet native (XLM) Stellar Asset Contract — implements SEP-41 `decimals()`.
const NATIVE_SAC: &str = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";

fn decimals_spec() -> Vec<u8> {
    let entry = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
        doc: "".try_into().unwrap(),
        name: ScSymbol("decimals".try_into().unwrap()),
        inputs: vec![].try_into().unwrap(),
        outputs: vec![ScSpecTypeDef::U32].try_into().unwrap(),
    });
    entry.to_xdr(Limits::none()).unwrap()
}

#[test]
#[ignore = "network"]
fn simulate_decimals_on_testnet() {
    let call = encode_call(&decimals_spec(), NATIVE_SAC, "decimals", &json!({}), None)
        .expect("encode call");

    let req = json!({
        "jsonrpc": "2.0", "id": 1, "method": "simulateTransaction",
        "params": { "transaction": call.tx_xdr }
    });
    let out = Command::new("curl")
        .args([
            "-s",
            "-X",
            "POST",
            RPC,
            "-H",
            "Content-Type: application/json",
            "-d",
        ])
        .arg(req.to_string())
        .output()
        .expect("curl");
    let resp: Value = serde_json::from_slice(&out.stdout).expect("json");
    println!("simulate response: {resp}");

    let result = &resp["result"];
    assert!(
        result["error"].is_null(),
        "simulate returned an error: {}",
        result["error"]
    );
    let xdr = result["results"][0]["xdr"].as_str().expect("result xdr");

    let decoded = decode_result(xdr, &call, None);
    println!("decoded decimals(): {decoded}");
    // Native SAC reports 7 decimals; assert we decoded a concrete number.
    assert_eq!(decoded["type"], "u32");
    assert!(decoded["value"].is_number());
}
