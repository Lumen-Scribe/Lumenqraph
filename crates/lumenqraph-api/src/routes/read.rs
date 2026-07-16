//! The read layer — Soroban's answer to `eth_call`, and transaction preview.
//!
//! `GET  /contracts/:id/functions` — the contract's callable functions + types.
//! `POST /contracts/:id/call`       — invoke a view function read-only (via RPC
//!                                    `simulateTransaction`) and get a typed result.
//! `POST /contracts/:id/simulate`   — dry-run *any* call and get the typed result
//!                                    plus the events it would emit and its cost.
//!
//! Argument encoding is driven by the contract's on-chain spec (captured at
//! index time), so calls are type-checked before they ever hit the network.

use axum::extract::{Path, State};
use axum::Json;
use lumenqraph_core::read::{self, EncodeError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use crate::rpc::SimOutcome;
use crate::state::AppState;

pub async fn list_functions(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let spec = state.specs.current(&state.pool, &contract_id).await?;
    Ok(Json(json!({
        "contract_id": contract_id,
        "functions": read::functions(&spec.section),
    })))
}

#[derive(Deserialize)]
pub struct CallRequest {
    /// Function to invoke.
    function: String,
    /// Arguments: a JSON object keyed by parameter name, or a positional array.
    #[serde(default)]
    args: Value,
    /// Optional `G…` source account for the simulation (defaults to the zero
    /// account, which read-only simulation accepts).
    #[serde(default)]
    source_account: Option<String>,
}

pub async fn call_function(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(req): Json<CallRequest>,
) -> ApiResult<Json<Value>> {
    let spec = state.specs.current(&state.pool, &contract_id).await?;

    let call = read::encode_call(
        &spec.section,
        &contract_id,
        &req.function,
        &req.args,
        req.source_account.as_deref(),
    )
    .map_err(encode_error_to_api)?;

    match state.rpc.simulate(&call.tx_xdr).await? {
        SimOutcome::Ok {
            result_xdr,
            latest_ledger,
            ..
        } => Ok(Json(json!({
            "contract_id": contract_id,
            "function": req.function,
            "result": read::decode_result(&result_xdr, &call, spec.parsed.as_ref()),
            "simulated_at_ledger": latest_ledger,
        }))),
        // A trap / bad call is the caller's problem, not a 500.
        SimOutcome::Error(msg) => Err(ApiError::bad_request(format!("simulation failed: {msg}"))),
    }
}

/// `POST /contracts/:id/simulate` — dry-run any call (including state-changing
/// ones like `transfer`) and return the typed result, the events it would emit
/// (decoded + enriched), and its estimated resource fee — nothing is signed or
/// submitted. Soroban's answer to Tenderly's transaction preview.
pub async fn simulate_call(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(req): Json<CallRequest>,
) -> ApiResult<Json<Value>> {
    let spec = state.specs.current(&state.pool, &contract_id).await?;

    let call = read::encode_call(
        &spec.section,
        &contract_id,
        &req.function,
        &req.args,
        req.source_account.as_deref(),
    )
    .map_err(encode_error_to_api)?;

    match state.rpc.simulate(&call.tx_xdr).await? {
        SimOutcome::Ok {
            result_xdr,
            events,
            min_resource_fee,
            latest_ledger,
        } => {
            // Enrich emitted events from this contract with its interface.
            let decoded_events = read::decode_events(&events, &contract_id, spec.parsed.as_ref());
            Ok(Json(json!({
                "contract_id": contract_id,
                "function": req.function,
                "result": read::decode_result(&result_xdr, &call, spec.parsed.as_ref()),
                "events": decoded_events,
                "min_resource_fee": min_resource_fee,
                "simulated_at_ledger": latest_ledger,
            })))
        }
        SimOutcome::Error(msg) => Err(ApiError::bad_request(format!("simulation failed: {msg}"))),
    }
}

/// All `EncodeError`s are client-fixable, so they map to `400`.
fn encode_error_to_api(e: EncodeError) -> ApiError {
    ApiError::bad_request(e.to_string())
}
