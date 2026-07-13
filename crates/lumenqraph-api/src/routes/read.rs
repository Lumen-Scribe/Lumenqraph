//! The read layer — Soroban's answer to `eth_call`.
//!
//! `GET  /contracts/:id/functions` — the contract's callable functions + types.
//! `POST /contracts/:id/call`       — invoke a view function read-only (via RPC
//!                                    `simulateTransaction`) and get a typed result.
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

/// Load a contract's stored raw spec section (hex). `None` if not indexed yet.
async fn load_spec_section(state: &AppState, contract_id: &str) -> ApiResult<Vec<u8>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT spec_section FROM contract_specs WHERE contract_id = $1")
            .bind(contract_id)
            .fetch_optional(&state.pool)
            .await?;
    let hex_section = row.map(|r| r.0).filter(|s| !s.is_empty()).ok_or_else(|| {
        ApiError::not_found(
            "no interface indexed for this contract yet (the indexer fetches it \
             on first sighting; Stellar Asset Contracts have no callable spec)",
        )
    })?;
    hex::decode(&hex_section).map_err(|_| ApiError::from(anyhow::anyhow!("corrupt stored spec")))
}

pub async fn list_functions(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let section = load_spec_section(&state, &contract_id).await?;
    Ok(Json(json!({
        "contract_id": contract_id,
        "functions": read::functions(&section),
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
    let section = load_spec_section(&state, &contract_id).await?;

    let call = read::encode_call(
        &section,
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
        } => Ok(Json(json!({
            "contract_id": contract_id,
            "function": req.function,
            "result": read::decode_result(&result_xdr, &call.output_type),
            "simulated_at_ledger": latest_ledger,
        }))),
        // A trap / bad call is the caller's problem, not a 500.
        SimOutcome::Error(msg) => Err(ApiError::bad_request(format!("simulation failed: {msg}"))),
    }
}

/// All `EncodeError`s are client-fixable, so they map to `400`.
fn encode_error_to_api(e: EncodeError) -> ApiError {
    ApiError::bad_request(e.to_string())
}
