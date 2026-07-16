//! `GET /contracts/:id/sdk` — a generated, typed client for the contract.
//!
//! The client is generated on demand from the contract's on-chain interface
//! (see `lumenqraph_core::codegen`), so it is always in sync with what is
//! actually deployed — and `?version=N` generates from a historical interface
//! version, i.e. "the client your integration was built against before the
//! upgrade".

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use lumenqraph_core::codegen;
use serde::Deserialize;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SdkQuery {
    /// Target language; only TypeScript (`ts`) so far.
    lang: Option<String>,
    /// Generate from a historical interface version instead of the current one.
    version: Option<i32>,
}

pub async fn contract_sdk(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<SdkQuery>,
) -> ApiResult<Response> {
    match q.lang.as_deref().unwrap_or("ts") {
        "ts" | "typescript" => {}
        other => {
            return Err(ApiError::bad_request(format!(
                "unsupported lang {other:?}; supported: ts"
            )))
        }
    }

    let spec = match q.version {
        Some(v) => state.specs.at_version(&state.pool, &contract_id, v).await?,
        None => state.specs.current(&state.pool, &contract_id).await?,
    };
    let Some(parsed) = spec.parsed.as_ref() else {
        // We stored a section we can't parse — our bug, not the caller's.
        return Err(ApiError::Internal(anyhow::anyhow!(
            "stored spec section could not be parsed"
        )));
    };

    let code = codegen::typescript_client(&contract_id, parsed);
    let filename = match q.version {
        Some(v) => format!("{contract_id}.v{v}.ts"),
        None => format!("{contract_id}.ts"),
    };
    Ok((
        [
            (
                header::CONTENT_TYPE,
                "text/typescript; charset=utf-8".to_string(),
            ),
            (
                header::CONTENT_DISPOSITION,
                format!("inline; filename=\"{filename}\""),
            ),
        ],
        code,
    )
        .into_response())
}
