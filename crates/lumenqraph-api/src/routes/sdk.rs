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
    /// Target language: `ts` (default), `python`, or `rust`.
    lang: Option<String>,
    /// Generate from a historical interface version instead of the current one.
    version: Option<i32>,
}

/// The supported codegen languages, together with their content-type and file
/// extension.
enum Lang {
    TypeScript,
    Python,
    Rust,
}

impl Lang {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "ts" | "typescript" => Some(Self::TypeScript),
            "python" | "py" => Some(Self::Python),
            "rust" | "rs" => Some(Self::Rust),
            _ => None,
        }
    }

    fn content_type(&self) -> &'static str {
        match self {
            Self::TypeScript => "text/typescript; charset=utf-8",
            Self::Python => "text/x-python; charset=utf-8",
            Self::Rust => "text/x-rust; charset=utf-8",
        }
    }

    fn extension(&self) -> &'static str {
        match self {
            Self::TypeScript => "ts",
            Self::Python => "py",
            Self::Rust => "rs",
        }
    }
}

pub async fn contract_sdk(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<SdkQuery>,
) -> ApiResult<Response> {
    let lang_str = q.lang.as_deref().unwrap_or("ts");
    let lang = Lang::from_str(lang_str).ok_or_else(|| {
        ApiError::bad_request(format!(
            "unsupported lang {lang_str:?}; supported: ts, python, rust"
        ))
    })?;

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

    let code = match lang {
        Lang::TypeScript => codegen::typescript_client(&contract_id, parsed),
        Lang::Python => codegen::python_client(&contract_id, parsed),
        Lang::Rust => codegen::rust_client(&contract_id, parsed),
    };

    let ext = lang.extension();
    let filename = match q.version {
        Some(v) => format!("{contract_id}.v{v}.{ext}"),
        None => format!("{contract_id}.{ext}"),
    };
    Ok((
        [
            (
                header::CONTENT_TYPE,
                lang.content_type().to_string(),
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
