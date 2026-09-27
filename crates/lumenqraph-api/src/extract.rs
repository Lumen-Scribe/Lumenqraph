//! Custom request extractors shared across route handlers.

use std::collections::HashMap;

use axum::async_trait;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;

use crate::error::ApiError;

/// The `:contract_id` path parameter, guaranteed to be a well-formed `C…`
/// contract strkey (#440).
///
/// Every route with a `:contract_id` segment takes this instead of
/// `Path<String>`, so a malformed ID is rejected with
/// `400 invalid_contract_id` before it reaches the database, RPC, logs, or an
/// SSE polling loop — and the check can't be forgotten on a new route.
#[derive(Debug, Clone)]
pub struct ValidContractId(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for ValidContractId
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // A map rather than `Path<String>` so this works on routes with more
        // than one path parameter (e.g. `/contracts/:contract_id/data/:key_hash`).
        let Path(params) = Path::<HashMap<String, String>>::from_request_parts(parts, state)
            .await
            .map_err(|_| ApiError::invalid_contract_id())?;
        let contract_id = params
            .get("contract_id")
            .ok_or_else(ApiError::invalid_contract_id)?;
        if !lumenqraph_core::is_valid_contract_id(contract_id) {
            return Err(ApiError::invalid_contract_id());
        }
        Ok(ValidContractId(contract_id.clone()))
    }
}
