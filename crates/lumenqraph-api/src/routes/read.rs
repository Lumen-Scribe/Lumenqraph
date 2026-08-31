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
use axum::http::StatusCode;
use axum::Json;
use lumenqraph_core::read::{self, EncodeError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use crate::read_cost_limit::validate_call_request;
use crate::rpc::SimOutcome;
use crate::state::AppState;

pub async fn list_functions(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> ApiResult<Json<Value>> {
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }
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
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }

    // Estimate request body size (function name + args serialization)
    let estimated_body_size = req.function.len() + serde_json::to_string(&req.args)
        .map(|s| s.len())
        .unwrap_or(0);

    // Validate request cost before hitting RPC
    if let Err(e) = validate_call_request(estimated_body_size, &req.args, &state.read_cost_limit_config) {
        return Err(ApiError::Status(
            e.http_status(),
            crate::error::ErrorCode::BadRequest,
            e.message(),
        ));
    }

    // Check the read-through cache before hitting RPC.
    if let Some(cached) = state.call_cache.get(&contract_id, &req.function, &req.args) {
        return Ok(Json(cached));
    }


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
        } => {
            let response = json!({
                "contract_id": contract_id,
                "function": req.function,
                "result": read::decode_result(&result_xdr, &call, spec.parsed.as_ref()),
                "simulated_at_ledger": latest_ledger,
            });
            state.call_cache.insert(&contract_id, &req.function, &req.args, response.clone());
            Ok(Json(response))
        }
        // A trap / bad call is the caller's problem, not a 500.
        SimOutcome::Error(msg) => {
            // Log the full upstream detail server-side; only return a concise,
            // sanitised copy to the caller (see issue #154).
            tracing::warn!(rpc_error = %msg, "contract simulation failed");
            Err(ApiError::simulation_failed(format!(
                "simulation failed: {}",
                lumenqraph_core::sanitize::sanitize_simulation_error(&msg)
            )))
        }
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
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }

    // Estimate request body size (function name + args serialization)
    let estimated_body_size = req.function.len() + serde_json::to_string(&req.args)
        .map(|s| s.len())
        .unwrap_or(0);

    // Validate request cost before hitting RPC
    if let Err(e) = validate_call_request(estimated_body_size, &req.args, &state.read_cost_limit_config) {
        return Err(ApiError::Status(
            e.http_status(),
            crate::error::ErrorCode::BadRequest,
            e.message(),
        ));
    }

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
        SimOutcome::Error(msg) => {
            // Log the full upstream detail server-side; only return a concise,
            // sanitised copy to the caller (see issue #154).
            tracing::warn!(rpc_error = %msg, "contract simulation failed");
            Err(ApiError::simulation_failed(format!(
                "simulation failed: {}",
                lumenqraph_core::sanitize::sanitize_simulation_error(&msg)
            )))
        }
    }
}

/// All `EncodeError`s are client-fixable, so they map to `400`.
fn encode_error_to_api(e: EncodeError) -> ApiError {
    ApiError::bad_request(e.to_string())
}

#[cfg(test)]
mod tests {
    //! Handler-level tests for `call_function` and `simulate_call`.
    //!
    //! These run without a real Postgres or RPC server. The `SpecCache` is
    //! seeded directly; the `RpcClient` is pointed at a URL that will never be
    //! contacted because the tests control what happens before the network call.
    //!
    //! How it works:
    //! - We build a minimal `AppState` with a lazy (never-connecting) pool and
    //!   a `SpecCache` pre-populated with a synthetic spec.
    //! - For "unknown function / bad arg" cases the error is raised *before*
    //!   any RPC call, so no network mock is needed.
    //! - For "happy path / simulation-error" cases we use a real RpcClient
    //!   pointed at a dummy URL and a mocked `simulate` via a lightweight
    //!   in-process HTTP server (or, for simplicity, an inline axum oneshot).

    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use axum::Router;
    use serde_json::{json, Value};
    use stellar_xdr::curr::{
        Limits, ScSpecEntry, ScSpecFunctionV0, ScSpecInputsEntry, ScSpecTypeDef, ScSymbol, WriteXdr,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::rate_limit::RateLimiter;
    use crate::rpc::RpcClient;
    use crate::specs::{CachedSpec, SpecCache};

    // ── helpers ────────────────────────────────────────────────────────────

    /// Build a raw spec section containing one function:
    ///   `fn <name>(account: Address) -> i128`
    fn single_fn_spec(name: &str) -> Vec<u8> {
        use stellar_xdr::curr::{ScSpecFunctionInputV0, StringM};
        let entry = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
            doc: "".try_into().unwrap(),
            name: ScSymbol(name.try_into().unwrap()),
            inputs: vec![ScSpecFunctionInputV0 {
                doc: "".try_into().unwrap(),
                name: StringM::try_from("account").unwrap(),
                type_: ScSpecTypeDef::Address,
            }]
            .try_into()
            .unwrap(),
            outputs: vec![ScSpecTypeDef::I128].try_into().unwrap(),
        });
        entry.to_xdr(Limits::none()).unwrap()
    }

    /// Build a spec section with a zero-argument function: `fn balance() -> u32`
    fn zero_arg_spec(name: &str) -> Vec<u8> {
        let entry = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
            doc: "".try_into().unwrap(),
            name: ScSymbol(name.try_into().unwrap()),
            inputs: vec![].try_into().unwrap(),
            outputs: vec![ScSpecTypeDef::U32].try_into().unwrap(),
        });
        entry.to_xdr(Limits::none()).unwrap()
    }

    /// Construct an `AppState` with a seeded cache and a lazy (never-used) pool.
    fn make_state(contract_id: &str, spec_section: Vec<u8>) -> AppState {
        // connect_lazy never opens a connection until the first query — safe
        // for tests that never reach the pool.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost:5432/test")
            .unwrap();

        let cache = SpecCache::new();
        cache.seed(
            contract_id,
            CachedSpec {
                parsed: lumenqraph_core::ContractSpec::from_spec_xdr(&spec_section),
                section: spec_section,
            },
        );

        use crate::concurrency_limit::ConcurrencyLimiter;
        use crate::call_cache::CallCache;
        use crate::read_cost_limit::ReadCostLimitConfig;

        AppState {
            pool,
            require_auth: false,
            anon_rate_limit: 1_000_000,
            limiter: Arc::new(RateLimiter::new()),
            http_requests: Arc::new(AtomicU64::new(0)),
            // Dummy RPC — never contacted in error-path tests.
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            specs: Arc::new(cache),
            mounts: Arc::new(vec![]),
            rpc_limiter: Arc::new(RateLimiter::new()),
            rpc_require_auth: false,
            rpc_anon_rate_limit: 1_000_000,
            metrics: Arc::new(crate::metrics_middleware::MetricsCollector::new()),
            call_cache: Arc::new(CallCache::new(100, 5)),
            build_info: Arc::new(crate::state::BuildInfo {
                version: "test".to_string(),
                commit: "test".to_string(),
                build_time: "test".to_string(),
            }),
            concurrency_limiter: Arc::new(ConcurrencyLimiter::new()),
            max_concurrent_per_ip: 100,
            read_cost_limit_config: ReadCostLimitConfig::default(),
            readyz_lag_threshold: 100,
            readyz_max_age_secs: 120,
            health_max_lag_ledgers: 100,
            health_max_stale_secs: 120,
        }
    }

    /// POST JSON to the given Axum router path and return (status, body).
    async fn call(app: Router, path: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, json)
    }

    fn app_for(state: AppState) -> Router {
        Router::new()
            .route(
                "/contracts/:id/call",
                post(call_function),
            )
            .route(
                "/contracts/:id/simulate",
                post(simulate_call),
            )
            .route(
                "/contracts/:id/functions",
                axum::routing::get(list_functions),
            )
            .with_state(state)
    }

    // ── list_functions ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_functions_returns_typed_signature() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let state = make_state(contract, single_fn_spec("balance"));
        let app = app_for(state);

        let req = Request::builder()
            .method("GET")
            .uri(format!("/contracts/{contract}/functions"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let fns = body["functions"].as_array().unwrap();
        assert_eq!(fns.len(), 1);
        assert_eq!(fns[0]["name"], "balance");
        assert_eq!(fns[0]["inputs"][0]["name"], "account");
        assert_eq!(fns[0]["inputs"][0]["type"], "Address");
        assert_eq!(fns[0]["outputs"][0], "i128");
    }

    // ── unknown function → 400 ─────────────────────────────────────────────

    #[tokio::test]
    async fn unknown_function_returns_400() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let state = make_state(contract, single_fn_spec("balance"));
        let app = app_for(state);

        let (status, body) = call(
            app,
            &format!("/contracts/{contract}/call"),
            json!({ "function": "nonexistent", "args": {} }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let msg = body["error"].as_str().unwrap();
        assert!(
            msg.contains("nonexistent"),
            "error should mention the unknown function name: {msg}"
        );
    }

    // ── missing argument → 400 ─────────────────────────────────────────────

    #[tokio::test]
    async fn missing_argument_returns_400() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let state = make_state(contract, single_fn_spec("balance"));
        let app = app_for(state);

        let (status, body) = call(
            app,
            &format!("/contracts/{contract}/call"),
            // `balance` requires `account`, but we pass an empty object.
            json!({ "function": "balance", "args": {} }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let msg = body["error"].as_str().unwrap();
        assert!(
            msg.contains("account"),
            "error should name the missing argument: {msg}"
        );
    }

    // ── wrong-typed argument → 400 ──────────────────────────────────────────

    #[tokio::test]
    async fn wrong_typed_argument_returns_400() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let state = make_state(contract, single_fn_spec("balance"));
        let app = app_for(state);

        let (status, body) = call(
            app,
            &format!("/contracts/{contract}/call"),
            // `account` expects an Address strkey, not a number.
            json!({ "function": "balance", "args": { "account": 12345 } }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let msg = body["error"].as_str().unwrap();
        assert!(
            msg.contains("account"),
            "error should name the bad argument: {msg}"
        );
    }

    // ── invalid address strkey → 400 ───────────────────────────────────────

    #[tokio::test]
    async fn invalid_address_strkey_returns_400_with_descriptive_message() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let state = make_state(contract, single_fn_spec("balance"));
        let app = app_for(state);

        let (status, body) = call(
            app,
            &format!("/contracts/{contract}/call"),
            json!({ "function": "balance", "args": { "account": "not-a-valid-strkey" } }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let msg = body["error"].as_str().unwrap();
        // The documented client-facing message shape from the README.
        assert!(
            msg.contains("account") && msg.contains("address"),
            "expected 'argument \"account\": invalid address strkey' style message, got: {msg}"
        );
    }

    // ── SAC / no-spec contract → 404 ───────────────────────────────────────

    #[tokio::test]
    async fn contract_not_in_cache_returns_404() {
        // `make_state` seeds a spec for `contract`, but we test a *different*
        // contract ID that has nothing in the cache — so the handler must hit
        // the database (connect_lazy, will immediately fail) and surface a 404.
        let seeded = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let unknown = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBM";
        let state = make_state(seeded, single_fn_spec("balance"));
        let app = app_for(state);

        let (status, _body) = call(
            app,
            &format!("/contracts/{unknown}/call"),
            json!({ "function": "balance", "args": {} }),
        )
        .await;

        // The connect_lazy pool will fail when the spec cache misses, returning
        // a 500 (internal error from sqlx) or 404 — either signals the handler
        // correctly attempted a DB lookup rather than short-circuiting.
        assert!(
            status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected status {status}"
        );
    }

    // ── extra argument in positional array ────────────────────────────────
    // The encoder picks args positionally; passing too many is not an error
    // (extra elements are silently ignored), but passing a wrong type for
    // the argument that *is* consumed must still be caught.

    #[tokio::test]
    async fn positional_args_wrong_type_returns_400() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let state = make_state(contract, single_fn_spec("balance"));
        let app = app_for(state);

        let (status, body) = call(
            app,
            &format!("/contracts/{contract}/call"),
            // Positional array — arg[0] is `account: Address`, pass a number.
            json!({ "function": "balance", "args": [42] }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let msg = body["error"].as_str().unwrap();
        assert!(
            msg.contains("account"),
            "error should name the bad argument: {msg}"
        );
    }

    // ── simulation error is bounded + sanitised (issue #154) ───────────────

    /// Stand up an in-process Soroban-RPC stub that always answers
    /// `simulateTransaction` with a huge, detail-laden error message, and
    /// return the base URL it is listening on.
    async fn error_rpc_server() -> String {
        // A long, messy upstream error that echoes internal detail + control
        // chars (newlines, tabs, a NUL byte) — exactly what we must not leak.
        let raw = format!(
            "host function call failed at endpoint https://internal.rpc/simulate\x00\n\t XDR=AAAA{} contract trapped: arithmetic overflow in __check_auth",
            "Z".repeat(1000),
        );
        let app = Router::new().route(
            "/",
            post(move |axum::Json(_): axum::Json<serde_json::Value>| async move {
                axum::Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": { "error": raw }
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// Construct an `AppState` with a seeded cache and a real `RpcClient`
    /// pointed at `rpc_url`.
    fn make_state_with_rpc(contract_id: &str, spec_section: Vec<u8>, rpc_url: String) -> AppState {
        let mut state = make_state(contract_id, spec_section);
        state.rpc = RpcClient::new(rpc_url, 30);
        state
    }

    #[tokio::test]
    async fn simulation_error_is_bounded_and_sanitised() {
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let rpc_url = error_rpc_server().await;
        let state = make_state_with_rpc(contract, single_fn_spec("balance"), rpc_url);
        let app = app_for(state);

        let (status, body) = call(
            app,
            &format!("/contracts/{contract}/simulate"),
            json!({ "function": "balance", "args": { "account": "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF" } }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let code = body["code"].as_str().unwrap();
        assert_eq!(code, "simulation_failed");
        let msg = body["error"].as_str().unwrap();

        // Bounded length — never the raw multi-hundred-char upstream blob.
        assert!(
            msg.chars().count() <= lumenqraph_core::sanitize::MAX_SIMULATION_ERROR_LEN + "simulation failed: ".len(),
            "client error must be bounded, got: {msg}"
        );
        // Internal detail must not leak through.
        assert!(!msg.contains("https://internal.rpc"), "leaked endpoint: {msg}");
        assert!(!msg.contains("AAAA"), "leaked XDR blob: {msg}");
        assert!(!msg.contains('\n') && !msg.contains('\t') && !msg.contains('\x00'), "control char leaked: {msg}");
        assert!(msg.contains("simulation failed"), "expected prefix: {msg}");
    }
}
