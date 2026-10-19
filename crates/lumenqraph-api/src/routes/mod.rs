//! Route table. `/health` and `/metrics` are public; everything else sits
//! behind the auth + rate-limit middleware.

pub mod contracts;
pub mod events;
pub mod health;
pub mod liquidity;
pub mod nfts;
pub mod openapi;
pub mod proxy;
pub mod read;
pub mod sdk;
pub mod stats;
pub mod stream;
pub mod swaps;
pub mod transfers;
pub mod webhooks;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse};
use axum::Json;
use axum::routing::{delete, get, patch, post};
use axum::{middleware, Extension, Router};
use serde_json::json;
use tower::Layer;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::auth::{
    auth_and_rate_limit, concurrency_limit, proxy_rate_limit, rpc_auth_and_rate_limit,
    webhook_auth_and_rate_limit, webhook_manage_auth_and_rate_limit,
};
use crate::graphql::{self, AppSchema};
use crate::metrics;
use crate::request_id;
use crate::state::AppState;

/// Execute a GraphQL query against the shared schema.
async fn graphql_handler(schema: Extension<AppSchema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

/// Serve the GraphiQL in-browser IDE, pointed at `/graphql`.
async fn graphiql(State(state): State<AppState>) -> impl IntoResponse {
    if !state.config.graphql.introspection_enabled {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": "GraphQL introspection is disabled",
                "message": "Use POST /graphql for queries. Introspection is not available in this environment."
            }))
        ).into_response();
    }

    Html(GraphiQLSource::build().endpoint("/graphql").finish()).into_response()
}

pub fn router(state: AppState) -> Router {
    let schema = graphql::build_schema(state.pool.clone(), &state.config.graphql);

    // Public, unauthenticated observability and documentation endpoints.
    let public = Router::new()
        .route("/health", get(health::health))
        .route("/livez", get(health::livez))
        .route("/readyz", get(health::readyz))
        .merge(openapi::router());

    // /metrics: public by default, but can be restricted to authenticated
    // callers via METRICS_REQUIRE_API_KEY=true (#213).
    let metrics_router = if state.metrics_require_auth {
        Router::new()
            .route("/metrics", get(metrics::metrics))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth_and_rate_limit,
            ))
    } else {
        Router::new().route("/metrics", get(metrics::metrics))
    };

    // RPC-backed routes with separate, tighter rate limiting (they hit upstream RPC).
    let rpc_routes = Router::new()
        .route("/contracts/:contract_id/call", post(read::call_function))
        .route(
            "/contracts/:contract_id/simulate",
            post(read::simulate_call),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rpc_auth_and_rate_limit,
        ));

    // Data + management endpoints, behind auth + rate limiting.
    let protected = Router::new()
        .route("/contracts", get(contracts::list_contracts))
        .route(
            "/contracts/:contract_id/interface",
            get(contracts::contract_interface),
        )
        .route(
            "/contracts/:contract_id/interface/history",
            get(contracts::contract_interface_history),
        )
        .route(
            "/contracts/:contract_id/interface/diff",
            get(contracts::contract_interface_diff),
        )
        .route(
            "/contracts/:contract_id/refresh",
            post(contracts::refresh_contract),
        )
        .route(
            "/contracts/:contract_id/state",
            get(contracts::contract_state),
        )
        .route(
            "/contracts/:contract_id/data",
            get(contracts::contract_data),
        )
        .route(
            "/contracts/:contract_id/data/:key_hash",
            get(contracts::contract_data_key),
        )
        .route("/contracts/:contract_id/sdk", get(sdk::contract_sdk))
        .route(
            "/contracts/:contract_id/functions",
            get(read::list_functions),
        )
        .route("/contracts/:contract_id/events", get(events::list_events))
        .route("/contracts/:contract_id/events/stream", get(stream::stream_events))
        .route("/contracts/:contract_id/stats", get(stats::contract_stats))
        .route(
            "/contracts/:contract_id/transfers",
            get(transfers::list_transfers),
        )
        .route(
            "/contracts/:contract_id/liquidity",
            get(liquidity::list_liquidity_events),
        )
        .route(
            "/contracts/:contract_id/nfts",
            get(nfts::list_nft_events),
        )
        .route(
            "/contracts/:contract_id/swaps",
            get(swaps::list_swaps),
        )
        // Single-event and by-transaction lookups (#123, #124).
        .route("/events/:event_id", get(events::get_event))
        .route(
            "/transactions/:tx_hash/events",
            get(events::transaction_events),
        )
        // GraphQL: POST executes queries, GET serves the GraphiQL IDE. Behind
        // the same auth + rate-limit middleware as the REST data routes.
        .route("/graphql", post(graphql_handler).get(graphiql))
        .layer(Extension(schema))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_and_rate_limit,
        ));

    // Webhook management routes: GET /webhooks, DELETE/PATCH /webhooks/:id,
    // and related sub-routes. These ALWAYS require a valid API key regardless
    // of REQUIRE_API_KEY (#420 — anonymous callers must not be able to list,
    // modify, or delete webhook subscriptions).
    let webhook_manage_routes = Router::new()
        .route(
            "/webhooks",
            get(webhooks::list_webhooks),
        )
        .route("/webhooks/:id", get(webhooks::get_webhook).delete(webhooks::delete_webhook).patch(webhooks::update_webhook))
        .route("/webhooks/:id/deliveries", get(webhooks::list_webhook_deliveries))
        .route("/webhooks/:id/redrive", post(webhooks::redrive_webhook))
        .route("/webhooks/:id/reenable", post(webhooks::reenable_webhook))
        .route("/webhooks/:id/rotate-secret", post(webhooks::rotate_webhook_secret))
        .route("/webhooks/:id/test", post(webhooks::test_webhook))
        // GraphQL: POST executes queries, GET serves the GraphiQL IDE. Behind
        // the same auth + rate-limit middleware as the REST data routes.
        .route("/graphql", post(graphql_handler).get(graphiql))
        .layer(Extension(schema))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            webhook_manage_auth_and_rate_limit,
        ));

    // Webhook creation route with separate, lower rate limiting (prevents subscription spam).
    let webhook_create_routes = Router::new()
        .route("/webhooks", post(webhooks::create_webhook))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            webhook_auth_and_rate_limit,
        ));

    let metrics_collector = state.metrics.clone();
    let mut app = public
        .merge(metrics_router)
        .merge(protected)
        .merge(rpc_routes)
        .merge(webhook_create_routes)
        .merge(webhook_manage_routes)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            concurrency_limit,
        ))
        .layer(middleware::from_fn(move |req: Request, next: Next| {
            let collector = metrics_collector.clone();
            collector.middleware(req, next)
        }))
        .layer(middleware::from_fn(request_id::request_id_middleware));

    // Sibling instances under a path prefix (see `proxy`). Each upstream
    // still enforces its own auth, but mounted routes get this instance's
    // per-IP concurrency cap and a per-IP rate limit too, so a single noisy
    // client can't monopolise the proxy (#442). The last layer added runs
    // first: concurrency cap, then rate limit.
    if !state.mounts.is_empty() {
        let mounts = proxy::mount_router(&state.mounts, &state.config.proxy)
            .layer(middleware::from_fn_with_state(
                state.clone(),
                proxy_rate_limit,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                concurrency_limit,
            ));
        app = app.merge(mounts);
    }

    // Serve the static explorer UI at the same origin as the API (so it needs
    // no CORS and no configured API base). Falls back to it for any unmatched
    // path; `/` resolves to explorer/index.html. Dir is configurable so the
    // container image can point at wherever the assets are COPYed.
    let explorer_dir = state.config.explorer_dir.clone();
    if std::path::Path::new(&explorer_dir).is_dir() {
        // `no-cache` means "revalidate before using", not "don't cache":
        // ServeDir serves Last-Modified, so an unchanged explorer costs a 304 —
        // but a deploy shows up on the next load instead of whenever the
        // browser's heuristic cache happens to expire.
        let revalidate = SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        // Strict CSP: disallow inline scripts and restrict sources to same-origin only.
        // Protects against stored XSS from malicious contract data in the explorer UI.
        let csp = SetResponseHeaderLayer::if_not_present(
            header::HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self' data:; connect-src 'self'"),
        );
        // Prevent MIME-type sniffing (guards against polyglot-file attacks).
        let nosniff = SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        // Prevent the explorer from being embedded in an iframe (clickjacking).
        let no_frame = SetResponseHeaderLayer::if_not_present(
            header::HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        );
        app.fallback_service(
            no_frame.layer(nosniff.layer(csp.layer(revalidate.layer(ServeDir::new(explorer_dir)))))
        )
    } else {
        app
    }
}

#[cfg(test)]
mod openapi_coverage_tests {
    //! Every route registered in this file must be documented in `openapi.yaml`.
    //!
    //! axum 0.7 does not expose its route table at runtime, so the test reads
    //! the route registrations from this file's source and checks each path
    //! against the spec's path keys.

    use std::collections::HashSet;

    /// The `.route(` call as text, split so this test's own source does not
    /// match when the file is scanned.
    const ROUTE_CALL: &str = concat!(".", "route", "(");

    /// Axum's `:param` segments become OpenAPI's `{param}`.
    fn to_openapi_path(route: &str) -> String {
        route
            .split('/')
            .map(|segment| match segment.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => segment.to_string(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Every route path registered in `source`, in OpenAPI form.
    fn registered_paths(source: &str) -> Vec<String> {
        let mut paths: Vec<String> = source
            .match_indices(ROUTE_CALL)
            .filter_map(|(idx, _)| {
                let rest = source[idx + ROUTE_CALL.len()..].trim_start();
                let rest = rest.strip_prefix('"')?;
                let end = rest.find('"')?;
                Some(to_openapi_path(&rest[..end]))
            })
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }

    /// Path keys of the spec: top-level entries under `paths:` are indented by
    /// exactly two spaces and start with `/`.
    fn documented_paths(spec: &str) -> HashSet<&str> {
        spec.lines()
            .filter_map(|line| line.strip_prefix("  ")?.strip_suffix(':'))
            .filter(|key| key.starts_with('/'))
            .collect()
    }

    #[test]
    fn route_params_convert_to_openapi_form() {
        assert_eq!(
            to_openapi_path("/webhooks/:id/rotate-secret"),
            "/webhooks/{id}/rotate-secret"
        );
        assert_eq!(
            to_openapi_path("/contracts/:contract_id/data/:key_hash"),
            "/contracts/{contract_id}/data/{key_hash}"
        );
    }

    #[test]
    fn every_registered_route_is_documented_in_openapi_yaml() {
        let routes = registered_paths(include_str!("mod.rs"));
        assert!(
            routes.len() > 20,
            "route scan found only {} paths; the scanner is probably broken",
            routes.len()
        );

        let spec = include_str!("../../../../openapi.yaml");
        let documented = documented_paths(spec);
        let missing: Vec<&String> = routes
            .iter()
            .filter(|path| !documented.contains(path.as_str()))
            .collect();

        assert!(
            missing.is_empty(),
            "routes registered in routes/mod.rs but missing from openapi.yaml: {missing:?}"
        );
    }
}
