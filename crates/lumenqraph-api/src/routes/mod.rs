//! Route table. `/health` and `/metrics` are public; everything else sits
//! behind the auth + rate-limit middleware.

pub mod contracts;
pub mod events;
pub mod health;
pub mod proxy;
pub mod read;
pub mod sdk;
pub mod transfers;
pub mod webhooks;

use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::Request;
use axum::http::{header, HeaderValue};
use axum::response::{Html, IntoResponse};
use axum::routing::{any, delete, get, post};
use axum::{middleware, Extension, Router};
use tower::Layer;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::auth::auth_and_rate_limit;
use crate::graphql::{self, AppSchema};
use crate::metrics;
use crate::state::AppState;

/// Execute a GraphQL query against the shared schema.
async fn graphql_handler(schema: Extension<AppSchema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

/// Serve the GraphiQL in-browser IDE, pointed at `/graphql`.
async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

pub fn router(state: AppState) -> Router {
    let schema = graphql::build_schema(state.pool.clone());

    // Public, unauthenticated observability endpoints.
    let public = Router::new()
        .route("/health", get(health::health))
        .route("/metrics", get(metrics::metrics));

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
        .route("/contracts/:contract_id/call", post(read::call_function))
        .route(
            "/contracts/:contract_id/simulate",
            post(read::simulate_call),
        )
        .route("/contracts/:contract_id/events", get(events::list_events))
        .route(
            "/contracts/:contract_id/transfers",
            get(transfers::list_transfers),
        )
        .route(
            "/webhooks",
            post(webhooks::create_webhook).get(webhooks::list_webhooks),
        )
        .route("/webhooks/:id", delete(webhooks::delete_webhook))
        // GraphQL: POST executes queries, GET serves the GraphiQL IDE. Behind
        // the same auth + rate-limit middleware as the REST data routes.
        .route("/graphql", post(graphql_handler).get(graphiql))
        .layer(Extension(schema))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_and_rate_limit,
        ));

    let mut app = public.merge(protected).with_state(state.clone());

    // Sibling instances under a path prefix (see `proxy`). Registered outside
    // the auth middleware: each upstream enforces its own policy.
    if !state.mounts.is_empty() {
        let client = Arc::new(reqwest::Client::new());
        for (name, upstream) in state.mounts.iter() {
            let (client, upstream, prefix) = (
                Arc::clone(&client),
                Arc::new(upstream.clone()),
                Arc::new(format!("/{name}")),
            );
            let handler = move |req: Request| {
                proxy::proxy(
                    Arc::clone(&client),
                    Arc::clone(&upstream),
                    Arc::clone(&prefix),
                    req,
                )
            };
            app = app
                .route(&format!("/{name}"), any(handler.clone()))
                .route(&format!("/{name}/*rest"), any(handler));
        }
    }

    // Serve the static explorer UI at the same origin as the API (so it needs
    // no CORS and no configured API base). Falls back to it for any unmatched
    // path; `/` resolves to explorer/index.html. Dir is configurable so the
    // container image can point at wherever the assets are COPYed.
    let explorer_dir = std::env::var("EXPLORER_DIR").unwrap_or_else(|_| "explorer".to_string());
    if std::path::Path::new(&explorer_dir).is_dir() {
        // `no-cache` means "revalidate before using", not "don't cache":
        // ServeDir serves Last-Modified, so an unchanged explorer costs a 304 —
        // but a deploy shows up on the next load instead of whenever the
        // browser's heuristic cache happens to expire.
        let revalidate = SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        app.fallback_service(revalidate.layer(ServeDir::new(explorer_dir)))
    } else {
        app
    }
}
