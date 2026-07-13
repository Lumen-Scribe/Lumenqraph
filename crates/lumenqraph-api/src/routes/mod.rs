//! Route table. `/health` and `/metrics` are public; everything else sits
//! behind the auth + rate-limit middleware.

pub mod contracts;
pub mod events;
pub mod health;
pub mod read;
pub mod transfers;
pub mod webhooks;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::response::{Html, IntoResponse};
use axum::routing::{delete, get, post};
use axum::{middleware, Extension, Router};

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

    public.merge(protected).with_state(state)
}
