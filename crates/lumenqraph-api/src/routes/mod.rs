//! Route table. `/health` and `/metrics` are public; everything else sits
//! behind the auth + rate-limit middleware.

pub mod contracts;
pub mod events;
pub mod health;
pub mod transfers;
pub mod webhooks;

use axum::routing::{delete, get, post};
use axum::{middleware, Router};

use crate::auth::auth_and_rate_limit;
use crate::metrics;
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    // Public, unauthenticated observability endpoints.
    let public = Router::new()
        .route("/health", get(health::health))
        .route("/metrics", get(metrics::metrics));

    // Data + management endpoints, behind auth + rate limiting.
    let protected = Router::new()
        .route("/contracts", get(contracts::list_contracts))
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
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_and_rate_limit,
        ));

    public.merge(protected).with_state(state)
}
