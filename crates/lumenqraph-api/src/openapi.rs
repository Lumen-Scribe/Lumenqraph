//! OpenAPI 3.1 specification for the Lumenqraph REST API.
//!
//! This module generates and serves a machine-readable OpenAPI specification
//! that describes all REST endpoints, parameters, and response schemas.

use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, Modify, ToSchema};
use utoipa::openapi::{InfoBuilder, OpenApiBuilder as UtoipaOpenApiBuilder};

/// Core data types used in OpenAPI responses
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct EventResponse {
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub event_type: String,
    pub event_name: Option<String>,
    pub topics: serde_json::Value,
    pub decoded_topics: serde_json::Value,
    pub value: String,
    pub decoded_value: serde_json::Value,
}

/// A machine-readable error response.
///
/// The `code` field is stable and suitable for SDK branching. The `error`
/// message is human-readable prose and may change between releases.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ErrorResponse {
    /// Stable machine-readable code. One of: `bad_request`,
    /// `invalid_contract_id`, `unauthorized`, `not_found`, `rate_limited`,
    /// `simulation_failed`, `spec_unavailable`, `internal_error`.
    pub code: String,
    /// Human-readable description of the error. Do not parse this field.
    pub error: String,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct TransferResponse {
    pub event_id: String,
    pub contract_id: String,
    pub from_addr: Option<String>,
    pub to_addr: Option<String>,
    pub amount: String,
    pub ledger: i64,
}

/// Keyset-paginated page of events. Shared envelope of every list endpoint.
///
/// Requests that use the deprecated `offset` parameter (capped at 10,000) are
/// answered with `Deprecation: true` and `Warning` headers.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct EventsPageResponse {
    pub data: Vec<EventResponse>,
    /// Whether more rows are available after this page.
    pub has_more: bool,
    /// Opaque cursor to pass as `after` for the next page; null on the last page.
    pub next_cursor: Option<String>,
}

/// Keyset-paginated page of token transfers.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct TransfersPageResponse {
    pub data: Vec<TransferResponse>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

/// Keyset-paginated page of materialized rows (swaps, NFT events, liquidity events).
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct MaterializedPageResponse {
    pub data: Vec<serde_json::Value>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct WebhookDeliveryResponse {
    pub id: i64,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub delivered_at: Option<String>,
    pub created_at: String,
}

/// Per-status delivery counts, only present with `?include_summary=true`.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct DeliverySummaryResponse {
    pub total: i64,
    pub delivered: i64,
    pub failed: i64,
    pub pending: i64,
}

/// Keyset-paginated page of webhook deliveries (cursor is the delivery id).
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct DeliveriesPageResponse {
    pub data: Vec<WebhookDeliveryResponse>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub summary: Option<DeliverySummaryResponse>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ContractResponse {
    pub contract_id: String,
    pub event_count: i64,
    pub first_seen_ledger: i64,
    pub last_seen_ledger: i64,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ContractInterfaceResponse {
    pub contract_id: String,
    pub has_events: bool,
    pub fetched_at: String,
    pub interface: serde_json::Value,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ContractDataResponse {
    pub contract_id: String,
    pub count: usize,
    pub keys: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct WebhookSubscriptionResponse {
    pub id: String,
    pub url: String,
    pub contract_id: Option<String>,
    pub event_name: Option<String>,
    pub active: bool,
    pub created_at: String,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct MetricsResponse {
    pub http_requests_total: u64,
    pub indexer_lag_ledgers: Option<i64>,
}

/// OpenAPI specification builder
pub struct OpenApiBuilder;

impl OpenApiBuilder {
    /// Build the OpenAPI 3.1 specification for the Lumenqraph API
    pub fn build() -> utoipa::openapi::OpenApi {
        let info = InfoBuilder::new()
            .title("Lumenqraph API")
            .version("0.1.0")
            .description(Some(
                "A high-performance indexer and query API for Soroban contract events on Stellar mainnet. \
                 Provides REST endpoints for contract discovery, event retrieval, state inspection, and \
                 webhook subscriptions."
            ))
            .build();

        let mut api = UtoipaOpenApiBuilder::default()
            .info(info)
            .build();

        // TODO: Add route-specific OpenAPI documentation via Modify trait
        // The routes will be documented using utoipa attributes on handler functions

        api
    }
}

/// OpenAPI documentation struct for axum integration
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Lumenqraph API",
        version = "0.1.0",
        description = "High-performance indexer and query API for Soroban contract events on Stellar mainnet"
    ),
    servers(
        (url = "/", description = "Main API server")
    ),
    components(
        schemas(
            EventResponse,
            ErrorResponse,
            TransferResponse,
            EventsPageResponse,
            TransfersPageResponse,
            MaterializedPageResponse,
            WebhookDeliveryResponse,
            DeliverySummaryResponse,
            DeliveriesPageResponse,
            ContractResponse,
            ContractInterfaceResponse,
            ContractDataResponse,
            WebhookSubscriptionResponse,
            HealthResponse,
            MetricsResponse
        )
    ),
    paths(
        // Core endpoints documented in routes
        get_health,
        get_metrics,
        list_contracts,
        get_contract_interface,
        refresh_contract,
        list_contract_events,
        get_event_by_id,
        list_transaction_events,
        list_contract_transfers,
        list_contract_swaps,
        list_contract_nfts,
        list_contract_liquidity,
        get_contract_data,
        get_contract_data_history,
        list_webhooks,
        create_webhook,
        delete_webhook,
        list_webhook_deliveries
    ),
    tags(
        (name = "Contracts", description = "Contract discovery and interface endpoints"),
        (name = "Events", description = "Contract event retrieval and filtering"),
        (name = "Transfers", description = "Token transfer tracking"),
        (name = "Data", description = "Per-key contract state snapshots"),
        (name = "Webhooks", description = "Event subscription and delivery management"),
        (name = "System", description = "Health and observability endpoints")
    )
)]
pub struct ApiDoc;

/// Placeholder operations for route documentation
/// These will be replaced with actual route annotations

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, description = "Service is healthy", body = HealthResponse)),
    tag = "System"
)]
pub async fn get_health() {}

#[utoipa::path(
    get,
    path = "/metrics",
    responses((status = 200, description = "Prometheus metrics", body = MetricsResponse)),
    tag = "System"
)]
pub async fn get_metrics() {}

#[utoipa::path(
    get,
    path = "/contracts",
    responses((status = 200, description = "List of contracts with event counts", body = Vec<ContractResponse>)),
    tag = "Contracts"
)]
pub async fn list_contracts() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/interface",
    params(("contract_id" = String, Path, description = "Soroban contract ID")),
    responses((status = 200, description = "Contract interface specification", body = ContractInterfaceResponse)),
    tag = "Contracts"
)]
pub async fn get_contract_interface() {}

#[utoipa::path(
    post,
    path = "/contracts/{contract_id}/refresh",
    params(("contract_id" = String, Path, description = "Soroban contract ID")),
    responses(
        (status = 200, description = "Refreshed contract interface specification", body = ContractInterfaceResponse),
        (status = 400, description = "Invalid contract ID or unparseable WASM spec", body = ErrorResponse),
        (status = 404, description = "Contract not found or Stellar Asset Contract", body = ErrorResponse)
    ),
    tag = "Contracts"
)]
pub async fn refresh_contract() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/events",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("limit" = Option<i64>, Query, description = "Max events to return (1-1000, default 50)"),
        ("offset" = Option<i64>, Query, description = "Deprecated: pagination offset (max 10000). Use `after`"),
        ("after" = Option<String>, Query, description = "Opaque cursor from a previous response's `next_cursor`"),
        ("event_name" = Option<String>, Query, description = "Filter by event name")
    ),
    responses(
        (status = 200, description = "Contract events, newest first", body = EventsPageResponse),
        (status = 400, description = "Invalid cursor, contract id, or offset above the cap", body = ErrorResponse)
    ),
    tag = "Events"
)]
pub async fn list_contract_events() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/transfers",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("limit" = Option<i64>, Query, description = "Max transfers to return (1-1000, default 50)"),
        ("offset" = Option<i64>, Query, description = "Deprecated: pagination offset (max 10000). Use `after`"),
        ("after" = Option<String>, Query, description = "Opaque cursor from a previous response's `next_cursor`"),
        ("from" = Option<String>, Query, description = "Filter by sender address"),
        ("to" = Option<String>, Query, description = "Filter by recipient address")
    ),
    responses(
        (status = 200, description = "Token transfers for the contract, newest first", body = TransfersPageResponse),
        (status = 400, description = "Invalid cursor, contract id, or offset above the cap", body = ErrorResponse)
    ),
    tag = "Transfers"
)]
pub async fn list_contract_transfers() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/swaps",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("limit" = Option<i64>, Query, description = "Max swaps to return (1-1000, default 50)"),
        ("offset" = Option<i64>, Query, description = "Deprecated: pagination offset (max 10000). Use `after`"),
        ("after" = Option<String>, Query, description = "Opaque cursor from a previous response's `next_cursor`"),
        ("sender" = Option<String>, Query, description = "Filter by sender address"),
        ("sell_token" = Option<String>, Query, description = "Filter by sold token"),
        ("buy_token" = Option<String>, Query, description = "Filter by bought token")
    ),
    responses(
        (status = 200, description = "AMM swaps for the contract, newest first", body = MaterializedPageResponse),
        (status = 400, description = "Invalid cursor, contract id, or offset above the cap", body = ErrorResponse)
    ),
    tag = "Transfers"
)]
pub async fn list_contract_swaps() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/nfts",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("limit" = Option<i64>, Query, description = "Max NFT events to return (1-1000, default 50)"),
        ("offset" = Option<i64>, Query, description = "Deprecated: pagination offset (max 10000). Use `after`"),
        ("after" = Option<String>, Query, description = "Opaque cursor from a previous response's `next_cursor`"),
        ("kind" = Option<String>, Query, description = "mint | transfer | burn"),
        ("from" = Option<String>, Query, description = "Filter by sender address"),
        ("to" = Option<String>, Query, description = "Filter by recipient address"),
        ("token_id" = Option<String>, Query, description = "Filter by token id")
    ),
    responses(
        (status = 200, description = "NFT events for the contract, newest first", body = MaterializedPageResponse),
        (status = 400, description = "Invalid cursor, kind, contract id, or offset above the cap", body = ErrorResponse)
    ),
    tag = "Transfers"
)]
pub async fn list_contract_nfts() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/liquidity",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("limit" = Option<i64>, Query, description = "Max liquidity events to return (1-1000, default 50)"),
        ("offset" = Option<i64>, Query, description = "Deprecated: pagination offset (max 10000). Use `after`"),
        ("after" = Option<String>, Query, description = "Opaque cursor from a previous response's `next_cursor`"),
        ("kind" = Option<String>, Query, description = "add | remove"),
        ("provider" = Option<String>, Query, description = "Filter by liquidity provider")
    ),
    responses(
        (status = 200, description = "Liquidity events for the contract, newest first", body = MaterializedPageResponse),
        (status = 400, description = "Invalid cursor, kind, contract id, or offset above the cap", body = ErrorResponse)
    ),
    tag = "Transfers"
)]
pub async fn list_contract_liquidity() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/data",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("label" = Option<String>, Query, description = "Filter by discovery label (e.g., 'balance')"),
        ("limit" = Option<i64>, Query, description = "Max keys to return (1-1000, default 100)")
    ),
    responses((status = 200, description = "Current contract data (latest per key)", body = ContractDataResponse)),
    tag = "Data"
)]
pub async fn get_contract_data() {}

#[utoipa::path(
    get,
    path = "/contracts/{contract_id}/data/{key_hash}",
    params(
        ("contract_id" = String, Path, description = "Soroban contract ID"),
        ("key_hash" = String, Path, description = "Hex SHA-256 hash of the storage key"),
        ("limit" = Option<i64>, Query, description = "Max versions to return (1-500, default 50)")
    ),
    responses((status = 200, description = "Version history of a single contract data entry", body = ContractDataResponse)),
    tag = "Data"
)]
pub async fn get_contract_data_history() {}

#[utoipa::path(
    get,
    path = "/webhooks",
    responses((status = 200, description = "List of webhook subscriptions", body = Vec<WebhookSubscriptionResponse>)),
    tag = "Webhooks"
)]
pub async fn list_webhooks() {}

#[utoipa::path(
    post,
    path = "/webhooks",
    request_body = serde_json::Value,
    responses((status = 201, description = "Webhook created", body = WebhookSubscriptionResponse)),
    tag = "Webhooks"
)]
pub async fn create_webhook() {}

#[utoipa::path(
    delete,
    path = "/webhooks/{id}",
    params(("id" = String, Path, description = "Webhook subscription ID")),
    responses((status = 204, description = "Webhook deleted")),
    tag = "Webhooks"
)]
pub async fn delete_webhook() {}

#[utoipa::path(
    get,
    path = "/webhooks/{id}/deliveries",
    params(
        ("id" = String, Path, description = "Webhook subscription ID"),
        ("limit" = Option<i64>, Query, description = "Max deliveries to return (1-500, default 50)"),
        ("offset" = Option<i64>, Query, description = "Deprecated: pagination offset (max 10000). Use `after`"),
        ("after" = Option<String>, Query, description = "Opaque cursor from a previous response's `next_cursor`"),
        ("include_summary" = Option<bool>, Query, description = "Include per-status counts (extra aggregate query; default false)")
    ),
    responses(
        (status = 200, description = "Delivery attempts, newest first", body = DeliveriesPageResponse),
        (status = 400, description = "Invalid cursor or offset above the cap", body = ErrorResponse),
        (status = 404, description = "Subscription not found", body = ErrorResponse)
    ),
    tag = "Webhooks"
)]
pub async fn list_webhook_deliveries() {}

#[utoipa::path(
    get,
    path = "/events/{event_id}",
    params(("event_id" = String, Path, description = "Unique event id from RPC")),
    responses(
        (status = 200, description = "Full event row (raw XDR, decoded JSON, enriched)", body = EventResponse),
        (status = 404, description = "Event not found", body = ErrorResponse)
    ),
    tag = "Events"
)]
pub async fn get_event_by_id() {}

#[utoipa::path(
    get,
    path = "/transactions/{tx_hash}/events",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash"),
        ("limit" = Option<i64>, Query, description = "Max events to return (1-1000, default 100)")
    ),
    responses(
        (status = 200, description = "All indexed events for the transaction, in emission order", body = Vec<EventResponse>),
        (status = 404, description = "No events found for that transaction", body = ErrorResponse)
    ),
    tag = "Events"
)]
pub async fn list_transaction_events() {}
