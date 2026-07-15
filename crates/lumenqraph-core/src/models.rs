//! The canonical data model, shared by every service so writer and readers can
//! never drift on schema.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Json;
use sqlx::FromRow;
use uuid::Uuid;

/// A decoded Soroban contract event, ready to be inserted.
///
/// We keep both the raw base64 XDR (`topics`, `value` — lossless) and the
/// decoded JSON (`decoded_topics`, `decoded_value` — queryable/serveable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEvent {
    /// Unique event id from RPC (`id` field). Used as the dedupe key.
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
    /// "contract" | "system" | "diagnostic"
    pub event_type: String,
    /// Raw base64 XDR topics, in order.
    pub topics: Vec<String>,
    /// Decoded topics as JSON, in order.
    pub decoded_topics: Vec<Value>,
    /// Best-effort event name (topic[0] as a symbol).
    pub event_name: Option<String>,
    /// Raw base64 XDR of the event body.
    pub value: String,
    /// Decoded event body as JSON.
    pub decoded_value: Value,
    /// Named, typed record built from the contract's on-chain spec, when one is
    /// available for this event. `None` falls back to the decoded_* fields.
    pub enriched: Option<Value>,
    pub tx_hash: String,
    pub in_successful_call: bool,
    pub paging_token: String,
}

/// A stored event row as served by the api.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventRow {
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
    pub event_type: String,
    pub topics: Json<Vec<String>>,
    pub decoded_topics: Json<Value>,
    pub event_name: Option<String>,
    pub value: String,
    pub decoded_value: Json<Value>,
    /// Named, typed record from the contract spec; null when no spec matched.
    pub enriched: Option<Json<Value>>,
    pub tx_hash: String,
    pub in_successful_call: bool,
    pub paging_token: String,
    pub created_at: DateTime<Utc>,
}

/// A contract we've seen events for, with per-contract aggregate stats.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Contract {
    pub contract_id: String,
    pub event_count: i64,
    pub first_seen_ledger: Option<i64>,
    pub last_seen_ledger: Option<i64>,
}

/// The indexer's position and health, stored as a single row (id = 1).
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct IndexerStatus {
    pub last_processed_ledger: i64,
    pub chain_tip_ledger: i64,
    pub events_ingested_total: i64,
    pub errors_total: i64,
    pub updated_at: DateTime<Utc>,
}

/// A materialized token-transfer, derived from `transfer` events. Amounts are
/// stored as decimal strings since i128 exceeds SQL numeric-in-i64 range.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TokenTransfer {
    pub event_id: String,
    pub contract_id: String,
    pub from_addr: Option<String>,
    pub to_addr: Option<String>,
    pub amount: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
}

/// A registered webhook subscription.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WebhookSubscription {
    pub id: Uuid,
    pub url: String,
    /// What this subscription fires on: `"event"` (an indexed contract event) or
    /// `"upgrade"` (the contract's on-chain interface changed).
    pub kind: String,
    /// Filter: only events from this contract (None = any).
    pub contract_id: Option<String>,
    /// Filter: only events with this name (None = any). Ignored by `upgrade`
    /// subscriptions, which aren't scoped to an event.
    pub event_name: Option<String>,
    /// Shared secret used to HMAC-sign delivery payloads.
    pub secret: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

/// An API key record. Only the SHA-256 hash of the key is stored.
#[derive(Debug, Clone, FromRow)]
pub struct ApiKey {
    pub key_hash: String,
    pub name: String,
    pub tier: String,
    pub rate_limit_per_min: i32,
    pub revoked: bool,
    pub created_at: DateTime<Utc>,
}
