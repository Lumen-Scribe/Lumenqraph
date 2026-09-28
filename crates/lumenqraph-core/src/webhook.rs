//! Webhook payload envelope types.
//!
//! These structures define the versioned, documented payload shape sent to
//! webhook subscribers, decoupled from internal database schema.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The root envelope for all webhook payloads. Provides a stable, versioned
/// container that is independent of database schema changes.
///
/// All webhook deliveries share this structure regardless of event type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEnvelope {
    /// Unique delivery identifier.
    pub id: String,
    /// Discriminates the payload type: "contract.event" or "contract.upgraded".
    #[serde(rename = "type")]
    pub payload_type: String,
    /// Envelope version for backward-compatible evolution (currently 1).
    pub version: u32,
    /// When this delivery was created.
    pub created_at: DateTime<Utc>,
    /// The actual payload, shape determined by `type`.
    pub data: Value,
}

/// The `data` field for "contract.event" deliveries.
///
/// This is the public contract for event webhooks. Internal columns
/// (seq, xid, created_at) are excluded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractEventData {
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
    pub event_name: Option<String>,
    pub topics: Vec<String>,
    pub value: String,
    pub decoded_topics: Value,
    pub decoded_value: Value,
    /// Named, typed record from the contract spec; null when no spec matched.
    pub enriched: Option<Value>,
    pub tx_hash: String,
    pub in_successful_call: bool,
}

/// The `data` field for "contract.upgraded" deliveries.
///
/// This is the public contract for upgrade webhooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractUpgradeData {
    pub contract_id: String,
    pub version: i32,
    pub wasm_hash: String,
    pub previous_wasm_hash: Option<String>,
    pub breaking: bool,
    pub diff: Value,
    pub observed_at: DateTime<Utc>,
}

impl WebhookEnvelope {
    /// Construct an event envelope from raw database columns.
    pub fn for_event(
        delivery_id: i64,
        created_at: DateTime<Utc>,
        event: ContractEventData,
    ) -> Self {
        Self {
            id: delivery_id.to_string(),
            payload_type: "contract.event".to_string(),
            version: 1,
            created_at,
            data: serde_json::to_value(event).unwrap_or(Value::Null),
        }
    }

    /// Construct an upgrade envelope from raw database columns.
    pub fn for_upgrade(
        delivery_id: i64,
        created_at: DateTime<Utc>,
        upgrade: ContractUpgradeData,
    ) -> Self {
        Self {
            id: delivery_id.to_string(),
            payload_type: "contract.upgraded".to_string(),
            version: 1,
            created_at,
            data: serde_json::to_value(upgrade).unwrap_or(Value::Null),
        }
    }
}
