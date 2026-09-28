//! GraphQL surface over the same Postgres the REST API reads.
//!
//! REST stays the primary, zero-dependency interface; GraphQL is offered
//! alongside it for clients that want to select fields and page through large
//! event/transfer histories with cursors. High-volume lists (`events`,
//! `transfers`, `contracts`) are exposed as Relay-style cursor connections;
//! naturally bounded lists (`contractState`, `contractData`) are plain lists.

use async_graphql::Json as GqlJson;
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Object, Result, Schema, SimpleObject,
};
use chrono::{DateTime, Utc};
use lumenqraph_core::{Contract, EventRow, TokenTransfer};
use serde_json::Value;
use sqlx::types::Json as SqlxJson;
use sqlx::PgPool;

use crate::config::GraphqlConfig;
use crate::pagination::{self, decode_cursor};

pub type AppSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Build the schema, injecting the shared connection pool as context data.
pub fn build_schema(pool: PgPool, config: &GraphqlConfig) -> AppSchema {
    let mut schema_builder = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .limit_depth(config.max_depth)
        .limit_complexity(config.max_complexity);

    if !config.introspection_enabled {
        schema_builder = schema_builder.disable_introspection();
    }

    schema_builder.finish()
}

// ---- Types ----

#[derive(SimpleObject)]
struct EnrichedParam {
    name: String,
    #[graphql(name = "type")]
    type_: String,
    value: GqlJson<Value>,
}

#[derive(SimpleObject)]
struct ContractStat {
    contract_id: String,
    event_count: i64,
    first_seen_ledger: Option<i64>,
    last_seen_ledger: Option<i64>,
}

impl From<Contract> for ContractStat {
    fn from(c: Contract) -> Self {
        Self {
            contract_id: c.contract_id,
            event_count: c.event_count,
            first_seen_ledger: c.first_seen_ledger,
            last_seen_ledger: c.last_seen_ledger,
        }
    }
}

struct Event {
    event_id: String,
    contract_id: String,
    ledger: i64,
    ledger_closed_at: DateTime<Utc>,
    event_type: String,
    event_name: Option<String>,
    decoded_topics: GqlJson<Value>,
    decoded_value: GqlJson<Value>,
    enriched: Option<GqlJson<Value>>,
    tx_hash: String,
    in_successful_call: bool,
}

impl From<EventRow> for Event {
    fn from(e: EventRow) -> Self {
        Self {
            event_id: e.event_id,
            contract_id: e.contract_id,
            ledger: e.ledger,
            ledger_closed_at: e.ledger_closed_at,
            event_type: e.event_type,
            event_name: e.event_name,
            decoded_topics: GqlJson(e.decoded_topics.0),
            decoded_value: GqlJson(e.decoded_value.0),
            enriched: e.enriched.map(|j| GqlJson(j.0)),
            tx_hash: e.tx_hash,
            in_successful_call: e.in_successful_call,
        }
    }
}

#[Object]
impl Event {
    async fn event_id(&self) -> &str {
        &self.event_id
    }

    async fn contract_id(&self) -> &str {
        &self.contract_id
    }

    async fn ledger(&self) -> i64 {
        self.ledger
    }

    async fn ledger_closed_at(&self) -> DateTime<Utc> {
        self.ledger_closed_at
    }

    async fn event_type(&self) -> &str {
        &self.event_type
    }

    async fn event_name(&self) -> &Option<String> {
        &self.event_name
    }

    async fn decoded_topics(&self) -> &GqlJson<Value> {
        &self.decoded_topics
    }

    async fn decoded_value(&self) -> &GqlJson<Value> {
        &self.decoded_value
    }

    async fn enriched(&self) -> &Option<GqlJson<Value>> {
        &self.enriched
    }

    async fn params(&self) -> Result<Vec<EnrichedParam>> {
        match &self.enriched {
            Some(enriched) => {
                if let Some(params_obj) = enriched.0.get("params").and_then(|v| v.as_object()) {
                    let params: Vec<EnrichedParam> = params_obj
                        .iter()
                        .map(|(name, value)| {
                            let type_ = value
                                .get("type")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();
                            let param_value = value
                                .get("value")
                                .cloned()
                                .unwrap_or_else(|| Value::Null);
                            EnrichedParam {
                                name: name.clone(),
                                type_,
                                value: GqlJson(param_value),
                            }
                        })
                        .collect();
                    Ok(params)
                } else {
                    Ok(Vec::new())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    async fn tx_hash(&self) -> &str {
        &self.tx_hash
    }

    async fn in_successful_call(&self) -> bool {
        self.in_successful_call
    }
}

#[derive(SimpleObject)]
struct EventEdge {
    cursor: String,
    node: Event,
}

#[derive(SimpleObject)]
struct PageInfo {
    has_next_page: bool,
    end_cursor: Option<String>,
}

#[derive(SimpleObject)]
struct EventConnection {
    edges: Vec<EventEdge>,
    page_info: PageInfo,
}

#[derive(SimpleObject)]
struct ContractEdge {
    cursor: String,
    node: ContractStat,
}

#[derive(SimpleObject)]
struct ContractConnection {
    edges: Vec<ContractEdge>,
    page_info: PageInfo,
}

#[derive(SimpleObject)]
struct Transfer {
    event_id: String,
    contract_id: String,
    from_addr: Option<String>,
    to_addr: Option<String>,
    amount: String,
    ledger: i64,
    ledger_closed_at: DateTime<Utc>,
}

impl From<TokenTransfer> for Transfer {
    fn from(t: TokenTransfer) -> Self {
        Self {
            event_id: t.event_id,
            contract_id: t.contract_id,
            from_addr: t.from_addr,
            to_addr: t.to_addr,
            amount: t.amount,
            ledger: t.ledger,
            ledger_closed_at: t.ledger_closed_at,
        }
    }
}

#[derive(SimpleObject)]
struct TransferEdge {
    cursor: String,
    node: Transfer,
}

#[derive(SimpleObject)]
struct TransferConnection {
    edges: Vec<TransferEdge>,
    page_info: PageInfo,
}

#[derive(SimpleObject)]
struct StateVersion {
    ledger: i64,
    storage: GqlJson<Value>,
    captured_at: DateTime<Utc>,
}

#[derive(SimpleObject)]
struct DataKey {
    key_hash: String,
    key: GqlJson<Value>,
    durability: String,
    ledger: i64,
    value: GqlJson<Value>,
    label: Option<String>,
    captured_at: DateTime<Utc>,
}

// ---- Query root ----

pub struct QueryRoot;

/// Hard cap for the deprecated, unpaginated `contracts` field.
const CONTRACTS_DEPRECATED_CAP: i64 = 200;

#[Object]
impl QueryRoot {
    /// Cursor-paginated contracts the indexer has seen events for, newest first.
    /// Uses the contract_summaries table (maintained by a trigger) for constant-time performance.
    async fn contracts(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Page size (1-200, default 20)")] first: Option<i32>,
        #[graphql(desc = "Opaque cursor from a previous page's endCursor")] after: Option<String>,
    ) -> Result<ContractConnection> {
        let pool = ctx.data::<PgPool>()?;
        let limit = pagination::clamp_limit(first, 20, 200);
        let cursor = after.as_deref().map(decode_cursor).transpose()?;

        let (after_ledger, after_contract_id) = match &cursor {
            Some(c) => (Some(c.ledger), Some(c.id.as_str())),
            None => (None, None),
        };

        // Fetch one extra row to determine hasNextPage.
        let rows: Vec<Contract> = sqlx::query_as(
            "SELECT contract_id, event_count, first_seen_ledger, last_seen_ledger
             FROM contract_summaries
             WHERE event_count > 0
               AND ($1::bigint IS NULL OR (last_seen_ledger, contract_id) < ($1, $2))
             ORDER BY last_seen_ledger DESC, contract_id
             LIMIT $3",
        )
        .bind(after_ledger)
        .bind(after_contract_id)
        .bind(limit + 1)
        .fetch_all(pool)
        .await?;

        let has_next_page = rows.len() as i64 > limit;
        let mut edges: Vec<ContractEdge> = rows
            .into_iter()
            .take(limit as usize)
            .map(|c| {
                let cursor = pagination::encode_cursor(c.last_seen_ledger.unwrap_or(0), &c.contract_id);
                ContractEdge {
                    cursor,
                    node: ContractStat::from(c),
                }
            })
            .collect();

        let end_cursor = edges.last().map(|e| e.cursor.clone());
        if !has_next_page {
            edges.shrink_to_fit();
        }

        Ok(ContractConnection {
            edges,
            page_info: PageInfo {
                has_next_page,
                end_cursor,
            },
        })
    }

    /// Deprecated: unpaginated list of contracts. Capped at 200 rows; use the
    /// cursor-paginated `contracts` connection instead.
    #[graphql(deprecation = "Use the cursor-paginated `contracts` connection instead")]
    async fn contracts_all(&self, ctx: &Context<'_>) -> Result<Vec<ContractStat>> {
        let pool = ctx.data::<PgPool>()?;
        let rows: Vec<Contract> = sqlx::query_as(
            "SELECT contract_id, event_count, first_seen_ledger, last_seen_ledger
             FROM contract_summaries
             WHERE event_count > 0
             ORDER BY last_seen_ledger DESC, contract_id
             LIMIT $1",
        )
        .bind(CONTRACTS_DEPRECATED_CAP)
        .fetch_all(pool)
        .await?;
        Ok(rows.into_iter().map(ContractStat::from).collect())
    }

    /// Cursor-paginated events for a contract, newest first.
    async fn events(
        &self,
        ctx: &Context<'_>,
        contract_id: String,
        event_name: Option<String>,
        #[graphql(desc = "Page size (1-200, default 20)")] first: Option<i32>,
        #[graphql(desc = "Opaque cu

/* … truncated 9092 chars — edit only what you need near the top … */
