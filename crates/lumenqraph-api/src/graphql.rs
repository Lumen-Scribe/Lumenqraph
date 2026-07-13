//! GraphQL surface over the same Postgres the REST API reads.
//!
//! REST stays the primary, zero-dependency interface; GraphQL is offered
//! alongside it for clients that want to select fields and page through large
//! event/transfer histories with cursors. High-volume lists (`events`,
//! `transfers`) are exposed as Relay-style cursor connections; naturally bounded
//! lists (`contracts`, `contractState`, `contractData`) are plain lists.

use async_graphql::Json as GqlJson;
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Object, Result, Schema, SimpleObject,
};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::{DateTime, Utc};
use lumenqraph_core::{Contract, EventRow, TokenTransfer};
use serde_json::Value;
use sqlx::types::Json as SqlxJson;
use sqlx::PgPool;

pub type AppSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Build the schema, injecting the shared connection pool as context data.
pub fn build_schema(pool: PgPool) -> AppSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}

// ---- Opaque cursors ----
//
// A cursor encodes the (ledger, event_id) position of the row it points at.
// Paging with `after` returns rows strictly older than that position under the
// canonical `ORDER BY ledger DESC, event_id DESC`.

fn encode_cursor(ledger: i64, id: &str) -> String {
    B64.encode(format!("{ledger}|{id}"))
}

/// Decode a cursor to `(ledger, event_id)`. Returns `None` for absent/malformed
/// cursors, which the callers treat as "start from the newest".
fn decode_cursor(cursor: Option<&str>) -> Option<(i64, String)> {
    let raw = cursor?;
    let bytes = B64.decode(raw).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let (ledger, id) = s.split_once('|')?;
    Some((ledger.parse().ok()?, id.to_string()))
}

// ---- Types ----

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

#[derive(SimpleObject)]
struct Event {
    event_id: String,
    contract_id: String,
    ledger: i64,
    ledger_closed_at: DateTime<Utc>,
    event_type: String,
    event_name: Option<String>,
    /// Decoded topics as JSON.
    decoded_topics: GqlJson<Value>,
    /// Decoded event body as JSON.
    decoded_value: GqlJson<Value>,
    /// Named, typed record from the contract spec; null when none matched.
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

#[Object]
impl QueryRoot {
    /// Contracts the indexer has seen events for, with per-contract counts.
    async fn contracts(&self, ctx: &Context<'_>) -> Result<Vec<ContractStat>> {
        let pool = ctx.data::<PgPool>()?;
        let rows: Vec<Contract> = sqlx::query_as(
            "SELECT contract_id, count(*)::bigint AS event_count,
                    min(ledger) AS first_seen_ledger, max(ledger) AS last_seen_ledger
             FROM events GROUP BY contract_id ORDER BY event_count DESC",
        )
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
        #[graphql(desc = "Opaque cursor from a previous page's endCursor")] after: Option<String>,
    ) -> Result<EventConnection> {
        let pool = ctx.data::<PgPool>()?;
        let limit = first.unwrap_or(20).clamp(1, 200) as i64;
        let after = decode_cursor(after.as_deref());
        let (after_ledger, after_id) = match after {
            Some((l, id)) => (Some(l), Some(id)),
            None => (None, None),
        };
        // Fetch one extra row to determine hasNextPage.
        let rows: Vec<EventRow> = sqlx::query_as(
            "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                    topics, decoded_topics, event_name, value, decoded_value,
                    enriched, tx_hash, in_successful_call, paging_token, created_at
             FROM events
             WHERE contract_id = $1
               AND ($2::text IS NULL OR event_name = $2)
               AND ($3::bigint IS NULL OR ledger < $3 OR (ledger = $3 AND event_id < $4))
             ORDER BY ledger DESC, event_id DESC
             LIMIT $5",
        )
        .bind(&contract_id)
        .bind(&event_name)
        .bind(after_ledger)
        .bind(after_id)
        .bind(limit + 1)
        .fetch_all(pool)
        .await?;

        Ok(build_event_connection(rows, limit))
    }

    /// Cursor-paginated token transfers, newest first. Filter by contract.
    async fn transfers(
        &self,
        ctx: &Context<'_>,
        contract_id: Option<String>,
        #[graphql(desc = "Page size (1-200, default 20)")] first: Option<i32>,
        after: Option<String>,
    ) -> Result<TransferConnection> {
        let pool = ctx.data::<PgPool>()?;
        let limit = first.unwrap_or(20).clamp(1, 200) as i64;
        let (after_ledger, after_id) = match decode_cursor(after.as_deref()) {
            Some((l, id)) => (Some(l), Some(id)),
            None => (None, None),
        };
        let rows: Vec<TokenTransfer> = sqlx::query_as(
            "SELECT event_id, contract_id, from_addr, to_addr, amount, ledger, ledger_closed_at
             FROM token_transfers
             WHERE ($1::text IS NULL OR contract_id = $1)
               AND ($2::bigint IS NULL OR ledger < $2 OR (ledger = $2 AND event_id < $3))
             ORDER BY ledger DESC, event_id DESC
             LIMIT $4",
        )
        .bind(&contract_id)
        .bind(after_ledger)
        .bind(after_id)
        .bind(limit + 1)
        .fetch_all(pool)
        .await?;

        Ok(build_transfer_connection(rows, limit))
    }

    /// Versioned instance-storage snapshots for a contract, newest first.
    async fn contract_state(
        &self,
        ctx: &Context<'_>,
        contract_id: String,
        #[graphql(desc = "How many versions (1-200, default 1)")] limit: Option<i32>,
    ) -> Result<Vec<StateVersion>> {
        let pool = ctx.data::<PgPool>()?;
        let limit = limit.unwrap_or(1).clamp(1, 200) as i64;
        let rows: Vec<(i64, SqlxJson<Value>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT ledger, storage, captured_at FROM contract_state
             WHERE contract_id = $1 ORDER BY ledger DESC LIMIT $2",
        )
        .bind(&contract_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(ledger, storage, captured_at)| StateVersion {
                ledger,
                storage: GqlJson(storage.0),
                captured_at,
            })
            .collect())
    }

    /// Latest value of each per-key entry (e.g. holder balances) for a contract.
    async fn contract_data(
        &self,
        ctx: &Context<'_>,
        contract_id: String,
        label: Option<String>,
        #[graphql(desc = "Max keys (1-1000, default 100)")] limit: Option<i32>,
    ) -> Result<Vec<DataKey>> {
        let pool = ctx.data::<PgPool>()?;
        let limit = limit.unwrap_or(100).clamp(1, 1000) as i64;
        type Row = (
            String,
            SqlxJson<Value>,
            String,
            i64,
            SqlxJson<Value>,
            Option<String>,
            DateTime<Utc>,
        );
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT key_hash, key, durability, ledger, value, label, captured_at FROM (
                 SELECT DISTINCT ON (key_hash)
                        key_hash, key, durability, ledger, value, label, captured_at
                 FROM contract_data
                 WHERE contract_id = $1 AND ($2::text IS NULL OR label = $2)
                 ORDER BY key_hash, ledger DESC
             ) latest
             ORDER BY ledger DESC LIMIT $3",
        )
        .bind(&contract_id)
        .bind(&label)
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(key_hash, key, durability, ledger, value, label, captured_at)| DataKey {
                    key_hash,
                    key: GqlJson(key.0),
                    durability,
                    ledger,
                    value: GqlJson(value.0),
                    label,
                    captured_at,
                },
            )
            .collect())
    }
}

/// Turn `limit + 1` rows into a connection: the extra row (if present) means
/// there's a next page; the last kept row's position becomes `endCursor`.
fn build_event_connection(mut rows: Vec<EventRow>, limit: i64) -> EventConnection {
    let has_next_page = rows.len() as i64 > limit;
    if has_next_page {
        rows.truncate(limit as usize);
    }
    let end_cursor = rows.last().map(|e| encode_cursor(e.ledger, &e.event_id));
    let edges = rows
        .into_iter()
        .map(|e| EventEdge {
            cursor: encode_cursor(e.ledger, &e.event_id),
            node: Event::from(e),
        })
        .collect();
    EventConnection {
        edges,
        page_info: PageInfo {
            has_next_page,
            end_cursor,
        },
    }
}

fn build_transfer_connection(mut rows: Vec<TokenTransfer>, limit: i64) -> TransferConnection {
    let has_next_page = rows.len() as i64 > limit;
    if has_next_page {
        rows.truncate(limit as usize);
    }
    let end_cursor = rows.last().map(|t| encode_cursor(t.ledger, &t.event_id));
    let edges = rows
        .into_iter()
        .map(|t| TransferEdge {
            cursor: encode_cursor(t.ledger, &t.event_id),
            node: Transfer::from(t),
        })
        .collect();
    TransferConnection {
        edges,
        page_info: PageInfo {
            has_next_page,
            end_cursor,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let c = encode_cursor(42, "abc");
        assert_eq!(decode_cursor(Some(&c)), Some((42, "abc".to_string())));
    }

    #[test]
    fn bad_cursor_decodes_to_none() {
        assert_eq!(decode_cursor(None), None);
        assert_eq!(decode_cursor(Some("!!!not-base64!!!")), None);
        // Valid base64 but wrong shape.
        let junk = B64.encode("no-separator");
        assert_eq!(decode_cursor(Some(&junk)), None);
    }

    #[test]
    fn schema_exposes_the_expected_graph() {
        // Building the schema (without a pool) validates the whole type graph
        // and that every resolver is registered — a runtime check with no DB.
        let sdl = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
            .finish()
            .sdl();
        for expected in [
            "type Query",
            "events(",
            "transfers(",
            "contractState(",
            "contractData(",
            "type EventConnection",
            "type PageInfo",
            "hasNextPage",
        ] {
            assert!(sdl.contains(expected), "SDL missing {expected:?}");
        }
    }
}
