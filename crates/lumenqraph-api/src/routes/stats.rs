//! `GET /contracts/:contract_id/stats` — aggregated event statistics with time/ledger
//! bucketing and optional grouping by event_name.
//!
//! Returns pre-aggregated event counts bucketed by hour, day, or ledger range,
//! with optional grouping by event_name for multi-series visualization.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct StatsQuery {
    /// Bucketing granularity: "hour", "day", or "ledger" (default: "day").
    #[serde(default = "default_bucket")]
    bucket: String,
    /// Optional grouping: "event_name" to split counts by event type.
    group_by: Option<String>,
    /// Optional time range filter: minimum timestamp (RFC3339).
    from: Option<String>,
    /// Optional time range filter: maximum timestamp (RFC3339).
    to: Option<String>,
    /// Optional ledger range filter: minimum ledger (inclusive).
    from_ledger: Option<i64>,
    /// Optional ledger range filter: maximum ledger (inclusive).
    to_ledger: Option<i64>,
}

fn default_bucket() -> String {
    "day".to_string()
}

#[derive(Serialize, Debug)]
pub struct StatsBucket {
    /// Bucket identifier: ISO8601 datetime (hour/day) or ledger number (ledger).
    pub bucket: String,
    /// Event count in this bucket (or total if grouped).
    pub count: i64,
    /// When group_by=event_name, contains event_name -> count mapping.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdown: Option<std::collections::HashMap<String, i64>>,
}

#[derive(Serialize)]
pub struct StatsResponse {
    /// Aggregated event statistics.
    pub data: Vec<StatsBucket>,
    /// Total events across all buckets.
    pub total: i64,
}

pub async fn contract_stats(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<StatsQuery>,
) -> ApiResult<Json<StatsResponse>> {
    // Validate bucket parameter
    if !["hour", "day", "ledger"].contains(&q.bucket.as_str()) {
        return Err(ApiError::bad_request(
            "Invalid bucket parameter: must be 'hour', 'day', or 'ledger'",
        ));
    }

    // Validate group_by parameter
    if let Some(ref group_by) = q.group_by {
        if group_by != "event_name" {
            return Err(ApiError::bad_request(
                "Invalid group_by parameter: only 'event_name' is supported",
            ));
        }
    }

    // Parse and validate time range
    let from_datetime = if let Some(ref from) = q.from {
        Some(
            chrono::DateTime::parse_from_rfc3339(from)
                .map_err(|_| ApiError::bad_request("Invalid 'from' timestamp format (RFC3339)"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let to_datetime = if let Some(ref to) = q.to {
        Some(
            chrono::DateTime::parse_from_rfc3339(to)
                .map_err(|_| ApiError::bad_request("Invalid 'to' timestamp format (RFC3339)"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    // Validate time range consistency
    if let (Some(from), Some(to)) = (from_datetime, to_datetime) {
        if from > to {
            return Err(ApiError::bad_request("'from' must be before 'to'"));
        }
    }

    // Validate ledger range consistency
    if let (Some(from_ledger), Some(to_ledger)) = (q.from_ledger, q.to_ledger) {
        if from_ledger > to_ledger {
            return Err(ApiError::bad_request("'from_ledger' must be <= 'to_ledger'"));
        }
    }

    // Build and execute aggregation query
    let buckets = match q.bucket.as_str() {
        "hour" => {
            if let Some(ref group_by) = q.group_by {
                if group_by == "event_name" {
                    query_stats_by_hour_grouped(&state, &contract_id, from_datetime, to_datetime)
                        .await?
                } else {
                    query_stats_by_hour(&state, &contract_id, from_datetime, to_datetime).await?
                }
            } else {
                query_stats_by_hour(&state, &contract_id, from_datetime, to_datetime).await?
            }
        }
        "day" => {
            if let Some(ref group_by) = q.group_by {
                if group_by == "event_name" {
                    query_stats_by_day_grouped(&state, &contract_id, from_datetime, to_datetime)
                        .await?
                } else {
                    query_stats_by_day(&state, &contract_id, from_datetime, to_datetime).await?
                }
            } else {
                query_stats_by_day(&state, &contract_id, from_datetime, to_datetime).await?
            }
        }
        "ledger" => {
            if let Some(ref group_by) = q.group_by {
                if group_by == "event_name" {
                    query_stats_by_ledger_grouped(
                        &state,
                        &contract_id,
                        q.from_ledger,
                        q.to_ledger,
                    )
                    .await?
                } else {
                    query_stats_by_ledger(&state, &contract_id, q.from_ledger, q.to_ledger).await?
                }
            } else {
                query_stats_by_ledger(&state, &contract_id, q.from_ledger, q.to_ledger).await?
            }
        }
        _ => unreachable!(),
    };

    let total: i64 = buckets.iter().map(|b| b.count).sum();

    Ok(Json(StatsResponse {
        data: buckets,
        total,
    }))
}

async fn query_stats_by_hour(
    state: &AppState,
    contract_id: &str,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> ApiResult<Vec<StatsBucket>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT date_trunc('hour', ledger_closed_at)::text as bucket, COUNT(*) as count
         FROM events
         WHERE contract_id = $1
           AND ($2::timestamp IS NULL OR ledger_closed_at >= $2)
           AND ($3::timestamp IS NULL OR ledger_closed_at <= $3)
         GROUP BY date_trunc('hour', ledger_closed_at)
         ORDER BY date_trunc('hour', ledger_closed_at) DESC",
    )
    .bind(contract_id)
    .bind(from)
    .bind(to)
    .fetch_all(&state.pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(bucket, count)| StatsBucket {
            bucket,
            count,
            breakdown: None,
        })
        .collect())
}

async fn query_stats_by_hour_grouped(
    state: &AppState,
    contract_id: &str,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> ApiResult<Vec<StatsBucket>> {
    let rows: Vec<(String, Option<String>, i64)> = sqlx::query_as(
        "SELECT date_trunc('hour', ledger_closed_at)::text as bucket, event_name, COUNT(*) as count
         FROM events
         WHERE contract_id = $1
           AND ($2::timestamp IS NULL OR ledger_closed_at >= $2)
           AND ($3::timestamp IS NULL OR ledger_closed_at <= $3)
         GROUP BY date_trunc('hour', ledger_closed_at), event_name
         ORDER BY date_trunc('hour', ledger_closed_at) DESC, event_name",
    )
    .bind(contract_id)
    .bind(from)
    .bind(to)
    .fetch_all(&state.pool)
    .await?;

    let mut result: std::collections::HashMap<String, (i64, std::collections::HashMap<String, i64>)> =
        std::collections::HashMap::new();

    for (bucket, event_name, count) in rows {
        let entry = result.entry(bucket).or_insert((0, std::collections::HashMap::new()));
        entry.0 += count;
        if let Some(name) = event_name {
            *entry.1.entry(name).or_insert(0) += count;
        }
    }

    Ok(result
        .into_iter()
        .map(|(bucket, (total, breakdown))| StatsBucket {
            bucket,
            count: total,
            breakdown: if breakdown.is_empty() {
                None
            } else {
                Some(breakdown)
            },
        })
        .collect())
}

async fn query_stats_by_day(
    state: &AppState,
    contract_id: &str,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> ApiResult<Vec<StatsBucket>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT date_trunc('day', ledger_closed_at)::text as bucket, COUNT(*) as count
         FROM events
         WHERE contract_id = $1
           AND ($2::timestamp IS NULL OR ledger_closed_at >= $2)
           AND ($3::timestamp IS NULL OR ledger_closed_at <= $3)
         GROUP BY date_trunc('day', ledger_closed_at)
         ORDER BY date_trunc('day', ledger_closed_at) DESC",
    )
    .bind(contract_id)
    .bind(from)
    .bind(to)
    .fetch_all(&state.pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(bucket, count)| StatsBucket {
            bucket,
            count,
            breakdown: None,
        })
        .collect())
}

async fn query_stats_by_day_grouped(
    state: &AppState,
    contract_id: &str,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> ApiResult<Vec<StatsBucket>> {
    let rows: Vec<(String, Option<String>, i64)> = sqlx::query_as(
        "SELECT date_trunc('day', ledger_closed_at)::text as bucket, event_name, COUNT(*) as count
         FROM events
         WHERE contract_id = $1
           AND ($2::timestamp IS NULL OR ledger_closed_at >= $2)
           AND ($3::timestamp IS NULL OR ledger_closed_at <= $3)
         GROUP BY date_trunc('day', ledger_closed_at), event_name
         ORDER BY date_trunc('day', ledger_closed_at) DESC, event_name",
    )
    .bind(contract_id)
    .bind(from)
    .bind(to)
    .fetch_all(&state.pool)
    .await?;

    let mut result: std::collections::HashMap<String, (i64, std::collections::HashMap<String, i64>)> =
        std::collections::HashMap::new();

    for (bucket, event_name, count) in rows {
        let entry = result.entry(bucket).or_insert((0, std::collections::HashMap::new()));
        entry.0 += count;
        if let Some(name) = event_name {
            *entry.1.entry(name).or_insert(0) += count;
        }
    }

    Ok(result
        .into_iter()
        .map(|(bucket, (total, breakdown))| StatsBucket {
            bucket,
            count: total,
            breakdown: if breakdown.is_empty() {
                None
            } else {
                Some(breakdown)
            },
        })
        .collect())
}

async fn query_stats_by_ledger(
    state: &AppState,
    contract_id: &str,
    from_ledger: Option<i64>,
    to_ledger: Option<i64>,
) -> ApiResult<Vec<StatsBucket>> {
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT ledger, COUNT(*) as count
         FROM events
         WHERE contract_id = $1
           AND ($2::bigint IS NULL OR ledger >= $2)
           AND ($3::bigint IS NULL OR ledger <= $3)
         GROUP BY ledger
         ORDER BY ledger DESC",
    )
    .bind(contract_id)
    .bind(from_ledger)
    .bind(to_ledger)
    .fetch_all(&state.pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(ledger, count)| StatsBucket {
            bucket: ledger.to_string(),
            count,
            breakdown: None,
        })
        .collect())
}

async fn query_stats_by_ledger_grouped(
    state: &AppState,
    contract_id: &str,
    from_ledger: Option<i64>,
    to_ledger: Option<i64>,
) -> ApiResult<Vec<StatsBucket>> {
    let rows: Vec<(i64, Option<String>, i64)> = sqlx::query_as(
        "SELECT ledger, event_name, COUNT(*) as count
         FROM events
         WHERE contract_id = $1
           AND ($2::bigint IS NULL OR ledger >= $2)
           AND ($3::bigint IS NULL OR ledger <= $3)
         GROUP BY ledger, event_name
         ORDER BY ledger DESC, event_name",
    )
    .bind(contract_id)
    .bind(from_ledger)
    .bind(to_ledger)
    .fetch_all(&state.pool)
    .await?;

    let mut result: std::collections::HashMap<String, (i64, std::collections::HashMap<String, i64>)> =
        std::collections::HashMap::new();

    for (ledger, event_name, count) in rows {
        let bucket_key = ledger.to_string();
        let entry = result
            .entry(bucket_key)
            .or_insert((0, std::collections::HashMap::new()));
        entry.0 += count;
        if let Some(name) = event_name {
            *entry.1.entry(name).or_insert(0) += count;
        }
    }

    Ok(result
        .into_iter()
        .map(|(bucket, (total, breakdown))| StatsBucket {
            bucket,
            count: total,
            breakdown: if breakdown.is_empty() {
                None
            } else {
                Some(breakdown)
            },
        })
        .collect())
}
