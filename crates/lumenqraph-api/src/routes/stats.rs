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

/// Maximum number of buckets that may be returned in a single response.
const MAX_BUCKETS: usize = 2_000;

#[derive(Deserialize)]
pub struct StatsQuery {
    /// Bucketing granularity: "hour", "day", or "ledger" (default: "day").
    #[serde(default = "default_bucket")]
    bucket: String,
    /// Resolution alias: "1m", "1h", "1d", "1w", "1M", "hour", "day" (preferred).
    resolution: Option<String>,
    /// Relative time window from now: e.g. "1h", "24h", "7d", "30d".
    window: Option<String>,
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

enum Granularity {
    Trunc(&'static str),
    Ledger,
}

fn parse_window(w: &str) -> Result<chrono::Duration, ApiError> {
    let w = w.trim();
    if w.is_empty() {
        return Err(ApiError::bad_request("Window parameter cannot be empty"));
    }
    let (num_str, unit) = w.split_at(w.len().saturating_sub(1));
    let num: i64 = num_str.parse().map_err(|_| {
        ApiError::bad_request("Invalid window format. Expected format like '7d', '24h', '30m'")
    })?;
    if num <= 0 {
        return Err(ApiError::bad_request("Window duration must be positive"));
    }
    match unit {
        "m" => Ok(chrono::Duration::minutes(num)),
        "h" => Ok(chrono::Duration::hours(num)),
        "d" => Ok(chrono::Duration::days(num)),
        "w" => Ok(chrono::Duration::weeks(num)),
        _ => Err(ApiError::bad_request(
            "Invalid window unit. Supported units: 'm' (minutes), 'h' (hours), 'd' (days), 'w' (weeks)",
        )),
    }
}

fn parse_granularity(resolution: Option<&str>, bucket: &str) -> Result<Granularity, ApiError> {
    if let Some(res) = resolution {
        match res.to_ascii_lowercase().as_str() {
            "1m" | "minute" | "min" => Ok(Granularity::Trunc("minute")),
            "1h" | "hour" => Ok(Granularity::Trunc("hour")),
            "1d" | "day" => Ok(Granularity::Trunc("day")),
            "1w" | "week" => Ok(Granularity::Trunc("week")),
            "1m" | "month" if res == "1M" || res.to_ascii_lowercase() == "month" => {
                Ok(Granularity::Trunc("month"))
            }
            "ledger" => Ok(Granularity::Ledger),
            _ => Err(ApiError::bad_request(
                "Invalid resolution parameter: must be '1m', '1h', '1d', '1w', '1M' (or 'hour', 'day')",
            )),
        }
    } else {
        match bucket.to_ascii_lowercase().as_str() {
            "hour" | "1h" => Ok(Granularity::Trunc("hour")),
            "day" | "1d" => Ok(Granularity::Trunc("day")),
            "minute" | "1m" => Ok(Granularity::Trunc("minute")),
            "week" | "1w" => Ok(Granularity::Trunc("week")),
            "month" | "1m" if bucket == "1M" || bucket.to_ascii_lowercase() == "month" => {
                Ok(Granularity::Trunc("month"))
            }
            "ledger" => Ok(Granularity::Ledger),
            _ => Err(ApiError::bad_request(
                "Invalid bucket parameter: must be 'hour', 'day', or 'ledger'",
            )),
        }
    }
}

/// Validate a contract id before it is used in a query.
fn validate_contract_id(contract_id: &str) -> Result<(), ApiError> {
    let valid = !contract_id.is_empty()
        && contract_id.len() <= 128
        && contract_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':');
    if !valid {
        return Err(ApiError::bad_request(
            "Invalid contract_id: must be 1-128 characters of [A-Za-z0-9_-:]",
        ));
    }
    Ok(())
}

/// Number of buckets a truncation granularity would produce over a range.
fn trunc_bucket_count(trunc: &str, from: chrono::DateTime<chrono::Utc>, to: chrono::DateTime<chrono::Utc>) -> i64 {
    let secs = (to - from).num_seconds().max(0);
    let unit_secs = match trunc {
        "minute" => 60,
        "hour" => 3_600,
        "day" => 86_400,
        "week" => 604_800,
        "month" => 2_592_000,
        _ => 86_400,
    };
    secs / unit_secs + 1
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
    validate_contract_id(&contract_id)?;

    let granularity = parse_granularity(q.resolution.as_deref(), &q.bucket)?;

    // Validate group_by parameter
    if let Some(ref group_by) = q.group_by {
        if group_by != "event_name" {
            return Err(ApiError::bad_request(
                "Invalid group_by parameter: only 'event_name' is supported",
            ));
        }
    }

    // Parse and validate time range
    let mut from_datetime = if let Some(ref from) = q.from {
        Some(
            chrono::DateTime::parse_from_rfc3339(from)
                .map_err(|_| ApiError::bad_request("Invalid 'from' timestamp format (RFC3339)"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let mut to_datetime = if let Some(ref to) = q.to {
        Some(
            chrono::DateTime::parse_from_rfc3339(to)
                .map_err(|_| ApiError::bad_request("Invalid 'to' timestamp format (RFC3339)"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    // If window is specified, compute relative time range
    if let Some(ref window) = q.window {
        let dur = parse_window(window)?;
        let to = to_datetime.unwrap_or_else(chrono::Utc::now);
        let from = to - dur;
        from_datetime = Some(from);
        to_datetime = Some(to);
    }

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

    // Apply default ranges so unbounded history scans are not possible, and
    // enforce the MAX_BUCKETS cap before running any aggregation query.
    let mut from_ledger = q.from_ledger;
    let mut to_ledger = q.to_ledger;
    match granularity {
        Granularity::Trunc(trunc) => {
            let now = chrono::Utc::now();
            let default_from = match trunc {
                "hour" => now - chrono::Duration::days(7),
                "day" => now - chrono::Duration::days(90),
                _ => now - chrono::Duration::days(90),
            };
            let from = from_datetime.unwrap_or(default_from);
            let to = to_datetime.unwrap_or(now);
            from_datetime = Some(from);
            to_datetime = Some(to);
            if trunc_bucket_count(trunc, from, to) > MAX_BUCKETS as i64 {
                return Err(ApiError::bad_request(format!(
                    "Requested range exceeds the maximum of {} buckets; narrow the 'from'/'to' range or use a coarser bucket",
                    MAX_BUCKETS
                )));
            }
        }
        Granularity::Ledger => {
            let to = to_ledger.unwrap_or(i64::MAX);
            let from = from_ledger.unwrap_or_else(|| {
                if to == i64::MAX {
                    // No explicit upper bound: default to the last 10,000 ledgers.
                    // The query clamps this against the contract's max ledger.
                    0
                } else {
                    to.saturating_sub(10_000)
                }
            });
            if to != i64::MAX && to.saturating_sub(from) > MAX_BUCKETS as i64 {
                return Err(ApiError::bad_request(format!(
                    "Requested ledger range exceeds the maximum of {} buckets; narrow the 'from_ledger'/'to_ledger' range",
                    MAX_BUCKETS
                )));
            }
            from_ledger = Some(from);
            to_ledger = if to == i64::MAX { None } else { Some(to) };
        }
    }

    // Build and execute aggregation query
    let buckets = match granularity {
        Granularity::Trunc(trunc) => {
            if let Some(ref group_by) = q.group_by {
                if group_by == "event_name" {
                    query_stats_by_trunc_grouped(&state, &contract_id, trunc, from_datetime, to_datetime)
                        .await?
                } else {
                    query_stats_by_trunc(&state, &contract_id, trunc, from_datetime, to_datetime).await?
                }
            } else {
                query_stats_by_trunc(&state, &contract_id, trunc, from_datetime, to_datetime).await?
            }
        }
        Granularity::Ledger => {
            if let Some(ref group_by) = q.group_by {
                if group_by == "event_name" {
                    query_stats_by_ledger_grouped(
                        &state,
                        &contract_id,
                        from_ledger,
                        to_ledger,
                    )
                    .await?
                } else {
                    query_stats_by_ledger(&state, &contract_id, from_ledger, to_ledger).await?
                }
            } else {
                query_stats_by_ledger(&state, &contract_id, from_ledger, to_ledger).await?
            }
        }
    };

    let total: i64 = buckets.iter().map(|b| b.count).sum();

    Ok(Json(StatsResponse {
        data: buckets,
        total,
    }))
}

async fn query_stats_by_trunc(
    state: &AppState,
    contract_id: &str,
    trunc: &str,
   

/* … truncated 4766 chars — edit only what you need near the top … */
