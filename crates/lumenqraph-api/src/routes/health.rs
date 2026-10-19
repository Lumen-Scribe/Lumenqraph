//! Health endpoints: `/health` for status, `/livez` for liveness, `/readyz` for readiness.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::state::AppState;

pub async fn health(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let status: Option<(i64, i64, i64, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(&state.pool)
    .await?;

    // Which network this deployment indexes, from the RPC itself. Resolved in
    // the background at startup (never on this request path), so `null` until
    // the first successful probe — an RPC outage cannot slow or fail /health.
    let passphrase = state.rpc.cached_network_passphrase();
    let network = passphrase.as_deref().map(network_name);
    // Sibling instances mounted under this origin (e.g. {"testnet": "/testnet"})
    // so clients can discover other networks without configuration.
    let mounts = (!state.mounts.is_empty()).then(|| {
        state
            .mounts
            .iter()
            .map(|(name, _)| (name.clone(), Value::String(format!("/{name}"))))
            .collect::<serde_json::Map<String, Value>>()
    });

    let Some((last, tip, ingested, errors, updated_at)) = status else {
        return Ok(Json(json!({
            "status": "starting",
            "network": network,
            "network_passphrase": passphrase,
            "mounts": mounts,
            "unrecovered_missed_ranges": 0,
            "version": state.build_info.version,
            "commit": state.build_info.commit,
            "build_time": state.build_info.build_time,
        })));
    };

    let unrecovered_missed_ranges: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM missed_ranges WHERE recovered_at IS NULL",
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or(0);

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (chrono::Utc::now() - updated_at).num_seconds();
    // Stale if the cursor hasn't advanced recently, or we're far behind.
    let healthy = indexer_fresh(
        lag_ledgers,
        secs_since_update,
        state.health_max_lag_ledgers,
        state.health_max_stale_secs,
    );

    Ok(Json(json!({
        "status": if healthy { "ok" } else { "degraded" },
        "network": network,
        "network_passphrase": passphrase,
        "mounts": mounts,
        "last_processed_ledger": last,
        "chain_tip_ledger": tip,
        "lag_ledgers": lag_ledgers,
        "seconds_since_cursor_update": secs_since_update,
        "events_ingested_total": ingested,
        "errors_total": errors,
        "unrecovered_missed_ranges": unrecovered_missed_ranges,
        "version": state.build_info.version,
        "commit": state.build_info.commit,
        "build_time": state.build_info.build_time,
    })))
}

/// Liveness probe: returns 200 if the process is running.
/// No database access required; used by orchestrators to detect dead processes.
pub async fn livez() -> StatusCode {
    StatusCode::OK
}

/// Whether the indexer cursor is recent and close enough to the chain tip.
/// Shared by `/health` (reports `degraded`) and, when opted in, `/readyz`.
pub fn indexer_fresh(lag_ledgers: i64, secs_since_update: i64, max_lag: i64, max_age_secs: i64) -> bool {
    secs_since_update <= max_age_secs && lag_ledgers < max_lag
}

/// Readiness probe: whether this API instance can serve requests. It checks
/// only what the API itself needs (database reachable, schema compatible).
/// Indexer freshness is not gated on by default, so an indexer stall does not
/// pull every API replica out of rotation (#579). Set
/// `READYZ_REQUIRE_FRESH_INDEX=true` to also require a fresh indexer.
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    if sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
        .is_err()
    {
        return not_ready("database unreachable");
    }
    if lumenqraph_core::schema::check(&state.pool).await.is_err() {
        return not_ready("database schema incompatible with this binary");
    }
    if state.config.readyz_require_fresh_index {
        let cursor: Result<Option<(i64, i64, chrono::DateTime<chrono::Utc>)>, sqlx::Error> =
            sqlx::query_as(
                "SELECT last_processed_ledger, chain_tip_ledger, updated_at
                 FROM indexer_cursor WHERE id = 1",
            )
            .fetch_optional(&state.pool)
            .await;
        match cursor {
            Err(_) => return not_ready("indexer status unavailable"),
            Ok(None) => return not_ready("indexer has not started"),
            Ok(Some((last, tip, updated_at))) => {
                let lag_ledgers = (tip - last).max(0);
                let secs_since_update = (chrono::Utc::now() - updated_at).num_seconds();
                if !indexer_fresh(
                    lag_ledgers,
                    secs_since_update,
                    state.readyz_lag_threshold,
                    state.readyz_max_age_secs,
                ) {
                    return not_ready("indexer lagging or stale");
                }
            }
        }
    }
    (StatusCode::OK, Json(json!({ "status": "ready" })))
}

fn not_ready(reason: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "status": "not_ready", "reason": reason })),
    )
}

/// Short name for the well-known Stellar network passphrases.
fn network_name(passphrase: &str) -> &'static str {
    match passphrase {
        "Public Global Stellar Network ; September 2015" => "mainnet",
        "Test SDF Network ; September 2015" => "testnet",
        "Test SDF Future Network ; October 2022" => "futurenet",
        _ => "custom",
    }
}

#[cfg(test)]
mod tests {
    use super::indexer_fresh;

    #[test]
    fn fresh_when_recent_and_close_to_tip() {
        assert!(indexer_fresh(0, 10, 100, 120));
    }

    #[test]
    fn stale_when_cursor_not_advancing() {
        assert!(!indexer_fresh(0, 121, 100, 120));
    }

    #[test]
    fn stale_when_lagging_behind_tip() {
        assert!(!indexer_fresh(100, 0, 100, 120));
    }
}
