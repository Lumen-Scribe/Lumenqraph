//! `GET /health` — indexing freshness and lag vs the chain tip.

use axum::extract::State;
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

    // Which network this deployment indexes, from the RPC itself (cached after
    // the first successful probe) — lets clients like the explorer adapt
    // instead of asking the user. `null` if the RPC is unreachable right now.
    let passphrase = state.rpc.network_passphrase().await;
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
        })));
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (chrono::Utc::now() - updated_at).num_seconds();
    // Stale if the cursor hasn't advanced recently, or we're far behind.
    let healthy = secs_since_update <= 120 && lag_ledgers < 100;

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
    })))
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
