//! Lumenqraph API — the public read + management surface. A separate binary
//! from the indexer, reading the same Postgres, so API traffic can never
//! interrupt ingestion.

mod auth;
mod call_cache;
mod concurrency_limit;
mod config;
mod error;
mod extract;
mod graphql;
mod key_cache;
mod metrics;
mod metrics_middleware;
mod openapi;
mod pagination;
mod rate_limit;
mod read_cost_limit;
mod request_id;
mod routes;
mod rpc;
mod specs;
mod state;
mod url_validation;

use std::future::IntoFuture;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::extract::DefaultBodyLimit;
use axum::http;
use axum::middleware;
use tower_http::compression::CompressionLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use call_cache::CallCache;
use concurrency_limit::ConcurrencyLimiter;
use config::ApiConfig;
use key_cache::KeyCache;
use rate_limit::RateLimiter;
use read_cost_limit::ReadCostLimitConfig;
use state::{AppState, BuildInfo};


fn version_string() -> String {
    format!(
        "lumenqraph-api {}\ncommit: {}\nbuilt: {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
        option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
    )
}

/// CORS for the configured allowlist. `CORS_ALLOWED_ORIGINS` was validated by
/// `ApiConfig::from_env`, so an invalid entry cannot reach this point (#581).
fn build_cors_layer(origins_str: &str) -> tower_http::cors::CorsLayer {
    use tower_http::cors::CorsLayer;

    if origins_str.trim() == "*" {
        info!("CORS: allowing all origins (permissive mode)");
        return CorsLayer::permissive();
    }
    let origins = config::parse_cors_origins(origins_str).expect("validated by ApiConfig::from_env");
    if origins.is_empty() {
        info!("CORS: no allowed origins configured; browsers will enforce same-origin policy");
        return CorsLayer::new();
    }
    info!(origins = ?origins, "CORS: allowing configured origins");
    let allowed: Vec<http::HeaderValue> = origins
        .iter()
        .map(|origin| http::HeaderValue::from_str(origin).expect("origin is a valid header value"))
        .collect();

    CorsLayer::new()
        .allow_origin(allowed)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PATCH,
            axum::http::Method::OPTIONS,
            axum::http::Method::DELETE,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
            axum::http::header::IF_NONE_MATCH,
            http::HeaderName::from_static("x-api-key"),
            http::HeaderName::from_static("x-request-id"),
        ])
        .expose_headers([
            http::HeaderName::from_static("x-request-id"),
            http::HeaderName::from_static("retry-after"),
            http::HeaderName::from_static("x-ratelimit-limit"),
            http::HeaderName::from_static("x-ratelimit-remaining"),
            http::HeaderName::from_static("etag"),
            http::HeaderName::from_static("deprecation"),
            http::HeaderName::from_static("link"),
        ])
        .max_age(Duration::from_secs(600))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("--version") {
        println!("{}", version_string());
        return Ok(());
    }

    // healthcheck: probe our own /health on localhost and exit 0/1 (#469).
    // Used by the Dockerfile HEALTHCHECK and compose; needs no curl.
    if args.get(1).map(|s| s.as_str()) == Some("healthcheck") {
        let _ = dotenvy::dotenv();
        let bind = std::env::var("API_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
        lumenqraph_core::healthcheck::run_and_exit(&bind, "/health");
    }

    // --print-openapi: dump the generated OpenAPI 3.1 spec as JSON to stdout
    // and exit.  Used by the CI drift check to compare against the committed
    // openapi.yaml without starting the full server or connecting to Postgres.
    //
    //   cargo run -p lumenqraph-api -- --print-openapi | \
    //     python3 scripts/check_openapi_drift.py openapi.yaml -
    if args.get(1).map(|s| s.as_str()) == Some("--print-openapi") {
        use utoipa::OpenApi as _;
        let spec = openapi::ApiDoc::openapi();
        let json = serde_json::to_string_pretty(&spec)?;
        println!("{json}");
        return Ok(());
    }

    let _ = dotenvy::dotenv();
    lumenqraph_core::telemetry::init_tracing();

    // Parse every setting once, up front: an invalid value exits here with a
    // message naming the variable instead of silently becoming the default.
    let config = Arc::new(ApiConfig::from_env()?);
    info!(config = ?config, "effective configuration");

    // Validate CONTRACT_IDS at startup so a misconfigured address is caught
    // immediately rather than silently ignored or causing runtime errors.
    lumenqraph_core::parse_contract_ids(
        &std::env::var("CONTRACT_IDS").unwrap_or_default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let pool = lumenqraph_core::db::connect_with_retry(&config.database_url, &config.db).await?;

    // Migrations are applied by `lumenqraph-indexer migrate`, not here. Refuse
    // to serve against a schema older than this binary instead of answering
    // with 500s for missing tables (#473).
    match lumenqraph_core::schema::check(&pool).await? {
        lumenqraph_core::schema::SchemaStatus::Current => {}
        lumenqraph_core::schema::SchemaStatus::Ahead { unknown_versions } => tracing::warn!(
            ?unknown_versions,
            "database schema is newer than this binary (rolling upgrade in progress?)"
        ),
    }

    let call_cache = Arc::new(CallCache::new(
        config.call_cache_max_entries,
        config.call_cache_ttl_secs,
    ));

    let build_info = Arc::new(BuildInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: option_env!("LUMENQRAPH_GIT_SHA")
            .unwrap_or("unknown")
            .to_string(),
        build_time: option_env!("LUMENQRAPH_BUILD_TIME")
            .unwrap_or("unknown")
            .to_string(),
    });

    // Cancelled by `shutdown_signal`; long-lived SSE streams watch it so they end
    // promptly instead of holding graceful shutdown until the hard timeout (#571).
    let shutdown = tokio_util::sync::CancellationToken::new();

    let state = AppState {
        pool,
        require_auth: config.require_auth,
        anon_rate_limit: config.anon_rate_limit,
        limiter,
        http_requests: Arc::new(AtomicU64::new(0)),
        rpc: rpc::RpcClient::new(config.rpc_url.clone(), config.rpc_timeout_secs),
        specs: Arc::new(specs::SpecCache::new()),
        mounts: Arc::new(config.mounts.clone()),
        rpc_limiter,
        rpc_require_auth: config.rpc_require_auth,
        rpc_anon_rate_limit: config.rpc_anon_rate_limit,
        metrics: Arc::new(metrics_middleware::MetricsCollector::new()),
        call_cache,
        build_info,
        concurrency_limiter: Arc::new(ConcurrencyLimiter::new()),
        max_concurrent_per_ip: config.max_concurrent_per_ip,
        read_cost_limit_config: ReadCostLimitConfig {
            max_request_size: config.read_max_request_size,
            max_args_size: config.read_max_args_size,
        },
        readyz_lag_threshold: config.readyz_lag_threshold,
        readyz_max_age_secs: config.readyz_max_age_secs,
        health_max_lag_ledgers: config.health_max_lag_ledgers,
        health_max_stale_secs: config.health_max_stale_secs,
        metrics_require_auth: config.metrics_require_auth,
        proxy_limiter,
        webhook_limiter,
        config: Arc::clone(&config),
        key_cache: Arc::new(KeyCache::new(
            config.api_key_cache_capacity,
            std::time::Duration::from_secs(config.api_key_cache_ttl_secs),
            std::time::Duration::from_secs(config.api_key_cache_neg_ttl_secs),
        )),
        ip_config: auth::IpConfig::from_env(),
        audit_tx: Some(audit_tx),
        audit_dropped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        shutdown: shutdown.clone(),
    };

    // Resolve the RPC network passphrase in the background so /health never
    // waits on the RPC (#578).
    let rpc = state.rpc.clone();
    tokio::spawn(async move { rpc.warm_network_passphrase().await });

    let bind_addr = config.bind_addr.clone();
    let max_body_bytes = config.max_body_bytes;
    let request_timeout_secs = config.request_timeout_secs;
    info!(max_body_bytes, "enforcing request body size limit");
    info!(request_timeout_secs, "enforcing request timeout");

    let app = routes::router(state)
        .layer(DefaultBodyLimit::max(max_body_bytes as usize))
        .layer(TimeoutLayer::new(Duration::from_secs(request_timeout_secs)))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(build_cors_layer(&config.cors_allowed_origins));

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;
    info!(addr = %bind_addr, "lumenqraph api listening");

    let shutdown_timeout_secs = config.shutdown_timeout_secs;
    let (drain_started_tx, drain_started_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal(shutdown_timeout_secs).await;
        let _ = drain_started_tx.send(());
    })
    .into_future();

    // Bound the drain, not the server's lifetime: the hard deadline starts only
    // once a shutdown signal has arrived. A slow or stuck client cannot keep the
    // process alive during a rolling restart, and the API serves until signalled.
    tokio::select! {
        result = server => result?,
        _ = force_exit_after_drain(drain_started_rx, shutdown_timeout_secs) => {
            info!(
                timeout_secs = shutdown_timeout_secs,
                "shutdown timeout reached; forcing exit"
            );
        }
    }
    Ok(())
}

/// Resolves `timeout_secs` after the shutdown signal, and never before it. If
/// the server stops without a signal, the sender is dropped and this stays
/// pending, so the server's own result is what `main` returns.
async fn force_exit_after_drain(
    drain_started: tokio::sync::oneshot::Receiver<()>,
    timeout_secs: u64,
) {
    if drain_started.await.is_ok() {
        tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn shutdown_signal(shutdown_timeout_secs: u64) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    // End long-lived streams now so axum's graceful shutdown can finish.
    shutdown.cancel();
    info!(
        "shutdown signal received; draining in-flight requests (up to {}s)",
        shutdown_timeout_secs
    );
}

#[cfg(test)]
mod tests {
    /// #219 — CONTRACT_IDS startup validation in lumenqraph-api.
    ///
    /// The API calls `lumenqraph_core::parse_contract_ids` at startup and
    /// propagates the error, refusing to proceed. These tests exercise the same
    /// validation logic directly, without needing a live Postgres or bind
    /// address, to ensure the guard never silently regresses.
    mod contract_ids_startup_validation {
        #[test]
        fn rejects_g_strkey_account_address() {
            // A G… strkey is a Stellar account, not a Soroban contract.
            let raw = "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3BEJD4";
            let err = lumenqraph_core::parse_contract_ids(raw).unwrap_err();
            assert!(
                err.contains("invalid CONTRACT_ID"),
                "error should mention invalid CONTRACT_ID: {err}"
            );
            assert!(
                err.contains("GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3BEJD4"),
                "error should quote the bad id: {err}"
            );
        }

        #[test]
        fn rejects_garbage_string() {
            let raw = "not-a-contract-id";
            let err = lumenqraph_core::parse_contract_ids(raw).unwrap_err();
            assert!(
                err.contains("invalid CONTRACT_ID"),
                "garbage string should be rejected: {err}"
            );
        }

        #[test]
        fn rejects_too_many_contract_ids() {
            // getEvents supports at most 25 IDs; the parser enforces this.
            // Build 26 syntactically valid-looking (but fake) C-strkey placeholders
            // by using the same test id repeated — the count check fires before
            // strkey validation so any 26 non-empty tokens trigger it.
            // Use a real C-strkey so each individual ID passes strkey validation.
            let single = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";
            let raw = std::iter::repeat(single).take(26).collect::<Vec<_>>().join(",");
            let err = lumenqraph_core::parse_contract_ids(&raw).unwrap_err();
            assert!(
                err.contains("26"),
                "error should mention the count 26: {err}"
            );
        }

        #[test]
        fn accepts_empty_string() {
            // Empty CONTRACT_IDS means "index all" — must not be an error.
            let ids = lumenqraph_core::parse_contract_ids("").unwrap();
            assert!(ids.is_empty(), "empty string should yield zero IDs");
        }

        #[test]
        fn accepts_valid_c_strkey() {
            let raw = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";
            let ids = lumenqraph_core::parse_contract_ids(raw).unwrap();
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], raw);
        }

        #[test]
        fn mixed_valid_and_invalid_is_rejected() {
            let raw = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC,GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3BEJD4";
            let err = lumenqraph_core::parse_contract_ids(raw).unwrap_err();
            assert!(
                err.contains("invalid CONTRACT_ID"),
                "a G-strkey mixed with a valid C-strkey should be rejected: {err}"
            );
        }
    }
}

#[cfg(test)]
mod shutdown_deadline {
    use std::time::Duration;

    use super::force_exit_after_drain;

    /// The hard deadline must not run while the API is serving normally, or it
    /// exits 30s after boot (the bug this guards against).
    #[tokio::test]
    async fn deadline_does_not_fire_without_a_shutdown_signal() {
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let fired = tokio::time::timeout(Duration::from_millis(50), force_exit_after_drain(rx, 0)).await;
        assert!(fired.is_err(), "deadline fired before any shutdown signal");
    }

    #[tokio::test]
    async fn deadline_fires_once_the_signal_has_arrived() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), force_exit_after_drain(rx, 0))
            .await
            .expect("deadline should fire after the signal");
    }
}
