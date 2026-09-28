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

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::extract::DefaultBodyLimit;
use axum::http;
use axum::middleware;
use sqlx::postgres::PgPoolOptions;
use tower_http::compression::CompressionLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use call_cache::CallCache;
use concurrency_limit::ConcurrencyLimiter;
use config::{ApiConfig, DbConfig};
use key_cache::KeyCache;
use rate_limit::RateLimiter;
use read_cost_limit::ReadCostLimitConfig;
use state::{AppState, BuildInfo};

async fn connect_with_retry(database_url: &str, db: &DbConfig) -> anyhow::Result<sqlx::PgPool> {
    let max_retries = db.connect_retries;
    let mut attempt = 0;
    let mut retry_delay = Duration::from_secs(1);
    let max_delay = Duration::from_secs(30);
    loop {
        match PgPoolOptions::new()
            .max_connections(db.max_connections)
            .min_connections(db.min_connections)
            .acquire_timeout(Duration::from_secs(db.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(db.idle_timeout_secs))
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                if attempt > 0 {
                    info!(attempt, "successfully connected to Postgres after retries");
                }
                return Ok(pool);
            }
            Err(e) if attempt < max_retries => {
                attempt += 1;
                tracing::warn!(
                    error = %e,
                    attempt,
                    max_retries,
                    retry_delay_secs = retry_delay.as_secs(),
                    "failed to connect to Postgres, retrying…"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(max_delay);
            }
            Err(e) => {
                return Err(anyhow::anyhow!("failed to connect to Postgres after {max_retries} retries: {e}"));
            }
        }
    }
}

fn version_string() -> String {
    format!(
        "lumenqraph-api {}\ncommit: {}\nbuilt: {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
        option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
    )
}

fn build_cors_layer(origins_str: &str) -> tower_http::cors::CorsLayer {
    use tower_http::cors::CorsLayer;

    if origins_str == "*" {
        info!("CORS: allowing all origins (permissive mode)");
        CorsLayer::permissive()
    } else if origins_str.is_empty() {
        info!("CORS: no allowed origins configured; browsers will enforce same-origin policy by default");
        // Return a minimal CorsLayer with no allowed origins configured.
        // This means no CORS headers will be added, preserving the current behavior.
        CorsLayer::new()
    } else {
        info!("CORS: allowing specific origins");
        let origins: Vec<&str> = origins_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut cors = CorsLayer::new()
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
            .max_age(Duration::from_secs(600));

        for origin_str in origins {
            match origin_str.parse::<http::HeaderValue>() {
                Ok(origin) => {
                    cors = cors.allow_origin(origin);
                }
                Err(_) => {
                    info!(origin = origin_str, "invalid origin in CORS_ALLOWED_ORIGINS, skipping");
                }
            }
        }
        cors
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("--version") {
        println!("{}", version_string());
        return Ok(());
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
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    // Parse every setting once, up front: an invalid value exits here with a
    // message naming the variable instead of silently becoming the default.
    let config = Arc::new(ApiConfig::from_env()?);
    info!(config = ?config, "effective configuration");
    if config.webhook_encryption_key.is_none() {
        tracing::warn!(
            "WEBHOOK_ENCRYPTION_KEY is not set; webhook secrets are encrypted with an \
             insecure built-in test key. Set it (openssl rand -hex 32) in production."
        );
    }

    // Validate CONTRACT_IDS at startup so a misconfigured address is caught
    // immediately rather than silently ignored or causing runtime errors.
    lumenqraph_core::parse_contract_ids(
        &std::env::var("CONTRACT_IDS").unwrap_or_default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let pool = connect_with_retry(&config.database_url, &config.db).await?;

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

    let state = AppState {
        pool,
        require_auth: config.require_auth,
        anon_rate_limit: config.anon_rate_limit,
        limiter: Arc::new(RateLimiter::new()),
        http_requests: Arc::new(AtomicU64::new(0)),
        rpc: rpc::RpcClient::new(config.rpc_url.clone(), config.rpc_timeout_secs),
        specs: Arc::new(specs::SpecCache::new()),
        mounts: Arc::new(config.mounts.clone()),
        rpc_limiter: Arc::new(RateLimiter::new()),
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
        proxy_limiter: Arc::new(RateLimiter::new()),
        config: Arc::clone(&config),
        key_cache: Arc::new(KeyCache::new(2000)),
        ip_config: auth::IpConfig::from_env(),
        audit_tx: None,
        audit_dropped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        shutdown: tokio_util::sync::CancellationToken::new(),
    };

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
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_timeout_secs));

    // Wrap the graceful shutdown in a hard timeout so a slow or stuck client
    // cannot keep the process alive indefinitely during a rolling restart.
    match tokio::time::timeout(
        Duration::from_secs(shutdown_timeout_secs),
        server,
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            info!(
                timeout_secs = shutdown_timeout_secs,
                "shutdown timeout reached; forcing exit"
            );
        }
    }
    Ok(())
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
