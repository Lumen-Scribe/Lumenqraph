//! Lumenqraph API — the public read + management surface. A separate binary
//! from the indexer, reading the same Postgres, so API traffic can never
//! interrupt ingestion.

mod auth;
mod call_cache;
mod concurrency_limit;
mod error;
mod graphql;
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
use rate_limit::RateLimiter;
use read_cost_limit::ReadCostLimitConfig;
use state::{AppState, BuildInfo};

async fn connect_with_retry(database_url: &str, max_retries: u32) -> anyhow::Result<sqlx::PgPool> {
    let mut attempt = 0;
    let mut retry_delay = Duration::from_secs(1);
    let max_delay = Duration::from_secs(30);
    loop {
        match PgPoolOptions::new()
            .max_connections(env_parse("DATABASE_MAX_CONNECTIONS", 10u32))
            .min_connections(env_parse("DATABASE_MIN_CONNECTIONS", 1u32))
            .acquire_timeout(Duration::from_secs(env_parse(
                "DATABASE_ACQUIRE_TIMEOUT_SECS",
                30u64,
            )))
            .idle_timeout(Duration::from_secs(env_parse(
                "DATABASE_IDLE_TIMEOUT_SECS",
                600u64,
            )))
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

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn version_string() -> String {
    format!(
        "lumenqraph-api {}\ncommit: {}\nbuilt: {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
        option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
    )
}

fn build_cors_layer() -> tower_http::cors::CorsLayer {
    use tower_http::cors::CorsLayer;

    let origins_str = std::env::var("API_CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "same_origin".to_string());

    if origins_str == "*" {
        info!("CORS: allowing all origins (permissive mode)");
        CorsLayer::permissive()
    } else if origins_str.to_lowercase() == "same_origin" || origins_str.is_empty() {
        info!("CORS: allowing same-origin requests only");
        CorsLayer::very_permissive()
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PATCH,
                axum::http::Method::OPTIONS,
                axum::http::Method::DELETE,
            ])
            .allow_headers([axum::http::header::CONTENT_TYPE])
    } else {
        info!("CORS: allowing specific origins");
        let origins: Vec<&str> = origins_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut cors = CorsLayer::new();
        for origin_str in origins {
            match origin_str.parse::<http::HeaderValue>() {
                Ok(origin) => {
                    cors = cors.allow_origin(origin);
                }
                Err(_) => {
                    info!(origin = origin_str, "invalid origin in API_CORS_ALLOWED_ORIGINS, skipping");
                }
            }
        }
        cors.allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PATCH,
            axum::http::Method::OPTIONS,
            axum::http::Method::DELETE,
        ])
        .allow_headers([axum::http::header::CONTENT_TYPE])
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("--version") {
        println!("{}", version_string());
        return Ok(());
    }

    let _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    let database_url = std::env::var("DATABASE_URL").context("missing DATABASE_URL")?;
    
    // Validate webhook encryption key is set for production security
    if std::env::var("WEBHOOK_ENCRYPTION_KEY").is_err() {
        anyhow::bail!(
            "WEBHOOK_ENCRYPTION_KEY must be set (generate with: openssl rand -hex 32). \
             The default test key provides no security and must not be used in production."
        );
    }
    
    let bind_addr = std::env::var("API_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://soroban-testnet.stellar.org".to_string());
    let rpc_timeout_secs: u64 = env_parse("RPC_TIMEOUT_SECS", 30u64);
    let request_timeout_secs: u64 = env_parse("API_REQUEST_TIMEOUT_SECS", 60u64);

    let max_connect_retries = env_parse("DATABASE_CONNECT_RETRIES", 30u32);
    let pool = connect_with_retry(&database_url, max_connect_retries).await?;

    let call_cache = Arc::new(CallCache::new(
        env_parse("CALL_CACHE_MAX_ENTRIES", 1000usize),
        env_parse("CALL_CACHE_TTL_SECS", 5u64),
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
        require_auth: env_bool("REQUIRE_API_KEY", false),
        anon_rate_limit: env_parse("ANON_RATE_LIMIT_PER_MIN", 60),
        limiter: Arc::new(RateLimiter::new()),
        http_requests: Arc::new(AtomicU64::new(0)),
        rpc: rpc::RpcClient::new(rpc_url, rpc_timeout_secs),
        specs: Arc::new(specs::SpecCache::new()),
        mounts: Arc::new(routes::proxy::mounts_from_env()),
        rpc_limiter: Arc::new(RateLimiter::new()),
        rpc_require_auth: env_bool("RPC_REQUIRE_API_KEY", false),
        rpc_anon_rate_limit: env_parse("RPC_ROUTE_RATE_LIMIT_PER_MIN", 10),
        metrics: Arc::new(metrics_middleware::MetricsCollector::new()),
        call_cache,
        build_info,
        concurrency_limiter: Arc::new(ConcurrencyLimiter::new()),
        max_concurrent_per_ip: env_parse("MAX_CONCURRENT_PER_IP", 100),
        read_cost_limit_config: ReadCostLimitConfig {
            max_request_size: env_parse("READ_MAX_REQUEST_SIZE", 256 * 1024),
            max_args_size: env_parse("READ_MAX_ARGS_SIZE", 128 * 1024),
        },
        readyz_lag_threshold: env_parse("READYZ_LAG_THRESHOLD", 100i64),
        readyz_max_age_secs: env_parse("READYZ_MAX_AGE_SECS", 120i64),
        health_max_lag_ledgers: env_parse("HEALTH_MAX_LAG_LEDGERS", 100i64),
        health_max_stale_secs: env_parse("HEALTH_MAX_STALE_SECS", 120i64),
    };

    let cors_layer = build_cors_layer();
    let max_body_bytes = env_parse::<u32>("API_MAX_BODY_BYTES", 256 * 1024);
    info!(max_body_bytes, "enforcing request body size limit");
    info!(request_timeout_secs, "enforcing request timeout");

    let app = routes::router(state)
        .layer(DefaultBodyLimit::max(max_body_bytes as usize))
        .layer(TimeoutLayer::new(Duration::from_secs(request_timeout_secs)))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors_layer);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;
    info!(addr = %bind_addr, "lumenqraph api listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

async fn shutdown_signal() {
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
    info!("shutdown signal received; stopping api");
}
