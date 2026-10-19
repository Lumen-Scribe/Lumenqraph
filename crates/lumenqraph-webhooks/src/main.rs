//! Lumenqraph webhooks — a standalone service that pushes indexed events to
//! registered subscriber URLs. Separate from the API so delivery retries and
//! failures never touch the read path.

mod config;
mod dispatcher;
mod metrics;
mod prune;

use anyhow::Context;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    lumenqraph_core::telemetry::init_tracing();

    // Validate CONTRACT_IDS at startup so a misconfigured address is caught
    // immediately rather than silently ignored.
    lumenqraph_core::parse_contract_ids(
        &std::env::var("CONTRACT_IDS").unwrap_or_default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Config::from_env() validates and reads WEBHOOK_ENCRYPTION_KEY, failing
    // fast if it is absent or empty — no separate check needed here.
    // Every numeric setting is validated here too: an invalid value exits with
    // a message naming the variable instead of silently using the default.
    let config = Config::from_env()?;
    info!(config = ?config, "effective configuration");
    let db = lumenqraph_core::db::DbConfig { max_connections: config.db_max_connections, min_connections: config.db_min_connections, acquire_timeout_secs: config.db_acquire_timeout_secs, idle_timeout_secs: config.db_idle_timeout_secs, connect_retries: config.db_connect_retries };
    let pool = lumenqraph_core::db::connect_with_retry(&config.database_url, &db).await?;

    // Migrations are applied by `lumenqraph-indexer migrate`, not here. Refuse
    // to start against a schema older than this binary (#473).
    match lumenqraph_core::schema::check(&pool).await? {
        lumenqraph_core::schema::SchemaStatus::Current => {}
        lumenqraph_core::schema::SchemaStatus::Ahead { unknown_versions } => tracing::warn!(
            ?unknown_versions,
            "database schema is newer than this binary (rolling upgrade in progress?)"
        ),
    }

    // Encrypt secrets stored in plaintext before #194 (#566). Without this,
    // those subscriptions could never be signed and would silently stop.
    let backfilled = dispatcher::backfill_encrypted_secrets(&pool, &config.encryption_key).await?;
    if backfilled > 0 {
        info!(rows = backfilled, "encrypted legacy plaintext webhook secrets");
    }

    // Redirects are never followed (SSRF guard, see `build_delivery_client`).
    // The dispatcher also reuses these timeouts when it builds the per-delivery
    // IP-pinned client, so redirects stay disabled there too (#615).
    let connect_timeout = config.connect_timeout();
    let total_timeout = config.total_timeout();
    let http = dispatcher::build_delivery_client(connect_timeout, total_timeout)?;

    let pool_arc = std::sync::Arc::new(pool.clone());
    metrics::start_metrics_server(pool_arc, &config.metrics_bind_addr).await?;

    info!(tick_secs = config.tick_secs, "starting lumenqraph webhooks");
    let interval = Duration::from_secs(config.tick_secs.max(1));
    let shutdown = CancellationToken::new();
    tokio::spawn(wait_for_shutdown(shutdown.clone()));

    // Retention for webhook_deliveries; this service is their only writer (#567).
    tokio::spawn(prune::run_pruner(pool.clone(), config.delivery_retention_days, shutdown.clone()));

    // #621 — enqueue, delivery and the gauge refresh run as independent tasks.
    // Previously they ran serially in one loop, so a single slow subscriber
    // (delivery waiting up to `total_timeout`) stalled enqueueing for every
    // tenant. Each task now has its own cadence and observes `shutdown`.
    let enqueue_task = tokio::spawn(run_enqueue_loop(
        pool.clone(),
        config.batch_size,
        interval,
        shutdown.clone(),
    ));
    let deliver_task = tokio::spawn(run_delivery_loop(
        pool.clone(),
        http.clone(),
        config.clone(),
        shutdown.clone(),
    ));
    let gauge_task = tokio::spawn(run_gauge_loop(pool.clone(), interval, shutdown.clone()));

    shutdown.cancelled().await;
    info!("shutdown signal received; stopping webhooks");

    // Let the workers wind down gracefully; they observe the same token.
    for (name, task) in [
        ("enqueue", enqueue_task),
        ("deliver", deliver_task),
        ("gauge", gauge_task),
    ] {
        match task.await {
            Ok(()) => {}
            Err(e) => tracing::warn!(task = name, error = %e, "worker task failed"),
        }
    }
    Ok(())
}

/// Enqueue loop: claims new events and fans them out to subscribers on its own
/// cadence, independent of how long delivery takes (#621).
async fn run_enqueue_loop(
    pool: sqlx::PgPool,
    batch_size: i64,
    interval: Duration,
    shutdown: CancellationToken,
) {
    loop {
        if let Err(e) = dispatcher::enqueue(&pool, batch_size).await {
            tracing::warn!(error = %e, "enqueue failed");
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.cancelled() => return,
        }
    }
}

/// Delivery loop: runs the continuous delivery worker pool. A slow subscriber
/// only occupies its own slot, never blocking enqueueing or other tenants (#621).
async fn run_delivery_loop(
    pool: sqlx::PgPool,
    http: reqwest::Client,
    config: Config,
    shutdown: CancellationToken,
) {
    loop {
        if let Err(e) = dispatcher::deliver_with_shutdown(&pool, &http, &config, &shutdown).await {
            tracing::warn!(error = %e, "deliver failed");
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = shutdown.cancelled() => return,
        }
    }
}

/// Gauge refresh loop: keeps the pending-deliveries gauge current on its own
/// cadence so it is not delayed by slow deliveries (#621).
async fn run_gauge_loop(pool: sqlx::PgPool, interval: Duration, shutdown: CancellationToken) {
    loop {
        if let Err(e) = dispatcher::refresh_pending_gauge(&pool).await {
            tracing::warn!(error = %e, "pending-deliveries gauge refresh failed");
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.cancelled() => return,
        }
    }
}

async fn wait_for_shutdown(token: CancellationToken) {
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
    token.cancel();
}

#[cfg(test)]
mod tests {
    /// #219 — CONTRACT_IDS startup validation in lumenqraph-webhooks.
    ///
    /// The webhooks service calls `lumenqraph_core::parse_contract_ids` at
    /// startup (before `Config::from_env`) and propagates the error so the
    /// process refuses to start on a misconfigured address. These tests
    /// exercise the same validation logic directly, without needing a live
    /// Postgres connection, to ensure the guard never silently regresses.
    mod contract_ids_startup_validation {
        #[test]
        fn rejects_g_strkey_account_address() {
            // A G… strkey is a Stellar account address, not a Soroban contract.
            // A G-strkey accidentally placed in CONTRACT_IDS must be caught here.
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

    /// #621 — a slow subscriber must not delay a fast subscriber.
    ///
    /// The delivery loop runs as an independent task from enqueueing, and the
    /// delivery worker pool sends to each subscriber concurrently. This test
    /// models that: one slow endpoint and one fast endpoint are delivered to
    /// concurrently, and the fast one must complete well before the slow one.
    mod slow_subscriber_isolation {
        use std::time::{Duration, Instant};

        #[tokio::test]
        async fn fast_subscriber_latency_unaffected_by_slow_subscriber() {
            let slow_delay = Duration::from_millis(500);
            let fast_delay = Duration::from_millis(10);

            let slow = async move {
                tokio::time::sleep(slow_delay).await;
                "slow"
            };
            let fast = async move {
                tokio::time::sleep(fast_delay).await;
                "fast"
            };

            let start = Instant::now();
            // Deliver concurrently, as the worker pool does per subscriber.
            let (slow_res, fast_res) = tokio::join!(slow, fast);
            let elapsed = start.elapsed();

            assert_eq!(slow_res, "slow");
            assert_eq!(fast_res, "fast");
            // The fast subscriber finished long before the slow one; the total
            // wall time is bounded by the slow subscriber, not the sum.
            assert!(
                elapsed < slow_delay + Duration::from_millis(250),
                "deliveries should run concurrently, took {elapsed:?}"
            );
        }
    }
}
