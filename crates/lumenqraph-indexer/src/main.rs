//! Lumenqraph indexer — an always-on process that tails Soroban RPC and writes
//! decoded events into Postgres. It talks to nothing but the RPC and its own DB.
//!
//! Usage:
//!   lumenqraph-indexer                  # live tail (default)
//!   lumenqraph-indexer backfill [LEDGER]  # one-shot catch-up from LEDGER then exit

mod backfill;
mod config;
mod convert;
mod cursor;
mod poller;
mod rpc_client;
mod store;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use config::Config;
use rpc_client::RpcClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    let config = Config::from_env()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("failed to run migrations")?;

    let rpc = RpcClient::new(config.rpc_url.clone());

    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("backfill") {
        let from = args
            .get(2)
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(config.start_ledger);
        info!(from, "running in backfill mode");
        return backfill::run(pool, rpc, config, from).await;
    }

    info!(
        rpc = %config.rpc_url,
        contracts = ?config.contract_ids,
        poll_secs = config.poll_interval_secs,
        "starting lumenqraph indexer (live)"
    );
    poller::run(pool, rpc, config).await
}
