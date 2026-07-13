//! Lumenqraph indexer — an always-on process that tails Soroban RPC and writes
//! decoded events into Postgres. It talks to nothing but the RPC and its own DB.
//!
//! Usage:
//!   lumenqraph-indexer                    # live tail (default)
//!   lumenqraph-indexer backfill [LEDGER]  # one-shot catch-up from LEDGER then exit
//!   lumenqraph-indexer inspect <CONTRACT> # print a contract's on-chain interface

mod backfill;
mod config;
mod convert;
mod cursor;
mod poller;
mod rpc_client;
mod specs;
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
    let rpc = RpcClient::new(config.rpc_url.clone());
    let args: Vec<String> = std::env::args().collect();

    // `inspect` needs only RPC — handle it before touching the database.
    if args.get(1).map(String::as_str) == Some("inspect") {
        let contract_id = args
            .get(2)
            .context("usage: lumenqraph-indexer inspect <contract_id>")?;
        return inspect(&rpc, contract_id).await;
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("failed to run migrations")?;

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

/// Fetch a contract's deployed WASM and print its parsed interface as JSON.
async fn inspect(rpc: &RpcClient, contract_id: &str) -> anyhow::Result<()> {
    let Some((wasm_hash, wasm)) = rpc.get_contract_wasm(contract_id).await? else {
        anyhow::bail!(
            "no WASM found for {contract_id} (not a contract, or a Stellar Asset Contract)"
        );
    };
    eprintln!("wasm hash {wasm_hash} ({} bytes)", wasm.len());
    match lumenqraph_core::ContractSpec::from_wasm(&wasm) {
        Some(spec) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&spec.to_interface_json())?
            );
            Ok(())
        }
        None => anyhow::bail!("contract has no contractspecv0 interface section"),
    }
}
