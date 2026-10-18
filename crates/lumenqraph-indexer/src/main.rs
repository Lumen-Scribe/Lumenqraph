//! Lumenqraph indexer CLI and process orchestration.

mod backfill;
mod config;
mod convert;
mod cursor;
mod deep_backfill;
mod http;
mod keys;
mod poller;
mod reenrich;
mod retention;
mod rpc_client;
mod specs;
mod state;
mod store;

#[cfg(all(test, feature = "smoke-tests"))]
mod smoke;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use sqlx::postgres::{PgConnection, PgPoolOptions};
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use config::Config;
use deep_backfill::{GalexieSource, HistoricalSource};
use lumenqraph_core::ContractSpec;
use rpc_client::RpcClient;

const INDEXER_LOCK_ID: i64 = 0x6c756d656e717261;
const MIGRATION_LOCK_ID: i64 = 0x6c756d656e717262;

#[derive(Parser)]
#[command(
    name = "lumenqraph-indexer",
    version,
    about = "Lumenqraph indexer — tails Soroban RPC and writes decoded events into Postgres"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Live tail (the default).
    Run,
    /// One-shot catch-up within the RPC retention window.
    Backfill {
        /// Start ledger; defaults to the persisted cursor or START_LEDGER.
        ledger: Option<u32>,
    },
    /// Gapless history from a data-lake or Horizon source.
    DeepBackfill {
        #[arg(long)]
        from: u32,
        #[arg(long)]
        to: Option<u32>,
        #[arg(long, default_value = "galexie")]
        source: String,
        #[arg(long = "input")]
        input: Vec<String>,
    },
    /// Replay all recorded missed ranges.
    RecoverGaps,
    /// Re-enrich historical events with newly available specs.
    Reenrich {
        #[arg(long)]
        contract: Option<String>,
        #[arg(long)]
        force: bool,
    },
    /// Print a contract's on-chain interface.
    Inspect { contract_id: String },
    /// Run database migrations and exit.
    Migrate,
}

struct LeaderLock {
    conn: Arc<Mutex<PgConnection>>,
}

impl LeaderLock {
    async fn acquire(pool: &sqlx::PgPool) -> anyhow::Result<Self> {
        let mut conn = pool
            .acquire()
            .await
            .context("failed to acquire advisory-lock connection")?
            .detach();
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(INDEXER_LOCK_ID)
            .fetch_one(&mut conn)
            .await
            .context("failed to acquire advisory lock")?;
        if !acquired {
            info!("another indexer holds the leader lock; waiting for it to release");
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(INDEXER_LOCK_ID)
                .execute(&mut conn)
                .await
                .context("failed while waiting for leader lock")?;
        }
        info!("indexer leader lock acquired");
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn spawn_keepalive(&self) {
        let conn = Arc::clone(&self.conn);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let mut conn = conn.lock().await;
                if let Err(error) = sqlx::query("SELECT 1").execute(&mut *conn).await {
                    tracing::error!(%error, "leader lock connection lost; stopping indexer");
                    std::process::exit(1);
                }
            }
        });
    }
}

async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire migration-lock connection")?
        .detach();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut conn)
        .await
        .context("failed to acquire migration lock")?;
    let result = sqlx::migrate!("../../migrations").run(&mut conn).await;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut conn)
        .await;
    result.context("database migrations failed")
}

async fn run_deep_backfill(
    pool: sqlx::PgPool,
    config: Config,
    from: u32,
    to: Option<u32>,
    source_type: String,
    input: Vec<String>,
) -> anyhow::Result<()> {
    let source: Box<dyn HistoricalSource> = match source_type.as_str() {
        "galexie" => Box::new(GalexieSource::new(
            input.iter().map(PathBuf::from).collect(),
        )),
        "horizon" => {
            let base_url = input
                .first()
                .filter(|value| value.as_str() != "-")
                .map(String::as_str)
                .unwrap_or("https://horizon.stellar.org");
            Box::new(deep_backfill::HorizonSource::new(base_url))
        }
        other => anyhow::bail!("unknown source type '{other}'; supported: galexie, horizon"),
    };
    deep_backfill::run(
        pool,
        config,
        source,
        i64::from(from),
        to.map(i64::from),
    )
    .await
}

async fn inspect(rpc: &RpcClient, contract_id: &str) -> anyhow::Result<()> {
    let (wasm_hash, wasm) = rpc
        .get_contract_wasm(contract_id)
        .await?
        .context("no WASM found for contract")?;
    match ContractSpec::from_wasm(&wasm) {
        Some(parsed) => {
            info!(%wasm_hash, complete = parsed.complete, "interface");
            println!("{}", serde_json::to_string_pretty(&parsed.spec)?);
        }
        None => info!(%wasm_hash, "no contractspecv0 section found"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Run);
    let config = Config::from_env().context("failed to load config")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;
    let rpc = RpcClient::with_urls(config.rpc_urls.clone(), config.rpc_timeout_secs);

    match &command {
        Command::Migrate => return run_migrations(&pool).await,
        Command::Inspect { contract_id } => return inspect(&rpc, contract_id).await,
        _ => {}
    }

    run_migrations(&pool).await?;
    match command {
        Command::Backfill { ledger } => {
            backfill::run(pool, rpc, config, ledger.map(i64::from).unwrap_or(0)).await
        }
        Command::DeepBackfill { from, to, source, input } => {
            run_deep_backfill(pool, config, from, to, source, input).await
        }
        Command::RecoverGaps => {
            let specs = specs::SpecCache::new(
                config.spec_cache_max_entries,
                config.spec_fetch_concurrency,
            );
            poller::recover_gaps(&pool, &rpc, &config, &specs).await
        }
        Command::Reenrich { .. } => reenrich::run_reenrich(pool, rpc, config).await,
        Command::Run => {
            let lock = LeaderLock::acquire(&pool).await?;
            lock.spawn_keepalive();
            let specs = Arc::new(specs::SpecCache::new(
                config.spec_cache_max_entries,
                config.spec_fetch_concurrency,
            ));
            let pool_for_http = Arc::new(pool.clone());
            let bind_addr = std::env::var("INDEXER_HTTP_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:9090".to_string());
            http::start_http_server(pool_for_http, Arc::clone(&specs), &bind_addr).await?;
            poller::run(pool, rpc, config, specs).await
        }
        Command::Migrate | Command::Inspect { .. } => unreachable!(),
    }
}
