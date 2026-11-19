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

#[cfg(test)]
mod projections_tests;

#[cfg(all(test, feature = "smoke-tests"))]
mod smoke;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use sqlx::PgConnection;
use tracing::info;

use config::Config;
use deep_backfill::{GalexieSource, HistoricalSource};
use lumenqraph_core::ContractSpec;
use reenrich::ReenrichOptions;
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
        /// Start ledger. When omitted, resumes at the persisted cursor + 1, or
        /// at START_LEDGER if there is no cursor yet. Fails if neither exists.
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
        /// Only re-enrich events from this contract (a `C...` strkey).
        #[arg(long)]
        contract: Option<String>,
        /// Recompute enrichment even for events that already have it.
        #[arg(long)]
        force: bool,
    },
    /// Print a contract's on-chain interface.
    Inspect { contract_id: String },
    /// Run database migrations and exit.
    Migrate,
}

/// Holds the session-level Postgres advisory lock that makes exactly one
/// live indexer the leader. The connection is pinged periodically; if it dies
/// the lock is gone and the process exits so another instance can take over.
struct LeaderLock {
    conn: Arc<tokio::sync::Mutex<PgConnection>>,
}

impl LeaderLock {
    async fn acquire(pool: &sqlx::PgPool) -> anyhow::Result<Self> {
        let mut conn = pool
            .acquire()
            .await
            .context("failed to acquire advisory lock (blocking)")?
            .detach();

        info!("acquiring indexer leader lock (id {})", INDEXER_LOCK_ID);
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
        info!("indexer leader lock acquired; this instance is now active");
        Ok(Self {
            conn: Arc::new(tokio::sync::Mutex::new(conn)),
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

/// Returns true when `SKIP_MIGRATIONS` is set to a truthy value.
fn skip_migrations() -> bool {
    matches!(
        std::env::var("SKIP_MIGRATIONS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
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
        other => anyhow::bail!("unknown source type '{other}'; supported: galexie"),
    };
    deep_backfill::run(pool, config, source, i64::from(from), to.map(i64::from)).await
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

/// Resolve the start ledger for `backfill` (#711).
///
/// An explicit ledger always wins. Otherwise resume after the persisted cursor
/// (`cursor + 1`), falling back to `START_LEDGER` when there is no cursor. A
/// cursor of 0 means nothing has been processed yet, so it does not count.
/// Never returns 0: starting from ledger 0 would backfill the whole retention
/// window instead of resuming.
fn resolve_backfill_start(
    explicit: Option<u32>,
    cursor: Option<i64>,
    start_ledger: i64,
) -> anyhow::Result<i64> {
    if let Some(ledger) = explicit {
        return Ok(i64::from(ledger));
    }
    if let Some(last) = cursor.filter(|c| *c > 0) {
        return Ok(last + 1);
    }
    if start_ledger > 0 {
        return Ok(start_ledger);
    }
    anyhow::bail!(
        "no backfill start: no persisted cursor and START_LEDGER is unset; pass a ledger explicitly"
    )
}

/// Check arguments that can be validated without touching the database or the
/// RPC, so a typo fails immediately (#712).
fn validate_args(command: &Command) -> anyhow::Result<()> {
    if let Command::Reenrich {
        contract: Some(contract),
        ..
    } = command
    {
        anyhow::ensure!(
            lumenqraph_core::is_valid_contract_id(contract),
            "invalid --contract '{contract}': expected a Soroban contract ID (C...)"
        );
    }
    Ok(())
}

/// Bind address for the health/metrics HTTP server. Defaults to loopback so
/// /health and /metrics are not exposed on public interfaces (#605).
fn http_bind_addr() -> String {
    std::env::var("INDEXER_HTTP_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    lumenqraph_core::telemetry::init_tracing();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Run);
    validate_args(&command)?;
    let config = Config::from_env().context("failed to load config")?;

    let db = Config::db_config()?;
    let pool = lumenqraph_core::db::connect_with_retry(&config.database_url, &db).await?;

    // Build the RPC client with all configured endpoints (#398).
    // RPC_URLS takes precedence over RPC_URL; both are parsed by Config::from_env.
    let rpc = RpcClient::with_urls(config.rpc_urls.clone(), config.rpc_timeout_secs);
    let network_passphrase = rpc.validate_network_passphrases().await?;
    info!(%network_passphrase, endpoints = config.rpc_urls.len(), "validated RPC network identity");

    match &command {
        Command::Migrate => return run_migrations(&pool).await,
        Command::Inspect { contract_id } => return inspect(&rpc, contract_id).await,
        _ => {}
    }

    // SKIP_MIGRATIONS lets the all-in-one entrypoint skip the migrator when it
    // has already run `lumenqraph-indexer migrate` (#603).
    if !skip_migrations() {
        run_migrations(&pool).await?;
    } else {
        info!("SKIP_MIGRATIONS is set; skipping database migrations");
    }

    match command {
        Command::Backfill { ledger } => {
            let cursor = cursor::read_last_processed(&pool).await?;
            let from = resolve_backfill_start(ledger, cursor, config.start_ledger)?;
            info!(from, "running in backfill mode");
            backfill::run(pool, rpc, config, from).await
        }
        Command::DeepBackfill {
            from,
            to,
            source,
            input,
        } => run_deep_backfill(pool, config, from, to, source, input).await,
        Command::RecoverGaps => {
            let specs = specs::SpecCache::new(
                config.spec_cache_max_entries,
                config.spec_fetch_concurrency,
            );
            poller::recover_gaps(&pool, &rpc, &config, &specs).await
        }
        Command::Reenrich { contract, force } => {
            reenrich::run_reenrich(pool, rpc, config, ReenrichOptions { contract, force }).await
        }
        Command::Run => {
            let lock = LeaderLock::acquire(&pool).await?;
            lock.spawn_keepalive();

            info!(
                rpc_host = %rpc.active_endpoint_host(),
                rpc_endpoints = config.rpc_urls.len(),
                contracts = ?config.contract_ids,
                poll_secs = config.poll_interval_secs,
                "starting lumenqraph indexer (live)"
            );

            let specs = Arc::new(specs::SpecCache::new(
                config.spec_cache_max_entries,
                config.spec_fetch_concurrency,
            ));
            http::start_http_server(Arc::new(pool.clone()), Arc::clone(&specs), &http_bind_addr())
                .await?;
            poller::run(pool, rpc, config, specs).await
        }
        Command::Migrate | Command::Inspect { .. } => unreachable!("handled above"),
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_backfill_start;

    #[test]
    fn explicit_ledger_wins_over_cursor_and_start_ledger() {
        assert_eq!(resolve_backfill_start(Some(500), Some(900), 100).unwrap(), 500);
    }

    #[test]
    fn resumes_after_cursor_when_no_ledger_given() {
        assert_eq!(resolve_backfill_start(None, Some(900), 100).unwrap(), 901);
    }

    #[test]
    fn falls_back_to_start_ledger_without_cursor() {
        assert_eq!(resolve_backfill_start(None, None, 100).unwrap(), 100);
    }

    #[test]
    fn zero_cursor_is_treated_as_no_cursor() {
        assert_eq!(resolve_backfill_start(None, Some(0), 100).unwrap(), 100);
    }

    #[test]
    fn reenrich_rejects_malformed_contract_before_any_io() {
        let bad = super::Command::Reenrich {
            contract: Some("CNOT-A-STRKEY".into()),
            force: false,
        };
        let err = super::validate_args(&bad).unwrap_err();
        assert!(err.to_string().contains("invalid --contract"));
    }

    #[test]
    fn reenrich_accepts_valid_contract_and_none() {
        let contract = stellar_strkey::Contract([7u8; 32]).to_string();
        let ok = super::Command::Reenrich {
            contract: Some(contract),
            force: true,
        };
        assert!(super::validate_args(&ok).is_ok());
        let none = super::Command::Reenrich {
            contract: None,
            force: false,
        };
        assert!(super::validate_args(&none).is_ok());
    }

    #[test]
    fn errors_when_neither_cursor_nor_start_ledger_exists() {
        let err = resolve_backfill_start(None, None, 0).unwrap_err();
        assert!(err.to_string().contains("pass a ledger explicitly"));
    }
}
