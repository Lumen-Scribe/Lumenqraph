//! Lumenqraph indexer CLI and process orchestration.
//!
//! Usage:
//!   lumenqraph-indexer                          # live tail (default)
//!   lumenqraph-indexer migrate                  # apply database migrations, then exit
//!   lumenqraph-indexer backfill [LEDGER]        # one-shot catch-up within RPC window
//!   lumenqraph-indexer deep-backfill --from N   # gapless history from a data-lake export
//!   lumenqraph-indexer recover-gaps             # replay all recorded missed ranges
//!   lumenqraph-indexer reenrich [--contract C] [--force]
//!   lumenqraph-indexer inspect <CONTRACT>       # print a contract's on-chain interface
//!   lumenqraph-indexer healthcheck               # GET own /health, exit 0 on 2xx (Docker HEALTHCHECK)
//!
//! `--help` and `--version` are handled by clap before any environment config
//! is read, so they work without `DATABASE_URL` or any RPC settings.
//!
//! Concurrency model: the live poller elects a single active instance via the
//! leader advisory lock (`INDEXER_LOCK_ID`). Migrations run under a separate
//! short lock (`MIGRATION_LOCK_ID`) so a standby never blocks anyone from
//! migrating.

mod backfill;
mod config;
mod convert;
mod cursor;
mod deep_backfill;
mod http;
mod keys;
mod poller;
#[cfg(test)]
mod projections_tests;
mod reenrich;
mod retention;
mod rpc_client;
mod specs;
mod state;
mod store;
mod timing;

#[cfg(test)]
mod projections_tests;

#[cfg(all(test, feature = "smoke-tests"))]
mod smoke;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use lumenqraph_core::ContractSpec;
use sqlx::postgres::PgConnection;
use tokio::sync::Mutex;
use tracing::info;

use config::Config;
use lumenqraph_core::ContractSpec;
use deep_backfill::{GalexieSource, HistoricalSource};
use reenrich::ReenrichOptions;
use rpc_client::RpcClient;

const INDEXER_LOCK_ID: i64 = 0x6c756d656e717261;
const MIGRATION_LOCK_ID: i64 = 0x6c756d656e717262;

/// Version, commit and build time shown by `--version` (clap prints the name).
fn version_string() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION.get_or_init(|| {
        format!(
            "{}\ncommit: {}\nbuilt: {}",
            env!("CARGO_PKG_VERSION"),
            option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
            option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
        )
    })
}

#[derive(Parser)]
#[command(
    name = "lumenqraph-indexer",
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
    /// Gapless history from a data-lake export.
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
    /// Container health probe (#469): GET this process's own `/health` on
    /// `INDEXER_HTTP_BIND_ADDR` and exit 0 on a 2xx response, 1 otherwise.
    /// `/health` is 503 while the indexer is degraded (#756), so Docker marks
    /// a stuck indexer unhealthy. Runs before any config/DB/RPC setup, which
    /// is what `docker-compose.full.yml`'s `lumenqraph-indexer healthcheck`
    /// healthcheck relies on.
    Healthcheck,
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
            .context("failed to acquire advisory-lock connection")?
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

/// Take the indexer leader lock without blocking, for one-shot commands
/// (`backfill`, `deep-backfill`, `recover-gaps`, `reenrich`). These write to the
/// same tables as the live poller and to `indexer_cursor`, so they must not run
/// beside it or beside each other (#710). Refuses with a clear message instead
/// of waiting. The lock is session-scoped: it is released when the returned
/// connection is dropped at the end of the command.
async fn try_one_shot_lock(pool: &sqlx::PgPool, command: &str) -> anyhow::Result<PgConnection> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire advisory-lock connection")?
        .detach();
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(INDEXER_LOCK_ID)
        .fetch_one(&mut conn)
        .await
        .context("failed to query the leader lock")?;
    anyhow::ensure!(
        acquired,
        "refusing to run `{command}`: another indexer or one-shot job holds the leader lock. \
         Stop the live indexer (or wait for the other job) and retry."
    );
    info!(command, "one-shot command holds the leader lock");
    Ok(conn)
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

/// Returns true when `ALLOW_NETWORK_CHANGE` is set to a truthy value. It lets
/// the database be re-pinned to a different Soroban network (#714).
fn allow_network_change() -> bool {
    matches!(
        std::env::var("ALLOW_NETWORK_CHANGE")
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
    // Parse arguments before touching any environment config so that
    // `--help` and `--version` succeed on a bare environment (#651).
    // Parsed before any config is read, so `--help`/`--version` need no env (#651).
    let matches = Cli::command().version(version_string()).get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    let command = cli.command.unwrap_or(Command::Run);

    // `healthcheck` runs before any config, database or RPC work so the
    // container probe works on a bare environment (#469): it GETs our own
    // /health on the HTTP bind address and exits 0 on 2xx, 1 otherwise.
    // /health is 503 while degraded (#756), so orchestrators see a stuck
    // indexer as unhealthy.
    if matches!(command, Command::Healthcheck) {
        let _ = dotenvy::dotenv();
        lumenqraph_core::healthcheck::run_and_exit(&http_bind_addr(), "/health");
    }

    let _ = dotenvy::dotenv();
    lumenqraph_core::telemetry::init_tracing();

    let config = Config::from_env().context("failed to load config")?;

    // Refuse (or warn about) index-all combined with state/key indexing before
    // any RPC work starts: with an empty CONTRACT_IDS filter every holder and
    // every active contract would be snapshotted each cycle (#746).
    config
        .validate_state_indexing()
        .context("unsupported state indexing configuration")?;
    if config.contract_ids.is_empty()
        && (config.key_indexing || config.state_indexing)
    {
        info!(
            max_state_keys_per_cycle = config.max_state_keys_per_cycle,
            max_state_contracts_per_cycle = config.max_state_contracts_per_cycle,
            "index-all state indexing is capped per cycle; remaining keys are deferred to later cycles"
        );
    }

    let db = Config::db_config()?;
    let pool = lumenqraph_core::db::connect_with_retry(&config.database_url, &db).await?;

    // Build the RPC client with all configured endpoints (#398).
    // RPC_URLS takes precedence over RPC_URL; both are parsed by Config::from_env.
    let rpc = RpcClient::with_urls_and_headers(
        config.rpc_urls.clone(),
        config.rpc_timeout_secs,
        config.rpc_headers.clone(),
    )
    .with_max_retry_after(std::time::Duration::from_secs(config.rpc_max_retry_after_secs));
    // Only commands that use the RPC need a reachable endpoint, and then only
    // one of them (#713). `migrate` and `deep-backfill` make no RPC calls.
    let live_passphrase = if matches!(command, Command::Migrate | Command::DeepBackfill { .. }) {
        None
    } else {
        let network_passphrase = rpc.validate_network_passphrases().await?;
        info!(%network_passphrase, endpoints = config.rpc_urls.len(), "validated RPC network identity");
        Some(network_passphrase)
    };

    match command {
        Command::Migrate => return run_migrations(&pool).await,
        Command::Inspect { contract_id } => return inspect(&rpc, &contract_id).await,
        _ => {}
    }

    // SKIP_MIGRATIONS lets the all-in-one entrypoint skip the migrator when it
    // has already run `lumenqraph-indexer migrate` (#603).
    if skip_migrations() {
        info!("SKIP_MIGRATIONS is set; skipping database migrations");
    } else {
        run_migrations(&pool).await?;
    }

    match command {
        Command::Backfill { ledger } => {
            let _lock = try_one_shot_lock(&pool, "backfill").await?;
            backfill::run(pool, rpc, config, ledger.map(i64::from).unwrap_or(0)).await
        }
        Command::DeepBackfill { from, to, source, input } => {
            let _lock = try_one_shot_lock(&pool, "deep-backfill").await?;
            run_deep_backfill(pool, config, from, to, source, input).await
        }
        Command::RecoverGaps => {
            let _lock = try_one_shot_lock(&pool, "recover-gaps").await?;
            let specs = specs::SpecCache::new(
                config.spec_cache_max_entries,
                config.spec_fetch_concurrency,
            );
            poller::recover_gaps(&pool, &rpc, &config, &specs).await
        }
        Command::Reenrich { contract, force } => {
            let _lock = try_one_shot_lock(&pool, "reenrich").await?;
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
            info!(
                rpc_host = %rpc.active_endpoint_host(),
                rpc_endpoints = config.rpc_urls.len(),
                contracts = ?config.contract_ids,
                poll_secs = config.poll_interval_secs,
                "starting lumenqraph indexer (live)"
            );
            // Default to loopback so /health and /metrics are not exposed on
            // public interfaces (#605). Set INDEXER_HTTP_BIND_ADDR=0.0.0.0:9090
            // (or the private network address) to allow Prometheus scraping.
            let bind_addr = std::env::var("INDEXER_HTTP_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:9090".to_string());
            http::start_http_server(Arc::new(pool.clone()), Arc::clone(&specs), &bind_addr).await?;
            poller::run(pool, rpc, config, specs).await
        }
        Command::Migrate | Command::Inspect { .. } | Command::Healthcheck => {
            unreachable!("handled above")
        }
    }
}

#[cfg(test)]
mod one_shot_lock_tests {
    use super::*;
    use lumenqraph_core::db_test::TestDb;

    /// A one-shot command must refuse while the live indexer holds the leader lock,
    /// and two one-shot commands must not overlap (#710).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn one_shot_commands_refuse_while_the_leader_lock_is_held() {
        let db = TestDb::new_with_migrations("../../migrations").await;

        // Stand in for the live indexer.
        let leader = LeaderLock::acquire(db.pool()).await.expect("leader acquires");

        let err = try_one_shot_lock(db.pool(), "backfill")
            .await
            .expect_err("backfill must not run beside the live indexer");
        assert!(err.to_string().contains("refusing to run `backfill`"), "got: {err}");

        // Release the leader. Dropping its connection releases the session lock; the
        // server may take a moment to notice, so retry briefly.
        drop(leader);
        let mut held = None;
        for _ in 0..50 {
            match try_one_shot_lock(db.pool(), "reenrich").await {
                Ok(conn) => {
                    held = Some(conn);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        let first = held.expect("one-shot runs once the leader is gone");

        // A second one-shot must not start while the first is still running.
        let err = try_one_shot_lock(db.pool(), "recover-gaps")
            .await
            .expect_err("one-shot commands must not overlap");
        assert!(err.to_string().contains("refusing to run `recover-gaps`"), "got: {err}");

        drop(first);
    }
}
