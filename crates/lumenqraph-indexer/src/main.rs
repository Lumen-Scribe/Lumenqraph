//! Lumenqraph indexer — an always-on process that tails Soroban RPC and writes
//! decoded events into Postgres. It talks to nothing but the RPC and its own DB.
//!
//! Usage:
//!   lumenqraph-indexer                    # live tail (default)
//!   lumenqraph-indexer migrate            # apply database migrations, then exit
//!   lumenqraph-indexer backfill [LEDGER]  # one-shot catch-up within RPC window (~7 days) then exit
//!   lumenqraph-indexer deep-backfill [OPTIONS]  # gapless history from a data-lake export (#84)
//!   lumenqraph-indexer recover-gaps       # replay all recorded missed ranges (#295)
//!   lumenqraph-indexer reenrich          # re-enrich historical events with newly-available specs
//!   lumenqraph-indexer inspect <CONTRACT> # print a contract's on-chain interface
//!
//! deep-backfill options:
//!   --from <LEDGER>   Start ledger (required)
//!   --to   <LEDGER>   End ledger   (default: max / run to EOF of input)
//!   --source <TYPE>   Source type: galexie, horizon (default: galexie)
//!   --input <PATH>    Input file(s); use '-' for stdin; may be repeated
//!
//! Migrations (#473):
//!   `migrate` is the explicit schema step: it needs only `DATABASE_URL`, runs
//!   the embedded migrations under `MIGRATION_LOCK_ID` and exits. Deployments
//!   run it before any service starts (compose init service, Fly
//!   `release_command`, `run-all-in-one.sh`). The other commands still apply
//!   pending migrations themselves for local development, unless
//!   `SKIP_MIGRATIONS=true`, in which case they only verify that the schema is
//!   current and refuse to start otherwise.
//!
//! Concurrency model:
//!   The live poller elects a single active instance via the leader advisory
//!   lock (`INDEXER_LOCK_ID`). One-shot maintenance commands (`backfill`,
//!   `reenrich`, `deep-backfill`, `recover-gaps`) do NOT take the leader lock:
//!   their writes are idempotent (`ON CONFLICT DO NOTHING`) and they never
//!   advance the live cursor, so they can safely run alongside a live indexer.
//!   Migrations run under a short, separate migration lock
//!   (`MIGRATION_LOCK_ID`), taken before the leader lock, so a hot standby
//!   never stops anyone from migrating.

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
// The end-to-end smoke test is gated behind the `smoke-tests` feature (as well
// as `#[ignore]`) so it is never compiled or run by a plain `cargo test`,
// including in offline CI. See CONTRIBUTING.md → "Smoke tests".
#[cfg(all(test, feature = "smoke-tests"))]
mod smoke;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use lumenqraph_core::schema::{self, SchemaStatus};
use sqlx::postgres::PgPoolOptions;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use config::Config;
use deep_backfill::{GalexieSource, HistoricalSource};
use rpc_client::RpcClient;

/// Postgres advisory lock id used to elect a single active indexer.
const INDEXER_LOCK_ID: i64 = 0x6c756d656e717261; // "lumenqra" as i64

/// Postgres advisory lock id used to serialize migrations across processes.
/// This is deliberately distinct from `INDEXER_LOCK_ID` so that running
/// migrations never blocks on (or is blocked by) the live leader lock.
const MIGRATION_LOCK_ID: i64 = 0x6c756d656e717262; // "lumenqrb" as i64

const USAGE: &str = "\
Lumenqraph indexer — tails Soroban RPC and writes decoded events into Postgres

Usage:
  lumenqraph-indexer [run]                 live tail (default)
  lumenqraph-indexer migrate               apply database migrations, then exit
  lumenqraph-indexer backfill [LEDGER]     one-shot catch-up within the RPC window
  lumenqraph-indexer deep-backfill --from <LEDGER> [--to <LEDGER>]
                     [--source galexie|horizon] [--input <PATH>]...
  lumenqraph-indexer recover-gaps          replay all recorded missed ranges
  lumenqraph-indexer reenrich [--contract <ID>] [--force]
  lumenqraph-indexer inspect <CONTRACT>    print a contract's on-chain interface
  lumenqraph-indexer --version | --help";

enum Command {
    /// Live tail (default when no subcommand is given).
    Run,
    /// Run database migrations and exit.
    Migrate,
    /// One-shot catch-up within the RPC window (~7 days) then exit.
    Backfill { ledger: Option<u32> },
    /// Gapless history from a data-lake export (#84).
    DeepBackfill {
        from: u32,
        to: Option<u32>,
        source: String,
        input: Vec<String>,
    },
    /// Replay all recorded missed ledger ranges (#295).
    RecoverGaps,
    /// Re-enrich historical events with newly-available specs.
    Reenrich { contract: Option<String>, force: bool },
    /// Print a contract's on-chain interface.
    Inspect { contract_id: String },
    /// Print version or usage and exit.
    Version,
    Help,
}

/// Parse the command line (without the program name). Unknown subcommands and
/// flags are errors, so a typo never silently starts the live poller (#376).
fn parse_command(args: &[String]) -> anyhow::Result<Command> {
    let Some((sub, rest)) = args.split_first() else {
        return Ok(Command::Run);
    };
    let no_args = |cmd: Command| -> anyhow::Result<Command> {
        match rest.first() {
            None => Ok(cmd),
            Some(extra) => anyhow::bail!("unexpected argument \"{extra}\" for {sub}"),
        }
    };
    match sub.as_str() {
        "run" => no_args(Command::Run),
        "migrate" => no_args(Command::Migrate),
        "recover-gaps" => no_args(Command::RecoverGaps),
        "--version" | "-V" => Ok(Command::Version),
        "--help" | "-h" | "help" => Ok(Command::Help),
        "backfill" => {
            if rest.len() > 1 {
                anyhow::bail!("backfill takes at most one LEDGER argument");
            }
            Ok(Command::Backfill {
                ledger: parse_backfill_ledger(rest.first().map(String::as_str))?,
            })
        }
        "inspect" => match rest {
            [contract_id] => Ok(Command::Inspect {
                contract_id: contract_id.clone(),
            }),
            _ => anyhow::bail!("usage: lumenqraph-indexer inspect <CONTRACT>"),
        },
        "reenrich" => {
            let mut contract = None;
            let mut force = false;
            let mut it = rest.iter();
            while let Some(arg) = it.next() {
                match arg.as_str() {
                    "--force" => force = true,
                    "--contract" => {
                        contract = Some(it.next().context("--contract requires a contract id")?.clone())
                    }
                    flag if flag.starts_with("--contract=") => {
                        contract = Some(flag.trim_start_matches("--contract=").to_string())
                    }
                    other => anyhow::bail!("unknown reenrich flag: {other}"),
                }
            }
            Ok(Command::Reenrich { contract, force })
        }
        "deep-backfill" => {
            let mut from = None;
            let mut to = None;
            let mut source = "galexie".to_string();
            let mut input = Vec::new();
            let mut it = rest.iter();
            while let Some(arg) = it.next() {
                let (flag, inline) = match arg.split_once('=') {
                    Some((flag, value)) if flag.starts_with("--") => (flag, Some(value.to_string())),
                    _ => (arg.as_str(), None),
                };
                let mut value = || -> anyhow::Result<String> {
                    match &inline {
                        Some(v) => Ok(v.clone()),
                        None => it.next().cloned().with_context(|| format!("{flag} requires a value")),
                    }
                };
                match flag {
                    "--from" => from = Some(value()?.parse::<u32>().context("--from: invalid ledger number")?),
                    "--to" => to = Some(value()?.parse::<u32>().context("--to: invalid ledger number")?),
                    "--source" => source = value()?,
                    "--input" => input.push(value()?),
                    other => anyhow::bail!("unknown deep-backfill flag: {other}"),
                }
            }
            let from = from.context(
                "deep-backfill requires --from <ledger>\n\
                 Example: lumenqraph-indexer deep-backfill \
                 --from 1000000 --input /data/export.ndjson",
            )?;
            Ok(Command::DeepBackfill { from, to, source, input })
        }
        other => anyhow::bail!("unknown subcommand \"{other}\"\n\n{USAGE}"),
    }
}

/// Parse the optional `backfill` ledger argument.
///
/// Returns `Ok(None)` when the argument is absent (caller falls back to
/// `START_LEDGER`). Returns an error when the argument is present but is not a
/// valid positive integer, so a typo like `51_000_000` or `5100000O` fails
/// loudly instead of silently backfilling a different range.
fn parse_backfill_ledger(arg: Option<&str>) -> anyhow::Result<Option<u32>> {
    match arg {
        None => Ok(None),
        Some(raw) => match raw.parse::<u32>() {
            Ok(ledger) if ledger > 0 => Ok(Some(ledger)),
            _ => anyhow::bail!(
                "invalid ledger \"{}\": expected a positive integer",
                raw
            ),
        },
    }
}

/// A leader lock held on a dedicated Postgres connection that is never
/// returned to the pool. Advisory locks are session-scoped, so the lock lives
/// exactly as long as this connection. If the connection drops (idle timeout,
/// network blip, `pg_terminate_backend`), the lock is released by Postgres and
/// the holder must stop polling so a standby can take over.
struct LeaderLock {
    conn: Arc<tokio::sync::Mutex<sqlx::PgConnection>>,
}

impl LeaderLock {
    /// Acquire the leader lock on a dedicated connection. If another instance
    /// holds it, block until it is released (hot standby).
    async fn acquire(pool: &sqlx::PgPool) -> anyhow::Result<Self> {
        // Detach a connection from the pool so it is never handed back while
        // we hold the session-scoped advisory lock.
        let mut conn = pool
            .acquire()
            .await
            .context("failed to acquire dedicated connection for leader lock")?
            .detach();

        info!("acquiring indexer leader lock (id {})", INDEXER_LOCK_ID);
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(INDEXER_LOCK_ID)
            .fetch_one(&mut conn)
            .await
            .context("failed to acquire advisory lock")?;

        if !acquired {
            info!(
                "another indexer instance holds the leader lock; \
                 blocking until it releases (this instance will become a hot standby)"
            );
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(INDEXER_LOCK_ID)
                .execute(&mut conn)
                .await
                .context("failed to acquire advisory lock (blocking)")?;
        }

        info!("indexer leader lock acquired; this instance is now active");
        Ok(Self {
            conn: Arc::new(tokio::sync::Mutex::new(conn)),
        })
    }

    /// Spawn a task that pings the lock-holding connection periodically. If the
    /// connection is lost, the task exits the process so the orchestrator can
    /// restart it and a standby can take over. This makes leadership loss fail
    /// fast instead of silently continuing to poll without the lock.
    fn spawn_keepalive(&self) {
        let conn = self.conn.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let mut conn = conn.lock().await;
                if let Err(err) = sqlx::query("SELECT 1").execute(&mut *conn).await {
                    tracing::error!(
                        error = %err,
                        "leader lock connection lost; stopping indexer to avoid split-brain"
                    );
                    std::process::exit(1);
                }
            }
        });
    }
}

/// Run migrations under a short, dedicated advisory lock so concurrent
/// processes serialize migrations without contending on the leader lock.
/// The lock is held on a dedicated connection for the duration of the run and
/// released when that connection is dropped.
async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire dedicated connection for migration lock")?
        .detach();

    info!("acquiring migration lock (id {})", MIGRATION_LOCK_ID);
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut conn)
        .await
        .context("failed to acquire migration lock")?;

    let result = schema::MIGRATOR
        .run(pool)
        .await
        .context("failed to run migrations");

    // Release the migration lock by dropping the dedicated connection.
    drop(conn);

    result
}

/// Bring the schema up to date before running any other command: migrate,
/// or, with `SKIP_MIGRATIONS=true` (a separate `migrate` step already ran),
/// only verify and refuse to start against an older schema.
async fn prepare_schema(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    if env_bool("SKIP_MIGRATIONS") {
        info!("SKIP_MIGRATIONS=true; verifying schema only");
    } else {
        run_migrations(pool).await?;
    }
    match schema::check(pool).await? {
        SchemaStatus::Current => {}
        SchemaStatus::Ahead { unknown_versions } => tracing::warn!(
            ?unknown_versions,
            "database schema is newer than this binary (rolling upgrade in progress?)"
        ),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse the CLI first so `--help`, `--version` and unknown subcommands
    // never touch the environment or the database.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = parse_command(&args)?;
    match command {
        Command::Version => {
            println!(
                "lumenqraph-indexer {}\ncommit: {}\nbuilt: {}",
                env!("CARGO_PKG_VERSION"),
                option_env!("LUMENQRAPH_GIT_SHA").unwrap_or("unknown"),
                option_env!("LUMENQRAPH_BUILD_TIME").unwrap_or("unknown"),
            );
            return Ok(());
        }
        Command::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        _ => {}
    }

    // Load .env if present (development convenience; no-op in production).
    let _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    // `migrate` needs only DATABASE_URL, so it can run as an init step without
    // the indexer's RPC configuration.
    if let Command::Migrate = command {
        let database_url =
            std::env::var("DATABASE_URL").context("missing required env var DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(env_parse_u64(
                "DATABASE_ACQUIRE_TIMEOUT_SECS",
                30,
            )))
            .connect(&database_url)
            .await
            .context("failed to connect to Postgres")?;
        info!("running database migrations");
        run_migrations(&pool).await?;
        info!("database migrations complete");
        return Ok(());
    }

    let config = Config::from_env().context("failed to load config")?;

    // Build the RPC client with all configured endpoints (#398).
    // RPC_URLS takes precedence over RPC_URL; both are parsed by Config::from_env.
    let rpc = RpcClient::with_urls(config.rpc_urls.clone(), config.rpc_timeout_secs);

    // `inspect` needs only RPC — handle it before touching the database.
    if let Command::Inspect { contract_id } = &command {
        return inspect(&rpc, contract_id).await;
    }

    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .min_connections(config.database_min_connections)
        .acquire_timeout(Duration::from_secs(env_parse_u64(
            "DATABASE_ACQUIRE_TIMEOUT_SECS",
            30,
        )))
        .idle_timeout(Duration::from_secs(env_parse_u64(
            "DATABASE_IDLE_TIMEOUT_SECS",
            600,
        )))
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;

    prepare_schema(&pool).await?;

    match command {
        Command::Migrate | Command::Inspect { .. } | Command::Version | Command::Help => {
            unreachable!("handled above")
        }
        Command::Reenrich { contract, force } => {
            if contract.is_some() || force {
                tracing::warn!("reenrich --contract/--force are not supported yet; ignoring");
            }
            info!("running in reenrich mode");
            reenrich::run_reenrich(pool, rpc, config).await
        }
        Command::Backfill { ledger } => {
            let from = ledger.map(i64::from).unwrap_or(config.start_ledger);
            info!(from, "running in backfill mode");
            backfill::run(pool, rpc, config, from).await
        }
        Command::DeepBackfill {
            from,
            to,
            source,
            input,
        } => run_deep_backfill(pool, config, from, to, &source, input).await,
        Command::RecoverGaps => {
            info!("running in recover-gaps mode");
            let specs =
                specs::SpecCache::new(config.spec_cache_max_entries, config.spec_fetch_concurrency);
            poller::recover_gaps(&pool, &rpc, &config, &specs).await
        }
        Command::Run => {
            // Live tail: elect a single active indexer via the leader lock.
            // Others block here and become hot standbys that take over on failure.
            let leader_lock = LeaderLock::acquire(&pool).await?;
            leader_lock.spawn_keepalive();

            info!(
                rpc = %config.rpc_url,
                rpc_endpoints = config.rpc_urls.len(),
                contracts = ?config.contract_ids,
                poll_secs = config.poll_interval_secs,
                "starting lumenqraph indexer (live)"
            );

            let specs = Arc::new(specs::SpecCache::new(
                config.spec_cache_max_entries,
                config.spec_fetch_concurrency,
            ));
            let bind_addr = std::env::var("INDEXER_HTTP_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:9090".to_string());
            http::start_http_server(Arc::new(pool.clone()), specs.clone(), &bind_addr).await?;

            // The lock is released when the process exits and its dedicated
            // connection closes.
            poller::run(pool, rpc, config, specs).await
        }
    }
}

fn env_bool(key: &str) -> bool {
    std::env::var(key)
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn env_parse_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Fetch a contract's deployed WASM and print its parsed interface as JSON.
async fn inspect(rpc: &RpcClient, contract_id: &str) -> anyhow::Result<()> {
    if !lumenqraph_core::is_valid_contract_id(contract_id) {
        anyhow::bail!("invalid contract id {contract_id:?}: expected a C… strkey");
    }
    let Some((wasm_hash, wasm)) = rpc.get_contract_wasm(contract_id).await? else {
        anyhow::bail!(
            "no WASM found for {contract_id} (not a contract, or a Stellar Asset Contract)"
        );
    };
    eprintln!("wasm hash {wasm_hash} ({} bytes)", wasm.len());
    match lumenqraph_core::ContractSpec::from_wasm(&wasm) {
        Some(parsed) => {
            if !parsed.complete {
                eprintln!(
                    "warning: interface is partial ({} entries skipped)",
                    parsed.skipped_entries
                );
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&parsed.spec.to_interface_json())?
            );
            Ok(())
        }
        None => anyhow::bail!("contract has no contractspecv0 interface section"),
    }
}

/// Build the requested historical source and dispatch to [`deep_backfill::run`].
async fn run_deep_backfill(
    pool: sqlx::PgPool,
    config: Config,
    from: u32,
    to: Option<u32>,
    source_type: &str,
    input: Vec<String>,
) -> anyhow::Result<()> {
    // Default to stdin when no --input is given.
    let mut inputs: Vec<PathBuf> = input.into_iter().map(PathBuf::from).collect();
    if inputs.is_empty() {
        inputs.push(PathBuf::from("-"));
    }

    let source: Box<dyn HistoricalSource> = match source_type {
        "galexie" => Box::new(GalexieSource::new(inputs)),
        "horizon" => {
            let base_url = inputs
                .first()
                .and_then(|p| p.to_str())
                .filter(|s| *s != "-")
                .unwrap_or("https://horizon.stellar.org");
            Box::new(deep_backfill::HorizonSource::new(base_url))
        }
        other => anyhow::bail!("unknown source type '{other}'; supported: galexie, horizon"),
    };

    deep_backfill::run(pool, config, source, i64::from(from), to.map(i64::from)).await
}
