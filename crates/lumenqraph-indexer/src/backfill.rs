//! Backfill mode: a one-shot catch-up that walks from a start ledger to the
//! current tip, then exits. Distinct from the live tail — used when registering
//! a contract that was already emitting events before the indexer came online.
//!
//! Bounded by RPC retention: `getEvents` only serves ~7 days of history, so
//! `from_ledger` is clamped to the oldest available ledger.

use sqlx::PgPool;
use tracing::{info, warn};

use crate::config::Config;
use crate::poller::fetch_and_store;
use crate::rpc_client::RpcClient;
use crate::{cursor, poller};

pub async fn run(
    pool: PgPool,
    rpc: RpcClient,
    config: Config,
    from_ledger: i64,
) -> anyhow::Result<()> {
    let tip = rpc.get_latest_ledger().await?;
    let oldest = tip - poller::max_lookback();
    let start = from_ledger.max(oldest).max(1);
    if start > from_ledger {
        warn!(
            requested = from_ledger,
            clamped_to = start,
            "backfill start is older than RPC retention; clamping"
        );
    }
    info!(from = start, to = tip, "starting backfill");

    let inserted = fetch_and_store(&pool, &rpc, &config, start, tip).await?;
    cursor::write_progress(&pool, tip, tip, inserted).await?;
    info!(inserted, up_to_ledger = tip, "backfill complete");
    Ok(())
}
