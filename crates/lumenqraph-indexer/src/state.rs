//! Contract state indexing: versioned snapshots of a contract's instance
//! storage.
//!
//! Events tell you what *happened*; this tells you what a contract currently
//! *holds*. We read the contract's instance ledger entry (the same one used to
//! find its WASM), decode its storage map to JSON, and store a new row whenever
//! the instance has changed since our last snapshot — so `contract_state`
//! becomes a time series: the newest row is current state, older rows are
//! history.
//!
//! Reading the instance also gives us the contract's current WASM hash, which we
//! feed back to the [`SpecCache`] so an upgraded contract's interface is
//! refreshed.

use lumenqraph_core::xdr;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use stellar_xdr::curr::{ContractDataDurability, Limits, ScVal, WriteXdr};
use tracing::{debug, warn};

use crate::rpc_client::RpcClient;
use crate::specs::SpecCache;

/// Snapshot a contract's instance storage if it has changed since the last
/// snapshot. Best-effort: errors are logged, never propagated to the poller.
pub async fn snapshot(pool: &PgPool, rpc: &RpcClient, specs: &SpecCache, contract_id: &str) {
    if let Err(e) = try_snapshot(pool, rpc, specs, contract_id).await {
        warn!(contract_id, error = %e, "state snapshot failed");
    }
}

async fn try_snapshot(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_id: &str,
) -> anyhow::Result<()> {
    let Some(instance) = rpc.get_contract_instance(contract_id).await? else {
        return Ok(());
    };

    // Reading the instance revealed the current executable — detect upgrades.
    if let Some(hash) = &instance.wasm_hash {
        specs.note_wasm_hash(pool, rpc, contract_id, hash).await;
    }

    // Change detection: the instance's lastModifiedLedgerSeq only advances when
    // the instance (incl. its storage) actually changes, so if we already have a
    // row at this ledger, there's nothing new to record.
    let latest: Option<i64> =
        sqlx::query_scalar("SELECT max(ledger) FROM contract_state WHERE contract_id = $1")
            .bind(contract_id)
            .fetch_one(pool)
            .await?;
    if latest == Some(instance.last_modified_ledger) {
        return Ok(());
    }

    let storage = decode_storage(&instance.storage);
    sqlx::query(
        "INSERT INTO contract_state (contract_id, ledger, storage)
         VALUES ($1, $2, $3)
         ON CONFLICT (contract_id, ledger) DO NOTHING",
    )
    .bind(contract_id)
    .bind(instance.last_modified_ledger)
    .bind(storage)
    .execute(pool)
    .await?;
    debug!(
        contract_id,
        ledger = instance.last_modified_ledger,
        "state snapshot recorded"
    );
    Ok(())
}

/// Snapshot a single contract-data entry (one key/value pair, e.g. a holder's
/// `Balance(Address)`) if it has changed since the last snapshot. Best-effort:
/// errors are logged, never propagated to the poller. `label` is an optional
/// grouping tag stored alongside the row (e.g. `"balance"`).
pub async fn snapshot_data(
    pool: &PgPool,
    rpc: &RpcClient,
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
    label: Option<&str>,
) {
    if let Err(e) = try_snapshot_data(pool, rpc, contract_id, key, durability, label).await {
        warn!(contract_id, error = %e, "contract-data snapshot failed");
    }
}

async fn try_snapshot_data(
    pool: &PgPool,
    rpc: &RpcClient,
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
    label: Option<&str>,
) -> anyhow::Result<()> {
    let Some(entry) = rpc.get_contract_data(contract_id, key, durability).await? else {
        // No entry (e.g. a holder whose balance was never written / has expired).
        return Ok(());
    };

    let key_xdr = key.to_xdr_base64(Limits::none())?;
    let key_hash = hex::encode(Sha256::digest(key_xdr.as_bytes()));
    let durability_str = match durability {
        ContractDataDurability::Persistent => "persistent",
        ContractDataDurability::Temporary => "temporary",
    };

    // Change detection: skip if we already have this key at this ledger.
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT max(ledger) FROM contract_data WHERE contract_id = $1 AND key_hash = $2",
    )
    .bind(contract_id)
    .bind(&key_hash)
    .fetch_one(pool)
    .await?;
    if latest == Some(entry.last_modified_ledger) {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(contract_id)
    .bind(&key_hash)
    .bind(decode_scval(key))
    .bind(&key_xdr)
    .bind(durability_str)
    .bind(entry.last_modified_ledger)
    .bind(decode_scval(&entry.val))
    .bind(label)
    .execute(pool)
    .await?;
    debug!(
        contract_id,
        ledger = entry.last_modified_ledger,
        label,
        "contract-data snapshot recorded"
    );
    Ok(())
}

/// Decode an instance-storage `ScVal` to friendly JSON by re-encoding it and
/// running it through the same decoder events use — so state and events share
/// one JSON shape (symbol-keyed maps become objects, i128 as decimal strings…).
fn decode_storage(storage: &ScVal) -> serde_json::Value {
    decode_scval(storage)
}

/// Decode any `ScVal` to friendly JSON via the shared event decoder.
fn decode_scval(v: &ScVal) -> serde_json::Value {
    match v.to_xdr_base64(Limits::none()) {
        Ok(b64) => xdr::decode_scval_base64(&b64),
        Err(_) => serde_json::Value::Null,
    }
}
