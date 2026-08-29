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

use crate::rpc_client::{DataEntry, RpcClient};
use crate::specs::SpecCache;

/// Max keys per `getLedgerEntries` RPC call when batching holder balance snapshots.
const MAX_BATCH_KEYS: usize = 100;

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
        specs.note_wasm_hash(pool, rpc, contract_id, hash, instance.last_modified_ledger).await;
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

/// Snapshot a batch of contract instances in a single `getLedgerEntries` call
/// with bounded concurrency. State indexing and upgrade detection both use the
/// instance entry, so this bundles both. Best-effort: errors are logged, never
/// propagated to the poller. Contracts are processed with bounded concurrency
/// to avoid overwhelming the RPC.
pub async fn snapshot_instances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_ids: &[String],
) {
    if contract_ids.is_empty() {
        return;
    }

    // Chunk contract IDs for RPC calls (avoid sending too many at once).
    const MAX_BATCH_INSTANCES: usize = 50;
    for chunk in contract_ids.chunks(MAX_BATCH_INSTANCES) {
        if let Err(e) = try_snapshot_instances_batch(pool, rpc, specs, chunk).await {
            warn!(error = %e, "batch instance snapshot failed");
        }
    }
}

async fn try_snapshot_instances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_ids: &[String],
) -> anyhow::Result<()> {
    let instances = rpc.get_contract_instances_batch(contract_ids).await?;

    for (contract_id, instance_opt) in contract_ids.iter().zip(instances.iter()) {
        let Some(instance) = instance_opt else {
            continue;
        };

        // Reading the instance revealed the current executable — detect upgrades.
        if let Some(hash) = &instance.wasm_hash {
            specs.note_wasm_hash(pool, rpc, contract_id, hash, instance.last_modified_ledger).await;
        }

        // Change detection: the instance's lastModifiedLedgerSeq only advances when
        // the instance (incl. its storage) actually changes.
        let latest: Option<i64> =
            sqlx::query_scalar("SELECT max(ledger) FROM contract_state WHERE contract_id = $1")
                .bind(contract_id)
                .fetch_one(pool)
                .await?;
        if latest == Some(instance.last_modified_ledger) {
            continue;
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
    }
    Ok(())
}

/// Snapshot a single contract-data entry (one key/value pair, e.g. a holder's
/// `Balance(Address)`) if it has changed since the last snapshot. Best-effort:
/// errors are logged, never propagated to the poller. `label` is an optional
/// grouping tag stored alongside the row (e.g. `"balance"`).
///
/// For bulk per-holder snapshots use [`snapshot_balances_batch`] instead.
#[allow(dead_code)]
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
    try_write_snapshot_data(pool, contract_id, key, durability, label, &entry).await
}

/// Snapshot per-holder balances discovered during a cycle in a small number of
/// batched `getLedgerEntries` calls instead of one call per holder.
///
/// Keys are chunked to `MAX_BATCH_KEYS` entries per RPC call. Change detection
/// and DB writes are unchanged from the per-entry path.
pub async fn snapshot_balances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    holders_by_contract: &std::collections::HashMap<String, std::collections::HashSet<String>>,
    contract_ids_filter: &[String],
    balance_key_symbol: &str,
    durability: ContractDataDurability,
) {
    use crate::keys;

    // Build the flat list of (contract_id, key, durability, label) to fetch.
    let mut batch: Vec<(String, ScVal, ContractDataDurability, &str)> = Vec::new();
    for (contract_id, holders) in holders_by_contract {
        if !contract_ids_filter.is_empty() && !contract_ids_filter.contains(contract_id) {
            continue;
        }
        for holder in holders {
            match keys::balance_key(balance_key_symbol, holder) {
                Ok(key) => batch.push((contract_id.clone(), key, durability, "balance")),
                Err(e) => debug!(holder, error = %e, "skipping unbuildable balance key"),
            }
        }
    }

    if batch.is_empty() {
        return;
    }

    debug!(count = batch.len(), "batching holder balance key snapshots");

    for chunk in batch.chunks(MAX_BATCH_KEYS) {
        let rpc_keys: Vec<(String, ScVal, ContractDataDurability)> = chunk
            .iter()
            .map(|(cid, key, dur, _)| (cid.clone(), key.clone(), *dur))
            .collect();

        let results = match rpc.get_contract_data_batch(&rpc_keys).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "batch contract-data fetch failed");
                continue;
            }
        };

        for ((contract_id, key, dur, label), entry_opt) in chunk.iter().zip(results.iter()) {
            let Some(entry) = entry_opt else { continue };
            if let Err(e) =
                try_write_snapshot_data(pool, contract_id, key, *dur, Some(label), entry).await
            {
                warn!(contract_id = %contract_id, error = %e, "contract-data snapshot write failed");
            }
        }
    }
}

/// Snapshot per-key entries from configurable templates discovered during a cycle.
/// Keys are chunked to MAX_BATCH_KEYS entries per RPC call.
pub async fn snapshot_template_keys_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    template_keys_by_contract: &std::collections::HashMap<String, Vec<(usize, Vec<String>)>>,
    contract_ids_filter: &[String],
    templates: &[crate::keys::KeyTemplate],
) {
    // Build the flat list of (contract_id, key, durability, label) to fetch.
    let mut batch: Vec<(String, ScVal, ContractDataDurability, Option<String>)> = Vec::new();
    for (contract_id, entries) in template_keys_by_contract {
        if !contract_ids_filter.is_empty() && !contract_ids_filter.contains(contract_id) {
            continue;
        }
        for (template_idx, params) in entries {
            if let Some(template) = templates.get(*template_idx) {
                match template.build_key(params) {
                    Ok(key) => batch.push((
                        contract_id.clone(),
                        key,
                        template.durability,
                        template.label.clone(),
                    )),
                    Err(e) => debug!(error = %e, "skipping unbuildable template key"),
                }
            }
        }
    }

    if batch.is_empty() {
        return;
    }

    debug!(count = batch.len(), "batching template key snapshots");

    for chunk in batch.chunks(MAX_BATCH_KEYS) {
        let rpc_keys: Vec<(String, ScVal, ContractDataDurability)> = chunk
            .iter()
            .map(|(cid, key, dur, _)| (cid.clone(), key.clone(), *dur))
            .collect();

        let results = match rpc.get_contract_data_batch(&rpc_keys).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "batch contract-data fetch failed");
                continue;
            }
        };

        for ((contract_id, key, dur, label), entry_opt) in chunk.iter().zip(results.iter()) {
            let Some(entry) = entry_opt else { continue };
            if let Err(e) = try_write_snapshot_data(
                pool,
                contract_id,
                key,
                *dur,
                label.as_deref(),
                entry,
            )
            .await
            {
                warn!(contract_id = %contract_id, error = %e, "contract-data snapshot write failed");
            }
        }
    }
}

/// Write a single contract-data snapshot row after change detection. Shared by
/// the per-entry path (`try_snapshot_data`) and the batch path (`snapshot_balances_batch`).
async fn try_write_snapshot_data(
    pool: &PgPool,
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
    label: Option<&str>,
    entry: &DataEntry,
) -> anyhow::Result<()> {
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
