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
//!
//! # Contract-data key hashes
//!
//! Each contract-data entry is identified by `ledger_key_hash`, the hex-encoded
//! SHA-256 of the XDR-encoded `LedgerKey::ContractData { contract, key,
//! durability }`. This is the standard Stellar ledger-key hash: it includes the
//! durability, so persistent and temporary entries with an identical `ScVal` key
//! are distinct entries, and clients can derive it from a key they already know.
//!
//! The legacy `key_hash` column (SHA-256 of the base64 key text, durability
//! ignored) is still populated as a deprecated alias for one release.

use lumenqraph_core::xdr;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::str::FromStr;
use stellar_xdr::curr::{
    AccountId, AlphaNum12, AlphaNum4, Asset, AssetCode12, AssetCode4, ContractDataDurability,
    ContractId, Hash, LedgerKey, LedgerKeyAccount, LedgerKeyContractData, LedgerKeyTrustLine,
    Limits, PublicKey, ScAddress, ScVal, TrustLineAsset, Uint256, WriteXdr,
};
use tracing::{debug, warn};

use crate::keys;
use crate::rpc_client::{DataEntry, RpcClient};
use crate::specs::SpecCache;

/// Max keys per `getLedgerEntries` RPC call when batching holder balance snapshots.
const MAX_BATCH_KEYS: usize = 100;

/// Snapshot a batch of contract instances. Contract IDs are chunked into
/// `getLedgerEntries` calls, and each chunk's change detection and writes are
/// done with a single grouped `SELECT max(ledger)` plus one multi-row
/// `INSERT … ON CONFLICT DO NOTHING` (no per-contract round-trips). State
/// indexing and upgrade detection both use the instance entry, so this bundles
/// both. Best-effort: errors are logged, never propagated to the poller.
///
/// `max_per_cycle` caps how many contracts are snapshotted this cycle (0 means
/// unlimited). When the input exceeds the cap, contracts are prioritised by
/// last-snapshot age (oldest / never-snapshotted first) and the remainder is
/// deferred to later cycles. Returns the number of contracts deferred.
pub async fn snapshot_instances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_ids: &[String],
    max_per_cycle: usize,
) -> usize {
    if contract_ids.is_empty() {
        return 0;
    }

    let (selected, deferred) =
        cap_and_prioritise_instances(pool, contract_ids, max_per_cycle).await;
    if deferred > 0 {
        debug!(
            deferred,
            selected = selected.len(),
            "deferring contract instance snapshots to later cycles"
        );
    }

    // Chunk contract IDs for RPC calls (avoid sending too many at once).
    const MAX_BATCH_INSTANCES: usize = 50;
    for chunk in selected.chunks(MAX_BATCH_INSTANCES) {
        if let Err(e) = try_snapshot_instances_batch(pool, rpc, specs, chunk).await {
            warn!(error = %e, "batch instance snapshot failed");
        }
    }
    deferred
}

/// Apply the per-cycle cap to a set of contract IDs, ordering by last-snapshot
/// age (never-snapshotted first, then oldest `max(ledger)`). Returns the
/// selected IDs and the number deferred.
async fn cap_and_prioritise_instances(
    pool: &PgPool,
    contract_ids: &[String],
    max_per_cycle: usize,
) -> (Vec<String>, usize) {
    if max_per_cycle == 0 || contract_ids.len() <= max_per_cycle {
        return (contract_ids.to_vec(), 0);
    }

    let latest_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT contract_id, max(ledger) FROM contract_state
         WHERE contract_id = ANY($1)
         GROUP BY contract_id",
    )
    .bind(contract_ids)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let latest: std::collections::HashMap<String, i64> = latest_rows.into_iter().collect();

    let mut ordered: Vec<(String, i64)> = contract_ids
        .iter()
        .map(|cid| (cid.clone(), latest.get(cid).copied().unwrap_or(i64::MIN)))
        .collect();
    ordered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    let deferred = ordered.len() - max_per_cycle;
    let selected = ordered
        .into_iter()
        .take(max_per_cycle)
        .map(|(cid, _)| cid)
        .collect();
    (selected, deferred)
}

async fn try_snapshot_instances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_ids: &[String],
) -> anyhow::Result<()> {
    let instances = rpc.get_contract_instances_batch(contract_ids).await?;

    // Reading the instance revealed the current executable — detect upgrades.
    for (contract_id, instance_opt) in contract_ids.iter().zip(instances.iter()) {
        if let Some(instance) = instance_opt {
            if let Some(hash) = &instance.wasm_hash {
                specs
                    .note_wasm_hash(pool, rpc, contract_id, hash, instance.last_modified_ledger)
                    .await;
            }
        }
    }

    // One grouped query fetches the latest ledger for every contract in the chunk.
    let latest_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT contract_id, max(ledger) FROM contract_state
         WHERE contract_id = ANY($1)
         GROUP BY contract_id",
    )
    .bind(contract_ids)
    .fetch_all(pool)
    .await?;
    let latest: std::collections::HashMap<String, i64> = latest_rows.into_iter().collect();

    // Build the changed rows. The instance's lastModifiedLedgerSeq only advances
    // when the instance (incl. its storage) actually changes.
    let mut changed_ids: Vec<String> = Vec::new();
    let mut changed_ledgers: Vec<i64> = Vec::new();
    let mut changed_storage: Vec<serde_json::Value> = Vec::new();
    for (contract_id, instance_opt) in contract_ids.iter().zip(instances.iter()) {
        let Some(instance) = instance_opt else {
            continue;
        };
        if latest.get(contract_id) == Some(&instance.last_modified_ledger) {
            continue;
        }
        changed_ids.push(contract_id.clone());
        changed_ledgers.push(instance.last_modified_ledger);
        changed_storage.push(decode_storage(&instance.storage));
    }

    if changed_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_state (contract_id, ledger, storage)
         SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::jsonb[])
         ON CONFLICT (contract_id, ledger) DO NOTHING",
    )
    .bind(&changed_ids)
    .bind(&changed_ledgers)
    .bind(&changed_storage)
    .execute(pool)
    .await?;
    debug!(count = changed_ids.len(), "state snapshots recorded");
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
    let entry_opt = rpc.get_contract_data(contract_id, key, durability).await?;
    let key_xdr = key.to_xdr_base64(Limits::none())?;
    let key_hash = hex::encode(Sha256::digest(key_xdr.as_bytes()));
    match entry_opt {
        Some(entry) => {
            try_write_snapshot_data(pool, contract_id, key, durability, label, &entry).await
        }
        None => {
            // No entry (e.g. a holder whose balance was never written / has expired).
            // If we have history for this key, record a tombstone so `/data` stops
            // reporting the stale value.
            write_tombstone_if_needed(pool, contract_id, &key_hash, durability, label).await
        }
    }
}

/// Snapshot per-holder balances discovered during a cycle in a small number of
/// batched `getLedgerEntries` calls instead of one call per holder.
///
/// Keys are deduplicated by `(contract_id, key_hash)` and chunked to
/// `MAX_BATCH_KEYS` entries per RPC call. Per chunk, change detection is one
/// grouped `SELECT max(ledger)` and writes are one multi-row `UNNEST` insert.
/// Keys that return no entry but have history get a tombstone row.
///
/// `max_per_cycle` caps how many keys are snapshotted this cycle (0 means
/// unlimited). When the batch exceeds the cap, keys are prioritised by
/// last-snapshot age (oldest / never-snapshotted first) and the remainder is
/// deferred to later cycles. Returns the number of keys deferred.
pub async fn snapshot_balances_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    holders_by_contract: &std::collections::HashMap<String, std::collections::HashSet<String>>,
    contract_ids_filter: &[String],
    balance_key_symbol: &str,
    durability: ContractDataDurability,
    max_per_cycle: usize,
) -> usize {
    // Build the flat list of (contract_id, key, durability, label) to fetch,
    // deduplicated by (contract_id, key_hash).
    let mut batch: Vec<(String, ScVal, ContractDataDurability, &str)> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    // Classic-account holders of SACs: (contract_id, holder, ledger key).
    let mut trustline_batch: Vec<(String, String, LedgerKey)> = Vec::new();
    let mut trustline_seen: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    for (contract_id, holders) in holders_by_contract {
        if !contract_ids_filter.is_empty() && !contract_ids_filter.contains(contract_id) {
            continue;
        }
        // Detect SACs once per contract; non-SACs keep the contract-data path.
        let sac_asset = if holders.iter().any(|h| is_classic_account(h)) {
            detect_sac_asset(rpc, contract_id).await
        } else {
            None
        };
        for holder in holders {
            if let Some(asset) = &sac_asset {
                if is_classic_account(holder) {
                    match trustline_ledger_key(holder, asset) {
                        Ok(ledger_key) => {
                            if trustline_seen.insert((contract_id.clone(), holder.clone())) {
                                trustline_batch.push((
                                    contract_id.clone(),
                                    holder.clone(),
                                    ledger_key,
                                ));
                            }
                        }
                        Err(e) => {
                            debug!(holder, error = %e, "skipping unbuildable trustline key")
                        }
                    }
                    continue;
                }
            }
            match keys::balance_key(balance_key_symbol, holder) {
                Ok(key) => {
                    let key_hash = match key_hash_of(&key) {
                        Ok(h) => h,
                        Err(e) => {
                            debug!(holder, error = %e, "skipping unhashable balance key");
                            continue;
                        }
                    };
                    if seen.insert((contract_id.clone(), key_hash)) {
                        batch.push((contract_id.clone(), key, durability, "balance"));
                    }
                }
                Err(e) => debug!(holder, error = %e, "skipping unbuildable balance key"),
            }
        }
    }

    if batch.is_empty() {
        return 0;
    }

    let (batch, deferred) = cap_and_prioritise_keys(pool, batch, max_per_cycle).await;
    if deferred > 0 {
        debug!(
            deferred,
            selected = batch.len(),
            "deferring holder balance key snapshots to later cycles"
        );
    }

    debug!(
        contract_data = batch.len(),
        trustlines = trustline_batch.len(),
        "batching holder balance key snapshots"
    );

    for chunk in batch.chunks(MAX_BATCH_KEYS) {
        let owned: Vec<(String, ScVal, ContractDataDurability, Option<String>)> = chunk
            .iter()
            .map(|(cid, key, dur, label)| (cid.clone(), key.clone(), *dur, Some(label.to_string())))
            .collect();
        if let Err(e) = snapshot_data_chunk(pool, rpc, &owned).await {
            warn!(error = %e, "batch contract-data snapshot failed");
        }
    }
    deferred
}

/// Apply the per-cycle cap to a batch of contract-data keys, ordering by
/// last-snapshot age (never-snapshotted first, then oldest `max(ledger)`).
/// Returns the selected batch and the number of keys deferred.
async fn cap_and_prioritise_keys<L>(
    pool: &PgPool,
    batch: Vec<(String, ScVal, ContractDataDurability, L)>,
    max_per_cycle: usize,
) -> (Vec<(String, ScVal, ContractDataDurability, L)>, usize) {
    if max_per_cycle == 0 || batch.len() <= max_per_cycle {
        return (batch, 0);
    }

    // Compute key hashes up front so we can look up last-snapshot ledgers.
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(batch.len());
    for (_, key, _, _) in &batch {
        hashes.push(key_hash_of(key).ok());
    }

    let contract_ids: Vec<String> = batch.iter().map(|(cid, _, _, _)| cid.clone()).collect();
    let key_hashes: Vec<String> = hashes.iter().flatten().cloned().collect();
    let latest_rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT contract_id, key_hash, max(ledger) FROM contract_data
         WHERE contract_id = ANY($1) AND key_hash = ANY($2)
         GROUP BY contract_id, key_hash",
    )
    .bind(&contract_ids)
    .bind(&key_hashes)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let latest: std::collections::HashMap<(String, String), i64> = latest_rows
        .into_iter()
        .map(|(cid, kh, ledger)| ((cid, kh), ledger))
        .collect();

    // Pair each item with its last-snapshot ledger (i64::MIN if never seen).
    let mut indexed: Vec<(usize, i64)> = batch
        .iter()
        .enumerate()
        .map(|(idx, (cid, _, _, _))| {
            let last = hashes[idx]
                .as_ref()
                .and_then(|kh| latest.get(&(cid.clone(), kh.clone())).copied())
                .unwrap_or(i64::MIN);
            (idx, last)
        })
        .collect();
    indexed.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    let deferred = indexed.len() - max_per_cycle;
    let mut slots: Vec<Option<(String, ScVal, ContractDataDurability, L)>> =
        batch.into_iter().map(Some).collect();
    let selected: Vec<(String, ScVal, ContractDataDurability, L)> = indexed
        .into_iter()
        .take(max_per_cycle)
        .filter_map(|(idx, _)| slots[idx].take())
        .collect();
    (selected, deferred)
}

/// Snapshot per-key entries from configurable templates discovered during a cycle.
/// Keys are deduplicated by `(contract_id, key_hash)` and chunked to
/// `MAX_BATCH_KEYS` entries per RPC call. Per chunk, change detection is one
/// grouped `SELECT max(ledger)` and writes are one multi-row `UNNEST` insert.
/// Keys that return no entry but have history get a tombstone row.
///
/// `max_per_cycle` caps how many keys are snapshotted this cycle (0 means
/// unlimited). When the batch exceeds the cap, keys are prioritised by
/// last-snapshot age (oldest / never-snapshotted first) and the remainder is
/// deferred to later cycles. Returns the number of keys deferred.
pub async fn snapshot_template_keys_batch(
    pool: &PgPool,
    rpc: &RpcClient,
    template_keys_by_contract: &std::collections::HashMap<
        String,
        std::collections::HashSet<(usize, Vec<String>)>,
    >,
    contract_ids_filter: &[String],
    templates: &[crate::keys::KeyTemplate],
    max_per_cycle: usize,
) -> usize {
    // Build the flat list of (contract_id, key, durability, label) to fetch,
    // deduplicated by (contract_id, key_hash).
    let mut batch: Vec<(String, ScVal, ContractDataDurability, Option<String>)> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for (contract_id, entries) in template_keys_by_contract {
        if !contract_ids_filter.is_empty() && !contract_ids_filter.contains(contract_id) {
            continue;
        }
        for (template_idx, params) in entries {
            if let Some(template) = templates.get(*template_idx) {
                match template.build_key(params) {
                    Ok(key) => {
                        let key_hash = match key_hash_of(&key) {
                            Ok(h) => h,
                            Err(e) => {
                                debug!(error = %e, "skipping unhashable template key");
                                continue;
                            }
                        };
                        if seen.insert((contract_id.clone(), key_hash)) {
                            batch.push((
                                contract_id.clone(),
                                key,
                                template.durability,
                                template.label.clone(),
                            ));
                        }
                    }
                    Err(e) => debug!(error = %e, "skipping unbuildable template key"),
                }
            }
        }
    }

    if batch.is_empty() {
        return 0;
    }

    let (batch, deferred) = cap_and_prioritise_keys(pool, batch, max_per_cycle).await;
    if deferred > 0 {
        debug!(
            deferred,
            selected = batch.len(),
            "deferring template key snapshots to later cycles"
        );
    }

    debug!(count = batch.len(), "batching template key snapshots");

    for chunk in batch.chunks(MAX_BATCH_KEYS) {
        if let Err(e) = snapshot_data_chunk(pool, rpc, chunk).await {
            warn!(error = %e, "batch contract-data snapshot failed");
        }
    }
    deferred
}

/// Fetch one chunk of contract-data keys via a single `getLedgerEntries` call,
/// then do change detection with one grouped query and write all changed rows
/// (including tombstones for keys that vanished) with one multi-row insert.
async fn snapshot_data_chunk(
    pool: &PgPool,
    rpc: &RpcClient,
    chunk: &[(String, ScVal, ContractDataDurability, Option<String>)],
) -> anyhow::Result<()> {
    let rpc_keys: Vec<(String, ScVal, ContractDataDurability)> = chunk
        .iter()
        .map(|(cid, key, dur, _)| (cid.clone(), key.clone(), *dur))
        .collect();

    let results = rpc.get_contract_data_batch(&rpc_keys).await?;

    // Precompute key hashes and durability strings for the chunk.
    let mut key_hashes: Vec<String> = Vec::with_capacity(chunk.len());
    let mut durability_strs: Vec<&str> = Vec::with_capacity(chunk.len());
    for (_, key, dur, _) in chunk {
        key_hashes.push(key_hash_of(key)?);
        durability_strs.push(durability_str(*dur));
    }

    // One grouped query fetches the latest ledger for every key in the chunk.
    let contract_ids: Vec<String> = chunk.iter().map(|(cid, _, _, _)| cid.clone()).collect();
    let latest_rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT contract_id, key_hash, max(ledger) FROM contract_data
         WHERE contract_id = ANY($1) AND key_hash = ANY($2)
         GROUP BY contract_id, key_hash",
    )
    .bind(&contract_ids)
    .bind(&key_hashes)
    .fetch_all(pool)
    .await?;
    let latest: std::collections::HashMap<(String, String), i64> = latest_rows
        .into_iter()
        .map(|(cid, kh, ledger)| ((cid, kh), ledger))
        .collect();

    // Build the changed rows (live values and tombstones).
    let mut ins_contract: Vec<String> = Vec::new();
    let mut ins_key_hash: Vec<String> = Vec::new();
    let mut ins_key: Vec<serde_json::Value> = Vec::new();
    let mut ins_key_xdr: Vec<String> = Vec::new();
    let mut ins_durability: Vec<String> = Vec::new();
    let mut ins_ledger: Vec<i64> = Vec::new();
    let mut ins_value: Vec<Option<serde_json::Value>> = Vec::new();
    let mut ins_label: Vec<Option<String>> = Vec::new();
    let mut ins_deleted: Vec<bool> = Vec::new();
    let mut ins_archived: Vec<bool> = Vec::new();

    for (idx, ((contract_id, key, dur, label), entry_opt)) in
        chunk.iter().zip(results.iter()).enumerate()
    {
        let key_hash = &key_hashes[idx];
        let latest_ledger = latest
            .get(&(contract_id.clone(), key_hash.clone()))
            .copied();

        match entry_opt {
            Some(entry) => {
                if latest_ledger == Some(entry.last_modified_ledger) {
                    continue;
                }
                ins_contract.push(contract_id.clone());
                ins_key_hash.push(key_hash.clone());
                ins_key.push(decode_scval(key));
                ins_key_xdr.push(key.to_xdr_base64(Limits::none())?);
                ins_durability.push(durability_strs[idx].to_string());
                ins_ledger.push(entry.last_modified_ledger);
                ins_value.push(Some(decode_scval(&entry.val)));
                ins_label.push(label.clone());
                ins_deleted.push(false);
                ins_archived.push(false);
            }
            None => {
                // No entry returned. Only record a tombstone if we have history
                // for this key; otherwise there is nothing stale to correct.
                if latest_ledger.is_none() {
                    continue;
                }
                let tip = match rpc.get_latest_ledger().await {
                    Ok(tip) => tip,
                    Err(e) => {
                        debug!(contract_id = %contract_id, error = %e, "skipping tombstone: latest ledger unavailable");
                        continue;
                    }
                };
                let archived = *dur == ContractDataDurability::Persistent
                    && entry_archived(rpc, contract_id, key, tip).await;
                ins_contract.push(contract_id.clone());
                ins_key_hash.push(key_hash.clone());
                ins_key.push(decode_scval(key));
                ins_key_xdr.push(key.to_xdr_base64(Limits::none())?);
                ins_durability.push(durability_strs[idx].to_string());
                ins_ledger.push(tip);
                ins_value.push(None);
                ins_label.push(label.clone());
                ins_deleted.push(true);
                ins_archived.push(archived);
            }
        }
    }

    if ins_contract.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label, deleted, archived)
         SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::jsonb[], $4::text[], $5::text[],
            $6::bigint[], $7::jsonb[], $8::text[], $9::boolean[], $10::boolean[]
         )
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(&ins_contract)
    .bind(&ins_key_hash)
    .bind(&ins_key)
    .bind(&ins_key_xdr)
    .bind(&ins_durability)
    .bind(&ins_ledger)
    .bind(&ins_value)
    .bind(&ins_label)
    .bind(&ins_deleted)
    .bind(&ins_archived)
    .execute(pool)
    .await?;
    debug!(
        count = ins_contract.len(),
        "contract-data snapshots recorded"
    );
    Ok(())
}

/// Latest snapshotted ledger per contract (`None` = never snapshotted), from
/// `contract_state`. Used to prioritise stale contracts (#746).
pub async fn last_snapshot_ledgers(
    pool: &PgPool,
    contract_ids: &[String],
) -> anyhow::Result<std::collections::HashMap<String, Option<i64>>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT contract_id, max(ledger) FROM contract_state
         WHERE contract_id = ANY($1)
         GROUP BY contract_id",
    )
    .bind(contract_ids)
    .fetch_all(pool)
    .await?;
    let mut out: std::collections::HashMap<String, Option<i64>> =
        contract_ids.iter().map(|c| (c.clone(), None)).collect();
    for (cid, ledger) in rows {
        out.insert(cid, Some(ledger));
    }
    Ok(out)
}

/// Latest balance-snapshot ledger per holder address (`None` = never
/// snapshotted). Balance keys decode to `[symbol, address]`, so the holder is
/// element 1 of the key JSON (#746).
pub async fn last_balance_snapshot_ledgers(
    pool: &PgPool,
    holders: &[String],
) -> anyhow::Result<std::collections::HashMap<String, Option<i64>>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT key->>1, max(ledger) FROM contract_data
         WHERE label = 'balance' AND key->>1 = ANY($1)
         GROUP BY key->>1",
    )
    .bind(holders)
    .fetch_all(pool)
    .await?;
    let mut out: std::collections::HashMap<String, Option<i64>> =
        holders.iter().map(|h| (h.clone(), None)).collect();
    for (holder, ledger) in rows {
        out.insert(holder, Some(ledger));
    }
    Ok(out)
}

/// Whether `holder` is a classic Stellar account (`G…`) rather than a contract.
fn is_classic_account(holder: &str) -> bool {
    keys::is_account_address(holder)
}

/// The ledger key holding a classic account's balance of `asset`: the account
/// entry for the native asset, otherwise its trustline.
fn trustline_ledger_key(holder: &str, asset: &Asset) -> anyhow::Result<LedgerKey> {
    keys::balance_ledger_key(true, Some(asset), holder, "Balance")
}

/// If `contract_id` is a Stellar Asset Contract, return the classic asset it
/// wraps (read from the instance's `METADATA.name`). `None` for WASM contracts
/// or when the instance can't be read.
async fn detect_sac_asset(rpc: &RpcClient, contract_id: &str) -> Option<Asset> {
    let inst = rpc.get_contract_instance(contract_id).await.ok()??;
    if inst.wasm_hash.is_some() {
        return None;
    }
    let ScVal::Map(Some(storage)) = &inst.storage else {
        return None;
    };
    let sym =
        |v: &ScVal, want: &str| matches!(v, ScVal::Symbol(s) if s.0.as_slice() == want.as_bytes());
    let metadata = storage.0.iter().find(|e| sym(&e.key, "METADATA"))?;
    let ScVal::Map(Some(meta)) = &metadata.val else {
        return None;
    };
    let name = meta.0.iter().find(|e| sym(&e.key, "name"))?;
    let ScVal::String(name) = &name.val else {
        return None;
    };
    keys::asset_from_sac_name(&ScVal::String(name.clone()))
}

/// Compute the hex-encoded SHA-256 of a key's XDR base64 encoding.
fn key_hash_of(key: &ScVal) -> anyhow::Result<String> {
    let key_xdr = key.to_xdr_base64(Limits::none())?;
    Ok(hex::encode(Sha256::digest(key_xdr.as_bytes())))
}

/// Map a durability to its stored string form.
fn durability_str(durability: ContractDataDurability) -> &'static str {
    match durability {
        ContractDataDurability::Persistent => "persistent",
        ContractDataDurability::Temporary => "temporary",
    }
}

/// Best-effort check whether a persistent entry that returned no data is
/// archived (restorable) rather than deleted. Uses the entry's TTL /
/// `liveUntilLedgerSeq` when the RPC provides it; defaults to `false`.
async fn entry_archived(rpc: &RpcClient, contract_id: &str, key: &ScVal, tip: i64) -> bool {
    match rpc
        .get_contract_data_ttl(contract_id, key, ContractDataDurability::Persistent)
        .await
    {
        Ok(Some(live_until)) => live_until > tip,
        Ok(None) => false,
        Err(e) => {
            debug!(contract_id, error = %e, "TTL lookup failed; assuming deleted");
            false
        }
    }
}

/// Write a tombstone row for a key that has history but returned no entry.
async fn write_tombstone_if_needed(
    pool: &PgPool,
    contract_id: &str,
    key_hash: &str,
    durability: ContractDataDurability,
    label: Option<&str>,
) -> anyhow::Result<()> {
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT max(ledger) FROM contract_data WHERE contract_id = $1 AND key_hash = $2",
    )
    .bind(contract_id)
    .bind(key_hash)
    .fetch_one(pool)
    .await?;
    let Some(latest_ledger) = latest else {
        return Ok(());
    };

    // Already a tombstone at the latest ledger? Nothing to do.
    let already_deleted: Option<bool> = sqlx::query_scalar(
        "SELECT deleted FROM contract_data
         WHERE contract_id = $1 AND key_hash = $2 AND ledger = $3",
    )
    .bind(contract_id)
    .bind(key_hash)
    .bind(latest_ledger)
    .fetch_optional(pool)
    .await?;
    if already_deleted == Some(true) {
        return Ok(());
    }

    let tip = sqlx::query_scalar::<_, i64>("SELECT max(ledger) FROM contract_data")
        .fetch_optional(pool)
        .await?
        .unwrap_or(latest_ledger);

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label, deleted, archived)
         VALUES ($1, $2, NULL, NULL, $3, $4, NULL, $5, true, false)
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(contract_id)
    .bind(key_hash)
    .bind(durability_str(durability))
    .bind(tip)
    .bind(label)
    .execute(pool)
    .await?;
    debug!(
        contract_id,
        ledger = tip,
        "contract-data tombstone recorded"
    );
    Ok(())
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
    // Deprecated alias: sha256 of the base64 key text (durability-blind).
    let key_hash = hex::encode(Sha256::digest(key_xdr.as_bytes()));
    let ledger_key_hash = ledger_key_hash(contract_id, key, durability)?;

    // Change detection: skip if we already have this key at this ledger.
    let latest: Option<i64> = sqlx::query_scalar(
        "SELECT max(ledger) FROM contract_data WHERE contract_id = $1 AND ledger_key_hash = $2",
    )
    .bind(contract_id)
    .bind(&ledger_key_hash)
    .fetch_one(pool)
    .await?;
    if latest == Some(entry.last_modified_ledger) {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO contract_data
            (contract_id, key_hash, key, key_xdr, durability, ledger, value, label, deleted, archived)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, false)
         ON CONFLICT (contract_id, key_hash, ledger) DO NOTHING",
    )
    .bind(contract_id)
    .bind(&key_hash)
    .bind(&ledger_key_hash)
    .bind(decode_scval(key))
    .bind(&key_xdr)
    .bind(durability_str(durability))
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

/// Compute the canonical ledger-key hash for a contract-data entry:
/// `hex(sha256(LedgerKey::ContractData { contract, key, durability }.to_xdr()))`.
///
/// This matches the identifier used by Stellar tooling and includes durability,
/// so persistent and temporary entries with the same key never collide.
pub fn ledger_key_hash(
    contract_id: &str,
    key: &ScVal,
    durability: ContractDataDurability,
) -> anyhow::Result<String> {
    let contract = parse_contract_address(contract_id)?;
    let ledger_key = LedgerKey::ContractData(LedgerKeyContractData {
        contract,
        key: key.clone(),
        durability,
    });
    let xdr_bytes = ledger_key.to_xdr(Limits::none())?;
    Ok(hex::encode(Sha256::digest(&xdr_bytes)))
}

/// Parse a contract ID string (`C…` StrKey) into an `ScAddress::Contract`.
fn parse_contract_address(contract_id: &str) -> anyhow::Result<ScAddress> {
    let contract = ContractId::from_str(contract_id)
        .map_err(|e| anyhow::anyhow!("invalid contract id {contract_id}: {e}"))?;
    Ok(ScAddress::Contract(contract))
}

/// Decode an instance-storage `ScVal` to friendly JSON directly, without any
/// base64 round-trip, using the shared decoder so state and events share one
/// JSON shape (symbol-keyed maps become objects, i128 as decimal strings…).
fn decode_storage(storage: &ScVal) -> serde_json::Value {
    decode_scval(storage)
}

/// Decode any `ScVal` to friendly JSON via the shared event decoder.
/// No base64 encoding/decoding is performed — the value is decoded directly
/// from the already-parsed typed enum.
fn decode_scval(v: &ScVal) -> serde_json::Value {
    xdr::decode_scval(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure prioritisation helper mirroring `cap_and_prioritise_*`: given items
    /// with their last-snapshot ledger (`None` = never snapshotted), return the
    /// selected indices in priority order and the deferred count.
    fn prioritise(lasts: &[Option<i64>], max_per_cycle: usize) -> (Vec<usize>, usize) {
        if max_per_cycle == 0 || lasts.len() <= max_per_cycle {
            return ((0..lasts.len()).collect(), 0);
        }
        let mut indexed: Vec<(usize, i64)> = lasts
            .iter()
            .enumerate()
            .map(|(idx, last)| (idx, last.unwrap_or(i64::MIN)))
            .collect();
        indexed.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let deferred = indexed.len() - max_per_cycle;
        let selected = indexed
            .into_iter()
            .take(max_per_cycle)
            .map(|(i, _)| i)
            .collect();
        (selected, deferred)
    }

    #[test]
    fn cap_zero_means_unlimited() {
        let (selected, deferred) = prioritise(&[Some(1), Some(2), Some(3)], 0);
        assert_eq!(selected, vec![0, 1, 2]);
        assert_eq!(deferred, 0);
    }

    #[test]
    fn cap_under_limit_selects_all() {
        let (selected, deferred) = prioritise(&[Some(1), Some(2)], 5);
        assert_eq!(selected, vec![0, 1]);
        assert_eq!(deferred, 0);
    }

    #[test]
    fn cap_truncates_and_defers_remainder() {
        let (selected, deferred) = prioritise(&[Some(10), Some(20), Some(30), Some(40)], 2);
        assert_eq!(selected.len(), 2);
        assert_eq!(deferred, 2);
    }

    #[test]
    fn cap_prioritises_oldest_and_never_snapshotted() {
        // Index 2 is never snapshotted (highest priority), then oldest ledger.
        let lasts = [Some(50), Some(10), None, Some(30)];
        let (selected, deferred) = prioritise(&lasts, 2);
        assert_eq!(selected, vec![2, 1]);
        assert_eq!(deferred, 2);
    }

    #[test]
    fn cap_breaks_ties_by_input_order() {
        let lasts = [Some(5), Some(5), Some(5)];
        let (selected, deferred) = prioritise(&lasts, 2);
        assert_eq!(selected, vec![0, 1]);
        assert_eq!(deferred, 1);
    }
}
