//! Per-contract interface cache.
//!
//! The first time we see events from a contract, we fetch its deployed WASM,
//! parse the embedded `contractspecv0` interface once, persist it (so the API
//! can serve `/contracts/:id/interface` and a restart need not refetch), and
//! keep it in memory to enrich every later event. Contracts with no usable spec
//! are remembered as `None` so we never refetch them on a hot loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lumenqraph_core::ContractSpec;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::rpc_client::RpcClient;

#[derive(Default)]
pub struct SpecCache {
    inner: Mutex<HashMap<String, Option<Arc<ContractSpec>>>>,
}

impl SpecCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The spec for a contract, fetching+parsing+persisting on first use.
    /// Best-effort: any failure caches `None` and enrichment is simply skipped.
    pub async fn get(
        &self,
        pool: &PgPool,
        rpc: &RpcClient,
        contract_id: &str,
    ) -> Option<Arc<ContractSpec>> {
        // Lock only to read/insert — never held across the network fetch.
        if let Some(cached) = self.inner.lock().unwrap().get(contract_id).cloned() {
            return cached;
        }
        let spec = load(pool, rpc, contract_id).await;
        self.inner
            .lock()
            .unwrap()
            .insert(contract_id.to_string(), spec.clone());
        spec
    }
}

async fn load(pool: &PgPool, rpc: &RpcClient, contract_id: &str) -> Option<Arc<ContractSpec>> {
    let (wasm_hash, wasm) = match rpc.get_contract_wasm(contract_id).await {
        Ok(Some(w)) => w,
        Ok(None) => {
            debug!(
                contract_id,
                "contract has no WASM spec (e.g. SAC); skipping"
            );
            return None;
        }
        Err(e) => {
            warn!(contract_id, error = %e, "failed to fetch contract WASM");
            return None;
        }
    };

    let spec = ContractSpec::from_wasm(&wasm)?;
    // The raw section (hex) lets the read layer re-parse exact argument types.
    let spec_section = lumenqraph_core::spec::spec_section_of(&wasm)
        .map(hex::encode)
        .unwrap_or_default();
    info!(
        contract_id,
        events = spec.events.len(),
        functions = spec.functions.len(),
        "parsed contract interface"
    );
    if let Err(e) = persist(pool, contract_id, &wasm_hash, &spec_section, &spec).await {
        warn!(contract_id, error = %e, "failed to persist contract spec");
    }
    Some(Arc::new(spec))
}

async fn persist(
    pool: &PgPool,
    contract_id: &str,
    wasm_hash: &str,
    spec_section: &str,
    spec: &ContractSpec,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (contract_id) DO UPDATE
           SET wasm_hash = EXCLUDED.wasm_hash,
               interface = EXCLUDED.interface,
               spec_section = EXCLUDED.spec_section,
               has_events = EXCLUDED.has_events,
               fetched_at = now()",
    )
    .bind(contract_id)
    .bind(wasm_hash)
    .bind(spec.to_interface_json())
    .bind(spec_section)
    .bind(spec.has_events())
    .execute(pool)
    .await?;
    Ok(())
}
