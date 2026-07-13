//! Per-contract interface cache.
//!
//! The first time we see events from a contract, we fetch its deployed WASM,
//! parse the embedded `contractspecv0` interface once, persist it (so the API
//! can serve `/contracts/:id/interface` and a restart need not refetch), and
//! keep it in memory to enrich every later event. Contracts with no usable spec
//! are remembered (with `spec: None`) so we never refetch them on a hot loop.
//!
//! Each entry also remembers the contract's WASM hash. When state indexing reads
//! a contract's instance entry and sees a *different* hash, it calls
//! [`SpecCache::note_wasm_hash`], which evicts the stale entry so the next event
//! re-parses the upgraded interface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lumenqraph_core::ContractSpec;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::rpc_client::RpcClient;

#[derive(Clone, Default)]
struct Cached {
    spec: Option<Arc<ContractSpec>>,
    /// The executable hash the cached spec was parsed from (`None` for SAC).
    wasm_hash: Option<String>,
}

#[derive(Default)]
pub struct SpecCache {
    inner: Mutex<HashMap<String, Cached>>,
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
            return cached.spec;
        }
        let (spec, wasm_hash) = load(pool, rpc, contract_id).await;
        self.inner.lock().unwrap().insert(
            contract_id.to_string(),
            Cached {
                spec: spec.clone(),
                wasm_hash,
            },
        );
        spec
    }

    /// Note the contract's *current* WASM hash (observed from its instance
    /// entry). If it differs from the cached spec's hash, the contract has been
    /// upgraded: evict the entry so the next event re-parses the new interface.
    pub fn note_wasm_hash(&self, contract_id: &str, current_hash: &str) {
        let mut map = self.inner.lock().unwrap();
        if let Some(cached) = map.get(contract_id) {
            if cached.wasm_hash.as_deref() != Some(current_hash) {
                map.remove(contract_id);
                info!(contract_id, "contract upgraded; evicted stale interface");
            }
        }
    }
}

/// Returns `(parsed spec, wasm hash)`. The hash is present for any WASM contract
/// (even if its spec fails to parse) and `None` for a Stellar Asset Contract.
async fn load(
    pool: &PgPool,
    rpc: &RpcClient,
    contract_id: &str,
) -> (Option<Arc<ContractSpec>>, Option<String>) {
    let (wasm_hash, wasm) = match rpc.get_contract_wasm(contract_id).await {
        Ok(Some(w)) => w,
        Ok(None) => {
            debug!(
                contract_id,
                "contract has no WASM spec (e.g. SAC); skipping"
            );
            return (None, None);
        }
        Err(e) => {
            warn!(contract_id, error = %e, "failed to fetch contract WASM");
            return (None, None);
        }
    };

    let Some(spec) = ContractSpec::from_wasm(&wasm) else {
        return (None, Some(wasm_hash));
    };
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
    (Some(Arc::new(spec)), Some(wasm_hash))
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
