//! Per-contract interface cache.
//!
//! The first time we see events from a contract, we fetch its deployed WASM,
//! parse the embedded `contractspecv0` interface once, persist it (so the API
//! can serve `/contracts/:id/interface` and a restart need not refetch), and
//! keep it in memory to enrich every later event. Contracts with no usable spec
//! are remembered (with `spec: None`) so we never refetch them on a hot loop.
//!
//! Each entry also remembers the contract's WASM hash. When the poller (or state
//! indexing) reads a contract's instance entry and sees a *different* hash, it
//! calls [`SpecCache::note_wasm_hash`], which drops the stale entry and re-reads
//! the upgraded interface.
//!
//! Every interface we parse is also appended to `contract_spec_versions` — the
//! contract's interface history — along with the [`SpecDiff`] against the
//! previous version. That's the upgrade watch: because a Soroban contract can be
//! upgraded in place, its interface is a time series, and this is how we record
//! what changed and when.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lumenqraph_core::{ContractSpec, SpecDiff};
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
    /// upgraded: drop the stale entry and re-read the interface immediately,
    /// which records the new version and its diff.
    ///
    /// Reloading eagerly rather than letting the next event do it lazily matters
    /// for two reasons: a tracked contract may emit no events at all after an
    /// upgrade (so a lazy reload would never fire), and the upgrade webhook
    /// should go out when the upgrade happens, not whenever the contract next
    /// happens to be used.
    pub async fn note_wasm_hash(
        &self,
        pool: &PgPool,
        rpc: &RpcClient,
        contract_id: &str,
        current_hash: &str,
    ) {
        // Only the check holds the lock; the reload below must not.
        let is_stale = {
            let mut map = self.inner.lock().unwrap();
            match map.get(contract_id) {
                Some(cached) if cached.wasm_hash.as_deref() != Some(current_hash) => {
                    map.remove(contract_id);
                    true
                }
                _ => false,
            }
        };
        if is_stale {
            info!(
                contract_id,
                wasm_hash = current_hash,
                "contract upgraded; re-reading interface"
            );
            self.get(pool, rpc, contract_id).await;
        }
    }
}

/// Check whether `contract_id` has been upgraded, and if so re-read its
/// interface and record the new version. Best-effort: errors are logged, never
/// propagated to the poller.
///
/// This is the standalone upgrade watch. State indexing reads the same instance
/// entry and detects upgrades as a side effect, so the poller only calls this
/// when state indexing is off — otherwise both would fetch the same entry.
pub async fn check_for_upgrade(
    pool: &PgPool,
    rpc: &RpcClient,
    specs: &SpecCache,
    contract_id: &str,
) {
    match rpc.get_contract_instance(contract_id).await {
        Ok(Some(instance)) => {
            if let Some(hash) = &instance.wasm_hash {
                specs.note_wasm_hash(pool, rpc, contract_id, hash).await;
            }
        }
        // No instance entry (e.g. archived), or the contract is a SAC with no
        // upgradable WASM — nothing to watch either way.
        Ok(None) => {}
        Err(e) => warn!(contract_id, error = %e, "upgrade check failed"),
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
    // Independent of the upsert above: that keeps only the current interface,
    // this appends to the history. A failure here must not cost us the spec.
    if let Err(e) = record_version(pool, contract_id, &wasm_hash, &spec_section, &spec).await {
        warn!(contract_id, error = %e, "failed to record contract spec version");
    }
    (Some(Arc::new(spec)), Some(wasm_hash))
}

/// Append this interface to the contract's version history, unless it's the
/// version we already have at the tip of that history.
///
/// Called on every spec load, not just on a detected upgrade, so the history is
/// self-healing: a restart, a missed eviction, or an upgrade that happened while
/// we were down all still land here, and the hash check keeps it idempotent.
async fn record_version(
    pool: &PgPool,
    contract_id: &str,
    wasm_hash: &str,
    spec_section: &str,
    spec: &ContractSpec,
) -> anyhow::Result<()> {
    let previous: Option<(i32, String, String)> = sqlx::query_as(
        "SELECT version, wasm_hash, spec_section FROM contract_spec_versions
         WHERE contract_id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(contract_id)
    .fetch_optional(pool)
    .await?;

    let (version, previous_hash, diff) = match previous {
        // Same executable as the newest version on record: nothing happened.
        Some((_, ref prev_hash, _)) if prev_hash == wasm_hash => return Ok(()),
        Some((prev_version, prev_hash, prev_section)) => {
            let diff = diff_against(&prev_section, spec);
            if let Some(d) = &diff {
                info!(
                    contract_id,
                    version = prev_version + 1,
                    breaking = d.breaking,
                    changes = d.summary.len(),
                    "contract interface changed"
                );
            }
            (prev_version + 1, Some(prev_hash), diff)
        }
        // First interface we've ever seen for this contract. It's a baseline,
        // not an upgrade: there's nothing to diff it against, and no consumer
        // could have been depending on an earlier version we never saw.
        None => (1, None, None),
    };

    sqlx::query(
        "INSERT INTO contract_spec_versions
            (contract_id, version, wasm_hash, previous_wasm_hash, interface, spec_section, diff, breaking)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (contract_id, version) DO NOTHING",
    )
    .bind(contract_id)
    .bind(version)
    .bind(wasm_hash)
    .bind(previous_hash)
    .bind(spec.to_interface_json())
    .bind(spec_section)
    .bind(diff.as_ref().map(|d| d.to_json()))
    .bind(diff.as_ref().is_some_and(|d| d.breaking))
    .execute(pool)
    .await?;
    Ok(())
}

/// Diff the new spec against a stored raw section. `None` when the previous
/// section can't be re-parsed — an honest "upgraded, diff unavailable" beats
/// diffing against an empty interface, which would report the whole contract as
/// newly added.
fn diff_against(previous_section: &str, new_spec: &ContractSpec) -> Option<SpecDiff> {
    let bytes = hex::decode(previous_section).ok()?;
    let previous = ContractSpec::from_spec_xdr(&bytes)?;
    Some(SpecDiff::between(&previous, new_spec))
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

#[cfg(test)]
mod tests {
    //! Interface-history tests. These need a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-indexer -- --ignored --nocapture

    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;
    use stellar_xdr::curr::{
        Limits, ScSpecEntry, ScSpecFunctionV0, ScSpecTypeDef, ScSymbol, WriteXdr,
    };

    /// Fresh schema per test, so tests don't see each other's rows.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        for stmt in ["DROP SCHEMA public CASCADE", "CREATE SCHEMA public"] {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .expect("reset schema");
        }
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    /// A spec section (hex) exposing exactly the named zero-arg functions, plus
    /// the ContractSpec it parses to — the pair `record_version` takes.
    fn spec_with(functions: &[&str]) -> (String, ContractSpec) {
        let mut body = Vec::new();
        for name in functions {
            let entry = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
                doc: "".try_into().unwrap(),
                name: ScSymbol((*name).try_into().unwrap()),
                inputs: vec![].try_into().unwrap(),
                outputs: vec![ScSpecTypeDef::U32].try_into().unwrap(),
            });
            body.extend(entry.to_xdr(Limits::none()).unwrap());
        }
        let spec = ContractSpec::from_spec_xdr(&body).expect("test spec should parse");
        (hex::encode(&body), spec)
    }

    async fn versions(pool: &PgPool) -> Vec<(i32, Option<String>, bool)> {
        sqlx::query(
            "SELECT version, previous_wasm_hash, breaking FROM contract_spec_versions
             WHERE contract_id = 'C1' ORDER BY version",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn first_interface_is_a_baseline_with_no_diff() {
        let pool = fixture().await;
        let (section, spec) = spec_with(&["balance"]);
        record_version(&pool, "C1", "hash1", &section, &spec)
            .await
            .unwrap();

        assert_eq!(versions(&pool).await, vec![(1, None, false)]);
        let diff: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT diff FROM contract_spec_versions WHERE version = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            diff.is_none(),
            "version 1 has nothing to diff against, so its diff must be NULL \
             rather than an empty diff"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn re_reading_the_same_executable_records_nothing() {
        let pool = fixture().await;
        let (section, spec) = spec_with(&["balance"]);
        // Every restart and every cache miss re-reads the spec; only a genuine
        // change may append to the history.
        for _ in 0..3 {
            record_version(&pool, "C1", "hash1", &section, &spec)
                .await
                .unwrap();
        }
        assert_eq!(versions(&pool).await.len(), 1);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_upgrade_appends_a_version_with_its_diff() {
        let pool = fixture().await;
        let (s1, spec1) = spec_with(&["balance", "withdraw"]);
        record_version(&pool, "C1", "hash1", &s1, &spec1)
            .await
            .unwrap();

        // v2 drops withdraw and adds pause: a breaking change.
        let (s2, spec2) = spec_with(&["balance", "pause"]);
        record_version(&pool, "C1", "hash2", &s2, &spec2)
            .await
            .unwrap();

        assert_eq!(
            versions(&pool).await,
            vec![(1, None, false), (2, Some("hash1".into()), true)],
            "v2 should chain to v1's hash and be flagged breaking"
        );

        let diff: serde_json::Value =
            sqlx::query_scalar("SELECT diff FROM contract_spec_versions WHERE version = 2")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(diff["breaking"], true);
        assert_eq!(
            diff["summary"],
            serde_json::json!([
                "removed function withdraw() -> u32",
                "added function pause() -> u32",
            ])
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn a_code_only_upgrade_is_recorded_as_a_non_breaking_empty_diff() {
        let pool = fixture().await;
        let (section, spec) = spec_with(&["balance"]);
        record_version(&pool, "C1", "hash1", &section, &spec)
            .await
            .unwrap();
        // New code, identical interface — e.g. a bug fix. Still an upgrade worth
        // recording, but nothing an integration needs to react to.
        record_version(&pool, "C1", "hash2", &section, &spec)
            .await
            .unwrap();

        let rows = versions(&pool).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1], (2, Some("hash1".into()), false));

        let diff: serde_json::Value =
            sqlx::query_scalar("SELECT diff FROM contract_spec_versions WHERE version = 2")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            diff["summary"],
            serde_json::json!([]),
            "an interface that didn't change should diff to no changes at all"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_unparseable_previous_section_records_the_version_without_a_diff() {
        let pool = fixture().await;
        // A version whose stored section can't be re-parsed (e.g. written before
        // we kept sections). The upgrade must still be recorded.
        sqlx::query(
            "INSERT INTO contract_spec_versions
                (contract_id, version, wasm_hash, interface, spec_section)
             VALUES ('C1', 1, 'hash1', '{}', '')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (s2, spec2) = spec_with(&["balance"]);
        record_version(&pool, "C1", "hash2", &s2, &spec2)
            .await
            .unwrap();

        let diff: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT diff FROM contract_spec_versions WHERE version = 2")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            diff.is_none(),
            "with no parseable baseline the honest answer is no diff, not a diff \
             claiming the whole interface was just added"
        );
        assert_eq!(versions(&pool).await.len(), 2);
    }
}
