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
//!
//! # Version-at-ledger enrichment (#736)
//!
//! Events are enriched with the interface that was in force at the event's
//! ledger, not the contract's current interface. Each contract keeps a sorted
//! `Vec<(effective_from_ledger, Arc<ContractSpec>)>`; `get` binary-searches it
//! for the newest version whose `effective_from_ledger <= event.ledger`. The
//! live tail (events at or after the newest known version) uses the current
//! spec.
//!
//! The `effective_from_ledger` comes from `contract_spec_versions.upgrade_ledger`
//! when known, falling back to `observed_ledger` (an upper bound) otherwise
//! (#737).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::store::ContractKind;
use lru::LruCache;
use lumenqraph_core::{ContractSpec, SpecDiff};
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::rpc_client::RpcClient;

/// Counters for `lumenqraph_spec_cache_loads_total{source}`: how many cache
/// misses were satisfied from Postgres vs. from RPC (#735).
#[derive(Default)]
struct LoadCounters {
    db: AtomicU64,
    rpc: AtomicU64,
}

impl LoadCounters {
    fn record(&self, source: LoadSource) {
        match source {
            LoadSource::Db => self.db.fetch_add(1, Ordering::Relaxed),
            LoadSource::Rpc => self.rpc.fetch_add(1, Ordering::Relaxed),
        };
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.db.load(Ordering::Relaxed),
            self.rpc.load(Ordering::Relaxed),
        )
    }
}

/// Where a cache miss was resolved from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoadSource {
    Db,
    Rpc,
}

#[derive(Clone)]
enum CachedSpec {
    Spec(Arc<ContractSpec>),
    /// Permanent failure: no WASM spec (e.g. SAC). Never retry.
    NoSpec,
    /// Transient failure: RPC error. Retry after TTL expires.
    FetchError {
        retried_at: Instant,
    },
}

/// One version of a contract's interface, keyed by the ledger at which it
/// became effective.
#[derive(Clone)]
struct SpecVersion {
    /// First ledger at which this interface was live. For versions where the
    /// exact upgrade ledger is unknown this is the observed ledger (an upper
    /// bound), so lookups may attribute an event to the newer version slightly
    /// early — never to a version that didn't exist yet.
    effective_from_ledger: i64,
    spec: Arc<ContractSpec>,
}

#[derive(Clone)]
struct Cached {
    spec: CachedSpec,
    /// The executable hash the cached spec was parsed from (`None` for SAC or
    /// when unknown, e.g. after a transient fetch failure).
    wasm_hash: Option<String>,
    /// Interface history for this contract, sorted ascending by
    /// `effective_from_ledger`. Empty when the contract has no usable spec.
    versions: Arc<Vec<SpecVersion>>,
    /// What kind of contract this is, when the interface (or being a SAC)
    /// confirms it. `None` means unknown, so kind-gated projections are skipped.
    kind: Option<ContractKind>,
}

/// The outcome of loading a contract's interface from the database or RPC.
struct Loaded {
    spec: Option<Arc<ContractSpec>>,
    wasm_hash: Option<String>,
    /// True when the failure (if any) is not worth retrying (SAC, parse error).
    is_permanent: bool,
    versions: Vec<SpecVersion>,
    kind: Option<ContractKind>,
}

impl Default for Cached {
    fn default() -> Self {
        Self {
            spec: CachedSpec::NoSpec,
            wasm_hash: None,
            versions: Arc::new(Vec::new()),
            kind: None,
        }
    }
}

impl Cached {
    /// Newest version whose `effective_from_ledger <= ledger`, or `None` if the
    /// event predates every version we know about.
    fn version_at(&self, ledger: i64) -> Option<Arc<ContractSpec>> {
        // partition_point gives the count of entries with effective <= ledger.
        let idx = self
            .versions
            .partition_point(|v| v.effective_from_ledger <= ledger);
        if idx == 0 {
            None
        } else {
            Some(self.versions[idx - 1].spec.clone())
        }
    }
}

/// TTL for transient fetch errors before retrying.
const FETCH_ERROR_TTL: Duration = Duration::from_secs(60);

pub struct SpecCache {
    inner: Mutex<LruCache<String, Cached>>,
    fetch_semaphore: Arc<Semaphore>,
    cache_size: AtomicUsize,
    loads: LoadCounters,
    /// One async lock per contract being fetched, so concurrent misses for the
    /// same contract coalesce into a single fetch (single-flight).
    inflight: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl SpecCache {
    pub fn new(max_entries: usize, concurrency: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(max_entries).expect("max_entries must be > 0"),
            )),
            fetch_semaphore: Arc::new(Semaphore::new(concurrency)),
            cache_size: AtomicUsize::new(0),
            loads: LoadCounters::default(),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// The confirmed kind of a cached contract, or `None` when unknown or not
    /// cached. Backs the kind resolver used to gate vertical projections.
    pub fn contract_kind(&self, contract_id: &str) -> Option<ContractKind> {
        self.inner.lock().unwrap().peek(contract_id)?.kind
    }

    /// Resolver closure for `store::*_with_resolver`.
    pub fn contract_kind_resolver(&self) -> impl Fn(&str) -> Option<ContractKind> + '_ {
        move |cid| self.contract_kind(cid)
    }

    /// Alias of [`Self::contract_kind`].
    pub fn kind(&self, contract_id: &str) -> Option<ContractKind> {
        self.contract_kind(contract_id)
    }

    /// Return `(db, rpc)` counts of cache misses resolved from Postgres and
    /// from RPC respectively. Used by `/metrics` to expose
    /// `lumenqraph_spec_cache_loads_total{source}`.
    pub fn load_counts(&self) -> (u64, u64) {
        self.loads.snapshot()
    }

    /// Return the current number of entries in the spec cache. Used by the
    /// `/metrics` endpoint to expose the `lumenqraph_spec_cache_size` gauge.
    pub fn cache_size(&self) -> usize {
        self.cache_size.load(Ordering::Relaxed)
    }

    /// Return the spec for a contract if it is already in the in-memory cache,
    /// without touching the database or making any network calls. Used by the
    /// deep-backfill path where RPC is not available.
    ///
    /// Prefers the version in force at `ledger` when known, falling back to the
    /// current spec.
    pub fn get_cached(&self, contract_id: &str, ledger: i64) -> Option<Arc<ContractSpec>> {
        let mut map = self.inner.lock().unwrap();
        let cached = map.get(contract_id)?;
        if let Some(spec) = cached.version_at(ledger) {
            return Some(spec);
        }
        match &cached.spec {
            CachedSpec::Spec(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// The spec for a contract at `ledger`, fetching+parsing+persisting on
    /// first use. Distinguishes transient failures (retryable) from permanent
    /// failures (SAC). Concurrent fetches are bounded by the semaphore and
    /// coalesced per contract; already-cached lookups bypass both.
    pub async fn get(
        &self,
        pool: &PgPool,
        rpc: &RpcClient,
        contract_id: &str,
        ledger: i64,
    ) -> Option<Arc<ContractSpec>> {
        // Fast path: a cached entry that can answer this ledger.
        if let Some(result) = self.try_cached(contract_id, ledger) {
            return result;
        }

        // Single-flight: serialise fetches per contract, then re-check the
        // cache, so a burst of misses costs one fetch, not one each.
        let gate = {
            let mut inflight = self.inflight.lock().unwrap();
            inflight
                .entry(contract_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let result = {
            let _guard = gate.lock().await;
            match self.try_cached(contract_id, ledger) {
                Some(result) => result,
                None => self.fetch_and_store(pool, rpc, contract_id, ledger).await,
            }
        };
        // Drop the gate once nobody else holds it, so the map doesn't grow.
        {
            let mut inflight = self.inflight.lock().unwrap();
            if let Some(g) = inflight.get(contract_id) {
                if Arc::strong_count(g) <= 2 {
                    inflight.remove(contract_id);
                }
            }
        }
        result
    }

    /// Check the cache for an answer at `ledger`. Returns:
    /// - `Some(Some(spec))` when a spec is available,
    /// - `Some(None)` when the entry is a permanent failure or a still-fresh
    ///   transient failure (no retry),
    /// - `None` when the entry is missing or its TTL has expired (fetch needed).
    fn try_cached(&self, contract_id: &str, ledger: i64) -> Option<Option<Arc<ContractSpec>>> {
        let mut map = self.inner.lock().unwrap();
        let cached = map.get(contract_id)?;
        match &cached.spec {
            CachedSpec::Spec(_) => {
                // Prefer the version in force at this ledger; fall back to the
                // current spec for the live tail.
                if let Some(spec) = cached.version_at(ledger) {
                    Some(Some(spec))
                } else {
                    match &cached.spec {
                        CachedSpec::Spec(s) => Some(Some(s.clone())),
                        _ => None,
                    }
                }
            }
            CachedSpec::NoSpec => Some(None),
            CachedSpec::FetchError { retried_at } => {
                if retried_at.elapsed() < FETCH_ERROR_TTL {
                    Some(None)
                } else {
                    None
                }
            }
        }
    }

    /// Perform the actual fetch, persist, and store in the cache. Called only
    /// by the single-flight leader.
    async fn fetch_and_store(
        &self,
        pool: &PgPool,
        rpc: &RpcClient,
        contract_id: &str,
        ledger: i64,
    ) -> Option<Arc<ContractSpec>> {
        let _permit = self
            .fetch_semaphore
            .acquire()
            .await
            .expect("semaphore acquire failed");

        // #735: prefer the persisted spec. A restart (or an LRU eviction) must
        // not cost a WASM download when we already have the interface in
        // Postgres. RPC is only touched when no row exists.
        let (loaded, source) = match load_persisted(pool, contract_id).await {
            Some((spec, wasm_hash)) => {
                let versions = match load_versions(pool, contract_id).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(contract_id, error = %e, "failed to load contract spec versions");
                        Vec::new()
                    }
                };
                let kind = classify_kind(&spec);
                (
                    Loaded {
                        spec: Some(spec),
                        wasm_hash,
                        is_permanent: true,
                        versions,
                        kind,
                    },
                    LoadSource::Db,
                )
            }
            None => (load(pool, rpc, contract_id, ledger).await, LoadSource::Rpc),
        };
        self.loads.record(source);
        let cached_spec = match loaded.spec.clone() {
            Some(s) => CachedSpec::Spec(s),
            None => {
                if loaded.is_permanent {
                    CachedSpec::NoSpec
                } else {
                    CachedSpec::FetchError {
                        retried_at: Instant::now(),
                    }
                }
            }
        };
        let versions = Arc::new(loaded.versions);
        {
            let mut map = self.inner.lock().unwrap();
            map.put(
                contract_id.to_string(),
                Cached {
                    spec: cached_spec.clone(),
                    wasm_hash: loaded.wasm_hash,
                    versions,
                    kind: loaded.kind,
                },
            );
            self.cache_size.store(map.len(), Ordering::Relaxed);
        }
        match cached_spec {
            CachedSpec::Spec(s) => Some(s),
            _ => None,
        }
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
    ///
    /// #738: A cached `FetchError` entry has no known previous hash, so it is
    /// *not* treated as an upgrade. Instead we record the observed hash on the
    /// entry (so the retry after the TTL can skip the instance hop) and leave
    /// the TTL intact.
    pub async fn note_wasm_hash(
        &self,
        pool: &PgPool,
        rpc: &RpcClient,
        contract_id: &str,
        current_hash: &str,
        ledger: i64,
    ) {
        // Only the check holds the lock; the reload below must not.
        let cached_hash = {
            let mut map = self.inner.lock().unwrap();
            map.get(contract_id).and_then(|c| c.wasm_hash.clone())
        };
        // #739: on a cache miss (never loaded since restart, or evicted under
        // LRU pressure) the in-memory entry can't tell us anything. Fall back to
        // the persisted current hash so upgrades of contracts that emit no
        // events are still noticed eagerly.
        let is_stale = match cached_hash {
            Some(hash) => hash != current_hash,
            None => {
                let persisted: Option<Option<String>> = sqlx::query_scalar(
                    "SELECT wasm_hash FROM contract_specs WHERE contract_id = $1",
                )
                .bind(contract_id)
                .fetch_optional(pool)
                .await
                .unwrap_or_else(|e| {
                    warn!(contract_id, error = %e, "failed to read persisted wasm hash");
                    None
                });
                match persisted {
                    // A persisted spec at a different hash: upgraded.
                    Some(Some(hash)) => hash != current_hash,
                    // No persisted spec at all: nothing to compare against, but
                    // we still want to load and record the interface.
                    Some(None) | None => true,
                }
            }
        };
        if is_stale {
            {
                let mut map = self.inner.lock().unwrap();
                map.pop(contract_id);
                self.cache_size.store(map.len(), Ordering::Relaxed);
            }
            info!(
                contract_id,
                wasm_hash = current_hash,
                "contract upgraded; re-reading interface"
            );
            let _ = self.get(pool, rpc, contract_id, ledger).await;
        }
    }
}

/// Classify a contract from its parsed spec using well-known interface shapes.
///
/// Heuristics, checked in order (most specific first):
/// - AMM: declares a `swap` event and `get_reserves`/`swap` functions.
/// - NFT: declares `owner_of`/`token_uri` functions.
/// - Liquidity pool: declares `deposit`/`withdraw` events and a `shares` field.
/// - Fungible token: declares the SEP-41 `transfer`/`approve`/`balance` functions.
///
/// Returns `None` when the spec matches none of these shapes. SAC detection is
/// left to `default_contract_kind`, which handles specs we never parse.
fn classify_kind(spec: &ContractSpec) -> Option<ContractKind> {
    let has_event = |name: &str| spec.events.iter().any(|e| e.name == name);
    let has_function = |name: &str| spec.functions.iter().any(|f| f.name == name);
    let has_field = |name: &str| {
        spec.structs
            .iter()
            .any(|s| s.fields.iter().any(|f| f.name == name))
    };

    if has_event("swap") && (has_function("get_reserves") || has_function("swap")) {
        return Some(ContractKind::Amm);
    }
    if has_function("owner_of") || has_function("token_uri") {
        return Some(ContractKind::NonFungibleToken);
    }
    if (has_event("deposit") || has_event("withdraw")) && has_field("shares") {
        return Some(ContractKind::LiquidityPool);
    }
    if is_sep41_token(spec) {
        return Some(ContractKind::FungibleToken);
    }
    None
}

/// Whether a spec declares the SEP-41 token interface. `decimals` is required:
/// it is what separates a fungible token from an NFT that also has
/// `balance`/`transfer`.
fn is_sep41_token(spec: &ContractSpec) -> bool {
    let has_function = |name: &str| spec.functions.iter().any(|f| f.name == name);
    has_function("balance") && has_function("transfer") && has_function("decimals")
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
    _ledger: i64,
) {
    match rpc.get_contract_instance(contract_id).await {
        Ok(Some(instance)) => {
            if let Some(hash) = &instance.wasm_hash {
                specs
                    .note_wasm_hash(pool, rpc, contract_id, hash, instance.last_modified_ledger)
                    .await;
            }
        }
        // No instance entry (e.g. archived), or the contract is a SAC with no
        // upgradable WASM — nothing to watch either way.
        Ok(None) => {}
        Err(e) => warn!(contract_id, error = %e, "upgrade check failed"),
    }
}

/// Read a previously persisted interface from `contract_specs` (#735).
///
/// Returns `(spec, wasm_hash)` when a row exists and its `spec_section` parses.
/// A row whose section is empty or unparseable yields `None`, so the caller
/// falls back to RPC rather than caching a bogus interface.
async fn load_persisted(
    pool: &PgPool,
    contract_id: &str,
) -> Option<(Arc<ContractSpec>, Option<String>)> {
    let row: Option<(String, String, bool)> = sqlx::query_as(
        "SELECT wasm_hash, spec_section, spec_complete FROM contract_specs
         WHERE contract_id = $1",
    )
    .bind(contract_id)
    .fetch_optional(pool)
    .await
    .unwrap_or_else(|e| {
        // A DB hiccup must not be fatal: fall through to RPC.
        warn!(contract_id, error = %e, "failed to read persisted contract spec");
        None
    });

    let (wasm_hash, spec_section, complete) = row?;
    let bytes = hex::decode(&spec_section).ok()?;
    let parsed = ContractSpec::from_spec_xdr(&bytes)?;
    if !complete {
        debug!(
            contract_id,
            wasm_hash, "loaded partial contract spec from database"
        );
    } else {
        debug!(contract_id, wasm_hash, "loaded contract spec from database");
    }
    Some((Arc::new(parsed.spec), Some(wasm_hash)))
}

/// Returns `(parsed spec, wasm hash, is_permanent_failure)`. The hash is present for any WASM contract
/// (even if its spec fails to parse) and `None` for a Stellar Asset Contract.
/// `is_permanent` is true when the failure is not retryable (SAC, parse error),
/// and false for transient failures (RPC error).
async fn load(pool: &PgPool, rpc: &RpcClient, contract_id: &str, ledger: i64) -> Loaded {
    let (wasm_hash, wasm) = match rpc.get_contract_wasm(contract_id).await {
        Ok(Some(w)) => w,
        Ok(None) => {
            debug!(
                contract_id,
                "contract has no WASM spec (e.g. SAC); skipping"
            );
            // Permanent: RPC succeeded but returned no WASM (SAC).
            return Loaded {
                spec: None,
                wasm_hash: None,
                is_permanent: true,
                versions: Vec::new(),
                // A Stellar Asset Contract is a SEP-41 token by construction.
                kind: Some(ContractKind::FungibleToken),
            };
        }
        Err(e) => {
            warn!(contract_id, error = %e, "failed to fetch contract WASM");
            // Transient: RPC error; should retry.
            return Loaded {
                spec: None,
                wasm_hash: None,
                is_permanent: false,
                versions: Vec::new(),
                kind: None,
            };
        }
    };

    let Some(parsed) = ContractSpec::from_wasm(&wasm) else {
        // Permanent: WASM exists but spec parsing failed entirely.
        return Loaded {
            spec: None,
            wasm_hash: Some(wasm_hash),
            is_permanent: true,
            versions: Vec::new(),
            kind: None,
        };
    };

    // #401: log partial parse so operators can investigate the contract.
    if !parsed.complete {
        warn!(
            contract_id,
            wasm_hash,
            skipped_entries = parsed.skipped_entries,
            first_error = parsed.error.as_deref().unwrap_or("unknown"),
            "contractspecv0 section is incomplete: some entries could not be decoded;              interface is partial and diffs against it will be skipped"
        );
    }

    let spec = parsed.spec;
    let complete = parsed.complete;
    let kind = classify_kind(&spec);

    // The raw section (hex) lets the read layer re-parse exact argument types.
    let spec_section = lumenqraph_core::spec::spec_section_of(&wasm)
        .map(hex::encode)
        .unwrap_or_default();
    info!(
        contract_id,
        events = spec.events.len(),
        functions = spec.functions.len(),
        complete,
        "parsed contract interface"
    );
    if let Err(e) = persist(
        pool,
        contract_id,
        &wasm_hash,
        &spec_section,
        &spec,
        complete,
    )
    .await
    {
        warn!(contract_id, error = %e, "failed to persist contract spec");
    }
    // Independent of the upsert above: that keeps only the current interface,
    // this appends to the history. A failure here must not cost us the spec.
    if let Err(e) = record_version(
        pool,
        contract_id,
        &wasm_hash,
        &spec_section,
        &spec,
        complete,
        ledger,
        None,
    )
    .await
    {
        warn!(contract_id, error = %e, "failed to record contract spec version");
    }

    // Load the version history so `get` can answer version-at-ledger queries.
    let versions = match load_versions(pool, contract_id).await {
        Ok(v) => v,
        Err(e) => {
            warn!(contract_id, error = %e, "failed to load contract spec versions");
            Vec::new()
        }
    };

    Loaded {
        spec: Some(Arc::new(spec)),
        wasm_hash: Some(wasm_hash),
        is_permanent: true,
        versions,
        kind,
    }
}

/// Load a contract's interface history from `contract_spec_versions`, sorted
/// ascending by effective ledger. Uses `upgrade_ledger` when known, falling back
/// to `observed_ledger` (#737).
async fn load_versions(pool: &PgPool, contract_id: &str) -> anyhow::Result<Vec<SpecVersion>> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT COALESCE(upgrade_ledger, observed_ledger) AS effective_ledger, spec_section
         FROM contract_spec_versions
         WHERE contract_id = $1
         ORDER BY effective_ledger ASC, version ASC",
    )
    .bind(contract_id)
    .fetch_all(pool)
    .await?;

    let mut versions = Vec::with_capacity(rows.len());
    for (effective_from_ledger, section) in rows {
        let Ok(bytes) = hex::decode(&section) else {
            continue;
        };
        let Some(parsed) = ContractSpec::from_spec_xdr(&bytes) else {
            continue;
        };
        versions.push(SpecVersion {
            effective_from_ledger,
            spec: Arc::new(parsed.spec),
        });
    }
    Ok(versions)
}

/// Append this interface to the contract's version history, unless it's the
/// version we already have at the tip of that history.
///
/// Called on every spec load, not just on a detected upgrade, so the history is
/// self-healing: a restart, a missed eviction, or an upgrade that happened while
/// we were down all still land here, and the hash check keeps it idempotent.
///
/// `observed_ledger` is the ledger at which we observed this interface (an
/// upper bound on when it went live). `upgrade_ledger` is the exact ledger of
/// the upgrade when known (#737); when `None` it is left NULL and consumers fall
/// back to `observed_ledger`.
#[allow(clippy::too_many_arguments)]
async fn record_version(
    pool: &PgPool,
    contract_id: &str,
    wasm_hash: &str,
    spec_section: &str,
    spec: &ContractSpec,
    // Whether the spec section was fully decoded (#401). When `false`, diffs
    // against this version are suppressed to prevent false "removal" alerts.
    complete: bool,
    observed_ledger: i64,
    upgrade_ledger: Option<i64>,
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
            // #401: skip diffing when the new spec is incomplete — a partial
            // parse would report every missing entry as "removed", triggering
            // false breaking-change webhooks.
            let diff = if complete {
                diff_against(&prev_section, spec)
            } else {
                warn!(
                    contract_id,
                    version = prev_version + 1,
                    "skipping interface diff: new spec is incomplete (partial parse)"
                );
                None
            };
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
            (contract_id, version, wasm_hash, previous_wasm_hash, interface, spec_section, diff, breaking, observed_ledger, upgrade_ledger, spec_complete)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
    .bind(observed_ledger)
    .bind(upgrade_ledger)
    .bind(complete)
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
    let previous = ContractSpec::from_spec_xdr(&bytes).map(|p| p.spec)?;
    Some(SpecDiff::between(&previous, new_spec))
}

async fn persist(
    pool: &PgPool,
    contract_id: &str,
    wasm_hash: &str,
    spec_section: &str,
    spec: &ContractSpec,
    // Whether the spec section was fully decoded (#401). Persisted so the
    // API can surface it in the interface response.
    complete: bool,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events, spec_complete)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (contract_id) DO UPDATE
           SET wasm_hash    = EXCLUDED.wasm_hash,
               interface    = EXCLUDED.interface,
               spec_section = EXCLUDED.spec_section,
               has_events   = EXCLUDED.has_events,
               spec_complete = EXCLUDED.spec_complete,
               fetched_at   = now()",
    )
    .bind(contract_id)
    .bind(wasm_hash)
    .bind(spec.to_interface_json())
    .bind(spec_section)
    .bind(spec.has_events())
    .bind(complete)
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
    //!
    //! Tests run in parallel — each gets its own isolated schema.

    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;
    use stellar_xdr::curr::{
        Limits, ScSpecEntry, ScSpecFunctionInputV0, ScSpecFunctionV0, ScSpecTypeDef, ScSymbol,
        WriteXdr,
    };

    /// Fresh, isolated schema per test — safe for parallel execution.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .expect("create test schema");
        admin.close().await;

        let option = format!("-c search_path={schema},public");
        let sep = if url.contains('?') { "&" } else { "?" };
        let schema_url = format!("{url}{sep}options={}", percent_encode(&option));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .expect("connect with search_path");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    fn percent_encode(s: &str) -> String {
        s.chars()
            .flat_map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => vec![c],
                c => format!("%{:02X}", c as u32).chars().collect(),
            })
            .collect()
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
        let spec = ContractSpec::from_spec_xdr_simple(&body).expect("test spec should parse");
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

    /// A spec section (hex) exposing exactly the named functions, each with the
    /// given input arity, plus the ContractSpec it parses to.
    fn spec_with_arities(functions: &[(&str, usize)]) -> (String, ContractSpec) {
        let mut body = Vec::new();
        for (name, arity) in functions {
            let inputs: Vec<ScSpecFunctionInputV0> = (0..*arity)
                .map(|i| ScSpecFunctionInputV0 {
                    doc: "".try_into().unwrap(),
                    name: format!("arg{i}").as_str().try_into().unwrap(),
                    type_: ScSpecTypeDef::U32,
                })
                .collect();
            let entry = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
                doc: "".try_into().unwrap(),
                name: ScSymbol((*name).try_into().unwrap()),
                inputs: inputs.try_into().unwrap(),
                outputs: vec![ScSpecTypeDef::U32].try_into().unwrap(),
            });
            body.extend(entry.to_xdr(Limits::none()).unwrap());
        }
        let spec = ContractSpec::from_spec_xdr_simple(&body).expect("test spec should parse");
        (hex::encode(&body), spec)
    }

    #[test]
    fn a_sep41_spec_is_a_fungible_token() {
        let (_, spec) = spec_with_arities(&[
            ("balance", 1),
            ("transfer", 3),
            ("decimals", 0),
            ("name", 0),
            ("symbol", 0),
        ]);
        assert!(is_sep41_token(&spec));
    }

    #[test]
    fn a_spec_without_decimals_is_not_a_fungible_token() {
        // An NFT: has transfer/balance but no decimals.
        let (_, spec) = spec_with_arities(&[("balance", 1), ("transfer", 3), ("name", 0)]);
        assert!(!is_sep41_token(&spec));
    }

    #[test]
    fn a_sac_contract_resolves_to_fungible_token() {
        let cache = SpecCache::new(16, 1);
        cache.inner.lock().unwrap().put(
            "CSAC".to_string(),
            Cached {
                spec: CachedSpec::NoSpec,
                wasm_hash: None,
                versions: Arc::new(Vec::new()),
                kind: Some(ContractKind::FungibleToken),
            },
        );
        assert_eq!(
            cache.contract_kind("CSAC"),
            Some(ContractKind::FungibleToken)
        );
    }

    #[test]
    fn a_wasm_contract_without_decimals_is_not_a_fungible_token() {
        let cache = SpecCache::new(16, 1);
        let (_, spec) = spec_with_arities(&[("balance", 1), ("transfer", 3), ("name", 0)]);
        let kind = classify_kind(&spec);
        cache.inner.lock().unwrap().put(
            "CNFT".to_string(),
            Cached {
                spec: CachedSpec::Spec(Arc::new(spec)),
                wasm_hash: Some("hash".into()),
                versions: Arc::new(Vec::new()),
                kind,
            },
        );
        assert_ne!(
            cache.contract_kind("CNFT"),
            Some(ContractKind::FungibleToken)
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn first_interface_is_a_baseline_with_no_diff() {
        let pool = fixture().await;
        let (section, spec) = spec_with(&["balance"]);
        record_version(&pool, "C1", "hash1", &section, &spec, true, 100, None)
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
            record_version(&pool, "C1", "hash1", &section, &spec, true, 100, None)
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
        record_version(&pool, "C1", "hash1", &s1, &spec1, true, 100, None)
            .await
            .unwrap();

        // v2 drops withdraw and adds pause: a breaking change.
        let (s2, spec2) = spec_with(&["balance", "pause"]);
        record_version(&pool, "C1", "hash2", &s2, &spec2, true, 101, None)
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
        record_version(&pool, "C1", "hash1", &section, &spec, true, 100, None)
            .await
            .unwrap();
        // New code, identical interface — e.g. a bug fix. Still an upgrade worth
        // recording, but nothing an integration needs to react to.
        record_version(&pool, "C1", "hash2", &section, &spec, true, 101, None)
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

    /// #735: with a `contract_specs` row present, `SpecCache::get` must serve
    /// the persisted interface without touching RPC. The RPC client here points
    /// at an unroutable address, so any `getLedgerEntries` call would fail (and
    /// the test would observe a `FetchError` instead of the spec).
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn get_serves_persisted_spec_without_rpc() {
        let pool = fixture().await;
        let (section, spec) = spec_with(&["balance"]);
        persist(&pool, "C1", "hash1", &section, &spec, true)
            .await
            .unwrap();

        // A client that can never reach a node: any RPC call fails.
        let rpc = RpcClient::new("http://127.0.0.1:1", 1);
        let cache = SpecCache::new(16, 4);

        let got = cache.get(&pool, &rpc, "C1", 100).await;
        let got = got.expect("persisted spec should be returned");
        assert_eq!(got.functions.len(), spec.functions.len());
        assert_eq!(cache.load_counts(), (1, 0), "load must come from the db");
    }

    /// #735: after a restart with many known contracts, the first cycle must
    /// make zero WASM fetches — every spec comes from `contract_specs`.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn restart_with_known_contracts_makes_no_rpc_fetches() {
        let pool = fixture().await;
        let (section, spec) = spec_with(&["balance"]);
        for i in 0..1_000 {
            let id = format!("C{i}");
            persist(&pool, &id, "hash1", &section, &spec, true)
                .await
                .unwrap();
        }

        let rpc = RpcClient::new("http://127.0.0.1:1", 1);
        let cache = SpecCache::new(2_000, 8);
        for i in 0..1_000 {
            let id = format!("C{i}");
            assert!(
                cache.get(&pool, &rpc, &id, 100).await.is_some(),
                "contract {id} should load from the database"
            );
        }
        assert_eq!(
            cache.load_counts(),
            (1_000, 0),
            "a warm restart must make zero RPC fetches"
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
                (contract_id, version, wasm_hash, interface, spec_section, observed_ledger)
             VALUES ('C1', 1, 'hash1', '{}', '', 100)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (s2, spec2) = spec_with(&["balance"]);
        record_version(&pool, "C1", "hash2", &s2, &spec2, true, 101, None)
            .await
            .unwrap();

        let diff: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT diff FROM contract_spec_versions WHERE version = 2")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            diff.is_none(),
            "an unparseable previous section must record no diff"
        );
    }
}
