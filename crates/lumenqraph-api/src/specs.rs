//! In-memory contract-spec cache.
//!
//! Every read-layer request (`/functions`, `/call`, `/simulate`) and every
//! interface diff needs a contract's raw `contractspecv0` section. Without a
//! cache each one re-reads that column from Postgres, hex-decodes it, and
//! re-parses the XDR — for a section that is tens of kilobytes and changes only
//! when the contract is upgraded, which is approximately never.
//!
//! # Staleness is not an option here
//!
//! A contract *can* be upgraded in place, and serving its old interface would
//! mean type-checking calls against a signature the chain no longer has — in a
//! project whose whole point is detecting exactly that change. It would also
//! mean this cache could silently outlive an upgrade the *indexer's own*
//! `SpecCache` already detected and recorded, since the two run in separate
//! processes with no direct notification between them. The cache keeps the
//! contract's `wasm_hash` and `contract_specs.fetched_at` alongside the parse
//! and revalidates both once [`REVALIDATE_AFTER`] has passed since the last
//! check (#647). Within that window a hit is served without a query, so an
//! upgrade is visible within that many seconds rather than immediately:
//!
//! - the validating query is a primary-key lookup returning one short hash and
//!   one timestamp, and
//! - the work it avoids is the large `spec_section` transfer, the hex decode,
//!   and the XDR parse.
//!
//! `wasm_hash` alone already forces a refetch on any genuine upgrade; `fetched_at`
//! is compared alongside it so that *any* write to the row — including one this
//! cache wasn't specifically watching for — is never missed, rather than trusting
//! hash equality as the sole word on freshness. See `docs/ARCHITECTURE.md` for the
//! full invalidation strategy.
//!
//! A hit used to cost one cheap round trip on every request. Since #647, a
//! validated entry is served without touching Postgres for
//! [`REVALIDATE_AFTER`]. That bounds how long an upgrade can go unseen to a few
//! seconds, in exchange for taking the validating query off the request path.
//! After the window, the next lookup re-runs the `wasm_hash`/`fetched_at` check
//! before the entry is used again.
//!
//! Historical versions (`contract_spec_versions`) need no such check: a version
//! that has been observed is immutable, so those are cached outright.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use lru::LruCache;
use lumenqraph_core::ContractSpec;
use parking_lot::Mutex;
use sqlx::PgPool;

use crate::error::{ApiError, ApiResult};

/// Max contracts held per map. In index-all mode the contract set is unbounded,
/// so this caps memory. When full, the least-recently-used entry is evicted
/// (#647). Clearing everything at once would cause a burst of re-parses.
const MAX_ENTRIES: usize = 256;

/// How long a validated `current` entry is served without querying Postgres
/// (#647). A genuine upgrade is visible to this API within this window.
const REVALIDATE_AFTER: Duration = Duration::from_secs(5);

/// A contract's interface in both the forms callers need: the parsed, indexed
/// view (the read layer's encoder resolves functions and UDTs against it, so a
/// request never re-parses the section) and the raw section, retained for
/// callers that need the original bytes. `parsed` is `None` when the section
/// doesn't parse — the read layer tolerates that, the diff endpoints don't.
pub struct CachedSpec {
    #[allow(dead_code)] // raw bytes for callers that re-parse; not read yet
    pub section: Vec<u8>,
    pub parsed: Option<ContractSpec>,
    /// Hash of the WASM this interface was parsed from. Part of the `/call`
    /// result-cache key, so an upgrade invalidates cached results.
    pub wasm_hash: String,
}

/// A `current` entry, with the row version it was parsed from.
struct CurrentEntry {
    /// `contract_specs.wasm_hash` at parse time.
    wasm_hash: String,
    /// `contract_specs.fetched_at` at parse time.
    fetched_at: DateTime<Utc>,
    /// When the row was last validated against Postgres.
    checked_at: Instant,
    spec: Arc<CachedSpec>,
}

pub struct SpecCache {
    /// contract_id -> entry. A `Mutex` rather than an `RwLock`, because an LRU
    /// must reorder on every read.
    current: Mutex<LruCache<String, CurrentEntry>>,
    /// (contract_id, version) -> entry. Immutable once observed.
    versions: Mutex<LruCache<(String, i32), Arc<CachedSpec>>>,
}

fn lru<K: std::hash::Hash + Eq, V>() -> Mutex<LruCache<K, V>> {
    let cap = NonZeroUsize::new(MAX_ENTRIES).expect("MAX_ENTRIES > 0");
    Mutex::new(LruCache::new(cap))
}

impl Default for SpecCache {
    fn default() -> Self {
        Self {
            current: lru(),
            versions: lru(),
        }
    }
}

impl SpecCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The contract's *current* interface. A validated entry younger than
    /// [`REVALIDATE_AFTER`] is returned without a database round trip. Older
    /// entries are revalidated against the stored `wasm_hash` and `fetched_at`.
    /// `404` with a precise message for SACs (which have no spec) or contracts
    /// that haven't been indexed yet.
    pub async fn current(&self, pool: &PgPool, contract_id: &str) -> ApiResult<Arc<CachedSpec>> {
        if let Some(spec) = self.fresh_current(contract_id) {
            return Ok(spec);
        }

        // In tests the cache may be pre-populated via `seed()` to avoid a DB
        // round-trip. A seeded entry uses the sentinel hash "test-hash".
        #[cfg(test)]
        if let Some(spec) = self.seeded(contract_id) {
            return Ok(spec);
        }

        let row: Option<(String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT wasm_hash, fetched_at FROM contract_specs WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_optional(pool)
        .await?;
        let (wasm_hash, fetched_at) = match row {
            Some((hash, fetched_at)) => (hash, fetched_at),
            None => {
                // Not in contract_specs; check if it's a SAC (has events but no WASM)
                let has_events: Option<(i64,)> =
                    sqlx::query_as("SELECT 1 FROM events WHERE contract_id = $1 LIMIT 1")
                        .bind(contract_id)
                        .fetch_optional(pool)
                        .await?;
                if has_events.is_some() {
                    return Err(ApiError::sac_not_supported(
                        "Stellar Asset Contract: no on-chain WASM interface. \
                         SACs publish only standard SEP-41 token conventions; \
                         use token metadata endpoints instead of /call or /simulate. \
                         Retrying will not help.",
                    ));
                }
                return Err(not_indexed());
            }
        };

        // `wasm_hash` changing means the contract was upgraded; `fetched_at` is
        // compared alongside it so any write to this row — including one made
        // by a code path this cache doesn't specifically know about — is never
        // missed.
        if let Some(spec) = self.revalidate(contract_id, &wasm_hash, fetched_at) {
            return Ok(spec);
        }

        let section: Option<(String,)> =
            sqlx::query_as("SELECT spec_section FROM contract_specs WHERE contract_id = $1")
                .bind(contract_id)
                .fetch_optional(pool)
                .await?;
        let hex_section = section
            .map(|r| r.0)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                ApiError::sac_not_supported(
                    "contract has no callable on-chain WASM interface (Stellar Asset Contract or \
                 equivalent). Retrying will not help; use token metadata endpoints instead of \
                 /call or /simulate.",
                )
            })?;
        let entry = parse(&hex_section, &wasm_hash)?;
        Ok(self.store_current(contract_id, wasm_hash, fetched_at, entry))
    }

    /// The entry for `contract_id` if it was validated within [`REVALIDATE_AFTER`].
    fn fresh_current(&self, contract_id: &str) -> Option<Arc<CachedSpec>> {
        let mut map = self.current.lock();
        let entry = map.get(contract_id)?;
        (entry.checked_at.elapsed() < REVALIDATE_AFTER).then(|| Arc::clone(&entry.spec))
    }

    /// Re-validate a cached entry against the row's current `wasm_hash` and
    /// `fetched_at`. On success the entry is marked as checked now and returned.
    /// `None` means the row changed, or nothing is cached, so the caller must
    /// re-fetch.
    fn revalidate(
        &self,
        contract_id: &str,
        wasm_hash: &str,
        fetched_at: DateTime<Utc>,
    ) -> Option<Arc<CachedSpec>> {
        let mut map = self.current.lock();
        let entry = map.get_mut(contract_id)?;
        if entry.wasm_hash == wasm_hash && entry.fetched_at >= fetched_at {
            entry.checked_at = Instant::now();
            return Some(Arc::clone(&entry.spec));
        }
        None
    }

    /// Insert a freshly parsed `current` entry, evicting the LRU entry if full.
    fn store_current(
        &self,
        contract_id: &str,
        wasm_hash: String,
        fetched_at: DateTime<Utc>,
        spec: CachedSpec,
    ) -> Arc<CachedSpec> {
        let spec = Arc::new(spec);
        self.current.lock().put(
            contract_id.to_string(),
            CurrentEntry {
                wasm_hash,
                fetched_at,
                checked_at: Instant::now(),
                spec: Arc::clone(&spec),
            },
        );
        spec
    }

    /// The contract's interface at a historical version. Cached without
    /// revalidation: an observed version never changes.
    pub async fn at_version(
        &self,
        pool: &PgPool,
        contract_id: &str,
        version: i32,
    ) -> ApiResult<Arc<CachedSpec>> {
        let key = (contract_id.to_string(), version);
        if let Some(spec) = self.versions.lock().get(&key) {
            return Ok(Arc::clone(spec));
        }

        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT wasm_hash, spec_section FROM contract_spec_versions
             WHERE contract_id = $1 AND version = $2",
        )
        .bind(contract_id)
        .bind(version)
        .fetch_optional(pool)
        .await?;
        let (wasm_hash, hex_section) = row.ok_or_else(|| {
            ApiError::not_found(format!("no version {version} recorded for this contract"))
        })?;
        let entry = Arc::new(parse(&hex_section, &wasm_hash)?);

        self.versions.lock().put(key, Arc::clone(&entry));
        Ok(entry)
    }

    /// Evict a contract's current interface from the in-memory cache, forcing
    /// subsequent lookups to re-read and re-parse.
    pub fn invalidate(&self, contract_id: &str) {
        self.current.lock().pop(contract_id);
    }

    /// Seed a pre-built spec directly into the cache, bypassing the database.
    /// Used only in handler tests where a real Postgres connection is not needed.
    #[cfg(test)]
    pub fn seed(&self, contract_id: &str, spec: CachedSpec) {
        self.current.lock().put(
            contract_id.to_string(),
            CurrentEntry {
                wasm_hash: "test-hash".to_string(),
                fetched_at: Utc::now(),
                checked_at: Instant::now(),
                spec: Arc::new(spec),
            },
        );
    }

    /// A seeded entry, if present. Seeded entries never expire in tests.
    #[cfg(test)]
    fn seeded(&self, contract_id: &str) -> Option<Arc<CachedSpec>> {
        let mut map = self.current.lock();
        let entry = map.get(contract_id)?;
        (entry.wasm_hash == "test-hash").then(|| Arc::clone(&entry.spec))
    }
}

/// Hex-decode and parse a stored section. A section we wrote but can't read back
/// is our bug, not the caller's, so it's a 500.
fn parse(hex_section: &str, wasm_hash: &str) -> ApiResult<CachedSpec> {
    let section = hex::decode(hex_section)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("corrupt stored spec: {e}")))?;
    let parsed = ContractSpec::from_spec_xdr(&section).map(|p| p.spec);
    Ok(CachedSpec {
        section,
        parsed,
        wasm_hash: wasm_hash.to_string(),
    })
}

fn not_indexed() -> ApiError {
    ApiError::spec_unavailable(
        "no interface indexed for this contract yet; the indexer fetches it \
         on first sighting — retry after the indexer has seen this contract",
    )
}

#[cfg(test)]
mod tests {
    //! These need a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-api -- --ignored
    //!
    //! Tests run in parallel — each gets its own isolated schema.

    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use stellar_xdr::curr::{
        Limits, ScSpecEntry, ScSpecFunctionV0, ScSpecTypeDef, ScSymbol, WriteXdr,
    };

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

    /// A spec section (hex) exposing exactly the named zero-arg functions.
    fn section_with(functions: &[&str]) -> String {
        let mut body = Vec::new();
        for name in functions {
            let e = ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
                doc: "".try_into().unwrap(),
                name: ScSymbol((*name).try_into().unwrap()),
                inputs: vec![].try_into().unwrap(),
                outputs: vec![ScSpecTypeDef::U32].try_into().unwrap(),
            });
            body.extend(e.to_xdr(Limits::none()).unwrap());
        }
        hex::encode(body)
    }

    async fn upsert_spec(pool: &PgPool, wasm_hash: &str, section: &str) {
        sqlx::query(
            "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events)
             VALUES ('C1', $1, '{}', $2, false)
             ON CONFLICT (contract_id) DO UPDATE
               SET wasm_hash = EXCLUDED.wasm_hash, spec_section = EXCLUDED.spec_section",
        )
        .bind(wasm_hash)
        .bind(section)
        .execute(pool)
        .await
        .unwrap();
    }

    fn fn_names(spec: &CachedSpec) -> Vec<String> {
        spec.parsed
            .as_ref()
            .unwrap()
            .functions
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn caches_a_parsed_spec_and_serves_it_again() {
        let pool = fixture().await;
        upsert_spec(&pool, "hash1", &section_with(&["balance"])).await;
        let cache = SpecCache::new();

        let a = cache.current(&pool, "C1").await.unwrap();
        let b = cache.current(&pool, "C1").await.unwrap();
        assert!(Arc::ptr_eq(&a, &b), "second lookup should reuse the parse");
        assert_eq!(fn_names(&a), ["balance"]);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_upgrade_invalidates_the_cached_spec() {
        let pool = fixture().await;
        upsert_spec(&pool, "hash1", &section_with(&["balance"])).await;
        let cache = SpecCache::new();
        assert_eq!(
            fn_names(&cache.current(&pool, "C1").await.unwrap()),
            ["balance"]
        );

        // The indexer upserts a new interface on upgrade. Serving the old one
        // here would type-check calls against a signature the chain no longer
        // has — the exact failure this cache must never cause.
        upsert_spec(&pool, "hash2", &section_with(&["balance", "pause"])).await;

        let after = cache.current(&pool, "C1").await.unwrap();
        assert_eq!(fn_names(&after), ["balance", "pause"]);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_unindexed_contract_is_not_found() {
        let pool = fixture().await;
        let cache = SpecCache::new();
        let err = cache
            .current(&pool, "CNOPE")
            .await
            .err()
            .expect("should 404");
        assert!(matches!(err, ApiError::Status(s, _, _) if s == axum::http::StatusCode::NOT_FOUND));
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn historical_versions_are_cached_and_independent() {
        let pool = fixture().await;
        for (v, hash, fns) in [
            (1, "hash1", vec!["balance", "withdraw"]),
            (2, "hash2", vec!["balance", "pause"]),
        ] {
            sqlx::query(
                "INSERT INTO contract_spec_versions
                    (contract_id, version, wasm_hash, interface, spec_section)
                 VALUES ('C1', $1, $2, '{}', $3)",
            )
            .bind(v)
            .bind(hash)
            .bind(section_with(&fns))
            .execute(&pool)
            .await
            .unwrap();
        }
        let cache = SpecCache::new();

        let v1 = cache.at_version(&pool, "C1", 1).await.unwrap();
        let v2 = cache.at_version(&pool, "C1", 2).await.unwrap();
        assert_eq!(fn_names(&v1), ["balance", "withdraw"]);
        assert_eq!(fn_names(&v2), ["balance", "pause"]);
        // Each version is keyed separately; one must not evict or shadow another.
        assert!(Arc::ptr_eq(
            &v1,
            &cache.at_version(&pool, "C1", 1).await.unwrap()
        ));

        let missing = cache
            .at_version(&pool, "C1", 9)
            .await
            .err()
            .expect("should 404");
        assert!(
            matches!(missing, ApiError::Status(s, _, _) if s == axum::http::StatusCode::NOT_FOUND)
        );
    }
}

/// Cache behaviour that needs no database (#647). A lazy pool that cannot
/// connect stands in for Postgres, so a query made on it fails the test.
#[cfg(test)]
mod cache_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn unreachable_pool() -> PgPool {
        PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/none")
            .expect("lazy pool")
    }

    fn spec(hash: &str) -> CachedSpec {
        CachedSpec {
            section: vec![],
            parsed: None,
            wasm_hash: hash.to_string(),
        }
    }

    #[tokio::test]
    async fn fresh_hit_makes_no_database_query() {
        let cache = SpecCache::new();
        cache.store_current("C_FRESH", "h1".into(), Utc::now(), spec("h1"));
        // Every query fails on this pool, so getting Ok proves none ran.
        let got = cache
            .current(&unreachable_pool(), "C_FRESH")
            .await
            .expect("fresh entry served from cache");
        assert_eq!(got.wasm_hash, "h1");
    }

    #[tokio::test]
    async fn stale_entry_is_revalidated_against_the_database() {
        let cache = SpecCache::new();
        cache.store_current("C_STALE", "h1".into(), Utc::now(), spec("h1"));
        cache.current.lock().get_mut("C_STALE").unwrap().checked_at =
            Instant::now() - REVALIDATE_AFTER - Duration::from_secs(1);
        // Past the window the lookup must query, and that fails on this pool.
        assert!(cache.current(&unreachable_pool(), "C_STALE").await.is_err());
    }

    #[test]
    fn revalidation_detects_upgrade_and_row_writes() {
        let cache = SpecCache::new();
        let t0 = Utc::now();
        cache.store_current("C", "h1".into(), t0, spec("h1"));

        assert!(
            cache.revalidate("C", "h1", t0).is_some(),
            "unchanged row keeps entry"
        );
        assert!(
            cache.revalidate("C", "h2", t0).is_none(),
            "new wasm_hash is an upgrade"
        );
        let later = t0 + chrono::Duration::seconds(1);
        assert!(
            cache.revalidate("C", "h1", later).is_none(),
            "rewritten row is a miss"
        );
    }

    #[test]
    fn revalidation_marks_entry_as_checked_now() {
        let cache = SpecCache::new();
        let t0 = Utc::now();
        cache.store_current("C", "h1".into(), t0, spec("h1"));
        cache.current.lock().get_mut("C").unwrap().checked_at =
            Instant::now() - REVALIDATE_AFTER - Duration::from_secs(1);
        assert!(cache.fresh_current("C").is_none());
        assert!(cache.revalidate("C", "h1", t0).is_some());
        assert!(
            cache.fresh_current("C").is_some(),
            "revalidation restarts the window"
        );
    }

    #[test]
    fn full_cache_evicts_least_recently_used_not_everything() {
        let cache = SpecCache::new();
        let now = Utc::now();
        for i in 0..MAX_ENTRIES {
            cache.store_current(&format!("C{i}"), "h".into(), now, spec("h"));
        }
        // Use C0 so that C1 becomes the least recently used entry.
        assert!(cache.fresh_current("C0").is_some());

        cache.store_current("C_NEW", "h".into(), now, spec("h"));

        assert!(
            cache.fresh_current("C0").is_some(),
            "recently used entry must survive"
        );
        assert!(
            cache.fresh_current("C1").is_none(),
            "LRU entry must be evicted"
        );
        assert!(
            cache.fresh_current("C2").is_some(),
            "other entries must survive, not be cleared"
        );
        assert_eq!(cache.current.lock().len(), MAX_ENTRIES);
    }

    #[test]
    fn invalidate_drops_current_entry() {
        let cache = SpecCache::new();
        cache.store_current("C", "h".into(), Utc::now(), spec("h"));
        cache.invalidate("C");
        assert!(cache.fresh_current("C").is_none());
    }
}
