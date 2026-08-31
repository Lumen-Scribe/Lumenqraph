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
//! processes with no direct notification between them. So this doesn't cache
//! on a timer and hope. Instead it keeps the contract's `wasm_hash` and
//! `contract_specs.fetched_at` alongside the parse and revalidates both on
//! every lookup:
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
//! So a hit still costs one cheap round trip and is always correct, which is the
//! right trade for a cache guarding a correctness-critical value.
//!
//! Historical versions (`contract_spec_versions`) need no such check: a version
//! that has been observed is immutable, so those are cached outright.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use lumenqraph_core::ContractSpec;
use sqlx::PgPool;

use crate::error::{ApiError, ApiResult};

/// Max contracts held per map. In index-all mode the contract set is unbounded,
/// so this caps memory. Eviction is a crude wholesale clear rather than an LRU:
/// refilling an entry costs one query, and a cache that never grows is easier to
/// reason about than one that pretends to be smart.
const MAX_ENTRIES: usize = 256;

/// A contract's interface in both the forms callers need: the raw section (the
/// read layer's encoder re-parses it to encode call arguments) and the parsed
/// view. `parsed` is `None` when the section doesn't parse — the read layer
/// tolerates that, the diff endpoints don't.
pub struct CachedSpec {
    pub section: Vec<u8>,
    pub parsed: Option<ContractSpec>,
}

#[derive(Default)]
pub struct SpecCache {
    /// contract_id -> (wasm_hash the entry was parsed from, contract_specs.fetched_at
    /// at parse time, entry). `wasm_hash` changing is the primary invalidation
    /// signal (a real upgrade); `fetched_at` is compared alongside it so a write
    /// to the row is never missed even in the cases hash equality alone wouldn't
    /// cover.
    current: RwLock<HashMap<String, (String, DateTime<Utc>, Arc<CachedSpec>)>>,
    /// (contract_id, version) -> entry. Immutable once observed.
    versions: RwLock<HashMap<(String, i32), Arc<CachedSpec>>>,
}

impl SpecCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The contract's *current* interface, revalidated against its stored
    /// `wasm_hash`. `404` with a precise message for SACs (which have no spec)
    /// or contracts that haven't been indexed yet.
    pub async fn current(&self, pool: &PgPool, contract_id: &str) -> ApiResult<Arc<CachedSpec>> {
        // In tests the cache may be pre-populated via `seed()` to avoid a DB
        // round-trip. A seeded entry uses the sentinel hash "test-hash".
        #[cfg(test)]
        if let Some((hash, _, spec)) = self.current.read().unwrap().get(contract_id) {
            if hash == "test-hash" {
                return Ok(Arc::clone(spec));
            }
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
                let has_events: Option<(i64,)> = sqlx::query_as(
                    "SELECT 1 FROM events WHERE contract_id = $1 LIMIT 1"
                )
                .bind(contract_id)
                .fetch_optional(pool)
                .await?;
                if has_events.is_some() {
                    return Err(ApiError::sac_not_supported(
                        "Stellar Asset Contract: no on-chain WASM interface. \
                         SACs publish only standard SEP-41 token conventions; \
                         use token metadata endpoints instead of /call or /simulate. \
                         Retrying will not help."
                    ));
                }
                return Err(not_indexed());
            }
        };

        // `wasm_hash` changing means the contract was upgraded; `fetched_at` is
        // compared alongside it so any write to this row — including one made
        // by a code path this cache doesn't specifically know about — is never
        // missed, with no reliance on a TTL.
        if let Some((hash, cached_fetched_at, spec)) = self.current.read().unwrap().get(contract_id) {
            if *hash == wasm_hash && *cached_fetched_at >= fetched_at {
                return Ok(Arc::clone(spec));
            }
            // Fall through: the contract's spec row changed since we cached it.
        }

        let section: Option<(String,)> =
            sqlx::query_as("SELECT spec_section FROM contract_specs WHERE contract_id = $1")
                .bind(contract_id)
                .fetch_optional(pool)
                .await?;
        let hex_section = section
            .map(|r| r.0)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ApiError::sac_not_supported(
                "contract has no callable on-chain WASM interface (Stellar Asset Contract or \
                 equivalent). Retrying will not help; use token metadata endpoints instead of \
                 /call or /simulate."
            ))?;
        let entry = Arc::new(parse(&hex_section)?);

        let mut map = self.current.write().unwrap();
        if map.len() >= MAX_ENTRIES {
            map.clear();
        }
        map.insert(contract_id.to_string(), (wasm_hash, fetched_at, Arc::clone(&entry)));
        Ok(entry)
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
        if let Some(spec) = self.versions.read().unwrap().get(&key) {
            return Ok(Arc::clone(spec));
        }

        let row: Option<(String,)> = sqlx::query_as(
            "SELECT spec_section FROM contract_spec_versions
             WHERE contract_id = $1 AND version = $2",
        )
        .bind(contract_id)
        .bind(version)
        .fetch_optional(pool)
        .await?;
        let hex_section = row.map(|r| r.0).ok_or_else(|| {
            ApiError::not_found(format!("no version {version} recorded for this contract"))
        })?;
        let entry = Arc::new(parse(&hex_section)?);

        let mut map = self.versions.write().unwrap();
        if map.len() >= MAX_ENTRIES {
            map.clear();
        }
        map.insert(key, Arc::clone(&entry));
        Ok(entry)
    }

    /// Evict a contract's current interface from the in-memory cache, forcing
    /// subsequent lookups to re-read and re-parse.
    pub fn invalidate(&self, contract_id: &str) {
        let mut map = self.current.write().unwrap();
        map.remove(contract_id);
    }

    /// Seed a pre-built spec directly into the cache, bypassing the database.
    /// Used only in handler tests where a real Postgres connection is not needed.
    #[cfg(test)]
    pub fn seed(&self, contract_id: &str, spec: CachedSpec) {
        let arc = Arc::new(spec);
        self.current.write().unwrap().insert(
            contract_id.to_string(),
            ("test-hash".to_string(), Utc::now(), arc),
        );
    }
}

/// Hex-decode and parse a stored section. A section we wrote but can't read back
/// is our bug, not the caller's, so it's a 500.
fn parse(hex_section: &str) -> ApiResult<CachedSpec> {
    let section = hex::decode(hex_section)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("corrupt stored spec: {e}")))?;
    let parsed = ContractSpec::from_spec_xdr(&section);
    Ok(CachedSpec { section, parsed })
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
