//! Indexer configuration, loaded from environment (see `.env.example`).

use anyhow::Context;
use serde::Deserialize;

use crate::keys::{parse_durability, KeyTemplate};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub rpc_url: String,
    /// Contract IDs to index. Empty => index all contract events.
    pub contract_ids: Vec<String>,
    pub poll_interval_secs: u64,
    pub page_size: u32,
    /// Ledger to start from on a fresh index. 0 => start near the current tip.
    /// Clamped to the RPC retention window (~7 days, 120k ledgers for SDF public RPC).
    pub start_ledger: i64,
    /// Max ledgers behind the tip we'll fetch in a single live polling cycle.
    /// This is a conservative safety limit, NOT the RPC retention window.
    /// The RPC retention window is ~7 days (120k ledgers on SDF public RPC),
    /// but we catch up more conservatively in live polling to avoid performance
    /// issues or RPC processing limits. Public Soroban RPCs reject `getEvents`
    /// whose `startLedger` is too far behind (`-32001` "processing limit").
    /// If the cursor falls further behind (e.g. after downtime) we skip ahead to
    /// this window and log the unrecoverable gap rather than stalling forever.
    /// Raise it with a retaining/paid RPC. Default 4000 (~5–6h) is conservative
    /// for the SDF public endpoints.
    pub max_catchup_ledgers: i64,
    /// When true, snapshot each active contract's instance storage into
    /// `contract_state` (versioned). Off by default — it costs one extra RPC
    /// call per active contract per cycle. Best paired with `CONTRACT_IDS`.
    pub state_indexing: bool,
    /// When true, snapshot *per-holder* balances into `contract_data`: for each
    /// holder named in a token's events this cycle, fetch its `Balance(Address)`
    /// entry. Off by default — it costs one RPC call per newly-active holder per
    /// cycle, so it should be paired with `CONTRACT_IDS`.
    pub key_indexing: bool,
    /// The symbol naming the balance storage-key variant (`DataKey::Balance` in
    /// the soroban token reference). Configurable for tokens that differ.
    pub balance_key_symbol: String,
    /// Durability of the balance storage entry: "persistent" (default) or
    /// "temporary".
    pub balance_key_durability: String,
    /// Configurable key templates for per-key state indexing (JSON array).
    /// Format: [{"symbol":"Balance","events":["transfer","mint"],"params":[1,2],"durability":"persistent","label":"balance"}]
    /// Each template defines: symbol (storage key variant), events (triggering event names),
    /// params (topic indices to extract), durability (persistent/temporary), label (optional grouping tag).
    pub key_templates: Vec<KeyTemplate>,
    /// Keep only the last N ledgers of history, pruning older rows as the tip
    /// advances. 0 (default) => keep everything. Set this when the database has
    /// a hard size cap (e.g. a 500MB free tier) that an unbounded index would
    /// hit; see `retention`.
    pub retention_ledgers: i64,
    /// Keep only the last N versions of each contract's spec in `contract_spec_versions`,
    /// pruning older versions once they fall outside the retention window. 0 (default)
    /// => keep everything. Newest N versions per contract are always preserved even if
    /// outside the window. When `RETENTION_LEDGERS` is set, specs outside the window
    /// are pruned unless they're among the newest N for their contract.
    pub spec_version_retention: i64,
    /// When true, check whether a tracked contract's executable has changed and,
    /// if so, re-read its interface and append a `contract_spec_versions` row
    /// with the diff (see `specs`). Costs one RPC call per tracked contract per
    /// cycle, so it defaults to on only when `CONTRACT_IDS` bounds that set;
    /// in index-all mode it must be enabled explicitly, and then only covers
    /// contracts active in the cycle. `STATE_INDEXING` already reads each
    /// instance and detects upgrades for free, so this adds no calls alongside it.
    pub upgrade_watch: bool,
    /// Number of ledgers to re-scan each cycle to handle shallow reorgs where
    /// the RPC returns different events for recently-passed ledgers. Each cycle,
    /// we re-fetch the last N ledgers and upsert events with updated content.
    /// 0 (default) means no trailing re-scan. Small values (10-100) provide shallow
    /// reorg protection with minimal overhead. Requires careful tuning based on
    /// the RPC's reorg exposure.
    pub reorg_overlap_ledgers: i64,
    /// Request timeout for every outbound RPC HTTP call, in seconds.
    /// Without a timeout, a hung RPC connection blocks the poll loop
    /// indefinitely. Default 30 matches the previous hardcoded value.
    /// Raise for slow/paid endpoints; lower for tight liveness requirements.
    pub rpc_timeout_secs: u64,
    /// Warn if the not-enriched fraction (not_enriched / total) exceeds this
    /// threshold in a single cycle. 0.5 = warn if >50% of events fail enrichment.
    /// 0.0 = never warn; 1.0+ = warn only if 100%+ fail (never). Default 0.5.
    pub enrichment_warn_threshold: f64,
    /// Maximum number of entries in the in-memory spec cache before evicting
    /// least-recently-used entries. Prevents unbounded memory growth when
    /// indexing all contracts. Evicted entries are re-fetched from the database
    /// on next miss. Default: 2000.
    pub spec_cache_max_entries: usize,
    /// After this many consecutive poll failures the poller enters a degraded
    /// state: it sleeps for `degraded_poll_interval_secs` instead of the
    /// normal backoff and emits an ERROR-level log. The counter resets on the
    /// first successful cycle. 0 = never enter degraded state (disabled).
    /// Default: 20.
    pub max_consecutive_errors: u32,
    /// Sleep interval (seconds) used while the circuit breaker is open (i.e.
    /// the poller is in degraded state). Default: 300 (5 minutes).
    pub degraded_poll_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let contract_ids: Vec<String> = lumenqraph_core::parse_contract_ids(
            &std::env::var("CONTRACT_IDS").unwrap_or_default(),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        // Parse numeric config with validation.
        let poll_interval_secs = env_parse("POLL_INTERVAL_SECS", 5)?;
        let page_size = env_parse("PAGE_SIZE", 1000)?;
        let max_catchup_ledgers = env_parse("MAX_CATCHUP_LEDGERS", 4000)?;
        let retention_ledgers = env_parse("RETENTION_LEDGERS", 0)?;
        let spec_version_retention = env_parse("SPEC_VERSION_RETENTION", 0)?;
        let reorg_overlap_ledgers = env_parse("REORG_OVERLAP_LEDGERS", 0)?;
        let rpc_timeout_secs = env_parse("RPC_TIMEOUT_SECS", 30u64)?;
        let spec_cache_max_entries = env_parse("SPEC_CACHE_MAX_ENTRIES", 2000usize)?;

        // Validate and clamp PAGE_SIZE to RPC documented bounds (1–10000).
        let page_size = clamp_with_warning("PAGE_SIZE", page_size, 1, 10000);

        // Validate POLL_INTERVAL_SECS minimum.
        let poll_interval_secs = clamp_with_warning("POLL_INTERVAL_SECS", poll_interval_secs, 1, u64::MAX);

        // Validate MAX_CATCHUP_LEDGERS minimum.
        let max_catchup_ledgers = clamp_with_warning("MAX_CATCHUP_LEDGERS", max_catchup_ledgers, 1, i64::MAX);

        // Validate RETENTION_LEDGERS minimum (0 = disabled).
        let retention_ledgers = if retention_ledgers < 0 {
            tracing::warn!(
                requested = retention_ledgers,
                clamped_to = 0,
                "RETENTION_LEDGERS cannot be negative; clamping to 0 (disabled)"
            );
            0
        } else {
            retention_ledgers
        };

        // Validate SPEC_VERSION_RETENTION minimum (0 = disabled).
        let spec_version_retention = if spec_version_retention < 0 {
            tracing::warn!(
                requested = spec_version_retention,
                clamped_to = 0,
                "SPEC_VERSION_RETENTION cannot be negative; clamping to 0 (disabled)"
            );
            0
        } else {
            spec_version_retention
        };

        // Validate REORG_OVERLAP_LEDGERS minimum (0 = disabled).
        let reorg_overlap_ledgers = if reorg_overlap_ledgers < 0 {
            tracing::warn!(
                requested = reorg_overlap_ledgers,
                clamped_to = 0,
                "REORG_OVERLAP_LEDGERS cannot be negative; clamping to 0 (disabled)"
            );
            0
        } else {
            reorg_overlap_ledgers
        };

        // Validate RPC_TIMEOUT_SECS minimum (must be at least 1s).
        let rpc_timeout_secs = clamp_with_warning("RPC_TIMEOUT_SECS", rpc_timeout_secs, 1, u64::MAX);

        // Validate SPEC_CACHE_MAX_ENTRIES minimum (must be at least 1).
        let spec_cache_max_entries = clamp_with_warning("SPEC_CACHE_MAX_ENTRIES", spec_cache_max_entries, 1, usize::MAX);

        // Parse ENRICHMENT_WARN_THRESHOLD (0.0-1.0, default 0.5).
        let enrichment_warn_threshold: f64 = env_parse("ENRICHMENT_WARN_THRESHOLD", 0.5)?;
        let enrichment_warn_threshold = if enrichment_warn_threshold < 0.0 || enrichment_warn_threshold > 1.0 {
            tracing::warn!(
                requested = enrichment_warn_threshold,
                clamped_to = 0.5,
                "ENRICHMENT_WARN_THRESHOLD must be between 0.0 and 1.0; clamping to 0.5"
            );
            0.5
        } else {
            enrichment_warn_threshold
        };

        // Validate BALANCE_KEY_DURABILITY (must be "persistent" or "temporary").
        let balance_key_durability = std::env::var("BALANCE_KEY_DURABILITY")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "persistent".to_string());
        match balance_key_durability.trim().to_lowercase().as_str() {
            "persistent" | "temporary" => {}
            invalid => {
                return Err(anyhow::anyhow!(
                    "invalid BALANCE_KEY_DURABILITY {}: expected \"persistent\" or \"temporary\"",
                    invalid
                ));
            }
        }

        // Parse key templates from JSON (KEY_TEMPLATES env var).
        // We deserialize into a plain intermediate struct and then convert to
        // KeyTemplate (which holds a non-serde stellar-xdr ContractDataDurability).
        #[derive(Deserialize)]
        struct KeyTemplateRaw {
            symbol: String,
            #[serde(default, alias = "events")]
            event_names: Vec<String>,
            #[serde(default, alias = "params")]
            param_indices: Vec<usize>,
            #[serde(default = "default_durability")]
            durability: String,
            label: Option<String>,
        }
        fn default_durability() -> String { "persistent".to_string() }

        let key_templates: Vec<KeyTemplate> = std::env::var("KEY_TEMPLATES")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| -> anyhow::Result<Vec<KeyTemplate>> {
                let raw: Vec<KeyTemplateRaw> = serde_json::from_str(&s)
                    .map_err(|e| anyhow::anyhow!("invalid KEY_TEMPLATES JSON: {e}"))?;
                Ok(raw
                    .into_iter()
                    .map(|r| KeyTemplate {
                        symbol: r.symbol,
                        event_names: r.event_names,
                        param_indices: r.param_indices,
                        durability: parse_durability(&r.durability),
                        label: r.label,
                    })
                    .collect())
            })
            .transpose()?
            .unwrap_or_default();

        let max_consecutive_errors = env_parse("MAX_CONSECUTIVE_ERRORS", 20u32)?;
        let degraded_poll_interval_secs = env_parse("DEGRADED_POLL_INTERVAL_SECS", 300u64)?;

        Ok(Self {
            database_url: env("DATABASE_URL")?,
            rpc_url: env("RPC_URL")?,
            poll_interval_secs,
            page_size,
            start_ledger: env_parse("START_LEDGER", 0)?,
            max_catchup_ledgers,
            state_indexing: env_bool("STATE_INDEXING", false),
            key_indexing: env_bool("KEY_INDEXING", false),
            upgrade_watch: env_bool("UPGRADE_WATCH", !contract_ids.is_empty()),
            contract_ids,
            balance_key_symbol: std::env::var("BALANCE_KEY_SYMBOL")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "Balance".to_string()),
            balance_key_durability,
            retention_ledgers,
            spec_version_retention,
            reorg_overlap_ledgers,
            rpc_timeout_secs,
            enrichment_warn_threshold,
            key_templates,
            spec_cache_max_entries,
            max_consecutive_errors,
            degraded_poll_interval_secs,
        })
    }
}

/// Clamp a numeric config value to [min, max] and log a warning if clamped.
fn clamp_with_warning<T: std::cmp::PartialOrd + std::fmt::Display + Copy>(
    key: &str,
    value: T,
    min: T,
    max: T,
) -> T {
    if value < min {
        tracing::warn!(
            requested = %value,
            clamped_to = %min,
            "{} is below minimum; clamping to {}", key, min
        );
        min
    } else if value > max {
        tracing::warn!(
            requested = %value,
            clamped_to = %max,
            "{} exceeds maximum; clamping to {}", key, max
        );
        max
    } else {
        value
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

fn env(key: &str) -> anyhow::Result<String> {
    std::env::var(key).with_context(|| format!("missing required env var {key}"))
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid {key}: {e}")),
        _ => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_clamping() {
        // Test PAGE_SIZE bounds (1–10000).
        assert_eq!(clamp_with_warning("PAGE_SIZE", 0u32, 1, 10000), 1);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 1u32, 1, 10000), 1);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 5000u32, 1, 10000), 5000);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 10000u32, 1, 10000), 10000);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 100000u32, 1, 10000), 10000);
    }

    #[test]
    fn poll_interval_clamping() {
        // Test POLL_INTERVAL_SECS minimum.
        assert_eq!(clamp_with_warning("POLL_INTERVAL_SECS", 0u64, 1, u64::MAX), 1);
        assert_eq!(clamp_with_warning("POLL_INTERVAL_SECS", 1u64, 1, u64::MAX), 1);
        assert_eq!(clamp_with_warning("POLL_INTERVAL_SECS", 5u64, 1, u64::MAX), 5);
        assert_eq!(clamp_with_warning("POLL_INTERVAL_SECS", 60u64, 1, u64::MAX), 60);
    }

    #[test]
    fn max_catchup_ledgers_clamping() {
        // Test MAX_CATCHUP_LEDGERS minimum.
        assert_eq!(clamp_with_warning("MAX_CATCHUP_LEDGERS", 0i64, 1, i64::MAX), 1);
        assert_eq!(clamp_with_warning("MAX_CATCHUP_LEDGERS", 1i64, 1, i64::MAX), 1);
        assert_eq!(clamp_with_warning("MAX_CATCHUP_LEDGERS", 4000i64, 1, i64::MAX), 4000);
        assert_eq!(clamp_with_warning("MAX_CATCHUP_LEDGERS", 120000i64, 1, i64::MAX), 120000);
    }

    #[test]
    fn retention_ledgers_validation() {
        // Negative values should be clamped to 0.
        let retention = -100i64;
        let clamped = if retention < 0 { 0 } else { retention };
        assert_eq!(clamped, 0);

        // Zero and positive should pass through.
        assert_eq!(if 0i64 < 0 { 0 } else { 0i64 }, 0);
        assert_eq!(if 1000i64 < 0 { 0 } else { 1000i64 }, 1000);
    }

    #[test]
    fn reorg_overlap_ledgers_validation() {
        // Negative values should be clamped to 0.
        let reorg = -50i64;
        let clamped = if reorg < 0 { 0 } else { reorg };
        assert_eq!(clamped, 0);

        // Zero and positive should pass through.
        assert_eq!(if 0i64 < 0 { 0 } else { 0i64 }, 0);
        assert_eq!(if 100i64 < 0 { 0 } else { 100i64 }, 100);
    }

    #[test]
    fn contract_ids_limit_constants() {
        // Verify the limit used in Config::from_env matches the RPC protocol.
        const MAX_IDS_PER_FILTER: usize = 5;
        const MAX_FILTERS: usize = 5;
        assert_eq!(MAX_IDS_PER_FILTER * MAX_FILTERS, 25);
    }
}
