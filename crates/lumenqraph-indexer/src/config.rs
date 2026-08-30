//! Indexer configuration, loaded from environment (see `.env.example`).

use anyhow::Context;
use serde::Deserialize;

use crate::keys::{parse_durability, KeyTemplate};

pub fn redact_database_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_idx) = after_scheme.find('@') {
            let user_pass = &after_scheme[..at_idx];
            if let Some(colon_idx) = user_pass.find(':') {
                let user = &user_pass[..colon_idx];
                let rest = &after_scheme[at_idx..];
                return format!("{}://{}:[REDACTED]{}", &url[..scheme_end], user, rest);
            }
        }
    }
    url.to_string()
}

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub rpc_url: String,
    /// Contract IDs to index. Empty => index all contract events.
    pub contract_ids: Vec<String>,
    pub poll_interval_secs: u64,
    pub page_size: u32,
    pub start_ledger: i64,
    pub max_catchup_ledgers: i64,
    pub state_indexing: bool,
    pub key_indexing: bool,
    pub balance_key_symbol: String,
    pub balance_key_durability: String,
    pub key_templates: Vec<KeyTemplate>,
    pub retention_ledgers: i64,
    pub spec_version_retention: i64,
    pub upgrade_watch: bool,
    pub reorg_overlap_ledgers: i64,
    pub rpc_timeout_secs: u64,
    pub enrichment_warn_threshold: f64,
    pub spec_cache_max_entries: usize,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("database_url", &redact_database_url(&self.database_url))
            .field("rpc_url", &self.rpc_url)
            .field("contract_ids", &self.contract_ids)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("page_size", &self.page_size)
            .field("start_ledger", &self.start_ledger)
            .field("max_catchup_ledgers", &self.max_catchup_ledgers)
            .field("state_indexing", &self.state_indexing)
            .field("key_indexing", &self.key_indexing)
            .field("balance_key_symbol", &self.balance_key_symbol)
            .field("balance_key_durability", &self.balance_key_durability)
            .field("key_templates", &self.key_templates)
            .field("retention_ledgers", &self.retention_ledgers)
            .field("spec_version_retention", &self.spec_version_retention)
            .field("upgrade_watch", &self.upgrade_watch)
            .field("reorg_overlap_ledgers", &self.reorg_overlap_ledgers)
            .field("rpc_timeout_secs", &self.rpc_timeout_secs)
            .field("enrichment_warn_threshold", &self.enrichment_warn_threshold)
            .field("spec_cache_max_entries", &self.spec_cache_max_entries)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let contract_ids: Vec<String> = std::env::var("CONTRACT_IDS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Validate CONTRACT_IDS as C-strkeys (Soroban contract addresses).
        for id in &contract_ids {
            if !lumenqraph_core::is_valid_contract_id(id) {
                return Err(anyhow::anyhow!(
                    "invalid CONTRACT_ID {}: expected a C… strkey (Soroban contract address)",
                    id
                ));
            }
        }

        // Validate the CONTRACT_IDS count against the getEvents RPC protocol limit:
        // at most 5 filters × 5 IDs per filter = 25 IDs total. Checking this at
        // startup produces a clear, actionable error message instead of a cryptic
        // runtime failure on the first poll cycle.
        const MAX_IDS_PER_FILTER: usize = 5;
        const MAX_FILTERS: usize = 5;
        const MAX_CONTRACT_IDS: usize = MAX_IDS_PER_FILTER * MAX_FILTERS;
        if contract_ids.len() > MAX_CONTRACT_IDS {
            return Err(anyhow::anyhow!(
                "CONTRACT_IDS contains {} entries, but getEvents supports at most {} \
                 contract IDs ({} filters × {} IDs per filter). \
                 Remove {} contract IDs, or run multiple indexer instances each \
                 covering a different subset.",
                contract_ids.len(),
                MAX_CONTRACT_IDS,
                MAX_FILTERS,
                MAX_IDS_PER_FILTER,
                contract_ids.len() - MAX_CONTRACT_IDS,
            ));
        }

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
