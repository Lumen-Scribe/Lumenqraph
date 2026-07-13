//! Indexer configuration, loaded from environment (see `.env.example`).

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub rpc_url: String,
    /// Contract IDs to index. Empty => index all contract events.
    pub contract_ids: Vec<String>,
    pub poll_interval_secs: u64,
    pub page_size: u32,
    /// Ledger to start from on a fresh index. 0 => start near the current tip.
    pub start_ledger: i64,
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
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: env("DATABASE_URL")?,
            rpc_url: env("RPC_URL")?,
            contract_ids: std::env::var("CONTRACT_IDS")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            poll_interval_secs: env_parse("POLL_INTERVAL_SECS", 5)?,
            page_size: env_parse("PAGE_SIZE", 1000)?,
            start_ledger: env_parse("START_LEDGER", 0)?,
            state_indexing: env_bool("STATE_INDEXING", false),
            key_indexing: env_bool("KEY_INDEXING", false),
            balance_key_symbol: std::env::var("BALANCE_KEY_SYMBOL")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "Balance".to_string()),
            balance_key_durability: std::env::var("BALANCE_KEY_DURABILITY")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "persistent".to_string()),
        })
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
