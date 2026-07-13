//! Thin JSON-RPC client for the Soroban RPC `getEvents` / `getLatestLedger`
//! methods. Only the fields we use are modeled.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use stellar_xdr::curr::{
    ContractDataDurability, ContractExecutable, LedgerEntryData, LedgerKey, LedgerKeyContractCode,
    LedgerKeyContractData, Limits, ReadXdr, ScAddress, ScVal, WriteXdr,
};

pub struct RpcClient {
    http: reqwest::Client,
    url: String,
}

#[derive(Serialize)]
struct RpcRequest<'a, P> {
    jsonrpc: &'a str,
    id: u32,
    method: &'a str,
    params: P,
}

#[derive(Deserialize)]
struct RpcResponse<R> {
    result: Option<R>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

// ---- getLatestLedger ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestLedgerResult {
    sequence: i64,
}

// ---- getEvents ----

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetEventsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_ledger: Option<i64>,
    filters: Vec<EventFilter>,
    pagination: Pagination,
    xdr_format: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventFilter {
    #[serde(rename = "type")]
    filter_type: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    contract_ids: Vec<String>,
}

#[derive(Serialize)]
struct Pagination {
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetEventsResult {
    /// Present in the RPC response; the poller reads the tip from
    /// getLatestLedger instead, so this is retained only for completeness.
    #[allow(dead_code)]
    pub latest_ledger: i64,
    pub events: Vec<EventInfo>,
    pub cursor: Option<String>,
}

/// One event as returned by RPC. `topic` and `value` are base64 XDR.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventInfo {
    #[serde(rename = "type")]
    pub event_type: String,
    pub ledger: i64,
    pub ledger_closed_at: String,
    pub contract_id: String,
    pub id: String,
    /// Newer RPC responses omit `pagingToken` (the unique `id` serves the same
    /// role), so treat it as optional.
    #[serde(default)]
    pub paging_token: String,
    #[serde(default)]
    pub in_successful_contract_call: bool,
    #[serde(default)]
    pub tx_hash: String,
    #[serde(default)]
    pub topic: Vec<String>,
    #[serde(default)]
    pub value: String,
}

impl RpcClient {
    pub fn new(url: impl Into<String>) -> Self {
        // A request timeout is essential for a 24/7 poller: without it, a hung
        // RPC connection blocks the poll loop indefinitely and the backoff path
        // (which only fires on an error) is never reached.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            http,
            url: url.into(),
        }
    }

    async fn call<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> anyhow::Result<R> {
        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let resp: RpcResponse<R> = self
            .http
            .post(&self.url)
            .json(&req)
            .send()
            .await
            .with_context(|| format!("rpc {method} request failed"))?
            .error_for_status()
            .with_context(|| format!("rpc {method} returned http error"))?
            .json()
            .await
            .with_context(|| format!("rpc {method} response decode failed"))?;

        if let Some(err) = resp.error {
            return Err(anyhow!("rpc {method} error {}: {}", err.code, err.message));
        }
        resp.result
            .ok_or_else(|| anyhow!("rpc {method} returned no result"))
    }

    /// Current tip ledger sequence.
    pub async fn get_latest_ledger(&self) -> anyhow::Result<i64> {
        let r: LatestLedgerResult = self.call("getLatestLedger", serde_json::json!({})).await?;
        Ok(r.sequence)
    }

    /// Fetch a page of events. Pass `start_ledger` on the first page of a scan,
    /// or `cursor` to continue a previous page (never both).
    pub async fn get_events(
        &self,
        start_ledger: Option<i64>,
        contract_ids: &[String],
        cursor: Option<String>,
        limit: u32,
    ) -> anyhow::Result<GetEventsResult> {
        let params = GetEventsParams {
            start_ledger: if cursor.is_some() { None } else { start_ledger },
            filters: vec![EventFilter {
                filter_type: "contract",
                contract_ids: contract_ids.to_vec(),
            }],
            pagination: Pagination { limit, cursor },
            xdr_format: "base64",
        };
        self.call("getEvents", params).await
    }

    /// Fetch a contract's deployed WASM (hex hash + bytes), so its on-chain
    /// interface spec can be parsed. Returns `Ok(None)` for contracts with no
    /// WASM (e.g. a Stellar Asset Contract) or when the entries aren't found.
    ///
    /// Two hops, both via `getLedgerEntries`: the contract's *instance* entry
    /// names the WASM hash of its executable; the *code* entry holds the bytes.
    pub async fn get_contract_wasm(
        &self,
        contract_id: &str,
    ) -> anyhow::Result<Option<(String, Vec<u8>)>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;

        let instance_key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: ScVal::LedgerKeyContractInstance,
            durability: ContractDataDurability::Persistent,
        });
        let Some((entry, _)) = self.get_ledger_entry(&instance_key).await? else {
            return Ok(None);
        };
        let wasm_hash = match entry {
            LedgerEntryData::ContractData(cd) => match cd.val {
                ScVal::ContractInstance(inst) => match inst.executable {
                    ContractExecutable::Wasm(hash) => hash,
                    ContractExecutable::StellarAsset => return Ok(None),
                },
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        let hash_hex = hex::encode(wasm_hash.0);
        let code_key = LedgerKey::ContractCode(LedgerKeyContractCode { hash: wasm_hash });
        let Some((entry, _)) = self.get_ledger_entry(&code_key).await? else {
            return Ok(None);
        };
        match entry {
            LedgerEntryData::ContractCode(cc) => Ok(Some((hash_hex, cc.code.into()))),
            _ => Ok(None),
        }
    }

    /// Fetch a contract's *instance* ledger entry: its current executable hash
    /// (`None` for a Stellar Asset Contract), its instance storage as a single
    /// `ScVal::Map`, and the ledger at which the instance last changed. Used for
    /// state snapshots and upgrade detection. `Ok(None)` if the contract's
    /// instance entry isn't found.
    pub async fn get_contract_instance(
        &self,
        contract_id: &str,
    ) -> anyhow::Result<Option<InstanceEntry>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;
        let key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: ScVal::LedgerKeyContractInstance,
            durability: ContractDataDurability::Persistent,
        });
        let Some((data, last_modified_ledger)) = self.get_ledger_entry(&key).await? else {
            return Ok(None);
        };
        let LedgerEntryData::ContractData(cd) = data else {
            return Ok(None);
        };
        let ScVal::ContractInstance(inst) = cd.val else {
            return Ok(None);
        };
        let wasm_hash = match inst.executable {
            ContractExecutable::Wasm(h) => Some(hex::encode(h.0)),
            ContractExecutable::StellarAsset => None,
        };
        Ok(Some(InstanceEntry {
            wasm_hash,
            // The instance storage map (may be empty/None) as one decodable ScVal.
            storage: ScVal::Map(inst.storage),
            last_modified_ledger,
        }))
    }

    /// Fetch a single contract-data entry by its exact storage `key` and
    /// `durability`. Unlike instance storage, these per-key entries aren't
    /// enumerable, so the caller must know the key (e.g. a `Balance(Address)`).
    /// Returns the decoded value `ScVal` and the ledger it last changed at, or
    /// `Ok(None)` if no such entry exists (e.g. a holder with a zero balance
    /// whose entry was never written or has expired).
    pub async fn get_contract_data(
        &self,
        contract_id: &str,
        key: &ScVal,
        durability: ContractDataDurability,
    ) -> anyhow::Result<Option<DataEntry>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;
        let ledger_key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: key.clone(),
            durability,
        });
        let Some((data, last_modified_ledger)) = self.get_ledger_entry(&ledger_key).await? else {
            return Ok(None);
        };
        let LedgerEntryData::ContractData(cd) = data else {
            return Ok(None);
        };
        Ok(Some(DataEntry {
            val: cd.val,
            last_modified_ledger,
        }))
    }

    /// Fetch and XDR-decode a single ledger entry by key, with the ledger it was
    /// last modified at. `None` if absent.
    async fn get_ledger_entry(
        &self,
        key: &LedgerKey,
    ) -> anyhow::Result<Option<(LedgerEntryData, i64)>> {
        let key_b64 = key
            .to_xdr_base64(Limits::none())
            .context("encode ledger key")?;
        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": [key_b64] }))
            .await?;
        let Some(first) = result.entries.unwrap_or_default().into_iter().next() else {
            return Ok(None);
        };
        let data = LedgerEntryData::from_xdr_base64(&first.xdr, Limits::none())
            .context("decode ledger entry")?;
        Ok(Some((data, first.last_modified_ledger_seq)))
    }
}

/// A contract's instance entry: executable hash, instance storage, and the
/// ledger it last changed at.
pub struct InstanceEntry {
    pub wasm_hash: Option<String>,
    pub storage: ScVal,
    pub last_modified_ledger: i64,
}

/// A single contract-data entry: its decoded value and the ledger it last
/// changed at.
pub struct DataEntry {
    pub val: ScVal,
    pub last_modified_ledger: i64,
}

#[derive(Deserialize)]
struct LedgerEntriesResult {
    #[serde(default)]
    entries: Option<Vec<LedgerEntryItem>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LedgerEntryItem {
    xdr: String,
    #[serde(default)]
    last_modified_ledger_seq: i64,
}
