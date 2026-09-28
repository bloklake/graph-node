//! Historical triggers from a datalake (the `bl serve` HTTP API: `POST /query` answering with
//! Arrow IPC streams, `GET /status`) instead of `eth_getLogs` + `eth_getBlockByHash`.
//!
//! One `/query` returns every matching log of a block range together with its transaction
//! (`joinTransactions`) and its block header, so a range of tens of thousands of blocks costs
//! one round trip instead of one `eth_getLogs` per filter plus one block fetch per block.
//!
//! The datalake is only used for ranges it has fully ingested, and only for filters it can
//! answer exactly: log triggers (receipts, when a handler asks for them, still come from
//! JSON-RPC). Call handlers and block handlers keep using JSON-RPC. Blocks built here carry
//! only the transactions of matched logs; they are never written to the chain store's block
//! cache.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, ListArray, StructArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use graph::blockchain::BlockPtr;
use graph::components::ethereum::{AnyBlock, AnyTransaction, LightEthereumBlock};
use graph::prelude::alloy::consensus::{
    Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702, TxEnvelope, TxLegacy,
    transaction::Recovered,
};
use graph::prelude::alloy::eips::eip2930::{AccessList, AccessListItem};
use graph::prelude::alloy::network::{AnyHeader, AnyTxEnvelope};
use graph::prelude::alloy::primitives::{
    Address, B64, B256, Bloom, Bytes, LogData, Signature, TxKind, U256,
};
use graph::prelude::alloy::rpc::types::{Block, BlockTransactions, Header, Log};
use graph::prelude::{BlockNumber, Logger, anyhow, debug, reqwest, serde_json, warn};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::adapter::{EthGetLogsFilter, EthereumLogFilter, TriggerFilter};

/// `[chains.<name>.datalake]` in the node config.
#[derive(Clone, Debug, PartialEq, Deserialize, serde::Serialize)]
pub struct DatalakeSettings {
    /// Base URL of the datalake, e.g. `http://127.0.0.1:8090`.
    pub url: String,
    /// Sent as `X-BL-Api-Key` when set.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Largest block range scanned in one step while the datalake serves the range.
    #[serde(default = "default_max_block_range")]
    pub max_block_range: BlockNumber,
    /// Ideal number of triggers per step while the datalake serves the range.
    #[serde(default = "default_target_triggers")]
    pub target_triggers: u64,
    /// `maxObjects` per request; the datalake pages larger answers at block boundaries.
    #[serde(default = "default_max_objects")]
    pub max_objects: u64,
    /// Per-request timeout, in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Treat blocks the datalake reports as finalized (`finalized_block`) as final, so they
    /// are indexed from the datalake even within the reorg threshold of the chain head.
    #[serde(default = "default_true")]
    pub trust_finalized: bool,
}

fn default_true() -> bool {
    true
}

fn default_max_block_range() -> BlockNumber {
    100_000
}
fn default_target_triggers() -> u64 {
    50_000
}
fn default_max_objects() -> u64 {
    200_000
}
fn default_timeout_secs() -> u64 {
    60
}

/// Filter objects per request and values per field the datalake accepts.
const MAX_FILTER_OBJECTS: usize = 256;
const MAX_VALUES_PER_FIELD: usize = 100_000;
/// How long a fetched archive height is trusted.
const HEIGHT_TTL: Duration = Duration::from_millis(500);
/// Attempts per request before a range falls back to JSON-RPC.
const ATTEMPTS: u32 = 4;

pub struct DatalakeClient {
    settings: DatalakeSettings,
    http: reqwest::Client,
    height: Mutex<Option<(Instant, Heights)>>,
    /// Scans started ahead of time, by filter fingerprint and first block, with the last block
    /// they were asked for.
    prefetched: Mutex<HashMap<(u64, BlockNumber), Prefetched>>,
    /// The last scans' logs, to answer re-scans of single blocks (for data sources created in
    /// the block) without a request.
    recent: std::sync::Mutex<std::collections::VecDeque<RecentScan>>,
}

struct RecentScan {
    from: BlockNumber,
    to: BlockNumber,
    filter: EthereumLogFilter,
    logs: HashMap<u64, Vec<Arc<Log>>>,
}

/// Ranges fetched ahead of the stream.
const PREFETCH_DEPTH: BlockNumber = 5;

type Prefetched = (BlockNumber, JoinHandle<anyhow::Result<Scanned>>);

/// Scans remembered for `recent_logs`.
const RECENT_SCANS: usize = 8;

impl std::fmt::Debug for DatalakeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatalakeClient")
            .field("url", &self.settings.url)
            .finish()
    }
}

#[derive(Deserialize)]
struct Status {
    archive_height: Option<u64>,
    finalized_block: Option<u64>,
}

#[derive(Clone, Copy)]
struct Heights {
    archive: Option<BlockNumber>,
    finalized: Option<BlockNumber>,
}

/// The streams of one `/query` answer.
#[derive(Default)]
struct Answer {
    transactions: Vec<RecordBatch>,
    logs: Vec<RecordBatch>,
    blocks: Vec<RecordBatch>,
    /// First block the answer does not cover.
    next_block: BlockNumber,
}

impl DatalakeClient {
    pub fn new(settings: DatalakeSettings) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(settings.timeout_secs))
            .pool_max_idle_per_host(16)
            .build()?;
        Ok(Self {
            settings,
            http,
            height: Mutex::new(None),
            prefetched: Mutex::new(HashMap::new()),
            recent: std::sync::Mutex::new(std::collections::VecDeque::new()),
        })
    }

    pub fn settings(&self) -> &DatalakeSettings {
        &self.settings
    }

    fn request(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.settings.api_key {
            Some(key) => builder.header("X-BL-Api-Key", key),
            None => builder,
        }
    }

    async fn heights(&self) -> anyhow::Result<Heights> {
        let mut cached = self.height.lock().await;
        if let Some((at, heights)) = *cached
            && at.elapsed() < HEIGHT_TTL
        {
            return Ok(heights);
        }
        let url = format!("{}/status", self.settings.url.trim_end_matches('/'));
        let status: Status = self
            .request(self.http.get(url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let number =
            |h: Option<u64>| h.map(|h| BlockNumber::try_from(h).unwrap_or(BlockNumber::MAX));
        let heights = Heights {
            archive: number(status.archive_height),
            finalized: number(status.finalized_block),
        };
        *cached = Some((Instant::now(), heights));
        Ok(heights)
    }

    /// The highest block the datalake answers queries for (cached briefly).
    pub async fn archive_height(&self) -> anyhow::Result<Option<BlockNumber>> {
        Ok(self.heights().await?.archive)
    }

    /// The highest block the datalake reports as finalized, when `trust_finalized` is set.
    pub async fn finalized_height(&self) -> anyhow::Result<Option<BlockNumber>> {
        if !self.settings.trust_finalized {
            return Ok(None);
        }
        let heights = self.heights().await?;
        Ok(match (heights.finalized, heights.archive) {
            (Some(f), Some(a)) => Some(f.min(a)),
            _ => None,
        })
    }

    async fn query_once(&self, body: &serde_json::Value) -> anyhow::Result<Answer> {
        let url = format!("{}/query", self.settings.url.trim_end_matches('/'));
        let resp = self.request(self.http.post(url)).json(body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("datalake answered {status}: {text}"));
        }
        let header = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let streams = header("x-bl-streams").unwrap_or_default();
        let next_block: BlockNumber = header("x-bl-next-block")
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| anyhow!("datalake answer without X-BL-Next-Block"))?;
        let body = resp.bytes().await?;
        let mut streams = decode_streams(body, &streams)?;
        let mut take = |name: &str| streams.remove(name).unwrap_or_default();
        Ok(Answer {
            transactions: take("transactions"),
            logs: take("logs"),
            blocks: take("blocks"),
            next_block,
        })
    }

    async fn query(&self, logger: &Logger, body: &serde_json::Value) -> anyhow::Result<Answer> {
        let mut delay = Duration::from_millis(100);
        let mut attempt = 1;
        loop {
            match self.query_once(body).await {
                Ok(answer) => return Ok(answer),
                Err(e) if attempt < ATTEMPTS => {
                    warn!(logger, "Datalake query failed, retrying";
                        "attempt" => attempt, "error" => format!("{e:#}"));
                    tokio::time::sleep(delay).await;
                    delay *= 4;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Splits a body of back-to-back Arrow IPC streams (named by `X-BL-Streams`, in order).
fn decode_streams(
    body: graph::bytes::Bytes,
    names: &str,
) -> anyhow::Result<HashMap<String, Vec<RecordBatch>>> {
    let mut buffer = Buffer::from(body);
    let mut out = HashMap::new();
    for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let mut decoder = StreamDecoder::new();
        let mut batches = Vec::new();
        loop {
            if buffer.is_empty() {
                decoder
                    .finish()
                    .map_err(|e| anyhow!("datalake stream `{name}` is truncated: {e}"))?;
                break;
            }
            match decoder.decode(&mut buffer) {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => {}
                // The decoder refuses to read past an end-of-stream marker: the next stream
                // starts here.
                Err(_) if decoder.finish().is_ok() => break,
                Err(e) => return Err(anyhow!("datalake stream `{name}`: {e}")),
            }
        }
        out.insert(name.to_string(), batches);
    }
    Ok(out)
}

// --- Arrow columns ---------------------------------------------------------------------------

struct Cols<'a>(&'a RecordBatch);

impl<'a> Cols<'a> {
    fn get<T: 'static>(&self, name: &str) -> anyhow::Result<&'a T> {
        self.0
            .column_by_name(name)
            .ok_or_else(|| anyhow!("datalake column `{name}` missing"))?
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| anyhow!("datalake column `{name}` has an unexpected type"))
    }
    fn opt<T: 'static>(&self, name: &str) -> Option<&'a T> {
        self.0
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<T>())
    }
    fn u64(&self, name: &str) -> anyhow::Result<&'a UInt64Array> {
        self.get(name)
    }
    fn fixed(&self, name: &str) -> anyhow::Result<&'a FixedSizeBinaryArray> {
        self.get(name)
    }
    fn binary(&self, name: &str) -> anyhow::Result<&'a BinaryArray> {
        self.get(name)
    }
}

fn b256(a: &FixedSizeBinaryArray, i: usize) -> B256 {
    B256::from_slice(a.value(i))
}
fn opt_b256(a: Option<&FixedSizeBinaryArray>, i: usize) -> Option<B256> {
    a.filter(|a| !a.is_null(i)).map(|a| b256(a, i))
}
fn address(a: &FixedSizeBinaryArray, i: usize) -> Address {
    Address::from_slice(a.value(i))
}
fn opt_address(a: Option<&FixedSizeBinaryArray>, i: usize) -> Option<Address> {
    a.filter(|a| !a.is_null(i)).map(|a| address(a, i))
}
/// A `u128` stored as 16 big-endian bytes.
fn opt_u128(a: Option<&FixedSizeBinaryArray>, i: usize) -> Option<u128> {
    a.filter(|a| !a.is_null(i))
        .map(|a| u128::from_be_bytes(a.value(i).try_into().expect("16-byte u128")))
}
fn opt_u64(a: Option<&UInt64Array>, i: usize) -> Option<u64> {
    a.filter(|a| !a.is_null(i)).map(|a| a.value(i))
}
fn b256_list(a: Option<&ListArray>, i: usize) -> Vec<B256> {
    let Some(a) = a.filter(|a| !a.is_null(i)) else {
        return vec![];
    };
    let values = a.value(i);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("list of 32-byte hashes");
    (0..values.len()).map(|j| b256(values, j)).collect()
}
fn access_list(a: Option<&ListArray>, i: usize) -> AccessList {
    let Some(a) = a.filter(|a| !a.is_null(i)) else {
        return AccessList::default();
    };
    let entries = a.value(i);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("access list entries");
    let addresses = entries
        .column_by_name("address")
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .expect("access list address");
    let keys = entries
        .column_by_name("storage_keys")
        .and_then(|c| c.as_any().downcast_ref::<ListArray>());
    AccessList(
        (0..entries.len())
            .map(|j| AccessListItem {
                address: address(addresses, j),
                storage_keys: b256_list(keys, j),
            })
            .collect(),
    )
}

// --- Rows -> alloy types ------------------------------------------------------------------------

/// Block fields requested for every block that has a matched log.
const BLOCK_FIELDS: &[&str] = &[
    "number",
    "hash",
    "parentHash",
    "timestamp",
    "size",
    "gasUsed",
    "gasLimit",
    "baseFeePerGas",
    "stateRoot",
    "transactionsRoot",
    "receiptsRoot",
    "logsBloom",
    "difficulty",
    "totalDifficulty",
    "miner",
    "nonce",
    "sha3Uncles",
    "uncles",
    "mixHash",
    "extraData",
    "withdrawalsRoot",
    "blobGasUsed",
    "excessBlobGas",
    "parentBeaconBlockRoot",
    "requestsHash",
];

const TRANSACTION_FIELDS: &[&str] = &[
    "blockNumber",
    "transactionIndex",
    "hash",
    "from",
    "to",
    "value",
    "input",
    "nonce",
    "gas",
    "gasPrice",
    "maxFeePerGas",
    "maxPriorityFeePerGas",
    "type",
    "accessList",
    "chainId",
    "v",
    "r",
    "s",
    "yParity",
    "maxFeePerBlobGas",
    "blobVersionedHashes",
];

const LOG_FIELDS: &[&str] = &[
    "blockNumber",
    "logIndex",
    "transactionHash",
    "transactionIndex",
    "address",
    "topics",
    "data",
];

fn field_map(fields: &[&str]) -> serde_json::Value {
    serde_json::Value::Object(
        fields
            .iter()
            .map(|f| (f.to_string(), serde_json::Value::Bool(true)))
            .collect(),
    )
}

fn headers(
    batches: &[RecordBatch],
) -> anyhow::Result<BTreeMap<u64, (Header<AnyHeader>, Vec<B256>)>> {
    let mut out = BTreeMap::new();
    for batch in batches {
        let c = Cols(batch);
        let number = c.u64("number")?;
        let hash = c.fixed("hash")?;
        let parent = c.fixed("parent_hash")?;
        let timestamp = c.u64("timestamp")?;
        let size = c.opt::<UInt64Array>("size");
        let gas_used = c.u64("gas_used")?;
        let gas_limit = c.u64("gas_limit")?;
        let base_fee = c.opt::<FixedSizeBinaryArray>("base_fee_per_gas");
        let state_root = c.fixed("state_root")?;
        let tx_root = c.fixed("transactions_root")?;
        let receipts_root = c.fixed("receipts_root")?;
        let bloom = c.fixed("logs_bloom")?;
        let difficulty = c.opt::<FixedSizeBinaryArray>("difficulty");
        let total_difficulty = c.opt::<FixedSizeBinaryArray>("total_difficulty");
        let miner = c.fixed("miner")?;
        let nonce = c.opt::<FixedSizeBinaryArray>("nonce");
        let uncles_hash = c.fixed("sha3_uncles")?;
        let uncles = c.opt::<ListArray>("uncles");
        let mix_hash = c.opt::<FixedSizeBinaryArray>("mix_hash");
        let extra = c.binary("extra_data")?;
        let withdrawals_root = c.opt::<FixedSizeBinaryArray>("withdrawals_root");
        let blob_gas_used = c.opt::<UInt64Array>("blob_gas_used");
        let excess_blob_gas = c.opt::<UInt64Array>("excess_blob_gas");
        let beacon_root = c.opt::<FixedSizeBinaryArray>("parent_beacon_block_root");
        let requests_hash = c.opt::<FixedSizeBinaryArray>("requests_hash");
        for i in 0..batch.num_rows() {
            let inner = AnyHeader {
                parent_hash: b256(parent, i),
                ommers_hash: b256(uncles_hash, i),
                beneficiary: address(miner, i),
                state_root: b256(state_root, i),
                transactions_root: b256(tx_root, i),
                receipts_root: b256(receipts_root, i),
                logs_bloom: Bloom::from_slice(bloom.value(i)),
                difficulty: U256::from(opt_u128(difficulty, i).unwrap_or(0)),
                number: number.value(i),
                gas_limit: gas_limit.value(i),
                gas_used: gas_used.value(i),
                timestamp: timestamp.value(i),
                extra_data: Bytes::copy_from_slice(extra.value(i)),
                mix_hash: opt_b256(mix_hash, i),
                nonce: nonce
                    .filter(|a| !a.is_null(i))
                    .map(|a| B64::from_slice(a.value(i))),
                base_fee_per_gas: opt_u128(base_fee, i).map(|v| v as u64),
                withdrawals_root: opt_b256(withdrawals_root, i),
                blob_gas_used: opt_u64(blob_gas_used, i),
                excess_blob_gas: opt_u64(excess_blob_gas, i),
                parent_beacon_block_root: opt_b256(beacon_root, i),
                requests_hash: opt_b256(requests_hash, i),
                ..Default::default()
            };
            let header = Header {
                hash: b256(hash, i),
                inner,
                total_difficulty: opt_u128(total_difficulty, i).map(U256::from),
                size: opt_u64(size, i).map(U256::from),
            };
            out.insert(number.value(i), (header, b256_list(uncles, i)));
        }
    }
    Ok(out)
}

/// The signature's parity bit: `yParity` when present, else derived from a legacy `v`.
fn parity(y_parity: Option<u8>, v: u64) -> bool {
    match y_parity {
        Some(p) => p == 1,
        None if v >= 35 => (v - 35) % 2 == 1,
        None => v == 28,
    }
}

fn transactions(
    batches: &[RecordBatch],
    block_hashes: &BTreeMap<u64, B256>,
) -> anyhow::Result<HashMap<u64, Vec<AnyTransaction>>> {
    let mut out: HashMap<u64, Vec<AnyTransaction>> = HashMap::new();
    for batch in batches {
        let c = Cols(batch);
        let block = c.u64("block_number")?;
        let index = c.get::<UInt32Array>("transaction_index")?;
        let hash = c.fixed("hash")?;
        let from = c.fixed("from")?;
        let to = c.opt::<FixedSizeBinaryArray>("to");
        let value = c.opt::<FixedSizeBinaryArray>("value");
        let input = c.binary("input")?;
        let nonce = c.u64("nonce")?;
        let gas = c.u64("gas")?;
        let gas_price = c.opt::<FixedSizeBinaryArray>("gas_price");
        let max_fee = c.opt::<FixedSizeBinaryArray>("max_fee_per_gas");
        let max_priority = c.opt::<FixedSizeBinaryArray>("max_priority_fee_per_gas");
        let ty = c.get::<UInt8Array>("type")?;
        let access = c.opt::<ListArray>("access_list");
        let chain_id = c.opt::<UInt64Array>("chain_id");
        let v = c.u64("v")?;
        let r = c.fixed("r")?;
        let s = c.fixed("s")?;
        let y_parity = c.opt::<UInt8Array>("y_parity");
        let max_fee_blob = c.opt::<FixedSizeBinaryArray>("max_fee_per_blob_gas");
        let blob_hashes = c.opt::<ListArray>("blob_versioned_hashes");
        for i in 0..batch.num_rows() {
            let number = block.value(i);
            let block_hash = *block_hashes.get(&number).ok_or_else(|| {
                anyhow!("datalake transaction in block {number} without its block")
            })?;
            let tx_hash = b256(hash, i);
            let to = opt_address(to, i);
            let kind = to.map_or(TxKind::Create, TxKind::Call);
            let value = U256::from(opt_u128(value, i).unwrap_or(0));
            let input = Bytes::copy_from_slice(input.value(i));
            let nonce = nonce.value(i);
            let gas_limit = gas.value(i);
            let chain_id = opt_u64(chain_id, i);
            let y = y_parity.filter(|a| !a.is_null(i)).map(|a| a.value(i));
            let signature = Signature::new(
                U256::from_be_slice(r.value(i)),
                U256::from_be_slice(s.value(i)),
                parity(y, v.value(i)),
            );
            let max_fee_per_gas = opt_u128(max_fee, i).unwrap_or(0);
            let max_priority_fee_per_gas = opt_u128(max_priority, i).unwrap_or(0);
            let envelope = match ty.value(i) {
                0 => TxEnvelope::Legacy(Signed::new_unchecked(
                    TxLegacy {
                        chain_id: chain_id.or_else(|| {
                            let v = v.value(i);
                            (v >= 35).then(|| (v - 35) / 2)
                        }),
                        nonce,
                        gas_price: opt_u128(gas_price, i).unwrap_or(0),
                        gas_limit,
                        to: kind,
                        value,
                        input,
                    },
                    signature,
                    tx_hash,
                )),
                1 => TxEnvelope::Eip2930(Signed::new_unchecked(
                    TxEip2930 {
                        chain_id: chain_id.unwrap_or(0),
                        nonce,
                        gas_price: opt_u128(gas_price, i).unwrap_or(0),
                        gas_limit,
                        to: kind,
                        value,
                        access_list: access_list(access, i),
                        input,
                    },
                    signature,
                    tx_hash,
                )),
                2 => TxEnvelope::Eip1559(Signed::new_unchecked(
                    TxEip1559 {
                        chain_id: chain_id.unwrap_or(0),
                        nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                        to: kind,
                        value,
                        access_list: access_list(access, i),
                        input,
                    },
                    signature,
                    tx_hash,
                )),
                3 => TxEnvelope::Eip4844(Signed::new_unchecked(
                    TxEip4844Variant::TxEip4844(TxEip4844 {
                        chain_id: chain_id.unwrap_or(0),
                        nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                        to: to.unwrap_or_default(),
                        value,
                        access_list: access_list(access, i),
                        blob_versioned_hashes: b256_list(blob_hashes, i),
                        max_fee_per_blob_gas: opt_u128(max_fee_blob, i).unwrap_or(0),
                        input,
                    }),
                    signature,
                    tx_hash,
                )),
                4 => TxEnvelope::Eip7702(Signed::new_unchecked(
                    TxEip7702 {
                        chain_id: chain_id.unwrap_or(0),
                        nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                        to: to.unwrap_or_default(),
                        value,
                        access_list: access_list(access, i),
                        authorization_list: vec![],
                        input,
                    },
                    signature,
                    tx_hash,
                )),
                other => {
                    return Err(anyhow!(
                        "transaction {tx_hash} has type {other}, which the datalake path does not build"
                    ));
                }
            };
            let tx = AnyTransaction {
                inner: Recovered::new_unchecked(
                    AnyTxEnvelope::Ethereum(envelope),
                    address(from, i),
                ),
                block_hash: Some(block_hash),
                block_number: Some(number),
                transaction_index: Some(u64::from(index.value(i))),
                effective_gas_price: None,
                block_timestamp: None,
            };
            out.entry(number).or_default().push(tx);
        }
    }
    Ok(out)
}

fn logs(batches: &[RecordBatch], block_hashes: &BTreeMap<u64, B256>) -> anyhow::Result<Vec<Log>> {
    let mut out = Vec::new();
    for batch in batches {
        let c = Cols(batch);
        let block = c.u64("block_number")?;
        let log_index = c.get::<UInt32Array>("log_index")?;
        let tx_hash = c.fixed("transaction_hash")?;
        let tx_index = c.get::<UInt32Array>("transaction_index")?;
        let address_col = c.fixed("address")?;
        let topics = [
            c.opt::<FixedSizeBinaryArray>("topic0"),
            c.opt::<FixedSizeBinaryArray>("topic1"),
            c.opt::<FixedSizeBinaryArray>("topic2"),
            c.opt::<FixedSizeBinaryArray>("topic3"),
        ];
        let data = c.binary("data")?;
        for i in 0..batch.num_rows() {
            let number = block.value(i);
            let block_hash = *block_hashes
                .get(&number)
                .ok_or_else(|| anyhow!("datalake log in block {number} without its block"))?;
            let topics: Vec<B256> = topics.iter().map_while(|t| opt_b256(*t, i)).collect();
            out.push(Log {
                inner: graph::prelude::alloy::primitives::Log {
                    address: address(address_col, i),
                    data: LogData::new_unchecked(topics, Bytes::copy_from_slice(data.value(i))),
                },
                block_hash: Some(block_hash),
                block_number: Some(number),
                block_timestamp: None,
                transaction_hash: Some(b256(tx_hash, i)),
                transaction_index: Some(u64::from(tx_index.value(i))),
                log_index: Some(u64::from(log_index.value(i))),
                removed: false,
            });
        }
    }
    Ok(out)
}

fn block(
    header: Header<AnyHeader>,
    uncles: Vec<B256>,
    transactions: Vec<AnyTransaction>,
) -> LightEthereumBlock {
    let block: AnyBlock = Block {
        header,
        uncles,
        transactions: BlockTransactions::Full(transactions),
        withdrawals: None,
    };
    LightEthereumBlock::new(block)
}

/// Identifies a set of filter objects, to match prefetched answers to later scans.
fn fingerprint(filters: &[serde_json::Value]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serde_json::Value::Array(filters.to_vec())
        .to_string()
        .hash(&mut hasher);
    hasher.finish()
}

/// Blocks and logs of one scanned range.
pub struct Scanned {
    /// Every block with a matched log, and the last block of the range (possibly without logs).
    pub blocks: Vec<(Arc<LightEthereumBlock>, Vec<Arc<Log>>)>,
    /// Last block the scan covers.
    pub to: BlockNumber,
}

/// The `/query` filter objects for a log filter, or `None` when the datalake cannot express it.
fn log_filters(filter: &TriggerFilter) -> Option<Vec<serde_json::Value>> {
    let hex = |v: &[u8]| format!("0x{}", hex::encode(v));
    let mut objects = Vec::new();
    for EthGetLogsFilter {
        contracts,
        event_signatures,
        topic1,
        topic2,
        topic3,
    } in filter
        .log
        .clone()
        .eth_get_logs_filters(MAX_VALUES_PER_FIELD)
    {
        let too_many = |len: usize| len > MAX_VALUES_PER_FIELD;
        if too_many(contracts.len())
            || too_many(event_signatures.len())
            || [&topic1, &topic2, &topic3]
                .iter()
                .any(|t| t.as_ref().is_some_and(|t| too_many(t.len())))
        {
            return None;
        }
        let mut obj = serde_json::Map::new();
        if !contracts.is_empty() {
            let values: Vec<String> = contracts.iter().map(|a| hex(a.as_slice())).collect();
            obj.insert("address".into(), values.into());
        }
        let mut put = |key: &str, values: &[B256]| {
            if !values.is_empty() {
                let values: Vec<String> = values.iter().map(|t| hex(t.as_slice())).collect();
                obj.insert(key.into(), values.into());
            }
        };
        put("topic0", &event_signatures);
        put("topic1", topic1.as_deref().unwrap_or_default());
        put("topic2", topic2.as_deref().unwrap_or_default());
        put("topic3", topic3.as_deref().unwrap_or_default());
        objects.push(serde_json::Value::Object(obj));
    }
    (objects.len() <= MAX_FILTER_OBJECTS).then_some(objects)
}

/// True when the datalake can answer every trigger this filter asks for.
pub fn supports(filter: &TriggerFilter) -> bool {
    filter.call.is_empty()
        && !filter.block.trigger_every_block
        && filter.block.polling_intervals.is_empty()
        && filter.block.contract_addresses.is_empty()
}

impl DatalakeClient {
    /// Scans `[from, to]` for the logs of `filter`. Returns `Ok(None)` when the datalake cannot
    /// serve the range (not ingested yet, or a filter it cannot express); the caller then uses
    /// JSON-RPC. The scan may stop early (the answer was paged); `Scanned::to` says where.
    ///
    /// Each scan starts fetching the following range of the same size in the background, so
    /// the next call usually finds its answer ready while the current one is processed.
    pub async fn scan(
        self: &Arc<Self>,
        logger: &Logger,
        from: BlockNumber,
        to: BlockNumber,
        filter: &TriggerFilter,
    ) -> anyhow::Result<Option<Scanned>> {
        if !supports(filter) {
            return Ok(None);
        }
        let Some(height) = self.archive_height().await? else {
            return Ok(None);
        };
        if from > height {
            return Ok(None);
        }
        let to = to.min(height);
        let Some(filters) = log_filters(filter) else {
            return Ok(None);
        };
        let key = fingerprint(&filters);
        let filters = Arc::new(filters);

        let prefetched = self.prefetched.lock().await.remove(&(key, from));
        let waited = std::time::Instant::now();
        let scanned = match prefetched {
            Some((_, handle)) => match handle.await.inspect(|_| {
                let ms = waited.elapsed().as_millis();
                if ms > 0 {
                    debug!(logger, "Waited for a prefetched datalake range"; "from" => from, "ms" => ms);
                }
            }) {
                Ok(Ok(scanned)) => Some(scanned),
                Ok(Err(e)) => {
                    debug!(logger, "Prefetched datalake range failed, fetching again";
                        "from" => from, "error" => format!("{e:#}"));
                    None
                }
                Err(_) => None,
            },
            None => None,
        };
        let scanned = match scanned {
            Some(scanned) if scanned.to <= to => scanned,
            Some(scanned) => {
                // Keep the part beyond `to` as the next range's answer.
                let (head, rest) = self.split(logger, scanned, to).await?;
                if let Some(rest) = rest {
                    let end = rest.to;
                    let handle = tokio::spawn(async move { Ok(rest) });
                    self.prefetched
                        .lock()
                        .await
                        .insert((key, to + 1), (end, handle));
                }
                head
            }
            None => {
                debug!(logger, "Datalake range not prefetched"; "from" => from, "to" => to);
                // Start on the next ranges now rather than after this one (at the stream's
                // start, say): sized like this one until there is a density to go by.
                if to < height {
                    let mut prefetched = self.prefetched.lock().await;
                    self.plan_prefetch(
                        logger,
                        &mut prefetched,
                        key,
                        &filters,
                        to + 1,
                        to - from + 1,
                        height,
                    );
                }
                self.fetch(logger, from, to, &filters).await?
            }
        };

        let next = scanned.to + 1;
        if next <= height {
            // Predict the stream's next range the way it sizes ranges: at most tenfold the last
            // one, and about `target_triggers` at the density just seen.
            let scanned_size = scanned.to - from + 1;
            let logs: usize = scanned.blocks.iter().map(|(_, logs)| logs.len()).sum();
            let by_density = if logs == 0 {
                BlockNumber::MAX
            } else {
                let per_block = logs as f64 / scanned_size as f64;
                (self.settings.target_triggers as f64 / per_block).max(1.0) as BlockNumber
            };
            let size = scanned_size
                .saturating_mul(10)
                .min(self.settings.max_block_range)
                .min(by_density);
            let mut prefetched = self.prefetched.lock().await;
            // A filter change or restarted stream leaves stale entries behind; they are few
            // and dropped here.
            prefetched.retain(|(k, start), _| *k == key && *start >= next);
            self.plan_prefetch(logger, &mut prefetched, key, &filters, next, size, height);
        }
        self.remember(from, &scanned, filter);
        Ok(Some(scanned))
    }

    /// Keeps `PREFETCH_DEPTH` ranges from `next` on in flight: fetching and decoding a dense
    /// range can take as long as indexing one. New ranges of `size` blocks continue from the
    /// ones already in flight, so they do not overlap them.
    #[allow(clippy::too_many_arguments)]
    fn plan_prefetch(
        self: &Arc<Self>,
        logger: &Logger,
        prefetched: &mut HashMap<(u64, BlockNumber), Prefetched>,
        key: u64,
        filters: &Arc<Vec<serde_json::Value>>,
        next: BlockNumber,
        size: BlockNumber,
        height: BlockNumber,
    ) {
        let mut start = next;
        for _ in 0..PREFETCH_DEPTH {
            if start > height {
                break;
            }
            if let Some((end, _)) = prefetched.get(&(key, start)) {
                start = end + 1;
                continue;
            }
            let client = self.clone();
            let logger = logger.clone();
            let filters = filters.clone();
            let end = (start + size.max(1) - 1).min(height);
            let handle =
                tokio::spawn(async move { client.fetch(&logger, start, end, &filters).await });
            prefetched.insert((key, start), (end, handle));
            start = end + 1;
        }
    }

    fn remember(&self, from: BlockNumber, scanned: &Scanned, filter: &TriggerFilter) {
        let logs = scanned
            .blocks
            .iter()
            .filter(|(_, logs)| !logs.is_empty())
            .map(|(block, logs)| (block.number_u64(), logs.clone()))
            .collect();
        let mut recent = self.recent.lock().unwrap();
        recent.push_back(RecentScan {
            from,
            to: scanned.to,
            filter: filter.log.clone(),
            logs,
        });
        while recent.len() > RECENT_SCANS {
            recent.pop_front();
        }
    }

    /// The logs `filter` matches in block `number`, from a recent scan whose filter included
    /// everything `filter` asks for; `None` when no such scan covered the block.
    pub fn recent_logs(
        &self,
        number: BlockNumber,
        filter: &EthereumLogFilter,
    ) -> Option<Vec<Arc<Log>>> {
        let recent = self.recent.lock().unwrap();
        let scan = recent
            .iter()
            .rev()
            .find(|scan| scan.from <= number && number <= scan.to)?;
        if !scan.filter.covers(filter) {
            return None;
        }
        let number = u64::try_from(number).ok()?;
        Some(
            scan.logs
                .get(&number)
                .map(|logs| {
                    logs.iter()
                        .filter(|log| filter.matches(log))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
        )
    }

    /// Splits a prefetched scan at `to`: the part up to `to` (which must then end with block
    /// `to`) and the rest, if any.
    async fn split(
        &self,
        logger: &Logger,
        mut scanned: Scanned,
        to: BlockNumber,
    ) -> anyhow::Result<(Scanned, Option<Scanned>)> {
        let last = u64::try_from(to).expect("block numbers are positive");
        let at = scanned
            .blocks
            .partition_point(|(block, _)| block.number_u64() <= last);
        let rest_blocks = scanned.blocks.split_off(at);
        let rest = (!rest_blocks.is_empty()).then(|| Scanned {
            blocks: rest_blocks,
            to: scanned.to,
        });
        if scanned.blocks.last().map(|(b, _)| b.number_u64()) != Some(last) {
            let mut headers = self.header(logger, to).await?;
            let (header, uncles) = headers
                .remove(&last)
                .ok_or_else(|| anyhow!("datalake has no block {to}"))?;
            scanned
                .blocks
                .push((Arc::new(block(header, uncles, vec![])), vec![]));
        }
        scanned.to = to;
        Ok((scanned, rest))
    }

    async fn header(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> anyhow::Result<BTreeMap<u64, (Header<AnyHeader>, Vec<B256>)>> {
        let body = serde_json::json!({
            "fromBlock": number,
            "toBlock": number,
            "fields": { "block": field_map(BLOCK_FIELDS) },
        });
        headers(&self.query(logger, &body).await?.blocks)
    }

    /// One scan of `[from, to]` (possibly ending early when the datalake pages the answer).
    async fn fetch(
        &self,
        logger: &Logger,
        from: BlockNumber,
        to: BlockNumber,
        filters: &[serde_json::Value],
    ) -> anyhow::Result<Scanned> {
        let start = std::time::Instant::now();
        let mut blocks = BTreeMap::new();
        let mut txs = HashMap::new();
        let mut matched = Vec::new();
        let mut covered = to;
        if !filters.is_empty() {
            let body = serde_json::json!({
                "fromBlock": from,
                "toBlock": to,
                "maxObjects": self.settings.max_objects,
                "joinTransactions": true,
                "logs": filters,
                "fields": {
                    "block": field_map(BLOCK_FIELDS),
                    "transaction": field_map(TRANSACTION_FIELDS),
                    "log": field_map(LOG_FIELDS),
                },
            });
            let answer = self.query(logger, &body).await?;
            covered = (answer.next_block - 1).min(to);
            if covered < from {
                return Err(anyhow!(
                    "datalake answer for [{from}, {to}] does not advance (next block {})",
                    answer.next_block
                ));
            }
            blocks = headers(&answer.blocks)?;
            let hashes: BTreeMap<u64, B256> =
                blocks.iter().map(|(n, (h, _))| (*n, h.hash)).collect();
            txs = transactions(&answer.transactions, &hashes)?;
            matched = logs(&answer.logs, &hashes)?;
        }

        // The stream advances to the last block of the range, so it must be in the answer even
        // when nothing matched there.
        let last = u64::try_from(covered).expect("block numbers are positive");
        if !blocks.contains_key(&last) {
            blocks.extend(self.header(logger, covered).await?);
            if !blocks.contains_key(&last) {
                return Err(anyhow!("datalake has no block {covered}"));
            }
        }

        let mut logs_by_block: HashMap<u64, Vec<Arc<Log>>> = HashMap::new();
        for log in matched {
            logs_by_block
                .entry(log.block_number.unwrap_or_default())
                .or_default()
                .push(Arc::new(log));
        }
        let blocks = blocks
            .into_iter()
            .map(|(number, (header, uncles))| {
                let mut transactions = txs.remove(&number).unwrap_or_default();
                transactions.sort_by_key(|tx| tx.transaction_index);
                let logs = logs_by_block.remove(&number).unwrap_or_default();
                (Arc::new(block(header, uncles, transactions)), logs)
            })
            .collect();
        debug!(logger, "Scanned blocks with the datalake";
            "from" => from, "to" => covered, "ms" => start.elapsed().as_millis());
        Ok(Scanned {
            blocks,
            to: covered,
        })
    }

    /// The canonical block pointer at `number`, if the datalake has it.
    pub async fn block_ptr(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> anyhow::Result<Option<BlockPtr>> {
        match self.archive_height().await? {
            Some(height) if number <= height => {}
            _ => return Ok(None),
        }
        let body = serde_json::json!({
            "fromBlock": number,
            "toBlock": number,
            "fields": { "block": { "number": true, "hash": true } },
        });
        let answer = self.query(logger, &body).await?;
        let Some(batch) = answer.blocks.iter().find(|b| b.num_rows() > 0) else {
            return Ok(None);
        };
        let c = Cols(batch);
        let hash = b256(c.fixed("hash")?, 0);
        let number = c.u64("number")?.value(0);
        Ok(Some(BlockPtr::from((hash, number))))
    }
}

/// Transaction hashes of logs that need a receipt, by block hash.
pub fn receipts_needed(filter: &TriggerFilter, logs: &[&Arc<Log>]) -> HashMap<B256, HashSet<B256>> {
    let mut out: HashMap<B256, HashSet<B256>> = HashMap::new();
    for log in logs {
        let Some(signature) = log.topics().first() else {
            continue;
        };
        if filter
            .log
            .requires_transaction_receipt(signature, Some(&log.address()), log.topics())
            && let (Some(block), Some(tx)) = (log.block_hash, log.transaction_hash)
        {
            out.entry(block).or_default().insert(tx);
        }
    }
    out
}
