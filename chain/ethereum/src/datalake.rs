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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, ListArray, StructArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use graph::blockchain::BlockPtr;
use graph::components::ethereum::{AnyBlock, AnyTransaction, LenientHeader, LightEthereumBlock};
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
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
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
    /// Requests in flight at once, shared by every chain using the same API key (the server
    /// rejects more with `429 too_many_in_flight`). Lowered automatically when the server
    /// reports a lower limit.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// The `event.transaction` and `event.block` fields the mappings on this chain read, by
    /// their graph-ts names. Only those are fetched; a field that is not listed reads as zero or
    /// empty in the mappings. Absent: every field is fetched. An empty `transaction` list fetches
    /// no transactions at all (each log's transaction then has only its hash and index).
    #[serde(default)]
    pub fields: Option<DatalakeFields>,
    /// Bytes held by ranges fetched ahead of the stream, at most (default 256 MiB): their
    /// decoded blocks, transactions and logs (estimated), or the answers' size on the wire when
    /// larger. Ranges ahead are sized from the bytes per block seen so far, so a dense filter
    /// is read ahead in shorter ranges instead of holding gigabytes.
    #[serde(default = "default_max_prefetch_bytes")]
    pub max_prefetch_bytes: u64,
}

fn default_max_prefetch_bytes() -> u64 {
    256 * 1024 * 1024
}

/// `fields = { transaction = [...], block = [...] }` of `[chains.<name>.datalake]`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, serde::Serialize)]
pub struct DatalakeFields {
    /// graph-ts `ethereum.Transaction` fields; absent: all of them.
    #[serde(default)]
    pub transaction: Option<Vec<String>>,
    /// graph-ts `ethereum.Block` fields; absent: all of them.
    #[serde(default)]
    pub block: Option<Vec<String>>,
}

/// graph-ts `ethereum.Transaction` fields and the datalake fields that fill them. `hash` and
/// `index` are always fetched (graph-node needs them to match logs to transactions).
const TX_FIELD_MAP: &[(&str, &[&str])] = &[
    ("hash", &[]),
    ("index", &[]),
    ("from", &["from"]),
    ("to", &["to"]),
    ("value", &["value"]),
    ("gasLimit", &["gas"]),
    // The effective gas price: the price fields, the type, and the block's base fee.
    (
        "gasPrice",
        &["gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "type"],
    ),
    ("input", &["input"]),
    ("nonce", &["nonce"]),
];
const TX_REQUIRED: &[&str] = &["blockNumber", "transactionIndex", "hash"];

/// graph-ts `ethereum.Block` fields and the datalake fields that fill them. `hash`,
/// `parentHash`, `number` and `timestamp` are always fetched (block pointers and ordering).
const BLOCK_FIELD_MAP: &[(&str, &[&str])] = &[
    ("hash", &[]),
    ("parentHash", &[]),
    ("number", &[]),
    ("timestamp", &[]),
    ("unclesHash", &["sha3Uncles"]),
    ("author", &["miner"]),
    ("stateRoot", &["stateRoot"]),
    ("transactionsRoot", &["transactionsRoot"]),
    ("receiptsRoot", &["receiptsRoot"]),
    ("gasUsed", &["gasUsed"]),
    ("gasLimit", &["gasLimit"]),
    ("difficulty", &["difficulty"]),
    ("totalDifficulty", &["totalDifficulty"]),
    ("size", &["size"]),
    ("baseFeePerGas", &["baseFeePerGas"]),
];
const BLOCK_REQUIRED: &[&str] = &["number", "hash", "parentHash", "timestamp"];

/// The fields requested from the datalake, resolved once from `DatalakeSettings::fields`.
#[derive(Clone, Debug, PartialEq)]
struct Selection {
    block: serde_json::Value,
    /// `None`: no transactions are joined (none of their fields were asked for).
    transaction: Option<serde_json::Value>,
}

/// The datalake fields for the graph-ts names in `wanted`, or an error naming the valid ones.
fn mapped<'a>(
    kind: &str,
    map: &[(&str, &'a [&'a str])],
    wanted: &[String],
) -> anyhow::Result<Vec<&'a str>> {
    let mut out = Vec::new();
    for name in wanted {
        let Some((_, fields)) = map.iter().find(|(n, _)| n == name) else {
            let valid: Vec<&str> = map.iter().map(|(n, _)| *n).collect();
            return Err(anyhow!(
                "datalake fields: unknown {kind} field `{name}`; valid: {}",
                valid.join(", ")
            ));
        };
        out.extend(fields.iter().copied());
    }
    Ok(out)
}

fn select(fields: Option<&DatalakeFields>) -> anyhow::Result<Selection> {
    let fields = fields.cloned().unwrap_or_default();
    let mut block: Vec<&str> = match &fields.block {
        None => BLOCK_FIELDS.to_vec(),
        Some(list) => BLOCK_REQUIRED
            .iter()
            .copied()
            .chain(mapped("block", BLOCK_FIELD_MAP, list)?)
            .collect(),
    };
    let transaction = match &fields.transaction {
        None => Some(TRANSACTION_FIELDS.to_vec()),
        Some(list) => {
            let extra = mapped("transaction", TX_FIELD_MAP, list)?;
            if extra.is_empty() && !list.iter().any(|n| n == "hash" || n == "index") {
                None
            } else {
                Some(TX_REQUIRED.iter().copied().chain(extra).collect::<Vec<_>>())
            }
        }
    };
    if transaction
        .as_ref()
        .is_some_and(|t| t.contains(&"maxFeePerGas"))
        && !block.contains(&"baseFeePerGas")
    {
        block.push("baseFeePerGas");
    }
    Ok(Selection {
        block: field_map(&block),
        transaction: transaction.map(|t| field_map(&t)),
    })
}

fn default_max_concurrent_requests() -> usize {
    DEFAULT_MAX_CONCURRENT_REQUESTS
}

/// The server's default per-key in-flight limit.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 4;
/// How long a request keeps waiting out `429`s (throttling, not failures) before the range
/// falls back to JSON-RPC.
const THROTTLE_BUDGET: Duration = Duration::from_secs(300);

/// Requests in flight for one API key (one URL without a key). The server counts a key's
/// requests across all of its chains, so every client using the key shares the permits.
///
/// Prefetches (ranges ahead of the block stream) also take one of `prefetch` permits, one fewer
/// than the limit: a request the stream is waiting on always finds a permit free or next in
/// line, instead of queueing behind ranges it needs later.
struct Limiter {
    permits: Arc<Semaphore>,
    prefetch: Arc<Semaphore>,
    max: AtomicUsize,
}

tokio::task_local! {
    /// Set in prefetch tasks: their requests take a prefetch permit too.
    static IS_PREFETCH: ();
}

/// Permits held for one request.
struct Permit {
    _request: OwnedSemaphorePermit,
    _prefetch: Option<OwnedSemaphorePermit>,
}

/// Prefetch permits for a request limit: one fewer, at least one.
fn prefetch_permits(max: usize) -> usize {
    max.saturating_sub(1).max(1)
}

impl Limiter {
    fn for_settings(settings: &DatalakeSettings) -> Arc<Limiter> {
        static LIMITERS: LazyLock<std::sync::Mutex<HashMap<String, Arc<Limiter>>>> =
            LazyLock::new(Default::default);
        let key = match &settings.api_key {
            Some(k) => format!("key:{k}"),
            None => format!("url:{}", settings.url.trim_end_matches('/')),
        };
        let max = settings.max_concurrent_requests.max(1);
        let mut limiters = LIMITERS.lock().unwrap();
        let limiter = limiters
            .entry(key)
            .or_insert_with(|| {
                Arc::new(Limiter {
                    permits: Arc::new(Semaphore::new(max)),
                    prefetch: Arc::new(Semaphore::new(prefetch_permits(max))),
                    max: AtomicUsize::new(max),
                })
            })
            .clone();
        limiter.lower_to(max);
        limiter
    }

    /// A permit for one request; in a prefetch task, a prefetch permit first.
    async fn acquire(&self) -> Permit {
        let prefetch = if IS_PREFETCH.try_with(|_| ()).is_ok() {
            Some(
                self.prefetch
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("the datalake limiter is never closed"),
            )
        } else {
            None
        };
        let request = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("the datalake limiter is never closed");
        Permit {
            _request: request,
            _prefetch: prefetch,
        }
    }

    /// Lowers the limit; permits in use are taken out of circulation as they come back.
    fn lower_to(&self, limit: usize) {
        let limit = limit.max(1);
        let old = self.max.fetch_min(limit, Ordering::Relaxed);
        if limit < old {
            Self::take(&self.permits, old - limit);
            Self::take(
                &self.prefetch,
                prefetch_permits(old) - prefetch_permits(limit),
            );
        }
    }

    /// Takes `n` permits out of circulation (once they are free).
    fn take(permits: &Arc<Semaphore>, n: usize) {
        if n == 0 {
            return;
        }
        let permits = permits.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    if let Ok(p) = permits.acquire_many_owned(n as u32).await {
                        p.forget();
                    }
                });
            }
            Err(_) => {
                let forgotten = permits.forget_permits(n);
                debug_assert!(forgotten <= n);
            }
        }
    }
}

/// A `429` from the datalake: wait `retry_after`, then try again (not counted as a failure).
#[derive(Debug)]
struct Throttled {
    retry_after: Duration,
    text: String,
}

impl std::fmt::Display for Throttled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "datalake answered 429 Too Many Requests: {}", self.text)
    }
}

impl std::error::Error for Throttled {}

/// The limit in a `too_many_in_flight` message (`4 requests of this key are still in flight`).
fn in_flight_limit(text: &str) -> Option<usize> {
    let body: serde_json::Value = serde_json::from_str(text).ok()?;
    let error = body.get("error")?;
    if error.get("code")?.as_str()? != "too_many_in_flight" {
        return None;
    }
    let message = error.get("message")?.as_str()?;
    message
        .split_once(" requests of this key")?
        .0
        .trim()
        .rsplit(' ')
        .next()?
        .parse()
        .ok()
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
    limiter: Arc<Limiter>,
    height: Mutex<Option<(Instant, Heights)>>,
    /// Scans started ahead of time, by filter fingerprint and first block, with the last block
    /// they were asked for.
    prefetched: Mutex<HashMap<(u64, BlockNumber), Prefetched>>,
    /// The last scans' logs, to answer re-scans of single blocks (for data sources created in
    /// the block) without a request.
    recent: std::sync::Mutex<std::collections::VecDeque<RecentScan>>,
    /// Whether the chain's missing fields (`bl.unavailable_fields`) were logged already.
    warned_unavailable: std::sync::atomic::AtomicBool,
    /// The fields requested (`DatalakeSettings::fields`).
    selection: Selection,
}

struct RecentScan {
    from: BlockNumber,
    to: BlockNumber,
    filter: EthereumLogFilter,
    logs: HashMap<u64, Vec<Arc<Log>>>,
}

/// Ranges fetched ahead of the block stream (`GRAPH_DATALAKE_PREFETCH_DEPTH`, default 5).
static PREFETCH_DEPTH: LazyLock<BlockNumber> = LazyLock::new(|| {
    std::env::var("GRAPH_DATALAKE_PREFETCH_DEPTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &BlockNumber| *v >= 0)
        .unwrap_or(5)
});

/// The first block at or after `start` that none of `ranges` (inclusive `(first, last)`)
/// covers: where a new range can begin without fetching a block already in flight.
fn first_uncovered(
    ranges: impl Iterator<Item = (BlockNumber, BlockNumber)> + Clone,
    start: BlockNumber,
) -> BlockNumber {
    let mut at = start;
    // Each pass moves past one range that holds `at`; ranges are few (`PREFETCH_DEPTH`).
    while let Some((_, last)) = ranges
        .clone()
        .find(|(first, last)| *first <= at && at <= *last)
    {
        at = last + 1;
    }
    at
}

/// `to`, cut short before the first range in `ranges` that starts inside `(from, to]`, so a
/// fetch of `[from, result]` never fetches a block that is already in flight.
fn up_to_in_flight(
    ranges: impl Iterator<Item = (BlockNumber, BlockNumber)>,
    from: BlockNumber,
    to: BlockNumber,
) -> BlockNumber {
    ranges
        .filter(|(first, _)| *first > from && *first <= to)
        .map(|(first, _)| first - 1)
        .min()
        .unwrap_or(to)
}

/// A page answer stops here at the latest: a range whose logs keep coming (a filter that
/// matches most of the chain) is returned in parts rather than held in memory whole.
const MAX_LOGS_PER_FETCH_FACTOR: u64 = 2;

/// A range fetched ahead of the stream: its last block, and the fetch.
type Prefetched = (BlockNumber, JoinHandle<anyhow::Result<Scanned>>);

/// Scans kept for `recent_logs` (`GRAPH_DATALAKE_RECENT_SCANS`, default 8).
static RECENT_SCANS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("GRAPH_DATALAKE_RECENT_SCANS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
});

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
    /// `bl.unavailable_fields`: selected fields the chain doesn't have (their columns are null).
    unavailable_fields: Vec<String>,
    /// Size of the answer's body.
    bytes: usize,
}

impl DatalakeClient {
    pub fn new(settings: DatalakeSettings) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(settings.timeout_secs))
            .pool_max_idle_per_host(16)
            .build()?;
        let selection = select(settings.fields.as_ref())?;
        Ok(Self {
            selection,
            limiter: Limiter::for_settings(&settings),
            settings,
            http,
            height: Mutex::new(None),
            prefetched: Mutex::new(HashMap::new()),
            recent: std::sync::Mutex::new(std::collections::VecDeque::new()),
            warned_unavailable: std::sync::atomic::AtomicBool::new(false),
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
        let status: Status = {
            let _permit = self.limiter.acquire().await;
            self.request(self.http.get(url))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?
        };
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
        // The permit is held until the whole body is read.
        let _permit = self.limiter.acquire().await;
        let resp = self.request(self.http.post(url)).json(body).send().await?;
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(1));
            let text = resp.text().await.unwrap_or_default();
            if let Some(limit) = in_flight_limit(&text) {
                self.limiter.lower_to(limit);
            }
            return Err(Throttled { retry_after, text }.into());
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("datalake answered {status}: {text}"));
        }
        let body = resp.bytes().await?;
        let mut answer = decode_answer(&body)?;
        answer.bytes = body.len();
        Ok(answer)
    }

    async fn query(&self, logger: &Logger, body: &serde_json::Value) -> anyhow::Result<Answer> {
        let mut delay = Duration::from_millis(100);
        let mut attempt = 1;
        let started = Instant::now();
        loop {
            match self.query_once(body).await {
                Ok(answer) => {
                    if !answer.unavailable_fields.is_empty()
                        && !self
                            .warned_unavailable
                            .swap(true, std::sync::atomic::Ordering::Relaxed)
                    {
                        warn!(logger, "Datalake chain lacks selected fields; they read as null";
                            "fields" => answer.unavailable_fields.join(","));
                    }
                    return Ok(answer);
                }
                // Throttling is not a failure: wait as told, without using up an attempt.
                Err(e)
                    if e.downcast_ref::<Throttled>().is_some()
                        && started.elapsed() < THROTTLE_BUDGET =>
                {
                    let wait = e.downcast_ref::<Throttled>().unwrap().retry_after;
                    debug!(logger, "Datalake throttled the query, waiting";
                        "wait_ms" => wait.as_millis() as u64, "error" => format!("{e:#}"));
                    tokio::time::sleep(wait.max(Duration::from_millis(100))).await;
                }
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

/// Splits a body of back-to-back Arrow IPC streams at their end-of-stream markers. A reader
/// takes a clean end of input for the end of a stream, so a body cut between two messages would
/// otherwise read as complete.
fn split_streams(body: &[u8]) -> anyhow::Result<Vec<&[u8]>> {
    let word = |at: usize| {
        body.get(at..at + 4)
            .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
    };
    let mut out = Vec::new();
    let (mut start, mut offset) = (0, 0);
    while let Some(mut length) = word(offset) {
        // A message is `0xFFFFFFFF`, metadata length, metadata, body; pre-1.0 writers omit the
        // continuation marker. A zero length is the end-of-stream marker.
        let mut prefix = 4;
        if length == -1 {
            prefix = 8;
            length = word(offset + 4).ok_or_else(|| anyhow!("datalake answer is truncated"))?;
        }
        if length == 0 {
            offset += prefix;
            out.push(&body[start..offset]);
            start = offset;
            continue;
        }
        let metadata = body
            .get(offset + prefix..offset + prefix + length as usize)
            .ok_or_else(|| anyhow!("datalake answer is truncated"))?;
        let message = arrow::ipc::root_as_message(metadata)
            .map_err(|e| anyhow!("datalake answer has an invalid Arrow message: {e}"))?;
        offset += prefix + length as usize + message.bodyLength() as usize;
    }
    if start != body.len() {
        return Err(anyhow!(
            "datalake answer is truncated: Arrow stream without end-of-stream marker"
        ));
    }
    Ok(out)
}

/// Decodes a `/query` answer. Every stream's schema metadata repeats the answer's `bl.*` keys:
/// `bl.streams` names every stream in order, so a body that ends cleanly after fewer streams is
/// truncated; `bl.next_block` is the first block not covered.
fn decode_answer(body: &[u8]) -> anyhow::Result<Answer> {
    let streams = split_streams(body)?;
    if streams.is_empty() {
        return Err(anyhow!("datalake answer is truncated: no Arrow stream"));
    }
    let mut answer = Answer::default();
    let mut names: Vec<String> = Vec::new();
    for (i, stream) in streams.iter().enumerate() {
        let reader = StreamReader::try_new(std::io::Cursor::new(*stream), None)
            .map_err(|e| anyhow!("datalake stream {i}: {e}"))?;
        if i == 0 {
            let schema = reader.schema();
            let meta = schema.metadata();
            let list = |key: &str| -> Vec<String> {
                meta.get(key)
                    .map(|s| {
                        s.split(',')
                            .filter(|n| !n.is_empty())
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default()
            };
            names = list("bl.streams");
            if names.is_empty() {
                return Err(anyhow!(
                    "datalake answer without bl.* stream metadata; the server is older than \
                     this client"
                ));
            }
            if streams.len() > names.len() {
                return Err(anyhow!(
                    "datalake answer has {} streams, but bl.streams lists {}",
                    streams.len(),
                    names.len()
                ));
            }
            answer.next_block = meta
                .get("bl.next_block")
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| anyhow!("datalake answer without bl.next_block"))?;
            answer.unavailable_fields = list("bl.unavailable_fields");
        }
        let name = &names[i];
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("datalake stream `{name}`: {e}"))?;
        match name.as_str() {
            "transactions" => answer.transactions = batches,
            "logs" => answer.logs = batches,
            "blocks" => answer.blocks = batches,
            other => return Err(anyhow!("unexpected datalake stream `{other}`")),
        }
    }
    if streams.len() != names.len() {
        return Err(anyhow!(
            "datalake answer is truncated: expected {} streams, got {}",
            names.len(),
            streams.len()
        ));
    }
    Ok(answer)
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
    "chainId",
];
// Not requested: the signature (`v`, `r`, `s`, `yParity`), `accessList`, `maxFeePerBlobGas` and
// `blobVersionedHashes`. Mappings never see them (`EthereumTransactionData` exposes hash, index,
// from, to, value, gas limit, effective gas price, input and nonce), the envelope is built with
// `new_unchecked` (no signature check, the hash is given), and datalake blocks are not written to
// the chain store. Leaving them out is ~10% of a backfill's bytes.

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
        let gas_used = c.opt::<UInt64Array>("gas_used");
        let gas_limit = c.opt::<UInt64Array>("gas_limit");
        let base_fee = c.opt::<FixedSizeBinaryArray>("base_fee_per_gas");
        let state_root = c.opt::<FixedSizeBinaryArray>("state_root");
        let tx_root = c.opt::<FixedSizeBinaryArray>("transactions_root");
        let receipts_root = c.opt::<FixedSizeBinaryArray>("receipts_root");
        let bloom = c.opt::<FixedSizeBinaryArray>("logs_bloom");
        let difficulty = c.opt::<FixedSizeBinaryArray>("difficulty");
        let total_difficulty = c.opt::<FixedSizeBinaryArray>("total_difficulty");
        let miner = c.opt::<FixedSizeBinaryArray>("miner");
        let nonce = c.opt::<FixedSizeBinaryArray>("nonce");
        let uncles_hash = c.opt::<FixedSizeBinaryArray>("sha3_uncles");
        let uncles = c.opt::<ListArray>("uncles");
        let mix_hash = c.opt::<FixedSizeBinaryArray>("mix_hash");
        let extra = c.opt::<BinaryArray>("extra_data");
        let withdrawals_root = c.opt::<FixedSizeBinaryArray>("withdrawals_root");
        let blob_gas_used = c.opt::<UInt64Array>("blob_gas_used");
        let excess_blob_gas = c.opt::<UInt64Array>("excess_blob_gas");
        let beacon_root = c.opt::<FixedSizeBinaryArray>("parent_beacon_block_root");
        let requests_hash = c.opt::<FixedSizeBinaryArray>("requests_hash");
        for i in 0..batch.num_rows() {
            let inner = AnyHeader {
                parent_hash: b256(parent, i),
                ommers_hash: opt_b256(uncles_hash, i).unwrap_or_default(),
                beneficiary: opt_address(miner, i).unwrap_or_default(),
                state_root: opt_b256(state_root, i).unwrap_or_default(),
                transactions_root: opt_b256(tx_root, i).unwrap_or_default(),
                receipts_root: opt_b256(receipts_root, i).unwrap_or_default(),
                logs_bloom: bloom
                    .filter(|a| !a.is_null(i))
                    .map_or_else(Bloom::default, |a| Bloom::from_slice(a.value(i))),
                difficulty: U256::from(opt_u128(difficulty, i).unwrap_or(0)),
                number: number.value(i),
                gas_limit: opt_u64(gas_limit, i).unwrap_or(0),
                gas_used: opt_u64(gas_used, i).unwrap_or(0),
                timestamp: timestamp.value(i),
                extra_data: extra
                    .filter(|a| !a.is_null(i))
                    .map_or_else(Bytes::new, |a| Bytes::copy_from_slice(a.value(i))),
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
        let from = c.opt::<FixedSizeBinaryArray>("from");
        let to = c.opt::<FixedSizeBinaryArray>("to");
        let value = c.opt::<FixedSizeBinaryArray>("value");
        let input = c.opt::<BinaryArray>("input");
        let nonce = c.opt::<UInt64Array>("nonce");
        let gas = c.opt::<UInt64Array>("gas");
        let gas_price = c.opt::<FixedSizeBinaryArray>("gas_price");
        let max_fee = c.opt::<FixedSizeBinaryArray>("max_fee_per_gas");
        let max_priority = c.opt::<FixedSizeBinaryArray>("max_priority_fee_per_gas");
        let ty = c.opt::<UInt8Array>("type");
        let access = c.opt::<ListArray>("access_list");
        let chain_id = c.opt::<UInt64Array>("chain_id");
        // Not requested (see TRANSACTION_FIELDS); read when present, else a placeholder.
        let v = c.opt::<UInt64Array>("v");
        let r = c.opt::<FixedSizeBinaryArray>("r");
        let s = c.opt::<FixedSizeBinaryArray>("s");
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
            let input = input
                .filter(|a| !a.is_null(i))
                .map_or_else(Bytes::new, |a| Bytes::copy_from_slice(a.value(i)));
            let nonce = opt_u64(nonce, i).unwrap_or(0);
            let gas_limit = opt_u64(gas, i).unwrap_or(0);
            let chain_id = opt_u64(chain_id, i);
            let y = y_parity.filter(|a| !a.is_null(i)).map(|a| a.value(i));
            let signature = Signature::new(
                r.filter(|a| !a.is_null(i))
                    .map_or(U256::ZERO, |a| U256::from_be_slice(a.value(i))),
                s.filter(|a| !a.is_null(i))
                    .map_or(U256::ZERO, |a| U256::from_be_slice(a.value(i))),
                parity(y, opt_u64(v, i).unwrap_or(0)),
            );
            let max_fee_per_gas = opt_u128(max_fee, i).unwrap_or(0);
            let max_priority_fee_per_gas = opt_u128(max_priority, i).unwrap_or(0);
            let envelope = match ty.filter(|a| !a.is_null(i)).map_or(0, |a| a.value(i)) {
                0 => TxEnvelope::Legacy(Signed::new_unchecked(
                    TxLegacy {
                        chain_id: chain_id.or_else(|| {
                            opt_u64(v, i).and_then(|v| (v >= 35).then(|| (v - 35) / 2))
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
                    opt_address(from, i).unwrap_or_default(),
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

/// One transaction per distinct transaction of `logs`, with only what the log says about it
/// (hash, index, block): used when no transaction fields are requested. Every other field is
/// zero or empty.
fn placeholder_transactions(logs: &[Log]) -> Vec<(u64, AnyTransaction)> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for log in logs {
        let (Some(hash), Some(number)) = (log.transaction_hash, log.block_number) else {
            continue;
        };
        if !seen.insert(hash) {
            continue;
        }
        let envelope = TxEnvelope::Legacy(Signed::new_unchecked(
            TxLegacy::default(),
            Signature::new(U256::ZERO, U256::ZERO, false),
            hash,
        ));
        out.push((
            number,
            AnyTransaction {
                inner: Recovered::new_unchecked(AnyTxEnvelope::Ethereum(envelope), Address::ZERO),
                block_hash: log.block_hash,
                block_number: Some(number),
                transaction_index: log.transaction_index,
                effective_gas_price: None,
                block_timestamp: None,
            },
        ));
    }
    out
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
        header: header.map(LenientHeader::from),
        uncles,
        transactions: BlockTransactions::Full(transactions),
        withdrawals: None,
    };
    LightEthereumBlock::new(block)
}

/// Estimated heap bytes a range holds once decoded: its blocks (header and the block that wraps
/// it), transactions and logs (with their data and topics).
fn held_bytes(blocks: usize, transactions: usize, logs: &[Log]) -> usize {
    const BLOCK: usize = std::mem::size_of::<LightEthereumBlock>() + 1_024;
    const TRANSACTION: usize = std::mem::size_of::<AnyTransaction>() + 64;
    let logs: usize = logs
        .iter()
        .map(|log| {
            std::mem::size_of::<Log>() + 32 + log.data().data.len() + 32 * log.topics().len()
        })
        .sum();
    blocks * BLOCK + transactions * TRANSACTION + logs
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
    /// Bytes the range holds: decoded, or on the wire if larger (for sizing ranges ahead).
    pub bytes: usize,
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
        // The last block the prefetched range was asked for: when its answer stops short of it,
        // the rest is fetched next, from where the answer ended.
        let planned_end = prefetched.as_ref().map(|(end, _)| *end);
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
                // Never fetch a block that a prefetch already has in flight: stop before it,
                // and the stream's next call picks that range up.
                let to = {
                    let prefetched = self.prefetched.lock().await;
                    up_to_in_flight(
                        prefetched
                            .iter()
                            .filter(|((k, _), _)| *k == key)
                            .map(|((_, start), (end, _))| (*start, *end)),
                        from,
                        to,
                    )
                };
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

        // A prefetched answer that stopped short of its range (paged): fetch the rest now, from
        // where it ended, so the stream's next call finds it rather than fetching it again.
        if let Some(end) = planned_end.filter(|end| scanned.to < *end) {
            let start = scanned.to + 1;
            let mut prefetched = self.prefetched.lock().await;
            let free = first_uncovered(
                prefetched
                    .iter()
                    .filter(|((k, _), _)| *k == key)
                    .map(|((_, s), (e, _))| (*s, *e)),
                start,
            );
            if free == start {
                let end = up_to_in_flight(
                    prefetched
                        .iter()
                        .filter(|((k, _), _)| *k == key)
                        .map(|((_, s), (e, _))| (*s, *e)),
                    start,
                    end,
                );
                let (client, logger2, filters2) = (self.clone(), logger.clone(), filters.clone());
                let handle =
                    tokio::spawn(
                        async move { client.fetch(&logger2, start, end, &filters2).await },
                    );
                prefetched.insert((key, start), (end, handle));
            }
        }

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
            // And at most an even share of `max_prefetch_bytes` at the bytes per block just seen.
            let by_bytes = if scanned.bytes == 0 {
                BlockNumber::MAX
            } else {
                let per_block = scanned.bytes as f64 / scanned_size as f64;
                let share =
                    self.settings.max_prefetch_bytes as f64 / (*PREFETCH_DEPTH).max(1) as f64;
                (share / per_block).max(1.0) as BlockNumber
            };
            let size = scanned_size
                .saturating_mul(10)
                .min(self.settings.max_block_range)
                .min(by_density)
                .min(by_bytes);
            let mut prefetched = self.prefetched.lock().await;
            // A filter change or restarted stream leaves stale entries behind; they are few
            // and dropped here.
            prefetched.retain(|(k, start), (_, handle)| {
                let keep = *k == key && *start >= next;
                if !keep {
                    // Its answer will never be read: stop the request rather than pay for it.
                    handle.abort();
                }
                keep
            });
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
        // Note: `PREFETCH_DEPTH` is the most ranges ahead of the stream, counting those already
        // in flight: adding that many on every scan would let them pile up whenever fetching
        // outpaces indexing (each scan consumes one), holding ever more blocks in memory.
        let ahead = prefetched
            .iter()
            .filter(|((k, s), _)| *k == key && *s >= next)
            .count() as BlockNumber;
        for _ in ahead..*PREFETCH_DEPTH {
            // Past every range of this filter already in flight, not only one that begins
            // exactly here: a range that ended early leaves the next start in the middle of
            // another, and fetching from there would read its blocks twice.
            start = first_uncovered(
                prefetched
                    .iter()
                    .filter(|((k, _), _)| *k == key)
                    .map(|((_, s), (e, _))| (*s, *e)),
                start,
            );
            if start > height {
                break;
            }
            let client = self.clone();
            let logger = logger.clone();
            let filters = filters.clone();
            // Up to the next range in flight at most.
            let end = up_to_in_flight(
                prefetched
                    .iter()
                    .filter(|((k, _), _)| *k == key)
                    .map(|((_, s), (e, _))| (*s, *e)),
                start,
                (start + size.max(1) - 1).min(height),
            );
            let handle = tokio::spawn(IS_PREFETCH.scope((), async move {
                client.fetch(&logger, start, end, &filters).await
            }));
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
        while recent.len() > *RECENT_SCANS {
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
        let span = (scanned.to
            - scanned
                .blocks
                .first()
                .map_or(to, |(b, _)| b.number_u64() as BlockNumber)
            + 1)
        .max(1);
        let rest_share = (scanned.to - to).max(0) as f64 / span as f64;
        let rest_bytes = (scanned.bytes as f64 * rest_share) as usize;
        let rest = (!rest_blocks.is_empty()).then(|| Scanned {
            blocks: rest_blocks,
            to: scanned.to,
            bytes: rest_bytes,
        });
        scanned.bytes -= rest_bytes.min(scanned.bytes);
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
            "fields": { "block": self.selection.block.clone() },
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
        let mut fetched_bytes = 0usize;
        if !filters.is_empty() {
            // Follow the pages to the end of the range: a range answered in part would leave the
            // stream's next start in the middle of a prefetched range, which then misses and is
            // fetched again. Stops early only past `MAX_LOGS_PER_FETCH_FACTOR` × `target_triggers`
            // logs (a filter that matches most of the chain), returning what it has.
            let max_logs = self
                .settings
                .target_triggers
                .saturating_mul(MAX_LOGS_PER_FETCH_FACTOR) as usize;
            let max_bytes =
                (self.settings.max_prefetch_bytes / (*PREFETCH_DEPTH).max(1) as u64) as usize;
            let mut cursor = from;
            loop {
                let mut body = serde_json::json!({
                    "fromBlock": cursor,
                    "toBlock": to,
                    "maxObjects": self.settings.max_objects,
                    "logs": filters,
                    "fields": {
                        "block": self.selection.block.clone(),
                        "log": field_map(LOG_FIELDS),
                    },
                });
                if let Some(transaction) = &self.selection.transaction {
                    body["joinTransactions"] = true.into();
                    body["fields"]["transaction"] = transaction.clone();
                }
                let answer = self.query(logger, &body).await?;
                let page_end = (answer.next_block - 1).min(to);
                if page_end < cursor {
                    return Err(anyhow!(
                        "datalake answer for [{cursor}, {to}] does not advance (next block {})",
                        answer.next_block
                    ));
                }
                let page_blocks = headers(&answer.blocks)?;
                let hashes: BTreeMap<u64, B256> =
                    page_blocks.iter().map(|(n, (h, _))| (*n, h.hash)).collect();
                for (n, list) in transactions(&answer.transactions, &hashes)? {
                    txs.entry(n).or_insert_with(Vec::new).extend(list);
                }
                let page_logs = logs(&answer.logs, &hashes)?;
                let mut page_txs = answer
                    .transactions
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>();
                if self.selection.transaction.is_none() {
                    // No transaction fields asked for: each log's transaction from the log alone.
                    for (n, tx) in placeholder_transactions(&page_logs) {
                        txs.entry(n).or_insert_with(Vec::new).push(tx);
                        page_txs += 1;
                    }
                }
                // Note: The budget is what the range holds once decoded, not its size on the
                // wire: with few fields selected the answers shrink several times, the decoded
                // blocks, transactions and logs do not.
                fetched_bytes +=
                    answer
                        .bytes
                        .max(held_bytes(page_blocks.len(), page_txs, &page_logs));
                matched.extend(page_logs);
                blocks.extend(page_blocks);
                covered = page_end;
                if page_end >= to || matched.len() >= max_logs || fetched_bytes >= max_bytes.max(1)
                {
                    break;
                }
                cursor = page_end + 1;
            }
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
            bytes: fetched_bytes,
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

#[cfg(test)]
mod tests {
    use graph::prelude::serde_json;

    #[test]
    fn field_selection() {
        use super::{DatalakeFields, select};
        let f = |tx: Option<Vec<&str>>, block: Option<Vec<&str>>| DatalakeFields {
            transaction: tx.map(|v| v.into_iter().map(String::from).collect()),
            block: block.map(|v| v.into_iter().map(String::from).collect()),
        };
        // No `fields`: everything, transactions joined.
        let all = select(None).unwrap();
        assert!(all.transaction.unwrap().get("input").is_some());
        assert!(all.block.get("logsBloom").is_some());
        // An empty transaction list: no transactions at all; only the required block fields.
        let none = select(Some(&f(Some(vec![]), Some(vec![])))).unwrap();
        assert!(none.transaction.is_none());
        let keys: Vec<&String> = none.block.as_object().unwrap().keys().collect();
        assert_eq!(keys.len(), 4, "{keys:?}");
        // `from` and `gasPrice`: their fields plus the required ones, and the base fee.
        let some = select(Some(&f(
            Some(vec!["from", "gasPrice"]),
            Some(vec!["timestamp"]),
        )))
        .unwrap();
        let tx = some.transaction.unwrap();
        for k in [
            "hash",
            "transactionIndex",
            "blockNumber",
            "from",
            "gasPrice",
            "maxFeePerGas",
            "type",
        ] {
            assert!(tx.get(k).is_some(), "{k}");
        }
        assert!(tx.get("input").is_none());
        assert!(some.block.get("baseFeePerGas").is_some());
        // Unknown names are rejected with the valid ones.
        let err = select(Some(&f(Some(vec!["gas"]), None)))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unknown transaction field `gas`") && err.contains("gasLimit"),
            "{err}"
        );
        // Settings parse from TOML-shaped JSON.
        let s: super::DatalakeSettings = serde_json::from_value(serde_json::json!({
            "url": "http://x", "fields": { "transaction": [], "block": ["timestamp"] }
        }))
        .unwrap();
        assert_eq!(s.fields.unwrap().transaction, Some(vec![]));
    }

    #[test]
    fn first_uncovered_skips_every_range_in_flight() {
        use super::first_uncovered;
        let ranges = vec![(100, 149), (150, 199), (230, 260)];
        // Exactly at a range start, and in the middle of one (a range that ended early).
        assert_eq!(first_uncovered(ranges.iter().copied(), 100), 200);
        assert_eq!(first_uncovered(ranges.iter().copied(), 120), 200);
        assert_eq!(first_uncovered(ranges.iter().copied(), 200), 200);
        assert_eq!(first_uncovered(ranges.iter().copied(), 240), 261);
        assert_eq!(first_uncovered(std::iter::empty(), 7), 7);
    }

    #[test]
    fn up_to_in_flight_stops_before_the_next_range() {
        use super::up_to_in_flight;
        let ranges = vec![(150, 199), (300, 399)];
        assert_eq!(up_to_in_flight(ranges.iter().copied(), 100, 500), 149);
        assert_eq!(up_to_in_flight(ranges.iter().copied(), 200, 500), 299);
        assert_eq!(up_to_in_flight(ranges.iter().copied(), 200, 250), 250);
        // A range that begins at `from` is the caller's own, not one to stop before.
        assert_eq!(up_to_in_flight(ranges.iter().copied(), 150, 170), 170);
    }

    /// Simulates the stream against a datalake whose answers stop after 30 blocks (paged):
    /// ranges are planned as `plan_prefetch` plans them, a short answer is continued from where
    /// it ended, and every block must be answered exactly once.
    #[test]
    fn every_block_is_fetched_once_when_answers_end_early() {
        use super::{first_uncovered, up_to_in_flight};
        const HEIGHT: i32 = 10_000;
        let plan = |in_flight: &mut Vec<(i32, i32)>, next: i32, size: i32| {
            let mut start = next;
            for _ in 0..5 {
                start = first_uncovered(in_flight.iter().copied(), start);
                if start > HEIGHT {
                    break;
                }
                let end = up_to_in_flight(
                    in_flight.iter().copied(),
                    start,
                    (start + size - 1).min(HEIGHT),
                );
                in_flight.push((start, end));
                start = end + 1;
            }
        };
        let mut in_flight: Vec<(i32, i32)> = Vec::new();
        let mut answered = vec![0u32; HEIGHT as usize + 1];
        plan(&mut in_flight, 0, 100);
        let mut next = 0;
        while next <= 3_000 {
            let i = in_flight
                .iter()
                .position(|r| r.0 == next)
                .expect("the stream's next start is always a range in flight");
            let (start, end) = in_flight.remove(i);
            // The datalake answers at most 30 blocks of the range.
            let answered_to = (start + 29).min(end);
            for b in start..=answered_to {
                answered[b as usize] += 1;
            }
            if answered_to < end {
                in_flight.push((answered_to + 1, end));
            }
            next = answered_to + 1;
            plan(&mut in_flight, next, 100);
            // In flight, no two ranges share a block.
            let mut sorted = in_flight.clone();
            sorted.sort();
            assert!(
                sorted.windows(2).all(|w| w[0].1 < w[1].0),
                "overlap: {sorted:?}"
            );
        }
        assert!(
            answered[..=3_000].iter().all(|n| *n == 1),
            "a block answered twice or never"
        );
    }

    #[test]
    fn in_flight_limit_from_429_body() {
        let body = r#"{"error":{"code":"too_many_in_flight","message":"4 requests of this key are still in flight; wait for one to finish"}}"#;
        assert_eq!(super::in_flight_limit(body), Some(4));
        let other = r#"{"error":{"code":"rate_limited","message":"rate limit of 50 requests/second exceeded"}}"#;
        assert_eq!(super::in_flight_limit(other), None);
        assert_eq!(super::in_flight_limit("not json"), None);
    }

    async fn max_in_flight(limiter: std::sync::Arc<super::Limiter>) -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let now = std::sync::Arc::new(AtomicUsize::new(0));
        let max = std::sync::Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let (limiter, now, max) = (limiter.clone(), now.clone(), max.clone());
                tokio::spawn(async move {
                    let _p = limiter.acquire().await;
                    let n = now.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    now.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        max.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn prefetches_leave_a_permit_for_the_stream() {
        let settings: super::DatalakeSettings = serde_json::from_value(serde_json::json!({
            "url": "http://prefetch-permits.test", "api_key": "test-prefetch-key",
            "max_concurrent_requests": 3
        }))
        .unwrap();
        let limiter = super::Limiter::for_settings(&settings);
        // Prefetches hold every permit they may take (two of three) ...
        let mut held = Vec::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        for _ in 0..4 {
            let (limiter, tx) = (limiter.clone(), tx.clone());
            held.push(tokio::spawn(super::IS_PREFETCH.scope((), async move {
                let permit = limiter.acquire().await;
                tx.send(()).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                drop(permit);
            })));
        }
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        // ... and the stream still gets one at once.
        let got =
            tokio::time::timeout(std::time::Duration::from_millis(50), limiter.acquire()).await;
        assert!(got.is_ok(), "the stream waited behind prefetches");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), rx.recv())
                .await
                .is_err(),
            "a third prefetch got a permit"
        );
        drop(got);
        for h in held {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn limiter_is_shared_per_key_and_can_be_lowered() {
        let settings = |url: &str| -> super::DatalakeSettings {
            serde_json::from_value(serde_json::json!({
                "url": url, "api_key": "test-limiter-key", "max_concurrent_requests": 3
            }))
            .unwrap()
        };
        let a = super::Limiter::for_settings(&settings("http://a"));
        let b = super::Limiter::for_settings(&settings("http://b"));
        assert!(std::sync::Arc::ptr_eq(&a, &b), "same key, same limiter");
        assert_eq!(max_in_flight(a.clone()).await, 3);
        a.lower_to(2);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(max_in_flight(a).await, 2);
    }

    #[test]
    fn default_limit_is_the_servers() {
        let s: super::DatalakeSettings =
            serde_json::from_value(serde_json::json!({ "url": "http://x" })).unwrap();
        assert_eq!(
            s.max_concurrent_requests,
            super::DEFAULT_MAX_CONCURRENT_REQUESTS
        );
    }
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;

    use super::decode_answer;

    /// One Arrow stream of `rows` block numbers, with the server's `bl.*` metadata.
    fn stream(row_type: &str, streams: &str, rows: &[u64], extra: &[(&str, &str)]) -> Vec<u8> {
        let mut metadata = HashMap::from([
            ("bl.row_type".to_owned(), row_type.to_owned()),
            ("bl.streams".to_owned(), streams.to_owned()),
            ("bl.next_block".to_owned(), "105".to_owned()),
            ("bl.archive_height".to_owned(), "200".to_owned()),
        ]);
        for (k, v) in extra {
            metadata.insert((*k).to_owned(), (*v).to_owned());
        }
        if streams.is_empty() {
            metadata.remove("bl.streams");
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("block_number", DataType::UInt64, false)],
            metadata,
        ));
        let mut out = Vec::new();
        let mut writer = StreamWriter::try_new(&mut out, &schema).unwrap();
        if !rows.is_empty() {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(rows.to_vec()))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        out
    }

    fn body(extra: &[(&str, &str)]) -> Vec<u8> {
        let streams = "transactions,logs,blocks";
        [
            stream("transactions", streams, &[100, 101], extra),
            stream("logs", streams, &[], extra),
            stream("blocks", streams, &[100, 101, 102], extra),
        ]
        .concat()
    }

    #[test]
    fn reads_streams_and_metadata() {
        let answer = decode_answer(&body(&[])).unwrap();
        assert_eq!(answer.next_block, 105);
        let rows = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(rows(&answer.transactions), 2);
        assert_eq!(rows(&answer.logs), 0);
        assert_eq!(rows(&answer.blocks), 3);
        assert!(answer.unavailable_fields.is_empty());

        let answer = decode_answer(&body(&[(
            "bl.unavailable_fields",
            "blocks.total_difficulty,transactions.mint",
        )]))
        .unwrap();
        assert_eq!(
            answer.unavailable_fields,
            ["blocks.total_difficulty", "transactions.mint"]
        );
    }

    #[test]
    fn rejects_a_body_cut_anywhere() {
        let full = body(&[]);
        for cut in 0..full.len() {
            assert!(decode_answer(&full[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn rejects_old_servers_and_extra_streams() {
        let old = stream("blocks", "", &[100], &[]);
        let err = decode_answer(&old).err().unwrap().to_string();
        assert!(err.contains("without bl.* stream metadata"), "{err}");

        let extra = [body(&[]), stream("blocks", "blocks", &[], &[])].concat();
        assert!(decode_answer(&extra).is_err());
    }

    /// Reads a live datalake and compares it with a JSON-RPC node. Opt-in:
    ///
    ///   DATALAKE_LIVE_URL=http://127.0.0.1:8090 DATALAKE_LIVE_RPC=http://127.0.0.1:8545 \
    ///   DATALAKE_LIVE_BLOCKS=58600000:58600039 cargo test -p graph-chain-ethereum --lib \
    ///   datalake::tests::live -- --ignored
    #[graph::test]
    #[ignore]
    async fn live() {
        use graph::prelude::{reqwest, serde_json, serde_json::json};
        use graph::slog::{Discard, Logger, o};

        let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is unset"));
        let (url, rpc) = (var("DATALAKE_LIVE_URL"), var("DATALAKE_LIVE_RPC"));
        let blocks = var("DATALAKE_LIVE_BLOCKS");
        let (from, to) = blocks.split_once(':').unwrap();
        let (from, to): (i32, i32) = (from.parse().unwrap(), to.parse().unwrap());
        let transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let logger = Logger::root(Discard, o!());
        let rpc_call = |method: &'static str, params: serde_json::Value| {
            let rpc = rpc.clone();
            async move {
                let body: serde_json::Value = reqwest::Client::new()
                    .post(rpc)
                    .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                assert!(body.get("result").is_some(), "{method}: {body}");
                body["result"].clone()
            }
        };

        // Small pages, so the scan has to follow `bl.next_block`.
        let client = super::DatalakeClient::new(
            serde_json::from_value(json!({ "url": url, "max_objects": 200 })).unwrap(),
        )
        .unwrap();
        assert!(client.archive_height().await.unwrap().unwrap() >= to);
        let filters = [json!({ "topic0": [transfer] })];
        let (mut logs, mut pages, mut next) = (0, 0, from);
        while next <= to {
            let scanned = client.fetch(&logger, next, to, &filters).await.unwrap();
            logs += scanned.blocks.iter().map(|(_, l)| l.len()).sum::<usize>();
            pages += 1;
            next = scanned.to + 1;
        }
        assert!(pages > 1, "the answer was not paged");
        let rpc_logs = rpc_call(
            "eth_getLogs",
            json!([{
                "fromBlock": format!("{from:#x}"),
                "toBlock": format!("{to:#x}"),
                "topics": [transfer],
            }]),
        )
        .await;
        assert_eq!(logs, rpc_logs.as_array().unwrap().len());

        let ptr = client.block_ptr(&logger, to).await.unwrap().unwrap();
        let block = rpc_call("eth_getBlockByNumber", json!([format!("{to:#x}"), false])).await;
        assert_eq!(
            format!("{:#x}", ptr.hash.as_b256()),
            block["hash"].as_str().unwrap()
        );
    }
}
