//! Built-in content store: graph-node keeps subgraph files (manifests, schemas, ABIs, WASM) itself
//! instead of reading them from an IPFS node.
//!
//! Files are added through the admin server's IPFS-compatible `/api/v0/add` endpoint (so the
//! stock `graph deploy --ipfs <admin url>` works) and are addressed by the same CIDv0 an IPFS node
//! would give them (UnixFS, 256 KiB chunks, balanced DAG, at most 174 links per node), so a
//! deployment id is the same with or without IPFS.
//!
//! Only whole files are kept: paths inside directories and raw blocks are reported as not
//! available.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use sha2::{Digest, Sha256};

use crate::ipfs::{
    ContentPath, IpfsClient, IpfsError, IpfsMetrics, IpfsRequest, IpfsResponse, IpfsResult,
};

/// The value of `--ipfs` that selects the built-in store (as does leaving `--ipfs` out).
pub const BUILTIN: &str = "builtin";

/// Durable storage for content-addressed files.
#[async_trait]
pub trait ContentStore: Send + Sync + 'static {
    /// Returns the file stored under `cid` (a CIDv0 string), if any.
    async fn get(&self, cid: &str) -> anyhow::Result<Option<Bytes>>;

    /// Stores `data` under `cid`. Storing the same CID twice is not an error.
    async fn put(&self, cid: &str, data: &[u8]) -> anyhow::Result<()>;
}

static STORE: OnceLock<Arc<dyn ContentStore>> = OnceLock::new();

/// Installs the process-wide content store. Only the first call has an effect.
pub fn set_content_store(store: Arc<dyn ContentStore>) {
    let _ = STORE.set(store);
}

/// The process-wide content store, once the node has set one up.
pub fn content_store() -> Option<Arc<dyn ContentStore>> {
    STORE.get().cloned()
}

/// True when these `--ipfs` addresses select the built-in store.
pub fn is_builtin<S: AsRef<str>>(addresses: &[S]) -> bool {
    addresses.is_empty() || (addresses.len() == 1 && addresses[0].as_ref() == BUILTIN)
}

/// Reads subgraph files from the built-in content store.
pub struct BuiltinIpfsClient {
    metrics: IpfsMetrics,
}

impl BuiltinIpfsClient {
    pub fn new(metrics: IpfsMetrics) -> Self {
        Self { metrics }
    }
}

fn not_available(path: ContentPath, reason: impl std::fmt::Display) -> IpfsError {
    IpfsError::ContentNotAvailable {
        path,
        reason: anyhow::anyhow!("{}", reason),
    }
}

/// Multicodec of dag-pb, the only codec a CIDv0 can name.
const DAG_PB: u64 = 0x70;

/// The key a file is stored under: the CIDv0 string, also for a CIDv1 of the same dag-pb node.
pub fn store_key(cid: &Cid) -> Option<String> {
    match cid.version() {
        cid::Version::V0 => Some(cid.to_string()),
        cid::Version::V1 if cid.codec() == DAG_PB => {
            Cid::new_v0(*cid.hash()).ok().map(|cid| cid.to_string())
        }
        cid::Version::V1 => None,
    }
}

#[async_trait]
impl IpfsClient for BuiltinIpfsClient {
    fn metrics(&self) -> &IpfsMetrics {
        &self.metrics
    }

    async fn call(self: Arc<Self>, req: IpfsRequest) -> IpfsResult<IpfsResponse> {
        let path = match req {
            IpfsRequest::Cat(path) => path,
            IpfsRequest::GetBlock(path) => {
                return Err(not_available(
                    path,
                    "the built-in store keeps no raw blocks",
                ));
            }
        };
        if path.path().is_some() {
            return Err(not_available(
                path,
                "the built-in store keeps files, not directories",
            ));
        }
        let Some(key) = store_key(path.cid()) else {
            return Err(not_available(path, "not a dag-pb CID"));
        };
        let Some(store) = content_store() else {
            return Err(not_available(path, "the built-in store is not ready"));
        };
        match store.get(&key).await {
            Ok(Some(bytes)) => Ok(IpfsResponse {
                path,
                response: reqwest::Response::from(http::Response::new(bytes)),
            }),
            Ok(None) => Err(not_available(path, "not in the built-in store")),
            Err(e) => Err(not_available(path, format!("built-in store: {e:#}"))),
        }
    }
}

// --- CIDv0 of a file, as an IPFS node computes it by default -------------------------------

const CHUNK_SIZE: usize = 262_144;
const MAX_LINKS: usize = 174;
/// UnixFS `DataType.File`.
const UNIXFS_FILE: u64 = 2;

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    put_varint(out, (field << 3) | 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_uint(out: &mut Vec<u8>, field: u64, v: u64) {
    put_varint(out, field << 3);
    put_varint(out, v);
}

/// UnixFS `Data` for a file node.
fn unixfs_file(data: Option<&[u8]>, filesize: u64, blocksizes: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uint(&mut out, 1, UNIXFS_FILE);
    if let Some(data) = data.filter(|d| !d.is_empty()) {
        put_bytes(&mut out, 2, data);
    }
    put_uint(&mut out, 3, filesize);
    for size in blocksizes {
        put_uint(&mut out, 4, *size);
    }
    out
}

/// One encoded dag-pb node, with what its parent needs to link to it.
struct Node {
    /// Multihash (sha2-256) of the encoded node.
    hash: Vec<u8>,
    /// Encoded size of the node plus the sizes of everything below it.
    tsize: u64,
    /// File bytes below this node.
    filesize: u64,
}

fn node(links: &[Node], data: &[u8]) -> (Node, Vec<u8>) {
    // dag-pb canonical form: links (field 2) before data (field 1).
    let mut out = Vec::new();
    for link in links {
        let mut l = Vec::new();
        put_bytes(&mut l, 1, &link.hash);
        put_bytes(&mut l, 2, b"");
        put_uint(&mut l, 3, link.tsize);
        put_bytes(&mut out, 2, &l);
    }
    put_bytes(&mut out, 1, data);
    let mut hash = vec![0x12, 0x20];
    hash.extend_from_slice(&Sha256::digest(&out));
    let tsize = out.len() as u64 + links.iter().map(|l| l.tsize).sum::<u64>();
    let filesize = links.iter().map(|l| l.filesize).sum();
    (
        Node {
            hash,
            tsize,
            filesize,
        },
        out,
    )
}

fn leaf(chunk: &[u8]) -> Node {
    let (mut n, _) = node(&[], &unixfs_file(Some(chunk), chunk.len() as u64, &[]));
    n.filesize = chunk.len() as u64;
    n
}

fn parent(children: Vec<Node>) -> Node {
    let filesize: u64 = children.iter().map(|c| c.filesize).sum();
    let sizes: Vec<u64> = children.iter().map(|c| c.filesize).collect();
    let (n, _) = node(&children, &unixfs_file(None, filesize, &sizes));
    n
}

/// The CIDv0 (`Qm…`) an IPFS node assigns to `data` added as a file with default settings.
pub fn cid_v0(data: &[u8]) -> String {
    let mut level: Vec<Node> = if data.is_empty() {
        vec![leaf(&[])]
    } else {
        data.chunks(CHUNK_SIZE).map(leaf).collect()
    };
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(MAX_LINKS));
        let mut it = level.into_iter().peekable();
        while it.peek().is_some() {
            next.push(parent(it.by_ref().take(MAX_LINKS).collect()));
        }
        level = next;
    }
    bs58::encode(&level[0].hash).into_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_files() {
        assert_eq!(
            cid_v0(b""),
            "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH"
        );
        assert_eq!(
            cid_v0(b"hello world\n"),
            "QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o"
        );
    }

    #[test]
    fn builtin_selection() {
        assert!(is_builtin::<&str>(&[]));
        assert!(is_builtin(&["builtin"]));
        assert!(!is_builtin(&["http://localhost:5001"]));
    }

    #[test]
    fn v1_key_is_v0() {
        let v0: Cid = "QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o"
            .parse()
            .unwrap();
        let v1 = Cid::new_v1(0x70, *v0.hash());
        assert_eq!(store_key(&v1).unwrap(), v0.to_string());
    }
}
