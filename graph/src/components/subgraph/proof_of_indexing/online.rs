//! This is an online (streaming) implementation of the reference implementation
//! Any hash constructed from here should be the same as if the same data was given
//! to the reference implementation, but this is updated incrementally

use super::{ProofOfIndexingEvent, ProofOfIndexingVersion};
use crate::{
    blockchain::BlockPtr,
    data::store::Id,
    prelude::{BlockNumber, DeploymentHash, ENV_VARS, Logger, debug},
    util::stable_hash_glue::AsBytes,
};
use alloy::primitives::Address;
use sha2::{Digest, Sha256};
use stable_hash::{FieldAddress, StableHash, StableHasher, fast::FastStableHasher};
use stable_hash_legacy::crypto::{Blake3SeqNo, SetHasher};
use stable_hash_legacy::prelude::{
    StableHash as StableHashLegacy, StableHasher as StableHasherLegacy, *,
};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

pub struct BlockEventStream {
    vec_length: u64,
    handler_start: u64,
    block_index: u64,
    hasher: Hashers,
}

enum Hashers {
    Fast(FastStableHasher),
    Legacy(SetHasher),
}

const STABLE_HASH_LEN: usize = 32;

impl Hashers {
    fn new(version: ProofOfIndexingVersion) -> Self {
        match version {
            ProofOfIndexingVersion::Legacy => Hashers::Legacy(SetHasher::new()),
            ProofOfIndexingVersion::Fast => Hashers::Fast(FastStableHasher::new()),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        match bytes.try_into() {
            Ok(bytes) => Hashers::Fast(FastStableHasher::from_bytes(bytes)),
            Err(_) => Hashers::Legacy(SetHasher::from_bytes(bytes)),
        }
    }

    fn write<T>(&mut self, value: &T, children: &[u64])
    where
        T: StableHash + StableHashLegacy,
    {
        match self {
            Hashers::Fast(fast) => {
                let addr = children.iter().fold(u128::root(), |s, i| s.child(*i));
                StableHash::stable_hash(value, addr, fast);
            }
            Hashers::Legacy(legacy) => {
                let seq_no = traverse_seq_no(children);
                StableHashLegacy::stable_hash(value, seq_no, legacy);
            }
        }
    }
}

/// Go directly to a SequenceNumber identifying a field within a struct.
/// This is best understood by example. Consider the struct:
///
/// struct Outer {
///    inners: Vec<Inner>,
///    outer_num: i32
/// }
/// struct Inner {
///    inner_num: i32,
///    inner_str: String,
/// }
///
/// Let's say that we have the following data:
/// Outer {
///    inners: vec![
///       Inner {
///           inner_num: 10,
///           inner_str: "THIS",
///       },
///    ],
///    outer_num: 0,
/// }
///
/// And we need to identify the string "THIS", at outer.inners[0].inner_str;
/// This would require the following:
/// traverse_seq_no(&[
///    0, // Outer.inners
///    0, // Vec<Inner>[0]
///    1, // Inner.inner_str
///])
// Performance: Could write a specialized function for this, avoiding a bunch of clones of Blake3SeqNo
fn traverse_seq_no(counts: &[u64]) -> Blake3SeqNo {
    counts.iter().fold(Blake3SeqNo::root(), |mut s, i| {
        s.skip(*i as usize);
        s.next_child()
    })
}

impl BlockEventStream {
    fn new(block_number: BlockNumber, version: ProofOfIndexingVersion) -> Self {
        let block_index: u64 = block_number.try_into().unwrap();

        Self {
            vec_length: 0,
            handler_start: 0,
            block_index,
            hasher: Hashers::new(version),
        }
    }

    /// Finishes the current block and returns the serialized hash function to
    /// be resumed later. Cases in which the hash function is resumed include
    /// when asking for the final PoI, or when combining with the next modified
    /// block via the argument `prev`
    pub fn pause(mut self, prev: Option<&[u8]>) -> Vec<u8> {
        self.hasher
            .write(&self.vec_length, &[1, 0, self.block_index, 0]);
        match self.hasher {
            Hashers::Legacy(mut digest) => {
                if let Some(prev) = prev {
                    let prev = SetHasher::from_bytes(prev);
                    // SequenceNumber::root() is misleading here since the parameter
                    // is unused.
                    digest.finish_unordered(prev, SequenceNumber::root());
                }
                digest.to_bytes()
            }
            Hashers::Fast(mut digest) => {
                if let Some(prev) = prev {
                    let prev = if prev.len() == STABLE_HASH_LEN {
                        prev.try_into()
                            .expect("Expected valid fast stable hash representation")
                    } else {
                        let mut hasher = Sha256::new();
                        hasher.update(prev);
                        hasher.finalize().into()
                    };
                    let prev = FastStableHasher::from_bytes(prev);
                    digest.mixin(&prev);
                }
                digest.to_bytes().to_vec()
            }
        }
    }

    fn write(&mut self, event: &ProofOfIndexingEvent<'_>) {
        let children = &event_address(self.block_index, self.vec_length);
        self.hasher.write(&event, children);
        self.vec_length += 1;
    }

    /// Writes the event `contribution` was computed for, if it was computed for the position
    /// the next event takes. Returns whether it did.
    fn write_contribution(&mut self, position: u64, contribution: &PoiContribution) -> bool {
        match &mut self.hasher {
            Hashers::Fast(hasher) if position == self.vec_length => {
                hasher.mixin(&contribution.0);
                self.vec_length += 1;
                true
            }
            _ => false,
        }
    }

    fn start_handler(&mut self) {
        self.handler_start = self.vec_length;
    }
}

/// The address of the `position`th event of a causality region in block `block_index`.
fn event_address(block_index: u64, position: u64) -> [u64; 5] {
    [
        1,           // kvp -> v
        0,           // PoICausalityRegion.blocks: Result<Vec<Block>>
        block_index, // Result<Vec<Block>> -> [i]
        0,           // Block.events -> Vec<ProofOfIndexingEvent>
        position,
    ]
}

/// What one event adds to a block's (fast) proof of indexing when it is the `position`th event
/// of its causality region. The fast hash mixes the hashes of its leaves in any order, so this
/// can be computed ahead of time, on another thread, and mixed in later: that is the same as
/// writing the event at that position.
#[derive(Clone, Debug)]
pub struct PoiContribution(FastStableHasher);

impl PoiContribution {
    pub fn new(block_number: BlockNumber, position: u64, event: &ProofOfIndexingEvent<'_>) -> Self {
        let block_index: u64 = block_number.try_into().unwrap();
        let addr = event_address(block_index, position)
            .iter()
            .fold(u128::root(), |s, i| s.child(*i));
        let mut hasher = FastStableHasher::new();
        StableHash::stable_hash(&event, addr, &mut hasher);
        PoiContribution(hasher)
    }
}

pub struct ProofOfIndexing {
    version: ProofOfIndexingVersion,
    block_number: BlockNumber,
    /// The POI is updated for each data source independently. This is necessary because
    /// some data sources (eg: IPFS files) may be unreliable and therefore cannot mix
    /// state with other data sources. This may also give us some freedom to change
    /// the order of triggers in the future.
    per_causality_region: HashMap<Id, BlockEventStream>,
}

impl fmt::Debug for ProofOfIndexing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProofOfIndexing").field(&"...").finish()
    }
}

impl ProofOfIndexing {
    pub fn new(block_number: BlockNumber, version: ProofOfIndexingVersion) -> Self {
        Self {
            version,
            block_number,
            per_causality_region: HashMap::new(),
        }
    }
}

impl ProofOfIndexing {
    pub fn write_deterministic_error(&mut self, logger: &Logger, causality_region: &str) {
        let redacted_events = self.with_causality_region(causality_region, |entry| {
            entry.vec_length - entry.handler_start
        });

        self.write(
            logger,
            causality_region,
            &ProofOfIndexingEvent::DeterministicError { redacted_events },
        )
    }

    /// Adds an event to the digest of the ProofOfIndexingStream local to the causality region
    pub fn write(
        &mut self,
        logger: &Logger,
        causality_region: &str,
        event: &ProofOfIndexingEvent<'_>,
    ) {
        if ENV_VARS.log_poi_events {
            debug!(
                logger,
                "Proof of indexing event";
                "event" => &event,
                "causality_region" => causality_region,
                "block_number" => self.block_number
            );
        }

        self.with_causality_region(causality_region, |entry| entry.write(event))
    }

    /// Writes the event `contribution` was computed for, if the causality region's next event
    /// is at `position` and this is a fast proof of indexing. Returns whether it did; if not, the
    /// caller writes the event with `write`.
    pub fn write_contribution(
        &mut self,
        causality_region: &str,
        position: u64,
        contribution: &PoiContribution,
    ) -> bool {
        if ENV_VARS.log_poi_events {
            return false;
        }
        self.with_causality_region(causality_region, |entry| {
            entry.write_contribution(position, contribution)
        })
    }

    pub fn start_handler(&mut self, causality_region: &str) {
        self.with_causality_region(causality_region, |entry| entry.start_handler())
    }

    // This is just here because the raw_entry API is not stabilized.
    fn with_causality_region<F, T>(&mut self, causality_region: &str, f: F) -> T
    where
        F: FnOnce(&mut BlockEventStream) -> T,
    {
        // A block has few causality regions; finding one by comparing names avoids building an
        // `Id` for every event.
        let existing = self
            .per_causality_region
            .iter_mut()
            .find(|(id, _)| matches!(id, Id::String(name) if name.as_str() == causality_region));
        if let Some((_, entry)) = existing {
            return f(entry);
        }
        let causality_region = Id::String(causality_region.to_owned().into());
        if let Some(causality_region) = self.per_causality_region.get_mut(&causality_region) {
            f(causality_region)
        } else {
            let mut entry = BlockEventStream::new(self.block_number, self.version);
            let result = f(&mut entry);
            self.per_causality_region.insert(causality_region, entry);
            result
        }
    }

    pub fn take(self) -> HashMap<Id, BlockEventStream> {
        self.per_causality_region
    }

    pub fn get_block(&self) -> BlockNumber {
        self.block_number
    }
}

pub struct ProofOfIndexingFinisher {
    block_number: BlockNumber,
    state: Hashers,
    causality_count: usize,
}

impl ProofOfIndexingFinisher {
    pub fn new(
        block: &BlockPtr,
        subgraph_id: &DeploymentHash,
        indexer: &Option<Address>,
        version: ProofOfIndexingVersion,
    ) -> Self {
        let mut state = Hashers::new(version);

        // Add PoI.subgraph_id
        state.write(&subgraph_id, &[1]);

        // Add PoI.block_hash
        state.write(&AsBytes(block.hash_slice()), &[2]);

        // Add PoI.indexer
        state.write(&indexer.as_ref().map(|i| AsBytes(i.as_slice())), &[3]);

        ProofOfIndexingFinisher {
            block_number: block.number,
            state,
            causality_count: 0,
        }
    }

    pub fn add_causality_region(&mut self, name: &Id, region: &[u8]) {
        let mut state = Hashers::from_bytes(region);

        // Finish the blocks vec by writing kvp[v], PoICausalityRegion.blocks.len()
        // + 1 is to account that the length of the blocks array for the genesis block is 1, not 0.
        state.write(&(self.block_number + 1), &[1, 0]);

        // Add the name (kvp[k]).
        state.write(&name, &[0]);

        // Mixin the region into PoI.causality_regions.
        match state {
            Hashers::Legacy(legacy) => {
                let state = legacy.finish();
                self.state.write(&AsBytes(&state), &[0, 1]);
            }
            Hashers::Fast(fast) => {
                let state = fast.to_bytes();
                self.state.write(&AsBytes(&state), &[0]);
            }
        }

        self.causality_count += 1;
    }

    pub fn finish(mut self) -> [u8; 32] {
        if let Hashers::Legacy(_) = self.state {
            // Add PoI.causality_regions.len()
            // Note that technically to get the same sequence number one would need
            // to call causality_regions_count_seq_no.skip(self.causality_count);
            // but it turns out that the result happens to be the same for
            // non-negative numbers.
            self.state.write(&self.causality_count, &[0, 2]);
        }

        match self.state {
            Hashers::Legacy(legacy) => legacy.finish(),
            Hashers::Fast(fast) => alloy::primitives::keccak256(fast.finish().to_le_bytes()).0,
        }
    }
}
