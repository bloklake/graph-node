//! Speculative trigger execution.
//!
//! Blocks the runner has read ahead from the block stream have their triggers matched and run on
//! the speculation threads (`GRAPH_SPECULATION_THREADS`) while the runner works through earlier
//! blocks. Each speculative run starts from a fresh block state over the store as it was when the
//! run was launched (the snapshot) and records what the handler read and, in order, what it
//! wrote.
//!
//! When the runner reaches the trigger, it uses the run only if nothing the handler read changed
//! after the snapshot: no later block and no earlier trigger of the same block wrote a key it
//! read (or, for derived-field lookups, any entity of the type it scanned). It then replays the
//! recorded writes through the same entity cache calls and proof of indexing events a normal run
//! makes, in the same order, so vids, entity versions and the proof of indexing come out the same.
//! Anything else (a changed read, a handler error, a generated id, a host set that differs) runs
//! the trigger normally.

use std::collections::HashMap;
use std::sync::Mutex;

use graph::blockchain::BlockHash;
use graph::components::subgraph::{
    MappingError, ProofOfIndexingEvent, SharedProofOfIndexing, SpeculativeOp,
};
use graph::futures03::channel::oneshot;
use graph::prelude::{BlockNumber, BlockState, EntityModification};
use graph::schema::{EntityKey, EntityType};
use graph::slog::Logger;

/// Blocks the runner reads ahead of the one it processes.
pub(super) const LOOKAHEAD_BLOCKS: usize = 1024;
/// Speculative runs in flight at most; reading ahead stops above this.
pub(super) const MAX_PENDING: usize = 16_384;
/// Below this much write capacity left in a block, triggers run normally (a replayed write
/// could fail on capacity where the speculative run, starting fresh, did not).
const MIN_WRITE_CAPACITY: usize = 256 * 1024 * 1024;

/// A trigger's speculative run: the block, the trigger's position in it, and the host.
pub(super) type Key = (BlockHash, usize, usize);

pub(super) struct Pending {
    /// Last block the store had when the run was launched.
    snapshot: BlockNumber,
    result: oneshot::Receiver<Result<BlockState, MappingError>>,
}

/// Which hosts each trigger of a read-ahead block was matched to when its runs were launched.
pub(super) struct LaunchedBlock {
    /// `SubgraphInstance::hosts_generation` at launch; while it is unchanged, matching the
    /// block's triggers again would give the same hosts.
    pub generation: u64,
    /// Per trigger, the hosts it matched, in order; every one has a pending run.
    pub hosts: Vec<Vec<usize>>,
}

#[derive(Default)]
struct Inner {
    pending: HashMap<Key, Pending>,
    blocks: HashMap<BlockHash, LaunchedBlock>,
    /// The last block in which each entity (type) was written, for blocks the runner processed
    /// while speculative runs may have been reading older state.
    written: HashMap<EntityKey, BlockNumber>,
    written_types: HashMap<EntityType, BlockNumber>,
}

#[derive(Default)]
pub(super) struct Speculations {
    inner: Mutex<Inner>,
}

/// Identifies a host by its address; hosts live behind `Arc`s for the runner's lifetime.
pub(super) fn host_id<H: ?Sized>(host: &H) -> usize {
    (host as *const H).cast::<()>() as usize
}

impl Speculations {
    pub(super) fn pending_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    pub(super) fn insert(
        &self,
        key: Key,
        snapshot: BlockNumber,
        result: oneshot::Receiver<Result<BlockState, MappingError>>,
    ) {
        self.inner
            .lock()
            .unwrap()
            .pending
            .insert(key, Pending { snapshot, result });
    }

    pub(super) fn launched(&self, hash: BlockHash, block: LaunchedBlock) {
        self.inner.lock().unwrap().blocks.insert(hash, block);
    }

    pub(super) fn take_launched(&self, hash: &BlockHash) -> Option<LaunchedBlock> {
        self.inner.lock().unwrap().blocks.remove(hash)
    }

    /// Takes the runs of all `keys`, or none of them when any is missing.
    pub(super) fn take_all(&self, keys: impl Iterator<Item = Key>) -> Option<Vec<Pending>> {
        let mut inner = self.inner.lock().unwrap();
        keys.map(|key| inner.pending.remove(&key)).collect()
    }

    /// Drops every pending run (reverts and restarts).
    pub(super) fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending.clear();
        inner.blocks.clear();
        inner.written.clear();
        inner.written_types.clear();
    }

    /// Notes what a processed block wrote.
    pub(super) fn record_writes(&self, block: BlockNumber, mods: &[EntityModification]) {
        let mut inner = self.inner.lock().unwrap();
        if inner.pending.is_empty() {
            // Only runs launched from now on can be checked, and their snapshots start here.
            inner.written.clear();
            inner.written_types.clear();
            return;
        }
        for m in mods {
            let key = m.key();
            inner.written.insert(key.clone(), block);
            if inner
                .written_types
                .get(&key.entity_type)
                .is_none_or(|b| *b < block)
            {
                inner.written_types.insert(key.entity_type.clone(), block);
            }
        }
        if inner.written.len() > 1_000_000 {
            // Writes at or before every pending snapshot cannot invalidate anything.
            if let Some(oldest) = inner.pending.values().map(|p| p.snapshot).min() {
                inner.written.retain(|_, b| *b > oldest);
                inner.written_types.retain(|_, b| *b > oldest);
            }
        }
    }

    /// Whether `spec` (a finished run from `snapshot`) is what running the trigger now would
    /// produce, given `state`, the block state so far.
    fn valid(&self, snapshot: BlockNumber, spec: &BlockState, state: &BlockState) -> bool {
        if !spec.deterministic_errors.is_empty()
            || state.write_capacity_remaining < MIN_WRITE_CAPACITY
        {
            return false;
        }
        let Some(log) = &spec.speculation else {
            return false;
        };
        if log.unsupported {
            return false;
        }
        let inner = self.inner.lock().unwrap();
        let changed = |b: Option<&BlockNumber>| b.is_some_and(|b| *b > snapshot);
        log.reads
            .iter()
            .all(|key| !changed(inner.written.get(key)) && !state.entity_cache.touched(key))
            && log.related_types.iter().all(|t| {
                !changed(inner.written_types.get(t)) && !state.entity_cache.touched_type(t)
            })
    }

    /// Waits for `pending` and returns their states if every run can be used as is.
    pub(super) async fn resolve(
        &self,
        pending: Vec<Pending>,
        state: &BlockState,
    ) -> Option<Vec<BlockState>> {
        let mut out = Vec::with_capacity(pending.len());
        for Pending { snapshot, result } in pending {
            let spec = match result.await {
                Ok(Ok(spec)) => spec,
                _ => return None,
            };
            if !self.valid(snapshot, &spec, state) {
                return None;
            }
            out.push(spec);
        }
        Some(out)
    }
}

/// Applies a speculative run's effects to `state` the way running the handler would have.
/// `region` is the proof of indexing causality region the handler's host writes to.
pub(super) async fn replay(
    logger: &Logger,
    mut spec: BlockState,
    state: &mut BlockState,
    proof_of_indexing: &SharedProofOfIndexing,
    region: &str,
) -> Result<(), MappingError> {
    let log = spec
        .speculation
        .take()
        .expect("speculative runs carry a speculation log");

    state.enter_handler();
    for op in log.ops {
        match op {
            SpeculativeOp::Set { key, entity } => {
                proof_of_indexing.write_event(
                    &ProofOfIndexingEvent::SetEntity {
                        entity_type: key.entity_type.typename(),
                        id: &key.entity_id.to_string(),
                        data: &entity,
                    },
                    region,
                    logger,
                );
                if let Err(e) = state
                    .entity_cache
                    .set(key, entity, Some(&mut state.write_capacity_remaining))
                    .await
                {
                    state.exit_handler();
                    return Err(MappingError::Unknown(
                        e.context("replaying a speculative handler run"),
                    ));
                }
            }
            SpeculativeOp::Remove {
                key,
                poi_type,
                poi_id,
            } => {
                proof_of_indexing.write_event(
                    &ProofOfIndexingEvent::RemoveEntity {
                        entity_type: &poi_type,
                        id: &poi_id,
                    },
                    region,
                    logger,
                );
                state.entity_cache.remove(key);
            }
        }
    }
    for ds in spec.drain_created_data_sources() {
        state.push_created_data_source(ds);
    }
    state.metrics.extend(spec.metrics);
    state.exit_handler();
    Ok(())
}
