//! Speculative trigger execution.
//!
//! Blocks the runner has read ahead from the block stream have their triggers matched and run as
//! speculative tasks (`GRAPH_SPECULATION_THREADS`) while the runner works through earlier blocks.
//! Each speculative run starts from a fresh block state over the store as it was when the run was
//! launched (the snapshot) and records what the handler read and, in order, what it wrote.
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
    MappingError, PoiContribution, ProofOfIndexingEvent, SharedProofOfIndexing, SpeculativeOp,
    SpeculativeResult, SpeculativeRun,
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

/// A run within a block: the trigger's position in the block and the host.
pub(super) type RunKey = (usize, usize);
pub(super) type RunResult = Result<SpeculativeRun, MappingError>;

/// Identifies a host by its address; hosts live behind `Arc`s for the runner's lifetime.
pub(super) fn host_id<H: ?Sized>(host: &H) -> usize {
    (host as *const H).cast::<()>() as usize
}

/// The runs of one read-ahead block, delivered together when all have finished.
struct BlockRuns {
    /// Last block the store had when the runs were launched.
    snapshot: BlockNumber,
    count: usize,
    runs: oneshot::Receiver<Vec<(RunKey, RunResult)>>,
}

/// A block's finished runs, taken by the runner when it processes the block.
pub(super) struct ReadyRuns {
    snapshot: BlockNumber,
    runs: HashMap<RunKey, RunResult>,
}

/// Which hosts each trigger of a read-ahead block was matched to when its runs were launched.
pub(super) struct LaunchedBlock {
    /// `SubgraphInstance::hosts_generation` at launch; while it is unchanged, matching the
    /// block's triggers again would give the same hosts.
    pub generation: u64,
    /// Per trigger, its candidate hosts, in order; every one has a run.
    pub hosts: Vec<Vec<usize>>,
}

/// How often a handler's speculative runs could be used. Handlers whose runs mostly read
/// state an earlier trigger changed (balances of busy accounts, say) are not worth running
/// twice; they are speculated again now and then in case that changes.
#[derive(Default, Clone, Copy)]
struct HandlerStats {
    tried: u32,
    used: u32,
}

/// Runs judged before a handler's success rate decides anything.
const STATS_MIN_TRIED: u32 = 64;
/// Handlers whose runs are usable less often than this are not speculated...
const STATS_MIN_USED_RATIO: f64 = 0.5;
/// ...except every this many triggers, to notice when that changes.
const STATS_PROBE_EVERY: u32 = 256;

/// Per-handler success counts, shared with the speculative tasks that consult them.
#[derive(Default)]
pub(super) struct Stats {
    inner: Mutex<StatsInner>,
}

#[derive(Default)]
struct StatsInner {
    handlers: HashMap<String, HandlerStats>,
    /// Triggers skipped since the last probe, per handler.
    skipped: HashMap<String, u32>,
}

impl Stats {
    /// Whether a trigger for `handler` should get a speculative run.
    pub(super) fn worth_running(&self, handler: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let stats = inner.handlers.get(handler).copied().unwrap_or_default();
        if stats.tried < STATS_MIN_TRIED
            || f64::from(stats.used) >= STATS_MIN_USED_RATIO * f64::from(stats.tried)
        {
            return true;
        }
        let skipped = inner.skipped.entry(handler.to_string()).or_default();
        *skipped += 1;
        if *skipped >= STATS_PROBE_EVERY {
            *skipped = 0;
            // Judge the handler afresh from the probes.
            inner.handlers.insert(
                handler.to_string(),
                HandlerStats {
                    tried: STATS_MIN_TRIED / 2,
                    used: STATS_MIN_TRIED / 2,
                },
            );
            return true;
        }
        false
    }

    fn judged(&self, handler: &str, used: bool) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.handlers.contains_key(handler) {
            inner
                .handlers
                .insert(handler.to_string(), HandlerStats::default());
        }
        let stats = inner.handlers.get_mut(handler).expect("inserted above");
        stats.tried = stats.tried.saturating_add(1);
        stats.used = stats.used.saturating_add(u32::from(used));
    }
}

#[derive(Default)]
struct Inner {
    blocks: HashMap<BlockHash, BlockRuns>,
    launched: HashMap<BlockHash, LaunchedBlock>,
    /// Runs of all `blocks`.
    pending_runs: usize,
    /// The last block in which each entity (type) was written, for blocks the runner processed
    /// while speculative runs may have been reading older state. Keys are only recorded for
    /// `watched` types.
    written: HashMap<EntityKey, BlockNumber>,
    written_types: HashMap<EntityType, BlockNumber>,
    /// Entity types some run read, with the first block whose writes are recorded. Types no
    /// run reads (insert-only events, say) cost no bookkeeping.
    watched: HashMap<EntityType, BlockNumber>,
}

#[derive(Default)]
pub(super) struct Speculations {
    inner: Mutex<Inner>,
    pub(super) stats: std::sync::Arc<Stats>,
}

impl Speculations {
    pub(super) fn pending_len(&self) -> usize {
        self.inner.lock().unwrap().pending_runs
    }

    /// Registers the `count` runs launched for block `hash`.
    pub(super) fn insert_block(
        &self,
        hash: BlockHash,
        snapshot: BlockNumber,
        count: usize,
        runs: oneshot::Receiver<Vec<(RunKey, RunResult)>>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending_runs += count;
        if let Some(old) = inner.blocks.insert(
            hash,
            BlockRuns {
                snapshot,
                count,
                runs,
            },
        ) {
            inner.pending_runs -= old.count;
        }
    }

    pub(super) fn launched(&self, hash: BlockHash, block: LaunchedBlock) {
        self.inner.lock().unwrap().launched.insert(hash, block);
    }

    pub(super) fn take_launched(&self, hash: &BlockHash) -> Option<LaunchedBlock> {
        self.inner.lock().unwrap().launched.remove(hash)
    }

    /// Takes block `hash`'s runs, waiting for them to finish.
    pub(super) async fn take_block(&self, hash: &BlockHash) -> Option<ReadyRuns> {
        let block = {
            let mut inner = self.inner.lock().unwrap();
            let block = inner.blocks.remove(hash)?;
            inner.pending_runs -= block.count;
            block
        };
        let runs = block.runs.await.ok()?;
        Some(ReadyRuns {
            snapshot: block.snapshot,
            runs: runs.into_iter().collect(),
        })
    }

    /// Drops every pending run (reverts and restarts).
    pub(super) fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.blocks.clear();
        inner.launched.clear();
        inner.pending_runs = 0;
        inner.written.clear();
        inner.written_types.clear();
        inner.watched.clear();
    }

    /// Notes what a processed block wrote.
    pub(super) fn record_writes(&self, block: BlockNumber, mods: &[EntityModification]) {
        let mut inner = self.inner.lock().unwrap();
        if inner.blocks.is_empty() {
            // Only runs launched from now on can be checked, and their snapshots start here.
            inner.written.clear();
            inner.written_types.clear();
            return;
        }
        for m in mods {
            let key = m.key();
            if inner.watched.contains_key(&key.entity_type) {
                inner.written.insert(key.clone(), block);
            }
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
            if let Some(oldest) = inner.blocks.values().map(|b| b.snapshot).min() {
                inner.written.retain(|_, b| *b > oldest);
                inner.written_types.retain(|_, b| *b > oldest);
            }
        }
    }

    /// Whether `spec` (a finished run from `snapshot`) is what running the trigger now would
    /// produce, given `state`, the state of `block` (being processed) so far.
    fn valid(
        &self,
        block: BlockNumber,
        snapshot: BlockNumber,
        spec: &SpeculativeResult,
        state: &BlockState,
    ) -> bool {
        if spec.failed || state.write_capacity_remaining < MIN_WRITE_CAPACITY {
            return false;
        }
        let log = &spec.log;
        if log.unsupported {
            return false;
        }
        if log.reads.is_empty() && log.related_types.is_empty() {
            return true;
        }
        let mut inner = self.inner.lock().unwrap();
        // Writes to a key are known from `since` on; they cover the run only if they are known
        // from just after its snapshot. A type read for the first time is watched from `block`
        // on (its writes are recorded when `block` is committed).
        let mut covered = true;
        for key in &log.reads {
            match inner.watched.get(&key.entity_type) {
                Some(since) if *since <= snapshot + 1 => {}
                Some(_) => covered = false,
                None => {
                    inner.watched.insert(key.entity_type.clone(), block);
                    covered = false;
                }
            }
        }
        if !covered {
            return false;
        }
        let changed = |b: Option<&BlockNumber>| b.is_some_and(|b| *b > snapshot);
        log.reads
            .iter()
            .all(|key| !changed(inner.written.get(key)) && !state.entity_cache.touched(key))
            && log.related_types.iter().all(|t| {
                !changed(inner.written_types.get(t)) && !state.entity_cache.touched_type(t)
            })
    }

    /// Takes trigger `trigger`'s runs for `hosts` (its candidate hosts, in order) and returns,
    /// per host, the state to replay (`None`: the trigger does not match the host), if every
    /// run can be used as is.
    pub(super) fn resolve(
        &self,
        ready: &mut ReadyRuns,
        block: BlockNumber,
        trigger: usize,
        hosts: impl Iterator<Item = usize>,
        state: &BlockState,
    ) -> Option<Vec<Option<SpeculativeResult>>> {
        let mut out = Vec::new();
        for host in hosts {
            match ready.runs.remove(&(trigger, host))? {
                Ok(SpeculativeRun::Ran {
                    result: spec,
                    handler,
                }) => {
                    let valid = self.valid(block, ready.snapshot, &spec, state);
                    self.stats.judged(&handler, valid);
                    if !valid {
                        return None;
                    }
                    out.push(Some(spec));
                }
                Ok(SpeculativeRun::NotMatched) => out.push(None),
                _ => return None,
            }
        }
        Some(out)
    }
}

/// The proof of indexing event a speculative write stands for.
fn poi_event(op: &SpeculativeOp) -> ProofOfIndexingEvent<'_> {
    match op {
        SpeculativeOp::Set {
            key,
            entity,
            poi_id,
            ..
        } => ProofOfIndexingEvent::SetEntity {
            entity_type: key.entity_type.typename(),
            id: poi_id,
            data: entity,
        },
        SpeculativeOp::Remove {
            poi_type, poi_id, ..
        } => ProofOfIndexingEvent::RemoveEntity {
            entity_type: poi_type,
            id: poi_id,
        },
    }
}

/// Computes the proof of indexing contributions of a block's runs (`results`, in the order the
/// runner replays them) for the positions their events take if every run is replayed. A run the
/// runner does not replay shifts the positions after it; replay then writes those events
/// normally.
pub(super) fn precompute_poi(block: BlockNumber, results: &mut [(RunKey, RunResult)]) {
    let mut position = 0;
    for (_, result) in results {
        let Ok(SpeculativeRun::Ran { result, .. }) = result else {
            continue;
        };
        let log = &mut result.log;
        log.poi_start = position;
        log.poi = log
            .ops
            .iter()
            .map(|op| {
                let contribution = PoiContribution::new(block, position, &poi_event(op));
                position += 1;
                contribution
            })
            .collect();
    }
}

/// Applies a speculative run's effects to `state` the way running the handler would have.
/// `region` is the proof of indexing causality region the handler's host writes to.
pub(super) async fn replay(
    logger: &Logger,
    spec: SpeculativeResult,
    state: &mut BlockState,
    proof_of_indexing: &SharedProofOfIndexing,
    region: &str,
) -> Result<(), MappingError> {
    let SpeculativeResult {
        log,
        created_data_sources,
        metrics,
        failed: _,
    } = spec;

    state.enter_handler();
    let mut position = log.poi_start;
    for (i, op) in log.ops.into_iter().enumerate() {
        let written = log
            .poi
            .get(i)
            .is_some_and(|c| proof_of_indexing.write_contribution(region, position, c));
        position += 1;
        if !written {
            proof_of_indexing.write_event(&poi_event(&op), region, logger);
        }
        match op {
            SpeculativeOp::Set {
                key, entity, valid, ..
            } => {
                if let Err(e) = state
                    .entity_cache
                    .set_validated(
                        key,
                        entity,
                        valid,
                        Some(&mut state.write_capacity_remaining),
                    )
                    .await
                {
                    state.exit_handler();
                    return Err(MappingError::Unknown(
                        e.context("replaying a speculative handler run"),
                    ));
                }
            }
            SpeculativeOp::Remove { key, .. } => {
                state.entity_cache.remove(key);
            }
        }
    }
    for ds in created_data_sources {
        state.push_created_data_source(ds);
    }
    state.metrics.extend(metrics);
    state.exit_handler();
    Ok(())
}
