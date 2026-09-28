use crate::{
    blockchain::{Blockchain, DataSourceTemplate as _},
    components::{
        metrics::block_state::BlockStateMetrics,
        store::{EntityLfuCache, ReadStore, SeqGenerator, StoredDynamicDataSource},
    },
    data::subgraph::schema::SubgraphError,
    data_source::{DataSourceTemplate, DataSourceTemplateInfo},
    prelude::*,
    schema::{EntityKey, EntityType},
};

#[derive(Debug, Clone)]
pub enum InstanceDSTemplate {
    Onchain(DataSourceTemplateInfo),
    Offchain(crate::data_source::offchain::DataSourceTemplate),
}

impl<C: Blockchain> From<&DataSourceTemplate<C>> for InstanceDSTemplate {
    fn from(value: &crate::data_source::DataSourceTemplate<C>) -> Self {
        match value {
            DataSourceTemplate::Onchain(ds) => Self::Onchain(ds.info()),
            DataSourceTemplate::Offchain(ds) => Self::Offchain(ds.clone()),
            DataSourceTemplate::Subgraph(_) => todo!(), // TODO(krishna)
        }
    }
}

impl InstanceDSTemplate {
    pub fn name(&self) -> &str {
        match self {
            Self::Onchain(ds) => &ds.name,
            Self::Offchain(ds) => &ds.name,
        }
    }

    pub fn is_onchain(&self) -> bool {
        match self {
            Self::Onchain(_) => true,
            Self::Offchain(_) => false,
        }
    }

    pub fn into_onchain(self) -> Option<DataSourceTemplateInfo> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
        }
    }

    pub fn manifest_idx(&self) -> Option<u32> {
        match self {
            InstanceDSTemplate::Onchain(info) => info.manifest_idx,
            InstanceDSTemplate::Offchain(info) => Some(info.manifest_idx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InstanceDSTemplateInfo {
    pub template: InstanceDSTemplate,
    pub params: Vec<String>,
    pub context: Option<DataSourceContext>,
    pub creation_block: BlockNumber,
}

/// A store write a speculatively executed handler made, in the order it made them.
#[derive(Debug)]
pub enum SpeculativeOp {
    /// `store.set`: the key and the entity as the handler passed it (no vid yet).
    Set { key: EntityKey, entity: Entity },
    /// `store.remove`: the key, and the type and id strings the proof of indexing records.
    Remove {
        key: EntityKey,
        poi_type: String,
        poi_id: String,
    },
}

/// What a handler run ahead of time on a speculation thread read and wrote. The runner replays
/// `ops` when none of what the handler read has changed since the speculation's snapshot, and
/// runs the handler normally otherwise.
#[derive(Debug, Default)]
pub struct SpeculationLog {
    pub ops: Vec<SpeculativeOp>,
    /// Keys the handler read (from the store or from the block's own writes), and keys whose
    /// write needed the stored entity to validate.
    pub reads: Vec<EntityKey>,
    /// Entity types a derived-field lookup (`loadRelated`) scanned.
    pub related_types: Vec<EntityType>,
    /// The handler did something that cannot be replayed (generated an id, `ipfs.map`).
    pub unsupported: bool,
}

#[derive(Debug)]
pub struct BlockState {
    pub entity_cache: EntityCache,
    pub deterministic_errors: Vec<SubgraphError>,
    created_data_sources: Vec<InstanceDSTemplateInfo>,

    // Data sources to be transacted into the store.
    pub persisted_data_sources: Vec<StoredDynamicDataSource>,

    // Data sources created in the current handler.
    handler_created_data_sources: Vec<InstanceDSTemplateInfo>,

    // data source that have been processed.
    pub processed_data_sources: Vec<StoredDynamicDataSource>,

    // Marks whether a handler is currently executing.
    in_handler: bool,

    pub metrics: BlockStateMetrics,

    pub write_capacity_remaining: usize,

    /// Set on the state of a speculatively executed handler: records what it read and wrote.
    pub speculation: Option<SpeculationLog>,
}

impl BlockState {
    pub fn new(store: impl ReadStore, lfu_cache: EntityLfuCache, vid_gen: SeqGenerator) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(Arc::new(store), lfu_cache, vid_gen),
            deterministic_errors: Vec::new(),
            created_data_sources: Vec::new(),
            persisted_data_sources: Vec::new(),
            handler_created_data_sources: Vec::new(),
            processed_data_sources: Vec::new(),
            in_handler: false,
            metrics: BlockStateMetrics::new(),
            write_capacity_remaining: ENV_VARS.block_write_capacity,
            speculation: None,
        }
    }

    pub fn seq_gen(&self) -> SeqGenerator {
        self.entity_cache.seq_gen()
    }
}

impl BlockState {
    pub fn extend(&mut self, other: BlockState) {
        assert!(!other.in_handler);

        let BlockState {
            entity_cache,
            deterministic_errors,
            created_data_sources,
            persisted_data_sources,
            handler_created_data_sources,
            processed_data_sources,
            in_handler,
            metrics,
            write_capacity_remaining,
            speculation: _,
        } = self;

        match in_handler {
            true => handler_created_data_sources.extend(other.created_data_sources),
            false => created_data_sources.extend(other.created_data_sources),
        }
        deterministic_errors.extend(other.deterministic_errors);
        entity_cache.extend(other.entity_cache);
        processed_data_sources.extend(other.processed_data_sources);
        persisted_data_sources.extend(other.persisted_data_sources);
        metrics.extend(other.metrics);
        *write_capacity_remaining =
            write_capacity_remaining.saturating_sub(other.write_capacity_remaining);
    }

    pub fn has_created_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        !self.created_data_sources.is_empty()
    }

    pub fn has_created_on_chain_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        self.created_data_sources
            .iter()
            .any(|ds| matches!(ds.template, InstanceDSTemplate::Onchain(_)))
    }

    pub fn drain_created_data_sources(&mut self) -> Vec<InstanceDSTemplateInfo> {
        assert!(!self.in_handler);
        std::mem::take(&mut self.created_data_sources)
    }

    pub fn enter_handler(&mut self) {
        assert!(!self.in_handler);
        self.in_handler = true;
        self.entity_cache.enter_handler()
    }

    pub fn exit_handler(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.created_data_sources
            .append(&mut self.handler_created_data_sources);
        self.entity_cache.exit_handler()
    }

    pub fn exit_handler_and_discard_changes_due_to_error(&mut self, e: SubgraphError) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.handler_created_data_sources.clear();
        self.entity_cache.exit_handler_and_discard_changes();
        self.deterministic_errors.push(e);
    }

    pub fn push_created_data_source(&mut self, ds: InstanceDSTemplateInfo) {
        assert!(self.in_handler);
        self.handler_created_data_sources.push(ds);
    }

    pub fn persist_data_source(&mut self, ds: StoredDynamicDataSource) {
        self.persisted_data_sources.push(ds)
    }

    /// Create a lightweight checkpoint for rollback.
    ///
    /// This captures the current counts of created and persisted data sources,
    /// allowing a partial rollback via `restore()`. Note that entity cache changes
    /// cannot be easily checkpointed; rollback clears the cache (acceptable per
    /// current behavior).
    pub fn checkpoint(&self) -> BlockStateCheckpoint {
        assert!(!self.in_handler);
        BlockStateCheckpoint {
            created_data_sources_count: self.created_data_sources.len(),
            persisted_data_sources_count: self.persisted_data_sources.len(),
            processed_data_sources_count: self.processed_data_sources.len(),
            deterministic_errors_count: self.deterministic_errors.len(),
        }
    }

    /// Restore state to a previously captured checkpoint (partial rollback).
    ///
    /// This truncates the data source vectors to their checkpoint sizes.
    /// Entity cache is NOT restored - caller should handle cache state if needed.
    pub fn restore(&mut self, checkpoint: BlockStateCheckpoint) {
        assert!(!self.in_handler);
        self.created_data_sources
            .truncate(checkpoint.created_data_sources_count);
        self.persisted_data_sources
            .truncate(checkpoint.persisted_data_sources_count);
        self.processed_data_sources
            .truncate(checkpoint.processed_data_sources_count);
        self.deterministic_errors
            .truncate(checkpoint.deterministic_errors_count);
    }
}

/// A lightweight checkpoint for `BlockState` rollback.
///
/// Captures counts of mutable vectors in `BlockState` to enable partial rollback.
/// Used before processing dynamic data sources to allow recovery if needed.
#[derive(Debug, Clone, Copy)]
pub struct BlockStateCheckpoint {
    created_data_sources_count: usize,
    persisted_data_sources_count: usize,
    processed_data_sources_count: usize,
    deterministic_errors_count: usize,
}
