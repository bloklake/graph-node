use crate::gas_rules::GasRules;
use crate::module::{ExperimentalFeatures, ToAscPtr, WasmInstance, WasmInstanceData};
use graph::blockchain::{BlockTime, Blockchain, HostFn};
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::data_source::{MappingTrigger, TriggerWithHandler};
use graph::futures01::sync::mpsc;
use graph::futures01::{Future as _, Stream as _};
use graph::futures03::channel::oneshot::Sender;
use graph::parking_lot::RwLock;
use graph::prelude::*;
use graph::runtime::IndexForAscTypeId;
use graph::runtime::gas::Gas;
use parity_wasm::elements::ExportEntry;
use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{panic, thread};

/// Spawn a wasm module in its own thread.
pub fn spawn_module<C: Blockchain>(
    raw_module: &[u8],
    logger: Logger,
    subgraph_id: DeploymentHash,
    host_metrics: Arc<HostMetrics>,
    runtime: tokio::runtime::Handle,
    timeout: Option<Duration>,
    experimental_features: ExperimentalFeatures,
) -> Result<mpsc::Sender<WasmRequest<C>>, anyhow::Error>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    let valid_module = Arc::new(ValidModule::new(&logger, raw_module, timeout)?);
    register_module(raw_module, &valid_module);

    // Create channel for event handling requests
    let (mapping_request_sender, mapping_request_receiver) = mpsc::channel(100);

    // It used to be that we had to create a dedicated thread since wasmtime
    // instances were not `Send` and could therefore not be scheduled by the
    // regular tokio executor. This isn't an issue anymore, but we still
    // spawn a dedicated thread since running WASM code async can block and
    // lock up the executor. See [the wasmtime
    // docs](https://docs.rs/wasmtime/latest/wasmtime/struct.Config.html#execution-in-poll)
    // on how this should be handled properly. As that is a fairly large
    // change to how we use wasmtime, we keep the threading model for now.
    // Once we are confident that things are working that way, we should
    // revisit this and remove the dedicated thread.
    //
    // In case of failure, this thread may panic or simply terminate,
    // dropping the `mapping_request_receiver` which ultimately causes the
    // subgraph to fail the next time it tries to handle an event.
    let next_id = THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
    let conf = thread::Builder::new().name(format!("mapping-{}-{:0>4}", &subgraph_id, next_id));
    conf.spawn(move || {
        let _runtime_guard = runtime.enter();

        // Pass incoming triggers to the WASM module and return entity changes;
        // Stop when canceled because all RuntimeHosts and their senders were dropped.
        match mapping_request_receiver
            .map_err(|()| unreachable!())
            .for_each(move |request| {
                let WasmRequest {
                    ctx,
                    inner,
                    result_sender,
                } = request;
                let logger = ctx.logger.clone();

                let handle_fut = async {
                    let result = instantiate_module::<C>(
                        valid_module.cheap_clone(),
                        ctx,
                        host_metrics.cheap_clone(),
                        experimental_features,
                    )
                    .await;
                    match result {
                        Ok(module) => match inner {
                            WasmRequestInner::TriggerRequest(trigger) => {
                                handle_trigger(&logger, module, trigger, host_metrics.cheap_clone())
                                    .await
                            }
                        },
                        Err(e) => Err(MappingError::Unknown(e)),
                    }
                };
                let result = panic::catch_unwind(AssertUnwindSafe(|| graph::block_on(handle_fut)));

                let result = match result {
                    Ok(result) => result,
                    Err(panic_info) => {
                        let err_msg = if let Some(payload) = panic_info
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or(panic_info.downcast_ref::<&str>().copied())
                        {
                            anyhow!("Subgraph panicked with message: {}", payload)
                        } else {
                            anyhow!("Subgraph panicked with an unknown payload.")
                        };
                        Err(MappingError::Unknown(err_msg))
                    }
                };

                result_sender
                    .send(result)
                    .map_err(|_| anyhow::anyhow!("WASM module result receiver dropped."))
            })
            .wait()
        {
            Ok(()) => debug!(logger, "Subgraph stopped, WASM runtime thread terminated"),
            Err(e) => debug!(logger, "WASM runtime thread terminated abnormally";
                                    "error" => e.to_string()),
        }
    })
    .map(|_| ())
    .context("Spawning WASM runtime thread failed")?;

    Ok(mapping_request_sender)
}

/// Compiled modules by the keccak hash of their bytes, so every host running the same module
/// finds it for speculative runs. `buffers` remembers the hash of each module buffer hosts were
/// built from (holding the buffer, so its address is not reused for other bytes).
struct ModuleRegistry {
    by_hash: HashMap<[u8; 32], std::sync::Weak<ValidModule>>,
    buffers: HashMap<usize, (Arc<Vec<u8>>, [u8; 32])>,
}

static MODULES: std::sync::LazyLock<std::sync::Mutex<ModuleRegistry>> =
    std::sync::LazyLock::new(|| {
        std::sync::Mutex::new(ModuleRegistry {
            by_hash: HashMap::new(),
            buffers: HashMap::new(),
        })
    });

fn module_hash(raw_module: &[u8]) -> [u8; 32] {
    graph::prelude::alloy::primitives::keccak256(raw_module).0
}

fn register_module(raw_module: &[u8], module: &Arc<ValidModule>) {
    let hash = module_hash(raw_module);
    let mut modules = MODULES.lock().unwrap();
    modules
        .by_hash
        .retain(|_, module| module.strong_count() > 0);
    modules.by_hash.insert(hash, Arc::downgrade(module));
}

/// The module compiled from these bytes, if it is still alive.
pub(crate) fn registered_module(raw_module: &Arc<Vec<u8>>) -> Option<Arc<ValidModule>> {
    let address = Arc::as_ptr(raw_module) as usize;
    let known = MODULES
        .lock()
        .unwrap()
        .buffers
        .get(&address)
        .map(|(_, hash)| *hash);
    let hash = match known {
        Some(hash) => hash,
        None => {
            let hash = module_hash(raw_module);
            let mut modules = MODULES.lock().unwrap();
            // Keep the table from growing without bound as data sources come and go.
            if modules.buffers.len() > 10_000 {
                modules.buffers.clear();
            }
            modules
                .buffers
                .insert(address, (raw_module.cheap_clone(), hash));
            hash
        }
    };
    MODULES
        .lock()
        .unwrap()
        .by_hash
        .get(&hash)
        .and_then(std::sync::Weak::upgrade)
}

/// The runtime speculative handler runs execute on (`GRAPH_SPECULATION_THREADS` workers), and
/// the limit on how many are in flight (`GRAPH_SPECULATION_CONCURRENCY`). Runs are tasks, not
/// threads: a run waiting on a host call (an `eth_call`, say) yields its worker, so many can
/// wait at once while only a few execute. `None` when speculation is off.
fn speculation_runtime() -> Option<&'static (tokio::runtime::Runtime, Arc<tokio::sync::Semaphore>)>
{
    static RUNTIME: std::sync::OnceLock<
        Option<(tokio::runtime::Runtime, Arc<tokio::sync::Semaphore>)>,
    > = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            let threads = ENV_VARS.mappings.speculation_threads;
            if threads == 0 {
                return None;
            }
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .thread_name("speculation")
                .enable_all()
                .build()
                .expect("building the speculation runtime");
            let permits = ENV_VARS.mappings.speculation_concurrency.max(1);
            Some((runtime, Arc::new(tokio::sync::Semaphore::new(permits))))
        })
        .as_ref()
}

/// Spawns `run` on the speculation runtime; `false` when speculation is off. Runs take a
/// `speculation_permit` before they instantiate a module.
pub fn spawn_speculative<F>(run: F) -> bool
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let Some((runtime, _)) = speculation_runtime() else {
        return false;
    };
    runtime.spawn(run);
    true
}

/// Whether speculative runs are on.
pub fn speculation_enabled() -> bool {
    speculation_runtime().is_some()
}

/// Waits for one of the `GRAPH_SPECULATION_CONCURRENCY` slots for a run in flight.
pub(crate) async fn speculation_permit() -> Option<tokio::sync::OwnedSemaphorePermit> {
    let (_, permits) = speculation_runtime()?;
    permits.clone().acquire_owned().await.ok()
}

/// Instantiates the module and runs `trigger`, as a speculative run does.
pub(crate) async fn run_speculative<C: Blockchain>(
    valid_module: Arc<ValidModule>,
    ctx: MappingContext,
    trigger: TriggerWithHandler<MappingTrigger<C>>,
    host_metrics: Arc<HostMetrics>,
    experimental_features: ExperimentalFeatures,
) -> Result<BlockState, MappingError>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    let run = async move {
        let module = WasmInstance::from_valid_module_with_ctx(
            valid_module,
            ctx,
            host_metrics,
            experimental_features,
        )
        .await
        .map_err(MappingError::Unknown)?;
        module
            .handle_trigger(trigger)
            .await
            .map(|(state, _gas)| state)
    };
    graph::futures03::FutureExt::catch_unwind(AssertUnwindSafe(run))
        .await
        .unwrap_or_else(|_| {
            Err(MappingError::Unknown(anyhow!(
                "speculative handler run panicked"
            )))
        })
}

async fn instantiate_module<C: Blockchain>(
    valid_module: Arc<ValidModule>,
    ctx: MappingContext,
    host_metrics: Arc<HostMetrics>,
    experimental_features: ExperimentalFeatures,
) -> Result<WasmInstance, anyhow::Error>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    // Start the WASM module runtime.
    let _section = host_metrics.stopwatch.start_section("module_init");
    WasmInstance::from_valid_module_with_ctx(
        valid_module,
        ctx,
        host_metrics.cheap_clone(),
        experimental_features,
    )
    .await
    .context("module instantiation failed")
}

async fn handle_trigger<C: Blockchain>(
    logger: &Logger,
    module: WasmInstance,
    trigger: TriggerWithHandler<MappingTrigger<C>>,
    host_metrics: Arc<HostMetrics>,
) -> Result<(BlockState, Gas), MappingError>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    let logger = logger.cheap_clone();

    let _section = host_metrics.stopwatch.start_section("run_handler");
    if ENV_VARS.log_trigger_data {
        debug!(logger, "trigger data: {:?}", trigger);
    }
    module.handle_trigger(trigger).await
}

pub struct WasmRequest<C: Blockchain> {
    pub(crate) ctx: MappingContext,
    pub(crate) inner: WasmRequestInner<C>,
    pub(crate) result_sender: Sender<Result<(BlockState, Gas), MappingError>>,
}

impl<C: Blockchain> WasmRequest<C> {
    pub(crate) fn new_trigger(
        ctx: MappingContext,
        trigger: TriggerWithHandler<MappingTrigger<C>>,
        result_sender: Sender<Result<(BlockState, Gas), MappingError>>,
    ) -> Self {
        WasmRequest {
            ctx,
            inner: WasmRequestInner::TriggerRequest(trigger),
            result_sender,
        }
    }
}

pub enum WasmRequestInner<C: Blockchain> {
    TriggerRequest(TriggerWithHandler<MappingTrigger<C>>),
}

pub struct MappingContext {
    pub logger: Logger,
    pub host_exports: Arc<crate::host_exports::HostExports>,
    pub block_ptr: BlockPtr,
    pub timestamp: BlockTime,
    pub state: BlockState,
    pub proof_of_indexing: SharedProofOfIndexing,
    pub host_fns: Arc<Vec<HostFn>>,
    pub debug_fork: Option<Arc<dyn SubgraphFork>>,
    /// Logger for messages coming from mappings
    pub mapping_logger: Logger,
    /// Whether to log details about host fn execution
    pub instrument: bool,
}

impl MappingContext {
    pub fn derive_with_empty_block_state(&self) -> Self {
        MappingContext {
            logger: self.logger.cheap_clone(),
            host_exports: self.host_exports.cheap_clone(),
            block_ptr: self.block_ptr.cheap_clone(),
            timestamp: self.timestamp,
            state: BlockState::new(
                self.state.entity_cache.store.clone(),
                Default::default(),
                self.state.seq_gen(),
            ),
            proof_of_indexing: self.proof_of_indexing.cheap_clone(),
            host_fns: self.host_fns.cheap_clone(),
            debug_fork: self.debug_fork.cheap_clone(),
            mapping_logger: Logger::new(&self.logger, o!("component" => "UserMapping")),
            instrument: self.instrument,
        }
    }
}

// See the start_index comment below for more information.
const GN_START_FUNCTION_NAME: &str = "gn::start";

/// Type id indexes below this are cached in a lock-free table.
const ASC_TYPE_ID_SLOTS: usize = 8192;
/// Marks a slot of that table whose type id is not known yet.
const UNKNOWN_TYPE_ID: u32 = u32::MAX;

/// The engine every module is compiled for. Sharing it lets all instances come from one
/// pool (`GRAPH_WASM_INSTANCE_POOL`), so a trigger's instance reuses memory instead of mapping
/// and unmapping a fresh reservation.
fn shared_engine() -> Result<wasmtime::Engine, anyhow::Error> {
    static ENGINE: std::sync::OnceLock<Result<wasmtime::Engine, String>> =
        std::sync::OnceLock::new();
    ENGINE
        .get_or_init(|| build_engine().map_err(|e| format!("{e:#}")))
        .clone()
        .map_err(|e| anyhow!("creating the WASM engine failed: {e}"))
}

fn build_engine() -> Result<wasmtime::Engine, anyhow::Error> {
    // We use Cranelift as a compilation engine. Cranelift is an optimizing compiler, but that
    // should not cause determinism issues since it adheres to the Wasm spec and NaN
    // canonicalization is enabled below. The optimization level is configurable via
    // GRAPH_WASM_OPT_LEVEL (default: speed).
    let mut config = wasmtime::Config::new();
    config.strategy(wasmtime::Strategy::Cranelift);
    config.epoch_interruption(true);
    config.cranelift_nan_canonicalization(true); // For NaN determinism.
    config.cranelift_opt_level(match ENV_VARS.mappings.wasm_opt_level {
        graph::env::WasmOptLevel::None => wasmtime::OptLevel::None,
        graph::env::WasmOptLevel::Speed => wasmtime::OptLevel::Speed,
        graph::env::WasmOptLevel::SpeedAndSize => wasmtime::OptLevel::SpeedAndSize,
    });
    config.max_wasm_stack(ENV_VARS.mappings.max_stack_size);

    let slots = ENV_VARS.mappings.wasm_instance_pool;
    if slots > 0 {
        let mut pool = wasmtime::PoolingAllocationConfig::default();
        pool.total_core_instances(slots)
            .total_memories(slots)
            .total_tables(slots)
            .total_stacks(slots)
            // Mappings keep their state small; zeroing the first MiB on reuse is cheaper
            // than faulting the pages back in.
            .linear_memory_keep_resident(1 << 20)
            .table_keep_resident(1 << 16)
            .table_elements(1 << 20)
            .max_core_instance_size(1 << 22);
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pool));
    }
    Ok(wasmtime::Engine::new(&config)?)
}

/// A pre-processed and valid WASM module, ready to be started as a WasmModule.
pub struct ValidModule {
    pub module: wasmtime::Module,

    /// Pre-linked instance template. Created once at module validation time and reused for every
    /// trigger instantiation, avoiding the cost of rebuilding the linker (~60 host function
    /// registrations) and resolving imports on each trigger.
    pub instance_pre: wasmtime::InstancePre<WasmInstanceData>,

    // Due to our internal architecture we don't want to run the start function at instantiation time,
    // so we track it separately so that we can run it at an appropriate time.
    // Since the start function is not an export, we will also create an export for it.
    // It's an option because start might not be present.
    pub start_function: Option<String>,

    // A wasm import consists of a `module` and a `name`. AS will generate imports such that they
    // have `module` set to the name of the file it is imported from and `name` set to the imported
    // function name or `namespace.function` if inside a namespace. We'd rather not specify names of
    // source files, so we consider that the import `name` uniquely identifies an import. Still we
    // need to know the `module` to properly link it, so here we map import names to modules.
    //
    // AS now has an `@external("module", "name")` decorator which would make things cleaner, but
    // the ship has sailed.
    pub import_name_to_modules: BTreeMap<String, Vec<String>>,

    // The timeout for the module.
    pub timeout: Option<Duration>,

    // Used as a guard to terminate this task dependency.
    epoch_counter_abort_handle: Option<tokio::task::AbortHandle>,

    /// Cache for asc_type_id results. Maps IndexForAscTypeId to their WASM runtime
    /// type IDs. Populated lazily on first use; deterministic per compiled module.
    asc_type_id_cache: RwLock<HashMap<IndexForAscTypeId, u32>>,
    /// Type ids for the (dense, small) indexes of real types, readable without a lock;
    /// every speculation thread looks them up for every object it creates.
    asc_type_ids: Box<[std::sync::atomic::AtomicU32]>,
}

impl ValidModule {
    /// Pre-process and validate the module.
    pub fn new(
        logger: &Logger,
        raw_module: &[u8],
        timeout: Option<Duration>,
    ) -> Result<Self, anyhow::Error> {
        // Add the gas calls here. Module name "gas" must match. See also
        // e3f03e62-40e4-4f8c-b4a1-d0375cca0b76. We do this by round-tripping the module through
        // parity - injecting gas then serializing again.
        let parity_module = parity_wasm::elements::Module::from_bytes(raw_module)?;
        let mut parity_module = match parity_module.parse_names() {
            Ok(module) => module,
            Err((errs, module)) => {
                for (index, err) in errs {
                    warn!(
                        logger,
                        "unable to parse function name for index {}: {}",
                        index,
                        err.to_string()
                    );
                }

                module
            }
        };

        let start_function = parity_module.start_section().map(|index| {
            let name = GN_START_FUNCTION_NAME.to_string();

            parity_module.clear_start_section();
            parity_module
                .export_section_mut()
                .unwrap()
                .entries_mut()
                .push(ExportEntry::new(
                    name.clone(),
                    parity_wasm::elements::Internal::Function(index),
                ));

            name
        });
        let backend = wasm_instrument::gas_metering::host_function::Injector::new("gas", "gas");
        let parity_module =
            wasm_instrument::gas_metering::inject(parity_module, backend, &GasRules)
                .map_err(|_| anyhow!("Failed to inject gas counter"))?;
        let raw_module = parity_module.into_bytes()?;

        let engine = &shared_engine()?;
        let module = wasmtime::Module::from_binary(engine, &raw_module)?;

        let mut import_name_to_modules: BTreeMap<String, Vec<String>> = BTreeMap::new();

        // Unwrap: Module linking is disabled.
        for (name, module) in module
            .imports()
            .map(|import| (import.name(), import.module()))
        {
            import_name_to_modules
                .entry(name.to_string())
                .or_default()
                .push(module.to_string());
        }

        // Timeouts on instantiated modules trigger on epoch deltas; one task advances the
        // shared engine's epoch every `timeout` (the timeout is the same for every module).
        // See also: runtime-timeouts
        let epoch_counter_abort_handle = None;
        if let Some(timeout) = timeout {
            static EPOCH_COUNTER: std::sync::Once = std::sync::Once::new();
            let engine = engine.clone();
            EPOCH_COUNTER.call_once(move || {
                graph::spawn(async move {
                    loop {
                        tokio::time::sleep(timeout).await;
                        engine.increment_epoch();
                    }
                });
            });
        }

        let linker = crate::module::build_linker(engine, &import_name_to_modules)?;
        let instance_pre = linker.instantiate_pre(&module)?;

        Ok(ValidModule {
            module,
            instance_pre,
            import_name_to_modules,
            start_function,
            timeout,
            epoch_counter_abort_handle,
            asc_type_id_cache: RwLock::new(HashMap::new()),
            asc_type_ids: (0..ASC_TYPE_ID_SLOTS)
                .map(|_| std::sync::atomic::AtomicU32::new(UNKNOWN_TYPE_ID))
                .collect(),
        })
    }

    pub fn get_cached_type_id(&self, idx: IndexForAscTypeId) -> Option<u32> {
        if let Some(slot) = self.asc_type_ids.get(idx as usize) {
            let id = slot.load(Ordering::Relaxed);
            return (id != UNKNOWN_TYPE_ID).then_some(id);
        }
        self.asc_type_id_cache.read().get(&idx).copied()
    }

    pub fn cache_type_id(&self, idx: IndexForAscTypeId, type_id: u32) {
        match self.asc_type_ids.get(idx as usize) {
            Some(slot) if type_id != UNKNOWN_TYPE_ID => slot.store(type_id, Ordering::Relaxed),
            _ => {
                self.asc_type_id_cache.write().insert(idx, type_id);
            }
        }
    }
}

impl Drop for ValidModule {
    fn drop(&mut self) {
        if let Some(handle) = self.epoch_counter_abort_handle.take() {
            handle.abort();
        }
    }
}
