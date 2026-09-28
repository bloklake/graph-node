use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use graph::{
    cheap_clone::CheapClone,
    components::store::EthereumCallCache,
    data::store::ethereum::call,
    prelude::{BlockPtr, CachedEthereumCall},
    slog::{Logger, error},
};

/// A wrapper around an Ethereum call cache that buffers call results in
/// memory. Results are kept per block for the most recent blocks, so
/// handlers of different blocks running at the same time (speculative
/// execution) never see each other's results.
pub struct BufferedCallCache {
    call_cache: Arc<dyn EthereumCallCache>,
    buffer: Arc<Mutex<BlockBuffers>>,
}

/// Blocks whose call results stay buffered. This covers the blocks the runner reads ahead of
/// the one it processes (1,024), whose handlers run speculatively: when the runner has to run
/// one of them again (an entity it read changed), its calls are answered from here.
const BUFFERED_BLOCKS: usize = 2048;

#[derive(Default)]
struct BlockBuffers {
    blocks: HashMap<BlockPtr, HashMap<call::Request, call::Retval>>,
}

impl BlockBuffers {
    fn block(&mut self, block: &BlockPtr) -> &mut HashMap<call::Request, call::Retval> {
        if !self.blocks.contains_key(block) && self.blocks.len() >= BUFFERED_BLOCKS {
            // Drop the oldest block's results.
            if let Some(oldest) = self.blocks.keys().min_by_key(|ptr| ptr.number).cloned() {
                self.blocks.remove(&oldest);
            }
        }
        self.blocks.entry(block.clone()).or_default()
    }
}

impl BufferedCallCache {
    pub fn new(call_cache: Arc<dyn EthereumCallCache>) -> Self {
        Self {
            call_cache,
            buffer: Arc::new(Mutex::new(BlockBuffers::default())),
        }
    }

    fn get(&self, call: &call::Request, block: &BlockPtr) -> Option<call::Response> {
        let buffer = self.buffer.lock().unwrap();
        buffer.blocks.get(block)?.get(call).map(|retval| {
            call.cheap_clone()
                .response(retval.clone(), call::Source::Memory)
        })
    }

    fn insert(&self, call: call::Request, block: &BlockPtr, retval: call::Retval) {
        self.buffer
            .lock()
            .unwrap()
            .block(block)
            .insert(call, retval);
    }
}

#[async_trait]
impl EthereumCallCache for BufferedCallCache {
    async fn get_call(
        &self,
        call: &call::Request,
        block: BlockPtr,
    ) -> Result<Option<call::Response>, graph::prelude::Error> {
        if let Some(value) = self.get(call, &block) {
            return Ok(Some(value));
        }

        let result = self.call_cache.get_call(call, block.clone()).await?;

        if let Some(call::Response {
            retval,
            req: _,
            source: _,
        }) = &result
        {
            self.insert(call.cheap_clone(), &block, retval.clone());
        }
        Ok(result)
    }

    async fn get_calls(
        &self,
        reqs: &[call::Request],
        block: BlockPtr,
    ) -> Result<(Vec<call::Response>, Vec<call::Request>), graph::prelude::Error> {
        let mut missing = Vec::new();
        let mut resps = Vec::new();

        for call in reqs {
            match self.get(call, &block) {
                Some(resp) => resps.push(resp),
                None => missing.push(call.cheap_clone()),
            }
        }

        let (stored, calls) = self.call_cache.get_calls(&missing, block.clone()).await?;

        for resp in &stored {
            self.insert(resp.req.cheap_clone(), &block, resp.retval.clone());
        }

        resps.extend(stored);
        Ok((resps, calls))
    }

    async fn get_calls_in_block(
        &self,
        block: BlockPtr,
    ) -> Result<Vec<CachedEthereumCall>, graph::prelude::Error> {
        self.call_cache.get_calls_in_block(block).await
    }

    async fn set_call(
        self: Arc<Self>,
        logger: &Logger,
        call: call::Request,
        block: BlockPtr,
        return_value: call::Retval,
    ) -> Result<(), graph::prelude::Error> {
        // Enter the call into the in-memory cache immediately so that
        // handlers will find it, but add it to the underlying cache in the
        // background so we do not have to wait for that as it will be a
        // cache backed by the database
        self.insert(call.cheap_clone(), &block, return_value.clone());

        let cache = self.call_cache.cheap_clone();
        let logger = logger.cheap_clone();
        if let Err(e) = cache
            .set_call(&logger, call.cheap_clone(), block, return_value)
            .await
        {
            error!(logger, "BufferedCallCache: call cache set error";
                            "contract_address" => format!("{:?}", call.address),
                            "error" => e.to_string())
        }

        Ok(())
    }
}
