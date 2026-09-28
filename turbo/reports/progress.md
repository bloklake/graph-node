# graph-node turbo — progress (2026-09-28)

Branch `turbo/main` in `~/Documents/graph-node`. The benchmark subgraphs (`turbo/bench/subgraphs/`)
are unchanged since they were written; every speed-up is in graph-node.

## Results

Median of 3 runs, this machine (CPUs 10–31 for graph-node), a fresh database per run, local
datalake (`bl serve`) and local JSON-RPC caches. "Launch" is process launch → the end block
indexed (includes ~0.8 s of graph-node start-up and database migrations); "deploy" is deploy →
the end block indexed.

| case | stock graph-node (launch) | fork (launch) | fork (deploy) | Ponder fork (launch) |
|---|---|---|---|---|
| USDC transfers, 790,560 events | 1,004 s | 14.6 s | 13.7 s | ~8 s |
| rETH balances, 7,594 events | 54.5 s | 3.2 s | 2.4 s | ~1.5 s |
| allowances, 19,125 events + 14,998 `eth_call`s at 200 ms | 2,930 s | 6.1 s | 5.3 s | ~3.2 s |
| Safe factories, 82,268 data sources | 276.7 s | 5.2 s | 4.4 s | ~3 s |

**Correctness:** each fork run's digest (every entity table plus `data_sources$`, rows alive or
created at or before the end block, in id order; data sources in vid order) equals stock
graph-node's on the same case. One mismatch was found during this work (one `allowance` value
answered for the parent block) and fixed: `BufferedCallCache` assumed one block at a time.

## Fork configuration

```toml
[chains.mainnet]
datalake = { url = "http://127.0.0.1:8084" }   # plus the usual providers
```
```
GRAPH_SPECULATION_THREADS=12
GRAPH_SPECULATION_CONCURRENCY=900
GRAPH_POSTPONE_ATTRIBUTE_INDEX_CREATION=true    # stock setting
```
No `--ipfs`: subgraph files live in Postgres and `graph deploy --ipfs <admin url>` works as is.

## Changes (in commit order)

1. **Built-in content store** — no IPFS node; real CIDv0 ids, checked against a reference
   importer up to three-level DAGs.
2. **Datalake log triggers** — one `/query` per range (logs, joined transactions, headers; Arrow);
   next range prefetched at the size the stream will ask for; static filters for templates;
   blocks the datalake reports as finalized are indexed without waiting for the ingestor.
3. **Ingestor cold start** — the last 250 blocks fetched concurrently.
4. **Entity cache** — no store lookups while the cache holds every entity of a deployment that
   started empty.
5. **Write batches** — weight added incrementally (was quadratic).
6. **One pooled wasmtime engine** for all modules.
7. **Speculative trigger execution** — handlers of up to 1,024 read-ahead blocks run as tasks
   (hundreds can wait on `eth_call`s at once); a run is replayed (same entity cache calls, POI
   events and vid order) only if nothing it read changed; per-handler success rates switch it
   off for handlers whose runs are mostly stale.
8. **Factories** — rescans for new data sources answered from the last scans; module bytes hashed
   once per buffer; data sources inserted 10,000 per statement.
9. **mimalloc**; lock-free AssemblyScript type ids; event signatures hashed once.
10. **Inserts** — scalar-only tables written with one array per column (`unnest`).
11. **Notifications** — batched for 20 ms instead of 500 ms (a new deployment started 0.5 s late).
12. **`BufferedCallCache`** — results buffered per block (a correctness fix under concurrency).

## Where the remaining time goes

- **USDC:** the runner is ~75% busy replaying (≈5 µs per trigger: proof of indexing hash and entity
  cache write) and committing blocks (≈30 µs per block); the writer spends ~7 s inserting; the
  speculative runs cost ~150 µs of CPU per trigger, most of it building the event's
  AssemblyScript objects (a `Vec<u8>` per object, copied into WASM memory) and gas accounting.
- **Small cases:** ~0.8 s of the launch time is graph-node start-up (≈0.5 s of it migrations on the
  fresh database), which a long-running node does not pay per deployment.

## Next

1. Marshalling: write AssemblyScript objects straight into WASM memory; cheaper gas accounting.
2. Replay: skip re-validating entities the speculative run validated; avoid per-event allocations
   in the proof of indexing stream.
3. Writer: binary `COPY` for inserts.
4. Tests for the new paths (content store, datalake conversion against JSON-RPC blocks, speculation
   validity rules), and operator docs.
