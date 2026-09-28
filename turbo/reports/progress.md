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
| USDC transfers, 790,560 events | 1,004 s | 8.9 s | 8.1 s | ~8 s |
| rETH balances, 7,594 events | 54.5 s | 2.2 s | 1.4 s | ~1.5 s |
| allowances, 19,125 events + 14,998 `eth_call`s at 200 ms | 2,930 s | 3.5 s | 2.8 s | ~3.2 s |
| Safe factories, 82,268 data sources | 276.7 s | 3.2 s | 2.4 s | ~3 s |

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
GRAPH_SPECULATION_THREADS=16
GRAPH_SPECULATION_CONCURRENCY=2000
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
12. **`BufferedCallCache`** — results buffered per block (a correctness fix under concurrency),
    for as many blocks as the runner reads ahead (2,048), so a handler the runner has to run
    again finds its calls answered.
13. **Marshalling** — AssemblyScript objects serialized without a vector per field or element.
14. **Proof of indexing on the speculation threads** — the fast POI hash mixes its leaves in any
    order, so each event's contribution is computed when its run finishes, for the position it
    will take in the block; the runner mixes it in (or hashes normally if the position differs).
15. **Logging** — records no drain would print are dropped before the async drain (it copied
    every `trace!`/`debug!` record); the two per-block info lines are debug while syncing.
16. **Head reads** — the chain head is read at most once a second for skipping empty blocks and
    while the chain has no head yet (it was a query per block).
17. **Datalake prefetch** — ranges in flight continue from each other instead of overlapping
    (overlaps tripled the datalake's work); prefetching starts with the stream's first range;
    five ranges ahead.
18. **Binary `COPY`** for entity inserts of tables with scalar columns (not `BigDecimal`, not
    skip-duplicates tables). `diesel-async` is vendored (`vendor/diesel-async`) to expose its
    `tokio_postgres` client; nothing else in it changed.
19. **Speculation fixes** — the WASM instance pool is at least the speculation concurrency plus
    256 (at 1,024 instances runs failed to start and ran again one at a time); a block taken
    straight from the stream (the first one, or after a stall) is speculated too; runs that
    asked the chain count as useful even when stale, since their answers are cached.

## Where the remaining time goes

- **USDC:** the writer and the runner are both ~80% busy. The writer's `COPY`s take 2.7 s of
  server time for `transfer_event` (the unique `id` index is the largest part) and 2.1 s for
  `poi2$` (its GiST exclusion constraint); the runner replays at ~2 µs per trigger plus ~30 µs
  per block.
- **Allowances:** bounded by the eth_calls' 200 ms latency and how many are in flight
  (`GRAPH_SPECULATION_CONCURRENCY`; 2,000–4,000 here).
- **rETH:** stale runs re-run on the runner (balances change from block to block).
- **Small cases:** ~1 s of the launch time is graph-node start-up (0.6 s migrations and 0.2 s
  cross-shard views on the fresh database), which a long-running node does not pay per
  deployment.

## Next

1. Mutable tables: the `gist (id, block_range)` exclusion constraint costs 25–45 µs per insert
   (`poi2$` is written every block); most of the writer's time on USDC after `COPY`.
2. Runs made stale by earlier blocks (rETH balances) run again on the runner; ~98% of rETH
   transfers.
3. Start-up on a fresh database: migrations 0.6 s, cross-shard views 0.2 s.
4. Tests for the new paths (content store, datalake conversion against JSON-RPC blocks, speculation
   validity rules), and operator docs.
