# graph-node turbo — plan

Goal: the same subgraph, unchanged, indexes as fast on this fork as the Ponder fork does on the
same cases (reference numbers below). graph-node also has to run on its own: no IPFS node.

## Constraints

- **Subgraphs are untouched.** Manifest, schema and mappings stay exactly as a developer wrote
  them. Every speed-up lives in graph-node. Operators may change graph-node's config (TOML,
  env).
- **The datalake is used as it is.** It is the existing `bl serve` HTTP API (`POST /query`, Arrow
  IPC, `/v1/sets`, `GET /status`); nothing is added on the datalake side.
- **Output is identical.** Entity tables (and the proof of indexing, where it is defined) must
  match stock graph-node over JSON-RPC. Every benchmark run is checked by a digest.
- **Opt-in.** Without the new config the fork behaves like upstream.

## Reference numbers (Ponder fork + local datalake, 2026-09-27, process launch → indexing done)

| case | range | work | target |
|---|---|---|---|
| USDC transfers, insert-only | 18,600,000–18,699,999 | 790,560 events | ~8 s |
| rETH balances, read-modify-write | 18,600,000–18,699,999 | 7,594 events, 1,747 accounts | ~1.5 s |
| allowances + `allowance()` calls | 25,600,000–25,601,199 | 19,125 events, 14,998 calls at 200 ms | ~3.2 s |
| Safe factories | 24,600,000–24,630,000 | 82,268 children, 84,981 events | ~3 s |

## Where stock graph-node spends time (from reading the code)

1. **Trigger scanning.** The polling stream fetches one block range at a time, sized to about
   100 triggers (`target_triggers_per_block_range`), with `eth_getLogs` and then one
   `eth_getBlockByHash` (full transactions) per block that has a trigger, capped at 10
   concurrent (`chain/ethereum/src/ethereum_adapter.rs:1715`). For USDC that is roughly 8,000
   round-trip rounds.
2. **eth_call.** `ethereum.call` inside a handler waits for the answer, one call at a time.
   Only calls declared in the manifest are prefetched, and only for the one trigger about to run.
3. **Per trigger.** A new WASM instance per trigger (`runtime/wasm/src/mapping.rs:125`), entity
   reads through the cache with a Postgres query per miss, and proof-of-indexing hashing.
4. **Per block.** Block pointer and cache bookkeeping, the write batch and its queue, and
   `INSERT` statements that are limited to 65,535 parameters.
5. **IPFS.** The manifest, schema, WASM and ABIs are fetched by content id from an IPFS
   gateway/RPC. At startup a `HEAD /ipfs/<cid>` probe must succeed.

## Phases (commits on the fork branch `turbo/main`; reports in `turbo/reports/`)

### G0 — baseline and harness
- Build stock graph-node (release). Run a dedicated Postgres container (`gn-bench-pg`, C
  locale, `pg_trgm`, `btree_gist`, `postgres_fdw`).
- Port the four cases to ordinary subgraphs in `turbo/bench/subgraphs/`, written the way
  `graph init` scaffolds them. They are frozen after G0; no later phase may edit them.
- Harness `turbo/bench/run.py`:
  - starts graph-node, deploys, and waits until the deployment reaches the end block;
  - times process launch → synced;
  - dumps the entity tables to a canonical digest;
  - cross-checks the rows against the Ponder apps' tables.
- Baseline: stock graph-node over the local JSON-RPC caches, default config, then the best
  config-only tuning.

### G1 — standalone (no IPFS node)
- graph-node stores subgraph files itself. The admin server (`:8020`) also answers
  `POST /api/v0/add`, `GET/HEAD /ipfs/<cid>` and `POST /api/v0/cat`. Files are kept in
  Postgres (`subgraphs.content`) and served to the link resolver directly.
- Ids are real CIDv0 (UnixFS, 256 KiB chunks, balanced DAG, like the IPFS reference
  implementation's defaults), so a deployment id matches the one any IPFS-based setup
  computes.
- Enabled when `--ipfs` is not given, or with `--ipfs builtin`. The stock CLI works unchanged:
  `graph deploy --node http://gn:8020 --ipfs http://gn:8020`.
- `ipfs.cat` and file data sources read the same store: available if the content was added,
  otherwise treated as not found (the same as an unreachable gateway).

### G2 — datalake trigger source
- Configured per chain: `[chains.<name>] datalake = { url = "...", token = "..." }`.
- Below the datalake's `archive_height`, and at least the reorg threshold behind the chain
  head, `blocks_with_triggers` becomes one `/query` per range:
  - log filters come from the trigger filter;
  - large address lists (dynamic data sources) go through `/v1/sets`;
  - `joinTransactions`, and the block fields the handlers can see;
  - ranges of 10k–100k blocks, paged by `maxObjects`;
  - Arrow is decoded straight into `LightEthereumBlock` + triggers.
- Falls back to JSON-RPC for:
  - call handlers (traces);
  - `receipt: true` handlers when the receipt's logs are needed;
  - anything above the datalake height;
  - the datalake being unreachable or behind.
- Blocks built from the datalake carry only the matched transactions. They are never written
  to the chain store's block cache, so the JSON-RPC path never sees a partial block.
- The next ranges are fetched while the current one is processed (a deeper pipeline than the
  100-event buffer).

### G3 — contract calls
- JSON-RPC batching for `eth_call`, with a concurrency window.
- **Learned call prediction** (automatic declared calls). graph-node records each
  `ethereum.call` a handler makes, together with its arguments. When the address and arguments
  are the event's address or one of its parameters, that call becomes a synthetic declared call
  for the handler. Upcoming triggers' calls are then prefetched in parallel, many blocks ahead,
  into the call cache the handler reads.
  - A wrong prediction only costs a wasted call.
  - The handler always gets a real answer, so results cannot change.

### G4 — execution and store
- Profile the USDC case first; decide from the profile.
- Candidates:
  - reuse WASM instances across triggers of one block (reset memory) instead of creating one per
    trigger;
  - `COPY` for bulk inserts;
  - larger write batches during catch-up;
  - cheaper POI bookkeeping where it is not enabled;
  - fewer per-block commits during backfill.

### G5 — hardening and docs
- Reorgs across the datalake/JSON-RPC boundary, the datalake failure modes (a fault proxy like
  the Ponder fork's), restarts mid-range, grafting, and dynamic data sources created inside a
  datalake range.
- Operator docs and config reference.

## Measurement rules

- Same machine, CPUs 8–9 offline, `taskset -c 0-7,10-31`, warm datalake, local RPC caches.
- Each number is the median of 3 runs, with the digest checked each time.
- Always report stock graph-node, with and without tuning, next to the fork.
