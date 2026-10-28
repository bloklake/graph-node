# Licensing

This repository is a fork of [graph-node](https://github.com/graphprotocol/graph-node) with
changes made by Bloklake (the datalake source, speculative execution, binary `COPY` inserts and
the `turbo/` benchmarks). It contains code under different licenses.

## Bloklake's changes: Elastic License 2.0

Copyright © 2026 Bloklake (bloklake.com).

Every change Bloklake made after upstream commit `6838f4e` (*graph, graphql, store: Drop
arrayref by bumping blake3 and stable-hash (#6715)*) is licensed under the
[Elastic License 2.0](LICENSE-ELASTIC) (SPDX: `Elastic-2.0`). That covers the new files and the
changes to the modified files listed below. This fork as a whole, as distributed by Bloklake, is
made available under the Elastic License 2.0, together with the upstream licenses for the
upstream code it contains.

In plain words (the [license text](LICENSE-ELASTIC) is what applies):

- **You may** use, copy, modify and run this software, including commercially, for yourself and
  your company: for example, indexing your own subgraphs on your own infrastructure.
- **You may not** provide it to third parties as a hosted or managed service that gives them
  access to a substantial set of its features: for example, a hosted subgraph-indexing service
  built on this fork.
- **You may not** remove or obscure the licensing or copyright notices.

Bloklake, as the licensor, is not bound by these limitations. For other terms (for example, to
offer a hosted service), contact support@bloklake.com.

## Upstream graph-node: MIT or Apache-2.0

Copyright © 2018-2019 Graph Protocol, Inc. and contributors.

The code from upstream graph-node stays licensed under the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option, and is available under those
terms from the [upstream repository](https://github.com/graphprotocol/graph-node). The Elastic
License 2.0 does not restrict upstream code obtained from upstream.

## Vendored crates

`vendor/diesel-async` is [diesel-async](https://github.com/weiznich/diesel_async) under its own
MIT or Apache-2.0 licenses (`vendor/diesel-async/LICENSE-*`). Bloklake modified it to expose the
underlying client (`raw_client`) for binary `COPY`.

## Files Bloklake added or modified

As both the Apache License 2.0 (section 4(b)) and the Elastic License 2.0 require, these are
the files Bloklake changed since `6838f4e` (`A` = added, `M` = modified). Regenerate with
`git diff --name-status 6838f4e HEAD -- . ':!vendor'`.

```
M  Cargo.lock
M  Cargo.toml
M  README.md
M  chain/ethereum/Cargo.toml
M  chain/ethereum/src/adapter.rs
M  chain/ethereum/src/buffered_call_cache.rs
M  chain/ethereum/src/chain.rs
M  chain/ethereum/src/data_source.rs
A  chain/ethereum/src/datalake.rs
M  chain/ethereum/src/ethereum_adapter.rs
M  chain/ethereum/src/ingestor.rs
M  chain/ethereum/src/lib.rs
M  chain/ethereum/src/polling_block_stream.rs
M  core/src/amp_subgraph/runner/data_processing.rs
M  core/src/subgraph/context/instance/hosts.rs
M  core/src/subgraph/context/instance/mod.rs
M  core/src/subgraph/instance_manager.rs
M  core/src/subgraph/runner/mod.rs
A  core/src/subgraph/runner/speculation.rs
M  core/src/subgraph/state.rs
M  graph/src/blockchain/block_stream.rs
M  graph/src/blockchain/mod.rs
M  graph/src/components/metrics/stopwatch.rs
M  graph/src/components/store/entity_cache.rs
M  graph/src/components/store/traits.rs
M  graph/src/components/store/write.rs
M  graph/src/components/subgraph/host.rs
M  graph/src/components/subgraph/instance.rs
M  graph/src/components/subgraph/mod.rs
M  graph/src/components/subgraph/proof_of_indexing/mod.rs
M  graph/src/components/subgraph/proof_of_indexing/online.rs
M  graph/src/data_source/causality_region.rs
M  graph/src/env/mappings.rs
A  graph/src/ipfs/builtin.rs
M  graph/src/ipfs/mod.rs
M  graph/src/log/mod.rs
M  graph/src/runtime/asc_ptr.rs
M  graph/src/runtime/mod.rs
M  node/Cargo.toml
M  node/src/chain.rs
M  node/src/config.rs
M  node/src/launcher.rs
M  node/src/main.rs
M  runtime/derive/src/lib.rs
M  runtime/wasm/src/asc_abi/v0_0_4.rs
M  runtime/wasm/src/asc_abi/v0_0_5.rs
M  runtime/wasm/src/host.rs
M  runtime/wasm/src/host_exports.rs
M  runtime/wasm/src/mapping.rs
M  runtime/wasm/src/module/context.rs
M  runtime/wasm/src/module/instance.rs
M  server/json-rpc/Cargo.toml
A  server/json-rpc/src/content.rs
M  server/json-rpc/src/lib.rs
M  server/json-rpc/src/server.rs
M  store/postgres/Cargo.toml
A  store/postgres/migrations/2026-09-28-000000_ipfs_content/down.sql
A  store/postgres/migrations/2026-09-28-000000_ipfs_content/up.sql
A  store/postgres/src/content_store.rs
A  store/postgres/src/copy_insert.rs
M  store/postgres/src/deployment_store.rs
M  store/postgres/src/dynds/private.rs
M  store/postgres/src/lib.rs
M  store/postgres/src/notification_listener.rs
M  store/postgres/src/relational.rs
M  store/postgres/src/relational_queries.rs
M  store/postgres/src/writable.rs
A  turbo/bench/__pycache__/run.cpython-314.pyc
A  turbo/bench/run.py
A  turbo/bench/subgraphs/.gitignore
A  turbo/bench/subgraphs/abis/ERC20.json
A  turbo/bench/subgraphs/abis/ProxyFactory130.json
A  turbo/bench/subgraphs/abis/ProxyFactory141.json
A  turbo/bench/subgraphs/abis/Safe.json
A  turbo/bench/subgraphs/allowances/schema.graphql
A  turbo/bench/subgraphs/allowances/src/mapping.ts
A  turbo/bench/subgraphs/allowances/subgraph.yaml
A  turbo/bench/subgraphs/package.json
A  turbo/bench/subgraphs/pnpm-lock.yaml
A  turbo/bench/subgraphs/reth-balances/schema.graphql
A  turbo/bench/subgraphs/reth-balances/src/mapping.ts
A  turbo/bench/subgraphs/reth-balances/subgraph.yaml
A  turbo/bench/subgraphs/safe/schema.graphql
A  turbo/bench/subgraphs/safe/src/factory.ts
A  turbo/bench/subgraphs/safe/src/safe.ts
A  turbo/bench/subgraphs/safe/subgraph.yaml
A  turbo/bench/subgraphs/usdc-transfers/schema.graphql
A  turbo/bench/subgraphs/usdc-transfers/src/mapping.ts
A  turbo/bench/subgraphs/usdc-transfers/subgraph.yaml
A  turbo/plan.md
A  turbo/reports/progress.md
```
