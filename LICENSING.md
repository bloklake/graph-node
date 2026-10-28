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

As both the Apache License 2.0 (section 4(b)) and the Elastic License 2.0 require, Bloklake
states which files it changed: every file added or modified after `6838f4e`. List them with
`git diff --name-status 6838f4e HEAD -- . ':!vendor'`.
