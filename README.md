# Graph Node

[![Build Status](https://github.com/graphprotocol/graph-node/actions/workflows/ci.yml/badge.svg)](https://github.com/graphprotocol/graph-node/actions/workflows/ci.yml?query=branch%3Amaster)
[![Docs](https://img.shields.io/badge/docs-graph--node-green.svg)](docs/)
[![Subgraphs](https://img.shields.io/badge/docs-subgraphs-green.svg)](https://thegraph.com/docs/en/subgraphs/quick-start/)

## Bloklake fork

This is [Bloklake](https://www.bloklake.com/indexers)'s fork of graph-node. For block ranges the
Bloklake API has fully ingested, it reads log triggers from the API instead of JSON-RPC: one
request returns the matching logs with their transactions and block headers. Your subgraph
(manifest, schema and mappings) is unchanged; the node's config gains a `datalake` line on the
chain:

```toml
[chains.mainnet]
shard = "primary"
provider = [ { label = "rpc", url = "https://your-rpc.example", features = [] } ]
datalake = { url = "https://eth.api.bloklake.com", api_key = "bl_live_..." }  # rh.api.bloklake.com for Robinhood Chain
```

JSON-RPC is still used for the chain head, blocks after the API's archive height, call and block
handlers, and receipts when a handler asks for them. Requests stay under the API key's in-flight
limit (`max_concurrent_requests`, default 4); a `429` waits for `Retry-After` and is not counted
as a failure.

### Benchmark

The fork over the Bloklake API on 10 subgraphs (November 2026), fresh database. Every table's row
count matches the same app written for Ponder.

| Workload | Blocks | Time · data received | Peak memory |
|---|---|---|---|
| USDC transfers | 7,000 | 185 s · 2.6 GB | 1,941 MB |
| USDT balances | 6,000 | 1,101 s · 2.9 GB | 1,831 MB |
| WETH balances + daily volume | 12,000 | 250 s · 2.7 GB | 1,848 MB |
| Every ERC-20 transfer | 500 | 156 s · 2.3 GB | 2,029 MB |
| Uniswap v4 (PoolManager) | 20,000 | 449 s · 3.4 GB | 2,048 MB |
| Uniswap v3 (9 pools) | 200,000 | 308 s · 5.0 GB | 2,048 MB |
| Aave v3 | 300,000 | 320 s · 1.9 GB | 1,723 MB |
| Lido stETH | 400,000 | 157 s · 1.3 GB | 1,242 MB |
| Every ERC-20 transfer (Robinhood Chain) | 150,000 | 423 s · 1.9 GB | 1,280 MB |
| DEX factories v2 + v3 (Robinhood Chain) | 500,000 | 238 s · 19.6 GB | 2,053 MB |

Setup: a 2-vCPU ARM server, each run capped at 1.5 CPUs and 2 GB of memory (several runs reached
the cap), Postgres 16 on the same machine, the production API with an ordinary key. The
comparison with upstream graph-node over JSON-RPC on the same machine is running and will be
added here. These runs predate a fix to the datalake prefetch (`aa66c3d`): the fork read most
blocks about four times, so the data figures above overstate what it now receives; they will be
replaced with re-measured ones.

### Build

```bash
git clone https://github.com/bloklake/graph-node
cd graph-node
cargo build --release --locked -p graph-node   # needs protoc and libpq, like upstream
./target/release/graph-node --config config.toml --ipfs http://127.0.0.1:5001
```

You need a Bloklake API key: [get one in the dashboard](https://dashboard.bloklake.com). Upstream
graph-node is MIT or Apache 2.0; Bloklake's changes are under the Elastic License 2.0 (see
[LICENSING.md](LICENSING.md)): you may run and modify them, including commercially, for your own
indexers, but not offer them as a hosted indexing service. [UPSTREAM.md](UPSTREAM.md) says how the
fork tracks upstream.

The rest of this README is upstream's.

## Overview

[The Graph](https://thegraph.com/) is a decentralized protocol that organizes and distributes blockchain data across the leading Web3 networks. A key component of The Graph's tech stack is Graph Node.

Before using `graph-node,` it is highly recommended that you read the [official Graph documentation](https://thegraph.com/docs/en/subgraphs/quick-start/) to understand Subgraphs, which are the central mechanism for extracting and organizing blockchain data.

This guide is for:

1. Subgraph developers who want to run `graph-node` locally to test their Subgraphs during development
2. Contributors who want to add features or fix bugs to `graph-node` itself

## Running `graph-node` from Docker images

For subgraph developers, it is highly recommended to use prebuilt Docker
images to set up a local `graph-node` environment. Please read [these
instructions](./docker/README.md) to learn how to do that.

## Running `graph-node` from source

This is usually only needed for developers who want to contribute to `graph-node`.

### Prerequisites

To build and run this project, you need to have the following installed on your system:

- Rust (latest stable): Follow [How to install
  Rust](https://rust-lang.org/tools/install/). Run `rustup install
stable` in _this directory_ to make sure all required components are
  installed. The `graph-node` code assumes that the latest available
  `stable` compiler is used.
- PostgreSQL: [PostgreSQL Downloads](https://www.postgresql.org/download/) lists
  downloads for almost all operating systems.
  - For OSX: We highly recommend [Postgres.app](https://postgresapp.com/).
  - For Linux: Use the Postgres version that comes with the distribution.
- IPFS: [Installing IPFS](https://docs.ipfs.io/install/)
- Protobuf Compiler: [Installing Protobuf](https://grpc.io/docs/protoc-installation/)

For Ethereum network data, you can either run your own Ethereum node or use an Ethereum node provider of your choice.

### Create a database

Once Postgres is running, you need to issue the following commands to create a database
and configure it for use with `graph-node`.

The name of the `SUPERUSER` depends on your installation, but is usually `postgres` or your username.

```bash
psql -U <SUPERUSER> <<EOF
create user graph with password '<password>';
create database "graph-node" with owner=graph template=template0 encoding='UTF8' locale='C';
create extension pg_trgm;
create extension btree_gist;
create extension postgres_fdw;
grant usage on foreign data wrapper postgres_fdw to graph;
EOF
```

For convenience, set the connection string to the database in an environment
variable, and save it, e.g., in `~/.bashrc`:

```bash
export POSTGRES_URL=postgresql://graph:<password>@localhost:5432/graph-node
```

Use the `POSTGRES_URL` from above to have `graph-node` connect to the
database. If you ever need to manually inspect the contents of your
database, you can do that by running `psql $POSTGRES_URL`. Running this
command is also a convenient way to check that the database is up and
running and that the connection string is correct.

### Build and Run `graph-node`

Clone this repository and run this command at the root of the repository:

```bash
export GRAPH_LOG=debug
cargo run -p graph-node --release -- \
  --postgres-url $POSTGRES_URL \
  --ethereum-rpc NETWORK_NAME:[CAPABILITIES]:URL \
  --ipfs 127.0.0.1:5001
```

The argument for `--ethereum-rpc` contains a network name (e.g. `mainnet`) and
a list of provider capabilities (e.g. `archive,traces`). The URL is the address
of the Ethereum node you want to connect to, usually a `https` URL, so that the
entire argument might be `mainnet:archive,traces:https://provider.io/some/path`.

When `graph-node` starts, it prints the various ports that it is listening on.
The most important of these is the GraphQL HTTP server, which by default
is at `http://localhost:8000`. You can use routes like `/subgraphs/name/<subgraph-name>`
and `/subgraphs/id/<IPFS hash>` to query subgraphs once you have deployed them.

### Deploying a Subgraph

Follow the [Subgraph deployment
guide](https://thegraph.com/docs/en/subgraphs/developing/introduction/).
After setting up `graph-cli` as described, you can deploy a Subgraph to your
local Graph Node instance.

### Advanced Configuration

The command line arguments generally are all that is needed to run a
`graph-node` instance. For advanced uses, various aspects of `graph-node`
can further be configured through [environment
variables](https://github.com/graphprotocol/graph-node/blob/master/docs/environment-variables.md).

Very large `graph-node` instances can also be configured using a
[configuration file](./docs/config.md) That is usually only necessary when
the `graph-node` needs to connect to multiple chains or if the work of
indexing and querying needs to be split across [multiple databases](./docs/config.md).

#### Log Storage

`graph-node` supports storing and querying subgraph logs through multiple backends:

- **File**: Local JSON Lines files (recommended for local development)
- **Elasticsearch**: Enterprise-grade search and analytics (for production)
- **Loki**: Grafana's log aggregation system (for production)
- **Disabled**: No log storage (default)

**Quick example (file-based logs for local development):**
```bash
mkdir -p ./graph-logs

cargo run -p graph-node --release -- \
  --postgres-url $POSTGRES_URL \
  --ethereum-rpc mainnet:archive:https://... \
  --ipfs 127.0.0.1:5001 \
  --log-store-backend file \
  --log-store-file-dir ./graph-logs
```

Logs are queried via GraphQL at `http://localhost:8000/graphql`:
```graphql
query {
  _logs(subgraphId: "QmYourSubgraphHash", level: ERROR, first: 10) {
    timestamp
    level
    text
  }
}
```

**For complete documentation**, see the **[Log Store Guide](./docs/log-store.md)**, which covers:
- How to configure each backend (Elasticsearch, Loki, File)
- Complete GraphQL query examples
- Choosing the right backend for your use case
- Performance considerations and best practices

## Contributing

Please check [CONTRIBUTING.md](CONTRIBUTING.md) for development flow and conventions we use.
Here's [a list of good first issues](https://github.com/graphprotocol/graph-node/labels/good%20first%20issue).

## License

This is Bloklake's fork of graph-node. Bloklake's changes are licensed under the
[Elastic License 2.0](LICENSE-ELASTIC): you may use and run them yourself, but not offer them to
others as a hosted or managed service. See [LICENSING.md](LICENSING.md) for which code is under
which license.

Copyright &copy; 2018-2019 Graph Protocol, Inc. and contributors.

Upstream graph-node is dual-licensed under the [MIT license](LICENSE-MIT) and the [Apache License, Version 2.0](LICENSE-APACHE).

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the License for the specific language governing permissions and limitations under the License.
