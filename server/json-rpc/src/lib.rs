//! JSON-RPC admin server for subgraph management.
//!
//! This crate provides a JSON-RPC 2.0 server for managing subgraphs,
//! supporting operations like create, deploy, remove, reassign, pause, and resume. It also
//! serves IPFS-compatible endpoints over the built-in content store (see `content`).

mod content;
mod handlers;
mod jsonrpc;
mod server;

pub use server::{JsonRpcServer, JsonRpcServerError};
