//! Axum-based JSON-RPC server implementation.
//!
//! This module provides the `JsonRpcServer` that serves JSON-RPC requests
//! over HTTP using axum.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use graph::prelude::{NodeId, SubgraphRegistrar};
use slog::{Logger, info};
use thiserror::Error;
use tokio::sync::Notify;

use crate::handlers::{AppState, jsonrpc_handler};

/// Largest upload `/api/v0/add` accepts (a subgraph's WASM is usually well under this).
const CONTENT_BODY_LIMIT: usize = 256 * 1024 * 1024;

/// Errors that can occur when starting the JSON-RPC server.
#[derive(Debug, Error)]
pub enum JsonRpcServerError {
    #[error("Failed to bind to address: {0}")]
    Bind(#[from] std::io::Error),
}

/// Handle to a running JSON-RPC server.
///
/// Dropping this handle does not stop the server. Use `stop()` for graceful shutdown.
pub struct JsonRpcServer {
    notify: Arc<Notify>,
}

impl JsonRpcServer {
    /// Start the JSON-RPC server.
    ///
    /// # Arguments
    ///
    /// * `port` - The port to listen on for JSON-RPC requests
    /// * `http_port` - The HTTP port used for subgraph route URLs in deploy responses
    /// * `registrar` - The subgraph registrar for handling operations
    /// * `node_id` - Default node ID for deployments
    /// * `logger` - Logger for request/response logging
    pub async fn serve<R>(
        port: u16,
        http_port: u16,
        registrar: Arc<R>,
        node_id: NodeId,
        logger: Logger,
    ) -> Result<Self, JsonRpcServerError>
    where
        R: SubgraphRegistrar,
    {
        let logger = logger.new(graph::prelude::o!("component" => "JsonRpcServer"));

        info!(
            logger,
            "Starting JSON-RPC admin server at: http://localhost:{}", port
        );

        let state = Arc::new(AppState {
            registrar,
            http_port,
            node_id,
            logger,
        });

        let content = Router::new()
            .route("/api/v0/add", post(crate::content::add))
            .route("/api/v0/cat", post(crate::content::cat))
            .route("/api/v0/pin/add", post(crate::content::pin_add))
            .route("/api/v0/version", post(crate::content::version))
            .route("/ipfs/{cid}", get(crate::content::get_file))
            .layer(DefaultBodyLimit::max(CONTENT_BODY_LIMIT));
        let app = Router::new()
            .route("/", post(jsonrpc_handler::<R>))
            .with_state(state)
            .merge(content);

        let addr: SocketAddr = (Ipv4Addr::new(0, 0, 0, 0), port).into();
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        graph::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                notify_clone.notified().await;
            })
            .await
            .unwrap_or_else(|err| panic!("JSON-RPC server failed: {err}"));
        });

        Ok(Self { notify })
    }

    /// Stop the server gracefully.
    pub fn stop(self) {
        self.notify.notify_one();
    }
}
