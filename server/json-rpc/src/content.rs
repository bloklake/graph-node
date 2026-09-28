//! IPFS-compatible endpoints over the built-in content store (see
//! [`graph::ipfs::builtin`]), so `graph deploy --ipfs <admin url>` works without an IPFS node.
//!
//! Supported: `POST /api/v0/add` (multipart, one or more files; CIDv0 only), `POST
//! /api/v0/cat?arg=<cid>`, `POST /api/v0/pin/add?arg=<cid>` (a no-op: the store never evicts),
//! `POST /api/v0/version` and `GET`/`HEAD /ipfs/<cid>`. When graph-node reads from an IPFS
//! server instead (`--ipfs <url>`), every endpoint answers `404`.

use std::collections::HashMap;

use axum::body::Body;
use axum::extract::{Multipart, Path, Query};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use graph::ipfs::ContentPath;
use graph::ipfs::builtin::{self, ContentStore};
use std::sync::Arc;

fn error(status: StatusCode, message: impl std::fmt::Display) -> Response {
    let body = serde_json::json!({ "Message": message.to_string(), "Code": 0, "Type": "error" });
    (status, axum::Json(body)).into_response()
}

fn store() -> Result<Arc<dyn ContentStore>, Response> {
    builtin::content_store().ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            "the built-in content store is off: this graph-node reads from an IPFS server",
        )
    })
}

/// Parses `<cid>`, `/ipfs/<cid>` or `ipfs://<cid>` into the key the file is stored under.
fn key(arg: &str) -> Result<String, Response> {
    let path = ContentPath::new(arg).map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    if path.path().is_some() {
        return Err(error(
            StatusCode::NOT_FOUND,
            "the built-in store keeps files, not directories",
        ));
    }
    builtin::store_key(path.cid()).ok_or_else(|| error(StatusCode::BAD_REQUEST, "not a dag-pb CID"))
}

async fn read(arg: &str) -> Result<Response, Response> {
    let store = store()?;
    let key = key(arg)?;
    match store.get(&key).await {
        Ok(Some(bytes)) => Ok((
            [(header::CONTENT_TYPE, "application/octet-stream")],
            Body::from(bytes),
        )
            .into_response()),
        Ok(None) => Err(error(StatusCode::NOT_FOUND, format!("{key} not found"))),
        Err(e) => Err(error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))),
    }
}

pub async fn add(mut multipart: Multipart) -> Response {
    let store = match store() {
        Ok(store) => store,
        Err(resp) => return resp,
    };
    let mut lines = String::new();
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        if field.content_type() == Some("application/x-directory") {
            continue;
        }
        let name = field
            .file_name()
            .or(field.name())
            .unwrap_or_default()
            .to_string();
        let data = match field.bytes().await {
            Ok(data) => data,
            Err(e) => return error(StatusCode::BAD_REQUEST, e),
        };
        let cid = builtin::cid_v0(&data);
        if let Err(e) = store.put(&cid, &data).await {
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
        }
        let name = if name.is_empty() { cid.clone() } else { name };
        let line = serde_json::json!({ "Name": name, "Hash": cid, "Size": data.len().to_string() });
        lines.push_str(&line.to_string());
        lines.push('\n');
    }
    ([(header::CONTENT_TYPE, "application/json")], lines).into_response()
}

pub async fn cat(Query(params): Query<HashMap<String, String>>) -> Response {
    let Some(arg) = params.get("arg") else {
        return error(StatusCode::BAD_REQUEST, "argument \"arg\" is required");
    };
    read(arg).await.unwrap_or_else(|resp| resp)
}

pub async fn pin_add(Query(params): Query<HashMap<String, String>>) -> Response {
    if let Err(resp) = store() {
        return resp;
    }
    let pins: Vec<&String> = params.get("arg").into_iter().collect();
    axum::Json(serde_json::json!({ "Pins": pins })).into_response()
}

pub async fn version() -> Response {
    if let Err(resp) = store() {
        return resp;
    }
    axum::Json(serde_json::json!({ "Version": "graph-node-builtin" })).into_response()
}

pub async fn get_file(Path(cid): Path<String>) -> Response {
    read(&cid).await.unwrap_or_else(|resp| resp)
}
