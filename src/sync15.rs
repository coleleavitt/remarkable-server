//! Sync 1.5 ("signed URL") protocol, used by current xochitl firmware.
//!
//! The device asks for a signed URL per blob (`/sync/v2/signed-urls/{downloads,uploads}`),
//! then GETs/PUTs the blob at that URL. The special blob `root` holds the root hash as text
//! and is versioned with GCS-style generations (`x-goog-generation`,
//! `x-goog-if-generation-match`) so concurrent writers get a 412 instead of a lost update.
//! Mirrors rmfakecloud's `blobStorageDownload`/`blobStorageUpload`/`/blobstorage`.

use axum::{body::Bytes, extract::{Query, State}, http::{header, HeaderMap, HeaderValue, StatusCode}, response::{IntoResponse, Response}, Json};
use serde::{Deserialize, Serialize};

use crate::{api::AppState, checksum, error::{Result, ServerError}, notifications::WsMessage, storage::is_valid_hash};

const ROOT_BLOB: &str = "root";
const GENERATION_HEADER: &str = "x-goog-generation";
const GENERATION_MATCH_HEADER: &str = "x-goog-if-generation-match";

#[derive(Deserialize)]
pub struct SignedUrlRequest {
    relative_path: String,
    #[serde(default)]
    initial_sync: bool,
}

#[derive(Serialize)]
pub struct SignedUrlResponse {
    expires: String,
    method: &'static str,
    relative_path: String,
    url: String,
}

#[derive(Deserialize)]
pub struct BlobQuery {
    blob: String,
    token: String,
}

#[derive(Deserialize)]
pub struct SyncCompleteRequest {
    #[serde(default)]
    generation: u64,
}

#[derive(Serialize)]
pub struct SyncCompleteResponse {
    id: String,
}

fn check_blob_id(blob: &str) -> Result<()> {
    if blob == ROOT_BLOB || is_valid_hash(blob) { return Ok(()); }
    tracing::warn!(blob, "rejected blob id");
    Err(ServerError::InvalidHash(blob.to_string()))
}

/// Parse a signed-URL request, logging the raw body if it doesn't match what we expect.
fn parse_signed_request(body: &Bytes) -> Result<SignedUrlRequest> {
    serde_json::from_slice(body).map_err(|e| {
        tracing::warn!(error = %e, body = %String::from_utf8_lossy(body), "bad signed-url request");
        ServerError::Json(e)
    })
}

fn signed_url(state: &AppState, headers: &HeaderMap, req: SignedUrlRequest, write: bool) -> Result<Json<SignedUrlResponse>> {
    state.auth_user(headers)?;
    check_blob_id(&req.relative_path)?;
    if write && req.initial_sync {
        tracing::info!("initial sync upload");
    }
    let (token, exp) = state.devices.sign_blob(&req.relative_path, write)?;
    let url = format!(
        "https://{}/blobstorage?blob={}&token={}",
        state.devices.get_endpoint(),
        urlencoding::encode(&req.relative_path),
        urlencoding::encode(&token),
    );
    Ok(Json(SignedUrlResponse {
        expires: exp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        method: if write { "PUT" } else { "GET" },
        relative_path: req.relative_path,
        url,
    }))
}

pub async fn signed_download(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Json<SignedUrlResponse>> {
    signed_url(&state, &headers, parse_signed_request(&body)?, false)
}

pub async fn signed_upload(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Json<SignedUrlResponse>> {
    signed_url(&state, &headers, parse_signed_request(&body)?, true)
}

pub async fn blob_get(State(state): State<AppState>, Query(q): Query<BlobQuery>) -> Result<Response> {
    state.devices.verify_blob(&q.token, &q.blob, false)?;
    check_blob_id(&q.blob)?;

    let (data, generation) = if q.blob == ROOT_BLOB {
        let root = state.storage.get_root();
        if root.hash.is_empty() {
            return Err(ServerError::NotFound(ROOT_BLOB.into()));
        }
        (root.hash.into_bytes(), Some(root.generation))
    } else {
        (state.storage.get(&q.blob)?, None)
    };

    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        [(header::HeaderName::from_static("x-goog-hash"), checksum::format_goog_hash(&data))],
        data,
    ).into_response();
    if let Some(generation) = generation {
        resp.headers_mut().insert(GENERATION_HEADER, HeaderValue::from(generation));
    }
    Ok(resp)
}

pub async fn blob_put(State(state): State<AppState>, Query(q): Query<BlobQuery>, headers: HeaderMap, body: Bytes) -> Result<Response> {
    state.devices.verify_blob(&q.token, &q.blob, true)?;
    check_blob_id(&q.blob)?;

    if let Some(goog) = headers.get("x-goog-hash").and_then(|v| v.to_str().ok()) {
        if checksum::parse_goog_hash(goog).is_some_and(|crc| crc != checksum::crc32c(&body)) {
            return Err(ServerError::ChecksumMismatch { expected: goog.to_string(), actual: checksum::format_goog_hash(&body) });
        }
    }

    if q.blob != ROOT_BLOB {
        state.storage.put_with_hash(&body, &q.blob, &q.blob)?;
        return Ok(Json(serde_json::json!({})).into_response());
    }

    let hash = std::str::from_utf8(&body).map(str::trim).unwrap_or_default();
    if !is_valid_hash(hash) {
        return Err(ServerError::InvalidHash(hash.to_string()));
    }
    let expected = headers
        .get(GENERATION_MATCH_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let root = state.storage.set_root_if(hash.to_string(), expected)?;
    tracing::info!(generation = root.generation, hash = %root.hash, "root updated");

    let mut resp = Json(serde_json::json!({})).into_response();
    resp.headers_mut().insert(GENERATION_HEADER, HeaderValue::from(root.generation));
    Ok(resp)
}

pub async fn sync_complete(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<SyncCompleteRequest>) -> Result<Json<SyncCompleteResponse>> {
    let (user_id, device_id, _) = state.devices.caller(headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or_default())?;
    tracing::info!(generation = req.generation, device = %device_id, "sync complete");
    // Attributed to the pushing device so it skips its own notification (xochitl 3.28 C.2).
    let msg = WsMessage::sync_complete(req.generation, &device_id, &user_id);
    let id = uuid::Uuid::new_v4().to_string();
    // No subscribers is fine; the device just isn't listening right now.
    let _ = state.notification_tx.send(msg);
    Ok(Json(SyncCompleteResponse { id }))
}
