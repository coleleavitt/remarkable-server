//! HTTP API handlers for sync endpoints

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use crate::checksum;
use crate::error::{Result, ServerError};
use crate::storage::Storage;
use crate::types::{SyncRoot, UploadResponse};
use serde::Serialize;

/// Application state
#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    /// Mock token for local testing
    pub mock_token: String,
}

impl AppState {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            mock_token: "local-mock-token-v1".to_string(),
        }
    }
}

// ============================================================================
// Sync v3 Endpoints
// ============================================================================

/// GET /sync/v3/root
/// 
/// Returns the current root hash and generation.
pub async fn get_root(State(state): State<AppState>) -> Json<SyncRoot> {
    Json(state.storage.get_root())
}

/// GET /sync/v3/files/{hash}
///
/// Download a file by its SHA-256 hash.
/// Requires `rm-filename` header to be present.
pub async fn get_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response> {
    // Validate rm-filename header (required by reMarkable API)
    let filename = headers
        .get("rm-filename")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::MissingHeader("rm-filename".to_string()))?;
    
    tracing::debug!("GET file hash={} rm-filename={}", hash, filename);
    
    // Get file data
    let data = state.storage.get(&hash)?;
    
    // Build response with checksum header
    let checksum = checksum::format_goog_hash(&data);
    
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CONTENT_LENGTH, &data.len().to_string()),
        ],
        [(header::HeaderName::from_static("x-goog-hash"), checksum)],
        data,
    ).into_response())
}

/// PUT /sync/v3/files/{hash}
///
/// Upload a file with CRC32C validation.
/// Required headers:
/// - `rm-filename`: filename for the file
/// - `x-goog-hash`: CRC32C checksum (optional but validated if present)
pub async fn put_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<UploadResponse>> {
    // Validate rm-filename header
    let filename = headers
        .get("rm-filename")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::MissingHeader("rm-filename".to_string()))?;
    
    tracing::debug!(
        "PUT file hash={} rm-filename={} size={}",
        hash, filename, body.len()
    );
    
    // Validate checksum if provided
    if let Some(goog_hash) = headers.get("x-goog-hash").and_then(|v| v.to_str().ok()) {
        if !checksum::verify_checksum(&body, goog_hash) {
            let actual = checksum::format_goog_hash(&body);
            return Err(ServerError::ChecksumMismatch {
                expected: goog_hash.to_string(),
                actual,
            });
        }
    }
    
    // Store file with explicit hash
    state.storage.put_with_hash(&body, &hash, filename)?;
    
    // Optional: Update root if rm-parent-hash indicates root update
    if let Some(parent_hash) = headers.get("rm-parent-hash").and_then(|v| v.to_str().ok()) {
        if parent_hash == "root" || parent_hash.is_empty() {
            state.storage.set_root(hash.clone())?;
        }
    }
    
    Ok(Json(UploadResponse {
        hash,
        size: body.len() as u64,
    }))
}

// ============================================================================
// Token Endpoints (Mock)
// ============================================================================

/// POST /token/json/2/user/new
///
/// Token refresh endpoint (mock for local testing).
/// Returns a mock token that the server will accept.
pub async fn refresh_token(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    // In local mode, we accept any Authorization header
    let _auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    
    // Return mock token as plain text (reMarkable API format)
    Ok(state.mock_token.clone())
}

/// POST /token/json/2/device/new
///
/// Device registration endpoint (mock for local testing).
pub async fn register_device(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse> {
    tracing::info!("Device registration: {:?}", body);
    
    // Return mock device token
    Ok(state.mock_token.clone())
}

// ============================================================================
// Discovery Endpoint
// ============================================================================

/// Discovery response
#[derive(Serialize)]
pub struct DiscoveryEndpoints {
    #[serde(rename = "Host")]
    pub host: String,
    #[serde(rename = "Status")]
    pub status: String,
}

/// GET /discovery/v1/endpoints or /service/json/1/document-storage
///
/// Returns endpoints for sync services.
pub async fn discovery(
    headers: HeaderMap,
) -> Json<DiscoveryEndpoints> {
    // Get host from request or use localhost
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:8080");
    
    Json(DiscoveryEndpoints {
        host: host.to_string(),
        status: "OK".to_string(),
    })
}

// ============================================================================
// Health & Debug Endpoints
// ============================================================================

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub storage: StorageInfo,
}

#[derive(Serialize)]
pub struct StorageInfo {
    pub file_count: usize,
    pub total_bytes: u64,
    pub root_hash: String,
    pub generation: u64,
}

/// GET /health
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let stats = state.storage.stats();
    
    Json(HealthResponse {
        status: "ok".to_string(),
        storage: StorageInfo {
            file_count: stats.file_count,
            total_bytes: stats.total_bytes,
            root_hash: stats.root_hash,
            generation: stats.generation,
        },
    })
}

/// GET /debug/files
/// 
/// List all files in storage (debug endpoint).
pub async fn list_files(State(state): State<AppState>) -> Result<Json<Vec<String>>> {
    let hashes = state.storage.list_hashes()?;
    Ok(Json(hashes))
}

/// DELETE /debug/clear
///
/// Clear all storage (debug endpoint).
pub async fn clear_storage(State(state): State<AppState>) -> Result<impl IntoResponse> {
    state.storage.clear()?;
    Ok(StatusCode::NO_CONTENT)
}
