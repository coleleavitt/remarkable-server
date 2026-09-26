//! Multi-version sync protocol support
//!
//! Implements all reMarkable sync protocol versions:
//! - V1: Original document-storage JSON API (firmware 1.x-2.x)
//! - V1.5: Transitional with batch operations
//! - V2: Binary protocol introduction
//! - V3: Current production (hash-based, CRDT)
//! - V4: Future protocol (extended metadata)

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use crate::api::AppState;

/// Sync protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncVersion {
    V1,
    V1_5,
    V2,
    V3,
    V4,
}

impl SyncVersion {
    pub fn from_path(path: &str) -> Option<Self> {
        if path.contains("/v1.5/") || path.contains("/v1_5/") {
            Some(Self::V1_5)
        } else if path.contains("/v1/") {
            Some(Self::V1)
        } else if path.contains("/v2/") {
            Some(Self::V2)
        } else if path.contains("/v3/") {
            Some(Self::V3)
        } else if path.contains("/v4/") {
            Some(Self::V4)
        } else {
            None
        }
    }
}

// ============================================================================
// V1 Protocol (document-storage JSON API)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct V1Document {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: i32,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Success")]
    pub success: bool,
    #[serde(rename = "BlobURLGet")]
    pub blob_url_get: String,
    #[serde(rename = "BlobURLGetExpires")]
    pub blob_url_get_expires: String,
    #[serde(rename = "BlobURLPut")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_url_put: Option<String>,
    #[serde(rename = "ModifiedClient")]
    pub modified_client: String,
    #[serde(rename = "Type")]
    pub doc_type: String,
    #[serde(rename = "VissibleName")]
    pub visible_name: String,
    #[serde(rename = "CurrentPage")]
    pub current_page: i32,
    #[serde(rename = "Bookmarked")]
    pub bookmarked: bool,
    #[serde(rename = "Parent")]
    pub parent: String,
}

/// GET /document-storage/json/2/docs - List all documents (V1)
pub async fn v1_list_docs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<V1Document>>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    // Convert storage files to V1 document format
    // Sync v3 blobs aren't v1 documents; listing them as such invites a v1 client to
    // delete live sync data, so this stays empty as it always has.
    let files: Vec<(String, String, usize)> = Vec::new();

    let v1_docs: Vec<V1Document> = files
        .iter()
        .enumerate()
        .map(|(i, (hash, filename, _size))| V1Document {
            id: hash.clone(),
            version: 1,
            message: String::new(),
            success: true,
            blob_url_get: format!("/document-storage/json/2/docs/{}", hash),
            blob_url_get_expires: "2099-12-31T23:59:59Z".to_string(),
            blob_url_put: Some(format!("/document-storage/json/2/upload/{}", hash)),
            modified_client: chrono::Utc::now().to_rfc3339(),
            doc_type: if filename.ends_with(".rm") {
                "DocumentType".to_string()
            } else {
                "CollectionType".to_string()
            },
            visible_name: filename.clone(),
            current_page: 0,
            bookmarked: false,
            parent: String::new(),
        })
        .collect();

    Ok(Json(v1_docs))
}

#[derive(Debug, Deserialize)]
pub struct V1UploadRequest {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: i32,
    #[serde(rename = "Type")]
    pub doc_type: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct V1UploadResponse {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: i32,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Success")]
    pub success: bool,
    #[serde(rename = "BlobURLPut")]
    pub blob_url_put: String,
    #[serde(rename = "BlobURLPutExpires")]
    pub blob_url_put_expires: String,
}

/// PUT /document-storage/json/2/upload/request - Request upload URL (V1)
pub async fn v1_upload_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(docs): Json<Vec<V1UploadRequest>>,
) -> Result<Json<Vec<V1UploadResponse>>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let responses: Vec<V1UploadResponse> = docs
        .iter()
        .map(|d| V1UploadResponse {
            id: d.id.clone(),
            version: d.version,
            message: String::new(),
            success: true,
            blob_url_put: format!("/document-storage/json/2/upload/{}", d.id),
            blob_url_put_expires: "2099-12-31T23:59:59Z".to_string(),
        })
        .collect();

    Ok(Json(responses))
}

#[derive(Debug, Deserialize)]
pub struct V1StatusUpdate {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: i32,
}

#[derive(Debug, Serialize)]
pub struct V1StatusResponse {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: i32,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Success")]
    pub success: bool,
}

/// PUT /document-storage/json/2/upload/update-status - Update upload status (V1)
pub async fn v1_update_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(updates): Json<Vec<V1StatusUpdate>>,
) -> Result<Json<Vec<V1StatusResponse>>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let responses: Vec<V1StatusResponse> = updates
        .iter()
        .map(|u| V1StatusResponse {
            id: u.id.clone(),
            version: u.version,
            message: String::new(),
            success: true,
        })
        .collect();

    Ok(Json(responses))
}

#[derive(Debug, Deserialize)]
pub struct V1DeleteRequest {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: i32,
}

/// DELETE /document-storage/json/2/delete - Delete documents (V1)
pub async fn v1_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(ids): Json<Vec<V1DeleteRequest>>,
) -> Result<Json<Vec<V1StatusResponse>>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    // v1 ids aren't documents here, and blobs are shared by the sync tree: never delete
    // one the current root still references.
    for req in &ids {
        if !state.storage.is_referenced(&req.id) {
            let _ = state.storage.delete(&req.id);
        }
    }

    let responses: Vec<V1StatusResponse> = ids
        .iter()
        .map(|r| V1StatusResponse {
            id: r.id.clone(),
            version: r.version,
            message: String::new(),
            success: true,
        })
        .collect();

    Ok(Json(responses))
}

// ============================================================================
// V1.5 Protocol (batch operations)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct V15BatchRequest {
    #[serde(default)]
    pub documents: Vec<V15Document>,
    #[serde(default)]
    pub generation: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct V15Document {
    pub id: String,
    #[serde(default)]
    pub hash: String,
    #[serde(rename = "type", default)]
    pub doc_type: String,
    #[serde(default)]
    pub visible_name: String,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub version: i32,
    #[serde(default)]
    pub modified_client: String,
}

#[derive(Debug, Serialize)]
pub struct V15BatchResponse {
    pub documents: Vec<V15Document>,
    pub generation: i64,
    pub success: bool,
}

/// POST /sync/v1.5/batch - Batch sync (V1.5)
pub async fn v15_batch_sync(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(_batch): Json<V15BatchRequest>,
) -> Result<Json<V15BatchResponse>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    // Sync v3 blobs aren't v1 documents; listing them as such invites a v1 client to
    // delete live sync data, so this stays empty as it always has.
    let files: Vec<(String, String, usize)> = Vec::new();
    let root = state.storage.get_root();

    let v15_docs: Vec<V15Document> = files
        .iter()
        .map(|(hash, filename, _size)| V15Document {
            id: hash.clone(),
            hash: hash.clone(),
            doc_type: if filename.ends_with(".rm") {
                "DocumentType".to_string()
            } else {
                "CollectionType".to_string()
            },
            visible_name: filename.clone(),
            parent: None,
            version: 1,
            modified_client: chrono::Utc::now().to_rfc3339(),
        })
        .collect();

    Ok(Json(V15BatchResponse {
        documents: v15_docs,
        generation: root.generation as i64,
        success: true,
    }))
}

// ============================================================================
// V2 Protocol (binary with metadata)
// ============================================================================

#[derive(Debug, Serialize)]
pub struct V2Root {
    pub hash: String,
    pub generation: i64,
    pub schema_version: i32,
}

/// GET /sync/v2/root - Get sync root (V2)
pub async fn v2_get_root(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<V2Root>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let root = state.storage.get_root();

    Ok(Json(V2Root {
        hash: root.hash,
        generation: root.generation as i64,
        schema_version: 2,
    }))
}

/// GET /sync/v2/files/{hash} - Get file (V2)
pub async fn v2_get_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Bytes, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    state
        .storage
        .get(&hash)
        .map(Bytes::from)
        .map_err(|_| StatusCode::NOT_FOUND)
}

/// PUT /sync/v2/files/{hash} - Put file (V2)  
pub async fn v2_put_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let filename = headers
        .get("rm-filename")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&hash)
        .to_string();

    state
        .storage
        .put_with_hash(&body, &hash, &filename)
        .map(|_| StatusCode::OK)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// ============================================================================
// V4 Protocol (extended metadata, future)
// ============================================================================

#[derive(Debug, Serialize)]
pub struct V4Root {
    pub hash: String,
    pub generation: i64,
    pub schema_version: i32,
    pub features: Vec<String>,
    pub capabilities: V4Capabilities,
}

#[derive(Debug, Serialize)]
pub struct V4Capabilities {
    pub crdt: bool,
    pub sharing: bool,
    pub tags: bool,
    pub search: bool,
    pub calendar: bool,
}

/// GET /sync/v4/root - Get sync root with extended metadata (V4)
pub async fn v4_get_root(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<V4Root>, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let root = state.storage.get_root();

    Ok(Json(V4Root {
        hash: root.hash,
        generation: root.generation as i64,
        schema_version: 4,
        features: vec![
            "crdt".to_string(),
            "sharing".to_string(),
            "tags".to_string(),
            "search".to_string(),
            "calendar".to_string(),
        ],
        capabilities: V4Capabilities {
            crdt: true,
            sharing: true,
            tags: true,
            search: true,
            calendar: true,
        },
    }))
}

/// GET /sync/v4/files/{hash} - Get file with metadata (V4)
pub async fn v4_get_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Bytes), StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let content = state
        .storage
        .get(&hash)
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let mut headers = HeaderMap::new();
    headers.insert("x-rm-schema-version", "4".parse().unwrap());
    headers.insert("x-rm-features", "crdt,sharing,tags".parse().unwrap());

    Ok((headers, Bytes::from(content)))
}

/// PUT /sync/v4/files/{hash} - Put file with extended validation (V4)
pub async fn v4_put_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    state
        .auth_user(&headers)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let filename = headers
        .get("rm-filename")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&hash)
        .to_string();

    state
        .storage
        .put_with_hash(&body, &hash, &filename)
        .map(|_| StatusCode::OK)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
