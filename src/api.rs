use axum::{body::Bytes, extract::{Path, State}, http::{header, HeaderMap, StatusCode}, response::{IntoResponse, Response}, Json};
use crate::{checksum, device::DeviceManager, error::{Result, ServerError}, storage::Storage};
use crate::integrations::IntegrationStore;
use crate::types::{DeviceInfo, DeviceRegisterRequest, PairingCodeResponse, SyncRoot, UploadResponse};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState { 
    pub storage: Storage, 
    pub devices: DeviceManager,
    pub integrations: IntegrationStore,
}

impl AppState {
    pub fn new(storage: Storage, devices: DeviceManager, integrations: IntegrationStore) -> Self { 
        Self { storage, devices, integrations } 
    }
    fn auth_user(&self, headers: &HeaderMap) -> Result<String> {
        self.devices.validate_token(headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?)
    }
}

pub async fn get_root(State(state): State<AppState>) -> Json<SyncRoot> { 
    Json(state.storage.get_root()) 
}

pub async fn get_file(State(state): State<AppState>, Path(hash): Path<String>, headers: HeaderMap) -> Result<Response> {
    let _filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    let data = state.storage.get(&hash)?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/octet-stream"), (header::CONTENT_LENGTH, &data.len().to_string())], [(header::HeaderName::from_static("x-goog-hash"), checksum::format_goog_hash(&data))], data).into_response())
}

pub async fn put_file(State(state): State<AppState>, Path(hash): Path<String>, headers: HeaderMap, body: Bytes) -> Result<Json<UploadResponse>> {
    let filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    if let Some(gh) = headers.get("x-goog-hash").and_then(|v| v.to_str().ok()) { 
        if !checksum::verify_checksum(&body, gh) { 
            return Err(ServerError::ChecksumMismatch { expected: gh.into(), actual: checksum::format_goog_hash(&body) }); 
        } 
    }
    state.storage.put_with_hash(&body, &hash, filename)?;
    if let Some(ph) = headers.get("rm-parent-hash").and_then(|v| v.to_str().ok()) { 
        if ph == "root" || ph.is_empty() { 
            state.storage.set_root(hash.clone())?; 
        } 
    }
    Ok(Json(UploadResponse { hash, size: body.len() as u64 }))
}

pub async fn create_pairing_code(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<PairingCodeResponse>> {
    let user_id = state.auth_user(&headers)?;
    Ok(Json(PairingCodeResponse { code: state.devices.create_pairing_code(&user_id)?, expires_in: 600 }))
}

pub async fn list_devices(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Vec<DeviceInfo>>> {
    let _ = state.auth_user(&headers)?;
    Ok(Json(state.devices.list_devices()?.into_iter().map(|d| DeviceInfo { 
        device_id: d.device_id, 
        device_desc: d.device_desc, 
        registered_at: d.registered_at.to_rfc3339(), 
        last_activity: d.last_refresh.to_rfc3339() 
    }).collect()))
}

pub async fn delete_device(State(state): State<AppState>, Path(id): Path<String>, headers: HeaderMap) -> Result<impl IntoResponse> {
    let _ = state.auth_user(&headers)?;
    if state.devices.delete_device(&id)? { Ok(StatusCode::NO_CONTENT) } else { Err(ServerError::NotFound(id)) }
}

pub async fn refresh_token(State(state): State<AppState>, headers: HeaderMap) -> Result<impl IntoResponse> {
    let token = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    let token = token.strip_prefix("Bearer ").unwrap_or(token);
    let new_token = state.devices.refresh_user_token(token)?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "text/plain")], new_token))
}

pub async fn register_device(State(state): State<AppState>, Json(req): Json<DeviceRegisterRequest>) -> Result<impl IntoResponse> {
    let (device_token, _user_token) = state.devices.exchange_code(&req.code, &req.device_id, &req.device_desc)?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "text/plain")], device_token))
}

pub async fn delete_device_token(State(state): State<AppState>, headers: HeaderMap) -> Result<impl IntoResponse> {
    let token = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    let token = token.strip_prefix("Bearer ").unwrap_or(token);
    let _user_id = state.devices.validate_token(token)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
pub struct DiscoveryResponse {
    pub status: String,
    #[serde(rename = "Host")]
    pub host: String,
}

pub async fn discovery(State(state): State<AppState>) -> Json<DiscoveryResponse> {
    let host = state.devices.get_endpoint();
    Json(DiscoveryResponse { status: "OK".into(), host })
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
}

pub async fn create_test_user(State(state): State<AppState>, Json(req): Json<CreateUserRequest>) -> Result<impl IntoResponse> {
    let token = state.devices.create_user_token(&req.email)?;
    Ok((StatusCode::CREATED, [(header::CONTENT_TYPE, "text/plain")], token))
}

pub async fn health() -> &'static str { "ok" }

#[derive(Serialize)]
pub struct FileInfo { pub hash: String, pub filename: String, pub size: u64 }

pub async fn list_files(State(state): State<AppState>) -> Result<Json<Vec<FileInfo>>> {
    let hashes = state.storage.list_hashes()?;
    let files = hashes.into_iter()
        .filter_map(|hash| {
            state.storage.get(&hash).ok().map(|data| FileInfo {
                size: data.len() as u64,
                filename: hash.clone(),
                hash,
            })
        })
        .collect();
    Ok(Json(files))
}

pub async fn clear_storage(State(state): State<AppState>) -> impl IntoResponse {
    let _ = state.storage.clear();
    StatusCode::NO_CONTENT
}

