use axum::{body::Bytes, extract::{Path, State}, http::{header, HeaderMap, StatusCode}, response::{IntoResponse, Response}, Json};
use crate::{checksum, device::DeviceManager, error::{Result, ServerError}, storage::Storage};
use crate::types::{DeviceInfo, DeviceRegisterRequest, PairingCodeResponse, SyncRoot, UploadResponse};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState { pub storage: Storage, pub devices: DeviceManager }

impl AppState {
    pub fn new(storage: Storage, devices: DeviceManager) -> Self { Self { storage, devices } }
    fn auth_user(&self, headers: &HeaderMap) -> Result<String> {
        self.devices.validate_token(headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?)
    }
}

pub async fn get_root(State(state): State<AppState>) -> Json<SyncRoot> { Json(state.storage.get_root()) }

pub async fn get_file(State(state): State<AppState>, Path(hash): Path<String>, headers: HeaderMap) -> Result<Response> {
    let _filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    let data = state.storage.get(&hash)?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/octet-stream"), (header::CONTENT_LENGTH, &data.len().to_string())], [(header::HeaderName::from_static("x-goog-hash"), checksum::format_goog_hash(&data))], data).into_response())
}

pub async fn put_file(State(state): State<AppState>, Path(hash): Path<String>, headers: HeaderMap, body: Bytes) -> Result<Json<UploadResponse>> {
    let _user_id = state.auth_user(&headers)?;
    let filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    let stored_hash = state.storage.put(&body, filename)?;
    if stored_hash != hash { return Err(ServerError::ChecksumMismatch { expected: hash, actual: stored_hash }); }
    Ok(Json(UploadResponse { hash: stored_hash, size: body.len() as u64 }))
}

pub async fn create_pairing_code(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<PairingCodeResponse>> {
    let user_id = state.auth_user(&headers)?;
    let code = state.devices.create_pairing_code(&user_id)?;
    Ok(Json(PairingCodeResponse { code, expires_in: 600 }))
}

pub async fn list_devices(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Vec<DeviceInfo>>> {
    let _user_id = state.auth_user(&headers)?;
    let devices = state.devices.list_devices()?.into_iter().map(|d| DeviceInfo {
        device_id: d.device_id,
        device_desc: d.device_desc,
        registered_at: d.registered_at.to_rfc3339(),
        last_activity: d.last_refresh.to_rfc3339(),
    }).collect();
    Ok(Json(devices))
}

pub async fn delete_device(State(state): State<AppState>, Path(id): Path<String>, headers: HeaderMap) -> Result<StatusCode> {
    let _user_id = state.auth_user(&headers)?;
    state.devices.delete_device(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn refresh_token(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<TokenResponse>> {
    let device_token = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    let user_token = state.devices.refresh_user_token(device_token)?;
    Ok(Json(TokenResponse { token: user_token }))
}

pub async fn register_device(State(state): State<AppState>, Json(req): Json<DeviceRegisterRequest>) -> Result<Json<RegisterResponse>> {
    let (device_token, user_token) = state.devices.exchange_code(&req.code, &req.device_id, &req.device_desc)?;
    Ok(Json(RegisterResponse { device_token, user_token }))
}

pub async fn delete_device_token(State(state): State<AppState>, headers: HeaderMap) -> Result<StatusCode> {
    let device_token = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    state.devices.delete_device(device_token)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn discovery(State(state): State<AppState>) -> Json<DiscoveryResponse> {
    let host = state.devices.get_endpoint();
    Json(DiscoveryResponse {
        status: "OK".into(),
        sync: format!("https://{}", host),
        device: format!("https://{}", host),
        mqtt: format!("wss://{}/notifications/ws/json/1", host),
    })
}

pub async fn health() -> &'static str { "OK" }

#[derive(Serialize)]
pub struct FileInfo { pub hash: String, pub filename: String, pub size: usize }

pub async fn list_files(State(state): State<AppState>) -> Json<Vec<FileInfo>> {
    Json(state.storage.list().into_iter().map(|(hash, filename, size)| FileInfo { hash, filename, size }).collect())
}

pub async fn clear_storage(State(state): State<AppState>) -> Result<StatusCode> {
    state.storage.clear()?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct CreateUserRequest { pub email: String }

pub async fn create_test_user(State(state): State<AppState>, Json(req): Json<CreateUserRequest>) -> Result<Json<TokenResponse>> {
    let token = state.devices.create_user_token(&req.email)?;
    Ok(Json(TokenResponse { token }))
}

#[derive(Serialize)]
pub struct TokenResponse { pub token: String }

#[derive(Serialize)]
pub struct RegisterResponse { pub device_token: String, pub user_token: String }

#[derive(Serialize)]
pub struct DiscoveryResponse { pub status: String, pub sync: String, pub device: String, pub mqtt: String }
