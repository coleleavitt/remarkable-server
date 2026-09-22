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
    let filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    if let Some(gh) = headers.get("x-goog-hash").and_then(|v| v.to_str().ok()) { if !checksum::verify_checksum(&body, gh) { return Err(ServerError::ChecksumMismatch { expected: gh.into(), actual: checksum::format_goog_hash(&body) }); } }
    state.storage.put_with_hash(&body, &hash, filename)?;
    if let Some(ph) = headers.get("rm-parent-hash").and_then(|v| v.to_str().ok()) { if ph == "root" || ph.is_empty() { state.storage.set_root(hash.clone())?; } }
    Ok(Json(UploadResponse { hash, size: body.len() as u64 }))
}

pub async fn create_pairing_code(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<PairingCodeResponse>> {
    let user_id = state.auth_user(&headers)?;
    Ok(Json(PairingCodeResponse { code: state.devices.create_pairing_code(&user_id)?, expires_in: 600 }))
}

pub async fn list_devices(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Vec<DeviceInfo>>> {
    let _ = state.auth_user(&headers)?;
    Ok(Json(state.devices.list_devices()?.into_iter().map(|d| DeviceInfo { device_id: d.device_id, device_desc: d.device_desc, registered_at: d.registered_at.to_rfc3339(), last_activity: d.last_refresh.to_rfc3339() }).collect()))
}

pub async fn delete_device(State(state): State<AppState>, Path(id): Path<String>, headers: HeaderMap) -> Result<impl IntoResponse> {
    let _ = state.auth_user(&headers)?;
    if state.devices.delete_device(&id)? { Ok(StatusCode::NO_CONTENT) } else { Err(ServerError::NotFound(id)) }
}

pub async fn refresh_token(State(state): State<AppState>, headers: HeaderMap) -> Result<impl IntoResponse> {
    let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    Ok(state.devices.refresh_user_token(auth.strip_prefix("Bearer ").ok_or(ServerError::Unauthorized)?)?)
}

pub async fn register_device(State(state): State<AppState>, Json(body): Json<DeviceRegisterRequest>) -> Result<impl IntoResponse> {
    Ok(state.devices.exchange_code(&body.code, &body.device_id, &body.device_desc)?.0)
}

pub async fn delete_device_token(State(_): State<AppState>, _: HeaderMap) -> Result<impl IntoResponse> { Ok(StatusCode::OK) }

#[derive(Serialize)] pub struct DiscoveryEndpoints { #[serde(rename = "Host")] pub host: String, #[serde(rename = "Status")] pub status: String }
pub async fn discovery(headers: HeaderMap) -> Json<DiscoveryEndpoints> { Json(DiscoveryEndpoints { host: headers.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("localhost:8080").into(), status: "OK".into() }) }

#[derive(Serialize)] pub struct HealthResponse { pub status: String, pub storage: StorageInfo, pub devices: DevicesInfo }
#[derive(Serialize)] pub struct StorageInfo { pub file_count: usize, pub total_bytes: u64, pub root_hash: String, pub generation: u64 }
#[derive(Serialize)] pub struct DevicesInfo { pub registered_count: usize }
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let s = state.storage.stats(); let d = state.devices.list_devices().unwrap_or_default();
    Json(HealthResponse { status: "ok".into(), storage: StorageInfo { file_count: s.file_count, total_bytes: s.total_bytes, root_hash: s.root_hash, generation: s.generation }, devices: DevicesInfo { registered_count: d.len() } })
}

pub async fn list_files(State(state): State<AppState>) -> Result<Json<Vec<String>>> { Ok(Json(state.storage.list_hashes()?)) }
pub async fn clear_storage(State(state): State<AppState>) -> Result<impl IntoResponse> { state.storage.clear()?; Ok(StatusCode::NO_CONTENT) }

#[derive(Deserialize)] pub struct CreateUserRequest { pub user_id: Option<String> }
#[derive(Serialize)] pub struct CreateUserResponse { pub user_id: String, pub device_token: String, pub user_token: String }
pub async fn create_test_user(State(state): State<AppState>, Json(body): Json<CreateUserRequest>) -> Result<Json<CreateUserResponse>> {
    let user_id = body.user_id.unwrap_or_else(|| format!("local|{}", uuid::Uuid::new_v4()));
    let code = state.devices.create_pairing_code(&user_id)?;
    let (dt, ut) = state.devices.exchange_code(&code, &format!("TEST-{}", &uuid::Uuid::new_v4().to_string()[..8]), "test-client")?;
    Ok(Json(CreateUserResponse { user_id, device_token: dt, user_token: ut }))
}
