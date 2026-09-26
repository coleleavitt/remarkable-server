use axum::{body::Bytes, extract::{Path, State}, http::{header, HeaderMap, StatusCode}, response::{IntoResponse, Response}, Json};
use crate::{checksum, device::DeviceManager, error::{Result, ServerError}, storage::Storage};
use crate::types::{DeviceInfo, DeviceRegisterRequest, PairingCodeResponse, SyncRoot, UploadResponse};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState { 
    pub storage: Storage, 
    pub devices: DeviceManager,
    pub notification_tx: tokio::sync::broadcast::Sender<crate::notifications::WsMessage>,
    pub screenshare: crate::screenshare_rest::RoomManager,
    pub ice_servers: std::sync::Arc<serde_json::Value>,
}

impl AppState {
    pub fn new(storage: Storage, devices: DeviceManager) -> Self { 
        let (notification_tx, _) = tokio::sync::broadcast::channel(64);
        Self { storage, devices, notification_tx, screenshare: crate::screenshare_rest::RoomManager::new(), ice_servers: std::sync::Arc::new(serde_json::json!([])) } 
    }
    /// Set the ICE server list handed out to screenshare clients.
    pub fn with_ice_servers(mut self, ice: serde_json::Value) -> Self { self.ice_servers = std::sync::Arc::new(ice); self }
    pub(crate) fn auth_user(&self, headers: &HeaderMap) -> Result<String> {
        self.devices.validate_token(headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?)
    }
}

pub async fn get_root(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<SyncRoot>> {
    state.auth_user(&headers)?;
    let root = state.storage.get_root();
    if root.hash.is_empty() {
        return Err(ServerError::NotFound("root not found".into()));
    }
    Ok(Json(root))
}

#[derive(Deserialize)]
pub struct PutRootRequest {
    pub generation: u64,
    pub hash: String,
    #[serde(default)]
    pub broadcast: bool,
}

#[derive(Serialize)]
pub struct PutRootResponse { pub generation: u64, pub hash: String }

/// Sync v3 root update: compare-and-swap on `generation`, 412 if another client got there first.
pub async fn put_root(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<PutRootRequest>) -> Result<Json<PutRootResponse>> {
    let (user_id, device_id, _) = state.devices.caller(bearer_header(&headers)?)?;
    if !crate::storage::is_valid_hash(&req.hash) {
        return Err(ServerError::InvalidHash(req.hash));
    }
    if !state.storage.exists(&req.hash) {
        return Err(ServerError::NotFound(format!("root index {} not uploaded", req.hash)));
    }
    let root = state.storage.set_root_if(req.hash, Some(req.generation))?;
    if req.broadcast {
        let _ = state.notification_tx.send(crate::notifications::WsMessage::sync_complete(root.generation, &device_id, &user_id));
    }
    Ok(Json(PutRootResponse { generation: root.generation, hash: root.hash }))
}

#[derive(Deserialize)]
pub struct CheckFilesRequest { #[serde(default)] pub files: Vec<String> }

#[derive(Serialize)]
pub struct CheckFilesResponse { #[serde(rename = "missingFiles")] pub missing_files: Vec<String> }

/// Which of the listed blobs the server doesn't have.
pub async fn check_files(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<CheckFilesRequest>) -> Result<Json<CheckFilesResponse>> {
    state.auth_user(&headers)?;
    let missing_files = req.files.into_iter().filter(|h| !state.storage.exists(h)).collect();
    Ok(Json(CheckFilesResponse { missing_files }))
}

#[derive(Serialize)]
pub struct MissingResponse { pub hashes: Vec<String> }

/// Blobs referenced from the current root's tree that aren't stored.
pub async fn missing_blobs(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<MissingResponse>> {
    state.auth_user(&headers)?;
    Ok(Json(MissingResponse { hashes: state.storage.missing_from_root()? }))
}

/// Every stored blob hash (rm_api uses this to skip per-file existence checks).
pub async fn files_list(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Vec<String>>> {
    state.auth_user(&headers)?;
    Ok(Json(state.storage.list_hashes()?))
}

pub async fn get_file(State(state): State<AppState>, Path(hash): Path<String>, headers: HeaderMap) -> Result<Response> {
    state.auth_user(&headers)?;
    let _filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    let data = state.storage.get(&hash)?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/octet-stream"), (header::CONTENT_LENGTH, &data.len().to_string())], [(header::HeaderName::from_static("x-goog-hash"), checksum::format_goog_hash(&data))], data).into_response())
}

pub async fn put_file(State(state): State<AppState>, Path(hash): Path<String>, headers: HeaderMap, body: Bytes) -> Result<Json<UploadResponse>> {
    let _user_id = state.auth_user(&headers)?;
    let filename = headers.get("rm-filename").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    // Verify transport integrity via crc32c when the client sends it; the sha256 in
    // the URL can't be checked against the body (index hashes are over child hashes).
    if let Some(goog) = headers.get("x-goog-hash").and_then(|v| v.to_str().ok()) {
        if checksum::parse_goog_hash(goog).is_some_and(|crc| crc != checksum::crc32c(&body)) {
            return Err(ServerError::ChecksumMismatch { expected: goog.to_string(), actual: checksum::format_goog_hash(&body) });
        }
    }
    state.storage.put_with_hash(&body, &hash, filename)?;
    Ok(Json(UploadResponse { hash, size: body.len() as u64 }))
}

/// The single local account, as `--pair` (main.rs `PAIRING_USER`) issues codes for.
const PAIRING_USER: &str = "local-user";

/// `POST /devices/v1` -> a one-time pairing code. Owner only (`x-admin-token`): a code registers a
/// new device, and a second device of the same user can approve the first one's passcode reset,
/// so an ordinary device/user token must not be able to mint one. `--pair` on the CLI is the other way.
pub async fn create_pairing_code(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<PairingCodeResponse>> {
    pairing_code_with(&state, &headers, admin_token().as_deref())
}

fn pairing_code_with(state: &AppState, headers: &HeaderMap, admin: Option<&str>) -> Result<Json<PairingCodeResponse>> {
    check_admin(admin, headers)?;
    let code = state.devices.create_pairing_code(PAIRING_USER)?;
    tracing::info!("pairing code issued via admin endpoint");
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

/// The raw `Authorization` header value (`Bearer ...`).
fn bearer_header(headers: &HeaderMap) -> Result<&str> {
    headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)
}

/// Extract the bearer token from the Authorization header.
fn bearer(headers: &HeaderMap) -> Result<&str> {
    headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ServerError::Unauthorized)
}

/// Device token -> user token. Like the real cloud, the body is the bare JWT, not JSON.
pub async fn refresh_token(State(state): State<AppState>, headers: HeaderMap) -> Result<String> {
    state.devices.refresh_user_token(bearer(&headers)?)
}

/// Pairing code -> device token (plain-text body, as the device expects).
pub async fn register_device(State(state): State<AppState>, Json(req): Json<DeviceRegisterRequest>) -> Result<String> {
    let (device_token, _user_token) = state.devices.exchange_code(&req.code, &req.device_id, &req.device_desc)?;
    Ok(device_token)
}

pub async fn delete_device_token(State(state): State<AppState>, headers: HeaderMap) -> Result<StatusCode> {
    let device_id = state.devices.device_id_for_token(bearer(&headers)?)?;
    state.devices.delete_device(&device_id)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn discovery(State(state): State<AppState>) -> Json<DiscoveryResponse> {
    // Bare hostname: the device builds wss://{notifications}/notifications/ws/json/1
    // on port 443, so it must be a name the device's /etc/hosts maps to this server
    // and that the server's TLS cert covers. Never the bind address.
    let host = state.devices.get_endpoint();
    Json(DiscoveryResponse {
        notifications: host.clone(),
        webapp: host.clone(),
        mqttbroker: host,
    })
}

pub async fn health() -> &'static str { "OK" }

#[derive(Serialize)]
pub struct FileInfo { pub hash: String, pub filename: String, pub size: usize }

pub async fn list_files(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Vec<FileInfo>>> {
    state.auth_user(&headers)?;
    Ok(Json(state.storage.list().into_iter().map(|(hash, filename, size)| FileInfo { hash, filename, size }).collect()))
}

pub async fn clear_storage(State(state): State<AppState>, headers: HeaderMap) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    state.storage.clear()?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct CreateUserRequest { pub email: String }

/// Admin-only endpoints are disabled unless `ADMIN_TOKEN` is set; callers must send it
/// in `x-admin-token`.
pub(crate) fn require_admin(headers: &HeaderMap) -> Result<()> { check_admin(admin_token().as_deref(), headers) }

/// The configured `ADMIN_TOKEN`, if set and non-empty.
pub(crate) fn admin_token() -> Option<String> { std::env::var("ADMIN_TOKEN").ok().filter(|t| !t.is_empty()) }

/// `require_admin` against an explicit expected token (`None` = admin disabled), so callers
/// can be tested without touching the process environment.
pub(crate) fn check_admin(expected: Option<&str>, headers: &HeaderMap) -> Result<()> {
    let expected = expected.filter(|t| !t.is_empty()).ok_or(ServerError::Unauthorized)?;
    let given = headers.get("x-admin-token").and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    if given.as_bytes() != expected.as_bytes() { return Err(ServerError::Unauthorized); }
    Ok(())
}

/// Mint a user token. Only enabled when the `ADMIN_TOKEN` env var is set, and the
/// request must carry it in `x-admin-token`. (Use `--pair` on the CLI for pairing codes.)
pub async fn create_test_user(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<CreateUserRequest>) -> Result<Json<TokenResponse>> {
    require_admin(&headers)?;
    let token = state.devices.create_user_token(&req.email)?;
    Ok(Json(TokenResponse { token }))
}

#[derive(Serialize)]
pub struct TokenResponse { pub token: String }

/// Discovery response format matching real reMarkable API
/// Device reads 'notifications' field to construct wss://{host}/notifications/ws/json/1
#[derive(Serialize)]
pub struct DiscoveryResponse {
    /// Notifications/sync host (device constructs WebSocket URL from this)
    pub notifications: String,
    /// Webapp host
    pub webapp: String,
    /// MQTT broker host (VerneMQ)
    pub mqttbroker: String,
}

/// Legacy service locator (`/service/json/1/{service}`), still used by desktop
/// clients (rm_api/moss) for document-storage and notifications.
#[derive(Serialize)]
pub struct ServiceResponse {
    #[serde(rename = "Host")]
    pub host: String,
    #[serde(rename = "Status")]
    pub status: &'static str,
}

pub async fn service_locator(State(state): State<AppState>, Path(service): Path<String>) -> Json<ServiceResponse> {
    let host = state.devices.get_endpoint();
    // blob-storage is the one service clients expect as a full URL (matches rmfakecloud)
    let host = if service == "blob-storage" { format!("https://{host}") } else { host };
    Json(ServiceResponse { host, status: "OK" })
}


pub async fn check_updates() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "available": false
    }))
}

#[cfg(test)]
mod pairing_tests {
    use super::*;
    use axum::http::HeaderValue;

    // Passed to `pairing_code_with` directly; the process `ADMIN_TOKEN` env var is never set.
    const ADMIN: &str = "pairing-test-admin-token";

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let devices = DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (AppState::new(Storage::new(tmp.path()).unwrap(), devices), tmp)
    }
    fn hdr(name: &'static str, v: &str) -> HeaderMap { let mut h = HeaderMap::new(); h.insert(name, HeaderValue::from_str(v).unwrap()); h }
    fn status_of<T>(r: Result<T>) -> StatusCode { r.err().expect("expected an error").into_response().status() }

    #[tokio::test]
    async fn device_and_user_tokens_cannot_mint_pairing_codes() {
        use tower::ServiceExt;
        let (state, _tmp) = setup();
        let (dev, _) = state.devices.exchange_code(&state.devices.create_pairing_code(PAIRING_USER).unwrap(), "tablet-a", "remarkable").unwrap();
        let user = state.devices.create_user_token(PAIRING_USER).unwrap();
        for tok in [dev.as_str(), user.as_str()] {
            let h = hdr("authorization", &format!("Bearer {tok}"));
            // even with admin enabled, a Bearer token is not the admin credential
            assert_eq!(status_of(pairing_code_with(&state, &h, Some(ADMIN))), StatusCode::UNAUTHORIZED);
            // and through the real route (public handler + router, as mounted in main.rs)
            let req = axum::http::Request::post("/devices/v1").header(header::AUTHORIZATION, format!("Bearer {tok}")).body(axum::body::Body::empty()).unwrap();
            assert_eq!(crate::create_router(state.clone()).oneshot(req).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        }
        let req = axum::http::Request::post("/devices/v1").body(axum::body::Body::empty()).unwrap();
        assert_eq!(crate::create_router(state.clone()).oneshot(req).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        // wrong admin token, or admin disabled (no ADMIN_TOKEN configured)
        assert_eq!(status_of(pairing_code_with(&state, &hdr("x-admin-token", "wrong"), Some(ADMIN))), StatusCode::UNAUTHORIZED);
        assert_eq!(status_of(pairing_code_with(&state, &hdr("x-admin-token", ADMIN), None)), StatusCode::UNAUTHORIZED);
        assert_eq!(status_of(pairing_code_with(&state, &hdr("x-admin-token", ""), Some(""))), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_token_issues_a_code_that_pairs_the_local_user() {
        let (state, _tmp) = setup();
        let Json(r) = pairing_code_with(&state, &hdr("x-admin-token", ADMIN), Some(ADMIN)).unwrap();
        assert_eq!((r.code.len(), r.expires_in), (8, 600));
        let (dev, _) = state.devices.exchange_code(&r.code, "tablet-b", "remarkable").unwrap();
        assert_eq!(state.devices.validate_token(&format!("Bearer {dev}")).unwrap(), PAIRING_USER);
        assert!(state.devices.exchange_code(&r.code, "tablet-c", "remarkable").is_err(), "single use");
    }
}
