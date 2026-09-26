use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::checksum;
use crate::device::DeviceManager;
use crate::error::{Result, ServerError};
use crate::storage::Storage;
use crate::types::{
    DeviceInfo,
    DeviceRegisterRequest,
    PairingCodeResponse,
    SyncRoot,
    UploadResponse,
};

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
        Self {
            storage,
            devices,
            notification_tx,
            screenshare: crate::screenshare_rest::RoomManager::new(),
            ice_servers: std::sync::Arc::new(serde_json::json!([])),
        }
    }
    /// Set the ICE server list handed out to screenshare clients.
    pub fn with_ice_servers(mut self, ice: serde_json::Value) -> Self {
        self.ice_servers = std::sync::Arc::new(ice);
        self
    }
    pub(crate) fn auth_user(&self, headers: &HeaderMap) -> Result<String> {
        self.devices.validate_token(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .ok_or(ServerError::Unauthorized)?,
        )
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
pub struct PutRootResponse {
    pub generation: u64,
    pub hash: String,
}

/// Sync v3 root update: compare-and-swap on `generation`, 412 if another client got there first.
pub async fn put_root(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PutRootRequest>,
) -> Result<Json<PutRootResponse>> {
    let (user_id, device_id, _) = state.devices.caller(bearer_header(&headers)?)?;
    if !crate::storage::is_valid_hash(&req.hash) {
        return Err(ServerError::InvalidHash(req.hash));
    }
    if !state.storage.exists(&req.hash) {
        return Err(ServerError::NotFound(format!(
            "root index {} not uploaded",
            req.hash
        )));
    }
    let root = state.storage.set_root_if(req.hash, Some(req.generation))?;
    if req.broadcast {
        let _ = state
            .notification_tx
            .send(crate::notifications::WsMessage::sync_complete(
                root.generation,
                &device_id,
                &user_id,
            ));
    }
    Ok(Json(PutRootResponse {
        generation: root.generation,
        hash: root.hash,
    }))
}

#[derive(Deserialize)]
pub struct CheckFilesRequest {
    #[serde(default)]
    pub files: Vec<String>,
}

#[derive(Serialize)]
pub struct CheckFilesResponse {
    #[serde(rename = "missingFiles")]
    pub missing_files: Vec<String>,
}

/// Which of the listed blobs the server doesn't have.
///
/// The client won't re-upload a blob reported present, so every listed blob is touched
/// (pulled inside GC's grace window) *before* existence is checked: a blob GC deletes
/// first is then reported missing, and one touched first is kept by GC's re-check. If the
/// touch fails this is a 500 so the client retries instead of trusting an unprotected blob.
pub async fn check_files(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CheckFilesRequest>,
) -> Result<Json<CheckFilesResponse>> {
    state.auth_user(&headers)?;
    state.storage.touch(&req.files).map_err(|e| {
        tracing::error!(error = %e, "could not mark checked blobs as recently used");
        ServerError::Internal("could not mark blobs as in use; retry".into())
    })?;
    let missing_files = req
        .files
        .into_iter()
        .filter(|h| !state.storage.exists(h))
        .collect();
    Ok(Json(CheckFilesResponse { missing_files }))
}

#[derive(Serialize)]
pub struct MissingResponse {
    pub hashes: Vec<String>,
}

/// Blobs referenced from the current root's tree that aren't stored.
pub async fn missing_blobs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<MissingResponse>> {
    state.auth_user(&headers)?;
    Ok(Json(MissingResponse {
        hashes: state.storage.missing_from_root()?,
    }))
}

/// Every stored blob hash (rm_api uses this to skip per-file existence checks).
pub async fn files_list(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<String>>> {
    state.auth_user(&headers)?;
    Ok(Json(state.storage.list_hashes()?))
}

pub async fn get_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response> {
    state.auth_user(&headers)?;
    let _filename = headers
        .get("rm-filename")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    let data = state.storage.get(&hash)?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CONTENT_LENGTH, &data.len().to_string()),
        ],
        [(
            header::HeaderName::from_static("x-goog-hash"),
            checksum::format_goog_hash(&data),
        )],
        data,
    )
        .into_response())
}

pub async fn put_file(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<UploadResponse>> {
    let _user_id = state.auth_user(&headers)?;
    let filename = headers
        .get("rm-filename")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::MissingHeader("rm-filename".into()))?;
    // Verify transport integrity via crc32c when the client sends it; the sha256 in
    // the URL can't be checked against the body (index hashes are over child hashes).
    checksum::verify_goog_hash_header(&headers, &body)?;
    state.storage.put_with_hash(&body, &hash, filename)?;
    Ok(Json(UploadResponse {
        hash,
        size: body.len() as u64,
    }))
}

/// Default account for pairing codes: the single local account, as `--pair` (main.rs `PAIRING_USER`) issues codes for.
const PAIRING_USER: &str = "local-user";

#[derive(Deserialize, Default)]
pub struct PairingQuery {
    user: Option<String>,
}

/// A user id we're willing to bind a pairing code to: 1..=254 printable ASCII chars (an email's max
/// length), no whitespace or path separators. Covers `local-user`, `auth0|...` and any email that
/// `/admin/create-user` accepts, including `+` tags (send `+` as `%2B` in the query string).
fn valid_user_id(u: &str) -> bool {
    !u.is_empty()
        && u.len() <= 254
        && u.bytes()
            .all(|b| b.is_ascii_graphic() && b != b'/' && b != b'\\')
}

/// `POST /devices/v1[?user=<id>]` -> a one-time pairing code for `user` (default `local-user`, same as `--pair`).
/// Owner only (`x-admin-token`): a code registers a new device, and a second device of the same user can
/// approve the first one's passcode reset, so an ordinary device/user token must not be able to mint one.
/// `--pair` on the CLI is the other way.
pub async fn create_pairing_code(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<PairingQuery>,
) -> Result<Json<PairingCodeResponse>> {
    pairing_code_with(
        &state,
        &headers,
        admin_token().as_deref(),
        q.user.as_deref(),
    )
}

fn pairing_code_with(
    state: &AppState,
    headers: &HeaderMap,
    admin: Option<&str>,
    user: Option<&str>,
) -> Result<Json<PairingCodeResponse>> {
    check_admin(admin, headers)?;
    let user = user.unwrap_or(PAIRING_USER);
    if !valid_user_id(user) {
        return Err(ServerError::BadRequest("user must be 1-254 printable ASCII chars without spaces, '/' or '\\' (send '+' as %2B)".into()));
    }
    let code = state.devices.create_pairing_code(user)?;
    tracing::info!(%user, "pairing code issued via admin endpoint");
    Ok(Json(PairingCodeResponse {
        code,
        expires_in: 600,
    }))
}

/// The caller's own devices only.
pub async fn list_devices(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<DeviceInfo>>> {
    let user_id = state.auth_user(&headers)?;
    let devices = state
        .devices
        .list_devices(Some(&user_id))?
        .into_iter()
        .map(|d| DeviceInfo {
            device_id: d.device_id,
            device_desc: d.device_desc,
            registered_at: d.registered_at.to_rfc3339(),
            last_activity: d.last_refresh.to_rfc3339(),
        })
        .collect();
    Ok(Json(devices))
}

/// Unregister one of the caller's own devices; anyone else's (or an unknown id) is a 404.
pub async fn delete_device(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    let user_id = state.auth_user(&headers)?;
    if !state.devices.delete_device(&id, Some(&user_id))? {
        return Err(ServerError::NotFound(id));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// The raw `Authorization` header value (`Bearer ...`).
fn bearer_header(headers: &HeaderMap) -> Result<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::Unauthorized)
}

/// Extract the bearer token from the Authorization header.
fn bearer(headers: &HeaderMap) -> Result<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ServerError::Unauthorized)
}

/// Device token -> user token. Like the real cloud, the body is the bare JWT, not JSON.
pub async fn refresh_token(State(state): State<AppState>, headers: HeaderMap) -> Result<String> {
    state.devices.refresh_user_token(bearer(&headers)?)
}

/// Pairing code -> device token (plain-text body, as the device expects).
pub async fn register_device(
    State(state): State<AppState>,
    Json(req): Json<DeviceRegisterRequest>,
) -> Result<String> {
    let (device_token, _user_token) =
        state
            .devices
            .exchange_code(&req.code, &req.device_id, &req.device_desc)?;
    Ok(device_token)
}

pub async fn delete_device_token(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    state.devices.revoke_device_token(bearer(&headers)?)?;
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

pub async fn health() -> &'static str {
    "OK"
}

#[derive(Serialize)]
pub struct FileInfo {
    pub hash: String,
    pub filename: String,
    pub size: usize,
}

pub async fn list_files(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<FileInfo>>> {
    state.auth_user(&headers)?;
    Ok(Json(
        state
            .storage
            .list()
            .into_iter()
            .map(|(hash, filename, size)| FileInfo {
                hash,
                filename,
                size,
            })
            .collect(),
    ))
}

/// Admin: delete every blob and reset the root. A device token is not enough: any paired
/// tablet or client could otherwise wipe the cloud.
pub async fn clear_storage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    require_admin(&headers)?;
    state.storage.clear()?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct UnreachableQuery {
    pub grace_secs: Option<u64>,
}

#[derive(Serialize)]
pub struct UnreachableResponse {
    #[serde(rename = "graceSecs")]
    pub grace_secs: u64,
    pub hashes: Vec<String>,
}

/// Admin: blobs no longer reachable from the current root (default grace 24h). Report only;
/// nothing is deleted. Includes server-side copies outside the tree, e.g. restored versions.
pub async fn unreachable_blobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<UnreachableQuery>,
) -> Result<Json<UnreachableResponse>> {
    require_admin(&headers)?;
    let grace_secs = q.grace_secs.unwrap_or(24 * 60 * 60);
    let hashes = state
        .storage
        .unreachable_blobs(std::time::Duration::from_secs(grace_secs))?;
    Ok(Json(UnreachableResponse { grace_secs, hashes }))
}

/// Default grace for `POST /admin/storage/gc`: 7 days.
const GC_DEFAULT_GRACE_SECS: u64 = 7 * 24 * 60 * 60;

#[derive(Deserialize)]
pub struct GcQuery {
    pub grace_secs: Option<u64>,
    /// Defaults to `true`: deleting takes an explicit `dry_run=false`.
    pub dry_run: Option<bool>,
}

/// Admin: delete blobs unreachable from the current and previous roots, outside version
/// history and untouched for `grace_secs` (default 7 days). A dry run unless
/// `dry_run=false`. Refuses (500) if the current tree isn't fully parsed, and answers 409
/// with the partial report if a sync committed a new root while deleting.
pub async fn storage_gc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<GcQuery>,
) -> Result<Response> {
    require_admin(&headers)?;
    run_gc(&state, q).await
}

async fn run_gc(state: &AppState, q: GcQuery) -> Result<Response> {
    let grace = std::time::Duration::from_secs(q.grace_secs.unwrap_or(GC_DEFAULT_GRACE_SECS));
    let dry_run = q.dry_run.unwrap_or(true);
    let storage = state.storage.clone();
    let report = tokio::task::spawn_blocking(move || storage.gc(grace, dry_run))
        .await
        .map_err(|e| ServerError::Internal(format!("gc task failed: {e}")))??;
    let status = if report.aborted.is_some() {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    Ok((status, Json(report)).into_response())
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
}

/// Admin-only endpoints are disabled unless `ADMIN_TOKEN` is set; callers must send it
/// in `x-admin-token`.
pub(crate) fn require_admin(headers: &HeaderMap) -> Result<()> {
    check_admin(admin_token().as_deref(), headers)
}

/// The configured `ADMIN_TOKEN`, if set and non-empty.
pub(crate) fn admin_token() -> Option<String> {
    std::env::var("ADMIN_TOKEN").ok().filter(|t| !t.is_empty())
}

/// `require_admin` against an explicit expected token (`None` = admin disabled), so callers
/// can be tested without touching the process environment.
pub(crate) fn check_admin(expected: Option<&str>, headers: &HeaderMap) -> Result<()> {
    let expected = expected
        .filter(|t| !t.is_empty())
        .ok_or(ServerError::Unauthorized)?;
    let given = headers
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::Unauthorized)?;
    if given.as_bytes() != expected.as_bytes() {
        return Err(ServerError::Unauthorized);
    }
    Ok(())
}

/// Mint a user token. Only enabled when the `ADMIN_TOKEN` env var is set, and the
/// request must carry it in `x-admin-token`. (Use `--pair` on the CLI for pairing codes.)
pub async fn create_test_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Result<Json<TokenResponse>> {
    require_admin(&headers)?;
    let token = state.devices.create_user_token(&req.email)?;
    Ok(Json(TokenResponse { token }))
}

#[derive(Serialize)]
pub struct TokenResponse {
    pub token: String,
}

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

pub async fn service_locator(
    State(state): State<AppState>,
    Path(service): Path<String>,
) -> Json<ServiceResponse> {
    let host = state.devices.get_endpoint();
    // blob-storage is the one service clients expect as a full URL (matches rmfakecloud)
    let host = if service == "blob-storage" {
        format!("https://{host}")
    } else {
        host
    };
    Json(ServiceResponse { host, status: "OK" })
}

pub async fn check_updates() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "available": false
    }))
}

#[cfg(test)]
mod pairing_tests {
    use axum::http::HeaderValue;

    use super::*;

    // Passed to `pairing_code_with` directly; the process `ADMIN_TOKEN` env var is never set.
    const ADMIN: &str = "pairing-test-admin-token";

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (
            AppState::new(Storage::new(tmp.path()).unwrap(), devices),
            tmp,
        )
    }
    fn hdr(name: &'static str, v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(name, HeaderValue::from_str(v).unwrap());
        h
    }
    fn status_of<T>(r: Result<T>) -> StatusCode {
        r.err().expect("expected an error").into_response().status()
    }

    #[tokio::test]
    async fn device_and_user_tokens_cannot_mint_pairing_codes() {
        use tower::ServiceExt;
        let (state, _tmp) = setup();
        let (dev, _) = state
            .devices
            .exchange_code(
                &state.devices.create_pairing_code(PAIRING_USER).unwrap(),
                "tablet-a",
                "remarkable",
            )
            .unwrap();
        let user = state.devices.create_user_token(PAIRING_USER).unwrap();
        for tok in [dev.as_str(), user.as_str()] {
            let h = hdr("authorization", &format!("Bearer {tok}"));
            // even with admin enabled, a Bearer token is not the admin credential
            assert_eq!(
                status_of(pairing_code_with(&state, &h, Some(ADMIN), None)),
                StatusCode::UNAUTHORIZED
            );
            // and through the real route (public handler + router, as mounted in main.rs)
            let req = axum::http::Request::post("/devices/v1")
                .header(header::AUTHORIZATION, format!("Bearer {tok}"))
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(
                crate::create_router(state.clone())
                    .oneshot(req)
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED
            );
        }
        let req = axum::http::Request::post("/devices/v1")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            crate::create_router(state.clone())
                .oneshot(req)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // wrong admin token, or admin disabled (no ADMIN_TOKEN configured)
        assert_eq!(
            status_of(pairing_code_with(
                &state,
                &hdr("x-admin-token", "wrong"),
                Some(ADMIN),
                None
            )),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_of(pairing_code_with(
                &state,
                &hdr("x-admin-token", ADMIN),
                None,
                None
            )),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_of(pairing_code_with(
                &state,
                &hdr("x-admin-token", ""),
                Some(""),
                None
            )),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn admin_token_issues_a_code_that_pairs_the_local_user() {
        let (state, _tmp) = setup();
        let Json(r) =
            pairing_code_with(&state, &hdr("x-admin-token", ADMIN), Some(ADMIN), None).unwrap();
        assert_eq!((r.code.len(), r.expires_in), (8, 600));
        let (dev, _) = state
            .devices
            .exchange_code(&r.code, "tablet-b", "remarkable")
            .unwrap();
        assert_eq!(
            state
                .devices
                .validate_token(&format!("Bearer {dev}"))
                .unwrap(),
            PAIRING_USER
        );
        assert!(
            state
                .devices
                .exchange_code(&r.code, "tablet-c", "remarkable")
                .is_err(),
            "single use"
        );
    }

    #[tokio::test]
    async fn admin_can_pair_an_explicit_user() {
        let (state, _tmp) = setup();
        let admin = hdr("x-admin-token", ADMIN);
        let Json(r) =
            pairing_code_with(&state, &admin, Some(ADMIN), Some("auth0|alice-2")).unwrap();
        let (dev, _) = state
            .devices
            .exchange_code(&r.code, "tablet-d", "remarkable")
            .unwrap();
        assert_eq!(
            state
                .devices
                .validate_token(&format!("Bearer {dev}"))
                .unwrap(),
            "auth0|alice-2"
        );
        // email-style ids with `+` tags (as `/admin/create-user` accepts) pair fine
        let Json(r) =
            pairing_code_with(&state, &admin, Some(ADMIN), Some("john+tablet@example.com"))
                .unwrap();
        let (dev, _) = state
            .devices
            .exchange_code(&r.code, "tablet-e", "remarkable")
            .unwrap();
        assert_eq!(
            state
                .devices
                .validate_token(&format!("Bearer {dev}"))
                .unwrap(),
            "john+tablet@example.com"
        );
        for bad in ["", "a b", "x/y", "x\\y", "tab\there", &"u".repeat(255)] {
            assert_eq!(
                status_of(pairing_code_with(&state, &admin, Some(ADMIN), Some(bad))),
                StatusCode::BAD_REQUEST,
                "{bad:?}"
            );
        }
        // auth is checked before the user id: a bad id without the admin token is still 401
        assert_eq!(
            status_of(pairing_code_with(
                &state,
                &HeaderMap::new(),
                Some(ADMIN),
                Some("")
            )),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn user_query_through_the_router_still_needs_admin() {
        use tower::ServiceExt;
        let (state, _tmp) = setup();
        // `?user=` parses through the real router; without an admin token it's still 401 (ADMIN_TOKEN is never set in tests)
        for uri in [
            "/devices/v1?user=bob",
            "/devices/v1?user=",
            "/devices/v1?other=1",
        ] {
            let req = axum::http::Request::post(uri)
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(
                crate::create_router(state.clone())
                    .oneshot(req)
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED,
                "{uri}"
            );
        }
    }
}

#[cfg(test)]
mod device_ownership_tests {
    use axum::http::HeaderValue;

    use super::*;

    fn hdrs(tk: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {tk}")).unwrap(),
        );
        h
    }
    /// Pair `device` to `user` and return that device's token.
    fn pair(state: &AppState, user: &str, device: &str) -> String {
        let code = state.devices.create_pairing_code(user).unwrap();
        state
            .devices
            .exchange_code(&code, device, "remarkable")
            .unwrap()
            .0
    }

    #[tokio::test]
    async fn users_only_see_and_delete_their_own_devices() {
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let state = AppState::new(Storage::new(tmp.path()).unwrap(), devices);
        let a_dev = pair(&state, "user-a", "RM110-A");
        let b_dev = pair(&state, "user-b", "RM110-B");
        let a = state.devices.create_user_token("user-a").unwrap();
        let ids = |v: Vec<DeviceInfo>| v.into_iter().map(|d| d.device_id).collect::<Vec<_>>();
        let Json(listed) = list_devices(State(state.clone()), hdrs(&a)).await.unwrap();
        assert_eq!(ids(listed), ["RM110-A"]);
        // B's device token authenticates as B and sees only B's device.
        let Json(listed) = list_devices(State(state.clone()), hdrs(&b_dev))
            .await
            .unwrap();
        assert_eq!(ids(listed), ["RM110-B"]);
        // A can't delete B's device: 404, and B's registration and token survive.
        let err = delete_device(State(state.clone()), Path("RM110-B".into()), hdrs(&a))
            .await
            .unwrap_err();
        assert!(matches!(err, ServerError::NotFound(_)), "{err:?}");
        assert!(state.devices.get_device("RM110-B").unwrap().is_some());
        assert_eq!(
            state
                .devices
                .validate_token(&format!("Bearer {b_dev}"))
                .unwrap(),
            "user-b"
        );
        // Own device: 204, and its token is revoked.
        assert_eq!(
            delete_device(State(state.clone()), Path("RM110-A".into()), hdrs(&a))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(
            state
                .devices
                .validate_token(&format!("Bearer {a_dev}"))
                .is_err()
        );
        let Json(listed) = list_devices(State(state.clone()), hdrs(&a)).await.unwrap();
        assert!(listed.is_empty());
        assert!(matches!(
            delete_device(State(state.clone()), Path("RM110-A".into()), hdrs(&a)).await,
            Err(ServerError::NotFound(_))
        ));
        // Admin-side (owner = None) still sees everything.
        assert_eq!(
            state
                .devices
                .list_devices(None)
                .unwrap()
                .iter()
                .map(|d| d.device_id.as_str())
                .collect::<Vec<_>>(),
            ["RM110-B"]
        );
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[tokio::test]
    async fn non_admin_token_cannot_clear_storage() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let hash = storage.put(b"keep me", "doc.pdf").unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let token = devices.create_user_token("user").unwrap();
        let state = AppState::new(storage, devices);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );

        let result = clear_storage(State(state.clone()), headers).await;
        assert!(matches!(result, Err(ServerError::Unauthorized)));
        assert!(state.storage.exists(&hash));
    }

    fn state_with_user(tmp: &tempfile::TempDir) -> (AppState, HeaderMap) {
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let token = devices.create_user_token("user").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        (AppState::new(storage, devices), headers)
    }

    /// Backdate every blob's row and file far past any grace period.
    fn age_all(tmp: &tempfile::TempDir, storage: &Storage) {
        let db = rusqlite::Connection::open(tmp.path().join("sync.db")).unwrap();
        db.execute("UPDATE blobs SET updated_at = 0", []).unwrap();
        for hash in storage.list_hashes().unwrap() {
            std::fs::File::options()
                .write(true)
                .open(tmp.path().join(&hash[..2]).join(&hash))
                .unwrap()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000))
                .unwrap();
        }
    }

    #[tokio::test]
    async fn check_files_protects_present_blobs_from_gc() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, headers) = state_with_user(&tmp);
        crate::documents::create_document(&state.storage, "Doc", "pdf", b"%PDF-1.4 doc").unwrap();
        let orphan = state.storage.put(b"orphan", "o.rm").unwrap();
        age_all(&tmp, &state.storage);
        let absent = "a".repeat(64);

        let Json(r) = check_files(
            State(state.clone()),
            headers,
            Json(CheckFilesRequest {
                files: vec![orphan.clone(), absent.clone()],
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.missing_files, vec![absent]);
        // The client now relies on `orphan` without re-uploading it: GC must keep it.
        let report = state
            .storage
            .gc(std::time::Duration::from_secs(3600), false)
            .unwrap();
        assert_eq!(report.deleted, 0);
        assert!(state.storage.exists(&orphan));
    }

    #[tokio::test]
    async fn check_files_fails_when_touch_fails() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, headers) = state_with_user(&tmp);
        let hash = state.storage.put(b"present", "p.rm").unwrap();
        rusqlite::Connection::open(tmp.path().join("sync.db"))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER no_touch BEFORE UPDATE ON blobs BEGIN SELECT RAISE(FAIL, 'boom'); END;",
            )
            .unwrap();

        let err = check_files(
            State(state),
            headers,
            Json(CheckFilesRequest { files: vec![hash] }),
        )
        .await
        .err()
        .expect("touch failure must not report the blob present");
        assert_eq!(
            err.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn gc_endpoint_needs_admin_and_defaults_to_dry_run() {
        use tower::ServiceExt;
        let tmp = tempfile::TempDir::new().unwrap();
        let (state, headers) = state_with_user(&tmp);
        crate::documents::create_document(&state.storage, "Doc", "pdf", b"%PDF-1.4 doc").unwrap();
        let orphan = state.storage.put(b"orphan", "o.rm").unwrap();
        age_all(&tmp, &state.storage);

        // A device/user token is not the admin credential (ADMIN_TOKEN is never set in tests).
        let mut req = axum::http::Request::post("/admin/storage/gc?dry_run=false&grace_secs=0");
        req.headers_mut().unwrap().extend(headers);
        let status = crate::create_router(state.clone())
            .oneshot(req.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(state.storage.exists(&orphan));

        // No dry_run parameter: report only.
        let default = GcQuery {
            grace_secs: None,
            dry_run: None,
        };
        let resp = run_gc(&state, default).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let report: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["dryRun"], true);
        assert_eq!(report["graceSecs"], GC_DEFAULT_GRACE_SECS);
        assert_eq!(report["hashes"], serde_json::json!([orphan]));
        assert_eq!(report["deleted"], 0);
        assert!(state.storage.exists(&orphan));

        let run = GcQuery {
            grace_secs: Some(3600),
            dry_run: Some(false),
        };
        assert_eq!(run_gc(&state, run).await.unwrap().status(), StatusCode::OK);
        assert!(!state.storage.exists(&orphan));
        assert!(state.storage.missing_from_root().unwrap().is_empty());
    }
}
