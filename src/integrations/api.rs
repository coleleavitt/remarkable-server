//! Integration API endpoints
//!
//! REST API for managing cloud storage integrations.

use crate::integrations::{
    dropbox::Dropbox,
    google_drive::GoogleDrive,
    oauth::{OAuthConfig, OAuthToken, PkceFlow, validate_state},
    onedrive::OneDrive,
    sync::{CloudSync, SyncConfig, SyncState},
    CloudProvider, ProviderType,
};
use axum::{
    extract::{OriginalUri, Path, Query, State},
    http::StatusCode,
    response::{Html, Redirect},
    Json,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Component, Path as FsPath, PathBuf};
use std::sync::Arc;

/// Integration state shared across requests
#[derive(Clone)]
pub struct IntegrationState {
    inner: Arc<IntegrationStateInner>,
}

struct IntegrationStateInner {
    /// Active OAuth flows (state -> flow)
    oauth_flows: RwLock<HashMap<String, PkceFlowState>>,
    /// Stored tokens (provider -> token)
    tokens: RwLock<HashMap<ProviderType, OAuthToken>>,
    /// OAuth configs (provider -> config)
    configs: RwLock<HashMap<ProviderType, OAuthConfig>>,
    /// Sync states (provider -> state)
    sync_states: RwLock<HashMap<ProviderType, SyncState>>,
    /// Serializes syncs: concurrent runs would race on local files and (Drive) could both
    /// create the same missing folder, splitting nested files across duplicate folders.
    sync_lock: tokio::sync::Mutex<()>,
    /// Root every sync `local_path` is confined to (None: syncing disabled).
    sync_base: Option<PathBuf>,
    /// HTTP client
    client: reqwest::Client,
}

struct PkceFlowState {
    flow: PkceFlow,
    #[allow(dead_code)]
    created_at: i64,
}

impl IntegrationState {
    /// State with syncing disabled (no sync base); see [`Self::with_sync_base`].
    pub fn new() -> Self {
        Self::build(None)
    }

    /// State whose syncs are confined to directories under `base` (created on demand).
    pub fn with_sync_base(base: impl Into<PathBuf>) -> Self {
        Self::build(Some(base.into()))
    }

    fn build(sync_base: Option<PathBuf>) -> Self {
        Self {
            inner: Arc::new(IntegrationStateInner {
                oauth_flows: RwLock::new(HashMap::new()),
                tokens: RwLock::new(HashMap::new()),
                configs: RwLock::new(HashMap::new()),
                sync_states: RwLock::new(HashMap::new()),
                sync_lock: tokio::sync::Mutex::new(()),
                sync_base,
                client: crate::integrations::http_client(),
            }),
        }
    }

    /// Configure a provider
    pub fn configure_provider(&self, config: OAuthConfig) {
        let provider = config.provider;
        self.inner.configs.write().insert(provider, config);
    }

    /// Get stored token for provider
    pub fn get_token(&self, provider: ProviderType) -> Option<OAuthToken> {
        self.inner.tokens.read().get(&provider).cloned()
    }

    /// Store token for provider
    pub fn set_token(&self, provider: ProviderType, token: OAuthToken) {
        self.inner.tokens.write().insert(provider, token);
    }
}

impl Default for IntegrationState {
    fn default() -> Self {
        Self::new()
    }
}

// === Request/Response types ===

#[derive(Debug, Deserialize)]
pub struct ConfigureProviderRequest {
    pub provider: ProviderType,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub redirect_uri: String,
}

#[derive(Debug, Serialize)]
pub struct ConfigureProviderResponse {
    pub provider: ProviderType,
    pub configured: bool,
}

#[derive(Debug, Serialize)]
pub struct AuthUrlResponse {
    pub auth_url: String,
    pub state: String,
}

#[derive(Debug, Deserialize)]
pub struct OAuthCallbackQuery {
    pub code: String,
    pub state: String,
}

#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub provider: ProviderType,
    pub authenticated: bool,
    pub expires_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SyncRequest {
    pub provider: ProviderType,
    #[serde(default)]
    pub local_path: Option<String>,
    #[serde(default)]
    pub cloud_folder: Option<String>,
    #[serde(default)]
    pub direction: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SyncResponse {
    pub status: String,
    pub uploaded: usize,
    pub downloaded: usize,
    pub deleted: usize,
    pub conflicts: usize,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct QuotaResponse {
    pub provider: ProviderType,
    pub used_bytes: u64,
    pub total_bytes: Option<u64>,
    pub used_percent: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ProviderStatus {
    pub provider: ProviderType,
    pub configured: bool,
    pub authenticated: bool,
    pub token_expires_at: Option<i64>,
}

// === Handlers ===

/// Configure a cloud provider
pub async fn configure_provider(
    State(state): State<IntegrationState>,
    Json(req): Json<ConfigureProviderRequest>,
) -> Json<ConfigureProviderResponse> {
    let config = match req.provider {
        ProviderType::GoogleDrive => {
            OAuthConfig::google_drive(req.client_id, req.client_secret, req.redirect_uri)
        }
        ProviderType::Dropbox => {
            OAuthConfig::dropbox(req.client_id, req.client_secret, req.redirect_uri)
        }
        ProviderType::OneDrive => {
            OAuthConfig::onedrive(req.client_id, req.client_secret, req.redirect_uri)
        }
    };

    state.configure_provider(config);

    Json(ConfigureProviderResponse {
        provider: req.provider,
        configured: true,
    })
}

/// Get OAuth authorization URL for a provider
pub async fn get_auth_url(
    State(state): State<IntegrationState>,
    Path(provider): Path<String>,
) -> std::result::Result<Json<AuthUrlResponse>, (StatusCode, String)> {
    let provider_type = parse_provider(&provider)?;

    let config = state
        .inner
        .configs
        .read()
        .get(&provider_type)
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "Provider not configured".to_string()))?;

    let flow = PkceFlow::new(config);
    let auth_url = flow.authorization_url();
    let flow_state = flow.state().to_string();

    // Store flow for callback
    state.inner.oauth_flows.write().insert(
        flow_state.clone(),
        PkceFlowState {
            flow,
            created_at: chrono::Utc::now().timestamp(),
        },
    );

    Ok(Json(AuthUrlResponse {
        auth_url,
        state: flow_state,
    }))
}

/// OAuth callback handler
pub async fn oauth_callback(
    State(state): State<IntegrationState>,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<OAuthCallbackQuery>,
) -> std::result::Result<Redirect, (StatusCode, String)> {
    // Find and remove the flow
    let flow_state = state
        .inner
        .oauth_flows
        .write()
        .remove(&query.state)
        .ok_or((StatusCode::BAD_REQUEST, "Invalid or expired state".to_string()))?;

    // Validate state
    validate_state(flow_state.flow.state(), &query.state)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    // Exchange code for token
    let token = flow_state
        .flow
        .exchange_code(&query.code, &state.inner.client)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Store token
    let provider = flow_state.flow.config.provider;
    state.set_token(provider, token);

    // Redirect to the success page under whichever mount (/cloud or /storage) served the callback
    Ok(Redirect::to(&success_redirect_path(uri.path(), provider)))
}

/// `<mount>/callback` -> `<mount>/providers/{provider}/success` (a routed path).
fn success_redirect_path(callback_path: &str, provider: ProviderType) -> String {
    let mount = callback_path.strip_suffix("/callback").unwrap_or("/integrations/v2/cloud");
    format!("{}/providers/{}/success", mount, provider)
}

/// Landing page after a completed OAuth flow
pub async fn oauth_success(
    Path(provider): Path<String>,
) -> std::result::Result<Html<String>, (StatusCode, String)> {
    let provider_type = parse_provider(&provider)?;
    Ok(Html(format!(
        "<!doctype html><meta charset=utf-8><title>Connected</title><p>{} connected. You can close this window.</p>",
        provider_type
    )))
}

/// Get token status for a provider
pub async fn get_token_status(
    State(state): State<IntegrationState>,
    Path(provider): Path<String>,
) -> std::result::Result<Json<TokenResponse>, (StatusCode, String)> {
    let provider_type = parse_provider(&provider)?;

    let token = state.get_token(provider_type);

    Ok(Json(TokenResponse {
        provider: provider_type,
        authenticated: token.is_some(),
        expires_at: token.and_then(|t| t.expires_at),
    }))
}

/// Refresh token for a provider
pub async fn refresh_token(
    State(state): State<IntegrationState>,
    Path(provider): Path<String>,
) -> std::result::Result<Json<TokenResponse>, (StatusCode, String)> {
    let provider_type = parse_provider(&provider)?;

    let config = state
        .inner
        .configs
        .read()
        .get(&provider_type)
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "Provider not configured".to_string()))?;

    let current_token = state
        .get_token(provider_type)
        .ok_or((StatusCode::BAD_REQUEST, "Not authenticated".to_string()))?;

    let refresh = current_token
        .refresh_token
        .as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "No refresh token".to_string()))?;

    let new_token = crate::integrations::oauth::refresh_token(&config, refresh, &state.inner.client)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let expires_at = new_token.expires_at;
    state.set_token(provider_type, new_token);

    Ok(Json(TokenResponse {
        provider: provider_type,
        authenticated: true,
        expires_at,
    }))
}

/// Resolve a client-supplied sync `local_path` to a directory inside `base`: only plain relative
/// components are accepted (no absolute paths, `..`, backslashes or NUL), missing directories are
/// created one level at a time, and every existing level is canonicalized and must stay under the
/// canonical base, so a symlink can't redirect the sync elsewhere. `None`/`"."` is the base itself.
async fn resolve_sync_dir(base: &FsPath, local_path: Option<&str>) -> std::result::Result<PathBuf, (StatusCode, String)> {
    let bad = |why: &str| (StatusCode::BAD_REQUEST, format!("invalid local_path: {}", why));
    let rel = FsPath::new(local_path.unwrap_or("."));
    if local_path.is_some_and(|p| p.contains('\0') || p.contains('\\')) { return Err(bad("NUL or backslash")); }
    if !rel.components().all(|c| matches!(c, Component::Normal(_) | Component::CurDir)) {
        return Err(bad("must be a relative path without '..'"));
    }
    let internal = |e: std::io::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("sync base: {}", e));
    tokio::fs::create_dir_all(base).await.map_err(internal)?;
    match crate::integrations::sync::create_dirs_within(base, rel).await {
        Ok(Some(dir)) => Ok(dir),
        Ok(None) => Err(bad("escapes the integrations directory")),
        Err(e) => Err((StatusCode::BAD_REQUEST, format!("invalid local_path: {}", e))),
    }
}

/// Trigger sync for a provider
pub async fn trigger_sync(
    State(state): State<IntegrationState>,
    Json(req): Json<SyncRequest>,
) -> std::result::Result<Json<SyncResponse>, (StatusCode, String)> {
    let base = state.inner.sync_base.as_deref()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "Sync directory not configured".to_string()))?;
    let local_path = resolve_sync_dir(base, req.local_path.as_deref()).await?;

    // Create sync config
    let sync_config = SyncConfig {
        local_path,
        cloud_folder: req.cloud_folder,
        direction: match req.direction.as_deref() {
            Some("upload") => crate::integrations::sync::SyncDirection::Upload,
            Some("download") => crate::integrations::sync::SyncDirection::Download,
            _ => crate::integrations::sync::SyncDirection::Bidirectional,
        },
        ..Default::default()
    };

    // Wait our turn *before* reading config/token: a sync queued behind another must not run
    // with a token captured before the provider was disconnected.
    let _running = state.inner.sync_lock.lock().await;
    let config = state
        .inner
        .configs
        .read()
        .get(&req.provider)
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "Provider not configured".to_string()))?;

    let token = state
        .get_token(req.provider)
        .ok_or((StatusCode::BAD_REQUEST, "Not authenticated".to_string()))?;

    // Create provider and run sync
    let result = match req.provider {
        ProviderType::GoogleDrive => {
            let provider = GoogleDrive::with_token(config, token);
            let mut sync = CloudSync::new(provider, sync_config);
            sync.sync().await
        }
        ProviderType::Dropbox => {
            let provider = Dropbox::with_token(config, token);
            let mut sync = CloudSync::new(provider, sync_config);
            sync.sync().await
        }
        ProviderType::OneDrive => {
            let provider = OneDrive::with_token(config, token);
            let mut sync = CloudSync::new(provider, sync_config);
            sync.sync().await
        }
    };

    let result = result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SyncResponse {
        status: format!("{:?}", result.status),
        uploaded: result.uploaded,
        downloaded: result.downloaded,
        deleted: result.deleted,
        conflicts: result.conflicts.len(),
        errors: result.errors,
        duration_ms: result.duration_ms,
    }))
}

/// Get storage quota for a provider
pub async fn get_quota(
    State(state): State<IntegrationState>,
    Path(provider): Path<String>,
) -> std::result::Result<Json<QuotaResponse>, (StatusCode, String)> {
    let provider_type = parse_provider(&provider)?;

    let config = state
        .inner
        .configs
        .read()
        .get(&provider_type)
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "Provider not configured".to_string()))?;

    let token = state
        .get_token(provider_type)
        .ok_or((StatusCode::BAD_REQUEST, "Not authenticated".to_string()))?;

    let quota = match provider_type {
        ProviderType::GoogleDrive => {
            let provider = GoogleDrive::with_token(config, token);
            provider.get_quota().await
        }
        ProviderType::Dropbox => {
            let provider = Dropbox::with_token(config, token);
            provider.get_quota().await
        }
        ProviderType::OneDrive => {
            let provider = OneDrive::with_token(config, token);
            provider.get_quota().await
        }
    };

    let quota = quota.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let used_percent = quota.total.map(|total| {
        if total > 0 {
            (quota.used as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    });

    Ok(Json(QuotaResponse {
        provider: provider_type,
        used_bytes: quota.used,
        total_bytes: quota.total,
        used_percent,
    }))
}

/// List all providers with their status
pub async fn list_providers(
    State(state): State<IntegrationState>,
) -> Json<Vec<ProviderStatus>> {
    let configs = state.inner.configs.read();
    let tokens = state.inner.tokens.read();

    let providers = [
        ProviderType::GoogleDrive,
        ProviderType::Dropbox,
        ProviderType::OneDrive,
    ];

    let statuses: Vec<ProviderStatus> = providers
        .iter()
        .map(|&provider| {
            let configured = configs.contains_key(&provider);
            let token = tokens.get(&provider);
            
            ProviderStatus {
                provider,
                configured,
                authenticated: token.is_some(),
                token_expires_at: token.and_then(|t| t.expires_at),
            }
        })
        .collect();

    Json(statuses)
}

/// Disconnect a provider (revoke token)
pub async fn disconnect_provider(
    State(state): State<IntegrationState>,
    Path(provider): Path<String>,
) -> std::result::Result<StatusCode, (StatusCode, String)> {
    let provider_type = parse_provider(&provider)?;

    // Always forget locally first; provider revocation is best-effort.
    let token = state.inner.tokens.write().remove(&provider_type);
    state.inner.sync_states.write().remove(&provider_type);

    if let Some(token) = token {
        match crate::integrations::oauth::revoke_token(provider_type, &token, &state.inner.client).await {
            Ok(true) => tracing::info!("revoked {} grant at provider", provider_type),
            Ok(false) => tracing::info!("{} has no revoke endpoint; grant must be removed in the account's app settings", provider_type),
            Err(e) => tracing::warn!("failed to revoke {} grant at provider (deleted locally): {}", provider_type, e),
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

// === Helper functions ===

fn parse_provider(s: &str) -> std::result::Result<ProviderType, (StatusCode, String)> {
    match s.to_lowercase().as_str() {
        "google_drive" | "googledrive" | "google" => Ok(ProviderType::GoogleDrive),
        "dropbox" => Ok(ProviderType::Dropbox),
        "onedrive" | "one_drive" => Ok(ProviderType::OneDrive),
        _ => Err((StatusCode::BAD_REQUEST, format!("Unknown provider: {}", s))),
    }
}

/// Create the integration router (API + OAuth browser routes)
pub fn integration_router(state: IntegrationState) -> axum::Router {
    integration_api_router(state.clone()).merge(integration_oauth_router(state))
}

/// Routes the provider redirects the user's *browser* to. That browser has no device token,
/// so these must sit outside token auth: the callback is authenticated by the one-time PKCE
/// `state` (only issued by the token-guarded `/auth` route), and the success page is static.
pub fn integration_oauth_router(state: IntegrationState) -> axum::Router {
    use axum::routing::get;
    axum::Router::new()
        .route("/providers/{provider}/success", get(oauth_success))
        .route("/callback", get(oauth_callback))
        .with_state(state)
}

/// Token-guarded integration API routes.
pub fn integration_api_router(state: IntegrationState) -> axum::Router {
    use axum::routing::{delete, get, post};

    axum::Router::new()
        .route("/providers", get(list_providers))
        .route("/providers/configure", post(configure_provider))
        .route("/providers/{provider}/auth", get(get_auth_url))
        .route("/providers/{provider}/status", get(get_token_status))
        .route("/providers/{provider}/refresh", post(refresh_token))
        .route("/providers/{provider}/quota", get(get_quota))
        .route("/providers/{provider}/disconnect", delete(disconnect_provider))
        .route("/sync", post(trigger_sync))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    #[tokio::test]
    async fn callback_redirect_target_is_routed() {
        for mount in ["/integrations/v2/cloud", "/integrations/v2/storage"] {
            let target = success_redirect_path(&format!("{}/callback", mount), ProviderType::Dropbox);
            assert_eq!(target, format!("{}/providers/dropbox/success", mount));
            let app = axum::Router::new().nest(mount, integration_router(IntegrationState::new()));
            let resp = app.oneshot(axum::http::Request::get(&target).body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{} not routed", target);
        }
        // Every provider's Display name must round-trip through parse_provider.
        for p in [ProviderType::GoogleDrive, ProviderType::Dropbox, ProviderType::OneDrive] {
            assert_eq!(parse_provider(&p.to_string()).unwrap(), p);
        }
    }

    #[tokio::test]
    async fn disconnect_deletes_locally_when_revoke_unavailable() {
        let state = IntegrationState::new();
        state.set_token(ProviderType::OneDrive, OAuthToken {
            access_token: "a".into(), refresh_token: Some("r".into()), token_type: "Bearer".into(), expires_at: None, scope: None,
        });
        let status = disconnect_provider(State(state.clone()), Path("onedrive".into())).await.unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(state.get_token(ProviderType::OneDrive).is_none());
        assert!(crate::integrations::oauth::revoke_endpoint(ProviderType::GoogleDrive).is_some());
        assert!(crate::integrations::oauth::revoke_endpoint(ProviderType::Dropbox).is_some());
    }

    #[tokio::test]
    async fn queued_sync_rechecks_token_after_lock() {
        let tmp = tempfile::TempDir::new().unwrap();
        let state = IntegrationState::with_sync_base(tmp.path());
        state.configure_provider(OAuthConfig::onedrive("id".into(), None, "http://localhost/cb".into()));
        state.set_token(ProviderType::OneDrive, OAuthToken {
            access_token: "old".into(), refresh_token: None, token_type: "Bearer".into(), expires_at: None, scope: None,
        });
        // A sync is "running": hold the lock while a second sync queues behind it.
        let running = state.inner.sync_lock.lock().await;
        let req = SyncRequest { provider: ProviderType::OneDrive, local_path: None, cloud_folder: None, direction: None };
        let queued = tokio::spawn(trigger_sync(State(state.clone()), Json(req)));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!queued.is_finished());
        disconnect_provider(State(state.clone()), Path("onedrive".into())).await.unwrap();
        drop(running);
        let err = queued.await.unwrap().err().expect("queued sync must not run with the revoked token");
        assert_eq!(err, (StatusCode::BAD_REQUEST, "Not authenticated".to_string()));
    }

    #[tokio::test]
    async fn sync_disabled_without_base() {
        let req = SyncRequest { provider: ProviderType::OneDrive, local_path: None, cloud_folder: None, direction: None };
        let err = trigger_sync(State(IntegrationState::new()), Json(req)).await.err().unwrap();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn sync_dir_confined_to_base() {
        let tmp = tempfile::TempDir::new().unwrap();
        let base = tmp.path().join("integrations");
        // Default and "." map to the (created) base itself.
        let canon_base = || std::fs::canonicalize(&base).unwrap();
        assert_eq!(resolve_sync_dir(&base, None).await.unwrap(), canon_base());
        assert_eq!(resolve_sync_dir(&base, Some(".")).await.unwrap(), canon_base());
        // Relative paths land (and are created) under the base.
        let dir = resolve_sync_dir(&base, Some("gdrive/notes")).await.unwrap();
        assert_eq!(dir, canon_base().join("gdrive/notes"));
        assert!(dir.is_dir());
        // Absolute paths, `..` and odd separators are rejected.
        for p in ["/etc", "/", "..", "../x", "a/../../x", "a/..", "a\\..\\x", "a\0b"] {
            let err = resolve_sync_dir(&base, Some(p)).await.err().unwrap_or_else(|| panic!("accepted {:?}", p));
            assert_eq!(err.0, StatusCode::BAD_REQUEST, "{:?}", p);
        }
        // A symlink inside the base can't redirect the sync outside it.
        let outside = tmp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, base.join("escape")).unwrap();
        for p in ["escape", "escape/sub"] {
            let err = resolve_sync_dir(&base, Some(p)).await.err().unwrap_or_else(|| panic!("accepted {:?}", p));
            assert_eq!(err.0, StatusCode::BAD_REQUEST, "{:?}", p);
        }
        assert!(!outside.join("sub").exists());
    }
}
