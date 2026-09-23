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
    extract::{Path, Query, State},
    http::StatusCode,
    response::Redirect,
    Json,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    /// HTTP client
    client: reqwest::Client,
}

struct PkceFlowState {
    flow: PkceFlow,
    #[allow(dead_code)]
    created_at: i64,
}

impl IntegrationState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(IntegrationStateInner {
                oauth_flows: RwLock::new(HashMap::new()),
                tokens: RwLock::new(HashMap::new()),
                configs: RwLock::new(HashMap::new()),
                sync_states: RwLock::new(HashMap::new()),
                client: reqwest::Client::new(),
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

    // Redirect to success page
    Ok(Redirect::to(&format!("/integrations/v2/cloud/{}/success", provider)))
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

/// Trigger sync for a provider
pub async fn trigger_sync(
    State(state): State<IntegrationState>,
    Json(req): Json<SyncRequest>,
) -> std::result::Result<Json<SyncResponse>, (StatusCode, String)> {
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

    // Create sync config
    let sync_config = SyncConfig {
        local_path: req.local_path.map(Into::into).unwrap_or_else(|| ".".into()),
        cloud_folder: req.cloud_folder,
        direction: match req.direction.as_deref() {
            Some("upload") => crate::integrations::sync::SyncDirection::Upload,
            Some("download") => crate::integrations::sync::SyncDirection::Download,
            _ => crate::integrations::sync::SyncDirection::Bidirectional,
        },
        ..Default::default()
    };

    // Get or create sync state
    let _sync_state = state
        .inner
        .sync_states
        .read()
        .get(&req.provider)
        .cloned()
        .unwrap_or_default();

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

    state.inner.tokens.write().remove(&provider_type);
    state.inner.sync_states.write().remove(&provider_type);

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

/// Create the integration router
pub fn integration_router(state: IntegrationState) -> axum::Router {
    use axum::routing::{delete, get, post};

    axum::Router::new()
        .route("/providers", get(list_providers))
        .route("/providers/configure", post(configure_provider))
        .route("/providers/{provider}/auth", get(get_auth_url))
        .route("/providers/{provider}/status", get(get_token_status))
        .route("/providers/{provider}/refresh", post(refresh_token))
        .route("/providers/{provider}/quota", get(get_quota))
        .route("/providers/{provider}/disconnect", delete(disconnect_provider))
        .route("/callback", get(oauth_callback))
        .route("/sync", post(trigger_sync))
        .with_state(state)
}
