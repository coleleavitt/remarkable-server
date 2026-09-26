//! Read-it-later API endpoints

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};
use crate::readlater::{
    ArticleQuery,
    OAuthCallback,
    ProviderAccount,
    ProviderConfig,
    ReadLaterError,
    ReadLaterManager,
    ReadLaterProvider,
    ReadStatus,
    SyncSettings,
};

// ============================================================================
// State
// ============================================================================

#[derive(Clone)]
pub struct ReadLaterState {
    pub manager: Arc<Mutex<ReadLaterManager>>,
}

impl ReadLaterState {
    pub fn new(manager: ReadLaterManager) -> Self {
        Self {
            manager: Arc::new(Mutex::new(manager)),
        }
    }
}

// ============================================================================
// Error Conversion
// ============================================================================

impl From<ReadLaterError> for ServerError {
    fn from(e: ReadLaterError) -> Self {
        match e {
            ReadLaterError::ProviderNotFound(id) | ReadLaterError::ArticleNotFound(id) => {
                ServerError::NotFound(id)
            }
            ReadLaterError::AuthRequired(_) => ServerError::Unauthorized,
            ReadLaterError::OAuth(msg)
            | ReadLaterError::Api(msg)
            | ReadLaterError::Conversion(msg) => ServerError::Internal(msg),
            ReadLaterError::Database(msg) => ServerError::Database(msg),
            ReadLaterError::Network(msg) => ServerError::Internal(format!("Network: {}", msg)),
            ReadLaterError::RateLimited(secs) => {
                ServerError::Internal(format!("Rate limited, retry after {} seconds", secs))
            }
            ReadLaterError::Io(e) => ServerError::Storage(e),
            ReadLaterError::Json(e) => ServerError::Json(e),
        }
    }
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct AddAccountRequest {
    pub name: String,
    pub provider: ReadLaterProvider,
    pub config: ProviderConfig,
    #[serde(default)]
    pub sync_settings: Option<SyncSettings>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAccountRequest {
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub sync_settings: Option<SyncSettings>,
}

#[derive(Debug, Deserialize)]
pub struct StartOAuthRequest {
    pub provider: ReadLaterProvider,
    pub redirect_uri: String,
    #[serde(default)]
    pub config: Option<ProviderConfig>,
}

#[derive(Debug, Serialize)]
pub struct OAuthResponse {
    pub state_id: String,
    pub auth_url: String,
}

#[derive(Debug, Deserialize)]
pub struct CompleteOAuthRequest {
    pub state_id: String,
    pub callback: OAuthCallback,
    pub account_name: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateArticleRequest {
    pub status: Option<ReadStatus>,
    pub favorite: Option<bool>,
    pub tags: Option<Vec<String>>,
    pub document_id: Option<String>,
    pub synced_to_device: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct AccountResponse {
    pub id: String,
    pub name: String,
    pub provider: ReadLaterProvider,
    pub enabled: bool,
    pub authenticated: bool,
    pub sync_settings: SyncSettings,
    pub last_sync: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SyncAllResponse {
    pub total_fetched: u32,
    pub total_synced: u32,
    pub total_errors: usize,
}

#[derive(Debug, Serialize)]
pub struct AccountListResponse {
    pub accounts: Vec<AccountResponse>,
}

#[derive(Debug, Serialize)]
pub struct ArticleListResponse {
    pub articles: Vec<crate::readlater::Article>,
    pub total: usize,
}

// ============================================================================
// Account Endpoints
// ============================================================================

pub async fn list_accounts(
    State(state): State<ReadLaterState>,
) -> Result<Json<AccountListResponse>> {
    let manager = state.manager.lock();
    let accounts: Vec<AccountResponse> = manager
        .list_accounts()
        .into_iter()
        .map(|a| AccountResponse {
            id: a.id,
            name: a.name,
            provider: a.provider,
            enabled: a.enabled,
            authenticated: match &a.config {
                ProviderConfig::Pocket { access_token, .. } => access_token.is_some(),
                ProviderConfig::Instapaper { oauth_token, .. } => oauth_token.is_some(),
                ProviderConfig::Wallabag { access_token, .. } => access_token.is_some(),
                ProviderConfig::Omnivore { api_key, .. } => api_key.is_some(),
            },
            sync_settings: a.sync_settings,
            last_sync: a.last_sync.map(|d| d.to_rfc3339()),
        })
        .collect();

    Ok(Json(AccountListResponse { accounts }))
}

pub async fn add_account(
    State(state): State<ReadLaterState>,
    Json(req): Json<AddAccountRequest>,
) -> Result<impl IntoResponse> {
    let account = ProviderAccount {
        id: uuid::Uuid::new_v4().to_string(),
        name: req.name,
        provider: req.provider,
        enabled: true,
        config: req.config,
        sync_settings: req.sync_settings.unwrap_or_default(),
        last_sync: None,
        created_at: chrono::Utc::now(),
    };

    let id = account.id.clone();
    state.manager.lock().add_account(account)?;

    Ok((StatusCode::CREATED, Json(serde_json::json!({"id": id}))))
}

pub async fn get_account(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
) -> Result<Json<AccountResponse>> {
    let manager = state.manager.lock();
    let account = manager
        .get_account(&id)
        .ok_or_else(|| ServerError::NotFound(id))?;

    Ok(Json(AccountResponse {
        id: account.id,
        name: account.name,
        provider: account.provider,
        enabled: account.enabled,
        authenticated: match &account.config {
            ProviderConfig::Pocket { access_token, .. } => access_token.is_some(),
            ProviderConfig::Instapaper { oauth_token, .. } => oauth_token.is_some(),
            ProviderConfig::Wallabag { access_token, .. } => access_token.is_some(),
            ProviderConfig::Omnivore { api_key, .. } => api_key.is_some(),
        },
        sync_settings: account.sync_settings,
        last_sync: account.last_sync.map(|d| d.to_rfc3339()),
    }))
}

pub async fn update_account(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateAccountRequest>,
) -> Result<StatusCode> {
    let mut manager = state.manager.lock();
    let mut account = manager
        .get_account(&id)
        .ok_or_else(|| ServerError::NotFound(id))?;

    if let Some(name) = req.name {
        account.name = name;
    }
    if let Some(enabled) = req.enabled {
        account.enabled = enabled;
    }
    if let Some(settings) = req.sync_settings {
        account.sync_settings = settings;
    }

    manager.update_account(account)?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_account_handler(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
) -> Result<StatusCode> {
    state.manager.lock().delete_account(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// OAuth Endpoints
// ============================================================================

pub async fn start_oauth(
    State(state): State<ReadLaterState>,
    Json(req): Json<StartOAuthRequest>,
) -> Result<Json<OAuthResponse>> {
    use crate::readlater::{
        InstapaperProvider,
        OmnivoreProvider,
        PocketProvider,
        ReadLaterProviderTrait,
        WallabagProvider,
    };

    let provider: Box<dyn ReadLaterProviderTrait> = match req.provider {
        ReadLaterProvider::Pocket => Box::new(PocketProvider::new()),
        ReadLaterProvider::Instapaper => {
            let key = std::env::var("INSTAPAPER_CONSUMER_KEY").unwrap_or_default();
            let secret = std::env::var("INSTAPAPER_CONSUMER_SECRET").unwrap_or_default();
            Box::new(InstapaperProvider::new(key, secret))
        }
        ReadLaterProvider::Wallabag => Box::new(WallabagProvider::new()),
        ReadLaterProvider::Omnivore => Box::new(OmnivoreProvider::new()),
    };

    let oauth_state = provider.start_oauth(&req.redirect_uri).await?;
    let state_id = state.manager.lock().save_oauth_state(&oauth_state)?;

    let auth_url = match req.provider {
        ReadLaterProvider::Pocket => {
            format!(
                "https://getpocket.com/auth/authorize?request_token={}&redirect_uri={}",
                oauth_state.request_token.as_deref().unwrap_or(""),
                urlencoding::encode(&req.redirect_uri)
            )
        }
        ReadLaterProvider::Wallabag => {
            if let Some(ProviderConfig::Wallabag {
                instance_url,
                client_id,
                ..
            }) = req.config
            {
                format!(
                    "{}/oauth/v2/auth?client_id={}&redirect_uri={}&response_type=code",
                    instance_url,
                    client_id,
                    urlencoding::encode(&req.redirect_uri)
                )
            } else {
                return Err(ServerError::Internal(
                    "Wallabag requires instance_url and client_id".into(),
                ));
            }
        }
        ReadLaterProvider::Instapaper => "xauth://instapaper".to_string(),
        ReadLaterProvider::Omnivore => "apikey://omnivore".to_string(),
    };

    Ok(Json(OAuthResponse { state_id, auth_url }))
}

pub async fn complete_oauth(
    State(state): State<ReadLaterState>,
    Json(req): Json<CompleteOAuthRequest>,
) -> Result<impl IntoResponse> {
    use crate::readlater::{
        InstapaperProvider,
        OmnivoreProvider,
        PocketProvider,
        ReadLaterProviderTrait,
        WallabagProvider,
    };

    let oauth_state = {
        let manager = state.manager.lock();
        manager
            .get_oauth_state(&req.state_id)?
            .ok_or_else(|| ServerError::Internal("Invalid OAuth state".into()))?
    };

    let provider: Box<dyn ReadLaterProviderTrait> = match oauth_state.provider {
        ReadLaterProvider::Pocket => Box::new(PocketProvider::new()),
        ReadLaterProvider::Instapaper => {
            let key = std::env::var("INSTAPAPER_CONSUMER_KEY").unwrap_or_default();
            let secret = std::env::var("INSTAPAPER_CONSUMER_SECRET").unwrap_or_default();
            Box::new(InstapaperProvider::new(key, secret))
        }
        ReadLaterProvider::Wallabag => Box::new(WallabagProvider::new()),
        ReadLaterProvider::Omnivore => Box::new(OmnivoreProvider::new()),
    };

    let config = provider.complete_oauth(&req.callback, &oauth_state).await?;

    let account = ProviderAccount {
        id: uuid::Uuid::new_v4().to_string(),
        name: req.account_name,
        provider: oauth_state.provider,
        enabled: true,
        config,
        sync_settings: SyncSettings::default(),
        last_sync: None,
        created_at: chrono::Utc::now(),
    };

    let id = account.id.clone();
    let mut manager = state.manager.lock();
    manager.add_account(account)?;
    manager.delete_oauth_state(&req.state_id)?;

    Ok((StatusCode::CREATED, Json(serde_json::json!({"id": id}))))
}

// ============================================================================
// Article Endpoints
// ============================================================================

pub async fn list_articles(
    State(state): State<ReadLaterState>,
    Query(query): Query<ArticleQuery>,
) -> Result<Json<ArticleListResponse>> {
    let manager = state.manager.lock();
    let articles = manager.query_articles(&query);
    let total = articles.len();
    Ok(Json(ArticleListResponse { articles, total }))
}

pub async fn get_article(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
) -> Result<Json<crate::readlater::Article>> {
    let manager = state.manager.lock();
    let article = manager
        .get_article(&id)
        .ok_or_else(|| ServerError::NotFound(id))?;
    Ok(Json(article))
}

pub async fn update_article(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateArticleRequest>,
) -> Result<Json<crate::readlater::Article>> {
    let mut manager = state.manager.lock();
    let mut article = manager
        .get_article(&id)
        .ok_or_else(|| ServerError::NotFound(id))?;

    if let Some(status) = req.status {
        article.status = status;
        if status == ReadStatus::Read {
            article.read_at = Some(chrono::Utc::now());
        }
    }
    if let Some(favorite) = req.favorite {
        article.favorite = favorite;
    }
    if let Some(tags) = req.tags {
        article.tags = tags;
    }
    if let Some(doc_id) = req.document_id {
        article.document_id = Some(doc_id);
    }
    if let Some(synced) = req.synced_to_device {
        article.synced_to_device = synced;
        if synced {
            article.last_sync = Some(chrono::Utc::now());
        }
    }

    article.updated_at = chrono::Utc::now();
    manager.save_article(&article)?;

    Ok(Json(article))
}

pub async fn delete_article_handler(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
) -> Result<StatusCode> {
    state.manager.lock().delete_article(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

// ============================================================================
// Sync Endpoints
// ============================================================================

pub async fn sync_account(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    // Get account details first, then release lock
    let account = {
        let manager = state.manager.lock();
        manager
            .get_account(&id)
            .ok_or_else(|| ServerError::NotFound(id.clone()))?
    };

    // Perform sync outside of lock - for now just return status
    // Full async sync would need a different architecture
    Ok(Json(serde_json::json!({
        "status": "queued",
        "account_id": account.id,
        "provider": account.provider.to_string(),
    })))
}

pub async fn sync_all(State(state): State<ReadLaterState>) -> Result<Json<SyncAllResponse>> {
    // Get enabled account count, then release lock
    let account_count = {
        let manager = state.manager.lock();
        manager
            .list_accounts()
            .into_iter()
            .filter(|a| a.enabled)
            .count()
    };

    // For now just return queued status
    // Full async sync would need a different architecture (background task)
    Ok(Json(SyncAllResponse {
        total_fetched: 0,
        total_synced: 0,
        total_errors: 0,
    }))
}

// ============================================================================
// Router
// ============================================================================

pub fn readlater_router(state: ReadLaterState) -> Router {
    Router::new()
        // Accounts
        .route("/accounts", get(list_accounts))
        .route("/accounts", post(add_account))
        .route("/accounts/{id}", get(get_account))
        .route("/accounts/{id}", put(update_account))
        .route("/accounts/{id}", delete(delete_account_handler))
        .route("/accounts/{id}/sync", post(sync_account))
        // OAuth
        .route("/oauth/start", post(start_oauth))
        .route("/oauth/complete", post(complete_oauth))
        // Articles
        .route("/articles", get(list_articles))
        .route("/articles/{id}", get(get_article))
        .route("/articles/{id}", put(update_article))
        .route("/articles/{id}", delete(delete_article_handler))
        // Sync
        .route("/sync", post(sync_all))
        .with_state(state)
}
