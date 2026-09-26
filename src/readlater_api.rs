//! Read-it-later API endpoints

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::{Result, ServerError};
use crate::notifications::WsMessage;
use crate::readlater::{
    ArticleQuery,
    OAuthCallback,
    OMNIVORE_DISCONTINUED,
    ProviderAccount,
    ProviderConfig,
    ReadLaterError,
    ReadLaterManager,
    ReadLaterProvider,
    ReadStatus,
    SyncResult,
    SyncSettings,
    provider_for,
};
use crate::readlater_sync::{ReadLaterSyncer, SyncAllReport, SyncError};
use crate::storage::Storage;

// ============================================================================
// State
// ============================================================================

#[derive(Clone)]
pub struct ReadLaterState {
    pub manager: Arc<Mutex<ReadLaterManager>>,
    /// Runs syncs into `storage` (the device's sync tree); shared with the scheduler.
    pub syncer: Arc<ReadLaterSyncer>,
}

impl ReadLaterState {
    /// Articles are delivered into `storage`, and devices are told to pull through
    /// `notification_tx` (`AppState::notification_tx`).
    pub fn new(
        manager: ReadLaterManager,
        storage: Storage,
        notification_tx: broadcast::Sender<WsMessage>,
    ) -> Self {
        let manager = Arc::new(Mutex::new(manager));
        let syncer = Arc::new(ReadLaterSyncer::new(
            Arc::clone(&manager),
            storage,
            notification_tx,
        ));
        Self { manager, syncer }
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
            ReadLaterError::Discontinued(msg) => ServerError::BadRequest(msg),
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

/// Client-facing view of an account. Deliberately carries no `ProviderConfig`, so stored
/// credentials are never returned to clients.
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

impl From<ProviderAccount> for AccountResponse {
    fn from(a: ProviderAccount) -> Self {
        Self {
            authenticated: a.config.is_authenticated(),
            id: a.id,
            name: a.name,
            provider: a.provider,
            enabled: a.enabled,
            sync_settings: a.sync_settings,
            last_sync: a.last_sync.map(|d| d.to_rfc3339()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SyncAllResponse {
    /// Articles the providers returned.
    pub total_fetched: u32,
    /// Articles put on the device as new documents.
    pub total_synced: u32,
    pub total_errors: usize,
    /// One entry per account synced.
    pub results: Vec<SyncResult>,
    /// Accounts skipped because a sync of them was already running.
    pub already_running: Vec<String>,
}

impl From<SyncAllReport> for SyncAllResponse {
    fn from(report: SyncAllReport) -> Self {
        Self {
            total_fetched: report.results.iter().map(|r| r.articles_fetched).sum(),
            total_synced: report.results.iter().map(|r| r.articles_synced).sum(),
            total_errors: report.results.iter().map(|r| r.errors.len()).sum(),
            results: report.results,
            already_running: report.already_running,
        }
    }
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
        .map(AccountResponse::from)
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

    Ok(Json(AccountResponse::from(account)))
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
    let provider = provider_for(req.provider)?;

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
        ReadLaterProvider::Omnivore => {
            return Err(ReadLaterError::Discontinued(OMNIVORE_DISCONTINUED.into()).into());
        }
    };

    Ok(Json(OAuthResponse { state_id, auth_url }))
}

pub async fn complete_oauth(
    State(state): State<ReadLaterState>,
    Json(req): Json<CompleteOAuthRequest>,
) -> Result<impl IntoResponse> {
    let oauth_state = {
        let manager = state.manager.lock();
        manager
            .get_oauth_state(&req.state_id)?
            .ok_or_else(|| ServerError::Internal("Invalid OAuth state".into()))?
    };

    let provider = provider_for(oauth_state.provider)?;

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

/// `POST /accounts/{id}/sync`: sync the account now and answer with its [`SyncResult`] (per-step
/// errors are listed there); 404 for an unknown account, 409 while it is already syncing.
///
/// The sync runs in its own task, so a client that disconnects doesn't cut it short.
pub async fn sync_account(
    State(state): State<ReadLaterState>,
    Path(id): Path<String>,
) -> Result<Response> {
    let syncer = Arc::clone(&state.syncer);
    let synced = tokio::spawn(async move { syncer.sync_account(&id).await })
        .await
        .map_err(|e| ServerError::Internal(format!("read-later sync task failed: {e}")))?;
    match synced {
        Ok(result) => Ok(Json(result).into_response()),
        Err(SyncError::AccountNotFound(id)) => Err(ServerError::NotFound(id)),
        Err(e @ SyncError::AlreadyRunning(_)) => Ok(already_running(&e)),
    }
}

/// `POST /sync`: sync every enabled account, one after another, and answer with the totals and
/// each account's result. Accounts already syncing are listed in `already_running`.
pub async fn sync_all(State(state): State<ReadLaterState>) -> Result<Json<SyncAllResponse>> {
    let syncer = Arc::clone(&state.syncer);
    let report = tokio::spawn(async move { syncer.sync_all().await })
        .await
        .map_err(|e| ServerError::Internal(format!("read-later sync task failed: {e}")))?;
    Ok(Json(report.into()))
}

/// 409 in the same `{error, details}` shape as [`ServerError`] responses.
fn already_running(e: &SyncError) -> Response {
    (
        StatusCode::CONFLICT,
        Json(serde_json::json!({"error": "already_running", "details": e.to_string()})),
    )
        .into_response()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state(dir: &std::path::Path) -> ReadLaterState {
        let mgr = ReadLaterManager::new(&dir.join("rl.db")).unwrap();
        let storage = Storage::new(dir.join("storage")).unwrap();
        ReadLaterState::new(mgr, storage, broadcast::channel(4).0)
    }

    #[tokio::test]
    async fn account_responses_never_include_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let req: AddAccountRequest = serde_json::from_value(serde_json::json!({
            "name": "wb",
            "provider": "wallabag",
            "config": {
                "type": "wallabag",
                "instance_url": "https://wb.example",
                "client_id": "cid",
                "client_secret": "SECRET-1",
                "access_token": "SECRET-2",
                "refresh_token": "SECRET-3",
                "token_expires_at": null,
                "username": "alice",
                "password": "SECRET-4"
            }
        }))
        .unwrap();
        add_account(State(state.clone()), Json(req)).await.unwrap();

        let Json(list) = list_accounts(State(state.clone())).await.unwrap();
        assert_eq!(list.accounts.len(), 1);
        assert!(list.accounts[0].authenticated);
        let id = list.accounts[0].id.clone();
        let Json(one) = get_account(State(state.clone()), Path(id.clone()))
            .await
            .unwrap();
        for json in [
            serde_json::to_string(&list).unwrap(),
            serde_json::to_string(&one).unwrap(),
            serde_json::to_string(&state.manager.lock().get_account(&id).unwrap()).unwrap(),
        ] {
            assert!(!json.contains("SECRET"), "{json}");
        }
    }

    #[tokio::test]
    async fn omnivore_is_rejected_as_discontinued() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let req: AddAccountRequest = serde_json::from_value(serde_json::json!({
            "name": "om",
            "provider": "omnivore",
            "config": {"type": "omnivore", "api_key": "k"}
        }))
        .unwrap();
        let err = add_account(State(state.clone()), Json(req))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, ServerError::BadRequest(ref m) if m.contains("Omnivore")));
        let req: StartOAuthRequest = serde_json::from_value(serde_json::json!({
            "provider": "omnivore",
            "redirect_uri": "http://cb"
        }))
        .unwrap();
        let err = start_oauth(State(state), Json(req)).await.err().unwrap();
        assert!(matches!(err, ServerError::BadRequest(_)));
    }
}
