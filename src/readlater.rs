//! Read-it-later integrations module
//! Supports Pocket, Instapaper and Wallabag. Omnivore (shut down November 2024) is kept only
//! as a discontinued marker so existing database rows still load.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Error, Debug)]
pub enum ReadLaterError {
    #[error("Provider not found: {0}")]
    ProviderNotFound(String),
    #[error("Article not found: {0}")]
    ArticleNotFound(String),
    #[error("Authentication required for {0}")]
    AuthRequired(String),
    #[error("OAuth error: {0}")]
    OAuth(String),
    #[error("API error: {0}")]
    Api(String),
    #[error("Rate limited, retry after {0} seconds")]
    RateLimited(u64),
    #[error("Conversion error: {0}")]
    Conversion(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Network error: {0}")]
    Network(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Discontinued(String),
}

/// Message for anything that tries to use the Omnivore provider.
pub(crate) const OMNIVORE_DISCONTINUED: &str =
    "Omnivore shut down in November 2024 and is no longer supported; delete this account";

pub type Result<T> = std::result::Result<T, ReadLaterError>;

// ============================================================================
// Provider Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadLaterProvider {
    Pocket,
    Instapaper,
    Wallabag,
    /// Discontinued (the service shut down in November 2024). Retained only so accounts and
    /// articles saved before the removal still load; every operation on it fails with
    /// [`ReadLaterError::Discontinued`].
    Omnivore,
}

impl std::fmt::Display for ReadLaterProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pocket => write!(f, "pocket"),
            Self::Instapaper => write!(f, "instapaper"),
            Self::Wallabag => write!(f, "wallabag"),
            Self::Omnivore => write!(f, "omnivore"),
        }
    }
}

// ============================================================================
// Article Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReadStatus {
    #[default]
    Unread,
    InProgress,
    Read,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ArticleFormat {
    #[default]
    Html,
    Epub,
    Pdf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Article {
    pub id: String,
    pub provider: ReadLaterProvider,
    pub provider_id: String,
    pub url: String,
    pub title: String,
    pub excerpt: Option<String>,
    pub author: Option<String>,
    pub word_count: Option<u32>,
    pub reading_time_minutes: Option<u32>,
    pub tags: Vec<String>,
    pub status: ReadStatus,
    pub favorite: bool,
    pub added_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
    pub content: Option<String>,
    pub image_url: Option<String>,
    pub document_id: Option<String>,
    pub synced_to_device: bool,
    pub last_sync: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArticleContent {
    pub html: String,
    pub images: Vec<ArticleImage>,
    pub styles: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArticleImage {
    pub url: String,
    pub alt: Option<String>,
    pub data: Option<Vec<u8>>,
}

// ============================================================================
// Provider Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAccount {
    pub id: String,
    pub name: String,
    pub provider: ReadLaterProvider,
    pub enabled: bool,
    pub config: ProviderConfig,
    pub sync_settings: SyncSettings,
    pub last_sync: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Provider settings. Credential fields are `skip_serializing` so they never appear in JSON
/// sent to clients (or in the `config` column); they are persisted separately in the
/// `secrets` column via [`ProviderSecrets`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ProviderConfig {
    Pocket {
        consumer_key: String,
        #[serde(skip_serializing)]
        access_token: Option<String>,
        username: Option<String>,
    },
    Instapaper {
        #[serde(skip_serializing)]
        oauth_token: Option<String>,
        #[serde(skip_serializing)]
        oauth_token_secret: Option<String>,
        username: Option<String>,
    },
    Wallabag {
        instance_url: String,
        client_id: String,
        #[serde(skip_serializing)]
        client_secret: Option<String>,
        #[serde(skip_serializing)]
        access_token: Option<String>,
        #[serde(skip_serializing)]
        refresh_token: Option<String>,
        token_expires_at: Option<DateTime<Utc>>,
        /// Wallabag account used for the OAuth password grant when there is no usable
        /// refresh token.
        #[serde(default)]
        username: Option<String>,
        #[serde(default, skip_serializing)]
        password: Option<String>,
    },
    /// Discontinued provider; any legacy fields (`api_key`, `api_url`) are ignored.
    Omnivore {},
}

/// Credential fields of a [`ProviderConfig`], stored as JSON in the `secrets` column of
/// `readlater_accounts`. Kept out of the `config` column and all API responses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ProviderSecrets {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oauth_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oauth_token_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password: Option<String>,
}

impl ProviderConfig {
    fn secrets(&self) -> ProviderSecrets {
        match self {
            Self::Pocket { access_token, .. } => ProviderSecrets {
                access_token: access_token.clone(),
                ..Default::default()
            },
            Self::Instapaper {
                oauth_token,
                oauth_token_secret,
                ..
            } => ProviderSecrets {
                oauth_token: oauth_token.clone(),
                oauth_token_secret: oauth_token_secret.clone(),
                ..Default::default()
            },
            Self::Wallabag {
                client_secret,
                access_token,
                refresh_token,
                password,
                ..
            } => ProviderSecrets {
                access_token: access_token.clone(),
                refresh_token: refresh_token.clone(),
                client_secret: client_secret.clone(),
                password: password.clone(),
                ..Default::default()
            },
            Self::Omnivore {} => ProviderSecrets::default(),
        }
    }

    /// Fill credential fields from stored secrets. A secret that is absent from the store
    /// leaves the field as deserialized, so configs written before secrets were split out
    /// (when they were still inside the `config` JSON) keep working.
    fn apply_secrets(&mut self, s: ProviderSecrets) {
        fn set(field: &mut Option<String>, v: Option<String>) {
            if v.is_some() {
                *field = v;
            }
        }
        match self {
            Self::Pocket { access_token, .. } => set(access_token, s.access_token),
            Self::Instapaper {
                oauth_token,
                oauth_token_secret,
                ..
            } => {
                set(oauth_token, s.oauth_token);
                set(oauth_token_secret, s.oauth_token_secret);
            }
            Self::Wallabag {
                client_secret,
                access_token,
                refresh_token,
                password,
                ..
            } => {
                set(client_secret, s.client_secret);
                set(access_token, s.access_token);
                set(refresh_token, s.refresh_token);
                set(password, s.password);
            }
            Self::Omnivore {} => {}
        }
    }

    /// Whether the config holds the credentials its provider needs for API calls.
    pub fn is_authenticated(&self) -> bool {
        match self {
            Self::Pocket { access_token, .. } => access_token.is_some(),
            Self::Instapaper { oauth_token, .. } => oauth_token.is_some(),
            Self::Wallabag { access_token, .. } => access_token.is_some(),
            Self::Omnivore {} => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSettings {
    pub auto_sync: bool,
    pub sync_interval_minutes: u32,
    pub max_articles: u32,
    pub convert_format: ArticleFormat,
    pub sync_read_status: bool,
    pub tag_filters: Vec<TagFilter>,
    pub include_favorites_only: bool,
    pub include_archived: bool,
    pub folder_id: Option<String>,
}

impl Default for SyncSettings {
    fn default() -> Self {
        Self {
            auto_sync: true,
            sync_interval_minutes: 60,
            max_articles: 50,
            convert_format: ArticleFormat::Epub,
            sync_read_status: true,
            tag_filters: Vec::new(),
            include_favorites_only: false,
            include_archived: false,
            folder_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagFilter {
    pub tag: String,
    #[serde(default)]
    pub include: bool,
}

// ============================================================================
// OAuth Flow Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthState {
    pub provider: ReadLaterProvider,
    pub request_token: Option<String>,
    pub redirect_uri: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthCallback {
    pub code: Option<String>,
    pub oauth_token: Option<String>,
    pub oauth_verifier: Option<String>,
    pub state: Option<String>,
    /// Account credentials for username/password flows (Instapaper xAuth). Never echoed back.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default, skip_serializing)]
    pub password: Option<String>,
}

// ============================================================================
// Sync Types
// ============================================================================

/// Outcome of one account's sync (see [`crate::readlater_sync`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    #[serde(default)]
    pub account_id: String,
    pub provider: ReadLaterProvider,
    /// Articles the provider returned (changed since the account's last sync).
    pub articles_fetched: u32,
    /// Articles put on the device in this sync, each as a new document.
    pub articles_synced: u32,
    /// Articles rendered to the account's format in this sync.
    pub articles_converted: u32,
    /// Selected articles skipped because they are already on the device.
    #[serde(default)]
    pub articles_already_synced: u32,
    pub read_status_synced: u32,
    pub errors: Vec<String>,
    pub duration_ms: u64,
    pub completed_at: DateTime<Utc>,
}

// ============================================================================
// Query Types
// ============================================================================

#[derive(Debug, Default, Deserialize)]
pub struct ArticleQuery {
    pub provider: Option<ReadLaterProvider>,
    pub status: Option<ReadStatus>,
    pub tags: Option<Vec<String>>,
    pub favorite: Option<bool>,
    pub synced: Option<bool>,
    pub search: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub sort_by: Option<ArticleSortBy>,
    pub sort_order: Option<SortOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ArticleSortBy {
    #[default]
    AddedAt,
    UpdatedAt,
    Title,
    ReadingTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Desc,
    Asc,
}

// ============================================================================
// Provider Trait
// ============================================================================

#[async_trait::async_trait]
pub trait ReadLaterProviderTrait: Send + Sync {
    fn provider_type(&self) -> ReadLaterProvider;

    async fn start_oauth(&self, redirect_uri: &str) -> Result<OAuthState>;
    async fn complete_oauth(
        &self,
        callback: &OAuthCallback,
        state: &OAuthState,
    ) -> Result<ProviderConfig>;
    async fn refresh_auth(&self, config: &ProviderConfig) -> Result<ProviderConfig>;
    fn is_authenticated(&self, config: &ProviderConfig) -> bool;

    async fn fetch_articles(
        &self,
        config: &ProviderConfig,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<Article>>;
    async fn fetch_article_content(
        &self,
        config: &ProviderConfig,
        article: &Article,
    ) -> Result<ArticleContent>;
    async fn update_read_status(
        &self,
        config: &ProviderConfig,
        provider_id: &str,
        status: ReadStatus,
    ) -> Result<()>;
    async fn add_article(
        &self,
        config: &ProviderConfig,
        url: &str,
        tags: &[String],
    ) -> Result<Article>;
    async fn delete_article(&self, config: &ProviderConfig, provider_id: &str) -> Result<()>;

    /// The config produced by the most recent credential refresh this provider performed
    /// inside one of the calls above (e.g. a refresh-and-retry after a 401), if any. Taking it
    /// clears it. Callers persist it and pass it to subsequent calls, since the old tokens may
    /// have been rotated out.
    fn take_refreshed_config(&self) -> Option<ProviderConfig> {
        None
    }
}

/// Construct the provider implementation for `kind`. Instapaper's consumer key/secret come
/// from `INSTAPAPER_CONSUMER_KEY` / `INSTAPAPER_CONSUMER_SECRET`.
pub fn provider_for(kind: ReadLaterProvider) -> Result<Box<dyn ReadLaterProviderTrait>> {
    Ok(match kind {
        ReadLaterProvider::Pocket => Box::new(PocketProvider::new()),
        ReadLaterProvider::Instapaper => {
            let key = std::env::var("INSTAPAPER_CONSUMER_KEY").unwrap_or_default();
            let secret = std::env::var("INSTAPAPER_CONSUMER_SECRET").unwrap_or_default();
            Box::new(InstapaperProvider::new(key, secret))
        }
        ReadLaterProvider::Wallabag => Box::new(WallabagProvider::new()),
        ReadLaterProvider::Omnivore => {
            return Err(ReadLaterError::Discontinued(OMNIVORE_DISCONTINUED.into()));
        }
    })
}

// ============================================================================
// Pocket Provider
// ============================================================================

pub struct PocketProvider {
    client: reqwest::Client,
}

impl PocketProvider {
    const API_BASE: &'static str = "https://getpocket.com/v3";

    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl ReadLaterProviderTrait for PocketProvider {
    fn provider_type(&self) -> ReadLaterProvider {
        ReadLaterProvider::Pocket
    }

    async fn start_oauth(&self, redirect_uri: &str) -> Result<OAuthState> {
        // Get consumer key from environment
        let consumer_key = std::env::var("POCKET_CONSUMER_KEY")
            .map_err(|_| ReadLaterError::OAuth("POCKET_CONSUMER_KEY not set".into()))?;

        // Request token from Pocket
        let resp = self
            .client
            .post(format!("{}/oauth/request", Self::API_BASE))
            .header("X-Accept", "application/json")
            .json(&serde_json::json!({
                "consumer_key": consumer_key,
                "redirect_uri": redirect_uri,
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::OAuth(format!(
                "Failed to get request token: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct TokenResponse {
            code: String,
        }

        let token_resp: TokenResponse = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::OAuth(e.to_string()))?;

        Ok(OAuthState {
            provider: ReadLaterProvider::Pocket,
            request_token: Some(token_resp.code),
            redirect_uri: redirect_uri.to_string(),
            created_at: Utc::now(),
        })
    }

    async fn complete_oauth(
        &self,
        _callback: &OAuthCallback,
        state: &OAuthState,
    ) -> Result<ProviderConfig> {
        let request_token = state
            .request_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::OAuth("Missing request token".into()))?;

        // This would need the consumer_key from the original config
        let consumer_key = std::env::var("POCKET_CONSUMER_KEY")
            .map_err(|_| ReadLaterError::OAuth("POCKET_CONSUMER_KEY not set".into()))?;

        let resp = self
            .client
            .post(format!("{}/oauth/authorize", Self::API_BASE))
            .header("X-Accept", "application/json")
            .json(&serde_json::json!({
                "consumer_key": consumer_key,
                "code": request_token,
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::OAuth(format!(
                "OAuth failed: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct AuthResponse {
            access_token: String,
            username: String,
        }

        let auth: AuthResponse = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::OAuth(e.to_string()))?;

        Ok(ProviderConfig::Pocket {
            consumer_key,
            access_token: Some(auth.access_token),
            username: Some(auth.username),
        })
    }

    async fn refresh_auth(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        // Pocket tokens don't expire
        Ok(config.clone())
    }

    fn is_authenticated(&self, config: &ProviderConfig) -> bool {
        match config {
            ProviderConfig::Pocket { access_token, .. } => access_token.is_some(),
            _ => false,
        }
    }

    async fn fetch_articles(
        &self,
        config: &ProviderConfig,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<Article>> {
        let ProviderConfig::Pocket {
            consumer_key,
            access_token,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config for Pocket".into()));
        };

        let access_token = access_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;

        let mut params = serde_json::json!({
            "consumer_key": consumer_key,
            "access_token": access_token,
            "state": "all",
            "detailType": "complete",
            "sort": "newest",
        });

        if let Some(ts) = since {
            params["since"] = serde_json::json!(ts.timestamp());
        }

        let resp = self
            .client
            .post(format!("{}/get", Self::API_BASE))
            .header("X-Accept", "application/json")
            .json(&params)
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ReadLaterError::RateLimited(60));
        }

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Pocket API error: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct PocketResponse {
            list: HashMap<String, PocketItem>,
        }

        #[derive(Deserialize)]
        struct PocketItem {
            item_id: String,
            given_url: String,
            given_title: Option<String>,
            resolved_title: Option<String>,
            excerpt: Option<String>,
            word_count: Option<String>,
            time_added: String,
            time_updated: String,
            time_read: Option<String>,
            status: String,
            favorite: String,
            tags: Option<HashMap<String, serde_json::Value>>,
            authors: Option<HashMap<String, PocketAuthor>>,
            image: Option<PocketImage>,
        }

        #[derive(Deserialize)]
        struct PocketAuthor {
            name: Option<String>,
        }

        #[derive(Deserialize)]
        struct PocketImage {
            src: Option<String>,
        }

        let data: PocketResponse = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        let articles = data
            .list
            .into_values()
            .map(|item| {
                let status = match item.status.as_str() {
                    "0" => ReadStatus::Unread,
                    "1" => ReadStatus::Archived,
                    "2" => ReadStatus::Read,
                    _ => ReadStatus::Unread,
                };

                let word_count = item.word_count.as_ref().and_then(|w| w.parse().ok());

                let reading_time = word_count.map(|w: u32| (w / 200).max(1));

                let tags: Vec<String> = item
                    .tags
                    .map(|t| t.keys().cloned().collect())
                    .unwrap_or_default();

                let author = item
                    .authors
                    .and_then(|a| a.values().next().and_then(|x| x.name.clone()));

                let added_ts = item.time_added.parse::<i64>().unwrap_or(0);
                let updated_ts = item.time_updated.parse::<i64>().unwrap_or(0);
                let read_ts = item
                    .time_read
                    .as_ref()
                    .and_then(|t| t.parse::<i64>().ok())
                    .filter(|&t| t > 0);

                Article {
                    id: uuid::Uuid::new_v4().to_string(),
                    provider: ReadLaterProvider::Pocket,
                    provider_id: item.item_id,
                    url: item.given_url,
                    title: item.resolved_title.or(item.given_title).unwrap_or_default(),
                    excerpt: item.excerpt,
                    author,
                    word_count,
                    reading_time_minutes: reading_time,
                    tags,
                    status,
                    favorite: item.favorite == "1",
                    added_at: DateTime::from_timestamp(added_ts, 0).unwrap_or_else(Utc::now),
                    updated_at: DateTime::from_timestamp(updated_ts, 0).unwrap_or_else(Utc::now),
                    read_at: read_ts.and_then(|t| DateTime::from_timestamp(t, 0)),
                    content: None,
                    image_url: item.image.and_then(|i| i.src),
                    document_id: None,
                    synced_to_device: false,
                    last_sync: None,
                }
            })
            .collect();

        Ok(articles)
    }

    async fn fetch_article_content(
        &self,
        _config: &ProviderConfig,
        article: &Article,
    ) -> Result<ArticleContent> {
        // Pocket doesn't provide article content directly via API
        // Use Mercury Parser or similar service
        let html = format!(
            "<html><head><title>{}</title></head><body><h1>{}</h1><p>{}</p><p><a href=\"{}\">Read original</a></p></body></html>",
            escape_html(&article.title),
            escape_html(&article.title),
            escape_html(article.excerpt.as_deref().unwrap_or("")),
            escape_html(&safe_href(&article.url))
        );

        Ok(ArticleContent {
            html,
            images: Vec::new(),
            styles: None,
        })
    }

    async fn update_read_status(
        &self,
        config: &ProviderConfig,
        provider_id: &str,
        status: ReadStatus,
    ) -> Result<()> {
        let ProviderConfig::Pocket {
            consumer_key,
            access_token,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let access_token = access_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;

        let action = match status {
            ReadStatus::Archived => "archive",
            ReadStatus::Read => "archive",
            ReadStatus::Unread => "readd",
            ReadStatus::InProgress => return Ok(()),
        };

        let resp = self
            .client
            .post(format!("{}/send", Self::API_BASE))
            .header("X-Accept", "application/json")
            .json(&serde_json::json!({
                "consumer_key": consumer_key,
                "access_token": access_token,
                "actions": [{
                    "action": action,
                    "item_id": provider_id,
                }]
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to update status: {}",
                resp.status()
            )));
        }

        Ok(())
    }

    async fn add_article(
        &self,
        config: &ProviderConfig,
        url: &str,
        tags: &[String],
    ) -> Result<Article> {
        let ProviderConfig::Pocket {
            consumer_key,
            access_token,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let access_token = access_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;

        let resp = self
            .client
            .post(format!("{}/add", Self::API_BASE))
            .header("X-Accept", "application/json")
            .json(&serde_json::json!({
                "consumer_key": consumer_key,
                "access_token": access_token,
                "url": url,
                "tags": tags.join(","),
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to add article: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct AddResponse {
            item: PocketItem,
        }

        #[derive(Deserialize)]
        struct PocketItem {
            item_id: String,
            given_url: String,
            title: Option<String>,
            excerpt: Option<String>,
        }

        let data: AddResponse = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        Ok(Article {
            id: uuid::Uuid::new_v4().to_string(),
            provider: ReadLaterProvider::Pocket,
            provider_id: data.item.item_id,
            url: data.item.given_url,
            title: data.item.title.unwrap_or_default(),
            excerpt: data.item.excerpt,
            author: None,
            word_count: None,
            reading_time_minutes: None,
            tags: tags.to_vec(),
            status: ReadStatus::Unread,
            favorite: false,
            added_at: Utc::now(),
            updated_at: Utc::now(),
            read_at: None,
            content: None,
            image_url: None,
            document_id: None,
            synced_to_device: false,
            last_sync: None,
        })
    }

    async fn delete_article(&self, config: &ProviderConfig, provider_id: &str) -> Result<()> {
        let ProviderConfig::Pocket {
            consumer_key,
            access_token,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let access_token = access_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;

        let resp = self
            .client
            .post(format!("{}/send", Self::API_BASE))
            .header("X-Accept", "application/json")
            .json(&serde_json::json!({
                "consumer_key": consumer_key,
                "access_token": access_token,
                "actions": [{
                    "action": "delete",
                    "item_id": provider_id,
                }]
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to delete: {}",
                resp.status()
            )));
        }

        Ok(())
    }
}

// ============================================================================
// Instapaper Provider
// ============================================================================

pub struct InstapaperProvider {
    client: reqwest::Client,
    consumer_key: String,
    consumer_secret: String,
    api_base: String,
}

impl InstapaperProvider {
    const API_BASE: &'static str = "https://www.instapaper.com/api/1";

    pub fn new(consumer_key: String, consumer_secret: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            consumer_key,
            consumer_secret,
            api_base: Self::API_BASE.to_string(),
        }
    }

    /// Point the provider at a different API root (used by tests against a local server).
    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into().trim_end_matches('/').to_string();
        self
    }

    /// Build a POST request signed with OAuth 1.0a HMAC-SHA1 (the only method Instapaper accepts).
    /// `body` params are form-encoded and included in the signature base string. `oauth_token`
    /// is omitted for the xAuth access-token request, which has no token yet.
    fn signed_post(
        &self,
        url: &str,
        oauth_token: Option<&str>,
        token_secret: Option<&str>,
        body: &[(&str, &str)],
    ) -> reqwest::RequestBuilder {
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let mut oauth: Vec<(&str, &str)> = vec![
            ("oauth_consumer_key", self.consumer_key.as_str()),
            ("oauth_nonce", &nonce),
            ("oauth_signature_method", "HMAC-SHA1"),
            ("oauth_timestamp", &timestamp),
        ];
        if let Some(token) = oauth_token {
            oauth.push(("oauth_token", token));
        }
        oauth.push(("oauth_version", "1.0"));
        let mut all = oauth.clone();
        all.extend_from_slice(body);
        let signature = oauth1_signature("POST", url, &all, &self.consumer_secret, token_secret);
        let mut header_params = oauth;
        header_params.push(("oauth_signature", &signature));
        let header = header_params
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}=\"{}\"",
                    oauth_percent_encode(k),
                    oauth_percent_encode(v)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.client
            .post(url)
            .header("Authorization", format!("OAuth {}", header))
            .form(body)
    }
}

/// Parse an `application/x-www-form-urlencoded` body into decoded name/value pairs.
fn parse_form_urlencoded(body: &str) -> Vec<(String, String)> {
    let decode = |s: &str| {
        let s = s.replace('+', " ");
        urlencoding::decode(&s).map(|c| c.into_owned()).unwrap_or(s)
    };
    body.trim()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(k), decode(v))
        })
        .collect()
}

/// RFC 3986 percent-encoding as required by OAuth 1.0a (RFC 5849 section 3.6):
/// everything except ALPHA / DIGIT / "-" / "." / "_" / "~" is encoded, with uppercase hex.
fn oauth_percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// Signature base string (RFC 5849 section 3.4.1). `base_url` must already be the
/// normalized base string URI (no query/fragment); `params` are decoded name/value pairs.
fn oauth1_base_string(method: &str, base_url: &str, params: &[(&str, &str)]) -> String {
    let mut encoded: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| (oauth_percent_encode(k), oauth_percent_encode(v)))
        .collect();
    encoded.sort();
    let normalized = encoded
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");
    format!(
        "{}&{}&{}",
        method.to_uppercase(),
        oauth_percent_encode(base_url),
        oauth_percent_encode(&normalized)
    )
}

/// OAuth 1.0a HMAC-SHA1 signature (RFC 5849 section 3.4.2), base64-encoded.
fn oauth1_signature(
    method: &str,
    base_url: &str,
    params: &[(&str, &str)],
    consumer_secret: &str,
    token_secret: Option<&str>,
) -> String {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    let key = format!(
        "{}&{}",
        oauth_percent_encode(consumer_secret),
        oauth_percent_encode(token_secret.unwrap_or(""))
    );
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(key.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(oauth1_base_string(method, base_url, params).as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

#[async_trait::async_trait]
impl ReadLaterProviderTrait for InstapaperProvider {
    fn provider_type(&self) -> ReadLaterProvider {
        ReadLaterProvider::Instapaper
    }

    async fn start_oauth(&self, redirect_uri: &str) -> Result<OAuthState> {
        // Instapaper uses xAuth (username/password OAuth), not redirect flow
        Ok(OAuthState {
            provider: ReadLaterProvider::Instapaper,
            request_token: None,
            redirect_uri: redirect_uri.to_string(),
            created_at: Utc::now(),
        })
    }

    /// xAuth: exchange the user's Instapaper username/password (from the callback) for an
    /// access token via a signed `oauth/access_token` request with `x_auth_mode=client_auth`.
    async fn complete_oauth(
        &self,
        callback: &OAuthCallback,
        _state: &OAuthState,
    ) -> Result<ProviderConfig> {
        if self.consumer_key.is_empty() || self.consumer_secret.is_empty() {
            return Err(ReadLaterError::OAuth(
                "INSTAPAPER_CONSUMER_KEY / INSTAPAPER_CONSUMER_SECRET not set".into(),
            ));
        }
        let username = callback
            .username
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or_else(|| ReadLaterError::OAuth("Instapaper xAuth requires a username".into()))?;
        // Instapaper accounts may have no password; send an empty one in that case.
        let password = callback.password.as_deref().unwrap_or("");

        let url = format!("{}/oauth/access_token", self.api_base);
        let resp = self
            .signed_post(
                &url,
                None,
                None,
                &[
                    ("x_auth_username", username),
                    ("x_auth_password", password),
                    ("x_auth_mode", "client_auth"),
                ],
            )
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(ReadLaterError::OAuth(format!(
                "Instapaper xAuth failed: {}",
                status
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        let mut oauth_token = None;
        let mut oauth_token_secret = None;
        for (k, v) in parse_form_urlencoded(&body) {
            match k.as_str() {
                "oauth_token" => oauth_token = Some(v),
                "oauth_token_secret" => oauth_token_secret = Some(v),
                _ => {}
            }
        }
        let (Some(oauth_token), Some(oauth_token_secret)) = (oauth_token, oauth_token_secret)
        else {
            return Err(ReadLaterError::OAuth(
                "Instapaper xAuth response missing oauth_token/oauth_token_secret".into(),
            ));
        };

        Ok(ProviderConfig::Instapaper {
            oauth_token: Some(oauth_token),
            oauth_token_secret: Some(oauth_token_secret),
            username: Some(username.to_string()),
        })
    }

    async fn refresh_auth(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        // Instapaper tokens don't expire
        Ok(config.clone())
    }

    fn is_authenticated(&self, config: &ProviderConfig) -> bool {
        match config {
            ProviderConfig::Instapaper { oauth_token, .. } => oauth_token.is_some(),
            _ => false,
        }
    }

    async fn fetch_articles(
        &self,
        config: &ProviderConfig,
        _since: Option<DateTime<Utc>>,
    ) -> Result<Vec<Article>> {
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let oauth_token = oauth_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();

        let url = format!("{}/bookmarks/list", self.api_base);
        let resp = self
            .signed_post(&url, Some(oauth_token), token_secret, &[("limit", "500")])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Instapaper API error: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum InstapaperItem {
            Bookmark {
                bookmark_id: i64,
                url: String,
                title: String,
                description: Option<String>,
                time: i64,
                progress: f64,
                starred: String,
            },
            Meta {
                // Never read, but serde needs it to recognise this (untagged) variant.
                #[allow(dead_code)]
                #[serde(rename = "type")]
                item_type: String,
            },
        }

        let items: Vec<InstapaperItem> = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        let articles = items
            .into_iter()
            .filter_map(|item| match item {
                InstapaperItem::Bookmark {
                    bookmark_id,
                    url,
                    title,
                    description,
                    time,
                    progress,
                    starred,
                } => {
                    let status = if progress >= 1.0 {
                        ReadStatus::Read
                    } else if progress > 0.0 {
                        ReadStatus::InProgress
                    } else {
                        ReadStatus::Unread
                    };

                    Some(Article {
                        id: uuid::Uuid::new_v4().to_string(),
                        provider: ReadLaterProvider::Instapaper,
                        provider_id: bookmark_id.to_string(),
                        url,
                        title,
                        excerpt: description,
                        author: None,
                        word_count: None,
                        reading_time_minutes: None,
                        tags: Vec::new(),
                        status,
                        favorite: starred == "1",
                        added_at: DateTime::from_timestamp(time, 0).unwrap_or_else(Utc::now),
                        updated_at: DateTime::from_timestamp(time, 0).unwrap_or_else(Utc::now),
                        read_at: if status == ReadStatus::Read {
                            Some(Utc::now())
                        } else {
                            None
                        },
                        content: None,
                        image_url: None,
                        document_id: None,
                        synced_to_device: false,
                        last_sync: None,
                    })
                }
                InstapaperItem::Meta { .. } => None,
            })
            .collect();

        Ok(articles)
    }

    async fn fetch_article_content(
        &self,
        config: &ProviderConfig,
        article: &Article,
    ) -> Result<ArticleContent> {
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let oauth_token = oauth_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();

        let url = format!("{}/bookmarks/get_text", self.api_base);
        let resp = self
            .signed_post(
                &url,
                Some(oauth_token),
                token_secret,
                &[("bookmark_id", article.provider_id.as_str())],
            )
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to get content: {}",
                resp.status()
            )));
        }

        let html = resp
            .text()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        Ok(ArticleContent {
            html,
            images: Vec::new(),
            styles: None,
        })
    }

    async fn update_read_status(
        &self,
        config: &ProviderConfig,
        provider_id: &str,
        status: ReadStatus,
    ) -> Result<()> {
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let oauth_token = oauth_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();

        let endpoint = match status {
            ReadStatus::Archived => "bookmarks/archive",
            ReadStatus::Unread => "bookmarks/unarchive",
            ReadStatus::Read | ReadStatus::InProgress => return Ok(()),
        };

        let url = format!("{}/{}", self.api_base, endpoint);
        let resp = self
            .signed_post(
                &url,
                Some(oauth_token),
                token_secret,
                &[("bookmark_id", provider_id)],
            )
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to update status: {}",
                resp.status()
            )));
        }

        Ok(())
    }

    async fn add_article(
        &self,
        config: &ProviderConfig,
        url: &str,
        _tags: &[String],
    ) -> Result<Article> {
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let oauth_token = oauth_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();

        let api_url = format!("{}/bookmarks/add", self.api_base);
        let resp = self
            .signed_post(&api_url, Some(oauth_token), token_secret, &[("url", url)])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to add article: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct Bookmark {
            bookmark_id: i64,
            url: String,
            title: String,
        }

        let items: Vec<Bookmark> = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        let bookmark = items
            .into_iter()
            .next()
            .ok_or_else(|| ReadLaterError::Api("No bookmark returned".into()))?;

        Ok(Article {
            id: uuid::Uuid::new_v4().to_string(),
            provider: ReadLaterProvider::Instapaper,
            provider_id: bookmark.bookmark_id.to_string(),
            url: bookmark.url,
            title: bookmark.title,
            excerpt: None,
            author: None,
            word_count: None,
            reading_time_minutes: None,
            tags: Vec::new(),
            status: ReadStatus::Unread,
            favorite: false,
            added_at: Utc::now(),
            updated_at: Utc::now(),
            read_at: None,
            content: None,
            image_url: None,
            document_id: None,
            synced_to_device: false,
            last_sync: None,
        })
    }

    async fn delete_article(&self, config: &ProviderConfig, provider_id: &str) -> Result<()> {
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let oauth_token = oauth_token
            .as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();

        let url = format!("{}/bookmarks/delete", self.api_base);
        let resp = self
            .signed_post(
                &url,
                Some(oauth_token),
                token_secret,
                &[("bookmark_id", provider_id)],
            )
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to delete: {}",
                resp.status()
            )));
        }

        Ok(())
    }
}

// ============================================================================
// Wallabag Provider
// ============================================================================

pub struct WallabagProvider {
    client: reqwest::Client,
    /// Config from the latest token refresh, handed out by `take_refreshed_config`.
    refreshed: parking_lot::Mutex<Option<ProviderConfig>>,
}

/// Wallabag's default access-token lifetime, assumed when a token response omits `expires_in`.
const WALLABAG_DEFAULT_TOKEN_SECS: i64 = 3600;

#[derive(Deserialize)]
struct WallabagToken {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

impl WallabagProvider {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            refreshed: parking_lot::Mutex::new(None),
        }
    }

    /// Return `config` unchanged unless its token needs refreshing (see
    /// [`wallabag_token_needs_refresh`]), in which case refresh it.
    async fn ensure_token(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        let ProviderConfig::Wallabag {
            access_token,
            token_expires_at,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        if !wallabag_token_needs_refresh(
            access_token.as_deref(),
            *token_expires_at,
            wallabag_can_refresh(config),
            Utc::now(),
        ) {
            return Ok(config.clone());
        }
        self.refresh_token(config).await
    }

    /// Obtain a new access token unconditionally: the refresh-token grant first, then the
    /// password grant if that fails or there is no refresh token. The new config is recorded
    /// for [`ReadLaterProviderTrait::take_refreshed_config`].
    async fn refresh_token(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        let ProviderConfig::Wallabag {
            instance_url,
            client_id,
            client_secret,
            refresh_token,
            username,
            password,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };

        let client_secret = client_secret
            .as_deref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag client secret".into()))?;
        let token_url = format!("{}/oauth/v2/token", instance_url.trim_end_matches('/'));

        let mut last_err = None;
        let mut token = None;
        if let Some(refresh) = refresh_token {
            match self
                .request_token(
                    &token_url,
                    &[
                        ("grant_type", "refresh_token"),
                        ("refresh_token", refresh),
                        ("client_id", client_id),
                        ("client_secret", client_secret),
                    ],
                )
                .await
            {
                Ok(t) => token = Some(t),
                Err(e) => last_err = Some(e),
            }
        }
        if token.is_none() {
            if let (Some(user), Some(pass)) = (username, password) {
                match self
                    .request_token(
                        &token_url,
                        &[
                            ("grant_type", "password"),
                            ("client_id", client_id),
                            ("client_secret", client_secret),
                            ("username", user),
                            ("password", pass),
                        ],
                    )
                    .await
                {
                    Ok(t) => token = Some(t),
                    Err(e) => last_err = Some(e),
                }
            }
        }
        let Some(token) = token else {
            return Err(last_err.unwrap_or_else(|| ReadLaterError::AuthRequired("Wallabag".into())));
        };

        let mut new_config = config.clone();
        if let ProviderConfig::Wallabag {
            access_token,
            refresh_token,
            token_expires_at,
            ..
        } = &mut new_config
        {
            *access_token = Some(token.access_token);
            if token.refresh_token.is_some() {
                *refresh_token = token.refresh_token;
            }
            *token_expires_at = Some(
                Utc::now()
                    + Duration::seconds(token.expires_in.unwrap_or(WALLABAG_DEFAULT_TOKEN_SECS)),
            );
        }
        *self.refreshed.lock() = Some(new_config.clone());
        Ok(new_config)
    }

    async fn request_token(&self, url: &str, form: &[(&str, &str)]) -> Result<WallabagToken> {
        let resp = self
            .client
            .post(url)
            .form(form)
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(ReadLaterError::OAuth(format!(
                "Token request failed: {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| ReadLaterError::OAuth(e.to_string()))
    }

    /// Send an authenticated API request built by `build(client, instance_url)`, refreshing the
    /// token first when it is missing or expiring. If the server still answers 401 (a revoked
    /// token, or one whose expiry was never recorded), refresh once and retry once.
    async fn send_authed<F>(&self, config: &ProviderConfig, build: F) -> Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder + Send + Sync,
    {
        let config = self.ensure_token(config).await?;
        let resp = self.send_with_token(&config, &build).await?;
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED || !wallabag_can_refresh(&config) {
            return Ok(resp);
        }
        let config = self.refresh_token(&config).await?;
        self.send_with_token(&config, &build).await
    }

    async fn send_with_token<F>(
        &self,
        config: &ProviderConfig,
        build: &F,
    ) -> Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder + Send + Sync,
    {
        let ProviderConfig::Wallabag {
            instance_url,
            access_token,
            ..
        } = config
        else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        let token = access_token
            .as_deref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        build(&self.client, instance_url.trim_end_matches('/'))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))
    }
}

/// Whether a new Wallabag token can be obtained: a client secret plus either a refresh token
/// or password-grant credentials.
fn wallabag_can_refresh(config: &ProviderConfig) -> bool {
    matches!(
        config,
        ProviderConfig::Wallabag {
            client_secret: Some(_),
            refresh_token,
            username,
            password,
            ..
        } if refresh_token.is_some() || (username.is_some() && password.is_some())
    )
}

/// Refresh only when there is no access token, or it has an expiry that is past or within
/// five minutes. A token with no recorded expiry is refreshed when new credentials can be
/// obtained (the refresh records an expiry, so this happens once) and otherwise used as-is;
/// a 401 on use still triggers a refresh-and-retry.
fn wallabag_token_needs_refresh(
    access_token: Option<&str>,
    expires_at: Option<DateTime<Utc>>,
    can_refresh: bool,
    now: DateTime<Utc>,
) -> bool {
    match (access_token, expires_at) {
        (None, _) => true,
        (Some(_), None) => can_refresh,
        (Some(_), Some(exp)) => exp <= now + Duration::minutes(5),
    }
}

#[async_trait::async_trait]
impl ReadLaterProviderTrait for WallabagProvider {
    fn provider_type(&self) -> ReadLaterProvider {
        ReadLaterProvider::Wallabag
    }

    async fn start_oauth(&self, redirect_uri: &str) -> Result<OAuthState> {
        Ok(OAuthState {
            provider: ReadLaterProvider::Wallabag,
            request_token: None,
            redirect_uri: redirect_uri.to_string(),
            created_at: Utc::now(),
        })
    }

    async fn complete_oauth(
        &self,
        callback: &OAuthCallback,
        _state: &OAuthState,
    ) -> Result<ProviderConfig> {
        let _code = callback
            .code
            .as_ref()
            .ok_or_else(|| ReadLaterError::OAuth("Missing authorization code".into()))?;

        // Would need instance_url, client_id, client_secret from somewhere
        Err(ReadLaterError::OAuth(
            "Wallabag OAuth requires instance configuration".into(),
        ))
    }

    async fn refresh_auth(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        self.ensure_token(config).await
    }

    fn is_authenticated(&self, config: &ProviderConfig) -> bool {
        match config {
            ProviderConfig::Wallabag { access_token, .. } => access_token.is_some(),
            _ => false,
        }
    }

    async fn fetch_articles(
        &self,
        config: &ProviderConfig,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<Article>> {
        let mut query = "perPage=100&sort=created&order=desc".to_string();
        if let Some(ts) = since {
            query.push_str(&format!("&since={}", ts.timestamp()));
        }

        let resp = self
            .send_authed(config, |c, base| {
                c.get(format!("{}/api/entries.json?{}", base, query))
            })
            .await?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Wallabag API error: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct WallabagResponse {
            _embedded: Embedded,
        }

        #[derive(Deserialize)]
        struct Embedded {
            items: Vec<WallabagEntry>,
        }

        #[derive(Deserialize)]
        struct WallabagEntry {
            id: i64,
            url: String,
            title: String,
            content: Option<String>,
            reading_time: Option<u32>,
            is_archived: i32,
            is_starred: i32,
            tags: Vec<WallabagTag>,
            created_at: String,
            updated_at: String,
            preview_picture: Option<String>,
        }

        #[derive(Deserialize)]
        struct WallabagTag {
            label: String,
        }

        let data: WallabagResponse = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        let articles = data
            ._embedded
            .items
            .into_iter()
            .map(|entry| {
                let status = if entry.is_archived == 1 {
                    ReadStatus::Archived
                } else {
                    ReadStatus::Unread
                };

                let tags: Vec<String> = entry.tags.into_iter().map(|t| t.label).collect();

                let excerpt = entry.content.as_ref().map(|c| {
                    let stripped: String = c.chars().take(300).collect();
                    stripped
                });

                let created = DateTime::parse_from_rfc3339(&entry.created_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                let updated = DateTime::parse_from_rfc3339(&entry.updated_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                Article {
                    id: uuid::Uuid::new_v4().to_string(),
                    provider: ReadLaterProvider::Wallabag,
                    provider_id: entry.id.to_string(),
                    url: entry.url,
                    title: entry.title,
                    excerpt,
                    author: None,
                    word_count: entry.reading_time.map(|t| t * 200),
                    reading_time_minutes: entry.reading_time,
                    tags,
                    status,
                    favorite: entry.is_starred == 1,
                    added_at: created,
                    updated_at: updated,
                    read_at: if status == ReadStatus::Archived {
                        Some(updated)
                    } else {
                        None
                    },
                    content: entry.content,
                    image_url: entry.preview_picture,
                    document_id: None,
                    synced_to_device: false,
                    last_sync: None,
                }
            })
            .collect();

        Ok(articles)
    }

    async fn fetch_article_content(
        &self,
        config: &ProviderConfig,
        article: &Article,
    ) -> Result<ArticleContent> {
        let resp = self
            .send_authed(config, |c, base| {
                c.get(format!("{}/api/entries/{}.json", base, article.provider_id))
            })
            .await?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to get content: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct Entry {
            content: Option<String>,
        }

        let entry: Entry = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        let html = entry.content.unwrap_or_else(|| {
            format!(
                "<html><body><h1>{}</h1><p><a href=\"{}\">Read original</a></p></body></html>",
                escape_html(&article.title),
                escape_html(&safe_href(&article.url))
            )
        });

        Ok(ArticleContent {
            html,
            images: Vec::new(),
            styles: None,
        })
    }

    async fn update_read_status(
        &self,
        config: &ProviderConfig,
        provider_id: &str,
        status: ReadStatus,
    ) -> Result<()> {
        let archive = match status {
            ReadStatus::Archived | ReadStatus::Read => 1,
            ReadStatus::Unread | ReadStatus::InProgress => 0,
        };
        let body = serde_json::json!({ "archive": archive });

        let resp = self
            .send_authed(config, |c, base| {
                c.patch(format!("{}/api/entries/{}.json", base, provider_id))
                    .json(&body)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to update: {}",
                resp.status()
            )));
        }

        Ok(())
    }

    async fn add_article(
        &self,
        config: &ProviderConfig,
        url: &str,
        tags: &[String],
    ) -> Result<Article> {
        let body = serde_json::json!({
            "url": url,
            "tags": tags.join(","),
        });

        let resp = self
            .send_authed(config, |c, base| {
                c.post(format!("{}/api/entries.json", base)).json(&body)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to add: {}",
                resp.status()
            )));
        }

        #[derive(Deserialize)]
        struct Entry {
            id: i64,
            url: String,
            title: String,
            reading_time: Option<u32>,
        }

        let entry: Entry = resp
            .json()
            .await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;

        Ok(Article {
            id: uuid::Uuid::new_v4().to_string(),
            provider: ReadLaterProvider::Wallabag,
            provider_id: entry.id.to_string(),
            url: entry.url,
            title: entry.title,
            excerpt: None,
            author: None,
            word_count: entry.reading_time.map(|t| t * 200),
            reading_time_minutes: entry.reading_time,
            tags: tags.to_vec(),
            status: ReadStatus::Unread,
            favorite: false,
            added_at: Utc::now(),
            updated_at: Utc::now(),
            read_at: None,
            content: None,
            image_url: None,
            document_id: None,
            synced_to_device: false,
            last_sync: None,
        })
    }

    async fn delete_article(&self, config: &ProviderConfig, provider_id: &str) -> Result<()> {
        let resp = self
            .send_authed(config, |c, base| {
                c.delete(format!("{}/api/entries/{}.json", base, provider_id))
            })
            .await?;

        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!(
                "Failed to delete: {}",
                resp.status()
            )));
        }

        Ok(())
    }

    fn take_refreshed_config(&self) -> Option<ProviderConfig> {
        self.refreshed.lock().take()
    }
}

// ============================================================================
// EPUB/PDF Converter
// ============================================================================

/// Escape text for HTML/XHTML element content and quoted attribute values.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Only allow http(s) links in generated `href`s; anything else (e.g. `javascript:`) becomes `#`.
/// The result still needs `escape_html` for the attribute context.
fn safe_href(url: &str) -> String {
    let lower = url.trim_start().to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        url.trim_start().to_string()
    } else {
        "#".to_string()
    }
}

/// An article rendered for the device.
#[derive(Debug, Clone)]
pub struct RenderedArticle {
    /// File extension, which is also the reMarkable `fileType`: `epub`, `pdf` or `html`.
    pub ext: &'static str,
    pub bytes: Vec<u8>,
}

/// Renders articles into the format an account delivers. Blocking: the EPUB is built in memory
/// and the PDF by running `weasyprint` or `wkhtmltopdf`, so async callers run it on a blocking
/// thread.
pub struct ArticleConverter;

impl ArticleConverter {
    pub fn render(
        article: &Article,
        content: &ArticleContent,
        format: ArticleFormat,
    ) -> Result<RenderedArticle> {
        let (ext, bytes) = match format {
            ArticleFormat::Html => ("html", Self::html_document(article, content).into_bytes()),
            ArticleFormat::Epub => ("epub", Self::epub(article, content)?),
            ArticleFormat::Pdf => ("pdf", Self::pdf(article, content)?),
        };
        Ok(RenderedArticle { ext, bytes })
    }

    fn html_document(article: &Article, content: &ArticleContent) -> String {
        format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{}</title>
    <style>
        body {{ font-family: Georgia, serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
        h1 {{ font-size: 2em; margin-bottom: 0.5em; }}
        .meta {{ color: #666; margin-bottom: 2em; }}
        img {{ max-width: 100%; height: auto; }}
    </style>
    {}
</head>
<body>
    <h1>{}</h1>
    <div class="meta">
        {}
        <a href="{}">Original</a>
    </div>
    {}
</body>
</html>"#,
            escape_html(&article.title),
            content.styles.as_deref().unwrap_or(""),
            escape_html(&article.title),
            article
                .author
                .as_deref()
                .map(|a| format!("By {} • ", escape_html(a)))
                .unwrap_or_default(),
            escape_html(&safe_href(&article.url)),
            content.html
        )
    }

    /// Build the EPUB in memory (the same library the feed EPUBs use), so no `zip` binary or
    /// scratch files are needed.
    fn epub(article: &Article, content: &ArticleContent) -> Result<Vec<u8>> {
        fn epub_err(e: impl std::fmt::Display) -> ReadLaterError {
            ReadLaterError::Conversion(format!("EPUB: {e}"))
        }
        let mut builder =
            epub_builder::EpubBuilder::new(epub_builder::ZipLibrary::new().map_err(epub_err)?)
                .map_err(epub_err)?;
        builder
            .metadata("title", article.title.as_str())
            .map_err(epub_err)?;
        if let Some(author) = &article.author {
            builder
                .metadata("author", author.as_str())
                .map_err(epub_err)?;
        }
        builder
            .metadata("generator", "remarkable-server")
            .map_err(epub_err)?;
        let xhtml = Self::epub_content_xhtml(article, content);
        builder
            .add_content(
                epub_builder::EpubContent::new("content.xhtml", xhtml.as_bytes())
                    .title(article.title.as_str()),
            )
            .map_err(epub_err)?;
        let mut out = Vec::new();
        builder.generate(&mut out).map_err(epub_err)?;
        Ok(out)
    }

    fn epub_content_xhtml(article: &Article, content: &ArticleContent) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>{}</title>
<style>
body {{ font-family: Georgia, serif; line-height: 1.6; margin: 1em; }}
h1 {{ font-size: 1.5em; }}
img {{ max-width: 100%; }}
</style>
</head>
<body>
<h1>{}</h1>
<p><em>By {}</em></p>
<p><a href="{}">Original article</a></p>
<hr/>
{}
</body>
</html>"#,
            escape_html(&article.title),
            escape_html(&article.title),
            escape_html(article.author.as_deref().unwrap_or("Unknown")),
            escape_html(&safe_href(&article.url)),
            content.html
        )
    }

    /// Print the HTML rendering with `weasyprint`, else `wkhtmltopdf`, in a scratch directory.
    fn pdf(article: &Article, content: &ArticleContent) -> Result<Vec<u8>> {
        use std::process::Command;

        let dir = tempfile::tempdir()?;
        let html_path = dir.path().join("article.html");
        let pdf_path = dir.path().join("article.pdf");
        std::fs::write(&html_path, Self::html_document(article, content))?;

        let converters: [(&str, &[&std::ffi::OsStr]); 2] = [
            ("weasyprint", &[html_path.as_os_str(), pdf_path.as_os_str()]),
            (
                "wkhtmltopdf",
                &[
                    std::ffi::OsStr::new("--quiet"),
                    html_path.as_os_str(),
                    pdf_path.as_os_str(),
                ],
            ),
        ];
        for (program, args) in converters {
            let printed = Command::new(program)
                .args(args)
                .output()
                .is_ok_and(|o| o.status.success());
            if printed {
                return Ok(std::fs::read(&pdf_path)?);
            }
        }
        Err(ReadLaterError::Conversion(
            "No PDF converter available (install weasyprint or wkhtmltopdf)".into(),
        ))
    }
}

// ============================================================================
// Read Later Manager
// ============================================================================

/// Read-later accounts and articles, persisted in `readlater.db`. Syncing them to the device is
/// [`crate::readlater_sync::ReadLaterSyncer`]'s job.
pub struct ReadLaterManager {
    db: Connection,
    accounts: Arc<RwLock<HashMap<String, ProviderAccount>>>,
    articles: Arc<RwLock<HashMap<String, Article>>>,
}

impl ReadLaterManager {
    pub fn new(db_path: &Path) -> Result<Self> {
        let db = Connection::open(db_path).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        Self::init_schema(&db)?;

        let mut mgr = Self {
            db,
            accounts: Arc::new(RwLock::new(HashMap::new())),
            articles: Arc::new(RwLock::new(HashMap::new())),
        };

        mgr.load_accounts()?;
        mgr.load_articles()?;

        Ok(mgr)
    }

    fn init_schema(db: &Connection) -> Result<()> {
        db.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS readlater_accounts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                provider TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                config TEXT NOT NULL,
                sync_settings TEXT NOT NULL,
                last_sync TEXT,
                created_at TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS readlater_articles (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                account_id TEXT,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                excerpt TEXT,
                author TEXT,
                word_count INTEGER,
                reading_time_minutes INTEGER,
                tags TEXT,
                status TEXT NOT NULL,
                favorite INTEGER DEFAULT 0,
                added_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                read_at TEXT,
                image_url TEXT,
                document_id TEXT,
                synced_to_device INTEGER DEFAULT 0,
                last_sync TEXT,
                UNIQUE(provider, provider_id)
            );
            
            CREATE TABLE IF NOT EXISTS readlater_oauth_states (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                request_token TEXT,
                redirect_uri TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_articles_provider ON readlater_articles(provider);
            CREATE INDEX IF NOT EXISTS idx_articles_status ON readlater_articles(status);
            CREATE INDEX IF NOT EXISTS idx_articles_synced ON readlater_articles(synced_to_device);
        "#,
        )
        .map_err(|e| ReadLaterError::Database(e.to_string()))?;

        // Provider credentials (JSON `ProviderSecrets`), split from `config` so they survive
        // restarts without ever being part of the client-facing config JSON.
        Self::ensure_column(db, "readlater_accounts", "secrets", "TEXT")?;

        Ok(())
    }

    /// `ALTER TABLE ... ADD COLUMN` unless the column already exists, so databases created
    /// by older versions are migrated in place.
    fn ensure_column(db: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
        let mut stmt = db
            .prepare(&format!("PRAGMA table_info({})", table))
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        let exists = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| ReadLaterError::Database(e.to_string()))?
            .filter_map(|name| name.ok())
            .any(|name| name == column);
        if !exists {
            db.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                table, column, decl
            ))
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        }
        Ok(())
    }

    fn load_accounts(&mut self) -> Result<()> {
        let mut stmt = self.db.prepare(
            "SELECT id, name, provider, enabled, config, sync_settings, last_sync, created_at, secrets FROM readlater_accounts"
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let name: String = row.get(1)?;
                let provider_str: String = row.get(2)?;
                let enabled: bool = row.get::<_, i32>(3)? != 0;
                let config_json: String = row.get(4)?;
                let sync_json: String = row.get(5)?;
                let last_sync: Option<String> = row.get(6)?;
                let created_at: String = row.get(7)?;
                let secrets_json: Option<String> = row.get(8)?;

                Ok((
                    id,
                    name,
                    provider_str,
                    enabled,
                    config_json,
                    sync_json,
                    last_sync,
                    created_at,
                    secrets_json,
                ))
            })
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;

        let mut accounts = self.accounts.write();

        for row in rows {
            let (
                id,
                name,
                provider_str,
                enabled,
                config_json,
                sync_json,
                last_sync,
                created_at,
                secrets_json,
            ) = row.map_err(|e| ReadLaterError::Database(e.to_string()))?;

            let provider: ReadLaterProvider =
                serde_json::from_str(&format!("\"{}\"", provider_str))
                    .map_err(|e| ReadLaterError::Database(e.to_string()))?;

            let mut config: ProviderConfig = serde_json::from_str(&config_json)
                .map_err(|e| ReadLaterError::Database(e.to_string()))?;
            if let Some(secrets_json) = secrets_json {
                let secrets: ProviderSecrets = serde_json::from_str(&secrets_json)
                    .map_err(|e| ReadLaterError::Database(e.to_string()))?;
                config.apply_secrets(secrets);
            }

            let sync_settings: SyncSettings = serde_json::from_str(&sync_json)
                .map_err(|e| ReadLaterError::Database(e.to_string()))?;

            let last_sync_dt = last_sync.and_then(|s| {
                DateTime::parse_from_rfc3339(&s)
                    .ok()
                    .map(|d| d.with_timezone(&Utc))
            });

            let created_dt = DateTime::parse_from_rfc3339(&created_at)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            accounts.insert(
                id.clone(),
                ProviderAccount {
                    id,
                    name,
                    provider,
                    enabled,
                    config,
                    sync_settings,
                    last_sync: last_sync_dt,
                    created_at: created_dt,
                },
            );
        }

        Ok(())
    }

    fn load_articles(&mut self) -> Result<()> {
        let mut stmt = self.db.prepare(
            "SELECT id, provider, provider_id, url, title, excerpt, author, word_count, reading_time_minutes,              tags, status, favorite, added_at, updated_at, read_at, image_url, document_id, synced_to_device, last_sync              FROM readlater_articles"
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let provider_str: String = row.get(1)?;
                let provider_id: String = row.get(2)?;
                let url: String = row.get(3)?;
                let title: String = row.get(4)?;
                let excerpt: Option<String> = row.get(5)?;
                let author: Option<String> = row.get(6)?;
                let word_count: Option<u32> = row.get(7)?;
                let reading_time: Option<u32> = row.get(8)?;
                let tags_json: Option<String> = row.get(9)?;
                let status_str: String = row.get(10)?;
                let favorite: bool = row.get::<_, i32>(11)? != 0;
                let added_at: String = row.get(12)?;
                let updated_at: String = row.get(13)?;
                let read_at: Option<String> = row.get(14)?;
                let image_url: Option<String> = row.get(15)?;
                let document_id: Option<String> = row.get(16)?;
                let synced: bool = row.get::<_, i32>(17)? != 0;
                let last_sync: Option<String> = row.get(18)?;

                Ok((
                    id,
                    provider_str,
                    provider_id,
                    url,
                    title,
                    excerpt,
                    author,
                    word_count,
                    reading_time,
                    tags_json,
                    status_str,
                    favorite,
                    added_at,
                    updated_at,
                    read_at,
                    image_url,
                    document_id,
                    synced,
                    last_sync,
                ))
            })
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;

        let mut articles = self.articles.write();

        for row in rows {
            let (
                id,
                provider_str,
                provider_id,
                url,
                title,
                excerpt,
                author,
                word_count,
                reading_time,
                tags_json,
                status_str,
                favorite,
                added_at,
                updated_at,
                read_at,
                image_url,
                document_id,
                synced,
                last_sync,
            ) = row.map_err(|e| ReadLaterError::Database(e.to_string()))?;

            let provider: ReadLaterProvider =
                serde_json::from_str(&format!("\"{}\"", provider_str))
                    .map_err(|e| ReadLaterError::Database(e.to_string()))?;

            let status: ReadStatus = serde_json::from_str(&format!("\"{}\"", status_str))
                .map_err(|e| ReadLaterError::Database(e.to_string()))?;

            let tags: Vec<String> = tags_json
                .map(|j| serde_json::from_str(&j).unwrap_or_default())
                .unwrap_or_default();

            let parse_dt = |s: &str| {
                DateTime::parse_from_rfc3339(s)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now())
            };

            articles.insert(
                id.clone(),
                Article {
                    id,
                    provider,
                    provider_id,
                    url,
                    title,
                    excerpt,
                    author,
                    word_count,
                    reading_time_minutes: reading_time,
                    tags,
                    status,
                    favorite,
                    added_at: parse_dt(&added_at),
                    updated_at: parse_dt(&updated_at),
                    read_at: read_at.map(|s| parse_dt(&s)),
                    content: None,
                    image_url,
                    document_id,
                    synced_to_device: synced,
                    last_sync: last_sync.map(|s| parse_dt(&s)),
                },
            );
        }

        Ok(())
    }

    // Account management

    /// Serialize a config into its `config` (credential-free) and `secrets` column values.
    fn config_columns(config: &ProviderConfig) -> Result<(String, String)> {
        Ok((
            serde_json::to_string(config)?,
            serde_json::to_string(&config.secrets())?,
        ))
    }

    pub fn add_account(&mut self, account: ProviderAccount) -> Result<()> {
        if account.provider == ReadLaterProvider::Omnivore {
            return Err(ReadLaterError::Discontinued(OMNIVORE_DISCONTINUED.into()));
        }
        let (config_json, secrets_json) = Self::config_columns(&account.config)?;
        let sync_json = serde_json::to_string(&account.sync_settings)?;

        self.db.execute(
            "INSERT INTO readlater_accounts (id, name, provider, enabled, config, sync_settings, last_sync, created_at, secrets)              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                account.id,
                account.name,
                account.provider.to_string(),
                account.enabled as i32,
                config_json,
                sync_json,
                account.last_sync.map(|d| d.to_rfc3339()),
                account.created_at.to_rfc3339(),
                secrets_json,
            ],
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        self.accounts.write().insert(account.id.clone(), account);
        Ok(())
    }

    pub fn update_account(&mut self, account: ProviderAccount) -> Result<()> {
        let (config_json, secrets_json) = Self::config_columns(&account.config)?;
        let sync_json = serde_json::to_string(&account.sync_settings)?;

        self.db.execute(
            "UPDATE readlater_accounts SET name=?2, enabled=?3, config=?4, sync_settings=?5, last_sync=?6, secrets=?7 WHERE id=?1",
            params![
                account.id,
                account.name,
                account.enabled as i32,
                config_json,
                sync_json,
                account.last_sync.map(|d| d.to_rfc3339()),
                secrets_json,
            ],
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        self.accounts.write().insert(account.id.clone(), account);
        Ok(())
    }

    /// Persist a new provider config (e.g. refreshed tokens) for an account, touching only
    /// the credential columns so concurrent edits to name/settings are not overwritten.
    pub fn update_account_config(&mut self, id: &str, config: &ProviderConfig) -> Result<()> {
        let (config_json, secrets_json) = Self::config_columns(config)?;
        let changed = self
            .db
            .execute(
                "UPDATE readlater_accounts SET config=?2, secrets=?3 WHERE id=?1",
                params![id, config_json, secrets_json],
            )
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        if changed == 0 {
            return Err(ReadLaterError::ProviderNotFound(id.into()));
        }
        if let Some(account) = self.accounts.write().get_mut(id) {
            account.config = config.clone();
        }
        Ok(())
    }

    /// Record the start of the last fully synced window, touching only `last_sync` so
    /// concurrent edits to name/settings are not overwritten.
    pub fn set_last_sync(&mut self, id: &str, at: DateTime<Utc>) -> Result<()> {
        let changed = self
            .db
            .execute(
                "UPDATE readlater_accounts SET last_sync=?2 WHERE id=?1",
                params![id, at.to_rfc3339()],
            )
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        if changed == 0 {
            return Err(ReadLaterError::ProviderNotFound(id.into()));
        }
        if let Some(account) = self.accounts.write().get_mut(id) {
            account.last_sync = Some(at);
        }
        Ok(())
    }

    pub fn delete_account(&mut self, id: &str) -> Result<()> {
        self.db
            .execute("DELETE FROM readlater_accounts WHERE id=?1", params![id])
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        self.accounts.write().remove(id);
        Ok(())
    }

    pub fn get_account(&self, id: &str) -> Option<ProviderAccount> {
        self.accounts.read().get(id).cloned()
    }

    pub fn list_accounts(&self) -> Vec<ProviderAccount> {
        self.accounts.read().values().cloned().collect()
    }

    // Article management

    pub fn save_article(&mut self, article: &Article) -> Result<()> {
        self.upsert_article(article).map(|_| ())
    }

    /// Insert or update an article keyed by its natural key `(provider, provider_id)`
    /// (the table's UNIQUE constraint). If a row already exists under a different id — e.g.
    /// a provider re-fetch that minted a fresh uuid — the existing id is kept, as is the
    /// device-side state (document_id / synced_to_device / last_sync) the provider can't
    /// know about, so the DB row and the in-memory map stay one entry per article.
    /// Returns the article as stored.
    pub(crate) fn upsert_article(&mut self, article: &Article) -> Result<Article> {
        let mut article = article.clone();
        let existing_id: Option<String> = self
            .db
            .query_row(
                "SELECT id FROM readlater_articles WHERE provider=?1 AND provider_id=?2",
                params![article.provider.to_string(), article.provider_id],
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(ReadLaterError::Database(e.to_string())),
            })?;
        if let Some(existing_id) = existing_id.filter(|id| *id != article.id) {
            if let Some(prev) = self.articles.read().get(&existing_id) {
                if article.document_id.is_none() {
                    article.document_id = prev.document_id.clone();
                }
                article.synced_to_device |= prev.synced_to_device;
                if article.last_sync.is_none() {
                    article.last_sync = prev.last_sync;
                }
            }
            article.id = existing_id;
        }
        let tags_json = serde_json::to_string(&article.tags)?;

        self.db.execute(
            "INSERT OR REPLACE INTO readlater_articles              (id, provider, provider_id, url, title, excerpt, author, word_count, reading_time_minutes,               tags, status, favorite, added_at, updated_at, read_at, image_url, document_id, synced_to_device, last_sync)              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            params![
                article.id,
                article.provider.to_string(),
                article.provider_id,
                article.url,
                article.title,
                article.excerpt,
                article.author,
                article.word_count,
                article.reading_time_minutes,
                tags_json,
                format!("{:?}", article.status).to_lowercase(),
                article.favorite as i32,
                article.added_at.to_rfc3339(),
                article.updated_at.to_rfc3339(),
                article.read_at.map(|d| d.to_rfc3339()),
                article.image_url,
                article.document_id,
                article.synced_to_device as i32,
                article.last_sync.map(|d| d.to_rfc3339()),
            ],
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        self.articles
            .write()
            .insert(article.id.clone(), article.clone());
        Ok(article)
    }

    pub fn get_article(&self, id: &str) -> Option<Article> {
        self.articles.read().get(id).cloned()
    }

    /// Record that `article_id` is on the device as document `document_id`, so later syncs
    /// never add it again.
    pub fn mark_delivered(&mut self, article_id: &str, document_id: &str) -> Result<Article> {
        let mut article = self
            .get_article(article_id)
            .ok_or_else(|| ReadLaterError::ArticleNotFound(article_id.into()))?;
        article.synced_to_device = true;
        article.document_id = Some(document_id.into());
        article.last_sync = Some(Utc::now());
        self.upsert_article(&article)
    }

    /// Articles of `provider` on the device whose read/archived status goes back to the
    /// provider when an account has `sync_read_status`.
    pub(crate) fn read_status_candidates(&self, provider: ReadLaterProvider) -> Vec<Article> {
        self.articles
            .read()
            .values()
            .filter(|a| {
                a.provider == provider
                    && a.synced_to_device
                    && matches!(a.status, ReadStatus::Read | ReadStatus::Archived)
            })
            .cloned()
            .collect()
    }

    pub fn query_articles(&self, query: &ArticleQuery) -> Vec<Article> {
        let articles = self.articles.read();
        let mut results: Vec<Article> = articles
            .values()
            .filter(|a| {
                if let Some(provider) = query.provider {
                    if a.provider != provider {
                        return false;
                    }
                }
                if let Some(status) = query.status {
                    if a.status != status {
                        return false;
                    }
                }
                if let Some(favorite) = query.favorite {
                    if a.favorite != favorite {
                        return false;
                    }
                }
                if let Some(synced) = query.synced {
                    if a.synced_to_device != synced {
                        return false;
                    }
                }
                if let Some(ref tags) = query.tags {
                    if !tags.iter().any(|t| a.tags.contains(t)) {
                        return false;
                    }
                }
                if let Some(ref search) = query.search {
                    let s = search.to_lowercase();
                    if !a.title.to_lowercase().contains(&s)
                        && !a
                            .excerpt
                            .as_ref()
                            .map(|e| e.to_lowercase().contains(&s))
                            .unwrap_or(false)
                    {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        // Sort
        match query.sort_by.unwrap_or_default() {
            ArticleSortBy::AddedAt => results.sort_by(|a, b| b.added_at.cmp(&a.added_at)),
            ArticleSortBy::UpdatedAt => results.sort_by(|a, b| b.updated_at.cmp(&a.updated_at)),
            ArticleSortBy::Title => results.sort_by(|a, b| a.title.cmp(&b.title)),
            ArticleSortBy::ReadingTime => {
                results.sort_by(|a, b| b.reading_time_minutes.cmp(&a.reading_time_minutes));
            }
        }

        if matches!(query.sort_order.unwrap_or_default(), SortOrder::Asc) {
            results.reverse();
        }

        // Pagination
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(50);

        results.into_iter().skip(offset).take(limit).collect()
    }

    pub fn delete_article(&mut self, id: &str) -> Result<()> {
        self.db
            .execute("DELETE FROM readlater_articles WHERE id=?1", params![id])
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        self.articles.write().remove(id);
        Ok(())
    }

    // OAuth state management

    pub fn save_oauth_state(&self, state: &OAuthState) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();

        self.db.execute(
            "INSERT INTO readlater_oauth_states (id, provider, request_token, redirect_uri, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id,
                state.provider.to_string(),
                state.request_token,
                state.redirect_uri,
                state.created_at.to_rfc3339(),
            ],
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        Ok(id)
    }

    pub fn get_oauth_state(&self, id: &str) -> Result<Option<OAuthState>> {
        let mut stmt = self.db.prepare(
            "SELECT provider, request_token, redirect_uri, created_at FROM readlater_oauth_states WHERE id=?1"
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;

        let result = stmt.query_row(params![id], |row| {
            let provider_str: String = row.get(0)?;
            let request_token: Option<String> = row.get(1)?;
            let redirect_uri: String = row.get(2)?;
            let created_at: String = row.get(3)?;
            Ok((provider_str, request_token, redirect_uri, created_at))
        });

        match result {
            Ok((provider_str, request_token, redirect_uri, created_at)) => {
                let provider: ReadLaterProvider =
                    serde_json::from_str(&format!("\"{}\"", provider_str))
                        .map_err(|e| ReadLaterError::Database(e.to_string()))?;
                let created = DateTime::parse_from_rfc3339(&created_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                Ok(Some(OAuthState {
                    provider,
                    request_token,
                    redirect_uri,
                    created_at: created,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(ReadLaterError::Database(e.to_string())),
        }
    }

    pub fn delete_oauth_state(&self, id: &str) -> Result<()> {
        self.db
            .execute(
                "DELETE FROM readlater_oauth_states WHERE id=?1",
                params![id],
            )
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        Ok(())
    }
}

/// Apply the account's sync filters (tags, favorites, archived) and cap the result at
/// `max_articles` (0 = unlimited), keeping the most recently added articles. The cap is per
/// sync: articles it drops are older than everything kept and are not revisited later.
/// Tag filters: an article must carry none of the exclude tags and, when any include
/// filters exist, at least one include tag.
pub(crate) fn select_articles_for_sync(
    articles: Vec<Article>,
    settings: &SyncSettings,
) -> Vec<Article> {
    let mut selected: Vec<Article> = articles
        .into_iter()
        .filter(|article| {
            let has_tag = |tag: &str| article.tags.iter().any(|t| t.eq_ignore_ascii_case(tag));
            if settings
                .tag_filters
                .iter()
                .any(|f| !f.include && has_tag(&f.tag))
            {
                return false;
            }
            let mut includes = settings.tag_filters.iter().filter(|f| f.include).peekable();
            if includes.peek().is_some() && !includes.any(|f| has_tag(&f.tag)) {
                return false;
            }
            if settings.include_favorites_only && !article.favorite {
                return false;
            }
            if !settings.include_archived && article.status == ReadStatus::Archived {
                return false;
            }
            true
        })
        .collect();
    selected.sort_by(|a, b| b.added_at.cmp(&a.added_at));
    if settings.max_articles > 0 {
        selected.truncate(settings.max_articles as usize);
    }
    selected
}

// Default implementations
impl Default for PocketProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for WallabagProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixtures shared by the read-later test modules.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn test_account(
        id: &str,
        provider: ReadLaterProvider,
        config: ProviderConfig,
        last_sync: Option<DateTime<Utc>>,
    ) -> ProviderAccount {
        ProviderAccount {
            id: id.into(),
            name: id.into(),
            provider,
            enabled: true,
            config,
            sync_settings: SyncSettings::default(),
            last_sync,
            created_at: Utc::now(),
        }
    }

    pub(crate) fn wallabag_config(
        instance_url: &str,
        access_token: Option<&str>,
        refresh_token: Option<&str>,
        token_expires_at: Option<DateTime<Utc>>,
    ) -> ProviderConfig {
        ProviderConfig::Wallabag {
            instance_url: instance_url.into(),
            client_id: "cid".into(),
            client_secret: Some("csecret".into()),
            access_token: access_token.map(Into::into),
            refresh_token: refresh_token.map(Into::into),
            token_expires_at,
            username: None,
            password: None,
        }
    }

    /// Serve `app` on an ephemeral local port; returns its base URL.
    pub(crate) async fn spawn_server(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{}", addr)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    fn article(id: &str, provider_id: &str, added_secs: i64) -> Article {
        let t = DateTime::from_timestamp(added_secs, 0).unwrap();
        Article {
            id: id.into(),
            provider: ReadLaterProvider::Pocket,
            provider_id: provider_id.into(),
            url: "https://example.com/a".into(),
            title: "T".into(),
            excerpt: None,
            author: None,
            word_count: None,
            reading_time_minutes: None,
            tags: Vec::new(),
            status: ReadStatus::Unread,
            favorite: false,
            added_at: t,
            updated_at: t,
            read_at: None,
            content: None,
            image_url: None,
            document_id: None,
            synced_to_device: false,
            last_sync: None,
        }
    }

    #[test]
    fn oauth_percent_encoding_is_rfc3986() {
        assert_eq!(oauth_percent_encode("abcABC123-._~"), "abcABC123-._~");
        assert_eq!(oauth_percent_encode("a b+c/=&%"), "a%20b%2Bc%2F%3D%26%25");
        assert_eq!(oauth_percent_encode("\u{2603}"), "%E2%98%83");
    }

    #[test]
    fn oauth_base_string_matches_rfc5849_3_4_1_1() {
        // RFC 5849 section 3.4.1.1 example (decoded query, body and oauth params).
        let params = [
            ("b5", "=%3D"),
            ("a3", "a"),
            ("c@", ""),
            ("a2", "r b"),
            ("oauth_consumer_key", "9djdj82h48djs9d2"),
            ("oauth_token", "kkk9d7dh3k39sjv7"),
            ("oauth_signature_method", "HMAC-SHA1"),
            ("oauth_timestamp", "137131201"),
            ("oauth_nonce", "7d8f3e4a"),
            ("c2", ""),
            ("a3", "2 q"),
        ];
        assert_eq!(
            oauth1_base_string("POST", "http://example.com/request", &params),
            "POST&http%3A%2F%2Fexample.com%2Frequest&a2%3Dr%2520b%26a3%3D2%2520q%26a3%3Da%26b5%3D%253D%25253D%26c%2540%3D%26c2%3D%26oauth_consumer_key%3D9djdj82h48djs9d2%26oauth_nonce%3D7d8f3e4a%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D137131201%26oauth_token%3Dkkk9d7dh3k39sjv7"
        );
    }

    #[test]
    fn oauth_hmac_sha1_signature_matches_known_vector() {
        // OAuth Core 1.0 Appendix A.5 (the photos.example.net example, reused by RFC 5849).
        let params = [
            ("file", "vacation.jpg"),
            ("size", "original"),
            ("oauth_consumer_key", "dpf43f3p2l4k3l03"),
            ("oauth_token", "nnch734d00sl2jdk"),
            ("oauth_signature_method", "HMAC-SHA1"),
            ("oauth_timestamp", "1191242096"),
            ("oauth_nonce", "kllo9940pd9333jh"),
            ("oauth_version", "1.0"),
        ];
        assert_eq!(
            oauth1_signature(
                "GET",
                "http://photos.example.net/photos",
                &params,
                "kd94hf93k423kf44",
                Some("pfkkdhi9sl3r4s00")
            ),
            "tR3+Ty81lMeYAr/Fid0kMTYa/WM="
        );
        // RFC 5849 section 1.2 authenticated request example.
        let params = [
            ("file", "vacation.jpg"),
            ("size", "original"),
            ("oauth_consumer_key", "dpf43f3p2l4k3l03"),
            ("oauth_token", "nnch734d00sl2jdk"),
            ("oauth_signature_method", "HMAC-SHA1"),
            ("oauth_timestamp", "137131202"),
            ("oauth_nonce", "chapoH"),
        ];
        assert_eq!(
            oauth1_signature(
                "GET",
                "http://photos.example.net/photos",
                &params,
                "kd94hf93k423kf44",
                Some("pfkkdhi9sl3r4s00")
            ),
            "MdpQcU8iPSUjWoN/UDMsK2sui9I="
        );
    }

    #[test]
    fn wallabag_refreshes_only_when_needed() {
        let now = Utc::now();
        assert!(wallabag_token_needs_refresh(None, None, true, now));
        assert!(wallabag_token_needs_refresh(
            None,
            Some(now + Duration::hours(1)),
            true,
            now
        ));
        // Unknown expiry: refresh once if we can (the refresh records an expiry), else use as-is.
        assert!(wallabag_token_needs_refresh(Some("t"), None, true, now));
        assert!(!wallabag_token_needs_refresh(Some("t"), None, false, now));
        assert!(!wallabag_token_needs_refresh(
            Some("t"),
            Some(now + Duration::hours(1)),
            true,
            now
        ));
        assert!(wallabag_token_needs_refresh(
            Some("t"),
            Some(now + Duration::minutes(2)),
            true,
            now
        ));
        assert!(wallabag_token_needs_refresh(
            Some("t"),
            Some(now - Duration::minutes(1)),
            true,
            now
        ));
    }

    #[test]
    fn escape_html_and_safe_href() {
        assert_eq!(
            escape_html(r#"<a href="x">'&'</a>"#),
            "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;"
        );
        assert_eq!(
            safe_href("https://ex.com/?a=1&b=2"),
            "https://ex.com/?a=1&b=2"
        );
        assert_eq!(safe_href("javascript:alert(1)"), "#");
        assert_eq!(safe_href(" JavaScript:alert(1)"), "#");
    }

    fn hostile_article() -> Article {
        let mut a = article("id-1", "p1", 0);
        a.title = "</title><script>alert(1)</script>".into();
        a.author = Some("A & B <x>".into());
        a.url = "https://ex.com/?q=\"><script>".into();
        a
    }

    #[test]
    fn html_export_escapes_metadata() {
        let content = ArticleContent {
            html: "<p>body</p>".into(),
            images: Vec::new(),
            styles: None,
        };
        let rendered =
            ArticleConverter::render(&hostile_article(), &content, ArticleFormat::Html).unwrap();
        assert_eq!(rendered.ext, "html");
        let html = String::from_utf8(rendered.bytes).unwrap();
        assert!(!html.contains("<script>"));
        assert!(
            html.contains("<title>&lt;/title&gt;&lt;script&gt;alert(1)&lt;/script&gt;</title>")
        );
        assert!(html.contains("By A &amp; B &lt;x&gt;"));
        assert!(html.contains(r#"href="https://ex.com/?q=&quot;&gt;&lt;script&gt;""#));
        assert!(html.contains("<p>body</p>"));
    }

    #[test]
    fn epub_xhtml_escapes_metadata() {
        let content = ArticleContent {
            html: "<p>body</p>".into(),
            images: Vec::new(),
            styles: None,
        };
        let xhtml = ArticleConverter::epub_content_xhtml(&hostile_article(), &content);
        assert!(!xhtml.contains("<script>"));
        assert!(xhtml.contains(r#"href="https://ex.com/?q=&quot;&gt;&lt;script&gt;""#));
        assert!(xhtml.contains("max-width: 100%;"));
    }

    /// EPUBs are built in memory (no `zip` binary): a ZIP whose first entry is the stored
    /// `mimetype`, as EPUB readers require, with the article as `OEBPS/content.xhtml`.
    #[test]
    fn epub_is_rendered_in_memory() {
        let content = ArticleContent {
            html: "<p>body text</p>".into(),
            images: Vec::new(),
            styles: None,
        };
        let rendered =
            ArticleConverter::render(&hostile_article(), &content, ArticleFormat::Epub).unwrap();
        assert_eq!(rendered.ext, "epub");
        let b = &rendered.bytes;
        assert_eq!(&b[..4], b"PK\x03\x04");
        assert_eq!(&b[30..38], b"mimetype");
        assert_eq!(&b[38..58], b"application/epub+zip");
        let find = |needle: &[u8]| b.windows(needle.len()).any(|w| w == needle);
        assert!(find(b"OEBPS/content.xhtml"));
        assert!(find(b"META-INF/container.xml"));
    }

    #[test]
    fn save_article_upserts_by_provider_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = ReadLaterManager::new(&dir.path().join("rl.db")).unwrap();
        let mut first = article("id-1", "p1", 0);
        first.synced_to_device = true;
        first.document_id = Some("doc".into());
        mgr.save_article(&first).unwrap();

        // A provider re-fetch mints a fresh uuid for the same provider item.
        let mut refetched = article("id-2", "p1", 0);
        refetched.title = "Updated".into();
        let stored = mgr.upsert_article(&refetched).unwrap();
        assert_eq!(stored.id, "id-1");
        assert_eq!(mgr.articles.read().len(), 1);
        let a = mgr.get_article("id-1").unwrap();
        assert_eq!(a.title, "Updated");
        assert!(a.synced_to_device);
        assert_eq!(a.document_id.as_deref(), Some("doc"));
        assert!(mgr.get_article("id-2").is_none());
        let rows: i64 = mgr
            .db
            .query_row("SELECT COUNT(*) FROM readlater_articles", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);

        // Reloading from disk yields the same single article.
        drop(mgr);
        let mgr = ReadLaterManager::new(&dir.path().join("rl.db")).unwrap();
        assert_eq!(mgr.articles.read().len(), 1);
        assert_eq!(mgr.get_article("id-1").unwrap().title, "Updated");
    }

    #[test]
    fn select_articles_applies_max_articles_newest_first() {
        let articles: Vec<Article> = (0..10)
            .map(|i| article(&format!("id{i}"), &format!("p{i}"), i * 100))
            .collect();
        let mut settings = SyncSettings {
            max_articles: 3,
            ..SyncSettings::default()
        };
        let picked = select_articles_for_sync(articles.clone(), &settings);
        assert_eq!(
            picked
                .iter()
                .map(|a| a.provider_id.as_str())
                .collect::<Vec<_>>(),
            ["p9", "p8", "p7"]
        );
        settings.max_articles = 0;
        assert_eq!(
            select_articles_for_sync(articles.clone(), &settings).len(),
            10
        );
        // Filters apply before the cap.
        settings.max_articles = 2;
        settings.include_favorites_only = true;
        let mut favs = articles;
        favs[1].favorite = true;
        favs[2].favorite = true;
        favs[3].favorite = true;
        let picked = select_articles_for_sync(favs, &settings);
        assert_eq!(
            picked
                .iter()
                .map(|a| a.provider_id.as_str())
                .collect::<Vec<_>>(),
            ["p3", "p2"]
        );
    }

    #[test]
    fn select_articles_tag_filters_require_include_and_honor_exclude() {
        let tagged = |p: &str, tags: &[&str]| {
            let mut a = article(p, p, 0);
            a.tags = tags.iter().map(|t| t.to_string()).collect();
            a
        };
        let articles = vec![
            tagged("none", &[]),
            tagged("inc", &["Rust"]),
            tagged("both", &["rust", "skip"]),
            tagged("exc", &["skip"]),
            tagged("other", &["go"]),
        ];
        let filter = |tag: &str, include: bool| TagFilter {
            tag: tag.into(),
            include,
        };
        let ids = |settings: &SyncSettings| {
            let mut v: Vec<String> = select_articles_for_sync(articles.clone(), settings)
                .into_iter()
                .map(|a| a.provider_id)
                .collect();
            v.sort();
            v
        };
        let mut settings = SyncSettings {
            max_articles: 0,
            tag_filters: vec![filter("rust", true), filter("skip", false)],
            ..SyncSettings::default()
        };
        assert_eq!(ids(&settings), ["inc"]);
        settings.tag_filters = vec![filter("skip", false)];
        assert_eq!(ids(&settings), ["inc", "none", "other"]);
        settings.tag_filters = vec![filter("rust", true), filter("go", true)];
        assert_eq!(ids(&settings), ["both", "inc", "other"]);
    }

    #[test]
    fn credentials_persist_across_restart_but_not_in_config_json() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("rl.db");
        let mut mgr = ReadLaterManager::new(&db).unwrap();
        let mut wb = wallabag_config("https://wb.example", Some("AT"), Some("RT"), None);
        if let ProviderConfig::Wallabag {
            username, password, ..
        } = &mut wb
        {
            *username = Some("alice".into());
            *password = Some("PW".into());
        }
        mgr.add_account(test_account("wb", ReadLaterProvider::Wallabag, wb, None))
            .unwrap();
        let ip = ProviderConfig::Instapaper {
            oauth_token: Some("OT".into()),
            oauth_token_secret: Some("OTS".into()),
            username: Some("bob".into()),
        };
        mgr.add_account(test_account("ip", ReadLaterProvider::Instapaper, ip, None))
            .unwrap();

        // The client-facing/config JSON carries no secrets.
        let config_cols: Vec<String> = mgr
            .db
            .prepare("SELECT config FROM readlater_accounts")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        for secret in ["AT", "RT", "csecret", "PW", "OT", "OTS"] {
            for col in &config_cols {
                assert!(!col.contains(&format!("\"{secret}\"")), "{col}");
            }
            let json = serde_json::to_string(&mgr.list_accounts()).unwrap();
            assert!(!json.contains(&format!("\"{secret}\"")), "{json}");
        }

        // A refreshed token is persisted by update_account_config.
        let refreshed = wallabag_config("https://wb.example", Some("AT2"), Some("RT2"), None);
        let refreshed = {
            let mut c = refreshed;
            if let ProviderConfig::Wallabag {
                username, password, ..
            } = &mut c
            {
                *username = Some("alice".into());
                *password = Some("PW".into());
            }
            c
        };
        mgr.update_account_config("wb", &refreshed).unwrap();
        drop(mgr);

        let mgr = ReadLaterManager::new(&db).unwrap();
        let ProviderConfig::Wallabag {
            client_secret,
            access_token,
            refresh_token,
            username,
            password,
            ..
        } = mgr.get_account("wb").unwrap().config
        else {
            panic!("wrong variant")
        };
        assert_eq!(client_secret.as_deref(), Some("csecret"));
        assert_eq!(access_token.as_deref(), Some("AT2"));
        assert_eq!(refresh_token.as_deref(), Some("RT2"));
        assert_eq!(username.as_deref(), Some("alice"));
        assert_eq!(password.as_deref(), Some("PW"));
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            ..
        } = mgr.get_account("ip").unwrap().config
        else {
            panic!("wrong variant")
        };
        assert_eq!(oauth_token.as_deref(), Some("OT"));
        assert_eq!(oauth_token_secret.as_deref(), Some("OTS"));
    }

    /// A database from before the `secrets` column (and with Omnivore rows) still loads.
    #[tokio::test]
    async fn migrates_legacy_db_and_loads_omnivore_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("rl.db");
        {
            let db = Connection::open(&db_path).unwrap();
            db.execute_batch(
                r#"
                CREATE TABLE readlater_accounts (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, provider TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1, config TEXT NOT NULL, sync_settings TEXT NOT NULL,
                    last_sync TEXT, created_at TEXT NOT NULL
                );
                CREATE TABLE readlater_articles (
                    id TEXT PRIMARY KEY, provider TEXT NOT NULL, provider_id TEXT NOT NULL,
                    account_id TEXT, url TEXT NOT NULL, title TEXT NOT NULL, excerpt TEXT,
                    author TEXT, word_count INTEGER, reading_time_minutes INTEGER, tags TEXT,
                    status TEXT NOT NULL, favorite INTEGER DEFAULT 0, added_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, read_at TEXT, image_url TEXT, document_id TEXT,
                    synced_to_device INTEGER DEFAULT 0, last_sync TEXT,
                    UNIQUE(provider, provider_id)
                );
                "#,
            )
            .unwrap();
            let settings = serde_json::to_string(&SyncSettings::default()).unwrap();
            let now = Utc::now().to_rfc3339();
            db.execute(
                "INSERT INTO readlater_accounts (id, name, provider, enabled, config, sync_settings, created_at) VALUES ('om', 'Omni', 'omnivore', 1, ?1, ?2, ?3)",
                params![r#"{"type":"omnivore","api_url":null}"#, settings, now],
            )
            .unwrap();
            // Very old rows could still carry a token inside the config JSON.
            db.execute(
                "INSERT INTO readlater_accounts (id, name, provider, enabled, config, sync_settings, created_at) VALUES ('pk', 'Pocket', 'pocket', 1, ?1, ?2, ?3)",
                params![
                    r#"{"type":"pocket","consumer_key":"ck","access_token":"legacy","username":null}"#,
                    settings,
                    now
                ],
            )
            .unwrap();
            db.execute(
                "INSERT INTO readlater_articles (id, provider, provider_id, url, title, status, added_at, updated_at) VALUES ('a1', 'omnivore', 'x', 'https://ex.com', 'T', 'unread', ?1, ?1)",
                params![now],
            )
            .unwrap();
        }

        let mut mgr = ReadLaterManager::new(&db_path).unwrap();
        let om = mgr.get_account("om").unwrap();
        assert_eq!(om.provider, ReadLaterProvider::Omnivore);
        assert!(!om.config.is_authenticated());
        assert_eq!(
            mgr.get_article("a1").unwrap().provider,
            ReadLaterProvider::Omnivore
        );
        let ProviderConfig::Pocket { access_token, .. } = mgr.get_account("pk").unwrap().config
        else {
            panic!("wrong variant")
        };
        assert_eq!(access_token.as_deref(), Some("legacy"));

        let err = provider_for(ReadLaterProvider::Omnivore).err().unwrap();
        assert!(matches!(err, ReadLaterError::Discontinued(_)));
        // New Omnivore accounts are rejected; the old one can still be deleted.
        let err = mgr
            .add_account(test_account(
                "om2",
                ReadLaterProvider::Omnivore,
                ProviderConfig::Omnivore {},
                None,
            ))
            .unwrap_err();
        assert!(matches!(err, ReadLaterError::Discontinued(_)));
        mgr.delete_account("om").unwrap();

        // The migration added the column and is idempotent; the legacy token was kept.
        mgr.update_account(mgr.get_account("pk").unwrap()).unwrap();
        drop(mgr);
        let mgr = ReadLaterManager::new(&db_path).unwrap();
        let secrets: String = mgr
            .db
            .query_row(
                "SELECT secrets FROM readlater_accounts WHERE id='pk'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(secrets.contains("legacy"));
        assert!(mgr.get_account("om").is_none());
    }

    #[derive(Default)]
    struct MockWallabag {
        token_requests: std::sync::Mutex<Vec<HashMap<String, String>>>,
        /// Access token the API accepts.
        valid_token: String,
        /// Access token the token endpoint hands out.
        issued_token: String,
    }

    async fn mock_wallabag(valid_token: &str, issued_token: &str) -> (String, Arc<MockWallabag>) {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::routing::{get, post};

        let state = Arc::new(MockWallabag {
            valid_token: valid_token.into(),
            issued_token: issued_token.into(),
            ..Default::default()
        });
        let app = axum::Router::new()
            .route(
                "/oauth/v2/token",
                post(
                    |State(s): State<Arc<MockWallabag>>,
                     axum::Form(form): axum::Form<HashMap<String, String>>| async move {
                        s.token_requests.lock().unwrap().push(form);
                        axum::Json(serde_json::json!({
                            "access_token": s.issued_token,
                            "refresh_token": "rotated-refresh",
                            "expires_in": 3600,
                            "token_type": "bearer",
                        }))
                    },
                ),
            )
            .route(
                "/api/entries.json",
                get(
                    |State(s): State<Arc<MockWallabag>>, headers: HeaderMap| async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        if auth != format!("Bearer {}", s.valid_token) {
                            return Err(StatusCode::UNAUTHORIZED);
                        }
                        Ok(axum::Json(serde_json::json!({"_embedded": {"items": [{
                            "id": 7, "url": "https://ex.com/7", "title": "Seven",
                            "content": "<p>7</p>", "reading_time": 1, "is_archived": 0,
                            "is_starred": 0, "tags": [],
                            "created_at": "2025-01-01T00:00:00+00:00",
                            "updated_at": "2025-01-01T00:00:00+00:00",
                            "preview_picture": null
                        }]}})))
                    },
                ),
            )
            .with_state(Arc::clone(&state));
        (spawn_server(app).await, state)
    }

    /// No refresh token: the password grant is used, and a retry that still gets 401 is not
    /// retried again.
    #[tokio::test]
    async fn wallabag_401_uses_password_grant_and_retries_only_once() {
        let (base, mock) = mock_wallabag("fresh", "fresh").await;
        let provider = WallabagProvider::new();
        let mut config = wallabag_config(&base, Some("revoked"), None, None);
        // Without refresh credentials a token with no expiry is used as-is; the 401 is final.
        let err = provider.fetch_articles(&config, None).await.unwrap_err();
        assert!(err.to_string().contains("401"), "{err}");
        assert!(mock.token_requests.lock().unwrap().is_empty());
        assert!(provider.take_refreshed_config().is_none());

        if let ProviderConfig::Wallabag {
            username,
            password,
            token_expires_at,
            ..
        } = &mut config
        {
            *username = Some("alice".into());
            *password = Some("pw".into());
            *token_expires_at = Some(Utc::now() + Duration::hours(1));
        }
        let articles = provider.fetch_articles(&config, None).await.unwrap();
        assert_eq!(articles.len(), 1);
        {
            let reqs = mock.token_requests.lock().unwrap();
            assert_eq!(reqs.len(), 1);
            assert_eq!(reqs[0]["grant_type"], "password");
            assert_eq!(reqs[0]["username"], "alice");
            assert_eq!(reqs[0]["password"], "pw");
        }
        let Some(ProviderConfig::Wallabag { access_token, .. }) = provider.take_refreshed_config()
        else {
            panic!("expected a refreshed config")
        };
        assert_eq!(access_token.as_deref(), Some("fresh"));

        // The server rejects even the new token: exactly one refresh, then the 401 surfaces.
        let (base, mock) = mock_wallabag("never", "also-bad").await;
        let config = wallabag_config(
            &base,
            Some("revoked"),
            Some("r"),
            Some(Utc::now() + Duration::hours(1)),
        );
        let err = provider.fetch_articles(&config, None).await.unwrap_err();
        assert!(err.to_string().contains("401"), "{err}");
        assert_eq!(mock.token_requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn parse_form_urlencoded_decodes() {
        assert_eq!(
            parse_form_urlencoded("oauth_token=a%2Bb&oauth_token_secret=c+d&flag\n"),
            vec![
                ("oauth_token".to_string(), "a+b".to_string()),
                ("oauth_token_secret".to_string(), "c d".to_string()),
                ("flag".to_string(), String::new()),
            ]
        );
    }

    #[tokio::test]
    async fn instapaper_xauth_signs_request_and_parses_tokens() {
        use axum::http::HeaderMap;
        use axum::routing::post;

        type Captured = Arc<std::sync::Mutex<Option<(String, String)>>>;
        let captured: Captured = Default::default();
        let app = axum::Router::new()
            .route(
                "/oauth/access_token",
                post(
                    |axum::extract::State(c): axum::extract::State<Captured>,
                     headers: HeaderMap,
                     body: String| async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        *c.lock().unwrap() = Some((auth, body));
                        "oauth_token=tok%2B1&oauth_token_secret=sec+ret"
                    },
                ),
            )
            .with_state(Arc::clone(&captured));
        let base = spawn_server(app).await;

        let provider =
            InstapaperProvider::new("ckey".into(), "csecret".into()).with_api_base(&base);
        let state = provider.start_oauth("http://cb").await.unwrap();
        let callback = OAuthCallback {
            code: None,
            oauth_token: None,
            oauth_verifier: None,
            state: None,
            username: Some("u@example.com".into()),
            password: Some("p&w =".into()),
        };
        let config = provider.complete_oauth(&callback, &state).await.unwrap();
        let ProviderConfig::Instapaper {
            oauth_token,
            oauth_token_secret,
            username,
        } = &config
        else {
            panic!("wrong variant")
        };
        assert_eq!(oauth_token.as_deref(), Some("tok+1"));
        assert_eq!(oauth_token_secret.as_deref(), Some("sec ret"));
        assert_eq!(username.as_deref(), Some("u@example.com"));

        // Verify the request the server saw: xAuth body params and a valid HMAC-SHA1 signature
        // over the oauth params plus the body (no oauth_token, empty token secret).
        let (auth, body) = captured.lock().unwrap().clone().unwrap();
        let body_params = parse_form_urlencoded(&body);
        let get = |k: &str| {
            body_params
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("x_auth_username"), Some("u@example.com"));
        assert_eq!(get("x_auth_password"), Some("p&w ="));
        assert_eq!(get("x_auth_mode"), Some("client_auth"));
        let header = auth.strip_prefix("OAuth ").expect("OAuth header");
        let mut oauth: Vec<(String, String)> = header
            .split(", ")
            .map(|kv| {
                let (k, v) = kv.split_once('=').unwrap();
                let v = urlencoding::decode(v.trim_matches('"'))
                    .unwrap()
                    .into_owned();
                (k.to_string(), v)
            })
            .collect();
        assert!(oauth.iter().all(|(k, _)| k != "oauth_token"));
        assert!(oauth.contains(&("oauth_consumer_key".into(), "ckey".into())));
        let sig_pos = oauth
            .iter()
            .position(|(k, _)| k == "oauth_signature")
            .unwrap();
        let (_, signature) = oauth.remove(sig_pos);
        let all: Vec<(&str, &str)> = oauth
            .iter()
            .chain(body_params.iter())
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let url = format!("{}/oauth/access_token", base);
        assert_eq!(
            oauth1_signature("POST", &url, &all, "csecret", None),
            signature
        );

        // The obtained tokens persist with the account.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("rl.db");
        let mut mgr = ReadLaterManager::new(&db).unwrap();
        mgr.add_account(test_account(
            "ip",
            ReadLaterProvider::Instapaper,
            config,
            None,
        ))
        .unwrap();
        drop(mgr);
        let mgr = ReadLaterManager::new(&db).unwrap();
        assert!(mgr.get_account("ip").unwrap().config.is_authenticated());

        // Missing username / consumer credentials are clear errors, not requests.
        let no_user = OAuthCallback {
            username: None,
            ..callback.clone()
        };
        assert!(provider.complete_oauth(&no_user, &state).await.is_err());
        let unconfigured =
            InstapaperProvider::new(String::new(), String::new()).with_api_base(&base);
        let err = unconfigured
            .complete_oauth(&callback, &state)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("INSTAPAPER_CONSUMER_KEY"), "{err}");
    }
}
