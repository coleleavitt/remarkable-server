//! Read-it-later integrations module
//! Supports Pocket, Instapaper, Wallabag, and Omnivore

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;

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
}

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
    },
    Omnivore {
        #[serde(skip_serializing)]
        api_key: Option<String>,
        api_url: Option<String>,
    },
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
}

// ============================================================================
// Sync Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub provider: ReadLaterProvider,
    pub articles_fetched: u32,
    pub articles_synced: u32,
    pub articles_converted: u32,
    pub read_status_synced: u32,
    pub errors: Vec<String>,
    pub duration_ms: u64,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum SyncCommand {
    SyncAll,
    SyncProvider(String),
    SyncArticle(String),
    ConvertArticle(String, ArticleFormat),
    SyncReadStatus(String),
    Stop,
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
    async fn complete_oauth(&self, callback: &OAuthCallback, state: &OAuthState) -> Result<ProviderConfig>;
    async fn refresh_auth(&self, config: &ProviderConfig) -> Result<ProviderConfig>;
    fn is_authenticated(&self, config: &ProviderConfig) -> bool;
    
    async fn fetch_articles(&self, config: &ProviderConfig, since: Option<DateTime<Utc>>) -> Result<Vec<Article>>;
    async fn fetch_article_content(&self, config: &ProviderConfig, article: &Article) -> Result<ArticleContent>;
    async fn update_read_status(&self, config: &ProviderConfig, provider_id: &str, status: ReadStatus) -> Result<()>;
    async fn add_article(&self, config: &ProviderConfig, url: &str, tags: &[String]) -> Result<Article>;
    async fn delete_article(&self, config: &ProviderConfig, provider_id: &str) -> Result<()>;
}

// ============================================================================
// Pocket Provider
// ============================================================================

pub struct PocketProvider {
    client: reqwest::Client,
}

impl PocketProvider {
    const API_BASE: &'static str = "https://getpocket.com/v3";
    const AUTH_URL: &'static str = "https://getpocket.com/auth/authorize";
    
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
        let resp = self.client
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
            return Err(ReadLaterError::OAuth(format!("Failed to get request token: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct TokenResponse { code: String }
        
        let token_resp: TokenResponse = resp.json().await
            .map_err(|e| ReadLaterError::OAuth(e.to_string()))?;
        
        Ok(OAuthState {
            provider: ReadLaterProvider::Pocket,
            request_token: Some(token_resp.code),
            redirect_uri: redirect_uri.to_string(),
            created_at: Utc::now(),
        })
    }
    
    async fn complete_oauth(&self, _callback: &OAuthCallback, state: &OAuthState) -> Result<ProviderConfig> {
        let request_token = state.request_token.as_ref()
            .ok_or_else(|| ReadLaterError::OAuth("Missing request token".into()))?;
        
        // This would need the consumer_key from the original config
        let consumer_key = std::env::var("POCKET_CONSUMER_KEY")
            .map_err(|_| ReadLaterError::OAuth("POCKET_CONSUMER_KEY not set".into()))?;
        
        let resp = self.client
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
            return Err(ReadLaterError::OAuth(format!("OAuth failed: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct AuthResponse { access_token: String, username: String }
        
        let auth: AuthResponse = resp.json().await
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
    
    async fn fetch_articles(&self, config: &ProviderConfig, since: Option<DateTime<Utc>>) -> Result<Vec<Article>> {
        let ProviderConfig::Pocket { consumer_key, access_token, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config for Pocket".into()));
        };
        
        let access_token = access_token.as_ref()
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
        
        let resp = self.client
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
            return Err(ReadLaterError::Api(format!("Pocket API error: {}", resp.status())));
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
        struct PocketAuthor { name: Option<String> }
        
        #[derive(Deserialize)]
        struct PocketImage { src: Option<String> }
        
        let data: PocketResponse = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        let articles = data.list.into_values().map(|item| {
            let status = match item.status.as_str() {
                "0" => ReadStatus::Unread,
                "1" => ReadStatus::Archived,
                "2" => ReadStatus::Read,
                _ => ReadStatus::Unread,
            };
            
            let word_count = item.word_count.as_ref()
                .and_then(|w| w.parse().ok());
            
            let reading_time = word_count.map(|w: u32| (w / 200).max(1));
            
            let tags: Vec<String> = item.tags
                .map(|t| t.keys().cloned().collect())
                .unwrap_or_default();
            
            let author = item.authors
                .and_then(|a| a.values().next().and_then(|x| x.name.clone()));
            
            let added_ts = item.time_added.parse::<i64>().unwrap_or(0);
            let updated_ts = item.time_updated.parse::<i64>().unwrap_or(0);
            let read_ts = item.time_read.as_ref()
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
        }).collect();
        
        Ok(articles)
    }
    
    async fn fetch_article_content(&self, config: &ProviderConfig, article: &Article) -> Result<ArticleContent> {
        // Pocket doesn't provide article content directly via API
        // Use Mercury Parser or similar service
        let html = format!(
            "<html><head><title>{}</title></head><body><h1>{}</h1><p>{}</p><p><a href=\"{}\">Read original</a></p></body></html>",
            article.title,
            article.title,
            article.excerpt.as_deref().unwrap_or(""),
            article.url
        );
        
        Ok(ArticleContent {
            html,
            images: Vec::new(),
            styles: None,
        })
    }
    
    async fn update_read_status(&self, config: &ProviderConfig, provider_id: &str, status: ReadStatus) -> Result<()> {
        let ProviderConfig::Pocket { consumer_key, access_token, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let access_token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;
        
        let action = match status {
            ReadStatus::Archived => "archive",
            ReadStatus::Read => "archive",
            ReadStatus::Unread => "readd",
            ReadStatus::InProgress => return Ok(()),
        };
        
        let resp = self.client
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
            return Err(ReadLaterError::Api(format!("Failed to update status: {}", resp.status())));
        }
        
        Ok(())
    }
    
    async fn add_article(&self, config: &ProviderConfig, url: &str, tags: &[String]) -> Result<Article> {
        let ProviderConfig::Pocket { consumer_key, access_token, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let access_token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;
        
        let resp = self.client
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
            return Err(ReadLaterError::Api(format!("Failed to add article: {}", resp.status())));
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
        
        let data: AddResponse = resp.json().await
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
        let ProviderConfig::Pocket { consumer_key, access_token, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let access_token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Pocket".into()))?;
        
        let resp = self.client
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
            return Err(ReadLaterError::Api(format!("Failed to delete: {}", resp.status())));
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
}

impl InstapaperProvider {
    const API_BASE: &'static str = "https://www.instapaper.com/api/1";
    
    pub fn new(consumer_key: String, consumer_secret: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            consumer_key,
            consumer_secret,
        }
    }
    
    fn oauth_signature(&self, method: &str, url: &str, params: &[(&str, &str)], token_secret: Option<&str>) -> String {
        use sha2::{Sha256, Digest};
        use base64::Engine;
        
        let mut sorted_params: Vec<_> = params.to_vec();
        sorted_params.sort_by(|a, b| a.0.cmp(&b.0));
        
        let param_string: String = sorted_params.iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        
        let base_string = format!(
            "{}&{}&{}",
            method.to_uppercase(),
            urlencoding::encode(url),
            urlencoding::encode(&param_string)
        );
        
        let signing_key = format!(
            "{}&{}",
            urlencoding::encode(&self.consumer_secret),
            token_secret.map(urlencoding::encode).unwrap_or_default()
        );
        
        let mut hasher = Sha256::new();
        hasher.update(format!("{}{}", signing_key, base_string));
        base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
    }
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
    
    async fn complete_oauth(&self, callback: &OAuthCallback, _state: &OAuthState) -> Result<ProviderConfig> {
        // For Instapaper, callback contains username/password via xAuth
        // This is a simplified implementation
        Err(ReadLaterError::OAuth("Instapaper requires xAuth with username/password".into()))
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
    
    async fn fetch_articles(&self, config: &ProviderConfig, _since: Option<DateTime<Utc>>) -> Result<Vec<Article>> {
        let ProviderConfig::Instapaper { oauth_token, oauth_token_secret, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let oauth_token = oauth_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();
        
        let url = format!("{}/bookmarks/list", Self::API_BASE);
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = uuid::Uuid::new_v4().to_string();
        
        let params = vec![
            ("oauth_consumer_key", self.consumer_key.as_str()),
            ("oauth_token", oauth_token.as_str()),
            ("oauth_signature_method", "HMAC-SHA256"),
            ("oauth_timestamp", &timestamp),
            ("oauth_nonce", &nonce),
            ("oauth_version", "1.0"),
            ("limit", "500"),
        ];
        
        let signature = self.oauth_signature("POST", &url, &params, token_secret);
        
        let resp = self.client
            .post(&url)
            .header("Authorization", format!(
                "OAuth oauth_consumer_key=\"{}\", oauth_token=\"{}\", oauth_signature_method=\"HMAC-SHA256\", oauth_signature=\"{}\", oauth_timestamp=\"{}\", oauth_nonce=\"{}\", oauth_version=\"1.0\"",
                self.consumer_key, oauth_token, urlencoding::encode(&signature), timestamp, nonce
            ))
            .form(&[("limit", "500")])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Instapaper API error: {}", resp.status())));
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
            Meta { #[serde(rename = "type")] item_type: String },
        }
        
        let items: Vec<InstapaperItem> = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        let articles = items.into_iter()
            .filter_map(|item| {
                match item {
                    InstapaperItem::Bookmark { bookmark_id, url, title, description, time, progress, starred } => {
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
                            read_at: if status == ReadStatus::Read { Some(Utc::now()) } else { None },
                            content: None,
                            image_url: None,
                            document_id: None,
                            synced_to_device: false,
                            last_sync: None,
                        })
                    }
                    InstapaperItem::Meta { .. } => None,
                }
            })
            .collect();
        
        Ok(articles)
    }
    
    async fn fetch_article_content(&self, config: &ProviderConfig, article: &Article) -> Result<ArticleContent> {
        let ProviderConfig::Instapaper { oauth_token, oauth_token_secret, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let oauth_token = oauth_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();
        
        let url = format!("{}/bookmarks/get_text", Self::API_BASE);
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = uuid::Uuid::new_v4().to_string();
        
        let params = vec![
            ("oauth_consumer_key", self.consumer_key.as_str()),
            ("oauth_token", oauth_token.as_str()),
            ("oauth_signature_method", "HMAC-SHA256"),
            ("oauth_timestamp", &timestamp),
            ("oauth_nonce", &nonce),
            ("oauth_version", "1.0"),
            ("bookmark_id", &article.provider_id),
        ];
        
        let signature = self.oauth_signature("POST", &url, &params, token_secret);
        
        let resp = self.client
            .post(&url)
            .header("Authorization", format!(
                "OAuth oauth_consumer_key=\"{}\", oauth_token=\"{}\", oauth_signature_method=\"HMAC-SHA256\", oauth_signature=\"{}\", oauth_timestamp=\"{}\", oauth_nonce=\"{}\", oauth_version=\"1.0\"",
                self.consumer_key, oauth_token, urlencoding::encode(&signature), timestamp, nonce
            ))
            .form(&[("bookmark_id", article.provider_id.as_str())])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to get content: {}", resp.status())));
        }
        
        let html = resp.text().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        Ok(ArticleContent {
            html,
            images: Vec::new(),
            styles: None,
        })
    }
    
    async fn update_read_status(&self, config: &ProviderConfig, provider_id: &str, status: ReadStatus) -> Result<()> {
        let ProviderConfig::Instapaper { oauth_token, oauth_token_secret, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let oauth_token = oauth_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();
        
        let endpoint = match status {
            ReadStatus::Archived => "bookmarks/archive",
            ReadStatus::Unread => "bookmarks/unarchive",
            ReadStatus::Read | ReadStatus::InProgress => return Ok(()),
        };
        
        let url = format!("{}/{}", Self::API_BASE, endpoint);
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = uuid::Uuid::new_v4().to_string();
        
        let params = vec![
            ("oauth_consumer_key", self.consumer_key.as_str()),
            ("oauth_token", oauth_token.as_str()),
            ("oauth_signature_method", "HMAC-SHA256"),
            ("oauth_timestamp", &timestamp),
            ("oauth_nonce", &nonce),
            ("oauth_version", "1.0"),
            ("bookmark_id", provider_id),
        ];
        
        let signature = self.oauth_signature("POST", &url, &params, token_secret);
        
        let resp = self.client
            .post(&url)
            .header("Authorization", format!(
                "OAuth oauth_consumer_key=\"{}\", oauth_token=\"{}\", oauth_signature_method=\"HMAC-SHA256\", oauth_signature=\"{}\", oauth_timestamp=\"{}\", oauth_nonce=\"{}\", oauth_version=\"1.0\"",
                self.consumer_key, oauth_token, urlencoding::encode(&signature), timestamp, nonce
            ))
            .form(&[("bookmark_id", provider_id)])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to update status: {}", resp.status())));
        }
        
        Ok(())
    }
    
    async fn add_article(&self, config: &ProviderConfig, url: &str, _tags: &[String]) -> Result<Article> {
        let ProviderConfig::Instapaper { oauth_token, oauth_token_secret, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let oauth_token = oauth_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();
        
        let api_url = format!("{}/bookmarks/add", Self::API_BASE);
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = uuid::Uuid::new_v4().to_string();
        
        let params = vec![
            ("oauth_consumer_key", self.consumer_key.as_str()),
            ("oauth_token", oauth_token.as_str()),
            ("oauth_signature_method", "HMAC-SHA256"),
            ("oauth_timestamp", &timestamp),
            ("oauth_nonce", &nonce),
            ("oauth_version", "1.0"),
            ("url", url),
        ];
        
        let signature = self.oauth_signature("POST", &api_url, &params, token_secret);
        
        let resp = self.client
            .post(&api_url)
            .header("Authorization", format!(
                "OAuth oauth_consumer_key=\"{}\", oauth_token=\"{}\", oauth_signature_method=\"HMAC-SHA256\", oauth_signature=\"{}\", oauth_timestamp=\"{}\", oauth_nonce=\"{}\", oauth_version=\"1.0\"",
                self.consumer_key, oauth_token, urlencoding::encode(&signature), timestamp, nonce
            ))
            .form(&[("url", url)])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to add article: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct Bookmark {
            bookmark_id: i64,
            url: String,
            title: String,
        }
        
        let items: Vec<Bookmark> = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        let bookmark = items.into_iter().next()
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
        let ProviderConfig::Instapaper { oauth_token, oauth_token_secret, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let oauth_token = oauth_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Instapaper".into()))?;
        let token_secret = oauth_token_secret.as_deref();
        
        let url = format!("{}/bookmarks/delete", Self::API_BASE);
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = uuid::Uuid::new_v4().to_string();
        
        let params = vec![
            ("oauth_consumer_key", self.consumer_key.as_str()),
            ("oauth_token", oauth_token.as_str()),
            ("oauth_signature_method", "HMAC-SHA256"),
            ("oauth_timestamp", &timestamp),
            ("oauth_nonce", &nonce),
            ("oauth_version", "1.0"),
            ("bookmark_id", provider_id),
        ];
        
        let signature = self.oauth_signature("POST", &url, &params, token_secret);
        
        let resp = self.client
            .post(&url)
            .header("Authorization", format!(
                "OAuth oauth_consumer_key=\"{}\", oauth_token=\"{}\", oauth_signature_method=\"HMAC-SHA256\", oauth_signature=\"{}\", oauth_timestamp=\"{}\", oauth_nonce=\"{}\", oauth_version=\"1.0\"",
                self.consumer_key, oauth_token, urlencoding::encode(&signature), timestamp, nonce
            ))
            .form(&[("bookmark_id", provider_id)])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to delete: {}", resp.status())));
        }
        
        Ok(())
    }
}


// ============================================================================
// Wallabag Provider
// ============================================================================

pub struct WallabagProvider {
    client: reqwest::Client,
}

impl WallabagProvider {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
    
    async fn ensure_token(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        let ProviderConfig::Wallabag {
            instance_url,
            client_id,
            client_secret,
            access_token,
            refresh_token,
            token_expires_at,
        } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        // Check if token needs refresh
        if let Some(expires) = token_expires_at {
            if *expires > Utc::now() + Duration::minutes(5) {
                return Ok(config.clone());
            }
        }
        
        let refresh = refresh_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        
        let client_secret = client_secret.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag client secret".into()))?;
        
        let resp = self.client
            .post(format!("{}/oauth/v2/token", instance_url))
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh),
                ("client_id", client_id),
                ("client_secret", client_secret),
            ])
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::OAuth(format!("Token refresh failed: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
            refresh_token: String,
            expires_in: i64,
        }
        
        let token: TokenResponse = resp.json().await
            .map_err(|e| ReadLaterError::OAuth(e.to_string()))?;
        
        Ok(ProviderConfig::Wallabag {
            instance_url: instance_url.clone(),
            client_id: client_id.clone(),
            client_secret: Some(client_secret.clone()),
            access_token: Some(token.access_token),
            refresh_token: Some(token.refresh_token),
            token_expires_at: Some(Utc::now() + Duration::seconds(token.expires_in)),
        })
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
    
    async fn complete_oauth(&self, callback: &OAuthCallback, state: &OAuthState) -> Result<ProviderConfig> {
        let code = callback.code.as_ref()
            .ok_or_else(|| ReadLaterError::OAuth("Missing authorization code".into()))?;
        
        // Would need instance_url, client_id, client_secret from somewhere
        Err(ReadLaterError::OAuth("Wallabag OAuth requires instance configuration".into()))
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
    
    async fn fetch_articles(&self, config: &ProviderConfig, since: Option<DateTime<Utc>>) -> Result<Vec<Article>> {
        let config = self.ensure_token(config).await?;
        
        let ProviderConfig::Wallabag { instance_url, access_token, .. } = &config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        
        let mut url = format!("{}/api/entries.json?perPage=100&sort=created&order=desc", instance_url);
        
        if let Some(ts) = since {
            url.push_str(&format!("&since={}", ts.timestamp()));
        }
        
        let resp = self.client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Wallabag API error: {}", resp.status())));
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
        
        let data: WallabagResponse = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        let articles = data._embedded.items.into_iter().map(|entry| {
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
                read_at: if status == ReadStatus::Archived { Some(updated) } else { None },
                content: entry.content,
                image_url: entry.preview_picture,
                document_id: None,
                synced_to_device: false,
                last_sync: None,
            }
        }).collect();
        
        Ok(articles)
    }
    
    async fn fetch_article_content(&self, config: &ProviderConfig, article: &Article) -> Result<ArticleContent> {
        let config = self.ensure_token(config).await?;
        
        let ProviderConfig::Wallabag { instance_url, access_token, .. } = &config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        
        let resp = self.client
            .get(format!("{}/api/entries/{}.json", instance_url, article.provider_id))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to get content: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct Entry {
            content: Option<String>,
        }
        
        let entry: Entry = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        let html = entry.content.unwrap_or_else(|| {
            format!("<html><body><h1>{}</h1><p><a href=\"{}\">Read original</a></p></body></html>",
                article.title, article.url)
        });
        
        Ok(ArticleContent {
            html,
            images: Vec::new(),
            styles: None,
        })
    }
    
    async fn update_read_status(&self, config: &ProviderConfig, provider_id: &str, status: ReadStatus) -> Result<()> {
        let config = self.ensure_token(config).await?;
        
        let ProviderConfig::Wallabag { instance_url, access_token, .. } = &config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        
        let archive = match status {
            ReadStatus::Archived | ReadStatus::Read => 1,
            ReadStatus::Unread | ReadStatus::InProgress => 0,
        };
        
        let resp = self.client
            .patch(format!("{}/api/entries/{}.json", instance_url, provider_id))
            .header("Authorization", format!("Bearer {}", token))
            .json(&serde_json::json!({ "archive": archive }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to update: {}", resp.status())));
        }
        
        Ok(())
    }
    
    async fn add_article(&self, config: &ProviderConfig, url: &str, tags: &[String]) -> Result<Article> {
        let config = self.ensure_token(config).await?;
        
        let ProviderConfig::Wallabag { instance_url, access_token, .. } = &config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        
        let resp = self.client
            .post(format!("{}/api/entries.json", instance_url))
            .header("Authorization", format!("Bearer {}", token))
            .json(&serde_json::json!({
                "url": url,
                "tags": tags.join(","),
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to add: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct Entry {
            id: i64,
            url: String,
            title: String,
            reading_time: Option<u32>,
        }
        
        let entry: Entry = resp.json().await
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
        let config = self.ensure_token(config).await?;
        
        let ProviderConfig::Wallabag { instance_url, access_token, .. } = &config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let token = access_token.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Wallabag".into()))?;
        
        let resp = self.client
            .delete(format!("{}/api/entries/{}.json", instance_url, provider_id))
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to delete: {}", resp.status())));
        }
        
        Ok(())
    }
}


// ============================================================================
// Omnivore Provider (GraphQL API)
// ============================================================================

pub struct OmnivoreProvider {
    client: reqwest::Client,
}

impl OmnivoreProvider {
    const DEFAULT_API_URL: &'static str = "https://api-prod.omnivore.app/api/graphql";
    
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
    
    fn api_url(config: &ProviderConfig) -> &str {
        match config {
            ProviderConfig::Omnivore { api_url, .. } => {
                api_url.as_deref().unwrap_or(Self::DEFAULT_API_URL)
            }
            _ => Self::DEFAULT_API_URL,
        }
    }
}

#[async_trait::async_trait]
impl ReadLaterProviderTrait for OmnivoreProvider {
    fn provider_type(&self) -> ReadLaterProvider {
        ReadLaterProvider::Omnivore
    }
    
    async fn start_oauth(&self, redirect_uri: &str) -> Result<OAuthState> {
        // Omnivore uses API keys, not OAuth
        Ok(OAuthState {
            provider: ReadLaterProvider::Omnivore,
            request_token: None,
            redirect_uri: redirect_uri.to_string(),
            created_at: Utc::now(),
        })
    }
    
    async fn complete_oauth(&self, callback: &OAuthCallback, _state: &OAuthState) -> Result<ProviderConfig> {
        // API key is provided directly
        let api_key = callback.code.clone()
            .ok_or_else(|| ReadLaterError::OAuth("API key required".into()))?;
        
        Ok(ProviderConfig::Omnivore {
            api_key: Some(api_key),
            api_url: None,
        })
    }
    
    async fn refresh_auth(&self, config: &ProviderConfig) -> Result<ProviderConfig> {
        // API keys don't expire
        Ok(config.clone())
    }
    
    fn is_authenticated(&self, config: &ProviderConfig) -> bool {
        match config {
            ProviderConfig::Omnivore { api_key, .. } => api_key.is_some(),
            _ => false,
        }
    }
    
    async fn fetch_articles(&self, config: &ProviderConfig, since: Option<DateTime<Utc>>) -> Result<Vec<Article>> {
        let ProviderConfig::Omnivore { api_key, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let api_key = api_key.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Omnivore".into()))?;
        
        let query = r#"
            query Search($after: String, $first: Int, $query: String) {
                search(after: $after, first: $first, query: $query) {
                    ... on SearchSuccess {
                        edges {
                            node {
                                id
                                slug
                                url
                                title
                                description
                                author
                                readingProgressPercent
                                isArchived
                                labels {
                                    name
                                }
                                savedAt
                                updatedAt
                                readAt
                                wordsCount
                                image
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                    ... on SearchError {
                        errorCodes
                    }
                }
            }
        "#;
        
        let search_query = since
            .map(|ts| format!("saved:{}", ts.format("%Y-%m-%d")))
            .unwrap_or_default();
        
        let resp = self.client
            .post(Self::api_url(config))
            .header("Authorization", api_key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "query": query,
                "variables": {
                    "first": 100,
                    "query": search_query,
                }
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Omnivore API error: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct GraphQLResponse {
            data: Option<DataWrapper>,
            errors: Option<Vec<serde_json::Value>>,
        }
        
        #[derive(Deserialize)]
        struct DataWrapper {
            search: SearchResult,
        }
        
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum SearchResult {
            Success { edges: Vec<Edge> },
            Error { #[serde(rename = "errorCodes")] error_codes: Vec<String> },
        }
        
        #[derive(Deserialize)]
        struct Edge {
            node: OmnivoreArticle,
        }
        
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OmnivoreArticle {
            id: String,
            slug: String,
            url: String,
            title: String,
            description: Option<String>,
            author: Option<String>,
            reading_progress_percent: f64,
            is_archived: bool,
            labels: Vec<Label>,
            saved_at: String,
            updated_at: String,
            read_at: Option<String>,
            words_count: Option<u32>,
            image: Option<String>,
        }
        
        #[derive(Deserialize)]
        struct Label {
            name: String,
        }
        
        let gql: GraphQLResponse = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        if let Some(errors) = gql.errors {
            return Err(ReadLaterError::Api(format!("GraphQL errors: {:?}", errors)));
        }
        
        let data = gql.data.ok_or_else(|| ReadLaterError::Api("No data".into()))?;
        
        let edges = match data.search {
            SearchResult::Success { edges } => edges,
            SearchResult::Error { error_codes } => {
                return Err(ReadLaterError::Api(format!("Search error: {:?}", error_codes)));
            }
        };
        
        let articles = edges.into_iter().map(|edge| {
            let item = edge.node;
            
            let status = if item.is_archived {
                ReadStatus::Archived
            } else if item.reading_progress_percent >= 100.0 {
                ReadStatus::Read
            } else if item.reading_progress_percent > 0.0 {
                ReadStatus::InProgress
            } else {
                ReadStatus::Unread
            };
            
            let tags: Vec<String> = item.labels.into_iter().map(|l| l.name).collect();
            
            let saved_at = DateTime::parse_from_rfc3339(&item.saved_at)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            
            let updated_at = DateTime::parse_from_rfc3339(&item.updated_at)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            
            let read_at = item.read_at.and_then(|r| {
                DateTime::parse_from_rfc3339(&r)
                    .map(|d| d.with_timezone(&Utc))
                    .ok()
            });
            
            let reading_time = item.words_count.map(|w| (w / 200).max(1));
            
            Article {
                id: uuid::Uuid::new_v4().to_string(),
                provider: ReadLaterProvider::Omnivore,
                provider_id: item.id,
                url: item.url,
                title: item.title,
                excerpt: item.description,
                author: item.author,
                word_count: item.words_count,
                reading_time_minutes: reading_time,
                tags,
                status,
                favorite: false,
                added_at: saved_at,
                updated_at,
                read_at,
                content: None,
                image_url: item.image,
                document_id: None,
                synced_to_device: false,
                last_sync: None,
            }
        }).collect();
        
        Ok(articles)
    }
    
    async fn fetch_article_content(&self, config: &ProviderConfig, article: &Article) -> Result<ArticleContent> {
        let ProviderConfig::Omnivore { api_key, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let api_key = api_key.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Omnivore".into()))?;
        
        let query = r#"
            query GetArticle($username: String!, $slug: String!) {
                article(username: $username, slug: $slug) {
                    ... on ArticleSuccess {
                        article {
                            id
                            content
                        }
                    }
                    ... on ArticleError {
                        errorCodes
                    }
                }
            }
        "#;
        
        // Extract slug from provider_id or use the article URL
        let slug = &article.provider_id;
        
        let resp = self.client
            .post(Self::api_url(config))
            .header("Authorization", api_key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "query": query,
                "variables": {
                    "username": "me",
                    "slug": slug,
                }
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to get content: {}", resp.status())));
        }
        
        #[derive(Deserialize)]
        struct Response {
            data: Option<DataWrapper>,
        }
        
        #[derive(Deserialize)]
        struct DataWrapper {
            article: ArticleResult,
        }
        
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ArticleResult {
            Success { article: ArticleContent2 },
            Error { #[serde(rename = "errorCodes")] error_codes: Vec<String> },
        }
        
        #[derive(Deserialize)]
        struct ArticleContent2 {
            content: String,
        }
        
        let data: Response = resp.json().await
            .map_err(|e| ReadLaterError::Api(e.to_string()))?;
        
        let content = match data.data {
            Some(DataWrapper { article: ArticleResult::Success { article } }) => article.content,
            Some(DataWrapper { article: ArticleResult::Error { error_codes } }) => {
                return Err(ReadLaterError::Api(format!("Article error: {:?}", error_codes)));
            }
            None => {
                return Err(ReadLaterError::Api("No data returned".into()));
            }
        };
        
        Ok(ArticleContent {
            html: content,
            images: Vec::new(),
            styles: None,
        })
    }
    
    async fn update_read_status(&self, config: &ProviderConfig, provider_id: &str, status: ReadStatus) -> Result<()> {
        let ProviderConfig::Omnivore { api_key, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let api_key = api_key.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Omnivore".into()))?;
        
        let mutation = match status {
            ReadStatus::Archived => r#"
                mutation SetArchive($input: ArchiveLinkInput!) {
                    setLinkArchived(input: $input) {
                        ... on ArchiveLinkSuccess { linkId }
                        ... on ArchiveLinkError { errorCodes }
                    }
                }
            "#,
            _ => return Ok(()),
        };
        
        let resp = self.client
            .post(Self::api_url(config))
            .header("Authorization", api_key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "query": mutation,
                "variables": {
                    "input": {
                        "linkId": provider_id,
                        "archived": status == ReadStatus::Archived,
                    }
                }
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to update: {}", resp.status())));
        }
        
        Ok(())
    }
    
    async fn add_article(&self, config: &ProviderConfig, url: &str, tags: &[String]) -> Result<Article> {
        let ProviderConfig::Omnivore { api_key, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let api_key = api_key.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Omnivore".into()))?;
        
        let mutation = r#"
            mutation SaveUrl($input: SaveUrlInput!) {
                saveUrl(input: $input) {
                    ... on SaveSuccess {
                        url
                        clientRequestId
                    }
                    ... on SaveError {
                        errorCodes
                    }
                }
            }
        "#;
        
        let client_request_id = uuid::Uuid::new_v4().to_string();
        
        let resp = self.client
            .post(Self::api_url(config))
            .header("Authorization", api_key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "query": mutation,
                "variables": {
                    "input": {
                        "url": url,
                        "clientRequestId": client_request_id,
                        "labels": tags.iter().map(|t| serde_json::json!({"name": t})).collect::<Vec<_>>(),
                    }
                }
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to add: {}", resp.status())));
        }
        
        Ok(Article {
            id: uuid::Uuid::new_v4().to_string(),
            provider: ReadLaterProvider::Omnivore,
            provider_id: client_request_id,
            url: url.to_string(),
            title: String::new(),
            excerpt: None,
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
        let ProviderConfig::Omnivore { api_key, .. } = config else {
            return Err(ReadLaterError::Api("Invalid config".into()));
        };
        
        let api_key = api_key.as_ref()
            .ok_or_else(|| ReadLaterError::AuthRequired("Omnivore".into()))?;
        
        let mutation = r#"
            mutation SetBookmarkArticle($input: SetBookmarkArticleInput!) {
                setBookmarkArticle(input: $input) {
                    ... on SetBookmarkArticleSuccess { bookmarkedArticle { id } }
                    ... on SetBookmarkArticleError { errorCodes }
                }
            }
        "#;
        
        let resp = self.client
            .post(Self::api_url(config))
            .header("Authorization", api_key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "query": mutation,
                "variables": {
                    "input": {
                        "articleID": provider_id,
                        "bookmark": false,
                    }
                }
            }))
            .send()
            .await
            .map_err(|e| ReadLaterError::Network(e.to_string()))?;
        
        if !resp.status().is_success() {
            return Err(ReadLaterError::Api(format!("Failed to delete: {}", resp.status())));
        }
        
        Ok(())
    }
}


// ============================================================================
// EPUB/PDF Converter
// ============================================================================

pub struct ArticleConverter {
    output_dir: PathBuf,
}

impl ArticleConverter {
    pub fn new(output_dir: PathBuf) -> Self {
        Self { output_dir }
    }
    
    pub async fn convert(&self, article: &Article, content: &ArticleContent, format: ArticleFormat) -> Result<PathBuf> {
        match format {
            ArticleFormat::Html => self.save_html(article, content).await,
            ArticleFormat::Epub => self.convert_to_epub(article, content).await,
            ArticleFormat::Pdf => self.convert_to_pdf(article, content).await,
        }
    }
    
    async fn save_html(&self, article: &Article, content: &ArticleContent) -> Result<PathBuf> {
        let filename = self.sanitize_filename(&article.title);
        let path = self.output_dir.join(format!("{}.html", filename));
        
        let html = format!(r#"<!DOCTYPE html>
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
            article.title,
            content.styles.as_deref().unwrap_or(""),
            article.title,
            article.author.as_deref().map(|a| format!("By {} • ", a)).unwrap_or_default(),
            article.url,
            content.html
        );
        
        tokio::fs::write(&path, html).await?;
        Ok(path)
    }
    
    async fn convert_to_epub(&self, article: &Article, content: &ArticleContent) -> Result<PathBuf> {
        let filename = self.sanitize_filename(&article.title);
        let path = self.output_dir.join(format!("{}.epub", filename));
        
        // Create a minimal EPUB structure
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path();
        
        // mimetype file
        tokio::fs::write(temp_path.join("mimetype"), "application/epub+zip").await?;
        
        // META-INF/container.xml
        tokio::fs::create_dir_all(temp_path.join("META-INF")).await?;
        tokio::fs::write(temp_path.join("META-INF/container.xml"), r#"<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>"#).await?;
        
        // OEBPS directory
        tokio::fs::create_dir_all(temp_path.join("OEBPS")).await?;
        
        // content.opf
        let opf = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="uid">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="uid">{}</dc:identifier>
    <dc:title>{}</dc:title>
    <dc:creator>{}</dc:creator>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">{}</meta>
  </metadata>
  <manifest>
    <item id="content" href="content.xhtml" media-type="application/xhtml+xml"/>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
  </manifest>
  <spine>
    <itemref idref="content"/>
  </spine>
</package>"#,
            article.id,
            article.title.replace('&', "&amp;").replace('<', "&lt;"),
            article.author.as_deref().unwrap_or("Unknown").replace('&', "&amp;").replace('<', "&lt;"),
            Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );
        tokio::fs::write(temp_path.join("OEBPS/content.opf"), opf).await?;
        
        // nav.xhtml
        let nav = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head><title>Navigation</title></head>
<body>
<nav epub:type="toc">
  <ol><li><a href="content.xhtml">{}</a></li></ol>
</nav>
</body>
</html>"#, article.title.replace('&', "&amp;").replace('<', "&lt;"));
        tokio::fs::write(temp_path.join("OEBPS/nav.xhtml"), nav).await?;
        
        // content.xhtml
        let content_xhtml = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>{}</title>
<style>
body {{ font-family: Georgia, serif; line-height: 1.6; margin: 1em; }}
h1 {{ font-size: 1.5em; }}
img {{ max-width: 100%%; }}
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
            article.title.replace('&', "&amp;").replace('<', "&lt;"),
            article.title.replace('&', "&amp;").replace('<', "&lt;"),
            article.author.as_deref().unwrap_or("Unknown").replace('&', "&amp;").replace('<', "&lt;"),
            article.url,
            content.html
        );
        tokio::fs::write(temp_path.join("OEBPS/content.xhtml"), content_xhtml).await?;
        
        // Create ZIP (EPUB is just a ZIP)
        use std::process::Command;
        
        let output = Command::new("zip")
            .args(["-X0", path.to_str().unwrap(), "mimetype"])
            .current_dir(temp_path)
            .output()
            .map_err(|e| ReadLaterError::Conversion(format!("zip mimetype: {}", e)))?;
        
        if !output.status.success() {
            return Err(ReadLaterError::Conversion("Failed to create EPUB mimetype".into()));
        }
        
        let output = Command::new("zip")
            .args(["-Xr9D", path.to_str().unwrap(), "META-INF", "OEBPS"])
            .current_dir(temp_path)
            .output()
            .map_err(|e| ReadLaterError::Conversion(format!("zip content: {}", e)))?;
        
        if !output.status.success() {
            return Err(ReadLaterError::Conversion("Failed to create EPUB content".into()));
        }
        
        Ok(path)
    }
    
    async fn convert_to_pdf(&self, article: &Article, content: &ArticleContent) -> Result<PathBuf> {
        let filename = self.sanitize_filename(&article.title);
        let pdf_path = self.output_dir.join(format!("{}.pdf", filename));
        
        // Save HTML first
        let html_path = self.save_html(article, content).await?;
        
        // Use wkhtmltopdf or weasyprint if available
        use std::process::Command;
        
        // Try weasyprint first
        let result = Command::new("weasyprint")
            .args([html_path.to_str().unwrap(), pdf_path.to_str().unwrap()])
            .output();
        
        if let Ok(output) = result {
            if output.status.success() {
                return Ok(pdf_path);
            }
        }
        
        // Try wkhtmltopdf
        let result = Command::new("wkhtmltopdf")
            .args(["--quiet", html_path.to_str().unwrap(), pdf_path.to_str().unwrap()])
            .output();
        
        if let Ok(output) = result {
            if output.status.success() {
                return Ok(pdf_path);
            }
        }
        
        Err(ReadLaterError::Conversion(
            "No PDF converter available (install weasyprint or wkhtmltopdf)".into()
        ))
    }
    
    fn sanitize_filename(&self, name: &str) -> String {
        name.chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
                c if c.is_control() => '_',
                c => c,
            })
            .take(200)
            .collect()
    }
}

// ============================================================================
// Read Later Manager
// ============================================================================

pub struct ReadLaterManager {
    db: Connection,
    accounts: Arc<RwLock<HashMap<String, ProviderAccount>>>,
    articles: Arc<RwLock<HashMap<String, Article>>>,
    converter: ArticleConverter,
    storage_path: PathBuf,
    sync_tx: Option<mpsc::Sender<SyncCommand>>,
}

impl ReadLaterManager {
    pub fn new(db_path: &Path, storage_path: &Path) -> Result<Self> {
        let db = Connection::open(db_path)
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        Self::init_schema(&db)?;
        
        let converter = ArticleConverter::new(storage_path.join("articles"));
        
        let mut mgr = Self {
            db,
            accounts: Arc::new(RwLock::new(HashMap::new())),
            articles: Arc::new(RwLock::new(HashMap::new())),
            converter,
            storage_path: storage_path.to_path_buf(),
            sync_tx: None,
        };
        
        mgr.load_accounts()?;
        mgr.load_articles()?;
        
        Ok(mgr)
    }
    
    fn init_schema(db: &Connection) -> Result<()> {
        db.execute_batch(r#"
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
        "#).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    fn load_accounts(&mut self) -> Result<()> {
        let mut stmt = self.db.prepare(
            "SELECT id, name, provider, enabled, config, sync_settings, last_sync, created_at FROM readlater_accounts"
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let provider_str: String = row.get(2)?;
            let enabled: bool = row.get::<_, i32>(3)? != 0;
            let config_json: String = row.get(4)?;
            let sync_json: String = row.get(5)?;
            let last_sync: Option<String> = row.get(6)?;
            let created_at: String = row.get(7)?;
            
            Ok((id, name, provider_str, enabled, config_json, sync_json, last_sync, created_at))
        }).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        let mut accounts = self.accounts.write();
        
        for row in rows {
            let (id, name, provider_str, enabled, config_json, sync_json, last_sync, created_at) = 
                row.map_err(|e| ReadLaterError::Database(e.to_string()))?;
            
            let provider: ReadLaterProvider = serde_json::from_str(&format!("\"{}\"", provider_str))
                .map_err(|e| ReadLaterError::Database(e.to_string()))?;
            
            let config: ProviderConfig = serde_json::from_str(&config_json)
                .map_err(|e| ReadLaterError::Database(e.to_string()))?;
            
            let sync_settings: SyncSettings = serde_json::from_str(&sync_json)
                .map_err(|e| ReadLaterError::Database(e.to_string()))?;
            
            let last_sync_dt = last_sync.and_then(|s| {
                DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&Utc))
            });
            
            let created_dt = DateTime::parse_from_rfc3339(&created_at)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            
            accounts.insert(id.clone(), ProviderAccount {
                id,
                name,
                provider,
                enabled,
                config,
                sync_settings,
                last_sync: last_sync_dt,
                created_at: created_dt,
            });
        }
        
        Ok(())
    }
    
    fn load_articles(&mut self) -> Result<()> {
        let mut stmt = self.db.prepare(
            "SELECT id, provider, provider_id, url, title, excerpt, author, word_count, reading_time_minutes,              tags, status, favorite, added_at, updated_at, read_at, image_url, document_id, synced_to_device, last_sync              FROM readlater_articles"
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        let rows = stmt.query_map([], |row| {
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
            
            Ok((id, provider_str, provider_id, url, title, excerpt, author, word_count, 
                reading_time, tags_json, status_str, favorite, added_at, updated_at, 
                read_at, image_url, document_id, synced, last_sync))
        }).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        let mut articles = self.articles.write();
        
        for row in rows {
            let (id, provider_str, provider_id, url, title, excerpt, author, word_count,
                reading_time, tags_json, status_str, favorite, added_at, updated_at,
                read_at, image_url, document_id, synced, last_sync) = 
                row.map_err(|e| ReadLaterError::Database(e.to_string()))?;
            
            let provider: ReadLaterProvider = serde_json::from_str(&format!("\"{}\"", provider_str))
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
            
            articles.insert(id.clone(), Article {
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
            });
        }
        
        Ok(())
    }
    
    // Account management
    
    pub fn add_account(&mut self, account: ProviderAccount) -> Result<()> {
        let config_json = serde_json::to_string(&account.config)?;
        let sync_json = serde_json::to_string(&account.sync_settings)?;
        
        self.db.execute(
            "INSERT INTO readlater_accounts (id, name, provider, enabled, config, sync_settings, last_sync, created_at)              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                account.id,
                account.name,
                account.provider.to_string(),
                account.enabled as i32,
                config_json,
                sync_json,
                account.last_sync.map(|d| d.to_rfc3339()),
                account.created_at.to_rfc3339(),
            ],
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        self.accounts.write().insert(account.id.clone(), account);
        Ok(())
    }
    
    pub fn update_account(&mut self, account: ProviderAccount) -> Result<()> {
        let config_json = serde_json::to_string(&account.config)?;
        let sync_json = serde_json::to_string(&account.sync_settings)?;
        
        self.db.execute(
            "UPDATE readlater_accounts SET name=?2, enabled=?3, config=?4, sync_settings=?5, last_sync=?6 WHERE id=?1",
            params![
                account.id,
                account.name,
                account.enabled as i32,
                config_json,
                sync_json,
                account.last_sync.map(|d| d.to_rfc3339()),
            ],
        ).map_err(|e| ReadLaterError::Database(e.to_string()))?;
        
        self.accounts.write().insert(account.id.clone(), account);
        Ok(())
    }
    
    pub fn delete_account(&mut self, id: &str) -> Result<()> {
        self.db.execute("DELETE FROM readlater_accounts WHERE id=?1", params![id])
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
        
        self.articles.write().insert(article.id.clone(), article.clone());
        Ok(())
    }
    
    pub fn get_article(&self, id: &str) -> Option<Article> {
        self.articles.read().get(id).cloned()
    }
    
    pub fn query_articles(&self, query: &ArticleQuery) -> Vec<Article> {
        let articles = self.articles.read();
        let mut results: Vec<Article> = articles.values()
            .filter(|a| {
                if let Some(provider) = query.provider {
                    if a.provider != provider { return false; }
                }
                if let Some(status) = query.status {
                    if a.status != status { return false; }
                }
                if let Some(favorite) = query.favorite {
                    if a.favorite != favorite { return false; }
                }
                if let Some(synced) = query.synced {
                    if a.synced_to_device != synced { return false; }
                }
                if let Some(ref tags) = query.tags {
                    if !tags.iter().any(|t| a.tags.contains(t)) { return false; }
                }
                if let Some(ref search) = query.search {
                    let s = search.to_lowercase();
                    if !a.title.to_lowercase().contains(&s) && 
                       !a.excerpt.as_ref().map(|e| e.to_lowercase().contains(&s)).unwrap_or(false) {
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
        self.db.execute("DELETE FROM readlater_articles WHERE id=?1", params![id])
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
                let provider: ReadLaterProvider = serde_json::from_str(&format!("\"{}\"", provider_str))
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
        self.db.execute("DELETE FROM readlater_oauth_states WHERE id=?1", params![id])
            .map_err(|e| ReadLaterError::Database(e.to_string()))?;
        Ok(())
    }
    
    // Sync operations
    
    pub async fn sync_account(&mut self, account_id: &str) -> Result<SyncResult> {
        let start = std::time::Instant::now();
        let mut result = SyncResult {
            provider: ReadLaterProvider::Pocket,
            articles_fetched: 0,
            articles_synced: 0,
            articles_converted: 0,
            read_status_synced: 0,
            errors: Vec::new(),
            duration_ms: 0,
            completed_at: Utc::now(),
        };
        
        let account = self.get_account(account_id)
            .ok_or_else(|| ReadLaterError::ProviderNotFound(account_id.into()))?;
        
        result.provider = account.provider;
        
        // Create provider instance
        let provider: Box<dyn ReadLaterProviderTrait> = match account.provider {
            ReadLaterProvider::Pocket => Box::new(PocketProvider::new()),
            ReadLaterProvider::Instapaper => {
                let key = std::env::var("INSTAPAPER_CONSUMER_KEY").unwrap_or_default();
                let secret = std::env::var("INSTAPAPER_CONSUMER_SECRET").unwrap_or_default();
                Box::new(InstapaperProvider::new(key, secret))
            }
            ReadLaterProvider::Wallabag => Box::new(WallabagProvider::new()),
            ReadLaterProvider::Omnivore => Box::new(OmnivoreProvider::new()),
        };
        
        // Fetch articles
        let since = account.last_sync;
        match provider.fetch_articles(&account.config, since).await {
            Ok(articles) => {
                result.articles_fetched = articles.len() as u32;
                
                for article in articles {
                    // Check tag filters
                    if !account.sync_settings.tag_filters.is_empty() {
                        let matches = account.sync_settings.tag_filters.iter().any(|f| {
                            let has_tag = article.tags.iter().any(|t| t.eq_ignore_ascii_case(&f.tag));
                            if f.include { has_tag } else { !has_tag }
                        });
                        if !matches { continue; }
                    }
                    
                    if account.sync_settings.include_favorites_only && !article.favorite {
                        continue;
                    }
                    
                    if !account.sync_settings.include_archived && article.status == ReadStatus::Archived {
                        continue;
                    }
                    
                    // Save article
                    if let Err(e) = self.save_article(&article) {
                        result.errors.push(format!("Save {}: {}", article.id, e));
                        continue;
                    }
                    
                    result.articles_synced += 1;
                    
                    // Convert to device format
                    if account.sync_settings.convert_format != ArticleFormat::Html {
                        match provider.fetch_article_content(&account.config, &article).await {
                            Ok(content) => {
                                match self.converter.convert(&article, &content, account.sync_settings.convert_format).await {
                                    Ok(_path) => {
                                        result.articles_converted += 1;
                                    }
                                    Err(e) => {
                                        result.errors.push(format!("Convert {}: {}", article.id, e));
                                    }
                                }
                            }
                            Err(e) => {
                                result.errors.push(format!("Content {}: {}", article.id, e));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                result.errors.push(format!("Fetch: {}", e));
            }
        }
        
        // Sync read status back
        if account.sync_settings.sync_read_status {
            let articles_to_sync: Vec<Article> = self.articles.read()
                .values()
                .filter(|a| {
                    a.provider == account.provider &&
                    a.synced_to_device &&
                    (a.status == ReadStatus::Read || a.status == ReadStatus::Archived)
                })
                .cloned()
                .collect();
            
            for article in articles_to_sync {
                match provider.update_read_status(&account.config, &article.provider_id, article.status).await {
                    Ok(()) => {
                        result.read_status_synced += 1;
                    }
                    Err(e) => {
                        result.errors.push(format!("Status {}: {}", article.id, e));
                    }
                }
            }
        }
        
        // Update last sync time
        let mut updated_account = account;
        updated_account.last_sync = Some(Utc::now());
        let _ = self.update_account(updated_account);
        
        result.duration_ms = start.elapsed().as_millis() as u64;
        result.completed_at = Utc::now();
        
        Ok(result)
    }
    
    pub async fn sync_all(&mut self) -> Vec<SyncResult> {
        let account_ids: Vec<String> = self.accounts.read()
            .values()
            .filter(|a| a.enabled)
            .map(|a| a.id.clone())
            .collect();
        
        let mut results = Vec::new();
        
        for id in account_ids {
            match self.sync_account(&id).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    results.push(SyncResult {
                        provider: ReadLaterProvider::Pocket,
                        articles_fetched: 0,
                        articles_synced: 0,
                        articles_converted: 0,
                        read_status_synced: 0,
                        errors: vec![e.to_string()],
                        duration_ms: 0,
                        completed_at: Utc::now(),
                    });
                }
            }
        }
        
        results
    }
    
    // Scheduler
    
    pub fn start_scheduler(&mut self) -> mpsc::Sender<SyncCommand> {
        let (tx, mut rx) = mpsc::channel::<SyncCommand>(32);
        self.sync_tx = Some(tx.clone());
        
        let accounts = Arc::clone(&self.accounts);
        let articles = Arc::clone(&self.articles);
        let storage_path = self.storage_path.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check for accounts that need sync
                        let accounts_to_sync: Vec<ProviderAccount> = {
                            let accounts = accounts.read();
                            accounts.values()
                                .filter(|a| {
                                    if !a.enabled || !a.sync_settings.auto_sync { return false; }
                                    
                                    let interval = Duration::minutes(a.sync_settings.sync_interval_minutes as i64);
                                    match a.last_sync {
                                        Some(last) => Utc::now() - last > interval,
                                        None => true,
                                    }
                                })
                                .cloned()
                                .collect()
                        };
                        
                        for account in accounts_to_sync {
                            tracing::info!("Scheduled sync for {} ({})", account.name, account.provider);
                            // Would trigger sync here - need to refactor to share manager
                        }
                    }
                    
                    Some(cmd) = rx.recv() => {
                        match cmd {
                            SyncCommand::Stop => break,
                            _ => {
                                // Handle other commands
                            }
                        }
                    }
                }
            }
        });
        
        tx
    }
    
    pub fn stop_scheduler(&mut self) {
        if let Some(tx) = self.sync_tx.take() {
            let _ = tx.try_send(SyncCommand::Stop);
        }
    }
}

// Default implementations
impl Default for PocketProvider {
    fn default() -> Self { Self::new() }
}

impl Default for WallabagProvider {
    fn default() -> Self { Self::new() }
}

impl Default for OmnivoreProvider {
    fn default() -> Self { Self::new() }
}
