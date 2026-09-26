//! Cloud storage integrations for remarkable-server
//!
//! Supports bidirectional sync with:
//! - Google Drive
//! - Dropbox
//! - OneDrive
//!
//! All providers use OAuth 2.0 PKCE flow for secure authentication.

pub mod api;
pub mod conflict;
pub mod dropbox;
pub mod google_drive;
pub mod oauth;
pub mod onedrive;
pub mod sync;

use std::path::PathBuf;

pub use api::{
    IntegrationState,
    integration_api_router,
    integration_oauth_router,
    integration_router,
};
use async_trait::async_trait;
pub use conflict::{ConflictResolution, ConflictResolver, ConflictStrategy};
pub use oauth::{OAuthConfig, OAuthProvider, OAuthToken, PkceFlow};
use serde::{Deserialize, Serialize};
pub use sync::{CloudSync, SyncConfig, SyncDirection, SyncResult, SyncStatus};
use thiserror::Error;

/// Errors from cloud integrations
#[derive(Debug, Error)]
pub enum IntegrationError {
    #[error("OAuth error: {0}")]
    OAuth(String),

    #[error("Token expired")]
    TokenExpired,

    #[error("Token refresh failed: {0}")]
    TokenRefreshFailed(String),

    #[error("API error: {0}")]
    Api(String),

    #[error("Rate limited, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    #[error("Quota exceeded")]
    QuotaExceeded,

    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Provider not configured")]
    NotConfigured,

    #[error("Unsafe path rejected: {0}")]
    InvalidPath(String),

    /// The item exists but its content can never be downloaded as-is (e.g. a Google Docs
    /// editor file, or the owner disabled downloads).
    #[error("Not downloadable: {0}")]
    NotDownloadable(String),
}

impl IntegrationError {
    /// Retrying can never succeed: the remote name is rejected by path validation (traversal,
    /// symlink escape), the item no longer exists, or its content can't be downloaded. Everything else (network, I/O, rate limit,
    /// auth, API errors) may be transient. Delta sync advances its cursor past permanent
    /// failures but holds it for transient ones so the change is fetched again next time.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::InvalidPath(_) | Self::NotFound(_) | Self::NotDownloadable(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, IntegrationError>;

/// Connect timeout for provider / OAuth requests.
pub(crate) const HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Whole-request ceiling (incl. body transfer): generous for large uploads/downloads, but finite
/// so a hung provider can't hold the sync lock forever.
pub(crate) const HTTP_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// HTTP client shared by all providers and the OAuth token/revoke calls.
pub(crate) fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
        .build()
        .expect("reqwest client with timeouts")
}

/// Cloud file metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFile {
    /// Provider-specific file ID
    pub id: String,
    /// File name
    pub name: String,
    /// MIME type
    pub mime_type: Option<String>,
    /// File size in bytes
    pub size: u64,
    /// Last modification time (Unix timestamp)
    pub modified_at: i64,
    /// Content hash (provider-specific)
    pub content_hash: Option<String>,
    /// Parent folder ID
    pub parent_id: Option<String>,
    /// Is this a folder?
    pub is_folder: bool,
    /// Full path in cloud storage
    pub path: String,
}

/// Cloud folder for selective sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFolder {
    pub id: String,
    pub name: String,
    pub path: String,
    pub parent_id: Option<String>,
}

/// Sync folder configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncFolderConfig {
    /// Cloud folder ID or path
    pub cloud_path: String,
    /// Local relative path
    pub local_path: PathBuf,
    /// Sync direction
    pub direction: SyncDirection,
    /// Include subfolders
    pub recursive: bool,
    /// File patterns to include (glob)
    pub include_patterns: Vec<String>,
    /// File patterns to exclude (glob)
    pub exclude_patterns: Vec<String>,
}

/// Provider type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderType {
    GoogleDrive,
    Dropbox,
    OneDrive,
}

impl std::fmt::Display for ProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderType::GoogleDrive => write!(f, "google_drive"),
            ProviderType::Dropbox => write!(f, "dropbox"),
            ProviderType::OneDrive => write!(f, "onedrive"),
        }
    }
}

/// Cloud storage provider trait
#[async_trait]
pub trait CloudProvider: Send + Sync {
    /// Get provider type
    fn provider_type(&self) -> ProviderType;

    /// Check if authenticated
    fn is_authenticated(&self) -> bool;

    /// Get current token (if any)
    fn get_token(&self) -> Option<&OAuthToken>;

    /// Set token (after OAuth flow or refresh)
    fn set_token(&mut self, token: OAuthToken);

    /// Refresh the access token
    async fn refresh_token(&mut self) -> Result<()>;

    /// List files in a folder
    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>>;

    /// List all folders (for selective sync UI)
    async fn list_folders(&self) -> Result<Vec<CloudFolder>>;

    /// Get file metadata
    async fn get_file_metadata(&self, file_id: &str) -> Result<CloudFile>;

    /// Download file content
    async fn download_file(&self, file_id: &str) -> Result<Vec<u8>>;

    /// Upload file (creates or updates)
    async fn upload_file(
        &self,
        parent_id: Option<&str>,
        name: &str,
        content: &[u8],
        mime_type: Option<&str>,
    ) -> Result<CloudFile>;

    /// Upload to a path relative to `parent_id`, given as already-validated components
    /// (`["dir", "sub", "name.pdf"]`). The default suits path-addressed providers, whose
    /// upload endpoints create missing intermediate folders themselves: Dropbox `files/upload`
    /// with `path = "{parent}/dir/sub/name.pdf"`, and Graph `PUT
    /// items/{parent}:/dir/sub/name.pdf:/content` (or `createUploadSession`). ID-addressed
    /// providers (Google Drive) must override this and create each folder.
    async fn upload_file_at(
        &self,
        parent_id: Option<&str>,
        components: &[&str],
        content: &[u8],
        mime_type: Option<&str>,
    ) -> Result<CloudFile> {
        self.upload_file(parent_id, &components.join("/"), content, mime_type)
            .await
    }

    /// Create folder
    async fn create_folder(&self, parent_id: Option<&str>, name: &str) -> Result<CloudFolder>;

    /// Delete file or folder
    async fn delete(&self, file_id: &str) -> Result<()>;

    /// Move file or folder
    async fn move_file(
        &self,
        file_id: &str,
        new_parent_id: &str,
        new_name: Option<&str>,
    ) -> Result<CloudFile>;

    /// Get changes since last sync (delta API if supported)
    async fn get_changes(&self, cursor: Option<&str>) -> Result<(Vec<CloudFile>, Option<String>)>;

    /// Changes since `cursor` under `folder_id` (default: the drive root), with paths relative
    /// to that folder like [`list_files`](Self::list_files). Providers whose change feed is
    /// already scoped and pathed that way can rely on the default, which ignores `folder_id`.
    async fn get_changes_in(
        &self,
        folder_id: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<(Vec<CloudFile>, Option<String>)> {
        let _ = folder_id;
        self.get_changes(cursor).await
    }

    /// Get storage quota info
    async fn get_quota(&self) -> Result<StorageQuota>;
}

/// Storage quota information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageQuota {
    /// Used space in bytes
    pub used: u64,
    /// Total space in bytes (None if unlimited)
    pub total: Option<u64>,
    /// Trash size in bytes
    pub trash: Option<u64>,
}

/// Integration manager for multiple providers
pub struct IntegrationManager {
    providers: std::collections::HashMap<ProviderType, Box<dyn CloudProvider>>,
    sync_configs: Vec<SyncFolderConfig>,
    conflict_resolver: ConflictResolver,
}

impl IntegrationManager {
    pub fn new() -> Self {
        Self {
            providers: std::collections::HashMap::new(),
            sync_configs: Vec::new(),
            conflict_resolver: ConflictResolver::new(ConflictStrategy::NewerWins),
        }
    }

    pub fn add_provider(&mut self, provider: Box<dyn CloudProvider>) {
        let ptype = provider.provider_type();
        self.providers.insert(ptype, provider);
    }

    pub fn get_provider(&self, ptype: ProviderType) -> Option<&dyn CloudProvider> {
        self.providers.get(&ptype).map(|p| p.as_ref())
    }

    pub fn get_provider_mut(&mut self, ptype: ProviderType) -> Option<&mut Box<dyn CloudProvider>> {
        self.providers.get_mut(&ptype)
    }

    pub fn set_conflict_strategy(&mut self, strategy: ConflictStrategy) {
        self.conflict_resolver = ConflictResolver::new(strategy);
    }

    pub fn add_sync_config(&mut self, config: SyncFolderConfig) {
        self.sync_configs.push(config);
    }
}

impl Default for IntegrationManager {
    fn default() -> Self {
        Self::new()
    }
}
