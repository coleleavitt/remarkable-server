//! Google Drive integration
//!
//! Full read/write access via Drive API v3.

use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::integrations::oauth::{OAuthConfig, OAuthToken, refresh_token};
use crate::integrations::sync::{MAX_LIST_DEPTH, is_safe_name};
use crate::integrations::{
    CloudFile,
    CloudFolder,
    CloudProvider,
    IntegrationError,
    ProviderType,
    Result,
    StorageQuota,
};

const API_BASE: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_BASE: &str = "https://www.googleapis.com/upload/drive/v3";
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";
/// Largest page size `files.list` accepts; fewer round-trips on big drives.
const PAGE_SIZE: u32 = 1000;
/// Fields requested for every listed file (plus `nextPageToken` for paging).
const LIST_FIELDS: &str =
    "nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum,parents)";

/// Escape a value for a single-quoted string in a Drive `q` expression.
fn escape_query(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Google Drive provider
pub struct GoogleDrive {
    config: OAuthConfig,
    token: Option<OAuthToken>,
    client: Client,
    api_base: String,
    upload_base: String,
}

impl GoogleDrive {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            token: None,
            client: crate::integrations::http_client(),
            api_base: API_BASE.into(),
            upload_base: UPLOAD_BASE.into(),
        }
    }

    pub fn with_token(config: OAuthConfig, token: OAuthToken) -> Self {
        Self {
            config,
            token: Some(token),
            client: crate::integrations::http_client(),
            api_base: API_BASE.into(),
            upload_base: UPLOAD_BASE.into(),
        }
    }

    /// Point the provider at a different Drive API / upload endpoint (tests, proxies).
    pub fn with_base_urls(mut self, api_base: &str, upload_base: &str) -> Self {
        self.api_base = api_base.trim_end_matches('/').into();
        self.upload_base = upload_base.trim_end_matches('/').into();
        self
    }

    fn access_token(&self) -> Result<&str> {
        self.token
            .as_ref()
            .map(|t| t.access_token.as_str())
            .ok_or(IntegrationError::NotConfigured)
    }

    /// Make authenticated request, auto-refreshing if needed
    async fn request(&self, method: reqwest::Method, url: &str) -> Result<reqwest::RequestBuilder> {
        let token = self.access_token()?;
        Ok(self.client.request(method, url).bearer_auth(token))
    }

    /// Find a non-trashed child folder of `parent` named `name`.
    async fn find_folder(&self, parent: &str, name: &str) -> Result<Option<String>> {
        let query = format!(
            "name = '{}' and '{}' in parents and mimeType = '{}' and trashed = false",
            escape_query(name),
            escape_query(parent),
            FOLDER_MIME
        );
        let url = format!(
            "{}/files?q={}&fields=files(id,name,mimeType,parents)",
            self.api_base,
            urlencoding::encode(&query)
        );
        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;
        let list: ListFilesResponse = self.handle_response(response).await?;
        Ok(list.files.into_iter().next().map(|f| f.id))
    }

    /// Every file matching `query`, following `nextPageToken` until the listing is exhausted.
    async fn list_query(&self, query: &str) -> Result<Vec<DriveFile>> {
        let mut files = Vec::new();
        let mut seen_tokens = std::collections::HashSet::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = format!(
                "{}/files?q={}&pageSize={}&orderBy=createdTime&fields={}",
                self.api_base,
                urlencoding::encode(query),
                PAGE_SIZE,
                urlencoding::encode(LIST_FIELDS)
            );
            if let Some(t) = &page_token {
                url.push_str("&pageToken=");
                url.push_str(&urlencoding::encode(t));
            }
            let response = self
                .request(reqwest::Method::GET, &url)
                .await?
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;
            let page: ListFilesResponse = self.handle_response(response).await?;
            files.extend(page.files);
            match page.next_page_token.filter(|t| !t.is_empty()) {
                // A repeated token would loop forever; fail rather than return a partial list
                // (a partial list makes sync re-upload everything it didn't see).
                Some(t) if !seen_tokens.insert(t.clone()) => {
                    return Err(IntegrationError::Api(format!(
                        "Drive returned repeated page token {:?}",
                        t
                    )));
                }
                Some(t) => page_token = Some(t),
                None => return Ok(files),
            }
        }
    }

    /// Handle API response, checking for errors
    async fn handle_response<T: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
    ) -> Result<T> {
        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| IntegrationError::Serialization(e.to_string()))
        } else {
            Err(response_error(response).await)
        }
    }

    /// Metadata of folder `id` for path resolution; `None` if it's gone or not visible.
    async fn folder_meta(&self, id: &str) -> Result<Option<DriveFile>> {
        let url = format!(
            "{}/files/{}?fields=id,name,mimeType,parents,trashed",
            self.api_base,
            urlencoding::encode(id)
        );
        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;
        match self.handle_response(response).await {
            Ok(f) => Ok(Some(f)),
            Err(IntegrationError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Path of folder `id` relative to the sync root (`""` for the root itself, `"/A/B"`
    /// below it), or `None` if no chain of parents within [`MAX_LIST_DEPTH`] reaches the root.
    /// Memoized per call in `cache`; a folder being resolved is provisionally `None`, which
    /// also breaks parent cycles.
    async fn folder_path(
        &self,
        id: &str,
        root: &SyncRoot,
        cache: &mut HashMap<String, Option<String>>,
        depth: usize,
    ) -> Result<Option<String>> {
        if root.is(id) {
            return Ok(Some(String::new()));
        }
        if let Some(hit) = cache.get(id) {
            return Ok(hit.clone());
        }
        if depth >= MAX_LIST_DEPTH {
            return Ok(None);
        }
        cache.insert(id.to_string(), None);
        let resolved = match self.folder_meta(id).await? {
            Some(f) if f.trashed != Some(true) && is_safe_name(&f.name) => {
                Box::pin(self.path_under(&f, root, cache, depth + 1)).await?
            }
            _ => None,
        };
        cache.insert(id.to_string(), resolved.clone());
        Ok(resolved)
    }

    /// Path of `f` relative to the sync root via the first of its parents that reaches it.
    async fn path_under(
        &self,
        f: &DriveFile,
        root: &SyncRoot,
        cache: &mut HashMap<String, Option<String>>,
        depth: usize,
    ) -> Result<Option<String>> {
        for parent in f.parents.iter().flatten() {
            if let Some(prefix) = self.folder_path(parent, root, cache, depth).await? {
                return Ok(Some(format!("{}/{}", prefix, f.name)));
            }
        }
        Ok(None)
    }

    /// The sync root for change resolution: the configured folder, or My Drive whose real id
    /// (the one that appears in `parents`) is looked up, since `root` is only an alias.
    async fn sync_root(&self, folder_id: Option<&str>) -> Result<SyncRoot> {
        match folder_id {
            Some(id) if id != "root" => Ok(SyncRoot(id.to_string(), None)),
            _ => {
                let url = format!("{}/files/root?fields=id", self.api_base);
                let response = self
                    .request(reqwest::Method::GET, &url)
                    .await?
                    .send()
                    .await
                    .map_err(|e| IntegrationError::Network(e.to_string()))?;
                let f: FileId = self.handle_response(response).await?;
                Ok(SyncRoot("root".into(), Some(f.id)))
            }
        }
    }
}

/// Map a non-success Drive response to an error. 404 is [`IntegrationError::NotFound`] and a
/// 403 for content that can never be downloaded (Docs editor files, downloads disabled by the
/// owner) is [`IntegrationError::NotDownloadable`], both permanent; auth, rate limits, other
/// 403s and 5xx stay retryable.
async fn response_error(response: reqwest::Response) -> IntegrationError {
    let status = response.status();
    match status.as_u16() {
        401 => IntegrationError::TokenExpired,
        429 => IntegrationError::RateLimited {
            retry_after_secs: response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
        },
        403 => {
            let body = response.text().await.unwrap_or_default();
            if body.contains("storageQuotaExceeded") {
                IntegrationError::QuotaExceeded
            } else if [
                "fileNotDownloadable",
                "cannotDownloadFile",
                "cannotDownloadAbusiveFile",
            ]
            .iter()
            .any(|r| body.contains(r))
            {
                IntegrationError::NotDownloadable(body)
            } else {
                IntegrationError::Api(format!("Forbidden: {}", body))
            }
        }
        404 => IntegrationError::NotFound("File not found".into()),
        _ => {
            let body = response.text().await.unwrap_or_default();
            IntegrationError::Api(format!("{}: {}", status, body))
        }
    }
}

/// Folder that change paths are resolved against: its id as configured, plus the real id when
/// that is the `root` alias.
struct SyncRoot(String, Option<String>);

impl SyncRoot {
    fn is(&self, id: &str) -> bool {
        self.0 == id || self.1.as_deref() == Some(id)
    }
}

#[derive(Deserialize)]
struct FileId {
    id: String,
}

/// Google Drive file response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveFile {
    id: String,
    name: String,
    mime_type: String,
    #[serde(default)]
    size: Option<String>,
    modified_time: Option<String>,
    #[serde(default)]
    md5_checksum: Option<String>,
    #[serde(default)]
    parents: Option<Vec<String>>,
    #[serde(default)]
    trashed: Option<bool>,
}

impl DriveFile {
    fn to_cloud_file(&self, path: String) -> CloudFile {
        CloudFile {
            id: self.id.clone(),
            name: self.name.clone(),
            mime_type: Some(self.mime_type.clone()),
            size: self.size.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0),
            modified_at: self
                .modified_time
                .as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|dt| dt.timestamp())
                .unwrap_or(0),
            content_hash: self.md5_checksum.clone(),
            parent_id: self.parents.as_ref().and_then(|p| p.first().cloned()),
            is_folder: self.mime_type == "application/vnd.google-apps.folder",
            path,
            deleted: false,
        }
    }
}

/// List files response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListFilesResponse {
    files: Vec<DriveFile>,
    next_page_token: Option<String>,
}

/// About response (for quota)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AboutResponse {
    storage_quota: StorageQuotaResponse,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageQuotaResponse {
    usage: String,
    limit: Option<String>,
    usage_in_drive_trash: Option<String>,
}

/// Changes response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangesResponse {
    changes: Vec<Change>,
    new_start_page_token: Option<String>,
    #[allow(dead_code)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Change {
    #[allow(dead_code)]
    file_id: Option<String>,
    file: Option<DriveFile>,
    removed: Option<bool>,
}

#[async_trait]
impl CloudProvider for GoogleDrive {
    fn provider_type(&self) -> ProviderType {
        ProviderType::GoogleDrive
    }

    fn is_authenticated(&self) -> bool {
        self.token.is_some()
    }

    fn get_token(&self) -> Option<&OAuthToken> {
        self.token.as_ref()
    }

    fn set_token(&mut self, token: OAuthToken) {
        self.token = Some(token);
    }

    async fn refresh_token(&mut self) -> Result<()> {
        let current = self.token.as_ref().ok_or(IntegrationError::NotConfigured)?;
        let refresh = current
            .refresh_token
            .as_ref()
            .ok_or_else(|| IntegrationError::TokenRefreshFailed("No refresh token".into()))?;

        let new_token = refresh_token(&self.config, refresh, &self.client).await?;
        self.token = Some(new_token);
        Ok(())
    }

    /// Recursively list everything under `folder_id` (default: My Drive root), paging through
    /// each folder. Paths are relative to that folder (`/Sub/dir/file.pdf`), matching the
    /// local scan so nested files aren't seen as missing and re-uploaded every sync.
    ///
    /// Drive is a graph, not a tree (multiple parents, same-name siblings, `/` in names), so
    /// the walk is defensive: each folder is entered once (no cycles), each file id is listed
    /// once, the first item wins a duplicate path (oldest, via `orderBy=createdTime`), names
    /// that aren't a single safe path segment are skipped, and depth is capped.
    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        let root = folder_id.unwrap_or("root").to_string();
        let mut visited_folders = HashSet::from([root.clone()]);
        let mut seen_files = HashSet::new();
        let mut seen_paths = HashSet::new();
        let mut queue = VecDeque::from([(root, String::new(), 0usize)]);
        let mut out = Vec::new();

        while let Some((folder, prefix, depth)) = queue.pop_front() {
            let query = format!("'{}' in parents and trashed = false", escape_query(&folder));
            for f in self.list_query(&query).await? {
                if !is_safe_name(&f.name) {
                    tracing::warn!(
                        "google drive: skipping unsafe name {:?} in {}",
                        f.name,
                        prefix
                    );
                    continue;
                }
                let path = format!("{}/{}", prefix, f.name);
                let is_folder = f.mime_type == FOLDER_MIME;
                if is_folder {
                    if !visited_folders.insert(f.id.clone()) {
                        continue; // already reached via another parent, or a cycle
                    }
                } else if !seen_files.insert(f.id.clone()) {
                    continue; // same file under several parents: list it once
                }
                if !seen_paths.insert(path.clone()) {
                    tracing::warn!("google drive: duplicate path {:?}, keeping the first", path);
                    continue;
                }
                if is_folder {
                    if depth + 1 < MAX_LIST_DEPTH {
                        queue.push_back((f.id.clone(), path.clone(), depth + 1));
                    } else {
                        tracing::warn!("google drive: not descending into {:?}: too deep", path);
                    }
                }
                out.push(f.to_cloud_file(path));
            }
        }
        Ok(out)
    }

    async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
        let query = format!("mimeType = '{}' and trashed = false", FOLDER_MIME);
        Ok(self
            .list_query(&query)
            .await?
            .into_iter()
            .map(|f| CloudFolder {
                id: f.id,
                name: f.name.clone(),
                path: format!("/{}", f.name),
                parent_id: f.parents.and_then(|p| p.into_iter().next()),
            })
            .collect())
    }

    async fn get_file_metadata(&self, file_id: &str) -> Result<CloudFile> {
        let url = format!(
            "{}/files/{}?fields=id,name,mimeType,size,modifiedTime,md5Checksum,parents",
            self.api_base, file_id
        );

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let file: DriveFile = self.handle_response(response).await?;
        Ok(file.to_cloud_file(format!("/{}", file.name)))
    }

    async fn download_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let url = format!("{}/files/{}?alt=media", self.api_base, file_id);

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(response_error(response).await);
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| IntegrationError::Network(e.to_string()))
    }

    async fn upload_file(
        &self,
        parent_id: Option<&str>,
        name: &str,
        content: &[u8],
        mime_type: Option<&str>,
    ) -> Result<CloudFile> {
        let mime = mime_type.unwrap_or("application/octet-stream");

        // Use multipart upload for simplicity
        #[derive(Serialize)]
        struct FileMetadata<'a> {
            name: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            parents: Option<Vec<&'a str>>,
        }

        let metadata = FileMetadata {
            name,
            parents: parent_id.map(|p| vec![p]),
        };

        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|e| IntegrationError::Serialization(e.to_string()))?;

        // Build multipart body
        let boundary = "remarkable_upload_boundary";
        let mut body = Vec::new();

        // Metadata part
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(b"Content-Type: application/json; charset=UTF-8\r\n\r\n");
        body.extend_from_slice(metadata_json.as_bytes());
        body.extend_from_slice(b"\r\n");

        // Content part
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", mime).as_bytes());
        body.extend_from_slice(content);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{}--", boundary).as_bytes());

        let url = format!(
            "{}/files?uploadType=multipart&fields=id,name,mimeType,size,modifiedTime,md5Checksum,parents",
            self.upload_base
        );

        let token = self.access_token()?;
        let response = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header(
                "Content-Type",
                format!("multipart/related; boundary={}", boundary),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let file: DriveFile = self.handle_response(response).await?;
        Ok(file.to_cloud_file(format!("/{}", file.name)))
    }

    async fn upload_file_at(
        &self,
        parent_id: Option<&str>,
        components: &[&str],
        content: &[u8],
        mime_type: Option<&str>,
    ) -> Result<CloudFile> {
        // Drive addresses folders by ID, not path: walk (find or create) each directory.
        let (name, dirs) = components
            .split_last()
            .ok_or_else(|| IntegrationError::InvalidPath("empty path".into()))?;
        let mut parent = parent_id.unwrap_or("root").to_string();
        for dir in dirs {
            parent = match self.find_folder(&parent, dir).await? {
                Some(id) => id,
                None => self.create_folder(Some(&parent), dir).await?.id,
            };
        }
        let mut file = self
            .upload_file(Some(&parent), name, content, mime_type)
            .await?;
        file.path = format!("/{}", components.join("/")); // full relative path, not just the basename
        Ok(file)
    }

    async fn create_folder(&self, parent_id: Option<&str>, name: &str) -> Result<CloudFolder> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CreateFolder<'a> {
            name: &'a str,
            mime_type: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            parents: Option<Vec<&'a str>>,
        }

        let body = CreateFolder {
            name,
            mime_type: "application/vnd.google-apps.folder",
            parents: parent_id.map(|p| vec![p]),
        };

        let url = format!("{}/files?fields=id,name,parents", self.api_base);

        let response = self
            .request(reqwest::Method::POST, &url)
            .await?
            .json(&body)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let file: DriveFile = self.handle_response(response).await?;

        Ok(CloudFolder {
            id: file.id,
            name: file.name.clone(),
            path: format!("/{}", file.name),
            parent_id: file.parents.and_then(|p| p.into_iter().next()),
        })
    }

    async fn delete(&self, file_id: &str) -> Result<()> {
        let url = format!("{}/files/{}", self.api_base, file_id);

        let response = self
            .request(reqwest::Method::DELETE, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        if response.status().is_success() || response.status().as_u16() == 204 {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(IntegrationError::Api(body))
        }
    }

    async fn move_file(
        &self,
        file_id: &str,
        new_parent_id: &str,
        new_name: Option<&str>,
    ) -> Result<CloudFile> {
        // First get current parents
        let meta = self.get_file_metadata(file_id).await?;
        let old_parent = meta.parent_id.unwrap_or_else(|| "root".into());

        let url = format!(
            "{}/files/{}?addParents={}&removeParents={}&fields=id,name,mimeType,size,modifiedTime,md5Checksum,parents",
            self.api_base, file_id, new_parent_id, old_parent
        );

        let body = if let Some(name) = new_name {
            serde_json::json!({ "name": name })
        } else {
            serde_json::json!({})
        };

        let response = self
            .request(reqwest::Method::PATCH, &url)
            .await?
            .json(&body)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let file: DriveFile = self.handle_response(response).await?;
        Ok(file.to_cloud_file(format!("/{}", file.name)))
    }

    async fn get_changes(&self, cursor: Option<&str>) -> Result<(Vec<CloudFile>, Option<String>)> {
        self.get_changes_in(None, cursor).await
    }

    /// Changes since `cursor`, keeping only items under `folder_id` (default: My Drive) and
    /// giving each the same relative path [`list_files`](CloudProvider::list_files) would, by
    /// walking `parents` up to the sync root. Items outside it, trashed, or whose chain can't be
    /// resolved (unsafe names, too deep, cycles) are skipped; with several parents the first
    /// one that reaches the root wins.
    async fn get_changes_in(
        &self,
        folder_id: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<(Vec<CloudFile>, Option<String>)> {
        // Get start page token if no cursor
        let page_token = if let Some(c) = cursor {
            c.to_string()
        } else {
            let url = format!("{}/changes/startPageToken", self.api_base);
            let response = self
                .request(reqwest::Method::GET, &url)
                .await?
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct StartToken {
                start_page_token: String,
            }
            let token: StartToken = self.handle_response(response).await?;
            return Ok((vec![], Some(token.start_page_token)));
        };

        let url = format!(
            "{}/changes?pageToken={}&fields=changes(fileId,file(id,name,mimeType,size,modifiedTime,md5Checksum,parents,trashed),removed),newStartPageToken,nextPageToken",
            self.api_base,
            urlencoding::encode(&page_token)
        );

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let changes: ChangesResponse = self.handle_response(response).await?;

        let live: Vec<DriveFile> = changes
            .changes
            .into_iter()
            .filter(|c| c.removed != Some(true))
            .filter_map(|c| c.file)
            .filter(|f| f.trashed != Some(true))
            .collect();

        let mut files = Vec::new();
        if !live.is_empty() {
            let root = self.sync_root(folder_id).await?;
            let mut cache = HashMap::new();
            for f in live {
                if !is_safe_name(&f.name) {
                    tracing::warn!(
                        "google drive: skipping change with unsafe name {:?}",
                        f.name
                    );
                    continue;
                }
                match self.path_under(&f, &root, &mut cache, 0).await? {
                    Some(path) => files.push(f.to_cloud_file(path)),
                    None => tracing::debug!(
                        "google drive: skipping change {:?} ({}): not under the sync folder",
                        f.name,
                        f.id
                    ),
                }
            }
        }

        let next_cursor = changes.new_start_page_token.or(changes.next_page_token);

        Ok((files, next_cursor))
    }

    async fn get_quota(&self) -> Result<StorageQuota> {
        let url = format!("{}/about?fields=storageQuota", self.api_base);

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let about: AboutResponse = self.handle_response(response).await?;

        Ok(StorageQuota {
            used: about.storage_quota.usage.parse().unwrap_or(0),
            total: about.storage_quota.limit.and_then(|l| l.parse().ok()),
            trash: about
                .storage_quota
                .usage_in_drive_trash
                .and_then(|t| t.parse().ok()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::Json;
    use axum::extract::{Query, State};
    use axum::routing::get;
    use serde_json::{Value, json};

    use super::*;

    fn file(id: &str, name: &str) -> Value {
        json!({ "id": id, "name": name, "mimeType": "application/pdf", "size": "3" })
    }

    fn folder(id: &str, name: &str) -> Value {
        json!({ "id": id, "name": name, "mimeType": FOLDER_MIME })
    }

    /// One page of `files.list` for `(parent, pageToken)`.
    fn page(parent: &str, token: Option<&str>) -> Value {
        let (files, next) = match (parent, token) {
            ("root", None) => (
                vec![file("r1", "top.pdf"), folder("A", "A")],
                Some("root-p2"),
            ),
            ("root", Some("root-p2")) => (
                vec![
                    file("r2", "second.pdf"),
                    file("slash", "a/b.pdf"), // `/` is legal in Drive names
                    folder("dots", ".."),
                    file("r3", "top.pdf"), // same-name sibling
                ],
                None,
            ),
            ("A", None) => (vec![file("a1", "a.pdf"), folder("B", "B")], None),
            ("B", None) => (
                vec![
                    file("b1", "b.pdf"),
                    folder("A", "cycle"),    // A is also a child of B
                    file("r1", "again.pdf"), // r1 also has B as a parent
                ],
                None,
            ),
            ("stuck", _) => (vec![file("s1", "s.pdf")], Some("same")),
            (p, None) if p.starts_with("deep") => {
                let n: usize = p[4..].parse().unwrap();
                (vec![folder(&format!("deep{}", n + 1), "d")], None)
            }
            _ => (vec![], None),
        };
        json!({ "files": files, "nextPageToken": next })
    }

    /// Fake Drive API on a random local port; returns the base URL and the logged queries.
    async fn fake_drive() -> (String, Arc<Mutex<Vec<HashMap<String, String>>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new()
            .route(
                "/files",
                get(
                    |State(log): State<Arc<Mutex<Vec<HashMap<String, String>>>>>,
                     Query(q): Query<HashMap<String, String>>| async move {
                        let parent = q["q"].split('\'').nth(1).unwrap().to_string();
                        let body = page(&parent, q.get("pageToken").map(String::as_str));
                        log.lock().unwrap().push(q);
                        Json(body)
                    },
                ),
            )
            .with_state(log.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{}", addr), log)
    }

    fn drive(base: &str) -> GoogleDrive {
        let config = OAuthConfig::google_drive("id".into(), None, "http://localhost/cb".into());
        let token = OAuthToken {
            access_token: "t".into(),
            refresh_token: None,
            token_type: "Bearer".into(),
            expires_at: None,
            scope: None,
        };
        GoogleDrive::with_token(config, token).with_base_urls(base, base)
    }

    #[tokio::test]
    async fn list_files_pages_and_recurses_with_full_paths() {
        let (base, log) = fake_drive().await;
        let files = drive(&base).list_files(None).await.unwrap();
        let mut paths: Vec<(&str, &str)> = files
            .iter()
            .map(|f| (f.path.as_str(), f.id.as_str()))
            .collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                ("/A", "A"),
                ("/A/B", "B"),
                ("/A/B/b.pdf", "b1"),
                ("/A/a.pdf", "a1"),
                ("/second.pdf", "r2"), // only on page 2: paging works
                ("/top.pdf", "r1"),    // first of the duplicate names wins
            ]
        );
        assert!(files.iter().find(|f| f.id == "A").unwrap().is_folder);
        let log = log.lock().unwrap();
        // root p1, root p2, A, B — each folder entered exactly once despite the cycle.
        assert_eq!(log.len(), 4);
        for q in log.iter() {
            assert_eq!(q["pageSize"], "1000");
            assert!(q["fields"].contains("nextPageToken"));
            assert!(q["fields"].contains("parents"));
            assert!(q["q"].ends_with("in parents and trashed = false"));
        }
        assert_eq!(log[1].get("pageToken").map(String::as_str), Some("root-p2"));
    }

    #[tokio::test]
    async fn list_files_relative_to_folder_and_depth_capped() {
        let (base, log) = fake_drive().await;
        let d = drive(&base);
        let files = d.list_files(Some("A")).await.unwrap();
        let mut paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        paths.sort();
        // The cycle back to A (the listing root) is not re-entered.
        assert_eq!(paths, vec!["/B", "/B/again.pdf", "/B/b.pdf", "/a.pdf"]);

        log.lock().unwrap().clear();
        let deep = d.list_files(Some("deep0")).await.unwrap();
        assert_eq!(deep.len(), MAX_LIST_DEPTH);
        assert_eq!(log.lock().unwrap().len(), MAX_LIST_DEPTH);
        let deepest = deep.iter().map(|f| f.path.matches('/').count()).max();
        assert_eq!(deepest, Some(MAX_LIST_DEPTH));
    }

    #[tokio::test]
    async fn list_files_fails_on_repeated_page_token() {
        let (base, _) = fake_drive().await;
        let err = drive(&base).list_files(Some("stuck")).await.unwrap_err();
        assert!(
            matches!(err, IntegrationError::Api(ref m) if m.contains("repeated")),
            "{err}"
        );
    }

    fn item(id: &str, name: &str, parents: &[&str]) -> Value {
        let mime = if name.ends_with(".pdf") {
            "application/pdf"
        } else {
            FOLDER_MIME
        };
        json!({ "id": id, "name": name, "mimeType": mime, "parents": parents })
    }

    /// Folder metadata served by the fake `files.get`. `ROOTID` is My Drive's real id; `ext`
    /// isn't under it; `c1`/`c2` are parents of each other; `lost` is 404.
    fn folder_meta(id: &str) -> Option<Value> {
        Some(match id {
            "A" => item("A", "A", &["ROOTID"]),
            "B" => item("B", "B", &["ROOTID"]),
            "sub" => item("sub", "sub", &["A"]),
            "ext" => item("ext", "Shared", &[]),
            "c1" => item("c1", "c1", &["c2"]),
            "c2" => item("c2", "c2", &["c1"]),
            "bad" => item("bad", "..", &["ROOTID"]),
            _ => return None,
        })
    }

    /// The change page for `pageToken`.
    fn changes_page(token: &str) -> Vec<Value> {
        let change = |f: Value| json!({ "fileId": f["id"], "file": f });
        match token {
            "all" => vec![
                change(item("top", "f.pdf", &["ROOTID"])),
                change(item("nested", "f.pdf", &["A"])),
                change(item("b", "f.pdf", &["B"])),
                change(item("deep", "f.pdf", &["sub"])),
                change(item("outside", "f.pdf", &["ext"])),
                change(item("multi", "m.pdf", &["ext", "A"])),
                change(item("cyc", "f.pdf", &["c1"])),
                change(item("orphan", "f.pdf", &["lost"])),
                change(item("unsafe", "f.pdf", &["bad"])),
                change(
                    json!({ "id": "t", "name": "t.pdf", "mimeType": "application/pdf",
                               "parents": ["ROOTID"], "trashed": true }),
                ),
                json!({ "fileId": "gone", "removed": true }),
            ],
            "nested" => vec![
                change(item("nested", "f.pdf", &["A"])),
                change(item("outside", "f.pdf", &["ext"])),
            ],
            "dl" => vec![
                change(item("missing", "missing.pdf", &["ROOTID"])),
                change(item("gdoc", "doc.pdf", &["ROOTID"])),
                change(item("ok", "ok.pdf", &["ROOTID"])),
            ],
            "flaky" => vec![
                change(item("ok", "ok.pdf", &["ROOTID"])),
                change(item("flaky", "flaky.pdf", &["ROOTID"])),
            ],
            _ => vec![],
        }
    }

    type Log = Arc<Mutex<Vec<String>>>;

    /// Fake Drive changes / files.get / download endpoints; logs every `files.get` id.
    async fn fake_changes() -> (String, Log) {
        use axum::extract::Path;
        use axum::http::StatusCode;
        use axum::response::{IntoResponse, Response};

        async fn get_file(
            State(log): State<Log>,
            Path(id): Path<String>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Response {
            log.lock().unwrap().push(id.clone());
            if q.get("alt").map(String::as_str) == Some("media") {
                return match id.as_str() {
                    "missing" => StatusCode::NOT_FOUND.into_response(),
                    "gdoc" => (
                        StatusCode::FORBIDDEN,
                        r#"{"error":{"errors":[{"reason":"fileNotDownloadable"}]}}"#,
                    )
                        .into_response(),
                    "flaky" => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                    _ => id.into_bytes().into_response(),
                };
            }
            if id == "root" {
                return Json(json!({ "id": "ROOTID" })).into_response();
            }
            match folder_meta(&id) {
                Some(v) => Json(v).into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }

        let log = Log::default();
        let app = axum::Router::new()
            .route(
                "/changes",
                get(|Query(q): Query<HashMap<String, String>>| async move {
                    Json(json!({
                        "changes": changes_page(&q["pageToken"]),
                        "newStartPageToken": "next",
                    }))
                }),
            )
            .route("/files/{id}", get(get_file))
            .with_state(log.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{}", addr), log)
    }

    fn paths(files: &[CloudFile]) -> Vec<(&str, &str)> {
        let mut v: Vec<_> = files
            .iter()
            .map(|f| (f.path.as_str(), f.id.as_str()))
            .collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn changes_get_full_paths_under_my_drive() {
        let (base, log) = fake_changes().await;
        let (files, cursor) = drive(&base).get_changes(Some("all")).await.unwrap();
        assert_eq!(cursor.as_deref(), Some("next"));
        // Same-name files in different folders keep distinct paths; items outside the root,
        // in a parent cycle, under a missing or unsafe folder, trashed or removed are dropped.
        assert_eq!(
            paths(&files),
            vec![
                ("/A/f.pdf", "nested"),
                ("/A/m.pdf", "multi"), // first parent (`ext`) doesn't reach root; `A` does
                ("/A/sub/f.pdf", "deep"),
                ("/B/f.pdf", "b"),
                ("/f.pdf", "top"),
            ]
        );
        // Every folder is fetched at most once per call.
        let mut log = log.lock().unwrap().clone();
        let n = log.len();
        log.sort();
        log.dedup();
        assert_eq!(n, log.len(), "folder metadata not cached: {log:?}");
    }

    #[tokio::test]
    async fn changes_relative_to_sync_folder() {
        let (base, log) = fake_changes().await;
        let (files, _) = drive(&base)
            .get_changes_in(Some("A"), Some("all"))
            .await
            .unwrap();
        assert_eq!(
            paths(&files),
            vec![
                ("/f.pdf", "nested"),
                ("/m.pdf", "multi"),
                ("/sub/f.pdf", "deep")
            ]
        );
        // A configured folder id needs no lookup of My Drive's id.
        assert!(!log.lock().unwrap().contains(&"root".to_string()));
    }

    fn sync_state(cursor: &str) -> crate::integrations::sync::SyncState {
        crate::integrations::sync::SyncState {
            cursor: Some(cursor.into()),
            ..Default::default()
        }
    }

    fn sync_for(
        base: &str,
        root: &std::path::Path,
        cursor: &str,
    ) -> crate::integrations::sync::CloudSync<GoogleDrive> {
        use crate::integrations::sync::{CloudSync, SyncConfig, SyncDirection};
        let config = SyncConfig {
            local_path: root.to_path_buf(),
            direction: SyncDirection::Download,
            ..Default::default()
        };
        CloudSync::with_state(drive(base), config, sync_state(cursor))
    }

    /// A delta change to `/A/f.pdf` lands there, never on the unrelated root-level `/f.pdf`,
    /// and a change outside the sync folder writes nothing.
    #[tokio::test]
    async fn delta_sync_applies_nested_change_at_its_path() {
        let (base, _) = fake_changes().await;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.pdf"), "keep").unwrap();
        let mut sync = sync_for(&base, dir.path(), "nested");
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.downloaded, 1);
        assert_eq!(
            std::fs::read(dir.path().join("A/f.pdf")).unwrap(),
            b"nested"
        );
        assert_eq!(std::fs::read(dir.path().join("f.pdf")).unwrap(), b"keep");
        assert_eq!(sync.state().cursor.as_deref(), Some("next"));
    }

    /// Deleted files (404) and Docs editor files (403 fileNotDownloadable) fail permanently,
    /// so the cursor advances; a 5xx is transient and holds it.
    #[tokio::test]
    async fn delta_sync_download_errors_permanent_vs_transient() {
        let (base, _) = fake_changes().await;
        let dir = tempfile::tempdir().unwrap();
        let mut sync = sync_for(&base, dir.path(), "dl");
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 2), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("next"));
        assert!(dir.path().join("ok.pdf").exists());

        let dir = tempfile::tempdir().unwrap();
        let mut sync = sync_for(&base, dir.path(), "flaky");
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 1), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("flaky"));
    }

    #[tokio::test]
    async fn download_error_mapping() {
        let (base, _) = fake_changes().await;
        let d = drive(&base);
        let err = d.download_file("missing").await.unwrap_err();
        assert!(matches!(err, IntegrationError::NotFound(_)), "{err}");
        let err = d.download_file("gdoc").await.unwrap_err();
        assert!(matches!(err, IntegrationError::NotDownloadable(_)), "{err}");
        assert!(err.is_permanent());
        let err = d.download_file("flaky").await.unwrap_err();
        assert!(!err.is_permanent(), "{err}");
        assert_eq!(d.download_file("ok").await.unwrap(), b"ok");
    }

    #[test]
    fn query_values_are_escaped() {
        assert_eq!(escape_query(r"it's\x"), r"it\'s\\x");
    }
}
