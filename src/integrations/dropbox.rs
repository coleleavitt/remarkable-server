//! Dropbox integration
//!
//! Full read/write access via Dropbox API v2.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;

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

const API_BASE: &str = "https://api.dropboxapi.com/2";
const CONTENT_BASE: &str = "https://content.dropboxapi.com/2";

/// Make a JSON `Dropbox-API-Arg` value header-safe: HTTP header values must be visible ASCII,
/// so Dropbox requires DEL and every non-ASCII character escaped as `\uXXXX` (UTF-16). Without
/// this, a path like `/Notes/café.pdf` makes the request fail before it is sent.
fn header_safe_json(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    for c in json.chars() {
        if c.is_ascii() && c != '\x7f' {
            out.push(c);
        } else {
            let mut buf = [0u16; 2];
            for unit in c.encode_utf16(&mut buf) {
                out.push_str(&format!("\\u{:04x}", unit));
            }
        }
    }
    out
}

/// The API spelling of a sync folder (a path or `id:`): Dropbox names its root `""` and
/// rejects `"/"` or a trailing slash.
fn api_path(folder: Option<&str>) -> &str {
    folder.unwrap_or("").trim_end_matches('/')
}

/// `error_summary` of a Dropbox error body (`"reset/.."`, `"path/not_found/.."`).
fn error_summary(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    v.get("error_summary")?.as_str().map(str::to_owned)
}

/// Argument for `files/list_folder` and `files/list_folder/get_latest_cursor`. Both use
/// [`ListFolderArg::recursive`], so a change cursor covers exactly what a full listing does.
#[derive(Serialize)]
struct ListFolderArg<'a> {
    path: &'a str,
    recursive: bool,
    include_mounted_folders: bool,
    include_non_downloadable_files: bool,
}

impl<'a> ListFolderArg<'a> {
    fn recursive(path: &'a str) -> Self {
        Self {
            path,
            recursive: true,
            include_mounted_folders: true,
            include_non_downloadable_files: false,
        }
    }
}

#[derive(Serialize)]
struct CursorArg<'a> {
    cursor: &'a str,
}

/// Dropbox provider
pub struct Dropbox {
    config: OAuthConfig,
    token: Option<OAuthToken>,
    client: Client,
    api_base: String,
    content_base: String,
}

impl Dropbox {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            token: None,
            client: crate::integrations::http_client(),
            api_base: API_BASE.into(),
            content_base: CONTENT_BASE.into(),
        }
    }

    pub fn with_token(config: OAuthConfig, token: OAuthToken) -> Self {
        Self {
            config,
            token: Some(token),
            client: crate::integrations::http_client(),
            api_base: API_BASE.into(),
            content_base: CONTENT_BASE.into(),
        }
    }

    /// Point the provider at different API / content endpoints (tests, proxies).
    pub fn with_base_urls(mut self, api_base: &str, content_base: &str) -> Self {
        self.api_base = api_base.trim_end_matches('/').into();
        self.content_base = content_base.trim_end_matches('/').into();
        self
    }

    fn access_token(&self) -> Result<&str> {
        self.token
            .as_ref()
            .map(|t| t.access_token.as_str())
            .ok_or(IntegrationError::NotConfigured)
    }

    async fn api_request<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &T,
    ) -> Result<R> {
        let token = self.access_token()?;
        let url = format!("{}/{}", self.api_base, endpoint);

        let response = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        self.handle_response(response).await
    }

    async fn handle_response<R: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
    ) -> Result<R> {
        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| IntegrationError::Serialization(e.to_string()))
        } else {
            Err(response_error(response).await)
        }
    }

    /// Every entry from `page` on, following `has_more` through `files/list_folder/continue`,
    /// plus the cursor after the last page.
    async fn drain(&self, mut page: ListFolderResponse) -> Result<(Vec<DropboxEntry>, String)> {
        let mut entries = Vec::new();
        let mut used = HashSet::new();
        loop {
            entries.extend(page.entries);
            if !page.has_more {
                return Ok((entries, page.cursor));
            }
            // A cursor handed back again would loop forever; fail rather than return a
            // partial list (a partial list makes sync re-upload everything it didn't see).
            if !used.insert(page.cursor.clone()) {
                return Err(IntegrationError::Api(format!(
                    "Dropbox returned repeated cursor {:?}",
                    page.cursor
                )));
            }
            page = self
                .api_request(
                    "files/list_folder/continue",
                    &CursorArg {
                        cursor: &page.cursor,
                    },
                )
                .await?;
        }
    }

    /// `files/get_metadata` of `path` (a path, in any case, or an `id:`).
    async fn metadata(&self, path: &str) -> Result<DropboxEntry> {
        #[derive(Serialize)]
        struct GetMetadataArg<'a> {
            path: &'a str,
        }
        let meta: MetadataResponse = self
            .api_request("files/get_metadata", &GetMetadataArg { path })
            .await?;
        Ok(meta.entry)
    }

    /// `path_lower` of the sync folder (`""` for the whole Dropbox), the prefix of every
    /// `path_lower` below it. Asked of Dropbox rather than lowercased here: its case folding is
    /// its own, and the folder may be given as an `id:`.
    async fn root_lower(&self, path: &str) -> Result<String> {
        if path.is_empty() {
            return Ok(String::new());
        }
        let meta = self.metadata(path).await?;
        match (meta.tag.as_str(), meta.path_lower) {
            ("folder", Some(lower)) => Ok(lower),
            _ => Err(IntegrationError::Api(format!(
                "sync folder {:?} is not a folder",
                path
            ))),
        }
    }

    /// The name, in its own casing, of every folder between the sync folder (`root`, a
    /// `path_lower`) and each of `entries`, keyed by `path_lower`. Folders listed in `entries`
    /// give their own `name`; any other is asked of `files/get_metadata`, once. Full listings
    /// and change feeds both take folder casing from here, so a file maps to the same local
    /// directory however it was seen. (`path_display` is no substitute: Dropbox only promises
    /// the casing of its last component.) A folder that's gone, or is no longer a folder, is
    /// left out, and [`relative_path`] then skips everything under it.
    async fn folder_names(
        &self,
        entries: &[&DropboxEntry],
        root: &str,
    ) -> Result<HashMap<String, String>> {
        let mut names: HashMap<String, String> = entries
            .iter()
            .filter(|e| e.tag == "folder")
            .filter_map(|e| Some((e.path_lower.clone()?, e.name.clone())))
            .collect();
        // Sorted, so lookups happen in a stable order.
        let mut missing = BTreeSet::new();
        for e in entries {
            let Some(parts) = e.path_lower.as_deref().and_then(|l| below(l, root)) else {
                continue;
            };
            if parts.len() > MAX_LIST_DEPTH {
                continue; // skipped by `relative_path` anyway
            }
            let mut key = root.to_string();
            for part in &parts[..parts.len() - 1] {
                key.push('/');
                key.push_str(part);
                if !names.contains_key(&key) {
                    missing.insert(key.clone());
                }
            }
        }
        for key in missing {
            match self.metadata(&key).await {
                Ok(meta) if meta.tag == "folder" => {
                    names.insert(key, meta.name);
                }
                Ok(_) | Err(IntegrationError::NotFound(_)) => {
                    tracing::debug!("dropbox: folder {:?} is gone; skipping what's in it", key);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(names)
    }
}

/// The components of `lower` (a `path_lower`) below `root` (`/Sub/b.pdf` under `/notes` gives
/// `["sub", "b.pdf"]`); `None` for `root` itself and anything outside it.
fn below<'a>(lower: &'a str, root: &str) -> Option<Vec<&'a str>> {
    let rest = lower
        .strip_prefix(root)?
        .strip_prefix('/')
        .filter(|r| !r.is_empty())?;
    Some(rest.split('/').collect())
}

/// Map a non-success Dropbox response to an error. A missing path (409 `path/not_found`, or a
/// plain 404) is [`IntegrationError::NotFound`] and content that can't be downloaded (409
/// `unsupported_file`, e.g. Paper docs, or `restricted_content`, e.g. a legal takedown) is
/// [`IntegrationError::NotDownloadable`], both permanent; auth, rate limits and 5xx stay
/// retryable.
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
        404 => IntegrationError::NotFound("Path not found".into()),
        409 => {
            let body = response.text().await.unwrap_or_default();
            if body.contains("path/not_found") {
                IntegrationError::NotFound("Path not found".into())
            } else if body.contains("unsupported_file") || body.contains("restricted_content") {
                IntegrationError::NotDownloadable(body)
            } else if body.contains("insufficient_space") {
                IntegrationError::QuotaExceeded
            } else if error_summary(&body).is_some_and(|s| s.starts_with("reset/")) {
                // `list_folder/continue`: the cursor was invalidated.
                IntegrationError::ResyncRequired(body)
            } else {
                IntegrationError::Conflict(body)
            }
        }
        _ => {
            let body = response.text().await.unwrap_or_default();
            IntegrationError::Api(format!("{}: {}", status, body))
        }
    }
}

/// Dropbox file metadata
#[derive(Debug, Deserialize)]
struct DropboxEntry {
    /// `file`, `folder` or (in change feeds) `deleted`.
    #[serde(rename = ".tag")]
    tag: String,
    id: Option<String>,
    name: String,
    path_lower: Option<String>,
    path_display: Option<String>,
    #[serde(default)]
    size: u64,
    server_modified: Option<String>,
    content_hash: Option<String>,
}

impl DropboxEntry {
    fn to_cloud_file(&self) -> CloudFile {
        self.to_cloud_file_at(self.path_display.clone().unwrap_or_default())
    }

    /// As a [`CloudFile`] at `path` (relative to the sync folder). A `deleted` entry has no id
    /// or metadata, so it becomes a deletion addressed by its `path_lower`.
    fn to_cloud_file_at(&self, path: String) -> CloudFile {
        if self.tag == "deleted" {
            return CloudFile {
                id: self.path_lower.clone().unwrap_or_default(),
                name: self.name.clone(),
                mime_type: None,
                size: 0,
                modified_at: 0,
                content_hash: None,
                parent_id: None,
                is_folder: false, // Dropbox doesn't say what was deleted
                path,
                deleted: true,
            };
        }
        CloudFile {
            id: self.id.clone().unwrap_or_default(),
            name: self.name.clone(),
            mime_type: None, // Dropbox doesn't provide MIME types
            size: self.size,
            modified_at: self
                .server_modified
                .as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|dt| dt.timestamp())
                .unwrap_or(0),
            content_hash: self.content_hash.clone(),
            parent_id: None, // Would need to parse from path
            is_folder: self.tag == "folder",
            path,
            deleted: false,
        }
    }

    fn to_cloud_folder(&self) -> CloudFolder {
        CloudFolder {
            id: self.id.clone().unwrap_or_default(),
            name: self.name.clone(),
            path: self.path_display.clone().unwrap_or_default(),
            parent_id: None,
        }
    }
}

/// Replay `list_folder` entries in order, as Dropbox asks clients to, into the state they
/// describe: a later entry for a path replaces the earlier one, a file replaces anything that
/// was below the path (it may have been a folder), and a `deleted` entry removes the path and
/// everything below it. A listing is paged over time, so an item edited, moved or deleted while
/// it pages shows up again in a later page. What remains comes back in the order it was last
/// written; `deleted` entries remain (as deletions) only with `keep_deletions`, which a change
/// feed needs because the caller may hold what they delete.
fn replay(entries: &[DropboxEntry], keep_deletions: bool) -> Vec<&DropboxEntry> {
    let mut state: BTreeMap<&str, (usize, &DropboxEntry)> = BTreeMap::new();
    for (i, e) in entries.iter().enumerate() {
        let Some(lower) = e.path_lower.as_deref() else {
            continue;
        };
        if e.tag != "folder" {
            let below = format!("{}/", lower);
            let gone: Vec<&str> = state
                .range::<str, _>((Bound::Included(below.as_str()), Bound::Unbounded))
                .map(|(k, _)| *k)
                .take_while(|k| k.starts_with(&below))
                .collect();
            for k in gone {
                state.remove(k);
            }
        }
        if e.tag == "deleted" && !keep_deletions {
            state.remove(lower);
        } else {
            state.insert(lower, (i, e));
        }
    }
    let mut live: Vec<(usize, &DropboxEntry)> = state.into_values().collect();
    live.sort_unstable_by_key(|(i, _)| *i);
    live.into_iter().map(|(_, e)| e).collect()
}

/// `e`'s path relative to the sync folder whose `path_lower` is `root` (`/Sub/b.pdf`), the
/// same shape as the local scan. `None` for the folder itself, anything outside it, paths
/// deeper than [`MAX_LIST_DEPTH`], a component that isn't a safe single segment, and a folder
/// on the way that `folders` doesn't name (gone).
///
/// Membership is decided on `path_lower` (Dropbox is case-insensitive). Casing comes from `name`
/// for the last component and from `folders` (see [`Dropbox::folder_names`]) for the folders
/// on the way.
fn relative_path(
    e: &DropboxEntry,
    root: &str,
    folders: &HashMap<String, String>,
) -> Option<String> {
    let lower = e.path_lower.as_deref()?;
    let parts = below(lower, root)?;
    if parts.len() > MAX_LIST_DEPTH {
        tracing::warn!("dropbox: skipping {:?}: too deep", lower);
        return None;
    }

    let mut key = root.to_string();
    let mut path = String::new();
    for (i, part) in parts.iter().enumerate() {
        key.push('/');
        key.push_str(part);
        let name = if i + 1 == parts.len() {
            e.name.as_str()
        } else if let Some(name) = folders.get(&key) {
            name.as_str()
        } else {
            tracing::debug!("dropbox: skipping {:?}: folder {:?} is gone", lower, key);
            return None;
        };
        if !is_safe_name(name) {
            tracing::warn!("dropbox: skipping {:?}: unsafe name {:?}", lower, name);
            return None;
        }
        path.push('/');
        path.push_str(name);
    }
    Some(path)
}

/// List folder response
#[derive(Debug, Deserialize)]
struct ListFolderResponse {
    entries: Vec<DropboxEntry>,
    cursor: String,
    has_more: bool,
}

/// Get metadata response
#[derive(Debug, Deserialize)]
struct MetadataResponse {
    #[serde(flatten)]
    entry: DropboxEntry,
}

/// Upload response
#[derive(Debug, Deserialize)]
struct UploadResponse {
    #[serde(flatten)]
    entry: DropboxEntry,
}

/// Space usage response
#[derive(Debug, Deserialize)]
struct SpaceUsageResponse {
    used: u64,
    allocation: SpaceAllocation,
}

#[derive(Debug, Deserialize)]
struct SpaceAllocation {
    #[serde(rename = ".tag")]
    #[allow(dead_code)]
    tag: String,
    allocated: Option<u64>,
}

/// List folder continue changes response  
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ListFolderLongpollResponse {
    changes: bool,
    backoff: Option<u64>,
}

#[async_trait]
impl CloudProvider for Dropbox {
    fn provider_type(&self) -> ProviderType {
        ProviderType::Dropbox
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

    /// Everything under `folder_id` (a path or `id:`; default: the whole Dropbox) in one
    /// recursive `list_folder`, paged with `list_folder/continue`. Paths are relative to that
    /// folder (`/Sub/dir/file.pdf`), matching the local scan so nested files aren't seen as
    /// missing and re-uploaded every sync. The pages are [replayed](replay) in order, so an
    /// item changed or deleted while they were fetched is listed as it ended up, once. Entries
    /// that can't be given a safe relative path (see [`relative_path`]) are skipped.
    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        let path = api_path(folder_id);
        let root = self.root_lower(path).await?;
        let first = self
            .api_request("files/list_folder", &ListFolderArg::recursive(path))
            .await?;
        let (entries, _) = self.drain(first).await?;
        let entries = replay(&entries, false);

        let folders = self.folder_names(&entries, &root).await?;
        let mut seen_paths = HashSet::new();
        let mut out = Vec::new();
        for e in entries {
            let Some(rel) = relative_path(e, &root, &folders) else {
                continue;
            };
            if !seen_paths.insert(rel.clone()) {
                tracing::warn!("dropbox: duplicate path {:?}, keeping the first", rel);
                continue;
            }
            out.push(e.to_cloud_file_at(rel));
        }
        Ok(out)
    }

    async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
        let first = self
            .api_request("files/list_folder", &ListFolderArg::recursive(""))
            .await?;
        let (entries, _) = self.drain(first).await?;
        Ok(entries
            .iter()
            .filter(|e| e.tag == "folder")
            .map(|e| e.to_cloud_folder())
            .collect())
    }

    async fn get_file_metadata(&self, file_id: &str) -> Result<CloudFile> {
        Ok(self.metadata(file_id).await?.to_cloud_file())
    }

    async fn download_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let token = self.access_token()?;

        #[derive(Serialize)]
        struct DownloadArg<'a> {
            path: &'a str,
        }

        let arg = serde_json::to_string(&DownloadArg { path: file_id })
            .map_err(|e| IntegrationError::Serialization(e.to_string()))?;

        let response = self
            .client
            .post(format!("{}/files/download", self.content_base))
            .bearer_auth(token)
            .header("Dropbox-API-Arg", header_safe_json(&arg))
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
        _mime_type: Option<&str>,
    ) -> Result<CloudFile> {
        let token = self.access_token()?;

        // Same folder spelling as listings: `/` or a trailing slash must not yield `//name`.
        let path = format!("{}/{}", api_path(parent_id), name);

        #[derive(Serialize)]
        struct UploadArg {
            path: String,
            mode: String,
            autorename: bool,
            mute: bool,
            strict_conflict: bool,
        }

        let arg = serde_json::to_string(&UploadArg {
            path,
            mode: "overwrite".into(),
            autorename: false,
            mute: false,
            strict_conflict: false,
        })
        .map_err(|e| IntegrationError::Serialization(e.to_string()))?;

        let response = self
            .client
            .post(format!("{}/files/upload", self.content_base))
            .bearer_auth(token)
            .header("Dropbox-API-Arg", header_safe_json(&arg))
            .header("Content-Type", "application/octet-stream")
            .body(content.to_vec())
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let upload: UploadResponse = self.handle_response(response).await?;
        Ok(upload.entry.to_cloud_file())
    }

    async fn create_folder(&self, parent_id: Option<&str>, name: &str) -> Result<CloudFolder> {
        // Same folder spelling as listings: `/` or a trailing slash must not yield `//name`.
        let path = format!("{}/{}", api_path(parent_id), name);

        #[derive(Serialize)]
        struct CreateFolderArg {
            path: String,
            autorename: bool,
        }

        #[derive(Deserialize)]
        struct CreateFolderResponse {
            #[allow(dead_code)]
            metadata: DropboxEntry,
        }

        let response: CreateFolderResponse = self
            .api_request(
                "files/create_folder_v2",
                &CreateFolderArg {
                    path,
                    autorename: false,
                },
            )
            .await?;

        Ok(response.metadata.to_cloud_folder())
    }

    async fn delete(&self, file_id: &str) -> Result<()> {
        #[derive(Serialize)]
        struct DeleteArg<'a> {
            path: &'a str,
        }

        #[derive(Deserialize)]
        struct DeleteResponse {
            #[allow(dead_code)]
            metadata: DropboxEntry,
        }

        let _: DeleteResponse = self
            .api_request("files/delete_v2", &DeleteArg { path: file_id })
            .await?;

        Ok(())
    }

    async fn move_file(
        &self,
        file_id: &str,
        new_parent_id: &str,
        new_name: Option<&str>,
    ) -> Result<CloudFile> {
        // Get current metadata to determine name
        let current = self.get_file_metadata(file_id).await?;
        let name = new_name.unwrap_or(&current.name);

        let to_path = format!("{}/{}", new_parent_id, name);

        #[derive(Serialize)]
        struct MoveArg<'a> {
            from_path: &'a str,
            to_path: String,
            autorename: bool,
            allow_ownership_transfer: bool,
        }

        #[derive(Deserialize)]
        struct MoveResponse {
            #[allow(dead_code)]
            metadata: DropboxEntry,
        }

        let response: MoveResponse = self
            .api_request(
                "files/move_v2",
                &MoveArg {
                    from_path: file_id,
                    to_path,
                    autorename: false,
                    allow_ownership_transfer: false,
                },
            )
            .await?;

        Ok(response.metadata.to_cloud_file())
    }

    async fn get_changes(&self, cursor: Option<&str>) -> Result<(Vec<CloudFile>, Option<String>)> {
        self.get_changes_in(None, cursor).await
    }

    /// Changes under `folder_id` since `cursor`, all pages of `list_folder/continue`, pathed like
    /// [`list_files`](CloudProvider::list_files). Without a cursor, returns no changes and
    /// `get_latest_cursor` for the folder (recursive, so the cursor itself is scoped to it).
    /// The feed is a log and is [replayed](replay): each path's last entry is kept (deleted
    /// then re-created gives just the file), a deleted folder drops what was listed under it
    /// earlier, and the result is in feed order; `deleted` entries become deletions. An
    /// invalidated cursor (409 `reset`) is [`IntegrationError::ResyncRequired`].
    async fn get_changes_in(
        &self,
        folder_id: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<(Vec<CloudFile>, Option<String>)> {
        let path = api_path(folder_id);
        let Some(cursor) = cursor else {
            #[derive(Deserialize)]
            struct LatestCursor {
                cursor: String,
            }
            let latest: LatestCursor = self
                .api_request(
                    "files/list_folder/get_latest_cursor",
                    &ListFolderArg::recursive(path),
                )
                .await?;
            return Ok((vec![], Some(latest.cursor)));
        };

        let first = self
            .api_request("files/list_folder/continue", &CursorArg { cursor })
            .await?;
        let (entries, next) = self.drain(first).await?;
        if entries.is_empty() {
            return Ok((vec![], Some(next)));
        }

        let root = self.root_lower(path).await?;
        let entries = replay(&entries, true);
        let folders = self.folder_names(&entries, &root).await?;
        let mut seen_paths = HashSet::new();
        let mut out = Vec::new();
        for e in entries {
            let Some(rel) = relative_path(e, &root, &folders) else {
                tracing::debug!(
                    "dropbox: skipping change {:?}: not under the sync folder",
                    e.path_lower
                );
                continue;
            };
            if !seen_paths.insert(rel.clone()) {
                tracing::warn!(
                    "dropbox: duplicate change path {:?}, keeping the first",
                    rel
                );
                continue;
            }
            out.push(e.to_cloud_file_at(rel));
        }
        Ok((out, Some(next)))
    }

    async fn get_quota(&self) -> Result<StorageQuota> {
        #[derive(Serialize)]
        struct Null;

        let usage: SpaceUsageResponse = self.api_request("users/get_space_usage", &Null).await?;

        Ok(StorageQuota {
            used: usage.used,
            total: usage.allocation.allocated,
            trash: None, // Dropbox doesn't report trash size separately
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;

    use super::*;

    /// Deleted paths (409 `path/not_found`), Paper docs (409 `unsupported_file`) and
    /// restricted content fail permanently; auth and 5xx stay retryable.
    #[tokio::test]
    async fn download_error_mapping() {
        let app = axum::Router::new().route(
            "/files/download",
            axum::routing::post(|headers: HeaderMap| async move {
                let arg: serde_json::Value =
                    serde_json::from_str(headers["Dropbox-API-Arg"].to_str().unwrap()).unwrap();
                match arg["path"].as_str().unwrap() {
                    "/gone" => (
                        StatusCode::CONFLICT,
                        r#"{"error_summary":"path/not_found/.."}"#,
                    )
                        .into_response(),
                    "/paper" => (
                        StatusCode::CONFLICT,
                        r#"{"error_summary":"unsupported_file/.."}"#,
                    )
                        .into_response(),
                    "/restricted" => (
                        StatusCode::CONFLICT,
                        r#"{"error_summary":"path/restricted_content/.."}"#,
                    )
                        .into_response(),
                    "/flaky" => StatusCode::BAD_GATEWAY.into_response(),
                    "/auth" => StatusCode::UNAUTHORIZED.into_response(),
                    _ => "content".into_response(),
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = OAuthConfig::dropbox("id".into(), None, "http://localhost/cb".into());
        let token = OAuthToken {
            access_token: "t".into(),
            refresh_token: None,
            token_type: "Bearer".into(),
            expires_at: None,
            scope: None,
        };
        let d = Dropbox::with_token(config, token).with_base_urls(&base, &base);

        let err = d.download_file("/gone").await.unwrap_err();
        assert!(matches!(err, IntegrationError::NotFound(_)), "{err}");
        for path in ["/paper", "/restricted"] {
            let err = d.download_file(path).await.unwrap_err();
            assert!(
                matches!(err, IntegrationError::NotDownloadable(_)),
                "{path}: {err}"
            );
        }
        for path in ["/flaky", "/auth"] {
            let err = d.download_file(path).await.unwrap_err();
            assert!(!err.is_permanent(), "{path}: {err}");
        }
        assert_eq!(d.download_file("/ok").await.unwrap(), b"content");
    }

    mod listing {
        use std::sync::{Arc, Mutex};

        use axum::Json;
        use axum::extract::State;
        use axum::routing::post;
        use serde_json::{Value, json};

        use super::*;
        use crate::integrations::sync::{CloudSync, SyncConfig, SyncDirection, SyncState};

        fn entry(tag: &str, display: &str) -> Value {
            let name = display.rsplit('/').next().unwrap();
            let mut e = json!({
                ".tag": tag,
                "name": name,
                "path_lower": display.to_lowercase(),
                "path_display": display,
            });
            if tag != "deleted" {
                e["id"] = json!(format!("id:{}", display.to_lowercase()));
            }
            if tag == "file" {
                e["size"] = json!(3);
                e["server_modified"] = json!("2024-01-02T03:04:05Z");
                e["content_hash"] = json!("h");
            }
            e
        }
        fn file(display: &str) -> Value {
            entry("file", display)
        }
        fn folder(display: &str) -> Value {
            entry("folder", display)
        }
        fn deleted(display: &str) -> Value {
            entry("deleted", display)
        }
        /// `file(display)` after an edit: newer time and hash.
        fn edited(display: &str) -> Value {
            let mut e = file(display);
            e["server_modified"] = json!("2024-05-06T07:08:09Z");
            e["content_hash"] = json!("h2");
            e
        }

        fn page(entries: Vec<Value>, cursor: &str, has_more: bool) -> Value {
            json!({ "entries": entries, "cursor": cursor, "has_more": has_more })
        }

        /// `list_folder` first pages, by lowercased `path`.
        fn first_page(path: &str) -> Value {
            match path {
                "/notes" | "id:notes" => page(
                    vec![
                        folder("/Notes"), // recursive listings include the folder itself
                        file("/Notes/a.pdf"),
                        // `path_display` casing is only reliable for the last component.
                        json!({ ".tag": "file", "id": "id:b", "name": "b.pdf",
                                "path_lower": "/notes/sub/b.pdf",
                                "path_display": "/notes/sub/b.pdf" }),
                        file("/Other/x.pdf"),  // outside the folder
                        file("/Notes2/y.pdf"), // shares the prefix, still outside
                    ],
                    "L1",
                    true,
                ),
                "" => page(
                    vec![file("/Top.pdf"), folder("/Notes"), file("/Notes/a.pdf")],
                    "R1",
                    false,
                ),
                "/deep" => {
                    let d = "/d".repeat(MAX_LIST_DEPTH - 1);
                    page(
                        vec![
                            file(&format!("/Deep{}/ok.pdf", d)),
                            file(&format!("/Deep{}/d/too-deep.pdf", d)),
                        ],
                        "D1",
                        false,
                    )
                }
                "/stuck" => page(vec![file("/Stuck/s.pdf")], "stuck", true),
                // Continued in `M1`, where the tree changes while the listing pages.
                "/moving" => page(
                    vec![
                        folder("/Moving"),
                        file("/Moving/x.pdf"),
                        folder("/Moving/Sub"),
                        file("/Moving/Sub/a.pdf"),
                        file("/Moving/edit.pdf"),
                        folder("/Moving/Was-a-folder"),
                        file("/Moving/Was-a-folder/in.pdf"),
                    ],
                    "M1",
                    true,
                ),
                _ => page(vec![], "none", false),
            }
        }

        /// `list_folder/continue` pages, by cursor.
        fn next_page(cursor: &str) -> Option<Value> {
            Some(match cursor {
                "L1" => page(
                    vec![
                        folder("/Notes/Sub"),
                        file("/Notes/Sub/Deeper/c.pdf"), // its folder isn't listed
                        file(r"/Notes/a\b.pdf"),
                        folder(r"/Notes/we\ird"),
                        file(r"/Notes/we\ird/z.pdf"),
                        file("/Notes/a.pdf"), // listed twice
                        deleted("/Notes/gone.pdf"),
                    ],
                    "L2",
                    false,
                ),
                "stuck" => page(vec![], "stuck", true),
                "M1" => page(
                    vec![
                        deleted("/Moving/x.pdf"),
                        // Sub renamed to Sub2: its old entries go, the new ones come.
                        deleted("/Moving/Sub"),
                        folder("/Moving/Sub2"),
                        file("/Moving/Sub2/a.pdf"),
                        edited("/Moving/edit.pdf"),
                        file("/Moving/Was-a-folder"), // a file now, where the folder was
                    ],
                    "M2",
                    false,
                ),
                "CD" => page(
                    vec![
                        file("/Notes/Sub/a.pdf"),
                        folder("/Notes/Dir"),
                        file("/Notes/Dir/in.pdf"),
                        deleted("/Notes/Sub"),
                        file("/Notes/Dir"),
                    ],
                    "CD2",
                    false,
                ),
                "C0" => page(
                    vec![
                        file("/Notes/new.pdf"),
                        deleted("/Notes/Sub/old.pdf"),
                        deleted("/Notes/re.pdf"),
                        file("/Notes/later-gone.pdf"),
                        file("/Other/x.pdf"),
                    ],
                    "C1",
                    true,
                ),
                "C1" => page(
                    vec![
                        file("/Notes/re.pdf"), // re-created after the deletion above
                        deleted("/Notes/later-gone.pdf"),
                        folder("/Notes/Sub"),
                    ],
                    "C2",
                    false,
                ),
                "RC" => page(vec![file("/Top.pdf")], "RC2", false),
                // Files changed on their own: their folders' entries aren't in the batch.
                "CASE" => page(
                    vec![
                        // `path_display` casing is only reliable for the last component.
                        json!({ ".tag": "file", "id": "id:b", "name": "b.pdf",
                                "path_lower": "/notes/sub/b.pdf",
                                "path_display": "/notes/sub/b.pdf" }),
                        json!({ ".tag": "file", "id": "id:c2", "name": "c2.pdf",
                                "path_lower": "/notes/sub/c2.pdf",
                                "path_display": "/NOTES/SUB/c2.pdf" }),
                        file("/Notes/Gone/x.pdf"), // its folder was deleted since
                    ],
                    "CASE2",
                    false,
                ),
                "empty" => page(vec![], "empty2", false),
                _ => return None,
            })
        }

        type Log = Arc<Mutex<Vec<(String, Value)>>>;

        /// Fake Dropbox API on a random local port; logs `(endpoint, body)` of each RPC call.
        async fn fake_dropbox() -> (String, Log) {
            use axum::http::{HeaderMap, StatusCode};
            use axum::response::IntoResponse;

            fn log(state: &Log, endpoint: &str, body: &Value) {
                state
                    .lock()
                    .unwrap()
                    .push((endpoint.to_string(), body.clone()));
            }
            let state = Log::default();
            let app = axum::Router::new()
                .route(
                    "/files/get_metadata",
                    post(|State(l): State<Log>, Json(b): Json<Value>| async move {
                        log(&l, "get_metadata", &b);
                        match b["path"].as_str().unwrap().to_lowercase().as_str() {
                            "/notes" | "id:notes" => Json(folder("/Notes")).into_response(),
                            "/notes/sub" => Json(folder("/Notes/Sub")).into_response(),
                            "/notes/sub/deeper" => {
                                Json(folder("/Notes/Sub/Deeper")).into_response()
                            }
                            p @ ("/deep" | "/stuck" | "/moving") => {
                                let display = format!("/{}{}", p[1..2].to_uppercase(), &p[2..]);
                                Json(folder(&display)).into_response()
                            }
                            p if p.starts_with("/deep/") => {
                                Json(folder(&format!("/Deep{}", &p["/deep".len()..])))
                                    .into_response()
                            }
                            "/notes/a.pdf" => Json(file("/Notes/a.pdf")).into_response(),
                            _ => (
                                StatusCode::CONFLICT,
                                r#"{"error_summary":"path/not_found/.."}"#,
                            )
                                .into_response(),
                        }
                    }),
                )
                .route(
                    "/files/list_folder",
                    post(|State(l): State<Log>, Json(b): Json<Value>| async move {
                        log(&l, "list_folder", &b);
                        Json(first_page(&b["path"].as_str().unwrap().to_lowercase()))
                    }),
                )
                .route(
                    "/files/list_folder/continue",
                    post(|State(l): State<Log>, Json(b): Json<Value>| async move {
                        log(&l, "continue", &b);
                        match next_page(b["cursor"].as_str().unwrap()) {
                            Some(p) => Json(p).into_response(),
                            None => (
                                StatusCode::CONFLICT,
                                r#"{"error_summary":"reset/..","error":{".tag":"reset"}}"#,
                            )
                                .into_response(),
                        }
                    }),
                )
                .route(
                    "/files/list_folder/get_latest_cursor",
                    post(|State(l): State<Log>, Json(b): Json<Value>| async move {
                        log(&l, "get_latest_cursor", &b);
                        Json(json!({ "cursor": format!("latest:{}", b["path"].as_str().unwrap()) }))
                    }),
                )
                .route(
                    "/files/download",
                    post(|headers: HeaderMap| async move {
                        let arg: Value =
                            serde_json::from_str(headers["Dropbox-API-Arg"].to_str().unwrap())
                                .unwrap();
                        arg["path"].as_str().unwrap().to_string()
                    }),
                )
                .route(
                    "/files/upload",
                    post(|State(l): State<Log>, headers: HeaderMap| async move {
                        let arg: Value =
                            serde_json::from_str(headers["Dropbox-API-Arg"].to_str().unwrap())
                                .unwrap();
                        log(&l, "upload", &arg);
                        Json(file(arg["path"].as_str().unwrap()))
                    }),
                )
                .route(
                    "/files/create_folder_v2",
                    post(|State(l): State<Log>, Json(b): Json<Value>| async move {
                        log(&l, "create_folder", &b);
                        Json(json!({ "metadata": folder(b["path"].as_str().unwrap()) }))
                    }),
                )
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (base, state)
        }

        fn dropbox(base: &str) -> Dropbox {
            let config = OAuthConfig::dropbox("id".into(), None, "http://localhost/cb".into());
            let token = OAuthToken {
                access_token: "t".into(),
                refresh_token: None,
                token_type: "Bearer".into(),
                expires_at: None,
                scope: None,
            };
            Dropbox::with_token(config, token).with_base_urls(base, base)
        }

        fn paths(files: &[CloudFile]) -> Vec<&str> {
            let mut v: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
            v.sort();
            v
        }

        fn calls(log: &Log, endpoint: &str) -> Vec<Value> {
            log.lock()
                .unwrap()
                .iter()
                .filter(|(e, _)| e == endpoint)
                .map(|(_, b)| b.clone())
                .collect()
        }

        #[tokio::test]
        async fn list_files_is_recursive_paged_and_relative_to_the_folder() {
            let (base, log) = fake_dropbox().await;
            let d = dropbox(&base);
            let files = d.list_files(Some("/Notes")).await.unwrap();
            // The folder itself, entries outside it, unsafe names (and everything under
            // them), repeats and deletions are dropped; intermediate folders take the casing
            // of their own entry, or of their own metadata when not listed.
            assert_eq!(
                paths(&files),
                vec!["/Sub", "/Sub/Deeper/c.pdf", "/Sub/b.pdf", "/a.pdf"]
            );
            let looked_up: Vec<Value> = calls(&log, "get_metadata")
                .iter()
                .map(|b| b["path"].clone())
                .collect();
            assert_eq!(looked_up, vec![json!("/Notes"), json!("/notes/sub/deeper")]);
            let a = files.iter().find(|f| f.path == "/a.pdf").unwrap();
            assert_eq!(
                (a.id.as_str(), a.size, a.is_folder, a.deleted),
                ("id:/notes/a.pdf", 3, false, false)
            );
            assert!(files.iter().find(|f| f.path == "/Sub").unwrap().is_folder);

            let listed = calls(&log, "list_folder");
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0]["path"], "/Notes");
            assert_eq!(listed[0]["recursive"], true);
            let cont = calls(&log, "continue");
            assert_eq!(cont.len(), 1);
            assert_eq!(cont[0]["cursor"], "L1");

            // The same folder by id, or spelled in another case with a trailing slash.
            for spelling in ["id:notes", "/notes/"] {
                let again = d.list_files(Some(spelling)).await.unwrap();
                assert_eq!(paths(&again), paths(&files), "{spelling}");
            }
        }

        #[tokio::test]
        async fn list_files_at_root_needs_no_folder_lookup() {
            let (base, log) = fake_dropbox().await;
            let d = dropbox(&base);
            for root in [None, Some(""), Some("/")] {
                let files = d.list_files(root).await.unwrap();
                assert_eq!(
                    paths(&files),
                    vec!["/Notes", "/Notes/a.pdf", "/Top.pdf"],
                    "{root:?}"
                );
            }
            assert!(calls(&log, "get_metadata").is_empty());
            assert!(
                calls(&log, "list_folder")
                    .iter()
                    .all(|b| b["path"] == "" && b["recursive"] == true)
            );
        }

        #[tokio::test]
        async fn list_files_caps_depth_and_rejects_bad_folders() {
            let (base, _) = fake_dropbox().await;
            let d = dropbox(&base);
            let files = d.list_files(Some("/Deep")).await.unwrap();
            assert_eq!(files.len(), 1);
            assert!(files[0].path.ends_with("/ok.pdf"));
            assert_eq!(files[0].path.matches('/').count(), MAX_LIST_DEPTH);

            let err = d.list_files(Some("/Stuck")).await.unwrap_err();
            assert!(
                matches!(err, IntegrationError::Api(ref m) if m.contains("repeated")),
                "{err}"
            );
            let err = d.list_files(Some("/Notes/a.pdf")).await.unwrap_err();
            assert!(
                matches!(err, IntegrationError::Api(ref m) if m.contains("not a folder")),
                "{err}"
            );
            let err = d.list_files(Some("/missing")).await.unwrap_err();
            assert!(matches!(err, IntegrationError::NotFound(_)), "{err}");
        }

        /// A listing that pages while the tree changes is replayed in order: deletions and
        /// renames seen on a later page drop the earlier entries (children included), an edit
        /// replaces the stale metadata, and a file replaces a folder and what was under it.
        #[tokio::test]
        async fn list_files_replays_changes_made_while_paging() {
            let (base, _) = fake_dropbox().await;
            let files = dropbox(&base).list_files(Some("/Moving")).await.unwrap();
            assert_eq!(
                paths(&files),
                vec!["/Sub2", "/Sub2/a.pdf", "/Was-a-folder", "/edit.pdf"]
            );
            let edit = files.iter().find(|f| f.path == "/edit.pdf").unwrap();
            assert_eq!(edit.content_hash.as_deref(), Some("h2"));
            assert_eq!(edit.modified_at, 1714979289);
            let was = files.iter().find(|f| f.path == "/Was-a-folder").unwrap();
            assert!(!was.is_folder);
        }

        /// Within one batch of changes, a folder deleted after entries under it were listed
        /// takes them with it, and a file replacing a folder does too.
        #[tokio::test]
        async fn changes_drop_entries_under_a_later_deletion() {
            let (base, _) = fake_dropbox().await;
            let (files, cursor) = dropbox(&base)
                .get_changes_in(Some("/Notes"), Some("CD"))
                .await
                .unwrap();
            assert_eq!(cursor.as_deref(), Some("CD2"));
            let got: Vec<(&str, bool, bool)> = files
                .iter()
                .map(|f| (f.path.as_str(), f.deleted, f.is_folder))
                .collect();
            assert_eq!(got, vec![("/Sub", true, false), ("/Dir", false, false)]);
        }

        /// Uploads and new folders land under the sync folder however it is spelled: `/`, a
        /// trailing slash or none never yields a `//` path, which Dropbox rejects.
        #[tokio::test]
        async fn upload_and_create_folder_join_the_folder_once() {
            let (base, log) = fake_dropbox().await;
            let d = dropbox(&base);
            let folders = [None, Some("/"), Some("/Notes"), Some("/Notes/")];
            for folder in folders {
                let up = d
                    .upload_file_at(folder, &["Sub", "a.pdf"], b"x", None)
                    .await
                    .unwrap();
                assert_eq!(up.name, "a.pdf");
                d.create_folder(folder, "New").await.unwrap();
            }
            let sent = |endpoint| -> Vec<Value> {
                calls(&log, endpoint)
                    .iter()
                    .map(|b| b["path"].clone())
                    .collect()
            };
            assert_eq!(
                sent("upload"),
                vec![
                    json!("/Sub/a.pdf"),
                    json!("/Sub/a.pdf"),
                    json!("/Notes/Sub/a.pdf"),
                    json!("/Notes/Sub/a.pdf"),
                ]
            );
            assert_eq!(
                sent("create_folder"),
                vec![
                    json!("/New"),
                    json!("/New"),
                    json!("/Notes/New"),
                    json!("/Notes/New"),
                ]
            );
        }

        #[tokio::test]
        async fn changes_start_with_a_cursor_scoped_to_the_folder() {
            let (base, log) = fake_dropbox().await;
            let (files, cursor) = dropbox(&base)
                .get_changes_in(Some("/Notes"), None)
                .await
                .unwrap();
            assert!(files.is_empty());
            assert_eq!(cursor.as_deref(), Some("latest:/Notes"));
            let latest = calls(&log, "get_latest_cursor");
            assert_eq!(latest.len(), 1);
            assert_eq!(latest[0]["recursive"], true);
            assert_eq!(latest[0]["include_mounted_folders"], true);
            assert!(calls(&log, "continue").is_empty());
        }

        #[tokio::test]
        async fn changes_are_paged_scoped_and_map_deletions() {
            let (base, log) = fake_dropbox().await;
            let d = dropbox(&base);
            let (files, cursor) = d.get_changes_in(Some("/Notes"), Some("C0")).await.unwrap();
            assert_eq!(cursor.as_deref(), Some("C2"));
            // Feed order, each path's last entry only, outside entries dropped.
            let got: Vec<(&str, bool)> =
                files.iter().map(|f| (f.path.as_str(), f.deleted)).collect();
            assert_eq!(
                got,
                vec![
                    ("/new.pdf", false),
                    ("/Sub/old.pdf", true),
                    ("/re.pdf", false),
                    ("/later-gone.pdf", true),
                    ("/Sub", false),
                ]
            );
            let gone = &files[1];
            assert_eq!(gone.id, "/notes/sub/old.pdf");
            let cont: Vec<Value> = calls(&log, "continue")
                .iter()
                .map(|b| b["cursor"].clone())
                .collect();
            assert_eq!(cont, vec![json!("C0"), json!("C1")]);

            // Unscoped: paths from the Dropbox root.
            let (files, cursor) = d.get_changes(Some("RC")).await.unwrap();
            assert_eq!(
                (paths(&files), cursor.as_deref()),
                (vec!["/Top.pdf"], Some("RC2"))
            );

            // Nothing changed: no folder lookup at all.
            log.lock().unwrap().clear();
            let (files, cursor) = d
                .get_changes_in(Some("/Notes"), Some("empty"))
                .await
                .unwrap();
            assert!(files.is_empty());
            assert_eq!(cursor.as_deref(), Some("empty2"));
            assert!(calls(&log, "get_metadata").is_empty());
        }

        /// A changed file whose folder isn't in the batch takes that folder's casing from the
        /// folder's own metadata (looked up once per call), as the full listing does, whatever
        /// its `path_display` says; so the same file lands in the same local directory however
        /// it was seen. A file whose folder is gone by then is skipped.
        #[tokio::test]
        async fn changes_and_listing_agree_on_folder_casing() {
            let (base, log) = fake_dropbox().await;
            let d = dropbox(&base);
            let listed = d.list_files(Some("/Notes")).await.unwrap();
            assert!(paths(&listed).contains(&"/Sub/b.pdf"));

            log.lock().unwrap().clear();
            let (changes, cursor) = d
                .get_changes_in(Some("/Notes"), Some("CASE"))
                .await
                .unwrap();
            assert_eq!(cursor.as_deref(), Some("CASE2"));
            assert_eq!(paths(&changes), vec!["/Sub/b.pdf", "/Sub/c2.pdf"]);
            let looked_up: Vec<Value> = calls(&log, "get_metadata")
                .iter()
                .map(|b| b["path"].clone())
                .collect();
            assert_eq!(
                looked_up,
                vec![json!("/Notes"), json!("/notes/gone"), json!("/notes/sub")]
            );
        }

        #[tokio::test]
        async fn reset_cursor_requires_resync() {
            let (base, _) = fake_dropbox().await;
            let err = dropbox(&base)
                .get_changes_in(Some("/Notes"), Some("expired"))
                .await
                .unwrap_err();
            assert!(matches!(err, IntegrationError::ResyncRequired(_)), "{err}");
        }

        fn sync_for(
            base: &str,
            root: &std::path::Path,
            cursor: Option<&str>,
        ) -> CloudSync<Dropbox> {
            let config = SyncConfig {
                local_path: root.to_path_buf(),
                cloud_folder: Some("/Notes".into()),
                direction: SyncDirection::Download,
                ..Default::default()
            };
            let state = SyncState {
                cursor: cursor.map(Into::into),
                ..Default::default()
            };
            CloudSync::with_state(dropbox(base), config, state)
        }

        /// Changes land at their folder-relative paths; a remote deletion never removes (or
        /// tries to download) the local copy.
        #[tokio::test]
        async fn delta_sync_applies_scoped_changes_and_skips_deletions() {
            let (base, _) = fake_dropbox().await;
            let dir = tempfile::tempdir().unwrap();
            std::fs::create_dir(dir.path().join("Sub")).unwrap();
            std::fs::write(dir.path().join("Sub/old.pdf"), "mine").unwrap();
            let mut sync = sync_for(&base, dir.path(), Some("C0"));
            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            assert_eq!((r.downloaded, r.deleted), (2, 0));
            assert_eq!(
                std::fs::read(dir.path().join("new.pdf")).unwrap(),
                b"id:/notes/new.pdf"
            );
            assert!(dir.path().join("re.pdf").exists());
            assert_eq!(
                std::fs::read(dir.path().join("Sub/old.pdf")).unwrap(),
                b"mine"
            );
            assert_eq!(sync.state().cursor.as_deref(), Some("C2"));
        }

        /// An invalidated cursor, or none yet, falls back to a full (recursive) sync and a
        /// fresh cursor.
        #[tokio::test]
        async fn delta_sync_resyncs_after_reset_or_without_cursor() {
            for cursor in [Some("expired"), None] {
                let (base, log) = fake_dropbox().await;
                let dir = tempfile::tempdir().unwrap();
                let mut sync = sync_for(&base, dir.path(), cursor);
                let r = sync.delta_sync().await.unwrap();
                assert!(r.errors.is_empty(), "{:?}", r.errors);
                assert_eq!(r.downloaded, 3);
                assert!(dir.path().join("Sub/Deeper/c.pdf").exists());
                assert!(dir.path().join("Sub/b.pdf").exists());
                assert_eq!(sync.state().cursor.as_deref(), Some("latest:/Notes"));
                // The fresh cursor was taken before the full listing started.
                let order: Vec<String> =
                    log.lock().unwrap().iter().map(|(e, _)| e.clone()).collect();
                let latest = order.iter().position(|e| e == "get_latest_cursor").unwrap();
                let listed = order.iter().position(|e| e == "list_folder").unwrap();
                assert!(latest < listed, "{cursor:?}: {order:?}");
            }
        }
    }

    #[test]
    fn api_arg_header_is_ascii_and_round_trips() {
        let path = "/Notes/café/😀 \u{7f}.pdf";
        let json = serde_json::to_string(&serde_json::json!({ "path": path })).unwrap();
        let safe = header_safe_json(&json);
        assert!(
            reqwest::header::HeaderValue::from_str(&safe).is_ok(),
            "{safe}"
        );
        assert!(safe.contains("caf\\u00e9") && safe.contains("\\ud83d\\ude00"));
        let back: serde_json::Value = serde_json::from_str(&safe).unwrap();
        assert_eq!(back["path"], path);
    }
}
