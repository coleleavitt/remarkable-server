//! OneDrive integration
//!
//! Full read/write access via Microsoft Graph API.

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

const GRAPH_BASE: &str = "https://graph.microsoft.com/v1.0";

/// Percent-encode each segment of a `/`-separated relative path for Graph path addressing
/// (`items/{id}:/{path}:/content`); raw `#`, `?` or `%` in a name would otherwise truncate or
/// corrupt the URL. Missing intermediate folders in the path are created by Graph itself.
fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|seg| urlencoding::encode(seg).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// OneDrive provider
pub struct OneDrive {
    config: OAuthConfig,
    token: Option<OAuthToken>,
    client: Client,
    graph_base: String,
}

impl OneDrive {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            token: None,
            client: crate::integrations::http_client(),
            graph_base: GRAPH_BASE.into(),
        }
    }

    pub fn with_token(config: OAuthConfig, token: OAuthToken) -> Self {
        Self {
            config,
            token: Some(token),
            client: crate::integrations::http_client(),
            graph_base: GRAPH_BASE.into(),
        }
    }

    /// Point the provider at a different Graph endpoint (tests, proxies).
    pub fn with_base_url(mut self, graph_base: &str) -> Self {
        self.graph_base = graph_base.trim_end_matches('/').into();
        self
    }

    fn access_token(&self) -> Result<&str> {
        self.token
            .as_ref()
            .map(|t| t.access_token.as_str())
            .ok_or(IntegrationError::NotConfigured)
    }

    async fn request(&self, method: reqwest::Method, url: &str) -> Result<reqwest::RequestBuilder> {
        let token = self.access_token()?;
        Ok(self.client.request(method, url).bearer_auth(token))
    }

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

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let response = self
            .request(reqwest::Method::GET, url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;
        self.handle_response(response).await
    }

    fn children_url(&self, folder_id: Option<&str>) -> String {
        match folder_id {
            Some(id) => format!("{}/me/drive/items/{}/children", self.graph_base, id),
            None => format!("{}/me/drive/root/children", self.graph_base),
        }
    }

    /// Every item of the collection at `url`, following `@odata.nextLink`.
    async fn get_all(&self, url: &str) -> Result<Vec<DriveItem>> {
        let mut items = Vec::new();
        let mut used = HashSet::from([url.to_string()]);
        let mut url = url.to_string();
        loop {
            let page: ListChildrenResponse = self.get_json(&url).await?;
            items.extend(page.value);
            match page.next_link {
                // A repeated link would loop forever; fail rather than return a partial list
                // (a partial list makes sync re-upload everything it didn't see).
                Some(next) if !used.insert(next.clone()) => {
                    return Err(IntegrationError::Api(format!(
                        "Graph returned repeated nextLink {:?}",
                        next
                    )));
                }
                Some(next) => url = next,
                None => return Ok(items),
            }
        }
    }

    /// Every item of a delta query from `url` through its last page, and the
    /// `@odata.deltaLink` for next time. A 410 (expired token) is
    /// [`IntegrationError::ResyncRequired`].
    async fn delta_all(&self, url: &str) -> Result<(Vec<DriveItem>, String)> {
        let mut items = Vec::new();
        let mut used = HashSet::from([url.to_string()]);
        let mut url = url.to_string();
        loop {
            let page: DeltaResponse = self.get_json(&url).await?;
            items.extend(page.value);
            match (page.delta_link, page.next_link) {
                (Some(done), _) => return Ok((items, done)),
                (None, Some(next)) if !used.insert(next.clone()) => {
                    return Err(IntegrationError::Api(format!(
                        "Graph returned repeated delta nextLink {:?}",
                        next
                    )));
                }
                (None, Some(next)) => url = next,
                (None, None) => {
                    return Err(IntegrationError::Api(
                        "delta page with neither nextLink nor deltaLink".into(),
                    ));
                }
            }
        }
    }

    /// Folder `id` for path resolution; `None` if it's gone.
    async fn folder_info(&self, id: &str) -> Result<Option<FolderInfo>> {
        let url = format!("{}/me/drive/items/{}", self.graph_base, id);
        match self.get_json::<DriveItem>(&url).await {
            Ok(item) => Ok(Some(FolderInfo::from(&item))),
            Err(IntegrationError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Path of folder `id` relative to `root` (`""` for the root itself, `"/A/B"` below it), or
    /// `None` if its chain of parents doesn't reach the root within [`MAX_LIST_DEPTH`] (outside
    /// the sync folder, deleted, unsafe name). Delta responses carry no `parentReference.path`,
    /// so the chain is walked by id: folders in the delta itself (`known`) are used as they
    /// are, others are fetched once and memoized in `cache`. A folder being resolved is
    /// provisionally `None`, which also breaks cycles.
    async fn folder_path(
        &self,
        id: &str,
        root: &SyncRoot<'_>,
        known: &HashMap<String, FolderInfo>,
        cache: &mut HashMap<String, Option<String>>,
        depth: usize,
    ) -> Result<Option<String>> {
        if matches!(root, SyncRoot::Folder(r) if *r == id) {
            return Ok(Some(String::new()));
        }
        if let Some(hit) = cache.get(id) {
            return Ok(hit.clone());
        }
        if depth >= MAX_LIST_DEPTH {
            return Ok(None);
        }
        cache.insert(id.to_string(), None);
        let info = match known.get(id) {
            Some(info) => Some(info.clone()),
            None => self.folder_info(id).await?,
        };
        let resolved = match info {
            Some(info) if info.is_drive_root => matches!(root, SyncRoot::Drive).then(String::new),
            Some(FolderInfo {
                name,
                parent: Some(parent),
                ..
            }) if is_safe_name(&name) => {
                Box::pin(self.folder_path(&parent, root, known, cache, depth + 1))
                    .await?
                    .map(|prefix| format!("{}/{}", prefix, name))
            }
            _ => None,
        };
        cache.insert(id.to_string(), resolved.clone());
        Ok(resolved)
    }
}

/// The folder change paths are relative to: the whole drive, or a folder by item id.
enum SyncRoot<'a> {
    Drive,
    Folder(&'a str),
}

impl<'a> SyncRoot<'a> {
    fn of(folder_id: Option<&'a str>) -> Self {
        match folder_id {
            None | Some("" | "root") => Self::Drive,
            Some(id) => Self::Folder(id),
        }
    }
}

/// What resolving a path needs to know about a folder.
#[derive(Clone)]
struct FolderInfo {
    name: String,
    parent: Option<String>,
    is_drive_root: bool,
}

impl From<&DriveItem> for FolderInfo {
    fn from(item: &DriveItem) -> Self {
        Self {
            name: item.name.clone(),
            parent: item.parent_reference.as_ref().and_then(|p| p.id.clone()),
            is_drive_root: item.root.is_some(),
        }
    }
}

/// Map a non-success Graph response to an error: 404 is [`IntegrationError::NotFound`]
/// (permanent); auth, rate limits and 5xx stay retryable.
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
        404 => IntegrationError::NotFound("Item not found".into()),
        // Only delta queries answer 410: the token expired (`resyncRequired`).
        410 => IntegrationError::ResyncRequired(response.text().await.unwrap_or_default()),
        507 => IntegrationError::QuotaExceeded,
        409 => IntegrationError::Conflict(response.text().await.unwrap_or_default()),
        _ => {
            let body = response.text().await.unwrap_or_default();
            IntegrationError::Api(format!("{}: {}", status, body))
        }
    }
}

/// OneDrive item (file or folder)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveItem {
    id: String,
    /// Absent on some deleted items in delta responses.
    #[serde(default)]
    name: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    folder: Option<FolderFacet>,
    #[serde(default)]
    file: Option<FileFacet>,
    last_modified_date_time: Option<String>,
    parent_reference: Option<ParentReference>,
    /// Facet present only on the drive's root folder.
    #[serde(default)]
    root: Option<serde_json::Value>,
    /// Facet present on items a delta reports as deleted.
    #[serde(default)]
    deleted: Option<serde_json::Value>,
    #[serde(rename = "@microsoft.graph.downloadUrl")]
    #[allow(dead_code)]
    download_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FolderFacet {
    #[serde(default)]
    #[allow(dead_code)]
    child_count: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileFacet {
    mime_type: Option<String>,
    hashes: Option<FileHashes>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileHashes {
    sha256_hash: Option<String>,
    quick_xor_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParentReference {
    id: Option<String>,
    path: Option<String>,
    #[allow(dead_code)]
    drive_id: Option<String>,
}

impl DriveItem {
    fn to_cloud_file(&self) -> CloudFile {
        let path = self
            .parent_reference
            .as_ref()
            .and_then(|p| p.path.as_ref())
            .map(|p| format!("{}/{}", p.replace("/drive/root:", ""), self.name))
            .unwrap_or_else(|| format!("/{}", self.name));
        self.to_cloud_file_at(path)
    }

    /// As a [`CloudFile`] at `path` (relative to the sync folder).
    fn to_cloud_file_at(&self, path: String) -> CloudFile {
        let is_folder = self.folder.is_some();
        let mime_type = self.file.as_ref().and_then(|f| f.mime_type.clone());
        let content_hash = self
            .file
            .as_ref()
            .and_then(|f| f.hashes.as_ref())
            .and_then(|h| h.sha256_hash.clone().or(h.quick_xor_hash.clone()));

        CloudFile {
            id: self.id.clone(),
            name: self.name.clone(),
            mime_type,
            size: self.size,
            modified_at: self
                .last_modified_date_time
                .as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|dt| dt.timestamp())
                .unwrap_or(0),
            content_hash,
            parent_id: self.parent_reference.as_ref().and_then(|p| p.id.clone()),
            is_folder,
            path,
            deleted: self.deleted.is_some(),
        }
    }

    fn to_cloud_folder(&self) -> CloudFolder {
        let path = self
            .parent_reference
            .as_ref()
            .and_then(|p| p.path.as_ref())
            .map(|p| format!("{}/{}", p.replace("/drive/root:", ""), self.name))
            .unwrap_or_else(|| format!("/{}", self.name));

        CloudFolder {
            id: self.id.clone(),
            name: self.name.clone(),
            path,
            parent_id: self.parent_reference.as_ref().and_then(|p| p.id.clone()),
        }
    }
}

/// List children response
#[derive(Debug, Deserialize)]
struct ListChildrenResponse {
    value: Vec<DriveItem>,
    #[serde(rename = "@odata.nextLink")]
    next_link: Option<String>,
}

/// Drive info response
#[derive(Debug, Deserialize)]
struct DriveResponse {
    quota: DriveQuota,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveQuota {
    used: u64,
    total: Option<u64>,
    deleted: Option<u64>,
}

/// Delta response
#[derive(Debug, Deserialize)]
struct DeltaResponse {
    value: Vec<DriveItem>,
    #[serde(rename = "@odata.deltaLink")]
    delta_link: Option<String>,
    #[serde(rename = "@odata.nextLink")]
    next_link: Option<String>,
}

/// Upload session response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadSessionResponse {
    upload_url: String,
    #[allow(dead_code)]
    expiration_date_time: String,
}

#[async_trait]
impl CloudProvider for OneDrive {
    fn provider_type(&self) -> ProviderType {
        ProviderType::OneDrive
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

    /// Recursively list everything under `folder_id` (an item id; default: the drive root),
    /// walking `children` breadth-first and paging each folder with `@odata.nextLink`. Works on
    /// personal and business drives alike (folder-scoped delta is personal-only). Paths are
    /// relative to that folder (`/Sub/dir/file.pdf`), matching the local scan so nested files
    /// aren't seen as missing and re-uploaded every sync. Each item is listed once, the first
    /// wins a duplicate path, names that aren't a single safe path segment are skipped (with
    /// everything under them), and folders deeper than [`MAX_LIST_DEPTH`] aren't entered.
    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        let folder_id = folder_id.filter(|id| !id.is_empty()); // same as `SyncRoot::of`
        let mut seen_items: HashSet<String> = folder_id.map(str::to_string).into_iter().collect();
        let mut seen_paths = HashSet::new();
        let mut queue = VecDeque::from([(self.children_url(folder_id), String::new(), 0usize)]);
        let mut out = Vec::new();

        while let Some((url, prefix, depth)) = queue.pop_front() {
            for item in self.get_all(&url).await? {
                if item.deleted.is_some() {
                    continue;
                }
                if !is_safe_name(&item.name) {
                    tracing::warn!(
                        "onedrive: skipping unsafe name {:?} in {}",
                        item.name,
                        prefix
                    );
                    continue;
                }
                if !seen_items.insert(item.id.clone()) {
                    continue;
                }
                let path = format!("{}/{}", prefix, item.name);
                if !seen_paths.insert(path.clone()) {
                    tracing::warn!("onedrive: duplicate path {:?}, keeping the first", path);
                    continue;
                }
                if item.folder.is_some() {
                    if depth + 1 < MAX_LIST_DEPTH {
                        let children = self.children_url(Some(&item.id));
                        queue.push_back((children, path.clone(), depth + 1));
                    } else {
                        tracing::warn!("onedrive: not descending into {:?}: too deep", path);
                    }
                }
                out.push(item.to_cloud_file_at(path));
            }
        }
        Ok(out)
    }

    async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
        // Use search to find all folders
        let url = format!(
            "{}/me/drive/root/search(q='')?$filter=folder ne null",
            self.graph_base
        );
        Ok(self
            .get_all(&url)
            .await?
            .iter()
            .filter(|i| i.folder.is_some())
            .map(|i| i.to_cloud_folder())
            .collect())
    }

    async fn get_file_metadata(&self, file_id: &str) -> Result<CloudFile> {
        let url = format!("{}/me/drive/items/{}", self.graph_base, file_id);

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let item: DriveItem = self.handle_response(response).await?;
        Ok(item.to_cloud_file())
    }

    async fn download_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let url = format!("{}/me/drive/items/{}/content", self.graph_base, file_id);

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        // OneDrive returns 302 redirect to download URL
        if response.status().is_redirection() {
            let download_url = response
                .headers()
                .get("Location")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| IntegrationError::Api("Missing redirect location".into()))?;

            let response = self
                .client
                .get(download_url)
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;
            if !response.status().is_success() {
                return Err(response_error(response).await);
            }
            let content = response
                .bytes()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            return Ok(content.to_vec());
        }

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
        // For files <= 4MB, use simple upload
        // For larger files, use upload session
        const SIMPLE_UPLOAD_LIMIT: usize = 4 * 1024 * 1024;

        let url = if let Some(id) = parent_id {
            format!(
                "{}/me/drive/items/{}:/{}:/content",
                self.graph_base,
                id,
                encode_path(name)
            )
        } else {
            format!(
                "{}/me/drive/root:/{}:/content",
                self.graph_base,
                encode_path(name)
            )
        };

        if content.len() <= SIMPLE_UPLOAD_LIMIT {
            // Simple upload
            let response = self
                .request(reqwest::Method::PUT, &url)
                .await?
                .header("Content-Type", "application/octet-stream")
                .body(content.to_vec())
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            let item: DriveItem = self.handle_response(response).await?;
            Ok(item.to_cloud_file())
        } else {
            // Resumable upload for large files
            self.upload_large_file(parent_id, name, content).await
        }
    }

    async fn create_folder(&self, parent_id: Option<&str>, name: &str) -> Result<CloudFolder> {
        let url = if let Some(id) = parent_id {
            format!("{}/me/drive/items/{}/children", self.graph_base, id)
        } else {
            format!("{}/me/drive/root/children", self.graph_base)
        };

        #[derive(Serialize)]
        struct CreateFolder<'a> {
            name: &'a str,
            folder: serde_json::Value,
            #[serde(rename = "@microsoft.graph.conflictBehavior")]
            conflict_behavior: &'a str,
        }

        let body = CreateFolder {
            name,
            folder: serde_json::json!({}),
            conflict_behavior: "fail",
        };

        let response = self
            .request(reqwest::Method::POST, &url)
            .await?
            .json(&body)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let item: DriveItem = self.handle_response(response).await?;
        Ok(item.to_cloud_folder())
    }

    async fn delete(&self, file_id: &str) -> Result<()> {
        let url = format!("{}/me/drive/items/{}", self.graph_base, file_id);

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
        let url = format!("{}/me/drive/items/{}", self.graph_base, file_id);

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct MoveRequest<'a> {
            parent_reference: ParentRef<'a>,
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<&'a str>,
        }

        #[derive(Serialize)]
        struct ParentRef<'a> {
            id: &'a str,
        }

        let body = MoveRequest {
            parent_reference: ParentRef { id: new_parent_id },
            name: new_name,
        };

        let response = self
            .request(reqwest::Method::PATCH, &url)
            .await?
            .json(&body)
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let item: DriveItem = self.handle_response(response).await?;
        Ok(item.to_cloud_file())
    }

    async fn get_changes(&self, cursor: Option<&str>) -> Result<(Vec<CloudFile>, Option<String>)> {
        self.get_changes_in(None, cursor).await
    }

    /// Changes under `folder_id` since `cursor` (a delta or next link), following every page to
    /// the `@odata.deltaLink`, pathed like [`list_files`](CloudProvider::list_files). Uses the
    /// root delta, the only one business drives support, keeping items whose parent chain
    /// reaches the folder (see [`OneDrive::folder_path`]). Without a cursor, returns no changes
    /// and a `token=latest` link. Each item's last occurrence is its state; deleted items become
    /// deletions and come first, so applying the list in order never removes a path that a
    /// live item in it re-creates. An expired token (410) is
    /// [`IntegrationError::ResyncRequired`].
    async fn get_changes_in(
        &self,
        folder_id: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<(Vec<CloudFile>, Option<String>)> {
        let Some(cursor) = cursor else {
            let latest = format!("{}/me/drive/root/delta?token=latest", self.graph_base);
            let (_, link) = self.delta_all(&latest).await?;
            return Ok((vec![], Some(link)));
        };
        let (all, next) = self.delta_all(cursor).await?;

        let mut latest: HashMap<&str, &DriveItem> = HashMap::new();
        let mut order = Vec::new();
        for item in &all {
            if latest.insert(item.id.as_str(), item).is_none() {
                order.push(item.id.as_str());
            }
        }
        let items: Vec<&DriveItem> = order.iter().map(|id| latest[id]).collect();
        if items.is_empty() {
            return Ok((vec![], Some(next)));
        }

        let root = SyncRoot::of(folder_id);
        let known: HashMap<String, FolderInfo> = items
            .iter()
            .filter(|i| i.folder.is_some() || i.root.is_some())
            .map(|i| (i.id.clone(), FolderInfo::from(*i)))
            .collect();
        let mut cache = HashMap::new();
        let mut deletions = Vec::new();
        let mut live = Vec::new();
        for item in items {
            if item.root.is_some() || matches!(root, SyncRoot::Folder(r) if r == item.id) {
                continue; // the sync folder itself
            }
            if !is_safe_name(&item.name) {
                tracing::debug!(
                    "onedrive: skipping change {} with unsafe name {:?}",
                    item.id,
                    item.name
                );
                continue;
            }
            let parent = item.parent_reference.as_ref().and_then(|p| p.id.as_deref());
            let prefix = match parent {
                Some(parent) => {
                    self.folder_path(parent, &root, &known, &mut cache, 0)
                        .await?
                }
                None => None,
            };
            let Some(prefix) = prefix else {
                tracing::debug!(
                    "onedrive: skipping change {:?} ({}): not under the sync folder",
                    item.name,
                    item.id
                );
                continue;
            };
            let path = format!("{}/{}", prefix, item.name);
            if path.matches('/').count() > MAX_LIST_DEPTH {
                tracing::warn!("onedrive: skipping change {:?}: too deep", path);
                continue;
            }
            let file = item.to_cloud_file_at(path);
            if file.deleted {
                deletions.push(file);
            } else {
                live.push(file);
            }
        }

        let mut seen = HashSet::new();
        live.retain(|f| {
            let first = seen.insert(f.path.clone());
            if !first {
                tracing::warn!(
                    "onedrive: duplicate change path {:?}, keeping the first",
                    f.path
                );
            }
            first
        });
        // A deletion at a path a live item holds is superseded by it.
        deletions.retain(|f| seen.insert(f.path.clone()));
        deletions.extend(live);
        Ok((deletions, Some(next)))
    }

    async fn get_quota(&self) -> Result<StorageQuota> {
        let url = format!("{}/me/drive", self.graph_base);

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let drive: DriveResponse = self.handle_response(response).await?;

        Ok(StorageQuota {
            used: drive.quota.used,
            total: drive.quota.total,
            trash: drive.quota.deleted,
        })
    }
}

impl OneDrive {
    /// Upload large file using resumable upload session
    async fn upload_large_file(
        &self,
        parent_id: Option<&str>,
        name: &str,
        content: &[u8],
    ) -> Result<CloudFile> {
        // Create upload session
        let session_url = if let Some(id) = parent_id {
            format!(
                "{}/me/drive/items/{}:/{}:/createUploadSession",
                self.graph_base,
                id,
                encode_path(name)
            )
        } else {
            format!(
                "{}/me/drive/root:/{}:/createUploadSession",
                self.graph_base,
                encode_path(name)
            )
        };

        #[derive(Serialize)]
        struct CreateSessionBody {
            item: CreateSessionItem,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CreateSessionItem {
            #[serde(rename = "@microsoft.graph.conflictBehavior")]
            conflict_behavior: String,
        }

        let session_response = self
            .request(reqwest::Method::POST, &session_url)
            .await?
            .json(&CreateSessionBody {
                item: CreateSessionItem {
                    conflict_behavior: "replace".into(),
                },
            })
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let session: UploadSessionResponse = self.handle_response(session_response).await?;

        // Upload in chunks (10MB each)
        const CHUNK_SIZE: usize = 10 * 1024 * 1024;
        let total_size = content.len();
        let mut offset = 0;

        let mut last_response: Option<DriveItem> = None;

        while offset < total_size {
            let end = std::cmp::min(offset + CHUNK_SIZE, total_size);
            let chunk = &content[offset..end];

            let content_range = format!("bytes {}-{}/{}", offset, end - 1, total_size);

            let response = self
                .client
                .put(&session.upload_url)
                .header("Content-Length", chunk.len())
                .header("Content-Range", content_range)
                .body(chunk.to_vec())
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            if response.status().as_u16() == 202 {
                // Chunk accepted, continue
                offset = end;
            } else if response.status().is_success() {
                // Upload complete
                let item: DriveItem = response
                    .json()
                    .await
                    .map_err(|e| IntegrationError::Serialization(e.to_string()))?;
                last_response = Some(item);
                break;
            } else {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(IntegrationError::Api(format!(
                    "Upload failed at offset {}: {} - {}",
                    offset, status, body
                )));
            }
        }

        last_response.map(|i| i.to_cloud_file()).ok_or_else(|| {
            IntegrationError::Api("Upload completed but no response received".into())
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::Path;
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;

    use super::*;

    /// A deleted item (404, directly or at the redirected download URL) fails permanently;
    /// throttling and 5xx stay retryable.
    #[tokio::test]
    async fn download_error_mapping() {
        let app = axum::Router::new()
            .route(
                "/me/drive/items/{id}/content",
                axum::routing::get(|Path(id): Path<String>| async move {
                    match id.as_str() {
                        "gone" => StatusCode::NOT_FOUND.into_response(),
                        "moved" => {
                            (StatusCode::FOUND, [(header::LOCATION, "/blob/gone")]).into_response()
                        }
                        "flaky" => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                        "busy" => StatusCode::TOO_MANY_REQUESTS.into_response(),
                        _ => (StatusCode::FOUND, [(header::LOCATION, "/blob/ok")]).into_response(),
                    }
                }),
            )
            .route(
                "/blob/{name}",
                axum::routing::get(|Path(name): Path<String>| async move {
                    if name == "ok" {
                        "content".into_response()
                    } else {
                        StatusCode::NOT_FOUND.into_response()
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = OAuthConfig::onedrive("id".into(), None, "http://localhost/cb".into());
        let token = OAuthToken {
            access_token: "t".into(),
            refresh_token: None,
            token_type: "Bearer".into(),
            expires_at: None,
            scope: None,
        };
        let d = OneDrive::with_token(config, token).with_base_url(&base);

        for id in ["gone", "moved"] {
            let err = d.download_file(id).await.unwrap_err();
            assert!(matches!(err, IntegrationError::NotFound(_)), "{id}: {err}");
        }
        for id in ["flaky", "busy"] {
            let err = d.download_file(id).await.unwrap_err();
            assert!(!err.is_permanent(), "{id}: {err}");
        }
        assert_eq!(d.download_file("ok").await.unwrap(), b"content");
    }

    mod listing {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        use axum::Json;
        use axum::extract::{Path, Query, State};
        use axum::http::StatusCode;
        use axum::response::{IntoResponse, Response};
        use axum::routing::get;
        use serde_json::{Value, json};

        use super::*;
        use crate::integrations::sync::{CloudSync, SyncConfig, SyncDirection, SyncState};

        fn file(id: &str, name: &str, parent: &str) -> Value {
            json!({
                "id": id, "name": name, "size": 3,
                "file": { "mimeType": "application/pdf" },
                "lastModifiedDateTime": "2024-01-02T03:04:05Z",
                "parentReference": { "id": parent },
            })
        }
        fn folder(id: &str, name: &str, parent: &str) -> Value {
            json!({ "id": id, "name": name, "folder": { "childCount": 1 },
                    "parentReference": { "id": parent } })
        }
        fn gone(id: &str, name: &str, parent: &str) -> Value {
            json!({ "id": id, "name": name, "deleted": { "state": "deleted" },
                    "parentReference": { "id": parent } })
        }
        fn drive_root() -> Value {
            json!({ "id": "ROOTID", "name": "root", "folder": {}, "root": {} })
        }

        /// `children` of folder `id` (`root` for the drive root), page `page`, and the next page.
        fn children(id: &str, page: Option<&str>) -> (Vec<Value>, Option<String>) {
            match (id, page) {
                ("root", _) => (
                    vec![
                        file("T", "top.pdf", "ROOTID"),
                        folder("F", "Notes", "ROOTID"),
                    ],
                    None,
                ),
                ("F", None) => (
                    vec![file("A", "a.pdf", "F"), folder("S", "Sub", "F")],
                    Some("F/children?page=2".into()),
                ),
                ("F", Some("2")) => (
                    vec![
                        file("B", "b.pdf", "F"),
                        file("BAD", "..", "F"),
                        folder("W", r"we\ird", "F"),
                        file("A", "a.pdf", "F"), // repeated across pages
                    ],
                    None,
                ),
                ("S", None) => (
                    vec![file("C", "c.pdf", "S"), folder("DP", "Deeper", "S")],
                    None,
                ),
                ("DP", None) => (vec![file("D", "d.pdf", "DP")], None),
                ("stuck", _) => (vec![], Some("stuck/children?page=1".into())),
                (deep, None) if deep.starts_with("deep") => {
                    let n: usize = deep[4..].parse().unwrap();
                    (vec![folder(&format!("deep{}", n + 1), "d", deep)], None)
                }
                _ => (vec![], None),
            }
        }

        /// Folders `GET items/{id}` knows (for path resolution); anything else is 404.
        fn item(id: &str) -> Option<Value> {
            Some(match id {
                "ROOTID" => drive_root(),
                "F" => folder("F", "Notes", "ROOTID"),
                "S" => folder("S", "Sub", "F"),
                "O" => folder("O", "Other", "ROOTID"),
                "X" => folder("X", "..", "F"),
                "c1" => folder("c1", "c1", "c2"),
                "c2" => folder("c2", "c2", "c1"),
                _ => return None,
            })
        }

        /// Delta pages by `token`: items, then `("next" | "delta", token)` for the link.
        fn delta(token: &str) -> Option<(Vec<Value>, Option<(&'static str, &'static str)>)> {
            Some(match token {
                "latest" => (vec![], Some(("delta", "D0"))),
                "D0" => (
                    vec![
                        drive_root(),
                        folder("F", "Notes", "ROOTID"),
                        file("N", "new.pdf", "F"),
                        file("Y", "y.pdf", "S"), // S isn't in the delta: looked up
                        file("Y2", "y2.pdf", "S"),
                        file("OUT", "out.pdf", "O"),
                        file("R", "first.pdf", "F"),
                        gone("G", "gone.pdf", "F"),
                        file("U", "u.pdf", "X"),
                        file("CY", "c.pdf", "c1"),
                        file("OR", "o.pdf", "LOST"),
                    ],
                    Some(("next", "D0p2")),
                ),
                "D0p2" => (
                    vec![
                        file("R", "second.pdf", "F"), // renamed: the last occurrence wins
                        gone("OLDRE", "re.pdf", "F"),
                        file("NEWRE", "re.pdf", "F"), // re-created under the same name
                        json!({ "id": "Q", "deleted": {}, "parentReference": {} }),
                    ],
                    Some(("delta", "D1")),
                ),
                "empty" => (vec![], Some(("delta", "E1"))),
                "loop" => (vec![], Some(("next", "loop"))),
                "none" => (vec![], None),
                _ => return None,
            })
        }

        #[derive(Clone, Default)]
        struct Fake {
            base: Arc<Mutex<String>>,
            log: Arc<Mutex<Vec<String>>>,
        }

        impl Fake {
            fn log(&self, what: String) {
                self.log.lock().unwrap().push(what);
            }
            fn calls(&self, prefix: &str) -> Vec<String> {
                let log = self.log.lock().unwrap();
                log.iter()
                    .filter(|c| c.starts_with(prefix))
                    .cloned()
                    .collect()
            }
        }

        async fn children_route(
            State(fake): State<Fake>,
            id: Option<Path<String>>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            let id = id.map_or("root".to_string(), |Path(id)| id);
            let page = q.get("page").map(String::as_str);
            fake.log(format!("children {} {:?}", id, page));
            let (value, next) = children(&id, page);
            let base = fake.base.lock().unwrap().clone();
            let mut body = json!({ "value": value });
            if let Some(next) = next {
                body["@odata.nextLink"] = json!(format!("{}/me/drive/items/{}", base, next));
            }
            Json(body)
        }

        async fn item_route(State(fake): State<Fake>, Path(id): Path<String>) -> Response {
            fake.log(format!("item {}", id));
            match item(&id) {
                Some(v) => Json(v).into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }

        async fn delta_route(
            State(fake): State<Fake>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Response {
            let token = q["token"].as_str();
            fake.log(format!("delta {}", token));
            let Some((value, link)) = delta(token) else {
                return (StatusCode::GONE, r#"{"error":{"code":"resyncRequired"}}"#)
                    .into_response();
            };
            let base = fake.base.lock().unwrap().clone();
            let mut body = json!({ "value": value });
            if let Some((kind, token)) = link {
                let url = format!("{}/me/drive/root/delta?token={}", base, token);
                let key = if kind == "next" {
                    "@odata.nextLink"
                } else {
                    "@odata.deltaLink"
                };
                body[key] = json!(url);
            }
            Json(body).into_response()
        }

        /// Fake Graph on a random local port.
        async fn fake_graph() -> (String, Fake) {
            let fake = Fake::default();
            let app = axum::Router::new()
                .route("/me/drive/root/children", get(children_route))
                .route("/me/drive/items/{id}/children", get(children_route))
                .route("/me/drive/items/{id}", get(item_route))
                .route(
                    "/me/drive/items/{id}/content",
                    get(|Path(id): Path<String>| async move { id }),
                )
                .route("/me/drive/root/delta", get(delta_route))
                .with_state(fake.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            *fake.base.lock().unwrap() = base.clone();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (base, fake)
        }

        fn onedrive(base: &str) -> OneDrive {
            let config = OAuthConfig::onedrive("id".into(), None, "http://localhost/cb".into());
            let token = OAuthToken {
                access_token: "t".into(),
                refresh_token: None,
                token_type: "Bearer".into(),
                expires_at: None,
                scope: None,
            };
            OneDrive::with_token(config, token).with_base_url(base)
        }

        fn paths(files: &[CloudFile]) -> Vec<&str> {
            let mut v: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
            v.sort();
            v
        }

        #[tokio::test]
        async fn list_files_walks_children_with_paging_relative_to_the_folder() {
            let (base, fake) = fake_graph().await;
            let files = onedrive(&base).list_files(Some("F")).await.unwrap();
            // Unsafe names are skipped (and never entered); a repeated item is listed once.
            assert_eq!(
                paths(&files),
                vec![
                    "/Sub",
                    "/Sub/Deeper",
                    "/Sub/Deeper/d.pdf",
                    "/Sub/c.pdf",
                    "/a.pdf",
                    "/b.pdf"
                ]
            );
            let sub = files.iter().find(|f| f.path == "/Sub").unwrap();
            assert!(sub.is_folder && !sub.deleted);
            assert_eq!(
                fake.calls("children"),
                vec![
                    "children F None",
                    "children F Some(\"2\")",
                    "children S None",
                    "children DP None"
                ]
            );

            let (base, _) = fake_graph().await;
            let all = onedrive(&base).list_files(None).await.unwrap();
            assert!(paths(&all).contains(&"/top.pdf"));
            assert!(paths(&all).contains(&"/Notes/Sub/Deeper/d.pdf"));
        }

        #[tokio::test]
        async fn list_files_caps_depth_and_fails_on_repeated_links() {
            let (base, fake) = fake_graph().await;
            let d = onedrive(&base);
            let deep = d.list_files(Some("deep0")).await.unwrap();
            assert_eq!(deep.len(), MAX_LIST_DEPTH);
            assert_eq!(fake.calls("children").len(), MAX_LIST_DEPTH);
            let deepest = deep.iter().map(|f| f.path.matches('/').count()).max();
            assert_eq!(deepest, Some(MAX_LIST_DEPTH));

            let err = d.list_files(Some("stuck")).await.unwrap_err();
            assert!(
                matches!(err, IntegrationError::Api(ref m) if m.contains("repeated")),
                "{err}"
            );
        }

        #[tokio::test]
        async fn changes_start_from_the_latest_token() {
            let (base, fake) = fake_graph().await;
            let (files, cursor) = onedrive(&base)
                .get_changes_in(Some("F"), None)
                .await
                .unwrap();
            assert!(files.is_empty());
            assert_eq!(
                cursor,
                Some(format!("{}/me/drive/root/delta?token=D0", base))
            );
            assert_eq!(fake.calls("delta"), vec!["delta latest"]);
        }

        #[tokio::test]
        async fn changes_follow_pages_and_are_scoped_to_the_folder() {
            let (base, fake) = fake_graph().await;
            let d = onedrive(&base);
            let cursor = format!("{}/me/drive/root/delta?token=D0", base);
            let (files, next) = d.get_changes_in(Some("F"), Some(&cursor)).await.unwrap();
            assert_eq!(next, Some(format!("{}/me/drive/root/delta?token=D1", base)));
            // Deletions first; each item's last state; outside the folder, under an unsafe
            // or missing folder, in a parent cycle, or nameless: dropped. The deletion of the
            // old `re.pdf` is superseded by the new one.
            let got: Vec<(&str, &str, bool)> = files
                .iter()
                .map(|f| (f.path.as_str(), f.id.as_str(), f.deleted))
                .collect();
            assert_eq!(
                got,
                vec![
                    ("/gone.pdf", "G", true),
                    ("/new.pdf", "N", false),
                    ("/Sub/y.pdf", "Y", false),
                    ("/Sub/y2.pdf", "Y2", false),
                    ("/second.pdf", "R", false),
                    ("/re.pdf", "NEWRE", false),
                ]
            );
            assert_eq!(fake.calls("delta"), vec!["delta D0", "delta D0p2"]);
            // Folders in the delta aren't fetched; others at most once each.
            let mut items = fake.calls("item");
            let n = items.len();
            items.sort();
            items.dedup();
            assert_eq!(n, items.len(), "{items:?}");
            assert!(!items.iter().any(|c| c == "item F" || c == "item ROOTID"));
            assert!(items.contains(&"item S".to_string()));

            // Whole drive: paths from the root, the root item itself skipped.
            let (files, _) = d.get_changes(Some(&cursor)).await.unwrap();
            assert_eq!(
                paths(&files),
                vec![
                    "/Notes",
                    "/Notes/Sub/y.pdf",
                    "/Notes/Sub/y2.pdf",
                    "/Notes/gone.pdf",
                    "/Notes/new.pdf",
                    "/Notes/re.pdf",
                    "/Notes/second.pdf",
                    "/Other/out.pdf",
                ]
            );
        }

        #[tokio::test]
        async fn changes_errors() {
            let (base, _) = fake_graph().await;
            let d = onedrive(&base);
            let link = |t: &str| format!("{}/me/drive/root/delta?token={}", base, t);

            let err = d
                .get_changes_in(Some("F"), Some(&link("expired")))
                .await
                .unwrap_err();
            assert!(matches!(err, IntegrationError::ResyncRequired(_)), "{err}");
            for bad in ["loop", "none"] {
                let err = d
                    .get_changes_in(Some("F"), Some(&link(bad)))
                    .await
                    .unwrap_err();
                assert!(matches!(err, IntegrationError::Api(_)), "{bad}: {err}");
            }
            let (files, next) = d
                .get_changes_in(Some("F"), Some(&link("empty")))
                .await
                .unwrap();
            assert!(files.is_empty());
            assert_eq!(next, Some(link("E1")));
        }

        fn sync_for(base: &str, root: &std::path::Path, token: &str) -> CloudSync<OneDrive> {
            let config = SyncConfig {
                local_path: root.to_path_buf(),
                cloud_folder: Some("F".into()),
                direction: SyncDirection::Download,
                ..Default::default()
            };
            let state = SyncState {
                cursor: Some(format!("{}/me/drive/root/delta?token={}", base, token)),
                ..Default::default()
            };
            CloudSync::with_state(onedrive(base), config, state)
        }

        #[tokio::test]
        async fn delta_sync_applies_scoped_changes_and_skips_deletions() {
            let (base, _) = fake_graph().await;
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("gone.pdf"), "mine").unwrap();
            let mut sync = sync_for(&base, dir.path(), "D0");
            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            assert_eq!((r.downloaded, r.deleted), (5, 0));
            assert_eq!(std::fs::read(dir.path().join("Sub/y.pdf")).unwrap(), b"Y");
            assert_eq!(std::fs::read(dir.path().join("re.pdf")).unwrap(), b"NEWRE");
            assert_eq!(std::fs::read(dir.path().join("gone.pdf")).unwrap(), b"mine");
            assert!(!dir.path().join("out.pdf").exists());
            assert_eq!(
                sync.state().cursor,
                Some(format!("{}/me/drive/root/delta?token=D1", base))
            );
        }

        #[tokio::test]
        async fn delta_sync_resyncs_after_410() {
            let (base, fake) = fake_graph().await;
            let dir = tempfile::tempdir().unwrap();
            let mut sync = sync_for(&base, dir.path(), "expired");
            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            assert_eq!(r.downloaded, 4);
            assert_eq!(
                std::fs::read(dir.path().join("Sub/Deeper/d.pdf")).unwrap(),
                b"D"
            );
            assert_eq!(
                sync.state().cursor,
                Some(format!("{}/me/drive/root/delta?token=D0", base))
            );
            assert_eq!(fake.calls("delta"), vec!["delta expired", "delta latest"]);
        }
    }

    #[test]
    fn path_segments_are_encoded_but_slashes_kept() {
        assert_eq!(
            encode_path("My Notes/50% #1?/café.pdf"),
            "My%20Notes/50%25%20%231%3F/caf%C3%A9.pdf"
        );
        assert_eq!(encode_path("a.pdf"), "a.pdf");
    }
}
