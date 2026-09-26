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

/// OneDrive's `quickXorHash` of `content`, in base64: byte `i` is XORed into a 160-bit value at
/// bit `11 * i` (wrapping round), and the length, as 8 little-endian bytes, into its last 8
/// bytes (<https://learn.microsoft.com/onedrive/developer/code-snippets/quickxorhash>).
fn quick_xor_hash(content: &[u8]) -> String {
    use base64::Engine;
    const BITS: usize = 160;
    let mut hash = [0u8; BITS / 8];
    for (i, &b) in content.iter().enumerate() {
        let at = (i % BITS) * 11 % BITS;
        let (byte, shift) = (at / 8, at % 8);
        hash[byte] ^= b << shift;
        if shift > 0 {
            hash[(byte + 1) % hash.len()] ^= b >> (8 - shift);
        }
    }
    let len = (content.len() as u64).to_le_bytes();
    for (h, l) in hash[BITS / 8 - len.len()..].iter_mut().zip(len) {
        *h ^= l;
    }
    base64::engine::general_purpose::STANDARD.encode(hash)
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

    /// Whether `url` has the Graph endpoint's origin (scheme, host and port), the only place the
    /// bearer token may go. Graph hands back full URLs (`@odata.nextLink`, `@odata.deltaLink`)
    /// that are followed as they are and stored as delta cursors, so none of them is trusted
    /// to point there.
    fn on_graph(&self, url: &str) -> bool {
        match (
            reqwest::Url::parse(url),
            reqwest::Url::parse(&self.graph_base),
        ) {
            (Ok(url), Ok(base)) => url.origin() == base.origin(),
            _ => false,
        }
    }

    /// `link`, if it is on the Graph endpoint (see [`on_graph`](Self::on_graph)); an error
    /// naming only its origin otherwise.
    fn graph_link(&self, link: String) -> Result<String> {
        if self.on_graph(&link) {
            return Ok(link);
        }
        let origin = reqwest::Url::parse(&link)
            .map(|u| u.origin().ascii_serialization())
            .unwrap_or_else(|_| "an unparseable URL".into());
        Err(IntegrationError::Api(format!(
            "refusing a Graph link to {} (not {})",
            origin, self.graph_base
        )))
    }

    /// A request carrying the bearer token. Refused (nothing sent) unless `url` is on the
    /// Graph endpoint: this is the one place the token is attached, so no link can leak it.
    async fn request(&self, method: reqwest::Method, url: &str) -> Result<reqwest::RequestBuilder> {
        let url = self.graph_link(url.to_string())?;
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

    /// The item `root` is: `.../me/drive/root` or `.../me/drive/items/{id}`.
    fn root_url(&self, root: &SyncRoot<'_>) -> String {
        match root {
            SyncRoot::Folder(id) => format!("{}/me/drive/items/{}", self.graph_base, id),
            SyncRoot::Drive => format!("{}/me/drive/root", self.graph_base),
        }
    }

    fn children_url(&self, root: &SyncRoot<'_>) -> String {
        format!("{}/children", self.root_url(root))
    }

    /// `action` (`content`, `createUploadSession`) on the item at `path`, a `/`-separated
    /// path below `root`: `root:/{path}:/{action}` or `items/{id}:/{path}:/{action}`.
    fn path_url(&self, root: &SyncRoot<'_>, path: &str, action: &str) -> String {
        format!("{}:/{}:/{}", self.root_url(root), encode_path(path), action)
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
                Some(next) => url = self.graph_link(next)?,
                None => return Ok(items),
            }
        }
    }

    /// One page of a delta query. Only here does 410 mean the token expired (Graph's
    /// `resyncRequired`); elsewhere it means the item is gone.
    async fn delta_page(&self, url: &str) -> Result<DeltaResponse> {
        let response = self
            .request(reqwest::Method::GET, url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;
        if response.status() == reqwest::StatusCode::GONE {
            return Err(IntegrationError::ResyncRequired(
                response.text().await.unwrap_or_default(),
            ));
        }
        self.handle_response(response).await
    }

    /// Every item of a delta query from `url` through its last page, and the
    /// `@odata.deltaLink` for next time. A 410 (expired token) is
    /// [`IntegrationError::ResyncRequired`]. A link off the Graph endpoint is an error, so it
    /// is neither followed nor stored as the next cursor.
    async fn delta_all(&self, url: &str) -> Result<(Vec<DriveItem>, String)> {
        let mut items = Vec::new();
        let mut used = HashSet::from([url.to_string()]);
        let mut url = url.to_string();
        loop {
            let page = self.delta_page(&url).await?;
            items.extend(page.value);
            match (page.delta_link, page.next_link) {
                (Some(done), _) => return Ok((items, self.graph_link(done)?)),
                (None, Some(next)) if !used.insert(next.clone()) => {
                    return Err(IntegrationError::Api(format!(
                        "Graph returned repeated delta nextLink {:?}",
                        next
                    )));
                }
                (None, Some(next)) => url = self.graph_link(next)?,
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
    /// `None` if its chain of parents doesn't reach the root (outside the sync folder, deleted,
    /// unsafe name, a cycle) or doesn't within [`MAX_LIST_DEPTH`] folders. Delta responses carry
    /// no `parentReference.path`, so the chain is walked by id through `folders`: it starts with
    /// the folders in the delta itself, and any other folder is fetched once and added.
    ///
    /// Every folder on a walk that reached an answer is memoized in `paths`. A walk cut short by
    /// the depth limit memoizes nothing: a folder on it may be only a few levels below the root
    /// and reached by a shorter walk from another item, so caching "not under the root" for it
    /// would make an item's fate depend on the order of the delta.
    async fn folder_path(
        &self,
        id: &str,
        root: &SyncRoot<'_>,
        folders: &mut HashMap<String, Option<FolderInfo>>,
        paths: &mut HashMap<String, Option<String>>,
    ) -> Result<Option<String>> {
        // Folders walked through, innermost first, with their names.
        let mut walked: Vec<(String, String)> = Vec::new();
        let mut cur = id.to_string();
        let settled = loop {
            if matches!(root, SyncRoot::Folder(r) if *r == cur) {
                break Some(String::new());
            }
            if let Some(hit) = paths.get(&cur) {
                break hit.clone();
            }
            if walked.iter().any(|(w, _)| *w == cur) {
                break None; // a cycle never reaches the root
            }
            if walked.len() >= MAX_LIST_DEPTH {
                return Ok(None);
            }
            if !folders.contains_key(&cur) {
                let info = self.folder_info(&cur).await?;
                folders.insert(cur.clone(), info);
            }
            match &folders[&cur] {
                Some(info) if info.is_drive_root => {
                    break matches!(root, SyncRoot::Drive).then(String::new);
                }
                Some(FolderInfo {
                    name,
                    parent: Some(parent),
                    ..
                }) if is_safe_name(name) => {
                    let parent = parent.clone();
                    walked.push((std::mem::replace(&mut cur, parent), name.clone()));
                }
                _ => break None,
            }
        };
        let mut path = settled;
        for (folder, name) in walked.into_iter().rev() {
            path = path.map(|prefix| format!("{}/{}", prefix, name));
            paths.insert(folder, path.clone());
        }
        Ok(path)
    }
}

/// The folder listings, change paths, uploads and new folders are relative to: the whole
/// drive, or a folder by item id. `root` is Graph's alias for the drive root, and `""` means
/// it too, so every call agrees on what a sync folder is.
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
        // Gone for good. A delta query's 410 (expired token) never gets here: `delta_page`
        // turns it into `ResyncRequired` first. Anywhere else, e.g. a download, the file
        // can't be fetched however often it's retried.
        410 => IntegrationError::NotFound("Item gone".into()),
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
    /// relative to that folder (`/Sub/dir/file.pdf`), matching the local scan, so a nested file
    /// is compared with its local copy rather than seen as missing. Each item is listed once,
    /// the first wins a duplicate path, names that aren't a single safe path segment are
    /// skipped (with everything under them), and folders deeper than [`MAX_LIST_DEPTH`] aren't
    /// entered.
    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        // The same root as `get_changes_in`, so `""` and `root` both mean the drive root.
        let root = SyncRoot::of(folder_id);
        let mut seen_items = HashSet::new();
        if let SyncRoot::Folder(id) = root {
            seen_items.insert(id.to_string());
        }
        let mut seen_paths = HashSet::new();
        let mut queue = VecDeque::from([(self.children_url(&root), String::new(), 0usize)]);
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
                        let children = self.children_url(&SyncRoot::Folder(&item.id));
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

    /// Before #34 a folder's listing pathed each item by its `parentReference.path` with
    /// `/drive/root:` removed, its path from the drive root, so the folder was kept under the
    /// components of its own path, worked out here the same way. `root` was listed the same way
    /// it is now, `""` not at all, and so was the drive root by its real id (its children's
    /// path is `/drive/root:`), recognized by its `root` facet. A folder with no
    /// `parentReference.path`, or a path the old layout couldn't have written locally, gives
    /// `None`.
    async fn legacy_layout_dir(&self, folder_id: Option<&str>) -> Result<Option<Vec<String>>> {
        let root = SyncRoot::of(folder_id);
        if matches!(root, SyncRoot::Drive) {
            return Ok(None);
        }
        let folder: DriveItem = self.get_json(&self.root_url(&root)).await?;
        if folder.root.is_some() {
            return Ok(None);
        }
        let Some(parent) = folder
            .parent_reference
            .as_ref()
            .and_then(|p| p.path.as_ref())
        else {
            return Ok(None);
        };
        let path = format!("{}/{}", parent.replace("/drive/root:", ""), folder.name);
        let parts: Vec<String> = path
            .split('/')
            .filter(|p| !p.is_empty())
            .map(str::to_owned)
            .collect();
        Ok(parts.iter().all(|p| is_safe_name(p)).then_some(parts))
    }

    /// Compares `content_hash` (Graph's `sha256Hash`, else its `quickXorHash`) with the same
    /// hash of `content`.
    fn content_matches(&self, file: &CloudFile, content: &[u8]) -> bool {
        use sha2::{Digest, Sha256};
        file.content_hash.as_deref().is_some_and(|h| {
            h.eq_ignore_ascii_case(&hex::encode(Sha256::digest(content)))
                || h == quick_xor_hash(content)
        })
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

        // The same root as the listing, so `""` uploads to the drive root it listed.
        let url = self.path_url(&SyncRoot::of(parent_id), name, "content");

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
        let url = self.children_url(&SyncRoot::of(parent_id));

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
    /// [`IntegrationError::ResyncRequired`], and so is a stored cursor that isn't on the Graph
    /// endpoint: it is never sent the token, and starting over from a fresh cursor is how to get
    /// past it.
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
        if !self.on_graph(cursor) {
            return Err(IntegrationError::ResyncRequired(format!(
                "stored delta link is not on {}",
                self.graph_base
            )));
        }
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
        let mut folders: HashMap<String, Option<FolderInfo>> = items
            .iter()
            .filter(|i| i.folder.is_some() || i.root.is_some())
            .map(|i| (i.id.clone(), Some(FolderInfo::from(*i))))
            .collect();
        let mut folder_paths = HashMap::new();
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
                    self.folder_path(parent, &root, &mut folders, &mut folder_paths)
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
        let session_url = self.path_url(&SyncRoot::of(parent_id), name, "createUploadSession");

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

    /// A deleted item (404 or 410, directly or at the redirected download URL) fails
    /// permanently; a 410 here is not an expired delta token. Throttling and 5xx stay
    /// retryable.
    #[tokio::test]
    async fn download_error_mapping() {
        let app = axum::Router::new()
            .route(
                "/me/drive/items/{id}/content",
                axum::routing::get(|Path(id): Path<String>| async move {
                    match id.as_str() {
                        "gone" => StatusCode::NOT_FOUND.into_response(),
                        "expired" => StatusCode::GONE.into_response(),
                        "moved" => {
                            (StatusCode::FOUND, [(header::LOCATION, "/blob/gone")]).into_response()
                        }
                        "moved-expired" => {
                            (StatusCode::FOUND, [(header::LOCATION, "/blob/expired")])
                                .into_response()
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
                    match name.as_str() {
                        "ok" => "content".into_response(),
                        "expired" => StatusCode::GONE.into_response(),
                        _ => StatusCode::NOT_FOUND.into_response(),
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

        for id in ["gone", "moved", "expired", "moved-expired"] {
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
        use crate::integrations::sync::{
            CloudSync,
            SyncConfig,
            SyncDirection,
            SyncState,
            SyncStatus,
        };

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
        /// Folder `id` whose parent is at `path`.
        fn at(id: &str, name: &str, path: &str) -> Value {
            json!({ "id": id, "name": name, "folder": { "childCount": 1 },
                    "parentReference": { "id": "P", "path": path } })
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

        /// Folders `GET items/{id}` knows (for path resolution); anything else is 404. `chainN`
        /// is `N` levels below `F`, each named `k`.
        fn item(id: &str) -> Option<Value> {
            Some(match id {
                "ROOTID" => drive_root(),
                "F" => folder("F", "Notes", "ROOTID"),
                // With the `parentReference.path` Graph gives for an item looked up by id.
                "DOCN" => at("DOCN", "Notes", "/drive/root:/Documents"),
                "TOPN" => at("TOPN", "Notes", "/drive/root:"),
                "ENC" => at("ENC", "My Notes", "/drive/root:/My%20Documents"),
                "S" => folder("S", "Sub", "F"),
                "O" => folder("O", "Other", "ROOTID"),
                "X" => folder("X", "..", "F"),
                "c1" => folder("c1", "c1", "c2"),
                "c2" => folder("c2", "c2", "c1"),
                chain if chain.starts_with("chain") => {
                    let n: usize = chain["chain".len()..].parse().ok()?;
                    let parent = match n {
                        1 => "F".to_string(),
                        n => format!("chain{}", n - 1),
                    };
                    folder(chain, "k", &parent)
                }
                _ => return None,
            })
        }

        /// A file 10 levels below `F` and one 70 levels below it (too deep), on the same chain
        /// of folders, which the delta doesn't include: the shallow one's folders are also on
        /// the deep one's walk.
        fn deep_and_shallow() -> [Value; 2] {
            [
                file("DEEP", "deep.pdf", "chain70"),
                file("SHALLOW", "s.pdf", "chain10"),
            ]
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
                // Neither the sync folder nor the drive root is in this delta: both are
                // reached through `GET items/{id}`.
                "rootless" => (
                    vec![file("Y", "y.pdf", "S"), file("OUT", "out.pdf", "O")],
                    Some(("delta", "RL1")),
                ),
                "deep-first" => (deep_and_shallow().to_vec(), Some(("delta", "DF1"))),
                "shallow-first" => {
                    let [deep, shallow] = deep_and_shallow();
                    (vec![shallow, deep], Some(("delta", "SF1")))
                }
                "empty" => (vec![], Some(("delta", "E1"))),
                "loop" => (vec![], Some(("next", "loop"))),
                "none" => (vec![], None),
                // Links to `Fake::foreign` rather than back to this server.
                "leak-next" => (
                    vec![file("N", "new.pdf", "F")],
                    Some(("foreign-next", "D0")),
                ),
                "leak-delta" => (
                    vec![file("N", "new.pdf", "F")],
                    Some(("foreign-delta", "D1")),
                ),
                _ => return None,
            })
        }

        #[derive(Clone, Default)]
        struct Fake {
            base: Arc<Mutex<String>>,
            /// Base of another server, for links that point away from this one.
            foreign: Arc<Mutex<String>>,
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

        /// `children` of `items/{id}`, logged by id, or of `root` (the drive root route), logged
        /// as `(root)`. `items/leak/children` pages on to `Fake::foreign`.
        async fn children_route(
            State(fake): State<Fake>,
            id: Option<Path<String>>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            let (id, label) = id.map_or(("root".to_string(), "(root)".to_string()), |Path(id)| {
                (id.clone(), id)
            });
            let page = q.get("page").map(String::as_str);
            fake.log(format!("children {} {:?}", label, page));
            if id == "leak" {
                let foreign = fake.foreign.lock().unwrap().clone();
                let next = format!("{}/me/drive/items/F/children", foreign);
                return Json(json!({ "value": [], "@odata.nextLink": next }));
            }
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
            let mut body = json!({ "value": value });
            if let Some((kind, token)) = link {
                let (base, kind) = match kind.strip_prefix("foreign-") {
                    Some(kind) => (fake.foreign.lock().unwrap().clone(), kind),
                    None => (fake.base.lock().unwrap().clone(), kind),
                };
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

        /// A server that answers anything and records each request and whether it carried an
        /// `Authorization` header. The bearer token must never reach it.
        async fn foreign_server() -> (String, Arc<Mutex<Vec<String>>>) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let app = axum::Router::new()
                .fallback(
                    |State(seen): State<Arc<Mutex<Vec<String>>>>,
                     uri: axum::http::Uri,
                     headers: axum::http::HeaderMap| async move {
                        let auth = headers.contains_key(axum::http::header::AUTHORIZATION);
                        seen.lock().unwrap().push(format!("{} auth={}", uri, auth));
                        Json(json!({ "value": [] }))
                    },
                )
                .with_state(seen.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (base, seen)
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

        /// `""` and Graph's `root` alias list the drive root through the same route as no
        /// folder at all, as the change feed already treated them.
        #[tokio::test]
        async fn list_files_treats_root_aliases_as_the_drive_root() {
            let (base, fake) = fake_graph().await;
            let d = onedrive(&base);
            let all = d.list_files(None).await.unwrap();
            for alias in ["", "root"] {
                let again = d.list_files(Some(alias)).await.unwrap();
                assert_eq!(paths(&again), paths(&all), "{alias:?}");
            }
            let children = fake.calls("children");
            let at_root: Vec<&String> = children
                .iter()
                .filter(|c| c.starts_with("children (root)"))
                .collect();
            assert_eq!(at_root.len(), 3, "{children:?}");
            assert!(!children.iter().any(|c| c.starts_with("children root")));
        }

        /// A Graph stand-in that records `METHOD path` of every request, after reading its
        /// body. Listings are empty, `createUploadSession` hands out `{base}/session`, and any
        /// other write answers with an item.
        async fn recording_graph() -> (String, Arc<Mutex<Vec<String>>>) {
            use axum::http::{Method, Uri};

            #[derive(Clone, Default)]
            struct Rec {
                base: Arc<Mutex<String>>,
                seen: Arc<Mutex<Vec<String>>>,
            }
            let rec = Rec::default();
            let app =
                axum::Router::new()
                    .fallback(
                        |State(rec): State<Rec>,
                         method: Method,
                         uri: Uri,
                         body: axum::body::Body| async move {
                            axum::body::to_bytes(body, usize::MAX).await.unwrap();
                            let path = uri.path().to_string();
                            rec.seen
                                .lock()
                                .unwrap()
                                .push(format!("{} {}", method, path));
                            if method == Method::GET {
                                Json(json!({ "value": [] })).into_response()
                            } else if path.ends_with(":/createUploadSession") {
                                let base = rec.base.lock().unwrap().clone();
                                Json(json!({
                                    "uploadUrl": format!("{}/session", base),
                                    "expirationDateTime": "2030-01-01T00:00:00Z",
                                }))
                                .into_response()
                            } else {
                                (StatusCode::CREATED, Json(file("NEW", "new", "P"))).into_response()
                            }
                        },
                    )
                    .with_state(rec.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            *rec.base.lock().unwrap() = base.clone();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (base, rec.seen)
        }

        /// Uploads (simple and resumable) and new folders go where the listing looked: `""`
        /// and `root` are the drive root for writes too (`root:/…`, `root/children`), never
        /// `items/:/…` or `items//children`, so a sync of `""` can upload what it lists.
        #[tokio::test]
        async fn writes_treat_root_aliases_as_the_drive_root() {
            let (base, seen) = recording_graph().await;
            let d = onedrive(&base);
            let big = vec![0u8; 4 * 1024 * 1024 + 1]; // over the simple-upload limit
            for folder in [None, Some(""), Some("root"), Some("F")] {
                seen.lock().unwrap().clear();
                d.list_files(folder).await.unwrap();
                d.upload_file_at(folder, &["dir", "x.pdf"], b"x", None)
                    .await
                    .unwrap();
                d.upload_file_at(folder, &["big.bin"], &big, None)
                    .await
                    .unwrap();
                d.create_folder(folder, "New").await.unwrap();
                let at = if folder == Some("F") {
                    "items/F"
                } else {
                    "root"
                };
                assert_eq!(
                    *seen.lock().unwrap(),
                    vec![
                        format!("GET /me/drive/{at}/children"),
                        format!("PUT /me/drive/{at}:/dir/x.pdf:/content"),
                        format!("POST /me/drive/{at}:/big.bin:/createUploadSession"),
                        "PUT /session".to_string(),
                        format!("POST /me/drive/{at}/children"),
                    ],
                    "{folder:?}"
                );
            }

            // A full sync of `""` lists the drive root and uploads into it.
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("x.pdf"), "x").unwrap();
            seen.lock().unwrap().clear();
            let config = SyncConfig {
                local_path: dir.path().to_path_buf(),
                cloud_folder: Some(String::new()),
                ..Default::default()
            };
            let r = CloudSync::new(d, config).sync().await.unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            assert_eq!((r.status, r.uploaded), (SyncStatus::Success, 1));
            assert_eq!(
                *seen.lock().unwrap(),
                vec![
                    "GET /me/drive/root/children",
                    "PUT /me/drive/root:/x.pdf:/content"
                ]
            );
        }

        /// A folder other than the drive root was kept under its own path from the drive root
        /// before #34, worked out from its `parentReference.path` as the listing did then. The
        /// root aliases need no request; the drive root by its real id, and a folder with no
        /// path, had no such layout.
        #[tokio::test]
        async fn legacy_layout_dir_is_the_folders_path_from_the_drive_root() {
            let (base, fake) = fake_graph().await;
            let d = onedrive(&base);
            for root in [None, Some(""), Some("root"), Some("ROOTID"), Some("F")] {
                assert_eq!(d.legacy_layout_dir(root).await.unwrap(), None, "{root:?}");
            }
            assert_eq!(fake.calls("item"), vec!["item ROOTID", "item F"]);
            for (id, dir) in [
                ("DOCN", vec!["Documents", "Notes"]),
                ("TOPN", vec!["Notes"]),
                ("ENC", vec!["My%20Documents", "My Notes"]),
            ] {
                let got = d.legacy_layout_dir(Some(id)).await.unwrap();
                assert_eq!(got.unwrap(), dir, "{id}");
            }
            let err = d.legacy_layout_dir(Some("missing")).await.unwrap_err();
            assert!(matches!(err, IntegrationError::NotFound(_)), "{err}");
        }

        /// The first full sync of such a folder moves `<local>/Documents/Notes` aside, however
        /// it was cased or percent-encoded; later ones don't.
        #[tokio::test]
        async fn full_sync_moves_the_old_layout_aside_once() {
            let (base, fake) = fake_graph().await;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            std::fs::create_dir_all(root.join("documents/Notes")).unwrap();
            std::fs::write(root.join("documents/Notes/a.pdf"), "old a").unwrap();
            let config = SyncConfig {
                local_path: root.to_path_buf(),
                cloud_folder: Some("DOCN".into()),
                direction: SyncDirection::Download,
                ..Default::default()
            };
            let r = CloudSync::new(onedrive(&base), config.clone())
                .sync()
                .await
                .unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            assert_eq!(r.notices.len(), 1, "{:?}", r.notices);
            assert!(
                r.notices[0]
                    .starts_with("Moved documents/Notes to .rms-old-layout/documents/Notes: ")
            );
            assert_eq!(
                std::fs::read(root.join(".rms-old-layout/documents/Notes/a.pdf")).unwrap(),
                b"old a"
            );

            std::fs::create_dir_all(root.join("documents/Notes")).unwrap();
            let r = CloudSync::new(onedrive(&base), config)
                .sync()
                .await
                .unwrap();
            assert!(r.errors.is_empty() && r.notices.is_empty(), "{r:?}");
            assert!(root.join("documents/Notes").exists());
            assert_eq!(fake.calls("item"), vec!["item DOCN", "item DOCN"]);
        }

        /// Neither the sync folder nor the drive root is in the delta, so the parent chains are
        /// walked through `GET items/{id}` and the drive root is recognized by its `root`
        /// facet: inside when syncing the whole drive, outside when syncing a folder.
        #[tokio::test]
        async fn changes_resolve_the_drive_root_by_lookup() {
            let (base, fake) = fake_graph().await;
            let d = onedrive(&base);
            let cursor = format!("{}/me/drive/root/delta?token=rootless", base);

            let (files, _) = d.get_changes(Some(&cursor)).await.unwrap();
            assert_eq!(paths(&files), vec!["/Notes/Sub/y.pdf", "/Other/out.pdf"]);
            let mut items = fake.calls("item");
            items.sort();
            assert_eq!(items, vec!["item F", "item O", "item ROOTID", "item S"]);

            fake.log.lock().unwrap().clear();
            let (files, _) = d.get_changes_in(Some("F"), Some(&cursor)).await.unwrap();
            assert_eq!(paths(&files), vec!["/Sub/y.pdf"]);
            let mut items = fake.calls("item");
            items.sort();
            assert_eq!(items, vec!["item O", "item ROOTID", "item S"]);
        }

        /// An item's fate doesn't depend on the order of the delta. Walking up from the deep
        /// file gives up at the depth limit, partway along the chain the shallow file reaches
        /// the folder through; that must not mark those folders as outside it.
        #[tokio::test]
        async fn depth_limited_walks_do_not_hide_shallow_items() {
            let (base, fake) = fake_graph().await;
            let d = onedrive(&base);
            let shallow = format!("{}/s.pdf", "/k".repeat(10));
            for token in ["deep-first", "shallow-first"] {
                fake.log.lock().unwrap().clear();
                let cursor = format!("{}/me/drive/root/delta?token={}", base, token);
                let (files, _) = d.get_changes_in(Some("F"), Some(&cursor)).await.unwrap();
                assert_eq!(paths(&files), vec![shallow.as_str()], "{token}");
                // Each folder is fetched once, however many walks pass through it.
                let items = fake.calls("item");
                let unique: HashSet<&String> = items.iter().collect();
                assert_eq!(unique.len(), items.len(), "{token}: {items:?}");
            }
        }

        /// The bearer token only goes to the Graph endpoint's origin. A nextLink or deltaLink
        /// pointing anywhere else is an error, neither followed nor handed back as a cursor; a
        /// stored cursor pointing elsewhere is never sent the token either, and asks for a
        /// resync instead.
        #[tokio::test]
        async fn links_off_the_graph_endpoint_are_refused() {
            let (base, fake) = fake_graph().await;
            let (foreign, seen) = foreign_server().await;
            *fake.foreign.lock().unwrap() = foreign.clone();
            let d = onedrive(&base);
            let refused = |err: &IntegrationError| matches!(err, IntegrationError::Api(m) if m.contains("refusing") && m.contains(&foreign));

            let err = d.list_files(Some("leak")).await.unwrap_err();
            assert!(refused(&err), "{err}");
            for token in ["leak-next", "leak-delta"] {
                let cursor = format!("{}/me/drive/root/delta?token={}", base, token);
                let err = d
                    .get_changes_in(Some("F"), Some(&cursor))
                    .await
                    .unwrap_err();
                assert!(refused(&err), "{token}: {err}");
            }
            let stored = format!("{}/me/drive/root/delta?token=D0", foreign);
            let err = d
                .get_changes_in(Some("F"), Some(&stored))
                .await
                .unwrap_err();
            assert!(matches!(err, IntegrationError::ResyncRequired(_)), "{err}");

            assert!(seen.lock().unwrap().is_empty(), "{:?}", seen.lock());
            assert_eq!(
                fake.calls("delta"),
                vec!["delta leak-next", "delta leak-delta"]
            );
        }

        /// Scheme, host and port must all match; path and query don't matter.
        #[test]
        fn graph_origin_check() {
            let d = onedrive(GRAPH_BASE);
            for ok in [
                "https://graph.microsoft.com/v1.0/me/drive/root/delta?token=x",
                "https://GRAPH.microsoft.com:443/v1.0/x",
                "https://graph.microsoft.com/beta/x",
            ] {
                assert!(d.on_graph(ok), "{ok}");
            }
            for bad in [
                "http://graph.microsoft.com/v1.0/x",
                "https://graph.microsoft.com:8443/v1.0/x",
                "https://graph.microsoft.com.evil.example/v1.0/x",
                "https://graph.microsoft.com@evil.example/v1.0/x",
                "https://evil.example/v1.0/x?https://graph.microsoft.com",
                "//graph.microsoft.com/v1.0/x",
                "/me/drive/root/delta",
                "not a url",
                "",
            ] {
                assert!(!d.on_graph(bad), "{bad}");
            }
        }

        fn sync_for(base: &str, root: &std::path::Path, cursor: &str) -> CloudSync<OneDrive> {
            let config = SyncConfig {
                local_path: root.to_path_buf(),
                cloud_folder: Some("F".into()),
                direction: SyncDirection::Download,
                ..Default::default()
            };
            let state = SyncState {
                cursor: Some(cursor.into()),
                ..Default::default()
            };
            CloudSync::with_state(onedrive(base), config, state)
        }

        fn delta_link(base: &str, token: &str) -> String {
            format!("{}/me/drive/root/delta?token={}", base, token)
        }

        /// A stored cursor off the Graph endpoint is replaced through a resync; the other
        /// server never hears from us.
        #[tokio::test]
        async fn delta_sync_resyncs_from_a_cursor_off_the_graph_endpoint() {
            let (base, fake) = fake_graph().await;
            let (foreign, seen) = foreign_server().await;
            let dir = tempfile::tempdir().unwrap();
            let mut sync = sync_for(&base, dir.path(), &delta_link(&foreign, "D0"));
            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            assert_eq!(r.downloaded, 4);
            assert_eq!(sync.state().cursor, Some(delta_link(&base, "D0")));
            assert_eq!(fake.calls("delta"), vec!["delta latest"]);
            assert!(seen.lock().unwrap().is_empty(), "{:?}", seen.lock());
        }

        #[tokio::test]
        async fn delta_sync_applies_scoped_changes_and_skips_deletions() {
            let (base, _) = fake_graph().await;
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("gone.pdf"), "mine").unwrap();
            let mut sync = sync_for(&base, dir.path(), &delta_link(&base, "D0"));
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
            let mut sync = sync_for(&base, dir.path(), &delta_link(&base, "expired"));
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

    /// OneDrive's quickXorHash: known values, and the byte-at-a-time implementation against the
    /// definition taken bit by bit, across the wrap at 160 bits and many lengths.
    #[test]
    fn quick_xor_hash_matches_its_definition() {
        use base64::Engine;
        assert_eq!(quick_xor_hash(b""), "AAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        assert_eq!(quick_xor_hash(&[0x4a]), "SgAAAAAAAAAAAAAAAQAAAAAAAAA=");
        assert_eq!(
            quick_xor_hash(&[0xb5, 0xb4]),
            "taAFAAAAAAAAAAAAAgAAAAAAAAA="
        );
        let by_bits = |data: &[u8]| {
            let mut bits = [false; 160];
            for (i, b) in data.iter().enumerate() {
                for k in 0..8 {
                    if b >> k & 1 == 1 {
                        let at = (11 * i + k) % 160;
                        bits[at] = !bits[at];
                    }
                }
            }
            let mut out = [0u8; 20];
            for (at, set) in bits.iter().enumerate() {
                if *set {
                    out[at / 8] |= 1 << (at % 8);
                }
            }
            for (o, l) in out[12..].iter_mut().zip((data.len() as u64).to_le_bytes()) {
                *o ^= l;
            }
            base64::engine::general_purpose::STANDARD.encode(out)
        };
        let data: Vec<u8> = (0..1000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        for len in (0..400).chain([999, 1000]) {
            assert_eq!(quick_xor_hash(&data[..len]), by_bits(&data[..len]), "{len}");
        }
    }

    /// Graph's `sha256Hash` (upper-case hex) or `quickXorHash`, whichever the listing kept.
    #[test]
    fn content_matches_either_hash() {
        use sha2::{Digest, Sha256};
        let d = OneDrive::new(OAuthConfig::onedrive(
            "id".into(),
            None,
            "http://x/cb".into(),
        ));
        let file = |hash: Option<String>| CloudFile {
            id: "X".into(),
            name: "x".into(),
            mime_type: None,
            size: 3,
            modified_at: 0,
            content_hash: hash,
            parent_id: None,
            is_folder: false,
            path: "/x".into(),
            deleted: false,
        };
        let sha = hex::encode_upper(Sha256::digest(b"abc"));
        assert!(d.content_matches(&file(Some(sha.clone())), b"abc"));
        assert!(d.content_matches(&file(Some(quick_xor_hash(b"abc"))), b"abc"));
        assert!(!d.content_matches(&file(Some(sha)), b"abd"));
        assert!(!d.content_matches(&file(Some(quick_xor_hash(b"abc"))), b"abd"));
        assert!(!d.content_matches(&file(None), b"abc"));
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
