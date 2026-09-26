//! OneDrive integration
//!
//! Full read/write access via Microsoft Graph API.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::integrations::oauth::{OAuthConfig, OAuthToken, refresh_token};
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

/// OneDrive provider
pub struct OneDrive {
    config: OAuthConfig,
    token: Option<OAuthToken>,
    client: Client,
}

impl OneDrive {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            token: None,
            client: crate::integrations::http_client(),
        }
    }

    pub fn with_token(config: OAuthConfig, token: OAuthToken) -> Self {
        Self {
            config,
            token: Some(token),
            client: crate::integrations::http_client(),
        }
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
        let status = response.status();

        if status.is_success() {
            response
                .json()
                .await
                .map_err(|e| IntegrationError::Serialization(e.to_string()))
        } else if status.as_u16() == 401 {
            Err(IntegrationError::TokenExpired)
        } else if status.as_u16() == 429 {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            Err(IntegrationError::RateLimited {
                retry_after_secs: retry_after,
            })
        } else if status.as_u16() == 404 {
            Err(IntegrationError::NotFound("Item not found".into()))
        } else if status.as_u16() == 507 {
            Err(IntegrationError::QuotaExceeded)
        } else if status.as_u16() == 409 {
            let body = response.text().await.unwrap_or_default();
            Err(IntegrationError::Conflict(body))
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(IntegrationError::Api(format!("{}: {}", status, body)))
        }
    }
}

/// OneDrive item (file or folder)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveItem {
    id: String,
    name: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    folder: Option<FolderFacet>,
    #[serde(default)]
    file: Option<FileFacet>,
    last_modified_date_time: Option<String>,
    parent_reference: Option<ParentReference>,
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
        let is_folder = self.folder.is_some();
        let mime_type = self.file.as_ref().and_then(|f| f.mime_type.clone());
        let content_hash = self
            .file
            .as_ref()
            .and_then(|f| f.hashes.as_ref())
            .and_then(|h| h.sha256_hash.clone().or(h.quick_xor_hash.clone()));

        let path = self
            .parent_reference
            .as_ref()
            .and_then(|p| p.path.as_ref())
            .map(|p| format!("{}/{}", p.replace("/drive/root:", ""), self.name))
            .unwrap_or_else(|| format!("/{}", self.name));

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

    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        let url = if let Some(id) = folder_id {
            format!("{}/me/drive/items/{}/children", GRAPH_BASE, id)
        } else {
            format!("{}/me/drive/root/children", GRAPH_BASE)
        };

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let list: ListChildrenResponse = self.handle_response(response).await?;

        let mut files: Vec<CloudFile> = list.value.into_iter().map(|i| i.to_cloud_file()).collect();

        // Handle pagination
        let mut next_link = list.next_link;
        while let Some(link) = next_link {
            let response = self
                .request(reqwest::Method::GET, &link)
                .await?
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            let page: ListChildrenResponse = self.handle_response(response).await?;
            files.extend(page.value.into_iter().map(|i| i.to_cloud_file()));
            next_link = page.next_link;
        }

        Ok(files)
    }

    async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
        // Use search to find all folders
        let url = format!(
            "{}/me/drive/root/search(q='')?$filter=folder ne null",
            GRAPH_BASE
        );

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let list: ListChildrenResponse = self.handle_response(response).await?;

        let mut folders: Vec<CloudFolder> = list
            .value
            .into_iter()
            .filter(|i| i.folder.is_some())
            .map(|i| i.to_cloud_folder())
            .collect();

        // Handle pagination
        let mut next_link = list.next_link;
        while let Some(link) = next_link {
            let response = self
                .request(reqwest::Method::GET, &link)
                .await?
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            let page: ListChildrenResponse = self.handle_response(response).await?;
            folders.extend(
                page.value
                    .into_iter()
                    .filter(|i| i.folder.is_some())
                    .map(|i| i.to_cloud_folder()),
            );
            next_link = page.next_link;
        }

        Ok(folders)
    }

    async fn get_file_metadata(&self, file_id: &str) -> Result<CloudFile> {
        let url = format!("{}/me/drive/items/{}", GRAPH_BASE, file_id);

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
        let url = format!("{}/me/drive/items/{}/content", GRAPH_BASE, file_id);

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

            let content = self
                .client
                .get(download_url)
                .send()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?
                .bytes()
                .await
                .map_err(|e| IntegrationError::Network(e.to_string()))?;

            return Ok(content.to_vec());
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(IntegrationError::Api(format!("{}: {}", status, body)));
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
            format!("{}/me/drive/items/{}:/{}:/content", GRAPH_BASE, id, name)
        } else {
            format!("{}/me/drive/root:/{}:/content", GRAPH_BASE, name)
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
            format!("{}/me/drive/items/{}/children", GRAPH_BASE, id)
        } else {
            format!("{}/me/drive/root/children", GRAPH_BASE)
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
        let url = format!("{}/me/drive/items/{}", GRAPH_BASE, file_id);

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
        let url = format!("{}/me/drive/items/{}", GRAPH_BASE, file_id);

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
        let url = if let Some(delta_link) = cursor {
            // Use delta link directly
            delta_link.to_string()
        } else {
            format!("{}/me/drive/root/delta", GRAPH_BASE)
        };

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let delta: DeltaResponse = self.handle_response(response).await?;

        let files: Vec<CloudFile> = delta.value.into_iter().map(|i| i.to_cloud_file()).collect();

        // Return the delta link for next sync
        let next_cursor = delta.delta_link.or(delta.next_link);

        Ok((files, next_cursor))
    }

    async fn get_quota(&self) -> Result<StorageQuota> {
        let url = format!("{}/me/drive", GRAPH_BASE);

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
                GRAPH_BASE, id, name
            )
        } else {
            format!(
                "{}/me/drive/root:/{}:/createUploadSession",
                GRAPH_BASE, name
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
