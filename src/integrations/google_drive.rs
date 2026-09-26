//! Google Drive integration
//!
//! Full read/write access via Drive API v3.

use crate::integrations::{
    oauth::{refresh_token, OAuthConfig, OAuthToken},
    CloudFile, CloudFolder, CloudProvider, IntegrationError, ProviderType, Result, StorageQuota,
};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

const API_BASE: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD_BASE: &str = "https://www.googleapis.com/upload/drive/v3";

/// Google Drive provider
pub struct GoogleDrive {
    config: OAuthConfig,
    token: Option<OAuthToken>,
    client: Client,
}

impl GoogleDrive {
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

    /// Make authenticated request, auto-refreshing if needed
    async fn request(&self, method: reqwest::Method, url: &str) -> Result<reqwest::RequestBuilder> {
        let token = self.access_token()?;
        Ok(self.client.request(method, url).bearer_auth(token))
    }

    /// Find a non-trashed child folder of `parent` named `name`.
    async fn find_folder(&self, parent: &str, name: &str) -> Result<Option<String>> {
        let esc = |s: &str| s.replace('\\', "\\\\").replace('\'', "\\'");
        let query = format!(
            "name = '{}' and '{}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            esc(name), esc(parent)
        );
        let url = format!("{}/files?q={}&fields=files(id,name,mimeType,parents)", API_BASE, urlencoding::encode(&query));
        let response = self.request(reqwest::Method::GET, &url).await?.send().await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;
        let list: ListFilesResponse = self.handle_response(response).await?;
        Ok(list.files.into_iter().next().map(|f| f.id))
    }

    /// Handle API response, checking for errors
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
        } else if status.as_u16() == 403 {
            let body = response.text().await.unwrap_or_default();
            if body.contains("storageQuotaExceeded") {
                Err(IntegrationError::QuotaExceeded)
            } else {
                Err(IntegrationError::Api(format!("Forbidden: {}", body)))
            }
        } else if status.as_u16() == 404 {
            Err(IntegrationError::NotFound("File not found".into()))
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(IntegrationError::Api(format!("{}: {}", status, body)))
        }
    }
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
        }
    }
}

/// List files response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListFilesResponse {
    files: Vec<DriveFile>,
    #[allow(dead_code)]
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

    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        let parent = folder_id.unwrap_or("root");
        let query = format!("'{}' in parents and trashed = false", parent);
        
        let url = format!(
            "{}/files?q={}&fields=files(id,name,mimeType,size,modifiedTime,md5Checksum,parents),nextPageToken",
            API_BASE,
            urlencoding::encode(&query)
        );

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let list: ListFilesResponse = self.handle_response(response).await?;
        
        Ok(list
            .files
            .into_iter()
            .map(|f| f.to_cloud_file(format!("/{}", f.name)))
            .collect())
    }

    async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
        let query = "mimeType = 'application/vnd.google-apps.folder' and trashed = false";
        let url = format!(
            "{}/files?q={}&fields=files(id,name,parents)",
            API_BASE,
            urlencoding::encode(query)
        );

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let list: ListFilesResponse = self.handle_response(response).await?;
        
        Ok(list
            .files
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
            API_BASE, file_id
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
        let url = format!("{}/files/{}?alt=media", API_BASE, file_id);

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

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
            UPLOAD_BASE
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
        let (name, dirs) = components.split_last().ok_or_else(|| IntegrationError::InvalidPath("empty path".into()))?;
        let mut parent = parent_id.unwrap_or("root").to_string();
        for dir in dirs {
            parent = match self.find_folder(&parent, dir).await? {
                Some(id) => id,
                None => self.create_folder(Some(&parent), dir).await?.id,
            };
        }
        let mut file = self.upload_file(Some(&parent), name, content, mime_type).await?;
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

        let url = format!("{}/files?fields=id,name,parents", API_BASE);
        
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
        let url = format!("{}/files/{}", API_BASE, file_id);

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
            API_BASE, file_id, new_parent_id, old_parent
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
        // Get start page token if no cursor
        let page_token = if let Some(c) = cursor {
            c.to_string()
        } else {
            let url = format!("{}/changes/startPageToken", API_BASE);
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
            "{}/changes?pageToken={}&fields=changes(fileId,file(id,name,mimeType,size,modifiedTime,md5Checksum,parents),removed),newStartPageToken,nextPageToken",
            API_BASE, page_token
        );

        let response = self
            .request(reqwest::Method::GET, &url)
            .await?
            .send()
            .await
            .map_err(|e| IntegrationError::Network(e.to_string()))?;

        let changes: ChangesResponse = self.handle_response(response).await?;
        
        let files: Vec<CloudFile> = changes
            .changes
            .into_iter()
            .filter_map(|c| {
                if c.removed == Some(true) {
                    None
                } else {
                    c.file.map(|f| f.to_cloud_file(format!("/{}", f.name)))
                }
            })
            .collect();

        let next_cursor = changes
            .new_start_page_token
            .or(changes.next_page_token);

        Ok((files, next_cursor))
    }

    async fn get_quota(&self) -> Result<StorageQuota> {
        let url = format!("{}/about?fields=storageQuota", API_BASE);

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
