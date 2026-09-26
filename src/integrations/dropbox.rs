//! Dropbox integration
//!
//! Full read/write access via Dropbox API v2.

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
}

/// Map a non-success Dropbox response to an error. A missing path (409 `path/not_found`, or a
/// plain 404) is [`IntegrationError::NotFound`] and content that can't be downloaded (409
/// `unsupported_file`, e.g. Paper docs) is [`IntegrationError::NotDownloadable`], both
/// permanent; auth, rate limits and 5xx stay retryable.
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
            } else if body.contains("unsupported_file") {
                IntegrationError::NotDownloadable(body)
            } else if body.contains("insufficient_space") {
                IntegrationError::QuotaExceeded
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
    #[serde(rename = ".tag")]
    #[allow(dead_code)]
    tag: String,
    id: Option<String>,
    name: String,
    #[allow(dead_code)]
    path_lower: Option<String>,
    path_display: Option<String>,
    #[serde(default)]
    size: u64,
    server_modified: Option<String>,
    content_hash: Option<String>,
}

impl DropboxEntry {
    fn to_cloud_file(&self) -> CloudFile {
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
            path: self.path_display.clone().unwrap_or_default(),
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

    async fn list_files(&self, folder_id: Option<&str>) -> Result<Vec<CloudFile>> {
        #[derive(Serialize)]
        struct ListFolderArg<'a> {
            path: &'a str,
            recursive: bool,
            include_mounted_folders: bool,
            include_non_downloadable_files: bool,
        }

        let path = folder_id.unwrap_or("");
        let arg = ListFolderArg {
            path,
            recursive: false,
            include_mounted_folders: true,
            include_non_downloadable_files: false,
        };

        let response: ListFolderResponse = self.api_request("files/list_folder", &arg).await?;

        let mut files: Vec<CloudFile> = response
            .entries
            .into_iter()
            .map(|e| e.to_cloud_file())
            .collect();

        // Handle pagination
        let mut cursor = response.cursor;
        let mut has_more = response.has_more;

        while has_more {
            #[derive(Serialize)]
            struct ContinueArg<'a> {
                cursor: &'a str,
            }

            let cont: ListFolderResponse = self
                .api_request(
                    "files/list_folder/continue",
                    &ContinueArg { cursor: &cursor },
                )
                .await?;

            files.extend(cont.entries.into_iter().map(|e| e.to_cloud_file()));
            cursor = cont.cursor;
            has_more = cont.has_more;
        }

        Ok(files)
    }

    async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
        // List all folders recursively from root
        #[derive(Serialize)]
        struct ListFolderArg {
            path: String,
            recursive: bool,
        }

        let arg = ListFolderArg {
            path: "".into(),
            recursive: true,
        };

        let response: ListFolderResponse = self.api_request("files/list_folder", &arg).await?;

        let mut folders: Vec<CloudFolder> = response
            .entries
            .into_iter()
            .filter(|e| e.tag == "folder")
            .map(|e| e.to_cloud_folder())
            .collect();

        // Handle pagination
        let mut cursor = response.cursor;
        let mut has_more = response.has_more;

        while has_more {
            #[derive(Serialize)]
            struct ContinueArg<'a> {
                cursor: &'a str,
            }

            let cont: ListFolderResponse = self
                .api_request(
                    "files/list_folder/continue",
                    &ContinueArg { cursor: &cursor },
                )
                .await?;

            folders.extend(
                cont.entries
                    .into_iter()
                    .filter(|e| e.tag == "folder")
                    .map(|e| e.to_cloud_folder()),
            );
            cursor = cont.cursor;
            has_more = cont.has_more;
        }

        Ok(folders)
    }

    async fn get_file_metadata(&self, file_id: &str) -> Result<CloudFile> {
        #[derive(Serialize)]
        struct GetMetadataArg<'a> {
            path: &'a str,
        }

        let response: MetadataResponse = self
            .api_request("files/get_metadata", &GetMetadataArg { path: file_id })
            .await?;

        Ok(response.entry.to_cloud_file())
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

        let path = if let Some(parent) = parent_id {
            format!("{}/{}", parent, name)
        } else {
            format!("/{}", name)
        };

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
        let path = if let Some(parent) = parent_id {
            format!("{}/{}", parent, name)
        } else {
            format!("/{}", name)
        };

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
        if let Some(cursor) = cursor {
            // Get changes since cursor
            #[derive(Serialize)]
            struct ListFolderContinueArg<'a> {
                cursor: &'a str,
            }

            let response: ListFolderResponse = self
                .api_request(
                    "files/list_folder/continue",
                    &ListFolderContinueArg { cursor },
                )
                .await?;

            let files: Vec<CloudFile> = response
                .entries
                .into_iter()
                .map(|e| e.to_cloud_file())
                .collect();

            Ok((files, Some(response.cursor)))
        } else {
            // Get initial cursor
            #[derive(Serialize)]
            struct ListFolderArg {
                path: String,
                recursive: bool,
            }

            let response: ListFolderResponse = self
                .api_request(
                    "files/list_folder",
                    &ListFolderArg {
                        path: "".into(),
                        recursive: true,
                    },
                )
                .await?;

            let files: Vec<CloudFile> = response
                .entries
                .into_iter()
                .map(|e| e.to_cloud_file())
                .collect();

            Ok((files, Some(response.cursor)))
        }
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

    /// Deleted paths (409 `path/not_found`) and Paper docs (409 `unsupported_file`) fail
    /// permanently; auth and 5xx stay retryable.
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
        let err = d.download_file("/paper").await.unwrap_err();
        assert!(matches!(err, IntegrationError::NotDownloadable(_)), "{err}");
        for path in ["/flaky", "/auth"] {
            let err = d.download_file(path).await.unwrap_err();
            assert!(!err.is_permanent(), "{path}: {err}");
        }
        assert_eq!(d.download_file("/ok").await.unwrap(), b"content");
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
