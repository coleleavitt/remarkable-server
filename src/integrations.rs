//! Integrations API module

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

use crate::api::AppState;
use crate::error::{Result, ServerError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IntegrationProvider {
    GoogleDrive, OneDrive, Dropbox, WebDAV, LocalFolder,
}

impl std::fmt::Display for IntegrationProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GoogleDrive => write!(f, "googledrive"),
            Self::OneDrive => write!(f, "onedrive"),
            Self::Dropbox => write!(f, "dropbox"),
            Self::WebDAV => write!(f, "webdav"),
            Self::LocalFolder => write!(f, "local"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncStatus { Idle, Syncing, Error, Disabled }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Integration {
    pub id: String,
    pub provider: IntegrationProvider,
    pub name: String,
    pub enabled: bool,
    pub sync_status: SyncStatus,
    pub last_sync: Option<DateTime<Utc>>,
    pub folder_path: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderInfo {
    pub id: String,
    pub name: String,
    pub description: String,
    pub requires_oauth: bool,
    pub supported_actions: Vec<String>,
}

#[derive(Clone)]
pub struct IntegrationStore { inner: Arc<IntegrationStoreInner> }

struct IntegrationStoreInner {
    conn: std::sync::Mutex<Connection>,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl IntegrationStore {
    pub fn new(db_path: &std::path::Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS integrations (
                id TEXT PRIMARY KEY, provider TEXT NOT NULL, name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1, sync_status TEXT NOT NULL DEFAULT 'idle',
                last_sync TEXT, folder_path TEXT, error_message TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )", [],
        )?;
        Ok(Self { inner: Arc::new(IntegrationStoreInner { conn: std::sync::Mutex::new(conn), db_path: db_path.to_path_buf() }) })
    }
    
    pub fn list(&self) -> Result<Vec<Integration>> {
        let conn = self.inner.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, provider, name, enabled, sync_status, last_sync, folder_path, error_message, created_at, updated_at 
             FROM integrations ORDER BY created_at DESC"
        )?;
        let integrations = stmt.query_map([], |row| {
            let provider_str: String = row.get(1)?;
            let provider = match provider_str.as_str() {
                "googledrive" => IntegrationProvider::GoogleDrive,
                "onedrive" => IntegrationProvider::OneDrive,
                "dropbox" => IntegrationProvider::Dropbox,
                "webdav" => IntegrationProvider::WebDAV,
                _ => IntegrationProvider::LocalFolder,
            };
            let status_str: String = row.get(4)?;
            let sync_status = match status_str.as_str() {
                "syncing" => SyncStatus::Syncing, "error" => SyncStatus::Error, "disabled" => SyncStatus::Disabled, _ => SyncStatus::Idle,
            };
            Ok(Integration {
                id: row.get(0)?, provider, name: row.get(2)?, enabled: row.get::<_, i32>(3)? != 0, sync_status,
                last_sync: row.get::<_, Option<String>>(5)?.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|dt| dt.with_timezone(&Utc)),
                folder_path: row.get(6)?, error_message: row.get(7)?,
                created_at: row.get::<_, String>(8)?.parse().unwrap_or_else(|_| Utc::now()),
                updated_at: row.get::<_, String>(9)?.parse().unwrap_or_else(|_| Utc::now()),
            })
        })?.filter_map(|r| r.ok()).collect();
        Ok(integrations)
    }
    
    pub fn get(&self, id: &str) -> Result<Option<Integration>> {
        self.list().map(|i| i.into_iter().find(|x| x.id == id))
    }
    
    pub fn create(&self, provider: IntegrationProvider, name: &str, folder_path: Option<&str>) -> Result<Integration> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        let conn = self.inner.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO integrations (id, provider, name, folder_path, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![&id, provider.to_string(), name, folder_path, now.to_rfc3339(), now.to_rfc3339()],
        )?;
        Ok(Integration { id, provider, name: name.into(), enabled: true, sync_status: SyncStatus::Idle, last_sync: None, folder_path: folder_path.map(String::from), error_message: None, created_at: now, updated_at: now })
    }
    
    pub fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.inner.conn.lock().unwrap();
        Ok(conn.execute("DELETE FROM integrations WHERE id = ?1", params![id])? > 0)
    }
    
    pub fn update_status(&self, id: &str, status: SyncStatus, error: Option<&str>) -> Result<()> {
        let conn = self.inner.conn.lock().unwrap();
        let s = match status { SyncStatus::Idle => "idle", SyncStatus::Syncing => "syncing", SyncStatus::Error => "error", SyncStatus::Disabled => "disabled" };
        conn.execute("UPDATE integrations SET sync_status = ?1, error_message = ?2, updated_at = ?3 WHERE id = ?4", params![s, error, Utc::now().to_rfc3339(), id])?;
        Ok(())
    }
}

pub async fn list_providers() -> Json<Vec<ProviderInfo>> {
    Json(vec![
        ProviderInfo { id: "googledrive".into(), name: "Google Drive".into(), description: "Sync with Google Drive".into(), requires_oauth: true, supported_actions: vec!["read".into(), "write".into()] },
        ProviderInfo { id: "local".into(), name: "Local Folder".into(), description: "Sync with a local folder".into(), requires_oauth: false, supported_actions: vec!["read".into(), "write".into()] },
    ])
}

pub async fn list_instances(State(state): State<AppState>) -> Result<Json<Vec<Integration>>> { Ok(Json(state.integrations.list()?)) }

pub async fn delete_instance(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse> {
    if state.integrations.delete(&id)? { Ok(StatusCode::NO_CONTENT) } else { Err(ServerError::NotFound(id)) }
}

#[derive(Debug, Deserialize)]
pub struct UpdateIntegrationRequest { pub name: Option<String>, pub enabled: Option<bool>, pub folder_path: Option<String> }

pub async fn update_instance(State(state): State<AppState>, Path(id): Path<String>, Json(_req): Json<UpdateIntegrationRequest>) -> Result<Json<Integration>> {
    state.integrations.get(&id)?.ok_or_else(|| ServerError::NotFound(id)).map(Json)
}

pub async fn trigger_sync(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse> {
    let _ = state.integrations.get(&id)?.ok_or_else(|| ServerError::NotFound(id.clone()))?;
    state.integrations.update_status(&id, SyncStatus::Syncing, None)?;
    state.integrations.update_status(&id, SyncStatus::Idle, None)?;
    Ok(StatusCode::ACCEPTED)
}

#[derive(Debug, Serialize)]
pub struct SyncStatusResponse { pub status: SyncStatus, pub last_sync: Option<DateTime<Utc>>, pub error_message: Option<String> }

pub async fn get_sync_status(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<SyncStatusResponse>> {
    let i = state.integrations.get(&id)?.ok_or_else(|| ServerError::NotFound(id))?;
    Ok(Json(SyncStatusResponse { status: i.sync_status, last_sync: i.last_sync, error_message: i.error_message }))
}

#[derive(Debug, Serialize)]
pub struct AuthCodeResponse { pub auth_url: String, pub state: String }

pub async fn get_auth_code(Path(provider): Path<String>) -> Result<Json<AuthCodeResponse>> {
    let state = uuid::Uuid::new_v4().to_string();
    let auth_url = match provider.as_str() {
        "googledrive" => format!("https://accounts.google.com/o/oauth2/v2/auth?state={}", state),
        _ => return Err(ServerError::NotFound(format!("Unknown provider: {}", provider))),
    };
    Ok(Json(AuthCodeResponse { auth_url, state }))
}

#[derive(Debug, Deserialize)]
pub struct AuthCallbackRequest { pub code: String, pub state: String }

pub async fn auth_callback(State(state): State<AppState>, Path(provider): Path<String>, Json(_req): Json<AuthCallbackRequest>) -> Result<Json<Integration>> {
    let p = match provider.as_str() {
        "googledrive" => IntegrationProvider::GoogleDrive,
        "onedrive" => IntegrationProvider::OneDrive,
        "dropbox" => IntegrationProvider::Dropbox,
        _ => return Err(ServerError::NotFound(format!("Unknown provider: {}", provider))),
    };
    Ok(Json(state.integrations.create(p, &format!("{} Integration", provider), None)?))
}

#[derive(Debug, Serialize)]
pub struct TosResponse { pub accepted: bool, pub version: String, pub accepted_at: Option<DateTime<Utc>> }

pub async fn get_tos() -> Json<TosResponse> { Json(TosResponse { accepted: true, version: "1.0".into(), accepted_at: Some(Utc::now()) }) }

#[derive(Debug, Deserialize)]
pub struct AcceptTosRequest { pub version: String }

pub async fn accept_tos(Json(_req): Json<AcceptTosRequest>) -> impl IntoResponse { StatusCode::OK }

#[derive(Debug, Deserialize)]
pub struct CreateLocalIntegrationRequest { pub name: String, pub folder_path: String }

pub async fn create_local_integration(State(state): State<AppState>, Json(req): Json<CreateLocalIntegrationRequest>) -> Result<Json<Integration>> {
    Ok(Json(state.integrations.create(IntegrationProvider::LocalFolder, &req.name, Some(&req.folder_path))?))
}

