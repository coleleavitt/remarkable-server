pub mod api;
pub mod calendar;
pub mod calendar_api;
pub mod checksum;
pub mod device;
pub mod error;
pub mod integrations;
pub mod storage;
pub mod types;
pub mod readlater;
pub mod readlater_api;

pub use api::AppState;
pub use calendar::{Calendar, CalendarManager, CalendarConfig, CalendarProvider};
pub use calendar_api::CalendarState;
pub use device::DeviceManager;
pub use error::{Result, ServerError};
pub use integrations::{
    CloudProvider, ConflictResolution, ConflictResolver, ConflictStrategy,
    IntegrationManager, OAuthConfig, OAuthToken, PkceFlow, ProviderType,
    SyncConfig, SyncDirection, SyncResult, SyncStatus,
    IntegrationState, integration_router,
};
pub use storage::Storage;
pub use readlater::{
    ReadLaterManager, ReadLaterProvider, Article, ArticleFormat, ReadStatus,
    ProviderAccount, ProviderConfig, SyncSettings, SyncResult as ReadLaterSyncResult,
};
pub use readlater_api::{ReadLaterState, readlater_router};

use axum::{routing::{delete, get, post, put}, Router};
use tower_http::trace::TraceLayer;
use std::path::Path;

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route("/token/json/3/device/delete", post(api::delete_device_token))
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

fn calendar_router(state: CalendarState) -> Router {
    Router::new()
        .route("/", get(calendar_api::list_calendars))
        .route("/", post(calendar_api::add_calendar))
        .route("/upcoming", get(calendar_api::get_upcoming_events))
        .route("/sync-all", post(calendar_api::sync_all_calendars))
        .route("/:id", get(calendar_api::get_calendar))
        .route("/:id", delete(calendar_api::delete_calendar))
        .route("/:id/events", get(calendar_api::get_events))
        .route("/:id/sync", post(calendar_api::sync_calendar_endpoint))
        .route("/:id/meeting-notes", get(calendar_api::list_meeting_notes))
        .route("/:id/events/:event_id/meeting-notes", post(calendar_api::create_meeting_note))
        .route("/webhook", post(calendar_api::calendar_webhook))
        .with_state(state)
}

pub fn create_router_with_all(
    state: AppState, 
    calendar_state: CalendarState,
    readlater_state: ReadLaterState,
) -> Router {
    let sync_router = Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route("/token/json/3/device/delete", post(api::delete_device_token))
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state);
    
    sync_router
        .nest("/integrations/v2/calendars", calendar_router(calendar_state))
        .nest("/integrations/v2/readlater", readlater_router(readlater_state))
        .layer(TraceLayer::new_for_http())
}

pub fn create_router_with_calendar(state: AppState, calendar_state: CalendarState) -> Router {
    let sync_router = Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route("/token/json/3/device/delete", post(api::delete_device_token))
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state);
    
    sync_router
        .nest("/integrations/v2/calendars", calendar_router(calendar_state))
        .layer(TraceLayer::new_for_http())
}

/// Create router with cloud storage integrations
pub fn create_router_with_integrations(
    state: AppState,
    integration_state: IntegrationState,
) -> Router {
    Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route("/token/json/3/device/delete", post(api::delete_device_token))
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state)
        .nest("/integrations/v2/cloud", integration_router(integration_state))
        .layer(TraceLayer::new_for_http())
}

/// Create full router with calendar and cloud integrations
pub fn create_full_router(
    state: AppState,
    calendar_state: CalendarState,
    integration_state: IntegrationState,
) -> Router {
    Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route("/token/json/3/device/delete", post(api::delete_device_token))
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state)
        .nest("/integrations/v2/calendars", calendar_router(calendar_state))
        .nest("/integrations/v2/cloud", integration_router(integration_state))
        .layer(TraceLayer::new_for_http())
}

pub fn init_calendar_manager(storage_path: &Path) -> anyhow::Result<CalendarManager> {
    let db_path = storage_path.join("calendars.db");
    Ok(CalendarManager::new(&db_path)?)
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    pub storage_path: String,
    pub db_path: String,
    pub region: String,
    pub enable_calendar: bool,
    pub enable_cloud_integrations: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".into(),
            storage_path: "./remarkable-storage".into(),
            db_path: "./remarkable-storage/devices.db".into(),
            region: "local".into(),
            enable_calendar: true,
            enable_cloud_integrations: true,
        }
    }
}

pub fn init_readlater_manager(storage_path: &Path) -> anyhow::Result<ReadLaterManager> {
    let db_path = storage_path.join("readlater.db");
    std::fs::create_dir_all(storage_path.join("articles"))?;
    Ok(ReadLaterManager::new(&db_path, storage_path)?)
}
