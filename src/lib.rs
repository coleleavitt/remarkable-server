//! remarkable-server - Local reMarkable sync server

pub mod api;
pub mod checksum;
pub mod device;
pub mod error;
pub mod integrations;
pub mod storage;
pub mod types;

pub use api::AppState;
pub use device::DeviceManager;
pub use error::{Result, ServerError};
pub use integrations::IntegrationStore;
pub use storage::Storage;

use axum::{routing::{delete, get, patch, post, put}, Router};
use tower_http::trace::TraceLayer;

pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Sync v3 API
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        
        // Device management
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        
        // Token management
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route("/token/json/3/device/delete", post(api::delete_device_token))
        
        // Discovery
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        
        // Integrations API
        .route("/integrations/v2/providers", get(integrations::list_providers))
        .route("/integrations/v2/instances", get(integrations::list_instances))
        .route("/integrations/v2/instances/{id}", delete(integrations::delete_instance))
        .route("/integrations/v2/instances/{id}", patch(integrations::update_instance))
        .route("/integrations/v2/instances/{id}/sync", post(integrations::trigger_sync))
        .route("/integrations/v2/instances/{id}/status", get(integrations::get_sync_status))
        .route("/integrations/v2/auth/{provider}/authcode", get(integrations::get_auth_code))
        .route("/integrations/v2/auth/{provider}/callback", post(integrations::auth_callback))
        .route("/integrations/v2/admin/tos", get(integrations::get_tos))
        .route("/integrations/v2/admin/tos", put(integrations::accept_tos))
        .route("/integrations/v2/local", post(integrations::create_local_integration))
        
        // Admin/debug
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    pub storage_path: String,
    pub db_path: String,
    pub region: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".into(),
            storage_path: "./remarkable-storage".into(),
            db_path: "./remarkable-storage/devices.db".into(),
            region: "local".into(),
        }
    }
}
