pub mod api;
pub mod checksum;
pub mod device;
pub mod error;
pub mod storage;
pub mod types;

pub use api::AppState;
pub use device::DeviceManager;
pub use error::{Result, ServerError};
pub use storage::Storage;

use axum::{routing::{delete, get, post, put}, Router};
use tower_http::trace::TraceLayer;

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
