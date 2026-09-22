//! Local reMarkable Sync Server
//!
//! A local implementation of the reMarkable sync v3 API for testing
//! and offline development.
//!
//! # Endpoints
//!
//! ## Sync v3
//! - `GET /sync/v3/root` - Get current root hash
//! - `GET /sync/v3/files/{hash}` - Download file by hash
//! - `PUT /sync/v3/files/{hash}` - Upload file with validation
//!
//! ## Token (Mock)
//! - `POST /token/json/2/user/new` - Refresh user token
//! - `POST /token/json/2/device/new` - Register device
//!
//! ## Discovery
//! - `GET /discovery/v1/endpoints` - Service discovery
//!
//! ## Debug
//! - `GET /health` - Health check with stats
//! - `GET /debug/files` - List all stored files
//! - `DELETE /debug/clear` - Clear all storage
//!
//! # Usage
//!
//! ```ignore
//! use remarkable_server::{Storage, create_router, AppState};
//!
//! let storage = Storage::new("./data")?;
//! let state = AppState::new(storage);
//! let router = create_router(state);
//!
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! axum::serve(listener, router).await?;
//! ```

pub mod api;
pub mod checksum;
pub mod error;
pub mod storage;
pub mod types;

pub use api::AppState;
pub use error::{Result, ServerError};
pub use storage::Storage;

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use tower_http::trace::TraceLayer;

/// Create the router with all endpoints
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Sync v3 endpoints
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route("/sync/v3/files/{hash}", put(api::put_file))
        // Token endpoints (mock)
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        // Discovery
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/document-storage", get(api::discovery))
        // Health & Debug
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        // State and tracing
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address
    pub bind: String,
    /// Storage directory
    pub storage_path: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".to_string(),
            storage_path: "./remarkable-storage".to_string(),
        }
    }
}
