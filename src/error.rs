//! Error types for the sync server

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// Server errors
#[derive(Error, Debug)]
pub enum ServerError {
    /// File not found in storage
    #[error("File not found: {0}")]
    NotFound(String),
    
    /// Missing required header
    #[error("Missing header: {0}")]
    MissingHeader(String),
    
    /// Invalid checksum
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    
    /// Invalid hash format
    #[error("Invalid hash: {0}")]
    InvalidHash(String),
    
    /// Storage I/O error
    #[error("Storage error: {0}")]
    Storage(#[from] std::io::Error),
    
    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    /// Authentication required
    #[error("Authentication required")]
    Unauthorized,
    
    /// Invalid token
    #[error("Invalid token")]
    InvalidToken,
    
    /// Conflict during sync
    #[error("Sync conflict: local={local}, remote={remote}")]
    Conflict { local: u64, remote: u64 },
}

/// Error response body
#[derive(Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<String>,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, error, details) = match &self {
            ServerError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", Some(msg.clone())),
            ServerError::MissingHeader(h) => (StatusCode::BAD_REQUEST, "missing_header", Some(h.clone())),
            ServerError::ChecksumMismatch { expected, actual } => (
                StatusCode::BAD_REQUEST,
                "checksum_mismatch",
                Some(format!("expected={}, actual={}", expected, actual)),
            ),
            ServerError::InvalidHash(h) => (StatusCode::BAD_REQUEST, "invalid_hash", Some(h.clone())),
            ServerError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error", None),
            ServerError::Json(_) => (StatusCode::BAD_REQUEST, "json_error", None),
            ServerError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", None),
            ServerError::InvalidToken => (StatusCode::UNAUTHORIZED, "invalid_token", None),
            ServerError::Conflict { local, remote } => (
                StatusCode::CONFLICT,
                "sync_conflict",
                Some(format!("local={}, remote={}", local, remote)),
            ),
        };

        let body = ErrorBody {
            error: error.to_string(),
            details,
        };

        (status, Json(body)).into_response()
    }
}

pub type Result<T> = std::result::Result<T, ServerError>;
