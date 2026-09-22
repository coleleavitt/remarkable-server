use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Not found: {0}")] NotFound(String),
    #[error("Missing header: {0}")] MissingHeader(String),
    #[error("Checksum mismatch")] ChecksumMismatch { expected: String, actual: String },
    #[error("Invalid hash: {0}")] InvalidHash(String),
    #[error("Storage error: {0}")] Storage(#[from] std::io::Error),
    #[error("JSON error: {0}")] Json(#[from] serde_json::Error),
    #[error("Unauthorized")] Unauthorized,
    #[error("Invalid token")] InvalidToken,
    #[error("Invalid code: {0}")] InvalidCode(String),
    #[error("Token error: {0}")] TokenError(String),
    #[error("Database error: {0}")] Database(String),
}

#[derive(Serialize)] struct ErrorBody { error: String, details: Option<String> }

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, err, det) = match &self {
            Self::NotFound(m) => (StatusCode::NOT_FOUND, "not_found", Some(m.clone())),
            Self::MissingHeader(h) => (StatusCode::BAD_REQUEST, "missing_header", Some(h.clone())),
            Self::ChecksumMismatch { expected, actual } => (StatusCode::BAD_REQUEST, "checksum_mismatch", Some(format!("expected={}, actual={}", expected, actual))),
            Self::InvalidHash(h) => (StatusCode::BAD_REQUEST, "invalid_hash", Some(h.clone())),
            Self::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error", None),
            Self::Json(_) => (StatusCode::BAD_REQUEST, "json_error", None),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", None),
            Self::InvalidToken => (StatusCode::UNAUTHORIZED, "invalid_token", None),
            Self::InvalidCode(m) => (StatusCode::BAD_REQUEST, "invalid_code", Some(m.clone())),
            Self::TokenError(m) => (StatusCode::INTERNAL_SERVER_ERROR, "token_error", Some(m.clone())),
            Self::Database(m) => (StatusCode::INTERNAL_SERVER_ERROR, "database_error", Some(m.clone())),
        };
        (status, Json(ErrorBody { error: err.into(), details: det })).into_response()
    }
}
pub type Result<T> = std::result::Result<T, ServerError>;
