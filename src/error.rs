use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Missing header: {0}")]
    MissingHeader(String),
    #[error("Invalid header: {0}")]
    InvalidHeader(String),
    #[error("Checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("Invalid hash: {0}")]
    InvalidHash(String),
    #[error("Generation mismatch: current {current}")]
    GenerationMismatch { current: u64 },
    #[error("Storage error: {0}")]
    Storage(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Invalid token")]
    InvalidToken,
    #[error("Invalid code: {0}")]
    InvalidCode(String),
    #[error("Token error: {0}")]
    TokenError(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Email error: {0}")]
    Email(String),
    #[error("IO error: {0}")]
    Io(String),
    #[error("Config error: {0}")]
    Config(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Bad request: {0}")]
    BadRequest(String),
}

impl From<rusqlite::Error> for ServerError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Database(e.to_string())
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    details: Option<String>,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, err, det) = match &self {
            Self::NotFound(m) => (StatusCode::NOT_FOUND, "not_found", Some(m.clone())),
            Self::MissingHeader(h) => (StatusCode::BAD_REQUEST, "missing_header", Some(h.clone())),
            Self::InvalidHeader(h) => (StatusCode::BAD_REQUEST, "invalid_header", Some(h.clone())),
            Self::ChecksumMismatch { expected, actual } => (
                StatusCode::BAD_REQUEST,
                "checksum_mismatch",
                Some(format!("expected={}, actual={}", expected, actual)),
            ),
            Self::InvalidHash(h) => (StatusCode::BAD_REQUEST, "invalid_hash", Some(h.clone())),
            Self::GenerationMismatch { current } => (
                StatusCode::PRECONDITION_FAILED,
                "generation_mismatch",
                Some(format!("current={current}")),
            ),
            Self::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage_error", None),
            Self::Json(_) => (StatusCode::BAD_REQUEST, "json_error", None),
            Self::Database(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                Some(m.clone()),
            ),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", None),
            Self::InvalidToken => (StatusCode::UNAUTHORIZED, "invalid_token", None),
            Self::InvalidCode(m) => (StatusCode::BAD_REQUEST, "invalid_code", Some(m.clone())),
            Self::TokenError(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_error",
                Some(m.clone()),
            ),
            Self::Internal(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                Some(m.clone()),
            ),
            Self::Email(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "email_error",
                Some(m.clone()),
            ),
            Self::Io(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "io_error",
                Some(m.clone()),
            ),
            Self::Config(m) => (StatusCode::BAD_REQUEST, "config_error", Some(m.clone())),
            Self::Forbidden(m) => (StatusCode::FORBIDDEN, "forbidden", Some(m.clone())),
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, "bad_request", Some(m.clone())),
        };
        (
            status,
            Json(ErrorBody {
                error: err.into(),
                details: det,
            }),
        )
            .into_response()
    }
}

pub type Result<T> = std::result::Result<T, ServerError>;
