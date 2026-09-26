//! Email-to-device HTTP API endpoints
//!
//! Provides REST endpoints for email management and statistics.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::email::{EmailRecord, EmailServer, EmailStats};
use crate::error::Result;

/// Email API state
#[derive(Clone)]
pub struct EmailState {
    pub server: EmailServer,
}

impl EmailState {
    pub fn new(server: EmailServer) -> Self {
        Self { server }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListEmailsQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    50
}

/// List emails for a device
pub async fn list_emails(
    State(state): State<EmailState>,
    Path(device_id): Path<String>,
    Query(query): Query<ListEmailsQuery>,
) -> Result<Json<Vec<EmailRecord>>> {
    let emails = state.server.list_emails(&device_id, query.limit)?;
    Ok(Json(emails))
}

/// Get email server statistics
pub async fn stats(State(state): State<EmailState>) -> Result<Json<EmailStats>> {
    let stats = state.server.stats()?;
    Ok(Json(stats))
}

/// Get email server configuration (non-sensitive parts)
#[derive(Serialize)]
pub struct EmailConfigResponse {
    pub smtp_port: u16,
    pub domain: String,
    pub address_format: String,
    pub supported_formats: Vec<&'static str>,
}

pub async fn config(State(state): State<EmailState>) -> Json<EmailConfigResponse> {
    let cfg = state.server.config();
    let port = cfg
        .smtp_bind
        .split(':')
        .last()
        .and_then(|p| p.parse().ok())
        .unwrap_or(2525);

    Json(EmailConfigResponse {
        smtp_port: port,
        domain: cfg.domain.clone(),
        address_format: format!("send@{{device-id}}.{}", cfg.domain),
        supported_formats: vec!["pdf", "epub"],
    })
}

/// Health check for email service
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "Email service running")
}
