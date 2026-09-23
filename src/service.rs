//! Small cloud endpoints the device calls outside of sync: telemetry, beta settings,
//! integrations listing and webapp discovery. Shapes follow rmfakecloud.

use axum::{extract::State, http::{HeaderMap, StatusCode}, Json};
use serde_json::{json, Value};

use crate::{api::AppState, error::Result};

/// Telemetry / crash / analytics reports (`ping.remarkable.com`). Accepted and dropped.
pub async fn null_report() -> StatusCode { StatusCode::OK }

pub async fn analytics_report() -> (StatusCode, Json<Value>) {
    (StatusCode::CREATED, Json(json!({ "message": "Success" })))
}

pub async fn get_beta() -> Json<Value> {
    Json(json!({ "enrolled": false, "available": true }))
}

/// Beta enrollment toggle; there is no beta channel locally, so just acknowledge it.
pub async fn post_beta(body: String) -> StatusCode {
    tracing::info!(body, "beta enrollment request ignored");
    StatusCode::OK
}

/// Third-party storage integrations (Google Drive, Dropbox, ...). None are configured.
pub async fn list_integrations(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    Ok(Json(json!({ "integrations": [] })))
}

/// `/discovery/v1/webapp`: the device only uses the host (https, no port).
pub async fn discovery_webapp(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "Host": state.devices.get_endpoint(), "Status": "OK" }))
}
