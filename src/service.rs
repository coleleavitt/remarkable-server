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

/// Beta program state. xochitl 3.28 requires exactly 200 with `enrolled`; POST (enroll)
/// and DELETE (un-enroll) replies go through the same parser. There is no local beta
/// channel, so enrollment is recorded but changes nothing. `project_key` is never sent
/// (the tablet writes it to a crash-reporting key file).
fn beta_state(enrolled: bool) -> Json<Value> {
    Json(json!({ "enrolled": enrolled }))
}

fn beta_enrolled() -> &'static std::sync::atomic::AtomicBool {
    static ENROLLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    &ENROLLED
}

pub async fn get_beta() -> Json<Value> {
    beta_state(beta_enrolled().load(std::sync::atomic::Ordering::Relaxed))
}

pub async fn post_beta(body: String) -> Json<Value> {
    tracing::info!(body, "beta enrollment requested (no local beta channel)");
    beta_enrolled().store(true, std::sync::atomic::Ordering::Relaxed);
    beta_state(true)
}

pub async fn delete_beta() -> Json<Value> {
    beta_enrolled().store(false, std::sync::atomic::Ordering::Relaxed);
    beta_state(false)
}

/// Search index settings (xochitl 3.27+): GET/PATCH `{searchEnabled, language?}`, the
/// reply echoing the stored object. Persisted next to the blobs.
fn search_settings_path(state: &AppState) -> std::path::PathBuf {
    state.storage.base_path().join("search_settings.json")
}

fn load_search_settings(state: &AppState) -> Value {
    std::fs::read(search_settings_path(state)).ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_else(|| json!({ "searchEnabled": true }))
}

pub async fn get_search_settings(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    Ok(Json(load_search_settings(&state)))
}

pub async fn patch_search_settings(State(state): State<AppState>, headers: HeaderMap, Json(patch): Json<Value>) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let mut current = load_search_settings(&state);
    if let (Some(cur), Some(upd)) = (current.as_object_mut(), patch.as_object()) {
        for key in ["searchEnabled", "language"] {
            if let Some(v) = upd.get(key) { cur.insert(key.into(), v.clone()); }
        }
    }
    std::fs::write(search_settings_path(&state), serde_json::to_vec_pretty(&current)?)?;
    Ok(Json(current))
}

/// Client-side search errors (`{error:{category,message,id}}`): logged only.
pub async fn search_error(State(state): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    tracing::warn!(error = %body["error"], "tablet reported a search error");
    Ok(StatusCode::NO_CONTENT)
}

/// Enterprise device management (mdm-agent): no instructions are ever queued locally.

/// Third-party storage integrations (Google Drive, Dropbox, ...). None are configured.
pub async fn list_integrations(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    Ok(Json(json!({ "integrations": [] })))
}

/// `/discovery/v1/webapp`: the device only uses the host (https, no port).

/// Messaging integration: POST /integrations/v2/messaging/{instance_id}/message
/// Used to send documents via messaging services (Slack, email, etc.).
/// No integrations are configured locally, so this accepts but does nothing.
pub async fn send_integration_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(instance_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    tracing::info!(instance_id = %instance_id, "messaging integration message (no-op)");
    Ok(Json(json!({ "status": "ok", "instance_id": instance_id })))
}


pub async fn discovery_webapp(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "Host": state.devices.get_endpoint(), "Status": "OK" }))
}
