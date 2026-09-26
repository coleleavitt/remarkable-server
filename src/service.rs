//! Small cloud endpoints the device calls outside of sync: telemetry, beta settings,
//! integrations listing and webapp discovery. Shapes follow rmfakecloud.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use serde_json::{Value, json};

use crate::api::AppState;
use crate::error::Result;

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

/// Enrolling/un-enrolling flips a server-wide flag, so it needs a paired device/user
/// token. xochitl's cloud client sends its `Authorization: Bearer` token on these calls
/// like on every other authenticated cloud request (e.g. `/search/v1/settings`).
pub async fn post_beta(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<Value>> {
    let user = state.auth_user(&headers)?;
    tracing::info!(
        user,
        body,
        "beta enrollment requested (no local beta channel)"
    );
    beta_enrolled().store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(beta_state(true))
}

pub async fn delete_beta(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    beta_enrolled().store(false, std::sync::atomic::Ordering::Relaxed);
    Ok(beta_state(false))
}

/// Search index settings (xochitl 3.27+): GET/PATCH `{searchEnabled, language?}`, the
/// reply echoing the stored object. Persisted next to the blobs.
fn search_settings_path(state: &AppState) -> std::path::PathBuf {
    state.storage.base_path().join("search_settings.json")
}

fn load_search_settings(state: &AppState) -> Value {
    std::fs::read(search_settings_path(state))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_else(|| json!({ "searchEnabled": true }))
}

pub async fn get_search_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    Ok(Json(load_search_settings(&state)))
}

pub async fn patch_search_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(patch): Json<Value>,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    // Serialize read-modify-write so concurrent PATCHes (different keys) don't drop each
    // other's update; write via temp+rename so a crash never leaves a torn file.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut current = load_search_settings(&state);
    if let (Some(cur), Some(upd)) = (current.as_object_mut(), patch.as_object()) {
        for key in ["searchEnabled", "language"] {
            if let Some(v) = upd.get(key) {
                cur.insert(key.into(), v.clone());
            }
        }
    }
    crate::storage::atomic_write(
        &search_settings_path(&state),
        &serde_json::to_vec_pretty(&current)?,
    )?;
    Ok(Json(current))
}

/// Client-side search errors (`{error:{category,message,id}}`): logged only.
pub async fn search_error(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    tracing::warn!(error = %body["error"], "tablet reported a search error");
    Ok(StatusCode::NO_CONTENT)
}

/// Enterprise device management (mdm-agent): no instructions are ever queued locally.

/// Third-party storage integrations (Google Drive, Dropbox, ...). None are configured.
pub async fn list_integrations(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    use crate::api::AppState;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    fn setup() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let token = devices.create_user_token("u1@test").unwrap();
        let state = AppState::new(Storage::new(tmp.path().join("storage")).unwrap(), devices);
        (state, token, tmp)
    }

    async fn call(
        state: &AppState,
        method: &str,
        uri: &str,
        auth: Option<&str>,
        body: &str,
    ) -> (StatusCode, serde_json::Value) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(t) = auth {
            req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        if !body.is_empty() {
            req = req.header(header::CONTENT_TYPE, "application/json");
        }
        let resp = crate::create_router(state.clone())
            .oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or_default())
    }

    #[tokio::test]
    async fn beta_writes_require_a_token_reads_do_not() {
        let (state, token, _tmp) = setup();
        for m in ["POST", "DELETE"] {
            let (s, _) = call(&state, m, "/settings/v1/beta", None, "").await;
            assert_eq!(s, StatusCode::UNAUTHORIZED, "{m} without token");
            let (s, _) = call(&state, m, "/settings/v1/beta", Some("bogus"), "").await;
            assert_eq!(s, StatusCode::UNAUTHORIZED, "{m} with bad token");
        }
        let (s, v) = call(&state, "POST", "/settings/v1/beta", Some(&token), "{}").await;
        assert_eq!((s, v["enrolled"].as_bool()), (StatusCode::OK, Some(true)));
        let (s, v) = call(&state, "GET", "/settings/v1/beta", None, "").await;
        assert_eq!((s, v["enrolled"].as_bool()), (StatusCode::OK, Some(true)));
        let (s, v) = call(&state, "DELETE", "/settings/v1/beta", Some(&token), "").await;
        assert_eq!((s, v["enrolled"].as_bool()), (StatusCode::OK, Some(false)));
    }

    #[tokio::test]
    async fn concurrent_search_settings_patches_are_not_lost() {
        let (state, token, _tmp) = setup();
        let mut tasks = Vec::new();
        for i in 0..20 {
            let (state, token) = (state.clone(), token.clone());
            tasks.push(tokio::spawn(async move {
                let body = if i % 2 == 0 {
                    r#"{"searchEnabled":false}"#
                } else {
                    r#"{"language":"de"}"#
                };
                call(&state, "PATCH", "/search/v1/settings", Some(&token), body)
                    .await
                    .0
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), StatusCode::OK);
        }
        let (_, v) = call(&state, "GET", "/search/v1/settings", Some(&token), "").await;
        assert_eq!(v["searchEnabled"], false);
        assert_eq!(v["language"], "de");
        // Written via temp+rename: no temp files left beside it.
        let leftovers: Vec<_> = std::fs::read_dir(state.storage.base_path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty());
    }
}
