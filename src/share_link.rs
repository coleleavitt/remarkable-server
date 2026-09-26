//! Share a page as a link (xochitl 3.27+, "share and convert").
//!
//! `POST /share/v1/link`: multipart with a `metadata` JSON part
//! (`{DocID, MailList:[], Pages:[]}`) and a `page` PNG; the device expects `{"link": url}`.
//! The PNG is stored under a random id and served at that URL without auth, so anyone
//! with the link can view it (that is the feature); ids are v4 UUIDs, not guessable.

use axum::Json;
use axum::extract::{Multipart, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::api::AppState;
use crate::error::{Result, ServerError};

fn shared_dir(state: &AppState) -> std::path::PathBuf {
    state.storage.base_path().join("shared")
}

fn bad(e: impl std::fmt::Display) -> ServerError {
    ServerError::Config(e.to_string())
}

pub async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut form: Multipart,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let (mut metadata, mut png) = (Value::Null, None);
    while let Some(field) = form.next_field().await.map_err(bad)? {
        match field.name().unwrap_or_default() {
            "metadata" => {
                metadata =
                    serde_json::from_str(&field.text().await.map_err(bad)?).unwrap_or(Value::Null)
            }
            "page" => png = Some(field.bytes().await.map_err(bad)?),
            _ => {}
        }
    }
    let png = png.ok_or_else(|| bad("missing 'page'"))?;
    if !png.starts_with(b"\x89PNG") {
        return Err(bad("'page' is not a PNG"));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let dir = shared_dir(&state);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(format!("{id}.png")), &png)?;
    let link = format!(
        "https://{}/share/v1/link/{id}.png",
        state.devices.get_endpoint()
    );
    tracing::info!(doc = %metadata["DocID"], %link, bytes = png.len(), "page shared as link");
    Ok(Json(json!({ "link": link })))
}

/// Serve a shared page. Only `<uuid>.png` names are accepted.
pub async fn get(State(state): State<AppState>, Path(name): Path<String>) -> Result<Response> {
    let id = name
        .strip_suffix(".png")
        .ok_or_else(|| ServerError::NotFound(name.clone()))?;
    uuid::Uuid::parse_str(id).map_err(|_| ServerError::NotFound(name.clone()))?;
    let data = std::fs::read(shared_dir(&state).join(format!("{id}.png")))
        .map_err(|_| ServerError::NotFound(name.clone()))?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "image/png")], data).into_response())
}
