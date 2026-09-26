//! MDM instruction queue (enterprise device management, software 3.28+).
//!
//! Recovered from mdm-agent: it polls `GET /mdm/v1/instruction` (and the legacy
//! `GET /mdm/devices/v0/instruction`) for the next instruction, applies it, and reports
//! `POST /mdm/v1/instruction/status`. This is a real queue: instructions are enqueued via
//! the admin API (e.g. minimumPincodeLength, autoLockMinutes, remoteWipe, lock), handed to
//! the device oldest-first, and cleared once the device reports a terminal status.
//! Instruction shape: {"id","name","data":{"key","value"}}. Status: {"id","status","detail"}.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::AppState;
use crate::error::Result;

const LOCAL_USER: &str = "local-user";

/// `GET /mdm/v1/instruction` (+ `/mdm/devices/v0/instruction`) -> next pending instruction, or 204.
pub async fn get_instruction(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response> {
    let user = state.auth_user(&headers)?;
    match state.devices.mdm_next_pending(&user)? {
        Some((id, name, key, value)) => {
            let mut data = serde_json::Map::new();
            if let Some(k) = key {
                data.insert("key".into(), Value::String(k));
            }
            if let Some(v) = value {
                data.insert("value".into(), Value::String(v));
            }
            Ok((
                StatusCode::OK,
                Json(json!({"id": id, "name": name, "data": Value::Object(data)})),
            )
                .into_response())
        }
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

/// `POST /mdm/v1/instruction/status` -> record the device's report and clear the instruction.
pub async fn post_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    let id = body
        .get("id")
        .or_else(|| body.get("instructionId"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let status = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("received");
    let detail = body
        .get("detail")
        .or_else(|| body.get("details"))
        .or_else(|| body.get("extendedStatus"))
        .and_then(|v| v.as_str());
    if !id.is_empty() {
        state.devices.mdm_set_status(id, status, detail)?;
    }
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
pub struct EnqueueReq {
    pub name: String,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
}

/// `POST /admin/mdm/enqueue` (admin) -> enqueue a policy command for the device. Returns {id}.
pub async fn admin_enqueue(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<EnqueueReq>,
) -> Result<Json<Value>> {
    crate::api::require_admin(&headers)?;
    let id = state.devices.mdm_enqueue(
        LOCAL_USER,
        &req.name,
        req.key.as_deref(),
        req.value.as_deref(),
    )?;
    Ok(Json(json!({"id": id})))
}

/// `GET /admin/mdm/instructions` (admin) -> all instructions and their status.
pub async fn admin_list(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    crate::api::require_admin(&headers)?;
    let items: Vec<Value> = state.devices.mdm_list(LOCAL_USER)?.into_iter()
        .map(|(id, name, status, detail)| json!({"id": id, "name": name, "status": status, "detail": detail}))
        .collect();
    Ok(Json(json!({"instructions": items})))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderValue, header};

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    #[tokio::test]
    async fn queue_poll_and_status() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token(LOCAL_USER).unwrap();
        let state = AppState::new(storage, devices);
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {tk}")).unwrap(),
        );

        // nothing queued -> 204
        assert_eq!(
            get_instruction(State(state.clone()), h.clone())
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );

        // enqueue a PIN-length policy, then the device polls and gets it
        let id = state
            .devices
            .mdm_enqueue(
                LOCAL_USER,
                "minimumPincodeLength",
                Some("minimumPincodeLength"),
                Some("6"),
            )
            .unwrap();
        let resp = get_instruction(State(state.clone()), h.clone())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["id"], id);
        assert_eq!(v["name"], "minimumPincodeLength");
        assert_eq!(v["data"]["value"], "6");

        // still pending on the next poll (not acknowledged yet)
        assert_eq!(
            get_instruction(State(state.clone()), h.clone())
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        // device reports completion -> cleared -> 204
        post_status(
            State(state.clone()),
            h.clone(),
            Json(json!({"id": id, "status": "completed"})),
        )
        .await
        .unwrap();
        assert_eq!(
            get_instruction(State(state.clone()), h.clone())
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
}
