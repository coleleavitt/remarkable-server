//! Passcode (PIN) reset flow. Firmware 3.3.2 (xochitl, sub_487698) POSTs
//! `/passcode/v1/resets/{id}` both to create a request and, with the saved id, to
//! resume/check it (method enum 2 = POST per sub_1A4264); newer clients may GET it. The owner
//! approves it (here via the admin endpoint), which also pushes `PasscodeResetApproved`
//! to the device over the notifications socket.

use axum::{extract::{Path, State}, http::{header, HeaderMap, StatusCode}, Json};
use chrono::{Duration, Utc};

use crate::{api::AppState, device::PasscodeReset, error::{Result, ServerError}, notifications::WsMessage};

/// Matches rmfakecloud's `passcodestore.ResetTTL`.
const RESET_TTL_HOURS: i64 = 24;

fn auth(headers: &HeaderMap) -> Result<&str> {
    headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)
}

/// `POST /passcode/v1/resets/{uuid}`: create the request, or re-check an existing one.
/// Returns the stored request (201 if new, 200 if it already existed) so a re-POST sees `Approved`.
pub async fn create(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<(StatusCode, Json<PasscodeReset>)> {
    let (user_id, device_id, device_desc) = state.devices.caller(auth(&headers)?)?;
    let now = Utc::now();
    let reset = PasscodeReset {
        device_id,
        device_name: if device_desc.is_empty() { "reMarkable".into() } else { device_desc },
        request_id,
        created: now,
        expires: now + Duration::hours(RESET_TTL_HOURS),
        approved: false,
    };
    let created = state.devices.create_passcode_reset(&reset, &user_id)?;
    if created {
        tracing::warn!(request_id = %reset.request_id, device = %reset.device_id,
            "passcode reset requested; approve with POST /admin/passcode/resets/{{id}}/approve");
    }
    let stored = state.devices.get_passcode_reset(&reset.request_id, &user_id)?;
    Ok((if created { StatusCode::CREATED } else { StatusCode::OK }, Json(stored)))
}

/// `GET /passcode/v1/resets/{uuid}`: device polls its request.
pub async fn get(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<Json<PasscodeReset>> {
    let (user_id, ..) = state.devices.caller(auth(&headers)?)?;
    Ok(Json(state.devices.get_passcode_reset(&request_id, &user_id)?))
}

/// `POST /admin/passcode/resets/{uuid}/approve` (requires `x-admin-token` = `ADMIN_TOKEN`).
pub async fn approve(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<Json<PasscodeReset>> {
    crate::api::require_admin(&headers)?;
    approve_and_notify(&state, &request_id, None)
}

fn approve_and_notify(state: &AppState, request_id: &str, owner: Option<&str>) -> Result<Json<PasscodeReset>> {
    let (user_id, reset) = state.devices.approve_passcode_reset(request_id, owner)?;
    let _ = state.notification_tx.send(WsMessage::passcode_reset_approved(&user_id, &reset.device_id, &reset.device_name, &reset.request_id));
    tracing::info!(%request_id, "passcode reset approved");
    Ok(Json(reset))
}

/// `POST /passcode/v1/reset/{uuid}/approve`: another of the owner's devices approves
/// (firmware sub_484934; POST per the method enum in sub_1A4264).
pub async fn device_approve(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<Json<PasscodeReset>> {
    let (user_id, ..) = state.devices.caller(auth(&headers)?)?;
    approve_and_notify(&state, &request_id, Some(&user_id))
}

/// `POST /passcode/v1/reset/{uuid}/deny` (firmware sub_485124).
pub async fn device_deny(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<StatusCode> {
    let (user_id, ..) = state.devices.caller(auth(&headers)?)?;
    if !state.devices.delete_passcode_reset(&request_id, &user_id)? {
        return Err(ServerError::NotFound(request_id));
    }
    let _ = state.notification_tx.send(WsMessage::passcode_reset_denied(&user_id, &request_id));
    tracing::info!(%request_id, "passcode reset denied");
    Ok(StatusCode::NO_CONTENT)
}
