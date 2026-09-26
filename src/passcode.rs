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

/// A device can't vouch for its own reset: approve/deny must come from a *different* device of the
/// requesting user. Other users' requests (and unknown/expired ids) are 404, same device is 403.
/// Expired rows are swept here since the lookup 404s them before deny could delete them.
fn require_other_device(state: &AppState, request_id: &str, user_id: &str, device_id: &str) -> Result<()> {
    state.devices.purge_expired_passcode_resets(user_id)?;
    let reset = state.devices.get_passcode_reset(request_id, user_id)?;
    if reset.device_id == device_id {
        tracing::warn!(%request_id, device = %device_id, "passcode reset self-approval/denial rejected");
        return Err(ServerError::Forbidden("a device cannot approve or deny its own passcode reset".into()));
    }
    Ok(())
}

/// `POST /passcode/v1/reset/{uuid}/approve`: another of the owner's devices approves
/// (firmware sub_484934; POST per the method enum in sub_1A4264).
pub async fn device_approve(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<Json<PasscodeReset>> {
    let (user_id, device_id, _) = state.devices.caller(auth(&headers)?)?;
    require_other_device(&state, &request_id, &user_id, &device_id)?;
    approve_and_notify(&state, &request_id, Some(&user_id))
}

/// `POST /passcode/v1/reset/{uuid}/deny` (firmware sub_485124).
pub async fn device_deny(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<StatusCode> {
    let (user_id, device_id, _) = state.devices.caller(auth(&headers)?)?;
    require_other_device(&state, &request_id, &user_id, &device_id)?;
    if !state.devices.delete_passcode_reset(&request_id, &user_id)? {
        return Err(ServerError::NotFound(request_id));
    }
    let _ = state.notification_tx.send(WsMessage::passcode_reset_denied(&user_id, &request_id));
    tracing::info!(%request_id, "passcode reset denied");
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{device::DeviceManager, storage::Storage};
    use axum::http::HeaderValue;

    const REQ: &str = "0b1c2d3e-0000-4000-8000-000000000001";

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices = DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (AppState::new(storage, devices), tmp)
    }

    /// Headers carrying the user token a paired tablet would send.
    fn device(state: &AppState, user: &str, device_id: &str) -> HeaderMap {
        let (tk, ..) = state.devices.oauth_bundle(user, device_id, "remarkable").unwrap();
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {tk}")).unwrap());
        h
    }

    async fn request_reset(state: &AppState, h: &HeaderMap) {
        let (status, Json(r)) = create(State(state.clone()), Path(REQ.into()), h.clone()).await.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert!(!r.approved);
    }

    fn status_of<T>(r: Result<T>) -> StatusCode { axum::response::IntoResponse::into_response(r.err().expect("expected an error")).status() }

    #[tokio::test]
    async fn requesting_device_cannot_approve_or_deny_itself() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        assert_eq!(status_of(device_approve(State(state.clone()), Path(REQ.into()), a.clone()).await), StatusCode::FORBIDDEN);
        assert_eq!(status_of(device_deny(State(state.clone()), Path(REQ.into()), a.clone()).await), StatusCode::FORBIDDEN);
        // still pending and still present
        assert!(!get(State(state.clone()), Path(REQ.into()), a.clone()).await.unwrap().0.approved);
    }

    #[tokio::test]
    async fn other_device_of_same_user_approves() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let b = device(&state, "user-1", "tablet-b");
        let Json(r) = device_approve(State(state.clone()), Path(REQ.into()), b).await.unwrap();
        assert!(r.approved);
        assert_eq!(r.device_id, "tablet-a");
        // the requester's re-POST (how firmware 3.3.2 resumes) now sees it approved
        let (status, Json(r)) = create(State(state.clone()), Path(REQ.into()), a).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(r.approved);
    }

    #[tokio::test]
    async fn other_device_of_same_user_denies() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let b = device(&state, "user-1", "tablet-b");
        assert_eq!(device_deny(State(state.clone()), Path(REQ.into()), b).await.unwrap(), StatusCode::NO_CONTENT);
        assert_eq!(status_of(get(State(state.clone()), Path(REQ.into()), a).await), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn other_users_device_is_rejected() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let mallory = device(&state, "user-2", "tablet-m");
        assert_eq!(status_of(device_approve(State(state.clone()), Path(REQ.into()), mallory.clone()).await), StatusCode::NOT_FOUND);
        assert_eq!(status_of(device_deny(State(state.clone()), Path(REQ.into()), mallory).await), StatusCode::NOT_FOUND);
        assert!(!get(State(state.clone()), Path(REQ.into()), a).await.unwrap().0.approved);
    }

    #[tokio::test]
    async fn deny_of_expired_request_removes_the_row() {
        let (state, _tmp) = setup();
        let now = Utc::now();
        let stale = PasscodeReset { device_id: "tablet-a".into(), device_name: "reMarkable".into(), request_id: REQ.into(), created: now - Duration::hours(48), expires: now - Duration::hours(24), approved: false };
        assert!(state.devices.create_passcode_reset(&stale, "user-1").unwrap());
        let b = device(&state, "user-1", "tablet-b");
        assert_eq!(status_of(device_deny(State(state.clone()), Path(REQ.into()), b).await), StatusCode::NOT_FOUND);
        // row is gone: the same id can be stored again
        assert!(state.devices.create_passcode_reset(&stale, "user-1").unwrap());
    }

    #[tokio::test]
    async fn admin_approval_still_works_for_the_requesting_device() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let mut rx = state.notification_tx.subscribe();
        // `approve` = require_admin (x-admin-token) + this; no device identity is involved.
        let Json(r) = approve_and_notify(&state, REQ, None).unwrap();
        assert!(r.approved);
        assert!(rx.try_recv().is_ok(), "PasscodeResetApproved pushed to the device");
        assert!(get(State(state.clone()), Path(REQ.into()), a).await.unwrap().0.approved);
    }
}
