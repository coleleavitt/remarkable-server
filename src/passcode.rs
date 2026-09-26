//! Passcode (PIN) reset flow. Firmware 3.3.2 (xochitl, sub_487698) POSTs
//! `/passcode/v1/resets/{id}` both to create a request and, with the saved id, to
//! resume/check it (method enum 2 = POST per sub_1A4264); newer clients may GET it. The owner
//! approves it (here via the admin endpoint), which also pushes `PasscodeResetApproved`
//! to the device over the notifications socket.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use chrono::{Duration, Utc};

use crate::api::AppState;
use crate::device::PasscodeReset;
use crate::error::{Result, ServerError};
use crate::notifications::WsMessage;

/// Matches rmfakecloud's `passcodestore.ResetTTL`.
const RESET_TTL_HOURS: i64 = 24;

fn auth(headers: &HeaderMap) -> Result<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::Unauthorized)
}

/// `POST /passcode/v1/resets/{uuid}`: create the request, or re-check an existing one.
/// Returns the stored request (201 if new, 200 if it already existed) so a re-POST sees `Approved`.
pub async fn create(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<PasscodeReset>)> {
    let (user_id, device_id, device_desc) = state.devices.caller(auth(&headers)?)?;
    let now = Utc::now();
    let reset = PasscodeReset {
        device_id,
        device_name: if device_desc.is_empty() {
            "reMarkable".into()
        } else {
            device_desc
        },
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
    let stored = state
        .devices
        .get_passcode_reset(&reset.request_id, &user_id)?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(stored),
    ))
}

/// `GET /passcode/v1/resets/{uuid}`: device polls its request.
pub async fn get(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<PasscodeReset>> {
    let (user_id, ..) = state.devices.caller(auth(&headers)?)?;
    Ok(Json(
        state.devices.get_passcode_reset(&request_id, &user_id)?,
    ))
}

/// `POST /admin/passcode/resets/{uuid}/approve` (requires `x-admin-token` = `ADMIN_TOKEN`).
pub async fn approve(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<PasscodeReset>> {
    crate::api::require_admin(&headers)?;
    approve_and_notify(&state, &request_id, None)
}

fn approve_and_notify(
    state: &AppState,
    request_id: &str,
    caller: Option<(&str, &str)>,
) -> Result<Json<PasscodeReset>> {
    let (user_id, reset) = state.devices.approve_passcode_reset(
        request_id,
        caller.map(|c| c.0),
        caller.map(|c| c.1),
    )?;
    let _ = state
        .notification_tx
        .send(WsMessage::passcode_reset_approved(
            &user_id,
            &reset.device_id,
            &reset.device_name,
            &reset.request_id,
        ));
    tracing::info!(%request_id, "passcode reset approved");
    Ok(Json(reset))
}

// A device can't vouch for its own reset: approve/deny must come from a *different* device of the
// requesting user. Other users' requests (and unknown/expired ids) are 404, same device is 403.
// The device check is part of the UPDATE/DELETE itself (no read-then-write race). Expired rows are
// swept first so deny can't delete (and report 204 for) a request lookups already treat as missing.

/// `POST /passcode/v1/reset/{uuid}/approve`: another of the owner's devices approves
/// (firmware sub_484934; POST per the method enum in sub_1A4264).
pub async fn device_approve(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<PasscodeReset>> {
    let (user_id, device_id, _) = state.devices.caller(auth(&headers)?)?;
    state.devices.purge_expired_passcode_resets(&user_id)?;
    approve_and_notify(&state, &request_id, Some((&user_id, &device_id)))
}

/// `POST /passcode/v1/reset/{uuid}/deny` (firmware sub_485124).
pub async fn device_deny(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    let (user_id, device_id, _) = state.devices.caller(auth(&headers)?)?;
    state.devices.purge_expired_passcode_resets(&user_id)?;
    state
        .devices
        .deny_passcode_reset(&request_id, &user_id, &device_id)?;
    let _ = state
        .notification_tx
        .send(WsMessage::passcode_reset_denied(&user_id, &request_id));
    tracing::info!(%request_id, "passcode reset denied");
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    const REQ: &str = "0b1c2d3e-0000-4000-8000-000000000001";

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (AppState::new(storage, devices), tmp)
    }

    /// Headers carrying the user token a paired tablet would send.
    fn device(state: &AppState, user: &str, device_id: &str) -> HeaderMap {
        let (tk, ..) = state
            .devices
            .oauth_bundle(user, device_id, "remarkable")
            .unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {tk}")).unwrap(),
        );
        h
    }

    async fn request_reset(state: &AppState, h: &HeaderMap) {
        let (status, Json(r)) = create(State(state.clone()), Path(REQ.into()), h.clone())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert!(!r.approved);
    }

    fn status_of<T>(r: Result<T>) -> StatusCode {
        axum::response::IntoResponse::into_response(r.err().expect("expected an error")).status()
    }

    #[tokio::test]
    async fn requesting_device_cannot_approve_or_deny_itself() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        assert_eq!(
            status_of(device_approve(State(state.clone()), Path(REQ.into()), a.clone()).await),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_of(device_deny(State(state.clone()), Path(REQ.into()), a.clone()).await),
            StatusCode::FORBIDDEN
        );
        // still pending and still present
        assert!(
            !get(State(state.clone()), Path(REQ.into()), a.clone())
                .await
                .unwrap()
                .0
                .approved
        );
    }

    #[tokio::test]
    async fn other_device_of_same_user_approves() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let b = device(&state, "user-1", "tablet-b");
        let Json(r) = device_approve(State(state.clone()), Path(REQ.into()), b)
            .await
            .unwrap();
        assert!(r.approved);
        assert_eq!(r.device_id, "tablet-a");
        // the requester's re-POST (how firmware 3.3.2 resumes) now sees it approved
        let (status, Json(r)) = create(State(state.clone()), Path(REQ.into()), a)
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(r.approved);
    }

    #[tokio::test]
    async fn other_device_of_same_user_denies() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let b = device(&state, "user-1", "tablet-b");
        assert_eq!(
            device_deny(State(state.clone()), Path(REQ.into()), b)
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            status_of(get(State(state.clone()), Path(REQ.into()), a).await),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn other_users_device_is_rejected() {
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        let mallory = device(&state, "user-2", "tablet-m");
        assert_eq!(
            status_of(
                device_approve(State(state.clone()), Path(REQ.into()), mallory.clone()).await
            ),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(device_deny(State(state.clone()), Path(REQ.into()), mallory).await),
            StatusCode::NOT_FOUND
        );
        assert!(
            !get(State(state.clone()), Path(REQ.into()), a)
                .await
                .unwrap()
                .0
                .approved
        );
    }

    #[tokio::test]
    async fn deny_of_expired_request_removes_the_row() {
        let (state, _tmp) = setup();
        let now = Utc::now();
        let stale = PasscodeReset {
            device_id: "tablet-a".into(),
            device_name: "reMarkable".into(),
            request_id: REQ.into(),
            created: now - Duration::hours(48),
            expires: now - Duration::hours(24),
            approved: false,
        };
        assert!(
            state
                .devices
                .create_passcode_reset(&stale, "user-1")
                .unwrap()
        );
        let b = device(&state, "user-1", "tablet-b");
        assert_eq!(
            status_of(device_deny(State(state.clone()), Path(REQ.into()), b).await),
            StatusCode::NOT_FOUND
        );
        // row is gone: the same id can be stored again
        assert!(
            state
                .devices
                .create_passcode_reset(&stale, "user-1")
                .unwrap()
        );
    }

    #[test]
    fn expired_reset_purge_uses_an_index() {
        let (state, tmp) = setup();
        drop(state);
        // an existing install's db (table created before the index) also gets it on the next start
        rusqlite::Connection::open(tmp.path().join("devices.db"))
            .unwrap()
            .execute_batch("DROP INDEX passcode_resets_user_expires")
            .unwrap();
        DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let db = rusqlite::Connection::open(tmp.path().join("devices.db")).unwrap();
        let plan: Vec<String> = db
            .prepare(
                "EXPLAIN QUERY PLAN DELETE FROM passcode_resets WHERE user_id = ? AND expires < ?",
            )
            .unwrap()
            .query_map(rusqlite::params!["u", "t"], |r| r.get::<_, String>(3))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(
            plan.iter().any(|d| d
                .contains("USING INDEX passcode_resets_user_expires (user_id=? AND expires<?)")),
            "{plan:?}"
        );
    }

    #[tokio::test]
    async fn device_check_is_part_of_the_write() {
        // The storage layer alone (no handler-side pre-check) refuses the requester's own device.
        let (state, _tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        assert_eq!(
            status_of(
                state
                    .devices
                    .approve_passcode_reset(REQ, Some("user-1"), Some("tablet-a"))
            ),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_of(state.devices.deny_passcode_reset(REQ, "user-1", "tablet-a")),
            StatusCode::FORBIDDEN
        );
        let r = state.devices.get_passcode_reset(REQ, "user-1").unwrap();
        assert!(!r.approved, "self-approval updated nothing");
        // wrong user / unknown id stay 404, even when the device id matches
        assert_eq!(
            status_of(
                state
                    .devices
                    .approve_passcode_reset(REQ, Some("user-2"), Some("tablet-a"))
            ),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(state.devices.approve_passcode_reset(
                "nope",
                Some("user-1"),
                Some("tablet-b")
            )),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(state.devices.deny_passcode_reset(REQ, "user-2", "tablet-a")),
            StatusCode::NOT_FOUND
        );
        // a different device of the same user can
        let (user, r) = state
            .devices
            .approve_passcode_reset(REQ, Some("user-1"), Some("tablet-b"))
            .unwrap();
        assert_eq!(
            (user.as_str(), r.approved, r.device_id.as_str()),
            ("user-1", true, "tablet-a")
        );
    }

    #[tokio::test]
    async fn expired_request_can_be_recreated_with_the_same_id() {
        let (state, tmp) = setup();
        let a = device(&state, "user-1", "tablet-a");
        request_reset(&state, &a).await;
        // force-expire it (and mark it approved, so a stale approval can't leak into the new request)
        let past = (Utc::now() - Duration::hours(1)).to_rfc3339();
        rusqlite::Connection::open(tmp.path().join("devices.db"))
            .unwrap()
            .execute(
                "UPDATE passcode_resets SET expires = ?, approved = 1 WHERE request_id = ?",
                rusqlite::params![past, REQ],
            )
            .unwrap();
        assert_eq!(
            status_of(get(State(state.clone()), Path(REQ.into()), a.clone()).await),
            StatusCode::NOT_FOUND
        );
        // the tablet re-POSTs the saved id: a fresh pending request, not 404
        request_reset(&state, &a).await;
        let r = get(State(state.clone()), Path(REQ.into()), a.clone())
            .await
            .unwrap()
            .0;
        assert!(!r.approved && r.expires > Utc::now());
        // and it's idempotent again while live
        assert_eq!(
            create(State(state.clone()), Path(REQ.into()), a)
                .await
                .unwrap()
                .0,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn expired_row_of_another_user_does_not_block_the_id() {
        let (state, _tmp) = setup();
        let now = Utc::now();
        let stale = PasscodeReset {
            device_id: "tablet-x".into(),
            device_name: "reMarkable".into(),
            request_id: REQ.into(),
            created: now - Duration::hours(48),
            expires: now - Duration::hours(24),
            approved: false,
        };
        assert!(
            state
                .devices
                .create_passcode_reset(&stale, "user-2")
                .unwrap()
        );
        request_reset(&state, &device(&state, "user-1", "tablet-a")).await;
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
        assert!(
            rx.try_recv().is_ok(),
            "PasscodeResetApproved pushed to the device"
        );
        assert!(
            get(State(state.clone()), Path(REQ.into()), a)
                .await
                .unwrap()
                .0
                .approved
        );
    }
}
