//! OAuth2 device-flow login for software 3.28 (recovered from user-authenticator-cli).
//!
//! 3.28 authenticates against auth.remarkable.com with the OAuth device-code grant and can
//! also migrate a legacy device credential. We front the same credentials our sync/gentree
//! auth already accepts: access = user credential, refresh = device credential, id = HS512
//! id credential carrying the auth.remarkable.com claims. Endpoints recovered:
//!   POST /oauth/device/code, POST /oauth/token, POST /oauth/revoke,
//!   POST /token/json/4/device/exchange
//! Device codes follow RFC 8628: `/oauth/token` answers `authorization_pending` until the owner
//! approves the `user_code` (`GET/POST /oauth/verify` or `POST /admin/oauth/approve`, with the
//! `ADMIN_TOKEN`, or with a paired device's credential as `Authorization: Bearer`), and
//! `expired_token` once the code is older than `expires_in`. Untested against a real 3.28 device.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::{extract::{Query, State}, http::{header, StatusCode}, response::{Html, IntoResponse, Response}, Form, Json};
use axum::http::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{api::AppState, error::{Result, ServerError}};

const LOCAL_USER: &str = "local-user";
const DEVICE_CODE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
const DEVICE_CODE_TTL: Duration = Duration::from_secs(600);
/// Pending device codes kept at once; the oldest is dropped past this (endpoint is unauthenticated).
const MAX_PENDING: usize = 256;
const POLL_INTERVAL: u64 = 5;
const SCOPES: &str = "openid profile email offline_access";
const EXPIRES_IN: u64 = 3 * 60 * 60;

struct Pending { device_id: String, user_code: String, at: Instant, approved_by: Option<String> }

#[derive(Debug, PartialEq)]
enum Poll { Pending, Approved { user_id: String, device_id: String }, Expired }

/// Outstanding device authorizations, keyed by device_code. Expired entries are dropped on every
/// insert/poll/approve and the map never holds more than `cap` entries.
struct PendingCodes { map: HashMap<String, Pending>, cap: usize }

impl PendingCodes {
    fn new(cap: usize) -> Self { Self { map: HashMap::new(), cap } }
    fn evict(&mut self, now: Instant) { self.map.retain(|_, p| now.saturating_duration_since(p.at) <= DEVICE_CODE_TTL); }
    fn insert(&mut self, device_code: String, user_code: String, now: Instant) {
        self.evict(now);
        while self.map.len() >= self.cap {
            let Some(oldest) = self.map.iter().min_by_key(|(_, p)| p.at).map(|(k, _)| k.clone()) else { break };
            self.map.remove(&oldest);
        }
        let device_id = format!("oauth-{}", &device_code[..8.min(device_code.len())]);
        self.map.insert(device_code, Pending { device_id, user_code, at: now, approved_by: None });
    }
    fn has_user_code(&self, user_code: &str) -> bool { self.map.values().any(|p| p.user_code == user_code) }
    /// Mark `user_code` approved for `user_id`. False if unknown or expired.
    fn approve(&mut self, user_code: &str, user_id: &str, now: Instant) -> bool {
        self.evict(now);
        match self.map.values_mut().find(|p| p.user_code == user_code) {
            Some(p) => { p.approved_by = Some(user_id.to_owned()); true }
            None => false,
        }
    }
    /// Poll a device_code; an approved code is consumed (single use).
    fn poll(&mut self, device_code: &str, now: Instant) -> Poll {
        let expired = match self.map.get(device_code) { Some(p) => now.saturating_duration_since(p.at) > DEVICE_CODE_TTL, None => true };
        self.evict(now);
        if expired { return Poll::Expired; }
        if self.map[device_code].approved_by.is_none() { return Poll::Pending; }
        let p = self.map.remove(device_code).expect("checked above");
        Poll::Approved { user_id: p.approved_by.unwrap_or_default(), device_id: p.device_id }
    }
}

static PENDING: LazyLock<Mutex<PendingCodes>> = LazyLock::new(|| Mutex::new(PendingCodes::new(MAX_PENDING)));

fn pending() -> std::sync::MutexGuard<'static, PendingCodes> { PENDING.lock().unwrap_or_else(|e| e.into_inner()) }

fn oauth_err(code: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": code}))).into_response()
}

fn bundle_value(access: String, refresh: String, id: String) -> Value {
    json!({
        "access_token": access,
        "refresh_token": refresh,
        "id_token": id,
        "token_type": "Bearer",
        "expires_in": EXPIRES_IN,
        "scope": SCOPES,
    })
}

/// `POST /oauth/device/code` -> a device authorization response the tablet polls on.
pub async fn device_code(State(state): State<AppState>, Form(_f): Form<HashMap<String, String>>) -> Json<Value> {
    let code = uuid::Uuid::new_v4().simple().to_string();
    let user_code = {
        use rand::Rng;
        let mut r = rand::thread_rng();
        let mut p = pending();
        let user_code = loop {
            let c = format!("{:04}-{:04}", r.gen_range(0..10000), r.gen_range(0..10000));
            if !p.has_user_code(&c) { break c; }
        };
        p.insert(code.clone(), user_code.clone(), Instant::now());
        user_code
    };
    let host = state.devices.get_endpoint();
    tracing::warn!(%user_code, "OAuth device code requested; approve at /oauth/verify or POST /admin/oauth/approve");
    Json(json!({
        "device_code": code,
        "user_code": user_code,
        "verification_uri": format!("https://{host}/oauth/verify"),
        "verification_uri_complete": format!("https://{host}/oauth/verify?user_code={user_code}"),
        "expires_in": DEVICE_CODE_TTL.as_secs(),
        "interval": POLL_INTERVAL,
    }))
}

/// `POST /oauth/token` -> device-code and refresh grants.
pub async fn token(State(state): State<AppState>, Form(f): Form<HashMap<String, String>>) -> Response {
    let grant = f.get("grant_type").map(String::as_str).unwrap_or("");
    if grant == DEVICE_CODE_GRANT {
        let Some(dc) = f.get("device_code") else { return oauth_err("invalid_request"); };
        let polled = pending().poll(dc, Instant::now());
        match polled {
            Poll::Pending => oauth_err("authorization_pending"),
            Poll::Expired => oauth_err("expired_token"),
            Poll::Approved { user_id, device_id } => match state.devices.oauth_bundle(&user_id, &device_id, "remarkable") {
                Ok((a, r, i)) => (StatusCode::OK, Json(bundle_value(a, r, i))).into_response(),
                Err(e) => e.into_response(),
            },
        }
    } else if grant == "refresh_token" {
        let Some(rt) = f.get("refresh_token") else { return oauth_err("invalid_request"); };
        match state.devices.refresh_oauth(rt) {
            Ok((a, r, i)) => (StatusCode::OK, Json(bundle_value(a, r, i))).into_response(),
            Err(_) => oauth_err("invalid_grant"),
        }
    } else {
        oauth_err("unsupported_grant_type")
    }
}

/// Who may approve a device code: the owner via `x-admin-token` (the single local account,
/// like the other `/admin` endpoints), or an already-paired device via its device (refresh)
/// credential, which can mint the same bundle through `/token/json/4/device/exchange` anyway.
/// Short-lived access credentials are not accepted, so they can't be upgraded to a long-lived one.
fn approver(state: &AppState, headers: &HeaderMap) -> Result<String> {
    if headers.contains_key("x-admin-token") {
        crate::api::require_admin(headers)?;
        return Ok(LOCAL_USER.to_owned());
    }
    let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    state.devices.device_token_user(auth.strip_prefix("Bearer ").ok_or(ServerError::Unauthorized)?)
}

fn approve_code(state: &AppState, headers: &HeaderMap, user_code: &str) -> Result<Json<Value>> {
    let user = approver(state, headers)?;
    let user_code = user_code.trim();
    if !pending().approve(user_code, &user, Instant::now()) {
        return Err(ServerError::NotFound("unknown or expired user_code".into()));
    }
    tracing::info!(%user_code, %user, "OAuth device code approved");
    Ok(Json(json!({"approved": true, "user_code": user_code})))
}

#[derive(Deserialize)]
pub struct ApproveRequest { user_code: String, #[serde(default)] admin_token: Option<String> }

/// `POST /admin/oauth/approve {"user_code": "1234-5678"}` (`x-admin-token` or device Bearer).
pub async fn admin_approve(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<ApproveRequest>) -> Result<Json<Value>> {
    approve_code(&state, &headers, &req.user_code)
}

/// `POST /oauth/verify` (form `user_code`, credential as `x-admin-token`/Bearer header or the
/// `admin_token` form field used by the page below).
pub async fn verify(State(state): State<AppState>, mut headers: HeaderMap, Form(req): Form<ApproveRequest>) -> Result<Json<Value>> {
    if let Some(t) = req.admin_token.as_deref().filter(|t| !t.is_empty()) {
        headers.insert("x-admin-token", HeaderValue::from_str(t).map_err(|_| ServerError::Unauthorized)?);
    }
    approve_code(&state, &headers, &req.user_code)
}

#[derive(Deserialize)]
pub struct VerifyQuery { #[serde(default)] user_code: String }

/// `GET /oauth/verify[?user_code=]` -> a minimal approval form (the advertised verification_uri).
pub async fn verify_page(Query(q): Query<VerifyQuery>) -> Html<String> {
    // Only echo well-formed codes (digits and '-') so nothing needs escaping.
    let code = if q.user_code.len() <= 16 && q.user_code.chars().all(|c| c.is_ascii_digit() || c == '-') { q.user_code } else { String::new() };
    Html(format!(r#"<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Approve device</title>
<h1>Approve reMarkable sign-in</h1><form method="post" action="/oauth/verify">
<p><label>Code <input name="user_code" value="{code}" required></label></p>
<p><label>Admin token <input name="admin_token" type="password" required></label></p>
<p><button>Approve</button></p></form>"#))
}

/// `POST /oauth/revoke` -> always 200 (credentials are stateless).
pub async fn revoke() -> StatusCode { StatusCode::OK }

/// `POST /token/json/4/device/exchange` -> migrate a legacy device credential to OAuth.
pub async fn device_exchange(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)?;
    let device = auth.strip_prefix("Bearer ").ok_or(ServerError::Unauthorized)?;
    let (access, refresh, id) = state.devices.exchange_device_token(device)?;
    Ok(Json(json!({
        "token_type": "Bearer",
        "oauth": {
            "access_token": access, "refresh_token": refresh, "id_token": id, "scope": SCOPES,
        }
    })))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{device::DeviceManager, storage::Storage};
    use axum::http::HeaderValue;

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices = DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (AppState::new(storage, devices), tmp)
    }
    async fn body_json(resp: Response) -> (StatusCode, Value) {
        let st = resp.status();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
    }

    fn grant(code: &str) -> Form<HashMap<String, String>> {
        Form(HashMap::from([("grant_type".to_string(), DEVICE_CODE_GRANT.to_string()), ("device_code".to_string(), code.to_string())]))
    }
    fn bearer(tok: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {tok}")).unwrap());
        h
    }
    async fn mint(state: &AppState) -> (String, String) {
        let Json(dc) = device_code(State(state.clone()), Form(HashMap::new())).await;
        (dc["device_code"].as_str().unwrap().to_string(), dc["user_code"].as_str().unwrap().to_string())
    }
    /// A paired device's long-lived (refresh) credential, as `--pair` + `device/new` would issue.
    fn paired_device_token(state: &AppState) -> String { state.devices.oauth_bundle(LOCAL_USER, "paired-tablet", "remarkable").unwrap().1 }

    #[tokio::test]
    async fn device_flow_refresh_and_exchange() {
        let (state, _tmp) = setup();

        // 1. device/code
        let Json(dc) = device_code(State(state.clone()), Form(HashMap::new())).await;
        let code = dc["device_code"].as_str().unwrap().to_string();
        let user_code = dc["user_code"].as_str().unwrap().to_string();
        assert!(dc["verification_uri_complete"].is_string());
        assert_eq!(dc["expires_in"], 600);

        // 2. not yet approved -> authorization_pending (no tokens)
        let (st, v) = body_json(token(State(state.clone()), grant(&code)).await).await;
        assert_eq!((st, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("authorization_pending")));

        // 3. owner approves with a paired device credential, then the grant -> access/refresh/id
        approve_code(&state, &bearer(&paired_device_token(&state)), &user_code).unwrap();
        let (st, v) = body_json(token(State(state.clone()), grant(&code)).await).await;
        assert_eq!(st, StatusCode::OK);
        let access = v["access_token"].as_str().unwrap().to_string();
        let refresh = v["refresh_token"].as_str().unwrap().to_string();
        assert!(v["id_token"].is_string());
        assert_eq!(v["token_type"], "Bearer");

        // access token is accepted by the sync/gentree auth path
        let (user, device, _) = state.devices.caller(&format!("Bearer {access}")).unwrap();
        assert_eq!(user, "local-user");
        assert!(device.starts_with("oauth-"));

        // 4. the code is single-use
        let (st, v) = body_json(token(State(state.clone()), grant(&code)).await).await;
        assert_eq!((st, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("expired_token")));

        // 5. refresh grant
        let mut fr = HashMap::new();
        fr.insert("grant_type".to_string(), "refresh_token".to_string());
        fr.insert("refresh_token".to_string(), refresh.clone());
        assert_eq!(token(State(state.clone()), Form(fr)).await.status(), StatusCode::OK);

        // 6. legacy device-token -> OAuth migration (the refresh token is a device token)
        let Json(x) = device_exchange(State(state.clone()), bearer(&refresh)).await.unwrap();
        assert!(x["oauth"]["access_token"].is_string());
        assert_eq!(x["token_type"], "Bearer");

        // 7. unsupported grant
        let mut fb = HashMap::new();
        fb.insert("grant_type".to_string(), "password".to_string());
        assert_eq!(token(State(state.clone()), Form(fb)).await.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn approval_requires_owner_credential() {
        let (state, _tmp) = setup();
        let (code, user_code) = mint(&state).await;
        // no credential, wrong admin token, a short-lived access credential, garbage bearer
        assert!(matches!(approve_code(&state, &HeaderMap::new(), &user_code), Err(ServerError::Unauthorized)));
        let mut bad = HeaderMap::new();
        bad.insert("x-admin-token", HeaderValue::from_static("definitely-not-the-admin-token"));
        assert!(approve_code(&state, &bad, &user_code).is_err());
        let access = state.devices.oauth_bundle(LOCAL_USER, "paired-tablet", "remarkable").unwrap().0;
        assert!(approve_code(&state, &bearer(&access), &user_code).is_err());
        assert!(approve_code(&state, &bearer("nope"), &user_code).is_err());
        // form field admin_token is checked the same way
        let r = verify(State(state.clone()), HeaderMap::new(), Form(ApproveRequest { user_code: user_code.clone(), admin_token: Some("wrong".into()) })).await;
        assert!(r.is_err());
        let (_, v) = body_json(token(State(state.clone()), grant(&code)).await).await;
        assert_eq!(v["error"], "authorization_pending");
        // unknown user_code with a valid credential
        assert!(matches!(approve_code(&state, &bearer(&paired_device_token(&state)), "0000-000x"), Err(ServerError::NotFound(_))));
    }

    #[tokio::test]
    async fn admin_token_approves_via_json_and_verify_form() {
        std::env::set_var("ADMIN_TOKEN", "oauth-test-admin-token");
        let (state, _tmp) = setup();
        let mut h = HeaderMap::new();
        h.insert("x-admin-token", HeaderValue::from_static("oauth-test-admin-token"));
        let (code, user_code) = mint(&state).await;
        admin_approve(State(state.clone()), h, Json(ApproveRequest { user_code, admin_token: None })).await.unwrap();
        assert_eq!(token(State(state.clone()), grant(&code)).await.status(), StatusCode::OK);

        let (code, user_code) = mint(&state).await;
        verify(State(state.clone()), HeaderMap::new(), Form(ApproveRequest { user_code, admin_token: Some("oauth-test-admin-token".into()) })).await.unwrap();
        let (st, v) = body_json(token(State(state.clone()), grant(&code)).await).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(state.devices.caller(&format!("Bearer {}", v["access_token"].as_str().unwrap())).unwrap().0, LOCAL_USER);
    }

    #[tokio::test]
    async fn expired_code_is_rejected_even_if_approved() {
        let (state, _tmp) = setup();
        let (code, user_code) = mint(&state).await;
        approve_code(&state, &bearer(&paired_device_token(&state)), &user_code).unwrap();
        // age the entry past expires_in
        pending().map.get_mut(&code).unwrap().at = Instant::now().checked_sub(DEVICE_CODE_TTL + Duration::from_secs(1)).unwrap();
        let (st, v) = body_json(token(State(state.clone()), grant(&code)).await).await;
        assert_eq!((st, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("expired_token")));
        assert!(!pending().map.contains_key(&code), "expired entry evicted");
        // and it can no longer be approved
        let (code2, user_code2) = mint(&state).await;
        pending().map.get_mut(&code2).unwrap().at = Instant::now().checked_sub(DEVICE_CODE_TTL + Duration::from_secs(1)).unwrap();
        assert!(approve_code(&state, &bearer(&paired_device_token(&state)), &user_code2).is_err());
    }

    #[test]
    fn pending_codes_evict_expired_and_cap() {
        let t0 = Instant::now();
        let mut p = PendingCodes::new(3);
        for i in 0..5 { p.insert(format!("code{i:04}xxxx"), format!("0000-000{i}"), t0 + Duration::from_secs(i)); }
        assert_eq!(p.map.len(), 3, "capped");
        assert!(!p.map.contains_key("code0000xxxx") && !p.map.contains_key("code0001xxxx"), "oldest dropped first");
        assert_eq!(p.poll("code0004xxxx", t0 + Duration::from_secs(5)), Poll::Pending);
        assert!(p.approve("0000-0004", "u", t0 + Duration::from_secs(5)));
        assert_eq!(p.poll("code0004xxxx", t0 + Duration::from_secs(6)), Poll::Approved { user_id: "u".into(), device_id: "oauth-code0004".into() });
        // everything left expires; a later insert evicts it without any poll
        p.insert("fresh000xxxx".into(), "1111-1111".into(), t0 + DEVICE_CODE_TTL + Duration::from_secs(10));
        assert_eq!(p.map.keys().collect::<Vec<_>>(), vec!["fresh000xxxx"]);
        assert_eq!(p.poll("missing", t0), Poll::Expired);
    }

    #[tokio::test]
    async fn device_code_grant_type_must_match_exactly() {
        let (state, _tmp) = setup();
        let (code, user_code) = mint(&state).await;
        approve_code(&state, &bearer(&paired_device_token(&state)), &user_code).unwrap();
        for g in ["device_code", "evil:device_code", "urn:ietf:params:oauth:grant-type:device_code "] {
            let f = Form(HashMap::from([("grant_type".to_string(), g.to_string()), ("device_code".to_string(), code.clone())]));
            let (_, v) = body_json(token(State(state.clone()), f).await).await;
            assert_eq!(v["error"], "unsupported_grant_type", "{g}");
        }
        assert_eq!(token(State(state.clone()), grant(&code)).await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verify_page_only_echoes_wellformed_codes() {
        let Html(ok) = verify_page(Query(VerifyQuery { user_code: "1234-5678".into() })).await;
        assert!(ok.contains(r#"value="1234-5678""#));
        let Html(bad) = verify_page(Query(VerifyQuery { user_code: "\"><script>".into() })).await;
        assert!(!bad.contains("<script>") && bad.contains(r#"value="""#));
    }
}
