//! OAuth2 device-flow login for software 3.28 (recovered from user-authenticator-cli).
//!
//! 3.28 authenticates against auth.remarkable.com with the OAuth device-code grant and can
//! also migrate a legacy device credential. We front the same credentials our sync/gentree
//! auth already accepts: access = user credential, refresh = device credential, id = HS512
//! id credential carrying the auth.remarkable.com claims. Endpoints recovered:
//!   POST /oauth/device/code, POST /oauth/token, POST /oauth/revoke,
//!   POST /credential/json/4/device/exchange
//! Single-user local server: a minted device-code auto-approves to the local account
//! (there is no web approval UI). Untested against a real 3.28 device.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Form, Json};
use serde_json::{Value, json};

use crate::api::AppState;
use crate::error::{Result, ServerError};

const LOCAL_USER: &str = "local-user";
const DEVICE_CODE_TTL: Duration = Duration::from_secs(600);
const SCOPES: &str = "openid profile email offline_access";
const EXPIRES_IN: u64 = 3 * 60 * 60;

struct Pending {
    device_id: String,
    at: Instant,
}
static PENDING: LazyLock<Mutex<HashMap<String, Pending>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
pub async fn device_code(
    State(state): State<AppState>,
    Form(_f): Form<HashMap<String, String>>,
) -> Json<Value> {
    let code = uuid::Uuid::new_v4().simple().to_string();
    let user_code: String = {
        use rand::Rng;
        let mut r = rand::thread_rng();
        format!("{:04}-{:04}", r.gen_range(0..10000), r.gen_range(0..10000))
    };
    let host = state.devices.get_endpoint();
    PENDING.lock().unwrap().insert(
        code.clone(),
        Pending {
            device_id: format!("oauth-{}", &code[..8]),
            at: Instant::now(),
        },
    );
    Json(json!({
        "device_code": code,
        "user_code": user_code,
        "verification_uri": format!("https://{host}/oauth/verify"),
        "verification_uri_complete": format!("https://{host}/oauth/verify?user_code={user_code}"),
        "expires_in": 600,
        "interval": 5,
    }))
}

/// `POST /oauth/token` -> device-code and refresh grants.
pub async fn token(
    State(state): State<AppState>,
    Form(f): Form<HashMap<String, String>>,
) -> Response {
    let grant = f.get("grant_type").map(String::as_str).unwrap_or("");
    if grant.ends_with("device_code") {
        let Some(dc) = f.get("device_code") else {
            return oauth_err("invalid_request");
        };
        let device_id = {
            let mut p = PENDING.lock().unwrap();
            match p.get(dc) {
                Some(pend) if pend.at.elapsed() <= DEVICE_CODE_TTL => {
                    let id = pend.device_id.clone();
                    p.remove(dc);
                    id
                }
                Some(_) => {
                    p.remove(dc);
                    return oauth_err("expired_token");
                }
                None => return oauth_err("expired_token"),
            }
        };
        match state
            .devices
            .oauth_bundle(LOCAL_USER, &device_id, "remarkable")
        {
            Ok((a, r, i)) => (StatusCode::OK, Json(bundle_value(a, r, i))).into_response(),
            Err(e) => e.into_response(),
        }
    } else if grant == "refresh_token" {
        let Some(rt) = f.get("refresh_token") else {
            return oauth_err("invalid_request");
        };
        match state.devices.refresh_oauth(rt) {
            Ok((a, r, i)) => (StatusCode::OK, Json(bundle_value(a, r, i))).into_response(),
            Err(_) => oauth_err("invalid_grant"),
        }
    } else {
        oauth_err("unsupported_grant_type")
    }
}

/// `POST /oauth/revoke` -> always 200 (credentials are stateless).
pub async fn revoke() -> StatusCode {
    StatusCode::OK
}

/// `POST /credential/json/4/device/exchange` -> migrate a legacy device credential to OAuth.
pub async fn device_exchange(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::Unauthorized)?;
    let device = auth
        .strip_prefix("Bearer ")
        .ok_or(ServerError::Unauthorized)?;
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
    use axum::http::HeaderValue;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (AppState::new(storage, devices), tmp)
    }
    async fn body_json(resp: Response) -> (StatusCode, Value) {
        let st = resp.status();
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn device_flow_refresh_and_exchange() {
        let (state, _tmp) = setup();

        // 1. device/code
        let Json(dc) = device_code(State(state.clone()), Form(HashMap::new())).await;
        let code = dc["device_code"].as_str().unwrap().to_string();
        assert!(dc["user_code"].is_string() && dc["verification_uri_complete"].is_string());

        // 2. device_code grant -> access/refresh/id
        let mut f = HashMap::new();
        f.insert(
            "grant_type".to_string(),
            "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        );
        f.insert("device_code".to_string(), code.clone());
        let (st, v) = body_json(token(State(state.clone()), Form(f)).await).await;
        assert_eq!(st, StatusCode::OK);
        let access = v["access_token"].as_str().unwrap().to_string();
        let refresh = v["refresh_token"].as_str().unwrap().to_string();
        assert!(v["id_token"].is_string());
        assert_eq!(v["token_type"], "Bearer");

        // access token is accepted by the sync/gentree auth path
        let (user, device, _) = state.devices.caller(&format!("Bearer {access}")).unwrap();
        assert_eq!(user, "local-user");
        assert!(device.starts_with("oauth-"));

        // 3. the code is single-use
        let mut f2 = HashMap::new();
        f2.insert(
            "grant_type".to_string(),
            "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        );
        f2.insert("device_code".to_string(), code);
        assert_eq!(
            token(State(state.clone()), Form(f2)).await.status(),
            StatusCode::BAD_REQUEST
        );

        // 4. refresh grant
        let mut fr = HashMap::new();
        fr.insert("grant_type".to_string(), "refresh_token".to_string());
        fr.insert("refresh_token".to_string(), refresh.clone());
        assert_eq!(
            token(State(state.clone()), Form(fr)).await.status(),
            StatusCode::OK
        );

        // 5. legacy device-token -> OAuth migration (the refresh token is a device token)
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {refresh}")).unwrap(),
        );
        let Json(x) = device_exchange(State(state.clone()), h).await.unwrap();
        assert!(x["oauth"]["access_token"].is_string());
        assert_eq!(x["token_type"], "Bearer");

        // 6. unsupported grant
        let mut fb = HashMap::new();
        fb.insert("grant_type".to_string(), "password".to_string());
        assert_eq!(
            token(State(state.clone()), Form(fb)).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
}
