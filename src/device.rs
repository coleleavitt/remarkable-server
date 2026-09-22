use crate::error::{Result, ServerError};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{encode, decode, Algorithm, EncodingKey, DecodingKey, Header, Validation};
use rand::Rng;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use parking_lot::Mutex;

const JWT_SECRET: &[u8] = b"remarkable-local-server-secret-key-v1";
const USER_TOKEN_LIFETIME: i64 = 3 * 60 * 60;
const CODE_LIFETIME: i64 = 10 * 60;

#[derive(Clone)]
pub struct DeviceManager { inner: Arc<Inner> }
struct Inner { 
    conn: Mutex<Connection>, 
    region: String, 
    issuer: String,
    encoding_key: jsonwebtoken::EncodingKey,
    decoding_key: jsonwebtoken::DecodingKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device { pub device_id: String, pub device_desc: String, pub registered_at: DateTime<Utc>, pub last_refresh: DateTime<Utc>, pub user_id: String }

#[derive(Debug, Serialize, Deserialize)]
struct DeviceTokenClaims { sub: String, iss: String, iat: i64, nbf: i64, jti: String, #[serde(rename = "device-id")] device_id: String, #[serde(rename = "device-desc")] device_desc: String, #[serde(rename = "auth0-userid")] auth0_userid: String }

#[derive(Debug, Serialize, Deserialize)]
struct UserTokenClaims { sub: String, iss: String, iat: i64, exp: i64, nbf: i64, jti: String, tectonic: String, scopes: String, #[serde(rename = "auth0-profile")] auth0_profile: Auth0Profile, #[serde(rename = "device-id")] device_id: String, #[serde(rename = "device-desc")] device_desc: String, #[serde(rename = "https://auth.remarkable.com/subscription")] subscription: SubscriptionClaim }

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Auth0Profile { #[serde(rename = "UserID")] user_id: String, #[serde(rename = "Email")] email: String, #[serde(rename = "IsConnected")] is_connected: bool, #[serde(rename = "IsBeta")] is_beta: bool }

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SubscriptionClaim { status: String, plan: String }

impl DeviceManager {
    pub fn new<P: AsRef<Path>>(db_path: P, region: &str, issuer: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS devices (device_id TEXT PRIMARY KEY, device_desc TEXT NOT NULL, registered_at TEXT NOT NULL, last_refresh TEXT NOT NULL, user_id TEXT NOT NULL); CREATE TABLE IF NOT EXISTS pending_codes (code TEXT PRIMARY KEY, user_id TEXT NOT NULL, expires_at TEXT NOT NULL);")?;
        let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "remarkable-local-server-secret".to_string());
        let encoding_key = jsonwebtoken::EncodingKey::from_secret(secret.as_bytes());
        let decoding_key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
        Ok(Self { inner: Arc::new(Inner { 
            conn: Mutex::new(conn), 
            region: region.into(), 
            issuer: issuer.into(),
            encoding_key,
            decoding_key,
        }) })
    }
    fn gen_code() -> String { const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789"; let mut rng = rand::thread_rng(); (0..8).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect() }
    pub fn create_pairing_code(&self, user_id: &str) -> Result<String> {
        let code = Self::gen_code(); let expires = Utc::now() + Duration::seconds(CODE_LIFETIME); let conn = self.inner.conn.lock();
        conn.execute("DELETE FROM pending_codes WHERE expires_at < ?", params![Utc::now().to_rfc3339()]).ok();
        conn.execute("INSERT INTO pending_codes (code, user_id, expires_at) VALUES (?, ?, ?)", params![code, user_id, expires.to_rfc3339()])?;
        Ok(code)
    }
    pub fn exchange_code(&self, code: &str, device_id: &str, device_desc: &str) -> Result<(String, String)> {
        let conn = self.inner.conn.lock();
        let (user_id, expires_str): (String, String) = conn.query_row("SELECT user_id, expires_at FROM pending_codes WHERE code = ?", params![code], |r| Ok((r.get(0)?, r.get(1)?))).map_err(|_| ServerError::InvalidCode("Code not found".into()))?;
        let expires = DateTime::parse_from_rfc3339(&expires_str).map_err(|_| ServerError::InvalidCode("Invalid expiry".into()))?.with_timezone(&Utc);
        if Utc::now() > expires { conn.execute("DELETE FROM pending_codes WHERE code = ?", params![code]).ok(); return Err(ServerError::InvalidCode("Code expired".into())); }
        conn.execute("DELETE FROM pending_codes WHERE code = ?", params![code]).ok();
        let now = Utc::now();
        conn.execute("INSERT INTO devices (device_id, device_desc, registered_at, last_refresh, user_id) VALUES (?, ?, ?, ?, ?) ON CONFLICT(device_id) DO UPDATE SET device_desc=excluded.device_desc, last_refresh=excluded.last_refresh, user_id=excluded.user_id", params![device_id, device_desc, now.to_rfc3339(), now.to_rfc3339(), user_id])?;
        drop(conn);
        let dt = self.gen_device_token(device_id, device_desc, &user_id)?; let ut = self.gen_user_token(device_id, device_desc, &user_id)?;
        Ok((dt, ut))
    }
    pub fn refresh_user_token(&self, device_token: &str) -> Result<String> {
        let claims = self.decode_device_token(device_token)?; let conn = self.inner.conn.lock();
        conn.execute("UPDATE devices SET last_refresh = ? WHERE device_id = ?", params![Utc::now().to_rfc3339(), claims.device_id]).ok();
        drop(conn); self.gen_user_token(&claims.device_id, &claims.device_desc, &claims.auth0_userid)
    }
    pub fn list_devices(&self) -> Result<Vec<Device>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare("SELECT device_id, device_desc, registered_at, last_refresh, user_id FROM devices")?;
        let devices = stmt.query_map([], |row| Ok(Device { device_id: row.get(0)?, device_desc: row.get(1)?, registered_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(2)?).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now()), last_refresh: DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now()), user_id: row.get(4)? }))?.filter_map(|r| r.ok()).collect();
        Ok(devices)
    }
    pub fn delete_device(&self, device_id: &str) -> Result<bool> { let conn = self.inner.conn.lock(); Ok(conn.execute("DELETE FROM devices WHERE device_id = ?", params![device_id])? > 0) }
    fn gen_device_token(&self, device_id: &str, device_desc: &str, user_id: &str) -> Result<String> {
        let now = Utc::now().timestamp();
        encode(&Header::new(Algorithm::HS256), &DeviceTokenClaims { sub: "rM Device Token".into(), iss: self.inner.issuer.clone(), iat: now, nbf: now, jti: uuid::Uuid::new_v4().to_string(), device_id: device_id.into(), device_desc: device_desc.into(), auth0_userid: user_id.into() }, &EncodingKey::from_secret(JWT_SECRET)).map_err(|e| ServerError::TokenError(e.to_string()))
    }
    fn gen_user_token(&self, device_id: &str, device_desc: &str, user_id: &str) -> Result<String> {
        let now = Utc::now().timestamp();
        encode(&Header::new(Algorithm::HS256), &UserTokenClaims { sub: user_id.into(), iss: self.inner.issuer.clone(), iat: now, exp: now + USER_TOKEN_LIFETIME, nbf: now, jti: uuid::Uuid::new_v4().to_string(), tectonic: self.inner.region.clone(), scopes: "intgr hwcmail:-1 hwc sync:fox screenshare mail:-1".into(), auth0_profile: Auth0Profile { user_id: user_id.into(), email: format!("local@{}", self.inner.issuer), is_connected: true, is_beta: false }, device_id: device_id.into(), device_desc: device_desc.into(), subscription: SubscriptionClaim { status: "active".into(), plan: "connect".into() } }, &EncodingKey::from_secret(JWT_SECRET)).map_err(|e| ServerError::TokenError(e.to_string()))
    }
    fn decode_device_token(&self, token: &str) -> Result<DeviceTokenClaims> {
        let mut val = Validation::new(Algorithm::HS256); val.validate_exp = false; val.set_required_spec_claims(&["sub", "iss", "iat"]);
        decode::<DeviceTokenClaims>(token, &DecodingKey::from_secret(JWT_SECRET), &val).map(|d| d.claims).map_err(|_| ServerError::InvalidToken)
    }
    pub fn validate_token(&self, auth: &str) -> Result<String> {
        let token = auth.strip_prefix("Bearer ").ok_or(ServerError::Unauthorized)?;
        if let Ok(c) = self.decode_device_token(token) { return Ok(c.auth0_userid); }
        let mut val = Validation::new(Algorithm::HS256); val.set_required_spec_claims(&["sub", "exp"]);
        if let Ok(d) = decode::<UserTokenClaims>(token, &DecodingKey::from_secret(JWT_SECRET), &val) { return Ok(d.claims.sub); }
        Err(ServerError::InvalidToken)
    }
    /// Get the endpoint URL for this server
    pub fn get_endpoint(&self) -> String {
        self.inner.issuer.clone()
    }
    
    /// Create a user token for admin/test purposes
    pub fn create_user_token(&self, user_id: &str) -> Result<String> {
        let claims = serde_json::json!({
            "sub": user_id,
            "iat": chrono::Utc::now().timestamp(),
            "exp": (chrono::Utc::now() + chrono::Duration::hours(24)).timestamp(),
            "iss": &self.inner.issuer,
        });
        
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
        jsonwebtoken::encode(&header, &claims, &self.inner.encoding_key)
            .map_err(|e| ServerError::TokenError(e.to_string()))
    }
    
    /// Get a device by ID
    pub fn get_device(&self, device_id: &str) -> Result<Option<Device>> {
        let conn = self.inner.conn.lock();
        match conn.query_row(
            "SELECT device_id, device_desc, registered_at, last_refresh, user_id FROM devices WHERE device_id = ?",
            params![device_id],
            |row| {
                Ok(Device {
                    device_id: row.get(0)?,
                    device_desc: row.get(1)?,
                    registered_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(2)?)
                        .map(|d| d.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    last_refresh: DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .map(|d| d.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    user_id: row.get(4)?,
                })
            },
        ) {
            Ok(device) => Ok(Some(device)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(ServerError::Database(e.to_string())),
        }
    }

}
