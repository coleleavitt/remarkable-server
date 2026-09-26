use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, Header, Validation, decode, encode};
use parking_lot::Mutex;
use rand::Rng;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};

const MIN_JWT_SECRET_LEN: usize = 32;
const JWT_SECRET_FILENAME: &str = "jwt_secret";
const USER_TOKEN_LIFETIME: i64 = 3 * 60 * 60;
const CODE_LIFETIME: i64 = 10 * 60;
const BLOB_URL_LIFETIME_MINUTES: i64 = 60;
/// Scopes as xochitl 3.3.2 parses them (sub_4AF728 and helpers): `hwc` and `mail` are
/// looked up by *substring* and must carry a number (non-zero enables; -1 = unlimited),
/// e.g. a bare `hwc` fails to parse and leaves handwriting conversion off.
/// `sync:fox` selects the sync tier; `intgr`/`docedit`/`screenshare` are plain flags.
const USER_SCOPES: &str = "intgr docedit screenshare sync:fox hwc:-1 mail:-1";
/// `device-id` of user tokens minted by `create_user_token` (admin/test); they are not tied to
/// a registration, so they skip the device check. Reserved: no device may register under it.
const ADMIN_DEVICE_ID: &str = "admin";
/// How often an open notification session re-checks its registration, as a backstop for a
/// missed revocation event.
const SESSION_RECHECK: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone)]
pub struct DeviceManager {
    inner: Arc<Inner>,
}
struct Inner {
    conn: Mutex<Connection>,
    revocations: tokio::sync::broadcast::Sender<DeviceRevoked>,
    region: String,
    issuer: String,
    encoding_key: jsonwebtoken::EncodingKey,
    decoding_key: jsonwebtoken::DecodingKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub device_id: String,
    pub device_desc: String,
    pub registered_at: DateTime<Utc>,
    pub last_refresh: DateTime<Utc>,
    pub user_id: String,
}

/// Emitted when a device's registration for `user_id` ends (deleted, self-unregistered, or
/// re-paired to another user): every token and open session of that device must stop working.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRevoked {
    pub user_id: String,
    pub device_id: String,
}

/// Who a bearer token authenticates: enough for a long-lived session (WebSocket/MQTT) to
/// re-check later that the registration it was opened under still stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    pub user_id: String,
    pub device_id: String,
    pub device_desc: String,
    epoch: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeviceTokenClaims {
    sub: String,
    iss: String,
    iat: i64,
    nbf: i64,
    jti: String,
    #[serde(rename = "device-id")]
    device_id: String,
    #[serde(rename = "device-desc")]
    device_desc: String,
    #[serde(rename = "auth0-userid")]
    auth0_userid: String,
    /// Revocation generation (see `device_token_epochs`). Omitted when 0, so tokens minted before any
    /// revocation (e.g. the already-paired tablet's) are byte-for-byte what they always were.
    #[serde(rename = "rms-epoch", default, skip_serializing_if = "is_zero")]
    epoch: i64,
}
fn is_zero(n: &i64) -> bool {
    *n == 0
}
const UPSERT_DEVICE: &str = "INSERT INTO devices (device_id, device_desc, registered_at, last_refresh, user_id) VALUES (?, ?, ?, ?, ?) ON CONFLICT(device_id) DO UPDATE SET device_desc=excluded.device_desc, last_refresh=excluded.last_refresh, user_id=excluded.user_id";

#[derive(Debug, Serialize, Deserialize)]
struct UserTokenClaims {
    sub: String,
    iss: String,
    iat: i64,
    exp: i64,
    nbf: i64,
    jti: String,
    #[serde(rename = "https://auth.remarkable.com/tectonic")]
    tectonic: String,
    scopes: String,
    #[serde(rename = "auth0-profile")]
    auth0_profile: Auth0Profile,
    #[serde(rename = "device-id")]
    device_id: String,
    #[serde(rename = "device-desc")]
    device_desc: String,
    #[serde(rename = "https://auth.remarkable.com/subscription")]
    subscription: SubscriptionClaim,
    /// The minting device's revocation epoch, as in `DeviceTokenClaims`. Omitted when 0, and a
    /// missing claim (user tokens minted before this existed) reads as 0.
    #[serde(rename = "rms-epoch", default, skip_serializing_if = "is_zero")]
    epoch: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Auth0Profile {
    #[serde(rename = "UserID")]
    user_id: String,
    #[serde(rename = "Email")]
    email: String,
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "Nickname", default)]
    nickname: String,
    #[serde(default)]
    level: String,
    #[serde(rename = "IsConnected")]
    is_connected: bool,
    #[serde(rename = "IsBeta")]
    is_beta: bool,
}

/// Tablet-facing passcode reset request (field names match the cloud API).
#[derive(Debug, Clone, Serialize)]
pub struct PasscodeReset {
    #[serde(rename = "DeviceID")]
    pub device_id: String,
    #[serde(rename = "DeviceName")]
    pub device_name: String,
    #[serde(rename = "RequestID")]
    pub request_id: String,
    #[serde(rename = "Created")]
    pub created: DateTime<Utc>,
    #[serde(rename = "Expires")]
    pub expires: DateTime<Utc>,
    #[serde(rename = "Approved")]
    pub approved: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlobClaims {
    blob: String,
    write: bool,
    exp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SubscriptionClaim {
    status: String,
    plan: String,
}

#[derive(Serialize)]
struct IdTokenClaims {
    sub: String,
    iss: String,
    aud: String,
    iat: i64,
    exp: i64,
    email: String,
    #[serde(rename = "https://auth.remarkable.com/tectonic")]
    tectonic: String,
    #[serde(rename = "https://auth.remarkable.com/subscription")]
    subscription: String,
    #[serde(rename = "https://auth.remarkable.com/mdm")]
    mdm: bool,
    #[serde(rename = "https://auth.remarkable.com/created_at")]
    created_at: String,
}

/// Load the JWT signing secret. Precedence:
/// 1. `JWT_SECRET_FILE` -- path to a file holding the secret (e.g. a systemd credential).
/// 2. `JWT_SECRET` -- the secret itself.
/// 3. `<storage>/jwt_secret` -- created with 64 random bytes (hex, mode 0600) on first start.
/// There is no built-in default: a shared default would let anyone forge tokens.
fn load_jwt_secret(storage_dir: &Path) -> Result<Vec<u8>> {
    let check = |secret: Vec<u8>, source: &str| -> Result<Vec<u8>> {
        if secret.len() < MIN_JWT_SECRET_LEN {
            return Err(ServerError::Config(format!(
                "JWT secret from {source} is {} bytes; need at least {MIN_JWT_SECRET_LEN}",
                secret.len()
            )));
        }
        Ok(secret)
    };
    let read_file = |path: &Path| -> Result<Vec<u8>> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            ServerError::Config(format!("reading JWT secret {}: {e}", path.display()))
        })?;
        Ok(raw.trim().as_bytes().to_vec())
    };
    if let Ok(path) = std::env::var("JWT_SECRET_FILE") {
        return check(read_file(Path::new(&path))?, "JWT_SECRET_FILE");
    }
    if let Ok(secret) = std::env::var("JWT_SECRET") {
        return check(secret.trim().as_bytes().to_vec(), "JWT_SECRET");
    }
    let path = storage_dir.join(JWT_SECRET_FILENAME);
    if path.exists() {
        return check(read_file(&path)?, &path.display().to_string());
    }
    let mut bytes = [0u8; 64];
    rand::thread_rng().fill(&mut bytes[..]);
    let secret = hex::encode(bytes);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts
        .open(&path)
        .map_err(|e| ServerError::Config(format!("creating JWT secret {}: {e}", path.display())))?;
    std::io::Write::write_all(&mut file, secret.as_bytes())?;
    file.sync_all()?;
    tracing::info!("Generated new JWT signing secret at {}", path.display());
    Ok(secret.into_bytes())
}

impl DeviceManager {
    pub fn new<P: AsRef<Path>>(db_path: P, region: &str, issuer: &str) -> Result<Self> {
        let conn = Connection::open(db_path.as_ref())?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS devices (device_id TEXT PRIMARY KEY, device_desc TEXT NOT NULL, registered_at TEXT NOT NULL, last_refresh TEXT NOT NULL, user_id TEXT NOT NULL); CREATE TABLE IF NOT EXISTS pending_codes (code TEXT PRIMARY KEY, user_id TEXT NOT NULL, expires_at TEXT NOT NULL); CREATE TABLE IF NOT EXISTS passcode_resets (request_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, device_id TEXT NOT NULL, device_name TEXT NOT NULL, created TEXT NOT NULL, expires TEXT NOT NULL, approved INTEGER NOT NULL DEFAULT 0); CREATE INDEX IF NOT EXISTS passcode_resets_user_expires ON passcode_resets (user_id, expires); CREATE TABLE IF NOT EXISTS mdm_instructions (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, name TEXT NOT NULL, data_key TEXT, data_value TEXT, status TEXT NOT NULL DEFAULT 'pending', detail TEXT, created TEXT NOT NULL); CREATE TABLE IF NOT EXISTS device_token_epochs (device_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL);")?;
        // One HS256 signing key for every token; see load_jwt_secret for where it comes from.
        let secret = load_jwt_secret(db_path.as_ref().parent().unwrap_or_else(|| Path::new(".")))?;
        let encoding_key = jsonwebtoken::EncodingKey::from_secret(&secret);
        let decoding_key = jsonwebtoken::DecodingKey::from_secret(&secret);
        let (revocations, _) = tokio::sync::broadcast::channel(64);
        Ok(Self {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                revocations,
                region: region.into(),
                issuer: issuer.into(),
                encoding_key,
                decoding_key,
            }),
        })
    }
    fn gen_code() -> String {
        const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::thread_rng();
        (0..8)
            .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
            .collect()
    }
    pub fn create_pairing_code(&self, user_id: &str) -> Result<String> {
        let code = Self::gen_code();
        let expires = Utc::now() + Duration::seconds(CODE_LIFETIME);
        let conn = self.inner.conn.lock();
        conn.execute(
            "DELETE FROM pending_codes WHERE expires_at < ?",
            params![Utc::now().to_rfc3339()],
        )
        .ok();
        conn.execute(
            "INSERT INTO pending_codes (code, user_id, expires_at) VALUES (?, ?, ?)",
            params![code, user_id, expires.to_rfc3339()],
        )?;
        Ok(code)
    }
    pub fn exchange_code(
        &self,
        code: &str,
        device_id: &str,
        device_desc: &str,
    ) -> Result<(String, String)> {
        let conn = self.inner.conn.lock();
        let (user_id, expires_str): (String, String) = conn
            .query_row(
                "SELECT user_id, expires_at FROM pending_codes WHERE code = ?",
                params![code],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| ServerError::InvalidCode("Code not found".into()))?;
        let expires = DateTime::parse_from_rfc3339(&expires_str)
            .map_err(|_| ServerError::InvalidCode("Invalid expiry".into()))?
            .with_timezone(&Utc);
        if Utc::now() > expires {
            conn.execute("DELETE FROM pending_codes WHERE code = ?", params![code])
                .ok();
            return Err(ServerError::InvalidCode("Code expired".into()));
        }
        conn.execute("DELETE FROM pending_codes WHERE code = ?", params![code])
            .ok();
        let epoch = self.register(&conn, device_id, device_desc, &user_id)?;
        drop(conn);
        let dt = self.gen_device_token(device_id, device_desc, &user_id, epoch)?;
        let ut = self.gen_user_token(device_id, device_desc, &user_id, epoch)?;
        Ok((dt, ut))
    }
    /// Unregister the device a (validly signed) device token names, scoped to the token's user.
    /// Idempotent: a token whose device is already gone is still accepted here and deletes nothing.
    /// A token from an earlier registration (older epoch) deletes nothing, so it can't knock out a re-pair.
    pub fn revoke_device_token(&self, device_token: &str) -> Result<bool> {
        let c = self.decode_device_token_signature(device_token)?;
        let conn = self.inner.conn.lock();
        match Self::check_registered(&conn, &c) {
            Ok(()) => {}
            Err(ServerError::InvalidToken) => return Ok(false),
            Err(e) => return Err(e),
        }
        self.unregister(&conn, &c.device_id, Some(&c.auth0_userid))
    }
    pub fn refresh_user_token(&self, device_token: &str) -> Result<String> {
        let c = self.touch_device_token(device_token)?;
        self.gen_user_token(&c.device_id, &c.device_desc, &c.auth0_userid, c.epoch)
    }
    /// Registered devices; `Some(user)` limits it to that user's own, `None` (admin/startup) lists all.
    pub fn list_devices(&self, owner: Option<&str>) -> Result<Vec<Device>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare("SELECT device_id, device_desc, registered_at, last_refresh, user_id FROM devices WHERE ?1 IS NULL OR user_id = ?1")?;
        let devices = stmt
            .query_map(params![owner], |row| {
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
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(devices)
    }
    /// Unregister a device and revoke every device token issued for it so far.
    /// `Some(user)` only deletes it if that user owns it (another user's device is a no-op, false);
    /// `None` (admin) deletes regardless of owner.
    pub fn delete_device(&self, device_id: &str, owner: Option<&str>) -> Result<bool> {
        self.unregister(&self.inner.conn.lock(), device_id, owner)
    }
    /// Delete the registration and bump the epoch in one transaction: a delete without the bump
    /// would let a later re-pair revive every token minted before it.
    /// Once committed, open sessions of the device are told to close (`subscribe_revocations`).
    fn unregister(&self, conn: &Connection, device_id: &str, owner: Option<&str>) -> Result<bool> {
        let tx = conn.unchecked_transaction()?; // callers hold the connection mutex
        let user: Option<String> = tx
            .query_row(
                "SELECT user_id FROM devices WHERE device_id = ?1 AND (?2 IS NULL OR user_id = ?2)",
                params![device_id, owner],
                |r| r.get(0),
            )
            .optional()?;
        let Some(user_id) = user else {
            return Ok(false);
        };
        tx.execute(
            "DELETE FROM devices WHERE device_id = ?",
            params![device_id],
        )?;
        Self::bump_epoch(&tx, device_id)?;
        tx.commit()?;
        self.announce_revoked(user_id, device_id);
        Ok(true)
    }
    fn announce_revoked(&self, user_id: String, device_id: &str) {
        tracing::info!(%device_id, "device revoked; closing its open sessions");
        // No receivers (no open sessions) is not an error.
        let _ = self.inner.revocations.send(DeviceRevoked {
            user_id,
            device_id: device_id.into(),
        });
    }
    /// Revocation events for open sessions. Subscribe before checking a session's registration,
    /// so a revocation landing in between is not missed.
    pub fn subscribe_revocations(&self) -> tokio::sync::broadcast::Receiver<DeviceRevoked> {
        self.inner.revocations.subscribe()
    }
    /// Invalidate all device tokens minted for `device_id` so far, even if it is paired again later.
    fn bump_epoch(conn: &Connection, device_id: &str) -> Result<()> {
        conn.execute("INSERT INTO device_token_epochs (device_id, epoch) VALUES (?, 1) ON CONFLICT(device_id) DO UPDATE SET epoch = epoch + 1", params![device_id])?;
        Ok(())
    }
    /// Upsert a device registration; returns the epoch to mint its device token with.
    /// Moving a device to another user revokes the previous owner's tokens (they'd otherwise
    /// come back to life if the device were later paired back to that user).
    /// Bump + upsert run in one transaction so a failure can't hand the device over un-revoked.
    fn register(
        &self,
        conn: &Connection,
        device_id: &str,
        device_desc: &str,
        user_id: &str,
    ) -> Result<i64> {
        if device_id == ADMIN_DEVICE_ID {
            return Err(ServerError::BadRequest(format!(
                "device id {ADMIN_DEVICE_ID:?} is reserved"
            )));
        }
        let tx = conn.unchecked_transaction()?; // callers hold the connection mutex
        let prev: Option<String> = tx
            .query_row(
                "SELECT user_id FROM devices WHERE device_id = ?",
                params![device_id],
                |r| r.get(0),
            )
            .optional()?;
        let revoked = prev.filter(|u| u != user_id);
        if revoked.is_some() {
            Self::bump_epoch(&tx, device_id)?;
        }
        let now = Utc::now().to_rfc3339();
        tx.execute(
            UPSERT_DEVICE,
            params![device_id, device_desc, now, now, user_id],
        )?;
        let epoch = tx.query_row(
            "SELECT COALESCE((SELECT epoch FROM device_token_epochs WHERE device_id = ?), 0)",
            params![device_id],
            |r| r.get(0),
        )?;
        tx.commit()?;
        if let Some(prev) = revoked {
            self.announce_revoked(prev, device_id);
        }
        Ok(epoch)
    }

    // ---- MDM instruction queue (enterprise device management, /mdm/v1) ----
    /// Enqueue an instruction for the user's devices; returns its id.
    pub fn mdm_enqueue(
        &self,
        user_id: &str,
        name: &str,
        key: Option<&str>,
        value: Option<&str>,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.inner.conn.lock();
        conn.execute(
            "INSERT INTO mdm_instructions (id, user_id, name, data_key, data_value, status, created) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            params![id, user_id, name, key, value, Utc::now().to_rfc3339()],
        )?;
        Ok(id)
    }

    /// Oldest still-pending instruction for the user: (id, name, key, value).
    pub fn mdm_next_pending(
        &self,
        user_id: &str,
    ) -> Result<Option<(String, String, Option<String>, Option<String>)>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare("SELECT id, name, data_key, data_value FROM mdm_instructions WHERE user_id = ? AND status = 'pending' ORDER BY created ASC LIMIT 1")?;
        let mut rows = stmt.query(params![user_id])?;
        match rows.next()? {
            Some(r) => Ok(Some((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))),
            None => Ok(None),
        }
    }

    /// Record a device-reported status for an instruction.
    /// Only the owning user's instructions can be updated; another user's id is a no-op (false).
    pub fn mdm_set_status(
        &self,
        user_id: &str,
        id: &str,
        status: &str,
        detail: Option<&str>,
    ) -> Result<bool> {
        let conn = self.inner.conn.lock();
        Ok(conn.execute(
            "UPDATE mdm_instructions SET status = ?, detail = ? WHERE id = ? AND user_id = ?",
            params![status, detail, id, user_id],
        )? > 0)
    }

    /// All instructions for the user: (id, name, status, detail).
    pub fn mdm_list(&self, user_id: &str) -> Result<Vec<(String, String, String, Option<String>)>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare("SELECT id, name, status, detail FROM mdm_instructions WHERE user_id = ? ORDER BY created ASC")?;
        let rows = stmt
            .query_map(params![user_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .filter_map(|x| x.ok())
            .collect();
        Ok(rows)
    }
    fn gen_device_token(
        &self,
        device_id: &str,
        device_desc: &str,
        user_id: &str,
        epoch: i64,
    ) -> Result<String> {
        let now = Utc::now().timestamp();
        encode(
            &Header::new(Algorithm::HS256),
            &DeviceTokenClaims {
                sub: "rM Device Token".into(),
                iss: self.inner.issuer.clone(),
                iat: now,
                nbf: now,
                jti: uuid::Uuid::new_v4().to_string(),
                device_id: device_id.into(),
                device_desc: device_desc.into(),
                auth0_userid: user_id.into(),
                epoch,
            },
            &self.inner.encoding_key,
        )
        .map_err(|e| ServerError::TokenError(e.to_string()))
    }
    fn gen_user_token(
        &self,
        device_id: &str,
        device_desc: &str,
        user_id: &str,
        epoch: i64,
    ) -> Result<String> {
        let now = Utc::now().timestamp();
        encode(
            &Header::new(Algorithm::HS256),
            &UserTokenClaims {
                sub: user_id.into(),
                iss: self.inner.issuer.clone(),
                iat: now,
                exp: now + USER_TOKEN_LIFETIME,
                nbf: now,
                jti: uuid::Uuid::new_v4().to_string(),
                tectonic: self.inner.region.clone(),
                scopes: USER_SCOPES.into(),
                auth0_profile: Auth0Profile {
                    user_id: user_id.into(),
                    email: format!("local@{}", self.inner.issuer),
                    name: user_id.into(),
                    nickname: user_id.into(),
                    level: "connect".into(),
                    is_connected: true,
                    is_beta: false,
                },
                device_id: device_id.into(),
                device_desc: device_desc.into(),
                subscription: SubscriptionClaim {
                    status: "active".into(),
                    plan: "connect".into(),
                },
                epoch,
            },
            &self.inner.encoding_key,
        )
        .map_err(|e| ServerError::TokenError(e.to_string()))
    }
    /// Device tokens carry no `exp` (long-lived by design, like the real cloud's), so revocation
    /// is by registration: the token is only honoured while its device is still in `devices`,
    /// still paired to the user the token names, and the token's epoch is the device's current
    /// one. Deleting the row bumps the epoch, so re-pairing doesn't revive old tokens.
    fn decode_device_token(&self, token: &str) -> Result<DeviceTokenClaims> {
        let c = self.decode_device_token_signature(token)?;
        Self::check_registered(&self.inner.conn.lock(), &c)?;
        Ok(c)
    }
    fn check_registered(conn: &Connection, c: &DeviceTokenClaims) -> Result<()> {
        Self::check_registration(conn, &c.device_id, &c.auth0_userid, c.epoch, "device")
    }
    /// A token (or session) of `device_id` for `user_id` minted at `epoch` is only honoured while
    /// that device is still registered to that user at that epoch.
    fn check_registration(
        conn: &Connection,
        device_id: &str,
        user_id: &str,
        token_epoch: i64,
        kind: &str,
    ) -> Result<()> {
        // devices.device_id and device_token_epochs.device_id are PRIMARY KEYs: two index lookups.
        let epoch: Option<i64> = conn.query_row("SELECT COALESCE((SELECT epoch FROM device_token_epochs WHERE device_id = ?1), 0) FROM devices WHERE device_id = ?1 AND user_id = ?2", params![device_id, user_id], |r| r.get(0)).optional()?;
        if epoch != Some(token_epoch) {
            tracing::warn!(%device_id, "rejecting {kind} token: device not registered to this user, or token revoked");
            return Err(ServerError::InvalidToken);
        }
        Ok(())
    }
    /// User tokens expire (3h) but are also revoked with the device that minted them: same
    /// registration + epoch check as device tokens. Admin tokens (`create_user_token`) have no
    /// device and skip it.
    fn decode_user_token(&self, token: &str) -> Result<UserTokenClaims> {
        let mut val = Validation::new(Algorithm::HS256);
        val.set_required_spec_claims(&["sub", "exp"]);
        let c = decode::<UserTokenClaims>(token, &self.inner.decoding_key, &val)
            .map_err(|_| ServerError::InvalidToken)?
            .claims;
        if c.device_id != ADMIN_DEVICE_ID {
            Self::check_registration(
                &self.inner.conn.lock(),
                &c.device_id,
                &c.sub,
                c.epoch,
                "user",
            )?;
        }
        Ok(c)
    }
    /// Validate a device token and record the refresh under one lock, so a delete/re-pair can't
    /// land between the registration check and the write (which would otherwise undo it).
    fn touch_device_token(&self, token: &str) -> Result<DeviceTokenClaims> {
        let c = self.decode_device_token_signature(token)?;
        let conn = self.inner.conn.lock();
        Self::check_registered(&conn, &c)?;
        conn.execute(
            "UPDATE devices SET last_refresh = ? WHERE device_id = ? AND user_id = ?",
            params![Utc::now().to_rfc3339(), c.device_id, c.auth0_userid],
        )?;
        Ok(c)
    }
    /// Signature/claims check only, no registration lookup. Use `decode_device_token` for auth.
    fn decode_device_token_signature(&self, token: &str) -> Result<DeviceTokenClaims> {
        let mut val = Validation::new(Algorithm::HS256);
        val.validate_exp = false;
        val.set_required_spec_claims(&["sub", "iss", "iat"]);
        decode::<DeviceTokenClaims>(token, &self.inner.decoding_key, &val)
            .map(|d| d.claims)
            .map_err(|_| ServerError::InvalidToken)
    }
    pub fn validate_token(&self, auth: &str) -> Result<String> {
        self.session_identity(auth).map(|s| s.user_id)
    }
    /// Resolve a bearer header to (user id, device id, device description).
    pub fn caller(&self, auth: &str) -> Result<(String, String, String)> {
        self.session_identity(auth)
            .map(|s| (s.user_id, s.device_id, s.device_desc))
    }
    /// Resolve a bearer header (device or user token) to who it authenticates.
    pub fn session_identity(&self, auth: &str) -> Result<SessionIdentity> {
        let token = auth
            .strip_prefix("Bearer ")
            .ok_or(ServerError::Unauthorized)?;
        if let Ok(c) = self.decode_device_token(token) {
            return Ok(SessionIdentity {
                user_id: c.auth0_userid,
                device_id: c.device_id,
                device_desc: c.device_desc,
                epoch: c.epoch,
            });
        }
        let c = self.decode_user_token(token)?;
        Ok(SessionIdentity {
            user_id: c.sub,
            device_id: c.device_id,
            device_desc: c.device_desc,
            epoch: c.epoch,
        })
    }
    /// Whether the registration a session was opened under still stands (token expiry is not
    /// re-checked: a session outliving its 3h user token is the tablet's normal behaviour).
    /// Fails closed on a DB error; the client just reconnects.
    pub fn session_still_valid(&self, s: &SessionIdentity) -> bool {
        if s.device_id == ADMIN_DEVICE_ID {
            return true;
        }
        match Self::check_registration(
            &self.inner.conn.lock(),
            &s.device_id,
            &s.user_id,
            s.epoch,
            "session",
        ) {
            Ok(()) => true,
            // Only a definite "not registered / revoked" ends the session. A transient DB error
            // (busy, I/O) must not drop the tablet's connection; the next tick re-checks, and real
            // revocations also arrive through the broadcast event.
            Err(ServerError::InvalidToken) => false,
            Err(e) => {
                tracing::warn!(device_id = %s.device_id, error = %e, "session re-check failed; keeping session open");
                true
            }
        }
    }
    /// Resolves once the session's device is revoked: on its `DeviceRevoked` event, or when a
    /// periodic re-check (every `SESSION_RECHECK`, and after a lagged event stream) finds the
    /// registration gone. Subscribes before the first check, so nothing slips in between.
    pub fn session_revoked(
        &self,
        s: &SessionIdentity,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.session_revoked_every(s, SESSION_RECHECK)
    }
    pub(crate) fn session_revoked_every(
        &self,
        s: &SessionIdentity,
        recheck: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        use tokio::sync::broadcast::error::RecvError;
        let mut rx = self.subscribe_revocations();
        let dm = self.clone();
        let s = s.clone();
        async move {
            if !dm.session_still_valid(&s) {
                return;
            }
            let start = tokio::time::Instant::now() + recheck;
            let mut tick = tokio::time::interval_at(start, recheck);
            let mut events_open = true;
            loop {
                tokio::select! {
                    ev = rx.recv(), if events_open => match ev {
                        Ok(r) if r.user_id == s.user_id && r.device_id == s.device_id => return,
                        Ok(_) => {}
                        Err(RecvError::Lagged(_)) => {
                            if !dm.session_still_valid(&s) {
                                return;
                            }
                        }
                        Err(RecvError::Closed) => events_open = false,
                    },
                    _ = tick.tick() => {
                        if !dm.session_still_valid(&s) {
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Record a passcode (PIN) reset request from a device. Idempotent per request id while it is live:
    /// expired rows (this user's, and any stale row holding `request_id`) are purged in the same
    /// transaction, so a tablet re-POSTing an expired id starts a fresh pending request instead of 404ing forever.
    /// Returns true if a new request was stored, false if a live `request_id` already existed.
    pub fn create_passcode_reset(&self, reset: &PasscodeReset, user_id: &str) -> Result<bool> {
        let mut conn = self.inner.conn.lock();
        let tx = conn.transaction()?;
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "DELETE FROM passcode_resets WHERE user_id = ? AND expires < ?",
            params![user_id, now],
        )?;
        tx.execute(
            "DELETE FROM passcode_resets WHERE request_id = ? AND expires < ?",
            params![reset.request_id, now],
        )?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO passcode_resets (request_id, user_id, device_id, device_name, created, expires, approved) VALUES (?, ?, ?, ?, ?, ?, 0)",
            params![reset.request_id, user_id, reset.device_id, reset.device_name, reset.created.to_rfc3339(), reset.expires.to_rfc3339()],
        )?;
        tx.commit()?;
        Ok(inserted > 0)
    }

    /// Look up a reset request owned by `user_id` (expired ones count as missing).
    pub fn get_passcode_reset(&self, request_id: &str, user_id: &str) -> Result<PasscodeReset> {
        Self::read_passcode_reset(&self.inner.conn.lock(), request_id, user_id)
    }
    fn read_passcode_reset(
        conn: &Connection,
        request_id: &str,
        user_id: &str,
    ) -> Result<PasscodeReset> {
        let row = conn.query_row(
            "SELECT device_id, device_name, created, expires, approved FROM passcode_resets WHERE request_id = ? AND user_id = ?",
            params![request_id, user_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, bool>(4)?)),
        ).map_err(|_| ServerError::NotFound(request_id.into()))?;
        let parse = |s: &str| {
            DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|e| ServerError::Internal(e.to_string()))
        };
        let reset = PasscodeReset {
            device_id: row.0,
            device_name: row.1,
            request_id: request_id.into(),
            created: parse(&row.2)?,
            expires: parse(&row.3)?,
            approved: row.4,
        };
        if reset.expires < Utc::now() {
            return Err(ServerError::NotFound(request_id.into()));
        }
        Ok(reset)
    }

    /// Why a guarded approve/deny touched no row: 403 if the (live) request is the caller's own,
    /// else 404. Only picks the status; the security decision is the guarded statement itself.
    fn passcode_reset_miss(
        conn: &Connection,
        request_id: &str,
        user_id: &str,
        caller_device: Option<&str>,
    ) -> ServerError {
        match Self::read_passcode_reset(conn, request_id, user_id) {
            Ok(r) if caller_device == Some(r.device_id.as_str()) => {
                tracing::warn!(%request_id, device = %r.device_id, "passcode reset self-approval/denial rejected");
                ServerError::Forbidden(
                    "a device cannot approve or deny its own passcode reset".into(),
                )
            }
            _ => ServerError::NotFound(request_id.into()),
        }
    }

    /// Approve a pending reset; returns (user id, reset) so the caller can notify the device.
    /// `owner`/`caller_device` are the approving device's user and id (None for the admin endpoint):
    /// the UPDATE itself requires `device_id != caller_device`, so a device can never approve its own request.
    pub fn approve_passcode_reset(
        &self,
        request_id: &str,
        owner: Option<&str>,
        caller_device: Option<&str>,
    ) -> Result<(String, PasscodeReset)> {
        let conn = self.inner.conn.lock();
        let user_id: String = conn
            .query_row(
                "SELECT user_id FROM passcode_resets WHERE request_id = ?",
                params![request_id],
                |r| r.get(0),
            )
            .map_err(|_| ServerError::NotFound(request_id.into()))?;
        if owner.is_some_and(|o| o != user_id) {
            return Err(ServerError::NotFound(request_id.into()));
        }
        let updated = conn.execute(
            "UPDATE passcode_resets SET approved = 1 WHERE request_id = ?1 AND user_id = ?2 AND expires >= ?3 AND (?4 IS NULL OR device_id != ?4)",
            params![request_id, user_id, Utc::now().to_rfc3339(), caller_device],
        )?;
        if updated == 0 {
            return Err(Self::passcode_reset_miss(
                &conn,
                request_id,
                &user_id,
                caller_device,
            ));
        }
        let reset = Self::read_passcode_reset(&conn, request_id, &user_id)?;
        Ok((user_id, reset))
    }

    /// Drop `user_id`'s expired reset requests (lookups already treat them as missing).
    pub fn purge_expired_passcode_resets(&self, user_id: &str) -> Result<usize> {
        // rfc3339 strings from `to_rfc3339()` share one format/offset, so they compare chronologically.
        Ok(self.inner.conn.lock().execute(
            "DELETE FROM passcode_resets WHERE user_id = ? AND expires < ?",
            params![user_id, Utc::now().to_rfc3339()],
        )?)
    }

    /// Deny: drop `user_id`'s reset request unless it is `caller_device`'s own (checked in the DELETE itself).
    /// 403 for the caller's own request, 404 if there is no such live request.
    pub fn deny_passcode_reset(
        &self,
        request_id: &str,
        user_id: &str,
        caller_device: &str,
    ) -> Result<()> {
        let conn = self.inner.conn.lock();
        if conn.execute(
            "DELETE FROM passcode_resets WHERE request_id = ? AND user_id = ? AND device_id != ?",
            params![request_id, user_id, caller_device],
        )? == 0
        {
            return Err(Self::passcode_reset_miss(
                &conn,
                request_id,
                user_id,
                Some(caller_device),
            ));
        }
        Ok(())
    }

    /// Get the endpoint URL for this server
    pub fn get_endpoint(&self) -> String {
        self.inner.issuer.clone()
    }

    /// Sign a short-lived token granting read or write access to one blob (sync 1.5 signed URLs).
    pub fn sign_blob(&self, blob: &str, write: bool) -> Result<(String, DateTime<Utc>)> {
        let exp = Utc::now() + Duration::minutes(BLOB_URL_LIFETIME_MINUTES);
        let claims = BlobClaims {
            blob: blob.into(),
            write,
            exp: exp.timestamp(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &self.inner.encoding_key,
        )
        .map_err(|e| ServerError::TokenError(e.to_string()))?;
        Ok((token, exp))
    }

    /// Check a blob token grants `write` (or read) access to `blob`.
    pub fn verify_blob(&self, token: &str, blob: &str, write: bool) -> Result<()> {
        let claims = decode::<BlobClaims>(
            token,
            &self.inner.decoding_key,
            &Validation::new(Algorithm::HS256),
        )
        .map_err(|_| ServerError::InvalidToken)?
        .claims;
        if claims.blob != blob || claims.write != write {
            return Err(ServerError::Unauthorized);
        }
        Ok(())
    }

    /// Create a user token for admin/test purposes
    pub fn create_user_token(&self, user_id: &str) -> Result<String> {
        // Same claims as a device-issued user token, so validate_token accepts it.
        self.gen_user_token(ADMIN_DEVICE_ID, ADMIN_DEVICE_ID, user_id, 0)
    }

    /// Register/refresh a device and mint an OAuth bundle for software 3.28:
    /// access = the same user auth data our sync/gentree auth already accepts,
    /// refresh = a device auth data, id = an HS512 id auth data with auth.remarkable.com claims.
    pub fn oauth_bundle(
        &self,
        user_id: &str,
        device_id: &str,
        device_desc: &str,
    ) -> Result<(String, String, String)> {
        let epoch = self.register(&self.inner.conn.lock(), device_id, device_desc, user_id)?;
        self.mint_bundle(user_id, device_id, device_desc, epoch)
    }
    fn mint_bundle(
        &self,
        user_id: &str,
        device_id: &str,
        device_desc: &str,
        epoch: i64,
    ) -> Result<(String, String, String)> {
        Ok((
            self.gen_user_token(device_id, device_desc, user_id, epoch)?,
            self.gen_device_token(device_id, device_desc, user_id, epoch)?,
            self.issue_id_token(user_id)?,
        ))
    }

    /// Re-mint an OAuth bundle from a refresh auth data (a device auth data).
    /// Only refreshes an existing registration (never re-creates a deleted one).
    pub fn refresh_oauth(&self, refresh: &str) -> Result<(String, String, String)> {
        let c = self.touch_device_token(refresh)?;
        self.mint_bundle(&c.auth0_userid, &c.device_id, &c.device_desc, c.epoch)
    }

    /// Exchange a legacy device auth data for an OAuth bundle (`/token/json/4/device/exchange`).
    pub fn exchange_device_token(&self, device: &str) -> Result<(String, String, String)> {
        let c = self.touch_device_token(device)?;
        self.mint_bundle(&c.auth0_userid, &c.device_id, &c.device_desc, c.epoch)
    }

    /// Owner of a device (refresh) auth data; lets a paired device approve an OAuth device code.
    /// The device must still be registered to that user, so a deleted device's credential can't approve.
    pub fn device_token_user(&self, device: &str) -> Result<String> {
        // decode_device_token checks registration and the revocation epoch.
        match self.decode_device_token(device) {
            Ok(c) => Ok(c.auth0_userid),
            Err(ServerError::InvalidToken) => Err(ServerError::Unauthorized),
            Err(e) => Err(e),
        }
    }

    fn issue_id_token(&self, user_id: &str) -> Result<String> {
        let now = Utc::now();
        let claims = IdTokenClaims {
            sub: user_id.into(),
            iss: self.inner.issuer.clone(),
            aud: "remarkable".into(),
            iat: now.timestamp(),
            exp: now.timestamp() + USER_TOKEN_LIFETIME,
            email: format!("local@{}", self.inner.issuer),
            tectonic: self.inner.region.clone(),
            subscription: "active".into(),
            mdm: false,
            created_at: now.to_rfc3339(),
        };
        encode(
            &Header::new(Algorithm::HS512),
            &claims,
            &self.inner.encoding_key,
        )
        .map_err(|e| ServerError::TokenError(e.to_string()))
    }

    /// Get a device by ID
    pub fn get_device(&self, device_id: &str) -> Result<Option<Device>> {
        let conn = self.inner.conn.lock();
        match conn.query_row(
            "SELECT device_id, device_desc, registered_at, last_refresh, user_id FROM devices WHERE device_id = ? COLLATE NOCASE",
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

#[cfg(test)]
mod jwt_secret_tests {
    use super::*;

    #[test]
    fn generated_secret_is_persisted_and_reused() {
        if std::env::var_os("JWT_SECRET").is_some() || std::env::var_os("JWT_SECRET_FILE").is_some()
        {
            return; // env override would bypass the file path under test
        }
        let dir = tempfile::tempdir().unwrap();
        let first = load_jwt_secret(dir.path()).unwrap();
        assert_eq!(first.len(), 128, "64 random bytes, hex-encoded");
        let second = load_jwt_secret(dir.path()).unwrap();
        assert_eq!(first, second, "second start must reuse the stored secret");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(JWT_SECRET_FILENAME))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let other = tempfile::tempdir().unwrap();
        assert_ne!(
            first,
            load_jwt_secret(other.path()).unwrap(),
            "each install gets its own secret"
        );
    }

    #[test]
    fn short_secret_file_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(JWT_SECRET_FILENAME), "too-short").unwrap();
        if std::env::var_os("JWT_SECRET").is_none() && std::env::var_os("JWT_SECRET_FILE").is_none()
        {
            assert!(load_jwt_secret(dir.path()).is_err());
        }
    }
}

#[cfg(test)]
mod revocation_tests {
    use super::*;

    fn setup() -> (DeviceManager, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        (
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap(),
            tmp,
        )
    }
    /// Pair `device` to `user` the way the tablet does (pairing code -> /token/json/2/device/new).
    fn pair(dm: &DeviceManager, user: &str, device: &str) -> String {
        let code = dm.create_pairing_code(user).unwrap();
        dm.exchange_code(&code, device, "remarkable").unwrap().0
    }
    fn bearer(t: &str) -> String {
        format!("Bearer {t}")
    }

    #[test]
    fn registered_device_token_is_accepted() {
        let (dm, _tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        assert_eq!(dm.validate_token(&bearer(&dt)).unwrap(), "local-user");
        assert_eq!(dm.caller(&bearer(&dt)).unwrap().1, "RM110-1");
        let ut = dm.refresh_user_token(&dt).unwrap();
        assert_eq!(
            dm.caller(&bearer(&ut)).unwrap(),
            ("local-user".into(), "RM110-1".into(), "remarkable".into())
        );
        assert!(dm.exchange_device_token(&dt).is_ok());
    }

    #[test]
    fn deleted_device_token_is_rejected_everywhere() {
        let (dm, _tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        assert!(dm.delete_device("RM110-1", None).unwrap());
        assert!(matches!(
            dm.validate_token(&bearer(&dt)),
            Err(ServerError::InvalidToken)
        ));
        assert!(dm.caller(&bearer(&dt)).is_err());
        assert!(dm.refresh_user_token(&dt).is_err());
        // Refresh/exchange must not silently re-register the deleted device.
        assert!(dm.refresh_oauth(&dt).is_err());
        assert!(dm.exchange_device_token(&dt).is_err());
        assert!(dm.get_device("RM110-1").unwrap().is_none());
    }

    #[test]
    fn self_revoke_invalidates_token_and_is_idempotent() {
        let (dm, _tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        let other = pair(&dm, "local-user", "RM110-2");
        assert!(dm.revoke_device_token(&dt).unwrap());
        assert!(dm.validate_token(&bearer(&dt)).is_err());
        assert!(
            !dm.revoke_device_token(&dt).unwrap(),
            "second delete is a no-op, not an error"
        );
        assert!(dm.revoke_device_token("not-a-jwt").is_err());
        assert!(
            dm.validate_token(&bearer(&other)).is_ok(),
            "other devices unaffected"
        );
    }

    #[test]
    fn token_for_another_users_device_is_rejected() {
        let (dm, _tmp) = setup();
        let _b = pair(&dm, "user-b", "RM110-B");
        // Validly signed, but claims user A for a device registered to user B.
        let forged = dm
            .gen_device_token("RM110-B", "remarkable", "user-a", 0)
            .unwrap();
        assert!(dm.validate_token(&bearer(&forged)).is_err());
        assert!(dm.caller(&bearer(&forged)).is_err());
        assert!(
            !dm.revoke_device_token(&forged).unwrap(),
            "must not delete user B's device"
        );
        assert!(dm.get_device("RM110-B").unwrap().is_some());
        // A device re-paired to a new user drops the old user's token.
        let old = pair(&dm, "user-a", "RM110-X");
        let new = pair(&dm, "user-b", "RM110-X");
        assert!(dm.validate_token(&bearer(&old)).is_err());
        assert_eq!(dm.validate_token(&bearer(&new)).unwrap(), "user-b");
    }

    #[test]
    fn repairing_does_not_revive_revoked_tokens() {
        let (dm, _tmp) = setup();
        let old = pair(&dm, "local-user", "RM110-1");
        let (_, old_refresh, _) = dm.refresh_oauth(&old).unwrap();
        assert!(dm.delete_device("RM110-1", None).unwrap());
        let new = pair(&dm, "local-user", "RM110-1");
        assert!(dm.validate_token(&bearer(&new)).is_ok());
        for t in [&old, &old_refresh] {
            assert!(dm.validate_token(&bearer(t)).is_err());
            assert!(dm.refresh_user_token(t).is_err());
            assert!(dm.refresh_oauth(t).is_err());
            assert!(dm.exchange_device_token(t).is_err());
            assert!(
                !dm.revoke_device_token(t).unwrap(),
                "stale token must not unregister the new pairing"
            );
        }
        assert!(dm.validate_token(&bearer(&new)).is_ok());
        // Self-revoke also bumps the epoch.
        assert!(dm.revoke_device_token(&new).unwrap());
        let newer = pair(&dm, "local-user", "RM110-1");
        assert!(dm.validate_token(&bearer(&new)).is_err());
        assert!(dm.validate_token(&bearer(&newer)).is_ok());
        // Refreshed tokens carry the current epoch and keep working.
        let (_, r, _) = dm.refresh_oauth(&newer).unwrap();
        assert!(dm.validate_token(&bearer(&r)).is_ok());
    }

    #[test]
    fn owner_round_trip_does_not_revive_first_owners_tokens() {
        let (dm, _tmp) = setup();
        let a1 = pair(&dm, "user-a", "RM110-X");
        let _b = pair(&dm, "user-b", "RM110-X");
        let a2 = pair(&dm, "user-a", "RM110-X");
        assert!(dm.validate_token(&bearer(&a1)).is_err());
        assert!(dm.validate_token(&bearer(&a2)).is_ok());
        // Plain same-user re-pairing (no delete, no owner change) keeps earlier tokens valid.
        let a3 = pair(&dm, "user-a", "RM110-X");
        assert!(dm.validate_token(&bearer(&a2)).is_ok() && dm.validate_token(&bearer(&a3)).is_ok());
    }

    #[test]
    fn refresh_never_recreates_or_reassigns_registration() {
        let (dm, _tmp) = setup();
        let a = pair(&dm, "user-a", "RM110-X");
        let _b = pair(&dm, "user-b", "RM110-X");
        assert!(dm.refresh_oauth(&a).is_err());
        assert!(dm.exchange_device_token(&a).is_err());
        assert_eq!(dm.get_device("RM110-X").unwrap().unwrap().user_id, "user-b");
    }

    #[test]
    fn epoch_zero_tokens_are_unchanged_on_the_wire() {
        let (dm, _tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        let payload = dt.split('.').nth(1).unwrap();
        let json = String::from_utf8(
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload)
                .unwrap(),
        )
        .unwrap();
        assert!(!json.contains("rms-epoch"), "{json}");
        // A pre-existing token (minted before this column/claim existed) is still accepted.
        assert!(dm.validate_token(&bearer(&dt)).is_ok());
        assert!(dm.refresh_oauth(&dt).is_ok());
    }

    /// A second connection to the same DB, to break things under the manager's feet.
    fn side_conn(tmp: &tempfile::TempDir) -> Connection {
        Connection::open(tmp.path().join("devices.db")).unwrap()
    }
    /// Make every epoch bump fail (both the insert and the ON CONFLICT update path).
    fn fail_epoch_bumps(tmp: &tempfile::TempDir) {
        side_conn(tmp).execute_batch("CREATE TRIGGER fail_ins BEFORE INSERT ON device_token_epochs BEGIN SELECT RAISE(ABORT, 'forced'); END; CREATE TRIGGER fail_upd BEFORE UPDATE ON device_token_epochs BEGIN SELECT RAISE(ABORT, 'forced'); END;").unwrap();
    }

    #[test]
    fn self_revoke_propagates_db_errors() {
        let (dm, tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        side_conn(&tmp)
            .execute_batch("ALTER TABLE devices RENAME TO devices_gone")
            .unwrap();
        assert!(
            matches!(dm.revoke_device_token(&dt), Err(ServerError::Database(_))),
            "a DB error is not 'already unregistered'"
        );
        side_conn(&tmp)
            .execute_batch("ALTER TABLE devices_gone RENAME TO devices")
            .unwrap();
        assert!(
            dm.validate_token(&bearer(&dt)).is_ok(),
            "nothing was deleted"
        );
        assert!(dm.revoke_device_token(&dt).unwrap());
    }

    #[test]
    fn failed_epoch_bump_rolls_back_the_delete() {
        let (dm, tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        pair(&dm, "local-user", "RM110-2");
        assert!(dm.delete_device("RM110-2", None).unwrap()); // RM110-2 now has an epochs row (update path)
        let other2 = pair(&dm, "local-user", "RM110-2");
        fail_epoch_bumps(&tmp);
        // Admin/user delete (insert path for RM110-1, update path for RM110-2) and self-unregister.
        assert!(dm.delete_device("RM110-1", None).is_err());
        assert!(dm.delete_device("RM110-2", Some("local-user")).is_err());
        assert!(dm.revoke_device_token(&dt).is_err());
        assert!(dm.revoke_device_token(&other2).is_err());
        for (d, t) in [("RM110-1", &dt), ("RM110-2", &other2)] {
            assert!(
                dm.get_device(d).unwrap().is_some(),
                "{d}: delete rolled back"
            );
            assert!(
                dm.validate_token(&bearer(t)).is_ok(),
                "{d}: token still valid, epoch unchanged"
            );
        }
        // Re-pairing to another user must not hand the device over un-revoked.
        let code = dm.create_pairing_code("user-b").unwrap();
        assert!(dm.exchange_code(&code, "RM110-1", "remarkable").is_err());
        assert_eq!(
            dm.get_device("RM110-1").unwrap().unwrap().user_id,
            "local-user"
        );
        assert!(dm.validate_token(&bearer(&dt)).is_ok());
        side_conn(&tmp)
            .execute_batch("DROP TRIGGER fail_ins; DROP TRIGGER fail_upd;")
            .unwrap();
        assert!(dm.delete_device("RM110-1", None).unwrap());
        assert!(dm.validate_token(&bearer(&dt)).is_err());
    }

    #[test]
    fn delete_device_respects_owner() {
        let (dm, _tmp) = setup();
        let b = pair(&dm, "user-b", "RM110-B");
        assert!(!dm.delete_device("RM110-B", Some("user-a")).unwrap());
        assert!(dm.validate_token(&bearer(&b)).is_ok());
        assert_eq!(dm.list_devices(Some("user-a")).unwrap().len(), 0);
        assert_eq!(dm.list_devices(Some("user-b")).unwrap().len(), 1);
        assert!(dm.delete_device("RM110-B", Some("user-b")).unwrap());
        assert!(dm.validate_token(&bearer(&b)).is_err());
    }

    fn payload_json(token: &str) -> String {
        let payload = token.split('.').nth(1).unwrap();
        String::from_utf8(
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload)
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn user_tokens_die_with_their_device() {
        let (dm, _tmp) = setup();
        let code = dm.create_pairing_code("local-user").unwrap();
        let (dt, paired_ut) = dm.exchange_code(&code, "RM110-1", "remarkable").unwrap();
        let refreshed = dm.refresh_user_token(&dt).unwrap();
        let (bundle_ut, ..) = dm.refresh_oauth(&dt).unwrap();
        let other = pair(&dm, "local-user", "RM110-2");
        let other_ut = dm.refresh_user_token(&other).unwrap();
        for t in [&paired_ut, &refreshed, &bundle_ut] {
            assert_eq!(dm.validate_token(&bearer(t)).unwrap(), "local-user");
        }
        assert!(dm.delete_device("RM110-1", None).unwrap());
        for t in [&paired_ut, &refreshed, &bundle_ut] {
            assert!(matches!(
                dm.validate_token(&bearer(t)),
                Err(ServerError::InvalidToken)
            ));
            assert!(dm.caller(&bearer(t)).is_err());
            assert!(dm.session_identity(&bearer(t)).is_err());
        }
        assert!(
            dm.validate_token(&bearer(&other_ut)).is_ok(),
            "other device's user token unaffected"
        );
        // Re-pairing the device does not revive user tokens minted before the delete.
        let new = pair(&dm, "local-user", "RM110-1");
        let new_ut = dm.refresh_user_token(&new).unwrap();
        assert!(payload_json(&new_ut).contains("\"rms-epoch\":1"));
        assert!(dm.validate_token(&bearer(&new_ut)).is_ok());
        for t in [&paired_ut, &refreshed, &bundle_ut] {
            assert!(dm.validate_token(&bearer(t)).is_err());
        }
        // Moving the device to another user kills the previous owner's user tokens too.
        let _b = pair(&dm, "user-b", "RM110-1");
        assert!(dm.validate_token(&bearer(&new_ut)).is_err());
        // Self-unregister (DELETE /token/json/3/device) does as well.
        let other_dt = other;
        assert!(dm.revoke_device_token(&other_dt).unwrap());
        assert!(dm.validate_token(&bearer(&other_ut)).is_err());
    }

    #[test]
    fn transient_db_error_does_not_end_a_session() {
        let (dm, tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        let id = dm.session_identity(&bearer(&dt)).unwrap();
        assert!(dm.session_still_valid(&id));
        // Break the devices table from outside: the re-check errors, but the session stays up.
        let other = Connection::open(tmp.path().join("devices.db")).unwrap();
        other
            .execute_batch("ALTER TABLE devices RENAME TO devices_gone")
            .unwrap();
        assert!(dm.session_still_valid(&id));
        other
            .execute_batch("ALTER TABLE devices_gone RENAME TO devices")
            .unwrap();
        // A real revocation still ends it.
        assert!(dm.delete_device("RM110-1", None).unwrap());
        assert!(!dm.session_still_valid(&id));
    }

    #[test]
    fn admin_user_tokens_are_not_tied_to_a_device() {
        let (dm, _tmp) = setup();
        let admin = dm.create_user_token("local-user").unwrap();
        let dt = pair(&dm, "local-user", "RM110-1");
        assert!(dm.delete_device("RM110-1", None).unwrap());
        assert!(dm.revoke_device_token(&dt).is_ok());
        assert_eq!(dm.validate_token(&bearer(&admin)).unwrap(), "local-user");
        let id = dm.session_identity(&bearer(&admin)).unwrap();
        assert_eq!(id.device_id, ADMIN_DEVICE_ID);
        assert!(dm.session_still_valid(&id));
        // The admin device id is reserved, so a real registration can't masquerade as one.
        let code = dm.create_pairing_code("local-user").unwrap();
        assert!(matches!(
            dm.exchange_code(&code, ADMIN_DEVICE_ID, "x"),
            Err(ServerError::BadRequest(_))
        ));
        assert!(dm.oauth_bundle("local-user", ADMIN_DEVICE_ID, "x").is_err());
        assert!(dm.get_device(ADMIN_DEVICE_ID).unwrap().is_none());
    }

    #[test]
    fn legacy_user_token_without_epoch_is_accepted_while_registered() {
        let (dm, _tmp) = setup();
        let dt = pair(&dm, "local-user", "RM110-1");
        // What the live tablet holds today: a user token minted before the claim existed.
        let legacy = dm
            .gen_user_token("RM110-1", "remarkable", "local-user", 0)
            .unwrap();
        assert!(!payload_json(&legacy).contains("rms-epoch"));
        assert!(
            !payload_json(&dm.refresh_user_token(&dt).unwrap()).contains("rms-epoch"),
            "epoch-0 user tokens are unchanged on the wire"
        );
        assert_eq!(
            dm.caller(&bearer(&legacy)).unwrap(),
            ("local-user".into(), "RM110-1".into(), "remarkable".into())
        );
        assert!(dm.delete_device("RM110-1", None).unwrap());
        assert!(dm.validate_token(&bearer(&legacy)).is_err());
        // A user token for a device that was never registered is refused.
        let unknown = dm
            .gen_user_token("never-paired", "x", "local-user", 0)
            .unwrap();
        assert!(dm.validate_token(&bearer(&unknown)).is_err());
    }

    #[tokio::test]
    async fn session_revoked_resolves_only_for_its_device() {
        use std::time::Duration;
        let (dm, tmp) = setup();
        let a = pair(&dm, "local-user", "RM110-1");
        let b = pair(&dm, "local-user", "RM110-2");
        let ut_a = dm.refresh_user_token(&a).unwrap();
        let ida = dm.session_identity(&bearer(&ut_a)).unwrap();
        let idb = dm.session_identity(&bearer(&b)).unwrap();
        let mut rev_a = Box::pin(dm.session_revoked(&ida));
        let mut rev_b = Box::pin(dm.session_revoked(&idb));
        async fn pending(f: impl std::future::Future<Output = ()>) -> bool {
            tokio::time::timeout(Duration::from_millis(50), f)
                .await
                .is_err()
        }
        assert!(pending(&mut rev_a).await && pending(&mut rev_b).await);
        assert!(dm.delete_device("RM110-1", None).unwrap());
        tokio::time::timeout(Duration::from_secs(5), &mut rev_a)
            .await
            .expect("revoked on the delete event");
        assert!(pending(&mut rev_b).await, "other device's session stays");
        assert!(!dm.session_still_valid(&ida) && dm.session_still_valid(&idb));
        // A session for an already-revoked identity resolves at once.
        tokio::time::timeout(Duration::from_secs(5), dm.session_revoked(&ida))
            .await
            .unwrap();
        // Backstop: a registration that vanishes without an event is caught by the re-check.
        let rev_b = dm.session_revoked_every(&idb, Duration::from_millis(20));
        side_conn(&tmp)
            .execute("DELETE FROM devices WHERE device_id = 'RM110-2'", [])
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), rev_b)
            .await
            .expect("periodic re-check closes it");
    }

    #[test]
    fn user_tokens_keep_exp_validation() {
        let (dm, _tmp) = setup();
        let ut = dm.create_user_token("local-user").unwrap();
        assert_eq!(dm.validate_token(&bearer(&ut)).unwrap(), "local-user");
        let now = Utc::now().timestamp();
        let expired = encode(
            &Header::new(Algorithm::HS256),
            &UserTokenClaims {
                sub: "local-user".into(),
                iss: "local.test".into(),
                iat: now - 7200,
                exp: now - 3600,
                nbf: now - 7200,
                jti: "x".into(),
                tectonic: "local".into(),
                scopes: USER_SCOPES.into(),
                auth0_profile: Auth0Profile {
                    user_id: "local-user".into(),
                    email: String::new(),
                    name: String::new(),
                    nickname: String::new(),
                    level: String::new(),
                    is_connected: true,
                    is_beta: false,
                },
                device_id: "RM110-1".into(),
                device_desc: "remarkable".into(),
                subscription: SubscriptionClaim {
                    status: "active".into(),
                    plan: "connect".into(),
                },
                epoch: 0,
            },
            &dm.inner.encoding_key,
        )
        .unwrap();
        assert!(dm.validate_token(&bearer(&expired)).is_err());
        assert!(dm.caller(&bearer(&expired)).is_err());
    }
}
