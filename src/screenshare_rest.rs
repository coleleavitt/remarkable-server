//! REST screenshare room broker for xochitl 3.27+/3.28 (`/screenshare/v1`).
//!
//! The 3.28 tablet no longer uses the MQTT signalling broker; it drives WebRTC
//! rooms over six REST routes and receives peer signalling as `ScreenshareMessage`
//! events on the notifications channel. The screen itself is peer-to-peer (RFB over
//! a WebRTC DataChannel); the server only tracks rooms, hands out ICE servers and
//! relays signalling. Modelled on rmfakecloud `internal/screenshare/rooms.go` and
//! the recovered xochitl contract (RoomBroker `sub_8889E8`, RestBroker).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use base64::Engine;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{Value, json};

use crate::api::AppState;
use crate::device::{DeviceManager, DeviceRevoked, SESSION_RECHECK, SessionIdentity};
use crate::error::{Result, ServerError};
use crate::notifications::WsMessage;

/// A room with no keepalive for this long is dropped (matches rmfakecloud's 60s).
const ROOM_TIMEOUT: Duration = Duration::from_secs(60);
/// The JSON field carrying the signalling body in broadcast/direct posts.
const BODY_KEY: &str = "payload";

#[derive(Clone, Serialize)]
pub struct RoomClient {
    #[serde(rename = "clientId")]
    pub client_id: String,
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(rename = "isOwner")]
    pub is_owner: bool,
    /// The device registration the client joined under; `None` for an in-process client (the
    /// server's own viewer), which no revocation concerns.
    #[serde(skip)]
    registration: Option<SessionIdentity>,
}

impl RoomClient {
    /// A device, as the registration its token was minted under.
    fn device(who: &SessionIdentity, is_owner: bool) -> Self {
        Self {
            client_id: who.device_id.clone(),
            user_id: who.user_id.clone(),
            is_owner,
            registration: Some(who.clone()),
        }
    }
}

struct Room {
    created_at: chrono::DateTime<chrono::Utc>,
    last_activity: Instant,
    owner_user_id: String,
    participants: HashMap<String, RoomClient>,
}

/// In-memory room registry. Clone-cheap (shared `Arc`).
#[derive(Clone, Default)]
pub struct RoomManager {
    rooms: Arc<Mutex<HashMap<String, Room>>>,
}

impl RoomManager {
    pub fn new() -> Self {
        Self::default()
    }

    fn sweep(rooms: &mut HashMap<String, Room>) {
        rooms.retain(|_, r| r.last_activity.elapsed() <= ROOM_TIMEOUT);
    }

    /// Create a room owned by `owner`'s account, with its device as the owner client.
    fn create(&self, owner: &SessionIdentity) -> (String, String) {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        let room_id = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now();
        let participants =
            HashMap::from([(owner.device_id.clone(), RoomClient::device(owner, true))]);
        rooms.insert(
            room_id.clone(),
            Room {
                created_at,
                last_activity: Instant::now(),
                owner_user_id: owner.user_id.clone(),
                participants,
            },
        );
        (room_id, created_at.to_rfc3339())
    }

    /// Newest live room owned by `user_id`, if any.
    fn find_active(&self, user_id: &str) -> Option<String> {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        rooms
            .iter()
            .filter(|(_, r)| r.owner_user_id == user_id)
            .max_by_key(|(_, r)| r.created_at)
            .map(|(id, _)| id.clone())
    }

    fn add_participant(&self, room_id: &str, who: &SessionIdentity) {
        if let Some(r) = self.rooms.lock().get_mut(room_id) {
            r.participants
                .entry(who.device_id.clone())
                .or_insert_with(|| RoomClient::device(who, false));
        }
    }

    fn clients(&self, room_id: &str) -> Vec<RoomClient> {
        self.rooms
            .lock()
            .get(room_id)
            .map(|r| r.participants.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Refresh `room_id`'s activity so it isn't swept, if `user_id` owns it.
    /// Checked and refreshed under one lock; false if the room is gone or
    /// belongs to another account.
    pub fn keepalive(&self, room_id: &str, user_id: &str) -> bool {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        match rooms.get_mut(room_id) {
            Some(r) if r.owner_user_id == user_id => {
                r.last_activity = Instant::now();
                true
            }
            _ => false,
        }
    }

    /// Whether `room_id` exists and belongs to `user_id`.
    fn owned_by(&self, room_id: &str, user_id: &str) -> bool {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        rooms
            .get(room_id)
            .is_some_and(|r| r.owner_user_id == user_id)
    }

    /// `device_id` leaves `room_id`; the room goes away only when its owner
    /// leaves. (The desktop viewer DELETEs rooms it merely joined when it
    /// disconnects, which must not end the tablet's share.)
    fn leave_or_close(&self, room_id: &str, device_id: &str) {
        let mut rooms = self.rooms.lock();
        let Some(room) = rooms.get_mut(room_id) else {
            return;
        };
        if room.participants.get(device_id).is_some_and(|c| c.is_owner) {
            rooms.remove(room_id);
        } else {
            room.participants.remove(device_id);
        }
    }

    /// Every device participant, as (room, the registration it joined under).
    fn registrations(&self) -> Vec<(String, SessionIdentity)> {
        let rooms = self.rooms.lock();
        rooms
            .iter()
            .flat_map(|(room_id, r)| {
                r.participants
                    .values()
                    .filter_map(|c| Some((room_id.clone(), c.registration.clone()?)))
            })
            .collect()
    }

    /// Take each (room, registration) participant out of its room: a room it owns closes (it can
    /// no longer share into it), one it only joined loses it. Only that very registration goes:
    /// the device joined under a later one (paired again) is another participant and stays.
    fn drop_registrations(&self, ended: &[(String, SessionIdentity)]) {
        let mut rooms = self.rooms.lock();
        for (room_id, reg) in ended {
            let Some(r) = rooms.get_mut(room_id) else {
                continue;
            };
            let Some(c) = r
                .participants
                .get(&reg.device_id)
                .filter(|c| c.registration.as_ref() == Some(reg))
            else {
                continue;
            };
            let device_id = &reg.device_id;
            if c.is_owner {
                tracing::info!(%room_id, %device_id, "screenshare room closed: owner device revoked");
                rooms.remove(room_id);
            } else {
                tracing::info!(%room_id, %device_id, "revoked device left screenshare room");
                r.participants.remove(device_id);
            }
        }
    }

    fn exists(&self, room_id: &str) -> bool {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        rooms.contains_key(room_id)
    }

    fn room_meta(&self, room_id: &str) -> Option<String> {
        self.rooms
            .lock()
            .get(room_id)
            .map(|r| r.created_at.to_rfc3339())
    }

    /// Newest live room owned by `user_id`, for in-process participants such
    /// as the server's screen share viewer.
    pub fn active_room(&self, user_id: &str) -> Option<String> {
        self.find_active(user_id)
    }

    /// Like [`active_room`](Self::active_room), with how long ago it was created.
    pub fn active_room_age(&self, user_id: &str) -> Option<(String, std::time::Duration)> {
        let id = self.find_active(user_id)?;
        let created = self.rooms.lock().get(&id)?.created_at;
        // A future `created_at` (clock skew / backward clock jump) makes the age
        // negative; `to_std()` errors on that. Treat it as very old rather than
        // age 0, so a skewed room can't masquerade as "just created" and always
        // win newest-room selection.
        Some((
            id,
            (chrono::Utc::now() - created)
                .to_std()
                .unwrap_or(std::time::Duration::MAX),
        ))
    }

    /// Join `room_id` of `user_id`'s account as an in-process client, such as the server's
    /// viewer, which holds no device registration. See [`join_as`](Self::join_as).
    pub fn join(&self, room_id: &str, client_id: &str, user_id: &str) -> bool {
        self.join_as(
            room_id,
            RoomClient {
                client_id: client_id.to_string(),
                user_id: user_id.to_string(),
                is_owner: false,
                registration: None,
            },
        )
    }

    /// Join `room_id` as `client`; false if the room is gone or belongs to another account.
    /// Checked and joined under one lock.
    fn join_as(&self, room_id: &str, client: RoomClient) -> bool {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        let Some(r) = rooms
            .get_mut(room_id)
            .filter(|r| r.owner_user_id == client.user_id)
        else {
            return false;
        };
        r.participants
            .entry(client.client_id.clone())
            .or_insert(client);
        r.last_activity = Instant::now();
        true
    }

    /// Leave `room_id`.
    pub fn leave(&self, room_id: &str, client_id: &str) {
        if let Some(r) = self.rooms.lock().get_mut(room_id) {
            r.participants.remove(client_id);
        }
    }
}

/// Take revoked devices out of REST rooms: on each revocation event (see
/// [`DeviceManager::subscribe_revocations`]), and for every room after missed events and every
/// [`SESSION_RECHECK`], which also catches an event whose check hit a DB error. Every REST call
/// authenticates afresh, so a revoked device can't act in a room anyway; this closes the rooms it
/// owned, which a viewer's keepalives would otherwise keep alive.
pub fn spawn_revocation_cleanup(
    devices: DeviceManager,
    rooms: RoomManager,
) -> tokio::task::JoinHandle<()> {
    use tokio::sync::broadcast::error::RecvError;
    let mut events = devices.subscribe_revocations();
    tokio::spawn(async move {
        let mut recheck = tokio::time::interval_at(
            tokio::time::Instant::now() + SESSION_RECHECK,
            SESSION_RECHECK,
        );
        recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let device = tokio::select! {
                ev = events.recv() => match ev {
                    Ok(ev) => Some(ev),
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!("screenshare room cleanup missed {n} revocation events; re-checking every room");
                        None
                    }
                    Err(RecvError::Closed) => return,
                },
                _ = recheck.tick() => None,
            };
            let ended = ended_registrations(&devices, &rooms, device.as_ref());
            rooms.drop_registrations(&ended);
        }
    })
}

/// The device participants whose registration has ended: in every room, or only `device`'s (on
/// its revocation event). Only a definite "not registered / revoked" counts; on a DB error the
/// participant stays until a later check gets an answer (the periodic one at the latest).
///
/// Checked outside the room lock (a DB lookup each), yet dropping them afterwards stays exact: an
/// ended registration never comes back (a re-pair gets a new epoch), and whatever the device
/// makes or joins under the new one holds that one instead.
fn ended_registrations(
    devices: &DeviceManager,
    rooms: &RoomManager,
    device: Option<&DeviceRevoked>,
) -> Vec<(String, SessionIdentity)> {
    rooms
        .registrations()
        .into_iter()
        .filter(|(_, reg)| {
            device.is_none_or(|d| d.user_id == reg.user_id && d.device_id == reg.device_id)
        })
        .filter(|(_, reg)| !devices.session_still_valid(reg))
        .collect()
}

fn authz(headers: &HeaderMap) -> Result<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::Unauthorized)
}

/// `POST /screenshare/v1/rooms` -> 201 `{roomId, createdAt, iceServers}`.
pub async fn create_room(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<Value>)> {
    let who = state.devices.session_identity(authz(&headers)?)?;
    let (room_id, created_at) = state.screenshare.create(&who);
    // Tell the user's other clients to join. sourceDeviceID = creator, so the creator drops it.
    let _ = state
        .notification_tx
        .send(WsMessage::screenshare_room_created(
            &who.user_id,
            &who.device_id,
            &room_id,
        ));
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "roomId": room_id, "createdAt": created_at, "iceServers": &*state.ice_servers,
        })),
    ))
}

/// `POST /screenshare/v1/rooms/join-active` -> 200 `{roomId, clients, iceServers}` or 404.
pub async fn join_active(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<Value>)> {
    let who = state.devices.session_identity(authz(&headers)?)?;
    let Some(room_id) = state.screenshare.find_active(&who.user_id) else {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no active room"})),
        ));
    };
    state.screenshare.add_participant(&room_id, &who);
    Ok((
        StatusCode::OK,
        Json(json!({
            "roomId": room_id, "clients": state.screenshare.clients(&room_id), "iceServers": &*state.ice_servers,
        })),
    ))
}

/// `GET /screenshare/v1/rooms/{roomId}` -> 200 `{roomId, createdAt, clients}` or 404 (viewer use).
pub async fn get_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Result<(StatusCode, Json<Value>)> {
    let user_id = state.auth_user(&headers)?;
    if !state.screenshare.owned_by(&room_id, &user_id) {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "room not found"})),
        ));
    }
    match state.screenshare.room_meta(&room_id) {
        Some(created_at) => Ok((
            StatusCode::OK,
            Json(json!({
                "roomId": room_id, "createdAt": created_at, "clients": state.screenshare.clients(&room_id),
            })),
        )),
        None => Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "room not found"})),
        )),
    }
}

/// `POST /screenshare/v1/rooms/{roomId}/keepalive` -> 200, or 404 when the
/// room is gone (swept, or lost in a restart) so the tablet notices instead of
/// sharing into nothing; the desktop treats a failed keepalive as
/// `connectionRefused`.
pub async fn keepalive(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Result<StatusCode> {
    let user_id = state.auth_user(&headers)?;
    if !state.screenshare.keepalive(&room_id, &user_id) {
        return Err(ServerError::NotFound("room not found".into()));
    }
    Ok(StatusCode::OK)
}

/// `POST /screenshare/v1/rooms/{roomId}/join` -> 200 `{roomId, clients, iceServers}` or 404.
/// RoomBroker::joinRoom in desktop 3.28 (unused there so far).
pub async fn join_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Result<(StatusCode, Json<Value>)> {
    let who = state.devices.session_identity(authz(&headers)?)?;
    if !state
        .screenshare
        .join_as(&room_id, RoomClient::device(&who, false))
    {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "room not found"})),
        ));
    }
    Ok((
        StatusCode::OK,
        Json(json!({
            "roomId": room_id, "clients": state.screenshare.clients(&room_id), "iceServers": &*state.ice_servers,
        })),
    ))
}

/// `DELETE /screenshare/v1/rooms/{roomId}` -> 204. Closes the room for its
/// owner; anyone else just leaves it.
pub async fn delete_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Result<StatusCode> {
    let (_, device_id, _) = state.devices.caller(authz(&headers)?)?;
    state.screenshare.leave_or_close(&room_id, &device_id);
    Ok(StatusCode::NO_CONTENT)
}

/// Pull the signalling body out of the posted object: senders wrap it as `{ "<BODY_KEY>": <obj> }`.
fn inner_body(body: &Value) -> Vec<u8> {
    let inner = body.get(BODY_KEY).unwrap_or(body);
    serde_json::to_vec(inner).unwrap_or_default()
}

/// `POST /screenshare/v1/rooms/{roomId}/messages/broadcast` -> 200. Relays to all the user's clients.
pub async fn broadcast(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    if !state.screenshare.exists(&room_id) {
        return Err(ServerError::NotFound("room not found".into()));
    }
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(inner_body(&body));
    let _ = state.notification_tx.send(WsMessage::screenshare_message(
        &user_id, &device_id, &room_id, None, &data_b64,
    ));
    Ok(StatusCode::OK)
}

/// `POST /screenshare/v1/rooms/{roomId}/messages/direct` -> 200. Relays to one target client.
pub async fn direct(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<StatusCode> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    if !state.screenshare.exists(&room_id) {
        return Err(ServerError::NotFound("room not found".into()));
    }
    let target = body.get("targetClientId").and_then(|v| v.as_str());
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(inner_body(&body));
    let _ = state.notification_tx.send(WsMessage::screenshare_message(
        &user_id, &device_id, &room_id, target, &data_b64,
    ));
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    fn state_and_auth() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        let state = AppState::new(storage, devices)
            .with_ice_servers(serde_json::json!([{"url": "stun:localhost:3478"}]));
        (state, tk, tmp)
    }

    fn hdrs(tk: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {tk}")).unwrap(),
        );
        h
    }

    fn wrap(target: Option<&str>, inner: Value) -> Value {
        let mut m = serde_json::Map::new();
        if let Some(t) = target {
            m.insert("targetClientId".into(), Value::String(t.into()));
        }
        m.insert(BODY_KEY.to_string(), inner);
        Value::Object(m)
    }

    #[tokio::test]
    async fn create_join_relay_delete() {
        let (state, tk, _tmp) = state_and_auth();
        let mut rx = state.notification_tx.subscribe();

        // create -> 201 {roomId, iceServers}, and a ScreenshareRoomCreated push
        let (code, Json(body)) = create_room(State(state.clone()), hdrs(&tk)).await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let room_id = body["roomId"].as_str().unwrap().to_string();
        assert!(body["iceServers"].as_array().is_some_and(|a| !a.is_empty()));
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.message.attributes.event, "ScreenshareRoomCreated");
        assert_eq!(
            ev.message.attributes.room_id.as_deref(),
            Some(room_id.as_str())
        );

        // join-active finds the live room
        let (code, Json(body)) = join_active(State(state.clone()), hdrs(&tk)).await.unwrap();
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["roomId"].as_str().unwrap(), room_id);

        // broadcast relays base64 of the inner body only
        let inner = serde_json::json!({"type": "request-offer"});
        let code = broadcast(
            State(state.clone()),
            hdrs(&tk),
            Path(room_id.clone()),
            Json(wrap(None, inner.clone())),
        )
        .await
        .unwrap();
        assert_eq!(code, StatusCode::OK);
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.message.attributes.event, "ScreenshareMessage");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(ev.message.data.unwrap())
            .unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&decoded).unwrap(), inner);

        // direct carries targetClientId
        let _ = direct(
            State(state.clone()),
            hdrs(&tk),
            Path(room_id.clone()),
            Json(wrap(Some("peer-1"), serde_json::json!({"type": "webtrc"}))),
        )
        .await
        .unwrap();
        let ev = rx.recv().await.unwrap();
        assert_eq!(
            ev.message.attributes.target_client_id.as_deref(),
            Some("peer-1")
        );

        // delete -> 204, then join-active is 404
        assert_eq!(
            delete_room(State(state.clone()), hdrs(&tk), Path(room_id.clone()))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        let (code, _) = join_active(State(state.clone()), hdrs(&tk)).await.unwrap();
        assert_eq!(code, StatusCode::NOT_FOUND);

        // relay into a missing room errors; missing auth is rejected
        assert!(
            broadcast(
                State(state.clone()),
                hdrs(&tk),
                Path("nope".into()),
                Json(serde_json::json!({}))
            )
            .await
            .is_err()
        );
        assert!(
            create_room(State(state.clone()), HeaderMap::new())
                .await
                .is_err()
        );
    }

    /// Pair `device` to `user` and return its device token.
    fn pair(dm: &DeviceManager, user: &str, device: &str) -> String {
        let code = dm.create_pairing_code(user).unwrap();
        dm.exchange_code(&code, device, "remarkable").unwrap().0
    }

    fn identity(dm: &DeviceManager, token: &str) -> SessionIdentity {
        dm.session_identity(&format!("Bearer {token}")).unwrap()
    }

    fn client_ids(rooms: &RoomManager, room_id: &str) -> Vec<String> {
        let mut ids: Vec<_> = rooms
            .clients(room_id)
            .into_iter()
            .map(|c| c.client_id)
            .collect();
        ids.sort();
        ids
    }

    /// Let the cleanup task handle what is pending. The clock is paused, and it only moves once
    /// every task is idle, so by the end of this sleep the task has done all it can.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn revoked_devices_leave_their_rooms() {
        let (state, _tk, _tmp) = state_and_auth();
        let dm = &state.devices;
        let tablet = pair(dm, "local-user", "RM110-1");
        let other = pair(dm, "local-user", "RM110-2");
        let rooms = &state.screenshare;
        let cleanup = spawn_revocation_cleanup(dm.clone(), rooms.clone());

        // The tablet shares; the in-process viewer and a second tablet join.
        let (code, Json(body)) = create_room(State(state.clone()), hdrs(&tablet))
            .await
            .unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let shared = body["roomId"].as_str().unwrap().to_string();
        assert!(rooms.join(&shared, "viewer", "local-user"));
        rooms.add_participant(&shared, &identity(dm, &other));
        // The second tablet's own room, which the first one joined.
        let (other_room, _) = rooms.create(&identity(dm, &other));
        rooms.add_participant(&other_room, &identity(dm, &tablet));

        assert!(dm.delete_device("RM110-1", None).unwrap());
        settle().await;
        assert!(!rooms.exists(&shared), "owner revoked, room kept");
        // The viewer's keepalive now fails, so it stops watching a room nobody shares into.
        assert!(!rooms.keepalive(&shared, "local-user"));
        assert_eq!(
            client_ids(rooms, &other_room),
            ["RM110-2"],
            "revoked member left, owner stays"
        );
        assert!(rooms.keepalive(&other_room, "local-user"));
        let (code, _) = join_active(State(state.clone()), hdrs(&other))
            .await
            .unwrap();
        assert_eq!(code, StatusCode::OK);
        cleanup.abort();
    }

    #[tokio::test]
    async fn late_revocation_event_spares_a_same_account_re_pair() {
        let (state, _tk, _tmp) = state_and_auth();
        let (dm, rooms) = (&state.devices, &state.screenshare);
        let old = identity(dm, &pair(dm, "local-user", "RM110-1"));
        let (old_room, _) = rooms.create(&old);
        // A device of the same account whose id differs only in case (ids are case-sensitive, as
        // in the token checks) has a room too, which the tablet joined.
        let twin = identity(dm, &pair(dm, "local-user", "rm110-1"));
        let (twin_room, _) = rooms.create(&twin);
        rooms.add_participant(&twin_room, &old);

        // Deleted and paired straight back, and the event is handled only now. Its registrations
        // are checked before the re-paired tablet shares again and rejoins the twin's room, and
        // dropped after: the widest window between the two.
        assert!(dm.delete_device("RM110-1", None).unwrap());
        let new = identity(dm, &pair(dm, "local-user", "RM110-1"));
        let ev = DeviceRevoked {
            user_id: "local-user".into(),
            device_id: "RM110-1".into(),
        };
        let ended = ended_registrations(dm, rooms, Some(&ev));
        let (new_room, _) = rooms.create(&new);
        rooms.leave_or_close(&twin_room, "RM110-1");
        rooms.add_participant(&twin_room, &new);
        rooms.drop_registrations(&ended);

        assert!(
            !rooms.exists(&old_room),
            "the revoked registration's room closed"
        );
        assert!(rooms.exists(&new_room), "the re-pair's room was closed");
        assert_eq!(client_ids(rooms, &twin_room), ["RM110-1", "rm110-1"]);
        // Handling the event again finds nothing more to drop.
        rooms.drop_registrations(&ended_registrations(dm, rooms, Some(&ev)));
        assert!(rooms.exists(&new_room));
        assert_eq!(client_ids(rooms, &twin_room), ["RM110-1", "rm110-1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn event_checked_during_a_db_error_is_caught_by_the_recheck() {
        let (state, _tk, tmp) = state_and_auth();
        let (dm, rooms) = (&state.devices, &state.screenshare);
        let tablet = identity(dm, &pair(dm, "local-user", "RM110-1"));
        let (room, _) = rooms.create(&tablet);
        let cleanup = spawn_revocation_cleanup(dm.clone(), rooms.clone());
        let side = rusqlite::Connection::open(tmp.path().join("devices.db")).unwrap();

        // The event arrives while every lookup fails: the room is not closed on a guess.
        assert!(dm.delete_device("RM110-1", None).unwrap());
        side.execute_batch("ALTER TABLE devices RENAME TO devices_gone")
            .unwrap();
        settle().await;
        assert!(rooms.exists(&room), "closed on a failed lookup");
        // Nor by a periodic re-check that fails too.
        tokio::time::sleep(SESSION_RECHECK).await;
        assert!(rooms.exists(&room));

        // The next re-check that gets an answer closes it.
        side.execute_batch("ALTER TABLE devices_gone RENAME TO devices")
            .unwrap();
        tokio::time::sleep(SESSION_RECHECK).await;
        assert!(
            !rooms.exists(&room),
            "revoked owner's room kept after the DB came back"
        );
        cleanup.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn missed_revocation_events_are_caught_up_at_once() {
        let (state, _tk, _tmp) = state_and_auth();
        let (dm, rooms) = (&state.devices, &state.screenshare);
        let revoked = identity(dm, &pair(dm, "local-user", "RM110-1"));
        let (revoked_room, _) = rooms.create(&revoked);
        let other = identity(dm, &pair(dm, "local-user", "RM110-2"));
        let (other_room, _) = rooms.create(&other);
        assert!(rooms.join(&other_room, "viewer", "local-user"));
        let cleanup = spawn_revocation_cleanup(dm.clone(), rooms.clone());

        // The tablet's event, then more than the channel holds (64) before the task runs: its
        // event is lost and the task only hears that it lagged.
        assert!(dm.delete_device("RM110-1", None).unwrap());
        for i in 0..70 {
            dm.announce_revoked_for_test("local-user", &format!("spare-{i}"));
        }
        let start = tokio::time::Instant::now();
        settle().await;
        assert!(start.elapsed() < SESSION_RECHECK);
        assert!(
            !rooms.exists(&revoked_room),
            "room of a missed revocation kept"
        );
        // In-process clients hold no registration: the re-check leaves them be.
        assert_eq!(client_ids(rooms, &other_room), ["RM110-2", "viewer"]);
        cleanup.abort();
    }

    #[tokio::test]
    async fn only_the_owner_closes_a_room() {
        let (state, owner, _tmp) = state_and_auth();
        let (viewer, _, _) = state
            .devices
            .oauth_bundle("u@test", "viewer-device", "desktop")
            .unwrap();
        let (_, Json(body)) = create_room(State(state.clone()), hdrs(&owner))
            .await
            .unwrap();
        let room_id = body["roomId"].as_str().unwrap().to_string();

        // The viewer joins by id, then DELETEs as the desktop does on disconnect.
        let (code, Json(body)) =
            join_room(State(state.clone()), hdrs(&viewer), Path(room_id.clone()))
                .await
                .unwrap();
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["clients"].as_array().unwrap().len(), 2);
        delete_room(State(state.clone()), hdrs(&viewer), Path(room_id.clone()))
            .await
            .unwrap();
        assert!(
            state.screenshare.exists(&room_id),
            "a viewer's DELETE closed the tablet's room"
        );
        assert_eq!(state.screenshare.clients(&room_id).len(), 1);

        delete_room(State(state.clone()), hdrs(&owner), Path(room_id.clone()))
            .await
            .unwrap();
        assert!(!state.screenshare.exists(&room_id));
    }

    #[tokio::test]
    async fn keepalive_and_join_report_missing_rooms() {
        let (state, tk, _tmp) = state_and_auth();
        assert!(
            keepalive(State(state.clone()), hdrs(&tk), Path("gone".into()))
                .await
                .is_err()
        );
        let (code, _) = join_room(State(state.clone()), hdrs(&tk), Path("gone".into()))
            .await
            .unwrap();
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn other_accounts_cannot_use_a_room() {
        let (state, owner, _tmp) = state_and_auth();
        let stranger = state
            .devices
            .create_user_token("someone-else@test")
            .unwrap();
        let (_, Json(body)) = create_room(State(state.clone()), hdrs(&owner))
            .await
            .unwrap();
        let room_id = body["roomId"].as_str().unwrap().to_string();

        let (code, _) = join_room(State(state.clone()), hdrs(&stranger), Path(room_id.clone()))
            .await
            .unwrap();
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, _) = get_room(State(state.clone()), hdrs(&stranger), Path(room_id.clone()))
            .await
            .unwrap();
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert!(
            keepalive(State(state.clone()), hdrs(&stranger), Path(room_id.clone()))
                .await
                .is_err()
        );
        assert_eq!(state.screenshare.clients(&room_id).len(), 1);
        // The owner still can.
        assert_eq!(
            keepalive(State(state.clone()), hdrs(&owner), Path(room_id.clone()))
                .await
                .unwrap(),
            StatusCode::OK
        );
    }
}
