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
use crate::device::{DeviceManager, DeviceRevoked};
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

    /// Create a room owned by `user_id`, with `device_id` as the owner client.
    fn create(&self, user_id: &str, device_id: &str) -> (String, String) {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        let room_id = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now();
        let mut participants = HashMap::new();
        participants.insert(
            device_id.to_string(),
            RoomClient {
                client_id: device_id.to_string(),
                user_id: user_id.to_string(),
                is_owner: true,
            },
        );
        rooms.insert(
            room_id.clone(),
            Room {
                created_at,
                last_activity: Instant::now(),
                owner_user_id: user_id.to_string(),
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

    fn add_participant(&self, room_id: &str, client_id: &str, user_id: &str) {
        if let Some(r) = self.rooms.lock().get_mut(room_id) {
            r.participants
                .entry(client_id.to_string())
                .or_insert(RoomClient {
                    client_id: client_id.to_string(),
                    user_id: user_id.to_string(),
                    is_owner: false,
                });
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

    /// Take a revoked device out of its account's rooms: rooms it owns close (it can no longer
    /// share into them), rooms it only joined lose it as a participant.
    fn drop_device(&self, user_id: &str, device_id: &str) {
        self.rooms.lock().retain(|room_id, r| {
            if r.owner_user_id != user_id {
                return true;
            }
            match r.participants.get(device_id) {
                Some(c) if c.is_owner => {
                    tracing::info!(%room_id, %device_id, "screenshare room closed: owner device revoked");
                    false
                }
                Some(_) => {
                    r.participants.remove(device_id);
                    true
                }
                None => true,
            }
        });
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

    /// Join `room_id` of `user_id`'s account; false if the room is gone or
    /// belongs to another account. Checked and joined under one lock.
    pub fn join(&self, room_id: &str, client_id: &str, user_id: &str) -> bool {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        let Some(r) = rooms
            .get_mut(room_id)
            .filter(|r| r.owner_user_id == user_id)
        else {
            return false;
        };
        r.participants
            .entry(client_id.to_string())
            .or_insert(RoomClient {
                client_id: client_id.to_string(),
                user_id: user_id.to_string(),
                is_owner: false,
            });
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

/// Drop revoked devices from REST rooms as their revocation events arrive (see
/// [`DeviceManager::subscribe_revocations`]). Every REST call authenticates afresh, so a revoked
/// device can't act in a room anyway; this closes the rooms it owned, which a viewer's keepalives
/// would otherwise keep alive.
pub fn spawn_revocation_cleanup(
    devices: DeviceManager,
    rooms: RoomManager,
) -> tokio::task::JoinHandle<()> {
    use tokio::sync::broadcast::error::RecvError;
    let mut events = devices.subscribe_revocations();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(ev) => on_revoked(&devices, &rooms, &ev),
                // Missed rooms still expire after ROOM_TIMEOUT once nobody keeps them alive.
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!("screenshare room cleanup missed {n} revocation events")
                }
                Err(RecvError::Closed) => return,
            }
        }
    })
}

fn on_revoked(devices: &DeviceManager, rooms: &RoomManager, ev: &DeviceRevoked) {
    // Deleted and at once paired again to the same account: rooms it has now may belong to the
    // new registration, so leave them be.
    if matches!(devices.get_device(&ev.device_id), Ok(Some(d)) if d.user_id == ev.user_id) {
        return;
    }
    rooms.drop_device(&ev.user_id, &ev.device_id);
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
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    let (room_id, created_at) = state.screenshare.create(&user_id, &device_id);
    // Tell the user's other clients to join. sourceDeviceID = creator, so the creator drops it.
    let _ = state
        .notification_tx
        .send(WsMessage::screenshare_room_created(
            &user_id, &device_id, &room_id,
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
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    let Some(room_id) = state.screenshare.find_active(&user_id) else {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no active room"})),
        ));
    };
    state
        .screenshare
        .add_participant(&room_id, &device_id, &user_id);
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
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    if !state.screenshare.join(&room_id, &device_id, &user_id) {
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

    #[tokio::test]
    async fn revoked_devices_leave_their_rooms() {
        let (state, _tk, _tmp) = state_and_auth();
        let dm = &state.devices;
        let pair = |user: &str, device: &str| {
            let code = dm.create_pairing_code(user).unwrap();
            dm.exchange_code(&code, device, "remarkable").unwrap().0
        };
        let tablet = pair("local-user", "RM110-1");
        let other = pair("local-user", "RM110-2");
        let rooms = &state.screenshare;
        let cleanup = spawn_revocation_cleanup(dm.clone(), rooms.clone());

        // The tablet shares; the in-process viewer and a second tablet join.
        let (code, Json(body)) = create_room(State(state.clone()), hdrs(&tablet))
            .await
            .unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let shared = body["roomId"].as_str().unwrap().to_string();
        assert!(rooms.join(&shared, "viewer", "local-user"));
        rooms.add_participant(&shared, "RM110-2", "local-user");
        // The second tablet's own room, which the first one joined.
        let (other_room, _) = rooms.create("local-user", "RM110-2");
        rooms.add_participant(&other_room, "RM110-1", "local-user");

        assert!(dm.delete_device("RM110-1", None).unwrap());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while rooms.exists(&shared) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "owner revoked, room kept"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // The viewer's keepalive now fails, so it stops watching a room nobody shares into.
        assert!(!rooms.keepalive(&shared, "local-user"));
        let ids: Vec<_> = rooms
            .clients(&other_room)
            .into_iter()
            .map(|c| c.client_id)
            .collect();
        assert_eq!(ids, ["RM110-2"], "revoked member left, owner stays");
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
        let dm = &state.devices;
        let code = dm.create_pairing_code("local-user").unwrap();
        dm.exchange_code(&code, "RM110-1", "remarkable").unwrap();
        let (room, _) = state.screenshare.create("local-user", "RM110-1");
        // The event of an earlier delete, handled after the device was paired back.
        let ev = DeviceRevoked {
            user_id: "local-user".into(),
            device_id: "RM110-1".into(),
        };
        on_revoked(dm, &state.screenshare, &ev);
        assert!(state.screenshare.exists(&room));
        // Handled while it is gone, the event does close the room.
        assert!(dm.delete_device("RM110-1", None).unwrap());
        on_revoked(dm, &state.screenshare, &ev);
        assert!(!state.screenshare.exists(&room));
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
