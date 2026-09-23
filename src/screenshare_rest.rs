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

use axum::{extract::{Path, State}, http::{header, HeaderMap, StatusCode}, Json};
use base64::Engine;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{json, Value};

use crate::{api::AppState, error::{Result, ServerError}, notifications::WsMessage};

/// A room with no keepalive for this long is dropped (matches rmfakecloud's 60s).
const ROOM_TIMEOUT: Duration = Duration::from_secs(60);
/// The JSON field carrying the signalling body in broadcast/direct posts.
const BODY_KEY: &str = "payload";

#[derive(Clone, Serialize)]
pub struct RoomClient {
    #[serde(rename = "clientId")] pub client_id: String,
    #[serde(rename = "userId")] pub user_id: String,
    #[serde(rename = "isOwner")] pub is_owner: bool,
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
    pub fn new() -> Self { Self::default() }

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
        participants.insert(device_id.to_string(), RoomClient {
            client_id: device_id.to_string(), user_id: user_id.to_string(), is_owner: true,
        });
        rooms.insert(room_id.clone(), Room {
            created_at, last_activity: Instant::now(), owner_user_id: user_id.to_string(), participants,
        });
        (room_id, created_at.to_rfc3339())
    }

    /// Newest live room owned by `user_id`, if any.
    fn find_active(&self, user_id: &str) -> Option<String> {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        rooms.iter()
            .filter(|(_, r)| r.owner_user_id == user_id)
            .max_by_key(|(_, r)| r.created_at)
            .map(|(id, _)| id.clone())
    }

    fn add_participant(&self, room_id: &str, client_id: &str, user_id: &str) {
        if let Some(r) = self.rooms.lock().get_mut(room_id) {
            r.participants.entry(client_id.to_string()).or_insert(RoomClient {
                client_id: client_id.to_string(), user_id: user_id.to_string(), is_owner: false,
            });
        }
    }

    fn clients(&self, room_id: &str) -> Vec<RoomClient> {
        self.rooms.lock().get(room_id).map(|r| r.participants.values().cloned().collect()).unwrap_or_default()
    }

    fn keepalive(&self, room_id: &str) {
        if let Some(r) = self.rooms.lock().get_mut(room_id) { r.last_activity = Instant::now(); }
    }

    fn delete(&self, room_id: &str) { self.rooms.lock().remove(room_id); }

    fn exists(&self, room_id: &str) -> bool {
        let mut rooms = self.rooms.lock();
        Self::sweep(&mut rooms);
        rooms.contains_key(room_id)
    }

    fn room_meta(&self, room_id: &str) -> Option<String> {
        self.rooms.lock().get(room_id).map(|r| r.created_at.to_rfc3339())
    }
}

fn authz(headers: &HeaderMap) -> Result<&str> {
    headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).ok_or(ServerError::Unauthorized)
}

/// `POST /screenshare/v1/rooms` -> 201 `{roomId, createdAt, iceServers}`.
pub async fn create_room(State(state): State<AppState>, headers: HeaderMap) -> Result<(StatusCode, Json<Value>)> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    let (room_id, created_at) = state.screenshare.create(&user_id, &device_id);
    // Tell the user's other clients to join. sourceDeviceID = creator, so the creator drops it.
    let _ = state.notification_tx.send(WsMessage::screenshare_room_created(&user_id, &device_id, &room_id));
    Ok((StatusCode::CREATED, Json(json!({
        "roomId": room_id, "createdAt": created_at, "iceServers": &*state.ice_servers,
    }))))
}

/// `POST /screenshare/v1/rooms/join-active` -> 200 `{roomId, clients, iceServers}` or 404.
pub async fn join_active(State(state): State<AppState>, headers: HeaderMap) -> Result<(StatusCode, Json<Value>)> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    let Some(room_id) = state.screenshare.find_active(&user_id) else {
        return Ok((StatusCode::NOT_FOUND, Json(json!({"error": "no active room"}))));
    };
    state.screenshare.add_participant(&room_id, &device_id, &user_id);
    Ok((StatusCode::OK, Json(json!({
        "roomId": room_id, "clients": state.screenshare.clients(&room_id), "iceServers": &*state.ice_servers,
    }))))
}

/// `GET /screenshare/v1/rooms/{roomId}` -> 200 `{roomId, createdAt, clients}` or 404 (viewer use).
pub async fn get_room(State(state): State<AppState>, headers: HeaderMap, Path(room_id): Path<String>) -> Result<(StatusCode, Json<Value>)> {
    state.auth_user(&headers)?;
    match state.screenshare.room_meta(&room_id) {
        Some(created_at) => Ok((StatusCode::OK, Json(json!({
            "roomId": room_id, "createdAt": created_at, "clients": state.screenshare.clients(&room_id),
        })))),
        None => Ok((StatusCode::NOT_FOUND, Json(json!({"error": "room not found"})))),
    }
}

/// `POST /screenshare/v1/rooms/{roomId}/keepalive` -> 200.
pub async fn keepalive(State(state): State<AppState>, headers: HeaderMap, Path(room_id): Path<String>) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    state.screenshare.keepalive(&room_id);
    Ok(StatusCode::OK)
}

/// `DELETE /screenshare/v1/rooms/{roomId}` -> 204.
pub async fn delete_room(State(state): State<AppState>, headers: HeaderMap, Path(room_id): Path<String>) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    state.screenshare.delete(&room_id);
    Ok(StatusCode::NO_CONTENT)
}

/// Pull the signalling body out of the posted object: senders wrap it as `{ "<BODY_KEY>": <obj> }`.
fn inner_body(body: &Value) -> Vec<u8> {
    let inner = body.get(BODY_KEY).unwrap_or(body);
    serde_json::to_vec(inner).unwrap_or_default()
}

/// `POST /screenshare/v1/rooms/{roomId}/messages/broadcast` -> 200. Relays to all the user's clients.
pub async fn broadcast(State(state): State<AppState>, headers: HeaderMap, Path(room_id): Path<String>, Json(body): Json<Value>) -> Result<StatusCode> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    if !state.screenshare.exists(&room_id) { return Err(ServerError::NotFound("room not found".into())); }
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(inner_body(&body));
    let _ = state.notification_tx.send(WsMessage::screenshare_message(&user_id, &device_id, &room_id, None, &data_b64));
    Ok(StatusCode::OK)
}

/// `POST /screenshare/v1/rooms/{roomId}/messages/direct` -> 200. Relays to one target client.
pub async fn direct(State(state): State<AppState>, headers: HeaderMap, Path(room_id): Path<String>, Json(body): Json<Value>) -> Result<StatusCode> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    if !state.screenshare.exists(&room_id) { return Err(ServerError::NotFound("room not found".into())); }
    let target = body.get("targetClientId").and_then(|v| v.as_str());
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(inner_body(&body));
    let _ = state.notification_tx.send(WsMessage::screenshare_message(&user_id, &device_id, &room_id, target, &data_b64));
    Ok(StatusCode::OK)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{device::DeviceManager, storage::Storage};
    use axum::http::HeaderValue;

    fn state_and_auth() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices = DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        let state = AppState::new(storage, devices)
            .with_ice_servers(serde_json::json!([{"url": "stun:localhost:3478"}]));
        (state, tk, tmp)
    }

    fn hdrs(tk: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {tk}")).unwrap());
        h
    }

    fn wrap(target: Option<&str>, inner: Value) -> Value {
        let mut m = serde_json::Map::new();
        if let Some(t) = target { m.insert("targetClientId".into(), Value::String(t.into())); }
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
        assert_eq!(ev.message.attributes.room_id.as_deref(), Some(room_id.as_str()));

        // join-active finds the live room
        let (code, Json(body)) = join_active(State(state.clone()), hdrs(&tk)).await.unwrap();
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["roomId"].as_str().unwrap(), room_id);

        // broadcast relays base64 of the inner body only
        let inner = serde_json::json!({"type": "request-offer"});
        let code = broadcast(State(state.clone()), hdrs(&tk), Path(room_id.clone()), Json(wrap(None, inner.clone()))).await.unwrap();
        assert_eq!(code, StatusCode::OK);
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.message.attributes.event, "ScreenshareMessage");
        let decoded = base64::engine::general_purpose::STANDARD.decode(ev.message.data.unwrap()).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&decoded).unwrap(), inner);

        // direct carries targetClientId
        let _ = direct(State(state.clone()), hdrs(&tk), Path(room_id.clone()), Json(wrap(Some("peer-1"), serde_json::json!({"type": "webtrc"})))).await.unwrap();
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.message.attributes.target_client_id.as_deref(), Some("peer-1"));

        // delete -> 204, then join-active is 404
        assert_eq!(delete_room(State(state.clone()), hdrs(&tk), Path(room_id.clone())).await.unwrap(), StatusCode::NO_CONTENT);
        let (code, _) = join_active(State(state.clone()), hdrs(&tk)).await.unwrap();
        assert_eq!(code, StatusCode::NOT_FOUND);

        // relay into a missing room errors; missing auth is rejected
        assert!(broadcast(State(state.clone()), hdrs(&tk), Path("nope".into()), Json(serde_json::json!({}))).await.is_err());
        assert!(create_room(State(state.clone()), HeaderMap::new()).await.is_err());
    }
}
