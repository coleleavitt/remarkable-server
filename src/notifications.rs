//! WebSocket Notifications Handler
//!
//! Implements the /notifications/ws/json/1 endpoint that devices connect to
//! for real-time sync notifications.
//!
//! Message format (from rmfakecloud):
//! ```json
//! {
//!   "message": {
//!     "attributes": {
//!       "auth0UserID": "...",
//!       "event": "SyncComplete",
//!       "sourceDeviceID": "...",
//!       "sourceDeviceDesc": "..."
//!     },
//!     "messageid": "...",
//!     "publishTime": "...",
//!     "publish_time": "..."
//!   },
//!   "subscription": "..."
//! }
//! ```

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::api::AppState;

/// Notification message wrapper (rmfakecloud format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub message: NotificationMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription: Option<String>,
}

/// Notification message (inner)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationMessage {
    pub attributes: NotificationAttributes,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    #[serde(rename = "message_id", skip_serializing_if = "Option::is_none")]
    pub message_id_2: Option<String>,

    #[serde(rename = "messageid", skip_serializing_if = "Option::is_none")]
    pub message_id_3: Option<String>,

    #[serde(rename = "publishTime", skip_serializing_if = "Option::is_none")]
    pub publish_time: Option<String>,

    #[serde(rename = "publish_time", skip_serializing_if = "Option::is_none")]
    pub publish_time_2: Option<String>,
}

/// Notification attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationAttributes {
    #[serde(rename = "auth0UserID")]
    pub auth0_user_id: String,

    pub event: String,

    #[serde(rename = "sourceDeviceID")]
    pub source_device_id: String,

    #[serde(rename = "deviceID", skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,

    #[serde(rename = "deviceName", skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,

    #[serde(rename = "sourceDeviceDesc", skip_serializing_if = "Option::is_none")]
    pub source_device_desc: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,

    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub doc_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    #[serde(rename = "vissibleName", skip_serializing_if = "Option::is_none")]
    pub visible_name: Option<String>,

    /// Screenshare room id (`ScreenshareRoomCreated`).
    #[serde(rename = "roomId", skip_serializing_if = "Option::is_none")]
    pub room_id: Option<String>,

    /// Screenshare direct-message target client id.
    #[serde(rename = "targetClientId", skip_serializing_if = "Option::is_none")]
    pub target_client_id: Option<String>,
}

impl WsMessage {
    /// Any event, attributed to `source_device_id` (the tablet drops notifications whose
    /// `sourceDeviceID` is its own id, so events it caused should carry its id).
    pub fn event(event: &str, source_device_id: &str, auth0_user_id: &str) -> Self {
        let mut msg = Self::sync_complete(0, source_device_id, auth0_user_id);
        msg.message.attributes.event = event.into();
        msg
    }

    /// Screenshare: a room was created; other clients should join. `source_device_id`
    /// is the creator's device, so the creator's own client drops it.
    pub fn screenshare_room_created(
        auth0_user_id: &str,
        source_device_id: &str,
        room_id: &str,
    ) -> Self {
        let mut msg = Self::event("ScreenshareRoomCreated", source_device_id, auth0_user_id);
        msg.message.attributes.room_id = Some(room_id.into());
        msg
    }

    /// Screenshare: relay a signalling message to the user's other clients. `data_b64`
    /// is base64 of the inner JSON object the sender posted.
    pub fn screenshare_message(
        auth0_user_id: &str,
        source_device_id: &str,
        room_id: &str,
        target_client_id: Option<&str>,
        data_b64: &str,
    ) -> Self {
        let mut msg = Self::event("ScreenshareMessage", source_device_id, auth0_user_id);
        msg.message.attributes.room_id = Some(room_id.into());
        msg.message.attributes.target_client_id = target_client_id.map(|s| s.to_string());
        msg.message.data = Some(data_b64.to_string());
        msg
    }

    /// Tell the device its passcode reset request was denied (xochitl 3.28 `PasscodeResetDenied`).
    pub fn passcode_reset_denied(auth0_user_id: &str, request_id: &str) -> Self {
        let mut msg = Self::event("PasscodeResetDenied", "local-server", auth0_user_id);
        msg.message.attributes.id = Some(request_id.into());
        msg
    }

    /// Tell the device its passcode reset request was approved (rmfakecloud `NotifyPasscodeReset`).
    pub fn passcode_reset_approved(
        auth0_user_id: &str,
        device_id: &str,
        device_name: &str,
        request_id: &str,
    ) -> Self {
        use base64::Engine;
        let mut msg = Self::sync_complete(0, "local-server", auth0_user_id);
        let attrs = &mut msg.message.attributes;
        attrs.event = "PasscodeResetApproved".into();
        attrs.device_id = Some(device_id.into());
        attrs.device_name = Some(device_name.into());
        attrs.id = Some(request_id.into());
        attrs.version = Some("1".into());
        msg.message.data =
            Some(base64::engine::general_purpose::STANDARD.encode("PasscodeResetApproved"));
        msg
    }

    pub fn sync_complete(generation: u64, source_device_id: &str, auth0_user_id: &str) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        let publish_time = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);

        Self {
            message: NotificationMessage {
                attributes: NotificationAttributes {
                    auth0_user_id: auth0_user_id.to_string(),
                    event: "SyncComplete".to_string(),
                    source_device_id: source_device_id.to_string(),
                    source_device_desc: Some("local-server".to_string()),
                    device_id: None,
                    device_name: None,
                    room_id: None,
                    target_client_id: None,
                    id: None,
                    parent: None,
                    doc_type: None,
                    version: None,
                    visible_name: None,
                },
                data: None,
                message_id: Some(uuid::Uuid::new_v4().to_string()),
                message_id_2: Some(uuid::Uuid::new_v4().to_string()),
                message_id_3: Some(timestamp),
                publish_time: Some(publish_time.clone()),
                publish_time_2: Some(publish_time),
            },
            subscription: Some("dummy-subscription".to_string()),
        }
    }
}

/// Client message types received from devices
#[derive(Debug, Deserialize)]
pub struct ClientMessage {
    #[serde(rename = "messageType", default)]
    pub message_type: Option<String>,

    #[serde(default)]
    pub attributes: Option<serde_json::Value>,
}

/// WebSocket upgrade handler for notifications endpoint
pub async fn notifications_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> crate::error::Result<impl IntoResponse> {
    // Same as the cloud: only authenticated devices/clients may subscribe.
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(crate::error::ServerError::Unauthorized)?;
    let identity = state.devices.session_identity(auth)?;
    // Closes the socket once the device it authenticated as is revoked.
    let revoked = state.devices.session_revoked(&identity);
    info!("WebSocket upgrade request for notifications");
    Ok(ws.on_upgrade(move |socket| {
        handle_notifications_socket(
            socket,
            state,
            identity.user_id,
            // For filtering direct screen share messages.
            Some(identity.device_id),
            revoked,
        )
    }))
}

/// Handle an individual WebSocket connection
/// Whether a notification belongs on the socket of `device_id` / `user_id`.
/// Screen share events are per account, and a direct screen share message
/// only goes to its `targetClientId`; everything else goes everywhere.
fn delivers_to(msg: &WsMessage, user_id: &str, device_id: Option<&str>) -> bool {
    let a = &msg.message.attributes;
    if !a.event.starts_with("Screenshare") {
        return true;
    }
    let for_device = match (a.target_client_id.as_deref(), device_id) {
        (Some(target), Some(device)) => target == device,
        _ => true,
    };
    a.auth0_user_id == user_id && for_device
}

async fn handle_notifications_socket(
    socket: WebSocket,
    state: AppState,
    user_id: String,
    device_id: Option<String>,
    revoked: impl std::future::Future<Output = ()> + Send + 'static,
) {
    let session_id = uuid::Uuid::new_v4().to_string();
    info!(session_id = %session_id, "New notifications WebSocket connection");

    let (mut sender, mut receiver) = socket.split();

    // Send initial SyncComplete notification immediately to trigger sync
    let initial_notif = WsMessage::sync_complete(
        state.storage.get_root().generation,
        "local-server",
        "local-user",
    );
    if let Ok(json) = serde_json::to_string(&initial_notif) {
        info!(session_id = %session_id, "Sending initial SyncComplete notification");
        let _ = sender.send(Message::Text(json.into())).await;
    }

    // Subscribe to broadcast channel for sync notifications
    let mut rx = state.notification_tx.subscribe();

    // Spawn task to forward broadcasts to this client. It owns the sending half, so it is also
    // what closes the socket when the device is revoked; it returns true in that case.
    let session_id_clone = session_id.clone();
    let storage = state.storage.clone();
    let mut forward_task = tokio::spawn(async move {
        tokio::pin!(revoked);
        loop {
            let notif = tokio::select! {
                notif = rx.recv() => notif,
                () = &mut revoked => {
                    info!(session_id = %session_id_clone, "device revoked, closing notifications WebSocket");
                    let _ = sender
                        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                            code: axum::extract::ws::close_code::POLICY,
                            reason: "device revoked".into(),
                        })))
                        .await;
                    return true;
                }
            };
            let msg = match notif {
                Ok(msg) => msg,
                // Missing a few events beats ending notifications for this client.
                // A skipped SyncComplete would leave it out of date, so send a
                // fresh one in place of whatever was lost.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(session_id = %session_id_clone, "notification client lagged, skipped {n} events");
                    WsMessage::sync_complete(
                        storage.get_root().generation,
                        "local-server",
                        "local-user",
                    )
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            };
            if !delivers_to(&msg, &user_id, device_id.as_deref()) {
                continue;
            }
            if let Ok(json) = serde_json::to_string(&msg) {
                debug!(session_id = %session_id_clone, "Sending notification: {}", json);
                if sender.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
        }
        debug!(session_id = %session_id_clone, "Forward task ended");
        false
    });

    // Handle incoming messages until the client leaves or its device is revoked.
    let mut forwarding = true;
    loop {
        let result = tokio::select! {
            incoming = receiver.next() => match incoming { Some(r) => r, None => break },
            ended = &mut forward_task, if forwarding => {
                forwarding = false;
                if matches!(ended, Ok(true)) {
                    break; // revoked: the Close frame is out, stop serving this client
                }
                continue;
            }
        };
        match result {
            Ok(Message::Text(text)) => {
                debug!(session_id = %session_id, "Received text: {}", text);

                if let Ok(msg) = serde_json::from_str::<ClientMessage>(&text) {
                    if let Some(msg_type) = &msg.message_type {
                        match msg_type.as_str() {
                            "sync-request" => {
                                info!("Client requested sync");
                                // Send sync complete notification
                                let notif = WsMessage::sync_complete(
                                    state.storage.get_root().generation,
                                    "local-server",
                                    "local-user",
                                );
                                let _ = state.notification_tx.send(notif);
                            }
                            "pong" => {
                                debug!("Received pong");
                            }
                            other => {
                                debug!("Unknown message type: {}", other);
                            }
                        }
                    }
                }
            }
            Ok(Message::Ping(_data)) => {
                debug!(session_id = %session_id, "Received ping");
                // Axum handles pong automatically
            }
            Ok(Message::Pong(_)) => {
                debug!(session_id = %session_id, "Received pong");
            }
            Ok(Message::Close(_)) => {
                info!(session_id = %session_id, "Client closed connection");
                break;
            }
            Ok(Message::Binary(data)) => {
                debug!(session_id = %session_id, "Received binary: {} bytes", data.len());
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("close_notify") {
                    debug!(session_id = %session_id, "WebSocket closed without TLS close_notify");
                } else {
                    warn!(session_id = %session_id, "WebSocket error: {}", e);
                }
                break;
            }
        }
    }

    forward_task.abort();
    info!(session_id = %session_id, "WebSocket connection closed");
}

#[cfg(test)]
mod delivery_tests {
    use super::*;

    #[test]
    fn screenshare_messages_reach_only_their_target() {
        let direct = WsMessage::screenshare_message("u", "tablet", "r", Some("viewer-a"), "e30=");
        assert!(delivers_to(&direct, "u", Some("viewer-a")));
        assert!(!delivers_to(&direct, "u", Some("viewer-b")));
        assert!(!delivers_to(&direct, "other-user", Some("viewer-a")));
        assert!(
            delivers_to(&direct, "u", None),
            "unknown device keeps the old behaviour"
        );
        let broadcast = WsMessage::screenshare_message("u", "viewer-a", "r", None, "e30=");
        assert!(delivers_to(&broadcast, "u", Some("tablet")));
        let sync = WsMessage::sync_complete(1, "local-server", "local-user");
        assert!(delivers_to(&sync, "someone-else", Some("x")));
    }
}
