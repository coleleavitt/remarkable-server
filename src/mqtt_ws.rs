//! MQTT over WebSocket Handler
//!
//! Implements MQTT 3.1.1 over WebSocket as an alternative framing of the
//! `/notifications/ws/json/1` sync push.
//!
//! **Off by default.** It is served at `/mqtt` only when [`ENABLE_ENV`]
//! (`MQTT_WS_NOTIFICATIONS`) is `1`, `true` or `on` (see [`router_if_enabled`]),
//! because the real tablet does not use it. Checked on the production server
//! (GAP_ANALYSIS.md, "MQTT: what the tablet actually uses"): xochitl 3.3.2 opened
//! `/notifications/ws/json/1` for every notification session and, in 15 days of
//! nginx logs, never requested any path containing `mqtt`, including after this
//! route went live. Its observed MQTT traffic is screen share signalling, raw MQTT
//! over TLS to the `SCREENSHARE_BIND` broker (`crate::screenshare`), not WebSocket.
//! Whether it also subscribes to sync topics on that broker is unconfirmed (accepted
//! SUBSCRIBE filters are logged at debug there); if it does, sync pushes belong on
//! that broker, not on this endpoint.
//!
//! Path and topic are unverified guesses kept for other clients: nothing verified
//! pins a path (`mqttbroker` in discovery is a bare host; remarkable-rs only
//! *expects* `wss://vernemq-.../mqtt`, on the broker host, not this API host), so
//! this uses the conventional MQTT-over-WebSocket path (VerneMQ's and Paho's
//! default), and publishes on each concrete topic the client subscribed to. A
//! wildcard filter has no concrete topic, so its SUBACK says so ([`SUBACK_FAILURE`]).

use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use axum::routing::get;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

use crate::api::AppState;
use crate::device::SessionIdentity;
use crate::error::ServerError;
use crate::notifications::WsMessage;

/// Environment variable that serves [`PATH`] when set to `1`, `true` or `on`.
pub const ENABLE_ENV: &str = "MQTT_WS_NOTIFICATIONS";

/// Where the endpoint is served when enabled.
pub const PATH: &str = "/mqtt";

/// How long a client has, from the WebSocket upgrade, to send its CONNECT. An upgrade
/// without an `Authorization` header is only authenticated at CONNECT, so without this
/// such a socket could stay open indefinitely (MQTT 3.1.1 §3.1.4: a server SHOULD close
/// a connection that sends no CONNECT within a reasonable time).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// SUBACK return code for a filter the server refuses (MQTT 3.1.1 §3.9.3).
const SUBACK_FAILURE: u8 = 0x80;

/// The [`PATH`] route, to merge into [`crate::create_router`]'s router, when `flag`
/// (the value of [`ENABLE_ENV`]) is `1`, `true` or `on`; `None` otherwise, including
/// when it is unset. No tablet uses it (see the module docs), so it stays off the
/// public attack surface unless asked for.
pub fn router_if_enabled(state: AppState, flag: Option<&str>) -> Option<Router> {
    matches!(flag, Some("1" | "true" | "on")).then(|| {
        Router::new()
            .route(PATH, get(mqtt_notifications_ws))
            .with_state(state)
            .layer(TraceLayer::new_for_http())
    })
}

/// An accepted CONNECT: the session's user, and a future that resolves when the device it
/// authenticated as is revoked (the session is then closed).
struct SessionAuth {
    user_id: String,
    revoked: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}

/// MQTT packet types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
}

impl TryFrom<u8> for PacketType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::Connack),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::Puback),
            5 => Ok(PacketType::Pubrec),
            6 => Ok(PacketType::Pubrel),
            7 => Ok(PacketType::Pubcomp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::Suback),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::Unsuback),
            12 => Ok(PacketType::Pingreq),
            13 => Ok(PacketType::Pingresp),
            14 => Ok(PacketType::Disconnect),
            _ => Err(()),
        }
    }
}

/// Parse MQTT remaining length (variable length encoding)
fn parse_remaining_length(data: &[u8]) -> Option<(usize, usize)> {
    let mut multiplier = 1;
    let mut value = 0usize;
    let mut idx = 0;

    loop {
        if idx >= data.len() {
            return None;
        }
        let byte = data[idx];
        value += (byte as usize & 0x7F) * multiplier;
        multiplier *= 128;
        idx += 1;

        if byte & 0x80 == 0 {
            break;
        }
        if multiplier > 128 * 128 * 128 {
            return None; // Malformed
        }
    }

    Some((value, idx))
}

/// The variable header and payload of the MQTT packet in `frame` (everything after the
/// fixed header). `None` when its remaining length is malformed or does not end exactly
/// at the end of the frame: this endpoint reads one whole packet per frame.
fn packet_body(frame: &[u8]) -> Option<&[u8]> {
    let (len, n) = parse_remaining_length(frame.get(1..)?)?;
    (frame.len() - 1 - n == len).then(|| &frame[1 + n..])
}

/// Append `len` in MQTT's variable-length remaining-length encoding.
fn push_remaining_length(packet: &mut Vec<u8>, mut len: usize) {
    loop {
        let byte = (len % 128) as u8;
        len /= 128;
        packet.push(if len > 0 { byte | 0x80 } else { byte });
        if len == 0 {
            break;
        }
    }
}

/// Parse MQTT binary data (2-byte length prefix)
fn parse_mqtt_bytes(data: &[u8]) -> Option<(&[u8], usize)> {
    if data.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + len {
        return None;
    }
    Some((&data[2..2 + len], 2 + len))
}

/// Parse MQTT string (2-byte length prefix + UTF-8)
fn parse_mqtt_string(data: &[u8]) -> Option<(&str, usize)> {
    let (b, n) = parse_mqtt_bytes(data)?;
    Some((std::str::from_utf8(b).ok()?, n))
}

/// Why a CONNECT is refused before its credentials are looked at.
#[derive(Debug, PartialEq)]
enum ConnectError {
    /// Not a conforming CONNECT (MQTT 3.1.1 §3.1): the connection is closed without a
    /// CONNACK [MQTT-3.1.4-1].
    Malformed,
    /// A protocol level other than MQTT 3.1.1 (or 3.1 as `MQIsdp`): CONNACK 0x01, then
    /// close [MQTT-3.1.2-2].
    UnsupportedLevel,
}

/// The token a CONNECT (variable header + payload) carries: its password, or its
/// username when the password is empty or absent, as the screenshare broker reads it.
/// `Ok(None)` when it has no credentials, or only non-UTF-8 ones. Every field its flags
/// declare must be there and nothing may follow them, so a malformed CONNECT is refused
/// even on an upgrade already authenticated by its `Authorization` header.
fn connect_token(body: &[u8]) -> Result<Option<String>, ConnectError> {
    use ConnectError::Malformed;
    let (protocol, mut at) = parse_mqtt_string(body).ok_or(Malformed)?;
    let &[level, flags, _, _] = body.get(at..at + 4).ok_or(Malformed)? else {
        return Err(Malformed); // level, flags, keep alive
    };
    at += 4;
    match (protocol, level) {
        ("MQTT", 4) | ("MQIsdp", 3) => {}
        ("MQTT" | "MQIsdp", _) => return Err(ConnectError::UnsupportedLevel),
        _ => return Err(Malformed), // [MQTT-3.1.2-1]
    }
    // Reserved bit [MQTT-3.1.2-3]; will QoS/retain without a will [MQTT-3.1.2-11/-13/-15];
    // a password without a username [MQTT-3.1.2-22].
    let (will, username_flag, password_flag) =
        (flags & 0x04 != 0, flags & 0x80 != 0, flags & 0x40 != 0);
    if flags & 0x01 != 0 || (!will && flags & 0x38 != 0) || (password_flag && !username_flag) {
        return Err(Malformed);
    }
    let mut field = |string: bool| -> Result<&[u8], ConnectError> {
        let (b, n) = parse_mqtt_bytes(body.get(at..).ok_or(Malformed)?).ok_or(Malformed)?;
        if string && std::str::from_utf8(b).is_err() {
            return Err(Malformed);
        }
        at += n;
        Ok(b)
    };
    field(true)?; // client id
    if will {
        field(true)?; // will topic
        field(false)?; // will message
    }
    let username = if username_flag {
        Some(field(true)?)
    } else {
        None
    };
    let password = if password_flag {
        Some(field(false)?)
    } else {
        None
    };
    if at != body.len() {
        return Err(Malformed);
    }
    let token = password.filter(|p| !p.is_empty()).or(username);
    Ok(token.and_then(|t| String::from_utf8(t.to_vec()).ok()))
}

/// Build CONNACK packet
fn build_connack(session_present: bool, return_code: u8) -> Vec<u8> {
    vec![
        0x20,                                      // CONNACK packet type
        0x02,                                      // Remaining length
        if session_present { 0x01 } else { 0x00 }, // Session present flag
        return_code,                               // Return code (0 = accepted)
    ]
}

/// Build SUBACK packet
fn build_suback(packet_id: u16, qos_levels: &[u8]) -> Vec<u8> {
    let mut packet = vec![0x90]; // SUBACK packet type
    push_remaining_length(&mut packet, 2 + qos_levels.len());
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(qos_levels);
    packet
}

/// Build PINGRESP packet
fn build_pingresp() -> Vec<u8> {
    vec![0xD0, 0x00] // PINGRESP with 0 remaining length
}

/// Build PUBLISH packet
fn build_publish(topic: &str, payload: &[u8], qos: u8, packet_id: Option<u16>) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let topic_len = topic_bytes.len();

    // Calculate remaining length
    let mut remaining_len = 2 + topic_len + payload.len();
    if qos > 0 {
        remaining_len += 2; // Packet ID
    }

    let mut packet = vec![
        0x30 | (qos << 1), // PUBLISH with QoS
    ];
    push_remaining_length(&mut packet, remaining_len);

    // Topic length + topic
    packet.push((topic_len >> 8) as u8);
    packet.push(topic_len as u8);
    packet.extend_from_slice(topic_bytes);

    // Packet ID (if QoS > 0)
    if let Some(id) = packet_id {
        packet.push((id >> 8) as u8);
        packet.push(id as u8);
    }

    // Payload
    packet.extend_from_slice(payload);

    packet
}

/// WebSocket upgrade handler for MQTT notifications endpoint (`/mqtt`, only routed
/// when enabled; see [`router_if_enabled`]).
///
/// Authenticated with the same tokens as `/notifications/ws/json/1`: a bearer
/// `Authorization` header on the upgrade (an invalid one is rejected with 401),
/// or, when there is no such header, the token as the MQTT CONNECT password (or
/// username), as the screenshare broker accepts. A CONNECT without a valid token
/// gets CONNACK "not authorized" and the socket is closed. The first packet must be
/// a well-formed CONNECT, sent within [`CONNECT_TIMEOUT`]: a malformed or truncated
/// CONNECT (checked field by field before any token, header or CONNECT, is looked at),
/// any other packet first, or none in time closes the socket without a CONNACK (MQTT
/// 3.1.1 §3.1.0, §3.1.4); a protocol level other than 3.1.1 or 3.1 gets CONNACK 0x01.
/// Each binary frame is read as one whole packet, as Paho and mqtt.js send them;
/// packets split across frames or sharing one (which §6.0 allows) are not handled, so
/// such a CONNECT counts as malformed. A connected
/// session is closed once the device its token belongs to is revoked (deleted or
/// re-paired).
pub async fn mqtt_notifications_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> crate::error::Result<impl IntoResponse> {
    let header_identity = match headers.get(AUTHORIZATION) {
        Some(v) => Some(
            state
                .devices
                .session_identity(v.to_str().map_err(|_| ServerError::Unauthorized)?)?,
        ),
        None => None,
    };
    info!("MQTT WebSocket upgrade request for notifications");
    Ok(ws
        .protocols(["mqtt"])
        .on_upgrade(move |socket| handle_mqtt_socket(socket, state, header_identity)))
}

/// Handle an individual MQTT WebSocket connection
async fn handle_mqtt_socket(
    socket: WebSocket,
    state: AppState,
    header_identity: Option<SessionIdentity>,
) {
    let (sender, receiver) = socket.split();
    // Subscribe to broadcast channel for sync notifications
    let rx = state.notification_tx.subscribe();
    let storage = state.storage.clone();
    let devices = state.devices.clone();
    let authenticate = move |token: Option<&str>| {
        let identity = match &header_identity {
            Some(i) => i.clone(),
            None => devices
                .session_identity(&format!("Bearer {}", token?))
                .ok()?,
        };
        // Built at CONNECT: its first registration check also covers a revocation that
        // landed between the upgrade and the CONNECT.
        Some(SessionAuth {
            revoked: Box::pin(devices.session_revoked(&identity)),
            user_id: identity.user_id,
        })
    };
    run_mqtt_session(
        sender,
        receiver,
        rx,
        move || storage.get_root().generation,
        authenticate,
    )
    .await;
}

/// `auth0UserID` of server-originated events not tied to an account (feed EPUBs
/// landing in the single shared sync tree); every authenticated user gets those.
const SERVER_USER: &str = "local-user";

/// Whether notifications can go out on `filter`: a non-empty topic without `+`/`#`.
/// A wildcard filter names no single topic to publish on, and nothing verified pins
/// the topic a sync push would use to match it against; such filters are refused in
/// the SUBACK ([`SUBACK_FAILURE`]) instead of being acknowledged and never served.
fn is_concrete(filter: &str) -> bool {
    !filter.is_empty() && !filter.contains(['+', '#'])
}

/// MQTT PUBLISH packets (QoS 0) carrying `msg` for the session of `user_id`.
///
/// Assumption: nothing pins a topic for sync pushes over MQTT-over-WebSocket
/// (xochitl does not use this endpoint), so the notification goes out on each
/// concrete topic the client SUBSCRIBEd to ([`is_concrete`]; the session only
/// keeps those, and others are skipped here too). The payload is the same
/// `WsMessage` JSON the `/notifications/ws/json/1` endpoint sends. Only events
/// for `user_id` (or [`SERVER_USER`]) are forwarded; screenshare events have
/// their own broker and are never forwarded here.
fn notification_publishes(
    msg: &WsMessage,
    subscriptions: &[String],
    user_id: &str,
) -> Vec<Vec<u8>> {
    let a = &msg.message.attributes;
    if a.event.starts_with("Screenshare")
        || (a.auth0_user_id != user_id && a.auth0_user_id != SERVER_USER)
    {
        return Vec::new();
    }
    let Ok(payload) = serde_json::to_vec(msg) else {
        return Vec::new();
    };
    let mut seen: Vec<&str> = Vec::new();
    subscriptions
        .iter()
        .filter(|t| is_concrete(t))
        .filter(|t| {
            if seen.contains(&t.as_str()) {
                false
            } else {
                seen.push(t);
                true
            }
        })
        .map(|t| build_publish(t, &payload, 0, None))
        .collect()
}

/// The MQTT session loop: answers the client's packets and pushes broadcast
/// notifications to it once it has CONNECTed. `generation` supplies the current
/// root generation for the catch-up SyncComplete sent after a lagged receiver.
/// `authenticate` maps the CONNECT's token (if any) to the session's user id and
/// revocation signal; `None` refuses the CONNECT and ends the session.
async fn run_mqtt_session<S, R>(
    mut sender: S,
    mut receiver: R,
    mut rx: broadcast::Receiver<WsMessage>,
    generation: impl Fn() -> u64,
    authenticate: impl Fn(Option<&str>) -> Option<SessionAuth>,
) where
    S: Sink<Message> + Unpin,
    R: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let session_id = uuid::Uuid::new_v4().to_string();
    info!(session_id = %session_id, "New MQTT WebSocket connection");

    let mut connected = false;
    let mut user_id = String::new();
    // Pending until CONNECT authenticates the session.
    let mut revoked: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(std::future::pending());
    let mut subscriptions: Vec<String> = Vec::new();
    let mut notifications_open = true;
    let connect_deadline = tokio::time::sleep(CONNECT_TIMEOUT);
    tokio::pin!(connect_deadline);

    loop {
        let result = tokio::select! {
            incoming = receiver.next() => match incoming { Some(r) => r, None => break },
            () = &mut connect_deadline, if !connected => {
                warn!(session_id = %session_id, "no MQTT CONNECT within {CONNECT_TIMEOUT:?}, closing");
                let _ = sender.send(Message::Close(None)).await;
                break;
            }
            () = &mut revoked => {
                info!(session_id = %session_id, "device revoked, closing MQTT session");
                // MQTT 3.1.1 has no server DISCONNECT; closing the connection is how a broker ends it.
                let _ = sender.send(Message::Close(None)).await;
                break;
            }
            notif = rx.recv(), if notifications_open => {
                let msg = match notif {
                    Ok(msg) => msg,
                    // A skipped SyncComplete would leave the client stale; send a fresh one instead.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(session_id = %session_id, "MQTT notification client lagged, skipped {n} events");
                        WsMessage::sync_complete(generation(), "local-server", &user_id)
                    }
                    Err(broadcast::error::RecvError::Closed) => { notifications_open = false; continue }
                };
                if !connected { continue }
                for packet in notification_publishes(&msg, &subscriptions, &user_id) {
                    if sender.send(Message::Binary(packet.into())).await.is_err() {
                        info!(session_id = %session_id, "MQTT WebSocket connection closed");
                        return;
                    }
                }
                continue;
            }
        };
        match result {
            Ok(Message::Binary(data)) => {
                debug!(session_id = %session_id, "Received MQTT binary: {} bytes", data.len());

                let packet_type_byte = data.first().map(|b| b >> 4);
                let packet_type = packet_type_byte.and_then(|t| PacketType::try_from(t).ok());
                if !connected && packet_type != Some(PacketType::Connect) {
                    // [MQTT-3.1.0-1]: the client's first packet MUST be CONNECT.
                    warn!(session_id = %session_id, "first MQTT packet is not CONNECT, closing");
                    break;
                }
                let Some(packet_type_byte) = packet_type_byte else {
                    continue;
                };
                let Some(packet_type) = packet_type else {
                    warn!(session_id = %session_id, "Unknown packet type: {}", packet_type_byte);
                    continue;
                };

                debug!(session_id = %session_id, "MQTT packet type: {:?}", packet_type);

                match packet_type {
                    PacketType::Connect => {
                        if connected {
                            warn!(session_id = %session_id, "second MQTT CONNECT, closing");
                            break;
                        }
                        // Validated before authentication, which a header token would
                        // otherwise pass whatever the CONNECT says.
                        let token = match packet_body(&data)
                            .ok_or(ConnectError::Malformed)
                            .and_then(connect_token)
                        {
                            Ok(token) => token,
                            Err(ConnectError::Malformed) => {
                                warn!(session_id = %session_id, "malformed MQTT CONNECT, closing");
                                break;
                            }
                            Err(ConnectError::UnsupportedLevel) => {
                                warn!(session_id = %session_id, "MQTT CONNECT for an unsupported protocol level, refusing");
                                let _ = sender
                                    .send(Message::Binary(build_connack(false, 1).into()))
                                    .await; // unacceptable protocol version
                                break;
                            }
                        };
                        info!(session_id = %session_id, "MQTT CONNECT received");
                        let Some(auth) = authenticate(token.as_deref()) else {
                            warn!(session_id = %session_id, "MQTT CONNECT without a valid token, refusing");
                            let _ = sender
                                .send(Message::Binary(build_connack(false, 5).into()))
                                .await; // not authorized
                            break;
                        };
                        user_id = auth.user_id;
                        revoked = auth.revoked;
                        connected = true;

                        // Send CONNACK
                        let connack = build_connack(false, 0); // Accepted
                        if sender.send(Message::Binary(connack.into())).await.is_err() {
                            break;
                        }
                        info!(session_id = %session_id, "MQTT CONNACK sent");
                    }

                    PacketType::Subscribe => {
                        // Parse SUBSCRIBE packet
                        if let Some((remaining_len, len_bytes)) = parse_remaining_length(&data[1..])
                        {
                            let payload_start = 1 + len_bytes;
                            if data.len() >= payload_start + 2 {
                                let packet_id = u16::from_be_bytes([
                                    data[payload_start],
                                    data[payload_start + 1],
                                ]);

                                // Parse topic filters
                                let mut offset = payload_start + 2;
                                let mut qos_results = Vec::new();
                                let known = subscriptions.len();

                                while offset < payload_start + remaining_len {
                                    if let Some((topic, topic_len)) =
                                        parse_mqtt_string(&data[offset..])
                                    {
                                        offset += topic_len;
                                        if offset < data.len() {
                                            let qos = data[offset] & 0x03;
                                            offset += 1;

                                            if is_concrete(topic) {
                                                info!(session_id = %session_id, "MQTT SUBSCRIBE to topic: {} (QoS {})", topic, qos);
                                                subscriptions.push(topic.to_string());
                                                qos_results.push(qos);
                                            } else {
                                                info!(session_id = %session_id, "MQTT SUBSCRIBE refused, no concrete topic: {:?}", topic);
                                                qos_results.push(SUBACK_FAILURE);
                                            }
                                        }
                                    } else {
                                        break;
                                    }
                                }

                                // Send SUBACK
                                let suback = build_suback(packet_id, &qos_results);
                                if sender.send(Message::Binary(suback.into())).await.is_err() {
                                    break;
                                }
                                info!(session_id = %session_id, "MQTT SUBACK sent");
                                // Like /notifications/ws/json/1's initial SyncComplete: anything
                                // broadcast before this subscription had nowhere to go.
                                let fresh: Vec<String> = subscriptions[known..]
                                    .iter()
                                    .filter(|t| !subscriptions[..known].contains(t))
                                    .cloned()
                                    .collect();
                                let catch_up = WsMessage::sync_complete(
                                    generation(),
                                    "local-server",
                                    &user_id,
                                );
                                for packet in notification_publishes(&catch_up, &fresh, &user_id) {
                                    if sender.send(Message::Binary(packet.into())).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }

                    PacketType::Pingreq => {
                        debug!(session_id = %session_id, "MQTT PINGREQ received");
                        let pingresp = build_pingresp();
                        if sender.send(Message::Binary(pingresp.into())).await.is_err() {
                            break;
                        }
                        debug!(session_id = %session_id, "MQTT PINGRESP sent");
                    }

                    PacketType::Disconnect => {
                        info!(session_id = %session_id, "MQTT DISCONNECT received");
                        break;
                    }

                    PacketType::Publish => {
                        debug!(session_id = %session_id, "MQTT PUBLISH received");
                        // Handle incoming publishes if needed
                    }

                    _ => {
                        debug!(session_id = %session_id, "Unhandled MQTT packet type: {:?}", packet_type);
                    }
                }
            }

            Ok(Message::Ping(_)) => {
                debug!(session_id = %session_id, "WebSocket ping");
            }

            Ok(Message::Pong(_)) => {
                debug!(session_id = %session_id, "WebSocket pong");
            }

            Ok(Message::Close(_)) => {
                info!(session_id = %session_id, "WebSocket close");
                break;
            }

            Ok(Message::Text(text)) => {
                warn!(session_id = %session_id, "Unexpected text message: {}", text);
            }

            Err(e) => {
                warn!(session_id = %session_id, "WebSocket error: {}", e);
                break;
            }
        }
    }

    info!(session_id = %session_id, "MQTT WebSocket connection closed");
}

/// Notification message for MQTT publish
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NotificationMessage {
    #[serde(rename = "messageType")]
    pub message_type: String,

    #[serde(rename = "sourceDeviceID", skip_serializing_if = "Option::is_none")]
    pub source_device_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}

impl NotificationMessage {
    pub fn sync_complete(generation: u64, source_device_id: Option<String>) -> Self {
        Self {
            message_type: "sync-complete".to_string(),
            source_device_id,
            generation: Some(generation),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    type Harness = (
        mpsc::UnboundedSender<Message>,
        mpsc::UnboundedReceiver<Vec<u8>>,
        broadcast::Sender<WsMessage>,
        tokio::task::JoinHandle<()>,
    );

    /// In-process MQTT client: feed packets in, read what the server wrote back.
    /// Every CONNECT is accepted as user `u1`.
    fn start() -> Harness {
        start_with(|_| Some("u1".into()))
    }

    /// A session of `user` that is never revoked.
    fn never_revoked(user: String) -> SessionAuth {
        SessionAuth {
            user_id: user,
            revoked: Box::pin(std::future::pending()),
        }
    }

    fn start_with(
        authenticate: impl Fn(Option<&str>) -> Option<String> + Send + 'static,
    ) -> Harness {
        start_with_auth(move |t| authenticate(t).map(never_revoked))
    }

    fn start_with_auth(
        authenticate: impl Fn(Option<&str>) -> Option<SessionAuth> + Send + 'static,
    ) -> Harness {
        let (in_tx, in_rx) = mpsc::unbounded_channel::<Message>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (notif_tx, notif_rx) = broadcast::channel(4);
        let incoming = Box::pin(futures_util::stream::unfold(in_rx, |mut rx| async move {
            rx.recv().await.map(|m| (Ok(m), rx))
        }));
        let sink = Box::pin(futures_util::sink::unfold(
            out_tx,
            |tx, m: Message| async move {
                if let Message::Binary(b) = m {
                    let _ = tx.send(b.to_vec());
                }
                Ok::<_, std::convert::Infallible>(tx)
            },
        ));
        let task = tokio::spawn(run_mqtt_session(
            sink,
            incoming,
            notif_rx,
            || 42,
            authenticate,
        ));
        (in_tx, out_rx, notif_tx, task)
    }

    async fn next(rx: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out")
            .expect("closed")
    }

    fn connect() -> Message {
        // CONNECT, MQTT 3.1.1, clean session, keepalive 60, client id "c"
        Message::Binary(
            vec![
                0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'c',
            ]
            .into(),
        )
    }

    /// CONNECT with username "dev" and `password`.
    fn connect_with_password(password: &str) -> Message {
        let mut p = vec![
            0x10,
            (10 + 3 + 5 + 2 + password.len()) as u8,
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            4,
            0xC2,
            0,
            60,
            0,
            1,
            b'c',
            0,
            3,
            b'd',
            b'e',
            b'v',
            0,
            password.len() as u8,
        ];
        p.extend_from_slice(password.as_bytes());
        Message::Binary(p.into())
    }

    fn subscribe(topic: &str) -> Message {
        let mut p = vec![
            0x82,
            (2 + 2 + topic.len() + 1) as u8,
            0,
            1,
            0,
            topic.len() as u8,
        ];
        p.extend_from_slice(topic.as_bytes());
        p.push(0);
        Message::Binary(p.into())
    }

    /// (topic, payload) of a QoS 0 PUBLISH with a one-byte remaining length.
    fn parse_publish(p: &[u8]) -> (String, serde_json::Value) {
        assert_eq!(p[0], 0x30, "QoS 0 PUBLISH");
        let (_, n) = parse_remaining_length(&p[1..]).unwrap();
        let (topic, tl) = parse_mqtt_string(&p[1 + n..]).unwrap();
        (
            topic.to_string(),
            serde_json::from_slice(&p[1 + n + tl..]).unwrap(),
        )
    }

    #[tokio::test]
    async fn sync_complete_is_published_to_subscribed_topic() {
        let (in_tx, mut out, notif, _task) = start();
        in_tx.send(connect()).unwrap();
        assert_eq!(next(&mut out).await, build_connack(false, 0));
        in_tx.send(subscribe("user/u1/sync")).unwrap();
        assert_eq!(next(&mut out).await[0], 0x90);
        let (topic, body) = parse_publish(&next(&mut out).await);
        assert_eq!(
            (topic.as_str(), &body["message"]["attributes"]["event"]),
            ("user/u1/sync", &serde_json::json!("SyncComplete")),
            "catch-up after SUBACK"
        );
        in_tx.send(subscribe("user/+/wild")).unwrap();
        in_tx.send(subscribe("user/u1/sync")).unwrap();
        assert_eq!(
            next(&mut out).await,
            build_suback(1, &[SUBACK_FAILURE]),
            "a wildcard filter is refused, not acknowledged and never served"
        );
        assert_eq!(
            next(&mut out).await,
            build_suback(1, &[0]),
            "wildcard or repeated subscription gets no catch-up"
        );

        notif
            .send(WsMessage::sync_complete(7, "local-server", "u1"))
            .unwrap();
        let (topic, body) = parse_publish(&next(&mut out).await);
        assert_eq!(topic, "user/u1/sync");
        assert_eq!(body["message"]["attributes"]["event"], "SyncComplete");

        // Screenshare traffic is per account; this endpoint has no identity.
        notif
            .send(WsMessage::screenshare_room_created("u1", "tablet", "r"))
            .unwrap();
        notif
            .send(WsMessage::sync_complete(8, "local-server", "u1"))
            .unwrap();
        let (_, body) = parse_publish(&next(&mut out).await);
        assert_eq!(
            body["message"]["attributes"]["event"], "SyncComplete",
            "wildcard filter and screenshare event produced nothing"
        );
        assert!(out.try_recv().is_err());
    }

    #[tokio::test]
    async fn nothing_is_pushed_before_connect() {
        let (in_tx, mut out, notif, _task) = start();
        notif
            .send(WsMessage::sync_complete(1, "local-server", "u1"))
            .unwrap();
        in_tx.send(connect()).unwrap();
        assert_eq!(
            next(&mut out).await,
            build_connack(false, 0),
            "first packet must be CONNACK"
        );
    }

    #[tokio::test]
    async fn lagged_receiver_gets_fresh_sync_complete_and_closed_channel_keeps_session() {
        let (in_tx, mut out, notif, task) = start();
        in_tx.send(connect()).unwrap();
        next(&mut out).await;
        in_tx.send(subscribe("t")).unwrap();
        next(&mut out).await; // SUBACK
        next(&mut out).await; // catch-up SyncComplete
        // Overflow the capacity-4 channel before the session can drain it.
        for i in 0..10 {
            let mut m = WsMessage::event("DocAdded", "x", "u1");
            m.message.attributes.id = Some(i.to_string());
            notif.send(m).unwrap();
        }
        let (_, first) = parse_publish(&next(&mut out).await);
        assert_eq!(
            first["message"]["attributes"]["event"], "SyncComplete",
            "lag is replaced by a catch-up SyncComplete"
        );
        for _ in 0..4 {
            next(&mut out).await;
        }

        drop(notif);
        in_tx
            .send(Message::Binary(vec![0xC0, 0x00].into()))
            .unwrap(); // PINGREQ
        assert_eq!(
            next(&mut out).await,
            build_pingresp(),
            "session survives a closed broadcast channel"
        );
        in_tx
            .send(Message::Binary(vec![0xE0, 0x00].into()))
            .unwrap(); // DISCONNECT
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn publish_topics_are_deduplicated_and_concrete() {
        let subs = vec![
            "a".to_string(),
            "a".to_string(),
            "#".to_string(),
            "b/+".to_string(),
            String::new(),
        ];
        let msg = WsMessage::sync_complete(1, "local-server", "u");
        let packets = notification_publishes(&msg, &subs, "u");
        assert_eq!(packets.len(), 1);
        assert_eq!(parse_publish(&packets[0]).0, "a");
        assert!(
            notification_publishes(&msg, &subs, "someone-else").is_empty(),
            "another user's event"
        );
        assert_eq!(
            notification_publishes(
                &WsMessage::sync_complete(1, "local-server", SERVER_USER),
                &subs,
                "u"
            )
            .len(),
            1,
            "server-wide event"
        );
    }

    #[test]
    fn remaining_length_round_trips_and_packet_body_bounds_the_frame() {
        // Every encoding-width boundary, up to the 4-byte maximum.
        for len in [
            0,
            127,
            128,
            16_383,
            16_384,
            2_097_151,
            2_097_152,
            268_435_455,
        ] {
            let mut encoded = Vec::new();
            push_remaining_length(&mut encoded, len);
            assert_eq!(
                parse_remaining_length(&encoded),
                Some((len, encoded.len())),
                "{len}"
            );
        }
        assert_eq!(
            parse_remaining_length(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
            None,
            "5 bytes"
        );

        assert_eq!(packet_body(&[0x10, 2, 7, 8]), Some(&[7, 8][..]));
        assert_eq!(
            packet_body(&[0x10, 1, 7, 8]),
            None,
            "one packet per frame: bytes past the packet are refused"
        );
        assert_eq!(packet_body(&[0x10, 3, 7, 8]), None, "truncated");
        assert_eq!(packet_body(&[0x10, 0x80]), None, "unterminated length");
        assert_eq!(packet_body(&[0x10]), None, "no length");
        assert_eq!(packet_body(&[]), None);

        // A SUBACK for more than 125 filters needs a two-byte remaining length.
        let suback = build_suback(7, &[0; 200]);
        assert_eq!(&suback[..5], &[0x90, 0xCA, 0x01, 0, 7]);
        assert_eq!(packet_body(&suback).map(<[u8]>::len), Some(202));
        assert_eq!(build_suback(1, &[SUBACK_FAILURE]), [0x90, 3, 0, 1, 0x80]);
    }

    proptest::proptest! {
        #[test]
        fn remaining_length_round_trips(len in 0usize..=268_435_455) {
            let mut encoded = Vec::new();
            push_remaining_length(&mut encoded, len);
            proptest::prop_assert_eq!(parse_remaining_length(&encoded), Some((len, encoded.len())));
        }

        #[test]
        fn packet_parsers_never_panic(frame in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
            if let Some(body) = packet_body(&frame) {
                proptest::prop_assert!(body.len() < frame.len());
            }
            let _ = connect_token(&frame);
        }
    }

    /// The session ended without writing a single packet (no CONNACK, no PINGRESP).
    async fn ended_silently(
        case: &str,
        mut out: mpsc::UnboundedReceiver<Vec<u8>>,
        task: tokio::task::JoinHandle<()>,
    ) {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap_or_else(|_| panic!("{case}: session still open"))
            .unwrap_or_else(|e| panic!("{case}: {e}"));
        assert_eq!(out.recv().await, None, "{case}: nothing written");
    }

    #[tokio::test]
    async fn anything_but_a_well_formed_connect_first_closes_without_connack() {
        let cases = [
            (
                "truncated CONNECT",
                vec![0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60],
            ),
            (
                "5-byte remaining length",
                vec![0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
            ),
            ("CONNECT without a remaining length", vec![0x10]),
            ("SUBSCRIBE first", vec![0x82, 6, 0, 1, 0, 1, b't', 0]),
            ("PINGREQ first", vec![0xC0, 0x00]),
            ("reserved packet type first", vec![0xF0, 0x00]),
            ("empty frame first", vec![]),
            // Well framed, but not a CONNECT body: authentication (which a header token
            // would pass whatever the CONNECT says) is never reached.
            ("CONNECT with an empty body", vec![0x10, 0]),
            (
                "CONNECT cut inside its protocol name",
                vec![0x10, 3, 0, 9, b'M'],
            ),
            (
                "CONNECT and a second packet in one frame",
                vec![
                    0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'c', 0xC0, 0,
                ],
            ),
        ];
        for (case, frame) in cases {
            let (in_tx, out, _notif, task) =
                start_with(|_| unreachable!("nothing reaches authentication"));
            in_tx.send(Message::Binary(frame.into())).unwrap();
            ended_silently(case, out, task).await;
        }

        // Another protocol level is a well-formed CONNECT this server doesn't speak.
        let (in_tx, mut out, _notif, task) =
            start_with(|_| unreachable!("nothing reaches authentication"));
        in_tx
            .send(Message::Binary(
                vec![
                    0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 60, 0, 1, b'c',
                ]
                .into(),
            ))
            .unwrap();
        assert_eq!(
            next(&mut out).await,
            build_connack(false, 1),
            "unacceptable protocol version"
        );
        ended_silently("MQTT 5 CONNECT", out, task).await;
    }

    /// Paused clock: `advance` and the runtime's auto-advance move time, nothing sleeps.
    #[tokio::test(start_paused = true)]
    async fn connect_must_arrive_within_the_deadline() {
        let (_in_tx, mut out, _notif, task) =
            start_with(|_| unreachable!("nothing reaches authentication"));
        let opened = tokio::time::Instant::now();
        tokio::time::timeout(CONNECT_TIMEOUT * 2, task)
            .await
            .expect("an idle socket is closed")
            .unwrap();
        assert!(
            opened.elapsed() >= CONNECT_TIMEOUT,
            "closed at the deadline, not before"
        );
        assert_eq!(
            out.recv().await,
            None,
            "idle socket closed, nothing written"
        );

        let (in_tx, mut out, _notif, _task) = start();
        tokio::task::yield_now().await; // the session starts its deadline
        tokio::time::advance(CONNECT_TIMEOUT - Duration::from_secs(1)).await;
        in_tx.send(connect()).unwrap();
        assert_eq!(
            next(&mut out).await,
            build_connack(false, 0),
            "a CONNECT just inside the deadline"
        );
        tokio::time::advance(CONNECT_TIMEOUT * 2).await;
        in_tx
            .send(Message::Binary(vec![0xC0, 0x00].into()))
            .unwrap(); // PINGREQ
        assert_eq!(
            next(&mut out).await,
            build_pingresp(),
            "no deadline once connected"
        );
    }

    #[test]
    fn connect_token_reads_password_else_username() {
        let body = |m: Message| {
            let Message::Binary(b) = m else {
                unreachable!()
            };
            b[2..].to_vec()
        };
        let token = |b: &[u8]| connect_token(b).map(|t| t.unwrap_or_else(|| "<none>".into()));
        assert_eq!(token(&body(connect_with_password("tok"))), Ok("tok".into()));
        assert_eq!(
            token(&body(connect_with_password(""))),
            Ok("dev".into()),
            "empty password falls back to username"
        );
        assert_eq!(connect_token(&body(connect())), Ok(None), "no credentials");
        // Will flag set: will topic and message come before the username/password.
        let will = [
            0, 4, b'M', b'Q', b'T', b'T', 4, 0xC6, 0, 60, 0, 1, b'c', 0, 1, b'w', 0, 2, 1, 2, 0, 1,
            b'u', 0, 2, b'p', b'w',
        ];
        assert_eq!(token(&will), Ok("pw".into()));
        let mqtt31 = [
            0, 6, b'M', b'Q', b'I', b's', b'd', b'p', 3, 0xC2, 0, 60, 0, 1, b'c', 0, 1, b'u', 0, 1,
            b't',
        ];
        assert_eq!(token(&mqtt31), Ok("t".into()), "MQTT 3.1");

        // Each of these is refused before any token is looked at.
        let with = |i: usize, b: u8| {
            let mut w = will.to_vec();
            w[i] = b;
            w
        };
        let mut trailing = will.to_vec();
        trailing.push(0);
        let malformed: [(&str, Vec<u8>); 9] = [
            ("empty", vec![]),
            ("truncated", will[..will.len() - 1].to_vec()),
            ("trailing byte", trailing),
            ("protocol name", with(2, b'X')),
            ("reserved flag bit", with(7, 0xC7)),
            ("password without username", with(7, 0x46)),
            ("will QoS without a will", with(7, 0xCA)),
            ("client id not UTF-8", with(12, 0xFF)),
            ("keep alive missing", will[..8].to_vec()),
        ];
        for (case, b) in malformed {
            assert_eq!(connect_token(&b), Err(ConnectError::Malformed), "{case}");
        }
        assert_eq!(
            connect_token(&with(6, 5)),
            Err(ConnectError::UnsupportedLevel),
            "MQTT 5"
        );
    }

    #[tokio::test]
    async fn unauthenticated_connect_is_refused_and_gets_nothing() {
        let (in_tx, mut out, notif, task) =
            start_with(|t| (t == Some("good")).then(|| "u1".to_string()));
        in_tx.send(connect()).unwrap();
        assert_eq!(
            next(&mut out).await,
            build_connack(false, 5),
            "not authorized"
        );
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        let _ = notif.send(WsMessage::sync_complete(1, "local-server", "u1"));
        assert!(
            out.recv().await.is_none(),
            "session closed without publishing"
        );

        let (in_tx, mut out, _notif, task) =
            start_with(|t| (t == Some("good")).then(|| "u1".to_string()));
        in_tx.send(connect_with_password("bad")).unwrap();
        assert_eq!(next(&mut out).await, build_connack(false, 5));
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn authenticated_client_only_gets_its_users_notifications() {
        let (in_tx, mut out, notif, _task) =
            start_with(|t| (t == Some("good")).then(|| "u1".to_string()));
        in_tx.send(connect_with_password("good")).unwrap();
        assert_eq!(next(&mut out).await, build_connack(false, 0));
        in_tx.send(subscribe("t")).unwrap();
        assert_eq!(next(&mut out).await[0], 0x90);
        let (_, body) = parse_publish(&next(&mut out).await);
        assert_eq!(
            body["message"]["attributes"]["auth0UserID"], "u1",
            "catch-up carries the session's user"
        );

        notif
            .send(WsMessage::sync_complete(5, "dev", "u2"))
            .unwrap();
        notif
            .send(WsMessage::sync_complete(6, "dev", "u1"))
            .unwrap();
        let (_, body) = parse_publish(&next(&mut out).await);
        assert_eq!(
            (
                &body["message"]["attributes"]["event"],
                &body["message"]["attributes"]["auth0UserID"]
            ),
            (&serde_json::json!("SyncComplete"), &serde_json::json!("u1")),
            "u2's event was skipped"
        );
        in_tx
            .send(Message::Binary(vec![0xC0, 0x00].into()))
            .unwrap(); // PINGREQ
        assert_eq!(
            next(&mut out).await,
            build_pingresp(),
            "nothing else was queued"
        );
    }
}
