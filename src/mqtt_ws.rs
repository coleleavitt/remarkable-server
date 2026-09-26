//! MQTT over WebSocket Handler
//!
//! Implements MQTT 3.1.1 protocol over WebSocket for device notifications.
//! The device expects full MQTT protocol, not plain JSON.
//!
//! Served at `/mqtt`. Nothing in the firmware notes or discovery pins a path
//! (`mqttbroker` in discovery is a bare host, and `/notifications/ws/json/1` is
//! the JSON endpoint), so this uses the conventional MQTT-over-WebSocket path
//! (VerneMQ's and Paho's default).

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::api::AppState;
use crate::notifications::WsMessage;

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

/// The token a CONNECT (variable header + payload) carries: its password, or its
/// username when the password is empty or absent, as the screenshare broker reads it.
/// `None` when it has no credentials or is malformed.
fn connect_token(body: &[u8]) -> Option<String> {
    let (_, mut at) = parse_mqtt_string(body)?; // protocol name
    let flags = *body.get(at + 1)?; // after the protocol level
    at += 4; // level, flags, keep alive
    at += parse_mqtt_bytes(body.get(at..)?)?.1; // client id
    if flags & 0x04 != 0 {
        for _ in 0..2 {
            at += parse_mqtt_bytes(body.get(at..)?)?.1;
        } // will topic, will message
    }
    let mut username = None;
    if flags & 0x80 != 0 {
        let (u, n) = parse_mqtt_bytes(body.get(at..)?)?;
        username = Some(u);
        at += n;
    }
    let password = if flags & 0x40 != 0 {
        Some(parse_mqtt_bytes(body.get(at..)?)?.0)
    } else {
        None
    };
    let token = password.filter(|p| !p.is_empty()).or(username)?;
    String::from_utf8(token.to_vec()).ok()
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
    let mut packet = vec![
        0x90,                         // SUBACK packet type
        (2 + qos_levels.len()) as u8, // Remaining length
        (packet_id >> 8) as u8,       // Packet ID MSB
        packet_id as u8,              // Packet ID LSB
    ];
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

    // Encode remaining length
    let mut len = remaining_len;
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if len == 0 {
            break;
        }
    }

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

/// WebSocket upgrade handler for MQTT notifications endpoint (`/mqtt`).
///
/// Authenticated with the same tokens as `/notifications/ws/json/1`: a bearer
/// `Authorization` header on the upgrade (an invalid one is rejected with 401),
/// or, when there is no such header, the token as the MQTT CONNECT password (or
/// username), as the screenshare broker accepts. A CONNECT without a valid token
/// gets CONNACK "not authorized" and the socket is closed.
pub async fn mqtt_notifications_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> crate::error::Result<impl IntoResponse> {
    let header_user = match headers.get(AUTHORIZATION) {
        Some(_) => Some(state.auth_user(&headers)?),
        None => None,
    };
    info!("MQTT WebSocket upgrade request for notifications");
    Ok(ws
        .protocols(["mqtt"])
        .on_upgrade(move |socket| handle_mqtt_socket(socket, state, header_user)))
}

/// Handle an individual MQTT WebSocket connection
async fn handle_mqtt_socket(socket: WebSocket, state: AppState, header_user: Option<String>) {
    let (sender, receiver) = socket.split();
    // Subscribe to broadcast channel for sync notifications
    let rx = state.notification_tx.subscribe();
    let storage = state.storage.clone();
    let devices = state.devices.clone();
    let authenticate = move |token: Option<&str>| {
        header_user
            .clone()
            .or_else(|| devices.validate_token(&format!("Bearer {}", token?)).ok())
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

/// MQTT PUBLISH packets (QoS 0) carrying `msg` for the session of `user_id`.
///
/// Assumption: nothing in this codebase pins the topic xochitl expects for
/// sync pushes over MQTT-over-WebSocket, so the notification goes out on each
/// concrete (wildcard-free) topic the client SUBSCRIBEd to. Filters with `+`/`#`
/// have no single concrete topic and are skipped. The payload is the same
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
        .filter(|t| !t.is_empty() && !t.contains(['+', '#']))
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
/// `authenticate` maps the CONNECT's token (if any) to the session's user id;
/// `None` refuses the CONNECT and ends the session.
async fn run_mqtt_session<S, R>(
    mut sender: S,
    mut receiver: R,
    mut rx: broadcast::Receiver<WsMessage>,
    generation: impl Fn() -> u64,
    authenticate: impl Fn(Option<&str>) -> Option<String>,
) where
    S: Sink<Message> + Unpin,
    R: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let session_id = uuid::Uuid::new_v4().to_string();
    info!(session_id = %session_id, "New MQTT WebSocket connection");

    let mut connected = false;
    let mut user_id = String::new();
    let mut subscriptions: Vec<String> = Vec::new();
    let mut notifications_open = true;

    loop {
        let result = tokio::select! {
            incoming = receiver.next() => match incoming { Some(r) => r, None => break },
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

                if data.is_empty() {
                    continue;
                }

                let packet_type_byte = (data[0] >> 4) & 0x0F;
                let packet_type = match PacketType::try_from(packet_type_byte) {
                    Ok(t) => t,
                    Err(_) => {
                        warn!(session_id = %session_id, "Unknown packet type: {}", packet_type_byte);
                        continue;
                    }
                };

                debug!(session_id = %session_id, "MQTT packet type: {:?}", packet_type);

                match packet_type {
                    PacketType::Connect => {
                        if connected {
                            warn!(session_id = %session_id, "second MQTT CONNECT, closing");
                            break;
                        }
                        // Parse CONNECT packet
                        if let Some((remaining_len, len_bytes)) = parse_remaining_length(&data[1..])
                        {
                            let payload_start = 1 + len_bytes;
                            if data.len() >= payload_start + remaining_len {
                                info!(session_id = %session_id, "MQTT CONNECT received");
                                let token = connect_token(
                                    &data[payload_start..payload_start + remaining_len],
                                );
                                let Some(user) = authenticate(token.as_deref()) else {
                                    warn!(session_id = %session_id, "MQTT CONNECT without a valid token, refusing");
                                    let _ = sender
                                        .send(Message::Binary(build_connack(false, 5).into()))
                                        .await; // not authorized
                                    break;
                                };
                                user_id = user;
                                connected = true;

                                // Send CONNACK
                                let connack = build_connack(false, 0); // Accepted
                                if sender.send(Message::Binary(connack.into())).await.is_err() {
                                    break;
                                }
                                info!(session_id = %session_id, "MQTT CONNACK sent");
                            }
                        }
                    }

                    PacketType::Subscribe => {
                        if !connected {
                            warn!(session_id = %session_id, "SUBSCRIBE before CONNECT");
                            continue;
                        }

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

                                            info!(session_id = %session_id, "MQTT SUBSCRIBE to topic: {} (QoS {})", topic, qos);
                                            subscriptions.push(topic.to_string());
                                            qos_results.push(qos);
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

    fn start_with(
        authenticate: impl Fn(Option<&str>) -> Option<String> + Send + 'static,
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
        assert_eq!(next(&mut out).await[0], 0x90);
        assert_eq!(
            next(&mut out).await[0],
            0x90,
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
    fn connect_token_reads_password_else_username() {
        let body = |m: Message| {
            let Message::Binary(b) = m else {
                unreachable!()
            };
            b[2..].to_vec()
        };
        assert_eq!(
            connect_token(&body(connect_with_password("tok"))).as_deref(),
            Some("tok")
        );
        assert_eq!(
            connect_token(&body(connect_with_password(""))).as_deref(),
            Some("dev"),
            "empty password falls back to username"
        );
        assert_eq!(connect_token(&body(connect())), None, "no credentials");
        // Will flag set: will topic and message come before the username/password.
        let will = [
            0, 4, b'M', b'Q', b'T', b'T', 4, 0xC6, 0, 60, 0, 1, b'c', 0, 1, b'w', 0, 2, 1, 2, 0, 1,
            b'u', 0, 2, b'p', b'w',
        ];
        assert_eq!(connect_token(&will).as_deref(), Some("pw"));
        assert_eq!(connect_token(&will[..will.len() - 1]), None, "truncated");
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
