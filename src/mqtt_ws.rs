//! MQTT over WebSocket Handler
//! 
//! Implements MQTT 3.1.1 protocol over WebSocket for device notifications.
//! The device expects full MQTT protocol, not plain JSON.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tracing::{debug, info, warn, error};

use crate::api::AppState;

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

/// Parse MQTT string (2-byte length prefix + UTF-8)
fn parse_mqtt_string(data: &[u8]) -> Option<(&str, usize)> {
    if data.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + len {
        return None;
    }
    let s = std::str::from_utf8(&data[2..2+len]).ok()?;
    Some((s, 2 + len))
}

/// Build CONNACK packet
fn build_connack(session_present: bool, return_code: u8) -> Vec<u8> {
    vec![
        0x20, // CONNACK packet type
        0x02, // Remaining length
        if session_present { 0x01 } else { 0x00 }, // Session present flag
        return_code, // Return code (0 = accepted)
    ]
}

/// Build SUBACK packet
fn build_suback(packet_id: u16, qos_levels: &[u8]) -> Vec<u8> {
    let mut packet = vec![
        0x90, // SUBACK packet type
        (2 + qos_levels.len()) as u8, // Remaining length
        (packet_id >> 8) as u8, // Packet ID MSB
        packet_id as u8, // Packet ID LSB
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

/// WebSocket upgrade handler for MQTT notifications endpoint
pub async fn mqtt_notifications_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    info!("MQTT WebSocket upgrade request for notifications");
    ws.on_upgrade(move |socket| handle_mqtt_socket(socket, state))
}

/// Handle an individual MQTT WebSocket connection
async fn handle_mqtt_socket(socket: WebSocket, state: AppState) {
    let session_id = uuid::Uuid::new_v4().to_string();
    info!(session_id = %session_id, "New MQTT WebSocket connection");
    
    let (mut sender, mut receiver) = socket.split();
    let mut connected = false;
    let mut subscriptions: Vec<String> = Vec::new();
    
    // Subscribe to broadcast channel for sync notifications
    let mut rx = state.notification_tx.subscribe();
    
    while let Some(result) = receiver.next().await {
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
                        // Parse CONNECT packet
                        if let Some((remaining_len, len_bytes)) = parse_remaining_length(&data[1..]) {
                            let payload_start = 1 + len_bytes;
                            if data.len() >= payload_start + remaining_len {
                                // Skip protocol name and version for now
                                info!(session_id = %session_id, "MQTT CONNECT received");
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
                        if let Some((remaining_len, len_bytes)) = parse_remaining_length(&data[1..]) {
                            let payload_start = 1 + len_bytes;
                            if data.len() >= payload_start + 2 {
                                let packet_id = u16::from_be_bytes([
                                    data[payload_start],
                                    data[payload_start + 1],
                                ]);
                                
                                // Parse topic filters
                                let mut offset = payload_start + 2;
                                let mut qos_results = Vec::new();
                                
                                while offset < payload_start + remaining_len {
                                    if let Some((topic, topic_len)) = parse_mqtt_string(&data[offset..]) {
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
