//! MQTT Broker for real-time sync notifications
//!
//! Embeds rumqttd to provide MQTT broker functionality:
//! - TCP port 1883 for raw MQTT connections  
//! - WebSocket port 8083 for browser/WebSocket clients
//!
//! # Topics
//!
//! - `/notifications/ws/json/1` - Real-time notifications (device format)
//! - `/sync/v2/sync-complete` - Sync completion events

use bytes::Bytes;
use parking_lot::Mutex;
use rumqttd::{
    local::{LinkRx, LinkTx},
    Broker, Config, ConnectionSettings, RouterConfig, ServerSettings,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

/// Sync completion event payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCompleteEvent {
    /// Document/folder ID that changed
    pub document_id: String,
    /// Type of change
    pub change_type: ChangeType,
    /// New hash after change
    pub hash: String,
    /// Timestamp (Unix milliseconds)
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeType {
    Created,
    Modified,
    Deleted,
}

/// Notification message wrapper (reMarkable format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    #[serde(rename = "type")]
    pub notification_type: String,
    pub payload: serde_json::Value,
}

/// MQTT Broker configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// TCP listener address for raw MQTT (optional)
    pub tcp_addr: Option<SocketAddr>,
    /// WebSocket listener address
    pub ws_addr: SocketAddr,
    /// Maximum connections
    pub max_connections: usize,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u16,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            tcp_addr: Some("127.0.0.1:1883".parse().unwrap()),
            ws_addr: "127.0.0.1:8083".parse().unwrap(),
            max_connections: 1000,
            connection_timeout_ms: 60000,
        }
    }
}

/// Embedded MQTT broker
pub struct MqttBroker {
    /// Broadcast channel for sync events (internal use)
    sync_tx: broadcast::Sender<SyncCompleteEvent>,
    /// Link to publish messages programmatically (needs &mut)
    link_tx: Option<Mutex<LinkTx>>,
    /// Link receiver (for receiving routed messages)
    #[allow(dead_code)]
    link_rx: Option<LinkRx>,
    /// Configuration used
    config: MqttConfig,
}

impl MqttBroker {
    /// Create and start the MQTT broker
    pub fn start(config: MqttConfig) -> Result<Self, MqttError> {
        let (sync_tx, _) = broadcast::channel(100);
        
        // Build rumqttd config
        let mut broker_config = Config {
            id: 0,
            router: RouterConfig {
                max_connections: config.max_connections,
                max_outgoing_packet_count: 200,
                max_segment_size: 10 * 1024 * 1024, // 10 MB
                max_segment_count: 10,
                ..Default::default()
            },
            ..Default::default()
        };

        // Configure TCP server (optional)
        if let Some(tcp_addr) = config.tcp_addr {
            let tcp_settings = ServerSettings {
                name: "mqtt-tcp".to_string(),
                listen: tcp_addr,
                tls: None,
                next_connection_delay_ms: 1,
                connections: ConnectionSettings {
                    connection_timeout_ms: config.connection_timeout_ms,
                    max_payload_size: 256 * 1024,
                    max_inflight_count: 100,
                    auth: None,
                    external_auth: None,
                    dynamic_filters: true,
                },
            };
            
            broker_config.v4 = Some(HashMap::from([("1".to_string(), tcp_settings)]));
        }

        // Configure WebSocket server
        let ws_settings = ServerSettings {
            name: "mqtt-ws".to_string(),
            listen: config.ws_addr,
            tls: None,
            next_connection_delay_ms: 1,
            connections: ConnectionSettings {
                connection_timeout_ms: config.connection_timeout_ms,
                max_payload_size: 256 * 1024,
                max_inflight_count: 100,
                auth: None,
                external_auth: None,
                dynamic_filters: true,
            },
        };
        
        broker_config.ws = Some(HashMap::from([("1".to_string(), ws_settings)]));

        // Create broker
        let mut broker = Broker::new(broker_config);
        
        // Get a link for publishing
        let (tx, rx) = broker.link("remarkable-server")
            .map_err(|e| MqttError::LinkError(e.to_string()))?;

        // Start broker in background thread
        let _broker_handle = thread::Builder::new()
            .name("mqtt-broker".to_string())
            .spawn(move || {
                if let Err(e) = broker.start() {
                    error!("MQTT broker error: {:?}", e);
                }
            })
            .map_err(|e| MqttError::SpawnError(e.to_string()))?;
        
        info!(
            "MQTT broker started - TCP: {:?}, WebSocket: {}",
            config.tcp_addr, config.ws_addr
        );

        Ok(Self {
            sync_tx,
            link_tx: Some(Mutex::new(tx)),
            link_rx: Some(rx),
            config,
        })
    }
    
    /// Get the WebSocket address
    pub fn ws_addr(&self) -> SocketAddr {
        self.config.ws_addr
    }
    
    /// Get the TCP address (if configured)
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.config.tcp_addr
    }

    /// Publish a sync-complete event
    pub fn publish_sync_complete(&self, event: SyncCompleteEvent) -> Result<(), MqttError> {
        let link_tx = self.link_tx.as_ref()
            .ok_or(MqttError::NotConnected)?;
        let mut link_tx = link_tx.lock();
        
        // Publish to sync-complete topic
        let topic = "/sync/v2/sync-complete";
        let payload = serde_json::to_vec(&event)
            .map_err(|e| MqttError::SerializeError(e.to_string()))?;
        
        link_tx.publish(topic, Bytes::from(payload))
            .map_err(|e| MqttError::PublishError(e.to_string()))?;
        
        debug!("Published sync-complete: doc={}, type={:?}", 
               event.document_id, event.change_type);
        
        // Also publish to notification topic
        let notification = Notification {
            notification_type: "sync-complete".to_string(),
            payload: serde_json::to_value(&event).unwrap_or_default(),
        };
        
        let notif_payload = serde_json::to_vec(&notification)
            .map_err(|e| MqttError::SerializeError(e.to_string()))?;
        
        link_tx.publish("/notifications/ws/json/1", Bytes::from(notif_payload))
            .map_err(|e| MqttError::PublishError(e.to_string()))?;
        
        // Broadcast internally
        let _ = self.sync_tx.send(event);
        
        Ok(())
    }

    /// Subscribe to sync events internally
    pub fn subscribe_sync_events(&self) -> broadcast::Receiver<SyncCompleteEvent> {
        self.sync_tx.subscribe()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("Failed to create broker link: {0}")]
    LinkError(String),
    
    #[error("Failed to spawn broker thread: {0}")]
    SpawnError(String),
    
    #[error("Broker not connected")]
    NotConnected,
    
    #[error("Failed to serialize message: {0}")]
    SerializeError(String),
    
    #[error("Failed to publish message: {0}")]
    PublishError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sync_complete_event_serialization() {
        let event = SyncCompleteEvent {
            document_id: "doc-123".to_string(),
            change_type: ChangeType::Modified,
            hash: "abc123".to_string(),
            timestamp: 1234567890000,
        };
        
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("doc-123"));
        assert!(json.contains("modified"));
    }
}
