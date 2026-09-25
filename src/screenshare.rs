//! Screenshare signaling: an MQTT 3.1.1 broker over TLS for xochitl 3.3.x.
//!
//! The firmware (sub_1F1848) connects to `ssl://vernemq-prod.cloud.remarkable.engineering:443`
//! and signals WebRTC rooms over MQTT; the screen itself goes peer-to-peer and never
//! touches the server. Besides plain topic routing, the broker answers signaling
//! messages published to `remarkable/screenshare/signaling/user/{uid}/client/{cid}`,
//! mirroring rmfakecloud's `internal/mqtt/broker.go`:
//! `create-room` -> `room-created`, `join-*-room` -> `room-joined`/`room-not-found`,
//! `broadcast`/`direct` -> relayed to the room's other clients.
//!
//! Auth: the device/user JWT in the CONNECT password (or username). ACL: a client may
//! only touch `user/{uid}/...` and `remarkable/screenshare/signaling/user/{uid}/...`.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::{Duration, Instant}};

use bytes::BytesMut;
use parking_lot::Mutex;
use rumqttc::mqttbytes::{matches, v4::{self, ConnAck, ConnectReturnCode, Packet, PingResp, PubAck, Publish, SubAck, SubscribeReasonCode, UnsubAck}, QoS};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::mpsc};

use crate::device::DeviceManager;

const MAX_PACKET: usize = 1024 * 1024;
/// Rooms without activity for this long are dropped (rmfakecloud `roomTimeout`).
const ROOM_TIMEOUT: Duration = Duration::from_secs(60);
const ROOM_SWEEP: Duration = Duration::from_secs(15);
const SIGNALING_PREFIX: &str = "remarkable/screenshare/signaling/user/";
/// Outbound queue per client; messages to a client that can't keep up are dropped.
const CLIENT_QUEUE: usize = 256;

#[derive(Deserialize, Default)]
#[serde(default)]
struct Signal {
    #[serde(rename = "type")]
    kind: String,
    room: String,
    #[serde(rename = "roomId")]
    room_id: String,
    #[serde(rename = "clientId")]
    client_id: String,
    payload: Map<String, Value>,
}

#[derive(Serialize)]
struct Reply<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    room: &'a str,
    #[serde(rename = "roomId", skip_serializing_if = "str::is_empty")]
    room_id: &'a str,
    #[serde(rename = "iceServers", skip_serializing_if = "Option::is_none")]
    ice_servers: Option<Value>,
}

struct Room {
    user_id: String,
    participants: Vec<String>, // MQTT client ids
    created: Instant,
    last_activity: Instant,
}

struct Client {
    user_id: String,
    subscriptions: Vec<(String, QoS)>,
    tx: mpsc::Sender<Publish>,
}

/// Shared broker state.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

struct Inner {
    devices: DeviceManager,
    ice_servers: Value,
    clients: Mutex<HashMap<String, Client>>,
    rooms: Mutex<HashMap<String, Room>>,
}

fn acl_allows(user_id: &str, topic: &str, write: bool) -> bool {
    topic.starts_with(&format!("user/{user_id}/"))
        || topic.starts_with(&format!("{SIGNALING_PREFIX}{user_id}/"))
        || (!write && topic == "remarkable/screenshare/signaling")
}

impl Broker {
    /// `ice_servers`: list for `room-joined` (xochitl wants each entry's key as singular `url`).
    pub fn new(devices: DeviceManager, ice_servers: Value) -> Self {
        Self { inner: Arc::new(Inner { devices, ice_servers, clients: Mutex::default(), rooms: Mutex::default() }) }
    }

    /// Deliver to every subscribed, authorised client (QoS = min(publish, subscription)).
    pub fn publish(&self, topic: &str, payload: Vec<u8>, qos: QoS) {
        let clients = self.inner.clients.lock();
        for (id, c) in clients.iter() {
            let Some(sub_qos) = c.subscriptions.iter().filter(|(f, _)| matches(topic, f)).map(|(_, q)| *q).max_by_key(|q| *q as u8) else { continue };
            if !acl_allows(&c.user_id, topic, false) { continue; }
            let qos = if (qos as u8) < (sub_qos as u8) { qos } else { sub_qos };
            if c.tx.try_send(Publish::new(topic, qos, payload.clone())).is_err() {
                tracing::warn!(client = %id, %topic, "screenshare: client queue full, dropping message");
            }
        }
    }

    fn reply(&self, topic: String, reply: &Reply, qos: QoS) {
        if let Ok(body) = serde_json::to_vec(reply) { self.publish(&topic, body, qos); }
    }

    fn active_room(&self, user_id: &str) -> Option<String> {
        self.inner.rooms.lock().iter().filter(|(_, r)| r.user_id == user_id)
            .max_by_key(|(_, r)| r.created).map(|(id, _)| id.clone())
    }

    fn touch_user_room(&self, user_id: &str) {
        if let Some(id) = self.active_room(user_id) {
            if let Some(r) = self.inner.rooms.lock().get_mut(&id) { r.last_activity = Instant::now(); }
        }
    }

    fn join(&self, room_id: &str, client_id: &str) -> bool {
        let mut rooms = self.inner.rooms.lock();
        let Some(r) = rooms.get_mut(room_id) else { return false };
        if !r.participants.iter().any(|p| p == client_id) { r.participants.push(client_id.into()); }
        r.last_activity = Instant::now();
        true
    }

    fn peers(&self, room_id: &str, except: &str) -> Vec<String> {
        self.inner.rooms.lock().get(room_id)
            .map(|r| r.participants.iter().filter(|p| *p != except).cloned().collect()).unwrap_or_default()
    }

    fn handle_signal(&self, user_id: &str, sender: &str, msg: Signal, qos: QoS) {
        match msg.kind.as_str() {
            "create-room" => {
                let room_id = match self.active_room(user_id) {
                    Some(existing) => { self.join(&existing, sender); existing }
                    None => {
                        let id = uuid::Uuid::new_v4().to_string();
                        let now = Instant::now();
                        self.inner.rooms.lock().insert(id.clone(), Room { user_id: user_id.into(), participants: vec![sender.into()], created: now, last_activity: now });
                        tracing::info!(room = %id, client = %sender, "screenshare room created");
                        id
                    }
                };
                self.reply(format!("user/{user_id}/signaling"), &Reply { kind: "room-created", room: &msg.room, room_id: &room_id, ice_servers: None }, qos);
            }
            "join-auth-room" | "join-active-room" => {
                let room_id = if msg.room_id.is_empty() { self.active_room(user_id).unwrap_or_default() } else { msg.room_id };
                if room_id.is_empty() || !self.join(&room_id, sender) {
                    self.reply(format!("user/{user_id}/client/{sender}/signaling/{room_id}"), &Reply { kind: "room-not-found", room: "", room_id: "", ice_servers: None }, qos);
                    return;
                }
                let ice = json!({ "ice_servers": self.inner.ice_servers });
                self.reply(format!("user/{user_id}/client/{sender}/signaling/room/{room_id}"), &Reply { kind: "room-joined", room: "", room_id: &room_id, ice_servers: Some(ice) }, qos);
            }
            "broadcast" => {
                let room_id = if msg.room_id.is_empty() { self.active_room(user_id).unwrap_or_default() } else { msg.room_id };
                let body = serde_json::to_vec(&json!({ "type": "broadcast", "clientId": sender, "payload": msg.payload })).unwrap_or_default();
                for peer in self.peers(&room_id, sender) {
                    self.publish(&format!("user/{user_id}/client/{peer}/signaling/{room_id}"), body.clone(), qos);
                }
            }
            "direct" => {
                if msg.client_id.is_empty() {
                    tracing::warn!(client = %sender, "screenshare direct message without clientId");
                    return;
                }
                let room_id = if msg.room_id.is_empty() { self.active_room(user_id).unwrap_or_default() } else { msg.room_id };
                let body = serde_json::to_vec(&json!({ "type": "direct", "clientId": sender, "payload": msg.payload })).unwrap_or_default();
                self.publish(&format!("user/{user_id}/client/{}/signaling/{room_id}", msg.client_id), body, qos);
            }
            other => tracing::warn!(kind = other, client = %sender, "unknown screenshare signaling message"),
        }
    }

    fn on_publish(&self, user_id: &str, p: &Publish) {
        self.touch_user_room(user_id);
        // remarkable/screenshare/signaling/user/{uid}/client/{cid}
        if let Some(rest) = p.topic.strip_prefix(SIGNALING_PREFIX) {
            let parts: Vec<&str> = rest.split('/').collect();
            if parts.len() >= 3 && parts[1] == "client" {
                match serde_json::from_slice::<Signal>(&p.payload) {
                    Ok(msg) => self.handle_signal(parts[0], parts[2], msg, p.qos),
                    Err(e) => tracing::warn!(topic = %p.topic, "bad screenshare signaling payload: {e}"),
                }
            }
        }
        self.publish(&p.topic, p.payload.to_vec(), p.qos);
    }

    fn remove_client(&self, client_id: &str) {
        self.inner.clients.lock().remove(client_id);
        let mut rooms = self.inner.rooms.lock();
        for r in rooms.values_mut() { r.participants.retain(|p| p != client_id); }
        rooms.retain(|id, r| {
            let keep = !r.participants.is_empty();
            if !keep { tracing::info!(room = %id, "screenshare room closed (no participants)"); }
            keep
        });
    }

    /// Attach a client that lives in this process rather than on an MQTT
    /// connection, e.g. the server's own screen share viewer. It is subject to
    /// the same ACL as a remote client of `user_id`, and leaves the broker
    /// (and any rooms) when dropped.
    pub fn local_client(&self, user_id: &str, client_id: &str, filters: &[String]) -> LocalClient {
        let (tx, rx) = mpsc::channel::<Publish>(CLIENT_QUEUE);
        let subscriptions = filters.iter()
            .filter(|f| acl_allows(user_id, &f.replace(['+', '#'], "x"), false) || f.starts_with(&format!("user/{user_id}/")))
            .map(|f| (f.clone(), QoS::AtLeastOnce))
            .collect();
        self.inner.clients.lock().insert(client_id.into(), Client { user_id: user_id.into(), subscriptions, tx });
        LocalClient { broker: self.clone(), user_id: user_id.into(), client_id: client_id.into(), rx }
    }

    fn sweep_rooms(&self) {
        self.inner.rooms.lock().retain(|id, r| {
            let keep = r.last_activity.elapsed() < ROOM_TIMEOUT;
            if !keep { tracing::info!(room = %id, "screenshare room expired"); }
            keep
        });
    }

    /// Accept TLS connections on `bind` until the process exits.
    pub async fn serve(self, bind: SocketAddr, tls: tokio_rustls::TlsAcceptor) -> std::io::Result<()> {
        let listener = crate::bind_when_available(bind).await?;
        tracing::info!(%bind, "screenshare MQTT broker listening (TLS)");
        let sweeper = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(ROOM_SWEEP);
            loop { tick.tick().await; sweeper.sweep_rooms(); }
        });
        loop {
            let (tcp, peer) = listener.accept().await?;
            let (broker, tls) = (self.clone(), tls.clone());
            tokio::spawn(async move {
                match tls.accept(tcp).await {
                    Ok(stream) => if let Err(e) = broker.session(stream).await {
                        tracing::debug!(%peer, "mqtt session ended: {e}");
                    },
                    Err(e) => tracing::warn!(%peer, "mqtt TLS handshake failed: {e}"),
                }
            });
        }
    }

    async fn session<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(&self, mut stream: S) -> anyhow::Result<()> {
        let mut buf = BytesMut::with_capacity(4096);
        let mut out = BytesMut::new();

        // CONNECT (must be first)
        let connect = loop {
            match v4::read(&mut buf, MAX_PACKET) {
                Ok(Packet::Connect(c)) => break c,
                Ok(other) => anyhow::bail!("expected CONNECT, got {other:?}"),
                Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {
                    if stream.read_buf(&mut buf).await? == 0 { anyhow::bail!("closed before CONNECT"); }
                }
                Err(e) => anyhow::bail!("bad packet: {e:?}"),
            }
        };
        let token = connect.login.as_ref().map(|l| if l.password.is_empty() { l.username.clone() } else { l.password.clone() }).unwrap_or_default();
        let Ok(user_id) = self.inner.devices.validate_token(&format!("Bearer {token}")) else {
            ConnAck::new(ConnectReturnCode::BadUserNamePassword, false).write(&mut out)?;
            stream.write_all(&out).await?;
            anyhow::bail!("auth failed for client {}", connect.client_id);
        };
        let client_id = if connect.client_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { connect.client_id.clone() };
        let (tx, mut rx) = mpsc::channel::<Publish>(CLIENT_QUEUE);
        // A reconnect with the same client id replaces the old session.
        self.inner.clients.lock().insert(client_id.clone(), Client { user_id: user_id.clone(), subscriptions: Vec::new(), tx });
        ConnAck::new(ConnectReturnCode::Success, false).write(&mut out)?;
        stream.write_all(&out).await?;
        tracing::info!(client = %client_id, user = %user_id, "mqtt client connected");

        // MQTT keepalive: disconnect after 1.5x the negotiated interval with no traffic.
        let idle = Duration::from_secs(u64::from(connect.keep_alive.max(1)) * 3 / 2);
        let mut next_pkid: u16 = 0;
        let result: anyhow::Result<()> = async {
            loop {
                // Drain any complete packets already buffered.
                loop {
                    let packet = match v4::read(&mut buf, MAX_PACKET) {
                        Ok(p) => p,
                        Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => break,
                        Err(e) => anyhow::bail!("bad packet: {e:?}"),
                    };
                    out.clear();
                    match packet {
                        Packet::Publish(p) => {
                            if !acl_allows(&user_id, &p.topic, true) {
                                tracing::warn!(client = %client_id, topic = %p.topic, "mqtt publish denied");
                                anyhow::bail!("publish to {} not allowed", p.topic);
                            }
                            if p.qos == QoS::AtLeastOnce { PubAck::new(p.pkid).write(&mut out)?; }
                            self.on_publish(&user_id, &p);
                        }
                        Packet::Subscribe(s) => {
                            let codes = s.filters.iter().map(|f| {
                                if acl_allows(&user_id, &f.path.replace(['+', '#'], "x"), false) || f.path.starts_with(&format!("user/{user_id}/")) {
                                    let qos = if f.qos == QoS::ExactlyOnce { QoS::AtLeastOnce } else { f.qos };
                                    if let Some(c) = self.inner.clients.lock().get_mut(&client_id) {
                                        c.subscriptions.retain(|(p, _)| p != &f.path);
                                        c.subscriptions.push((f.path.clone(), qos));
                                    }
                                    SubscribeReasonCode::Success(qos)
                                } else {
                                    tracing::warn!(client = %client_id, filter = %f.path, "mqtt subscribe denied");
                                    SubscribeReasonCode::Failure
                                }
                            }).collect();
                            SubAck::new(s.pkid, codes).write(&mut out)?;
                        }
                        Packet::Unsubscribe(u) => {
                            if let Some(c) = self.inner.clients.lock().get_mut(&client_id) {
                                c.subscriptions.retain(|(p, _)| !u.topics.contains(p));
                            }
                            UnsubAck::new(u.pkid).write(&mut out)?;
                        }
                        Packet::PingReq => {
                            self.touch_user_room(&user_id);
                            PingResp.write(&mut out)?;
                        }
                        Packet::Disconnect => return Ok(()),
                        Packet::PubAck(_) => {}
                        other => tracing::debug!(client = %client_id, "ignoring mqtt packet {other:?}"),
                    }
                    if !out.is_empty() { stream.write_all(&out).await?; }
                }

                tokio::select! {
                    read = tokio::time::timeout(idle, stream.read_buf(&mut buf)) => {
                        match read {
                            Ok(Ok(0)) => return Ok(()),
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => return Err(e.into()),
                            Err(_) => anyhow::bail!("keepalive timeout"),
                        }
                    }
                    Some(mut p) = rx.recv() => {
                        if p.qos != QoS::AtMostOnce {
                            next_pkid = next_pkid.checked_add(1).unwrap_or(1);
                            p.pkid = next_pkid;
                        }
                        out.clear();
                        p.write(&mut out)?;
                        stream.write_all(&out).await?;
                    }
                }
            }
        }.await;

        self.remove_client(&client_id);
        tracing::info!(client = %client_id, "mqtt client disconnected");
        result
    }
}

/// An in-process broker client; see [`Broker::local_client`].
pub struct LocalClient {
    broker: Broker,
    user_id: String,
    client_id: String,
    rx: mpsc::Receiver<Publish>,
}

impl LocalClient {
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Publish as this client, exactly as if it had arrived over MQTT.
    pub fn publish(&self, topic: &str, payload: Vec<u8>) -> anyhow::Result<()> {
        if !acl_allows(&self.user_id, topic, true) {
            anyhow::bail!("publish to {topic} not allowed");
        }
        self.broker.on_publish(&self.user_id, &Publish::new(topic, QoS::AtLeastOnce, payload));
        Ok(())
    }

    /// Next message delivered to this client's subscriptions.
    pub async fn recv(&mut self) -> Option<Publish> {
        self.rx.recv().await
    }
}

impl Drop for LocalClient {
    fn drop(&mut self) {
        self.broker.remove_client(&self.client_id);
    }
}
