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
//! A connection lives only as long as the registration its token was minted under: once
//! that device is revoked (deleted, self-unregistered, or re-paired to another user) the
//! broker closes it, drops its subscriptions and leaves its rooms. In-process clients
//! ([`Broker::local_client`]) present no token and are not tied to a device.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use parking_lot::Mutex;
use rumqttc::mqttbytes::v4::{
    self,
    ConnAck,
    ConnectReturnCode,
    Packet,
    PingResp,
    PubAck,
    Publish,
    SubAck,
    SubscribeReasonCode,
    UnsubAck,
};
use rumqttc::mqttbytes::{QoS, matches};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::device::{DeviceManager, SESSION_RECHECK};

const MAX_PACKET: usize = 1024 * 1024;
/// Rooms without activity for this long are dropped (rmfakecloud `roomTimeout`).
const ROOM_TIMEOUT: Duration = Duration::from_secs(60);
const ROOM_SWEEP: Duration = Duration::from_secs(15);
const SIGNALING_PREFIX: &str = "remarkable/screenshare/signaling/user/";
/// Outbound queue per client; messages to a client that can't keep up are dropped.
const CLIENT_QUEUE: usize = 256;
/// How long to wait for a TLS close_notify to go out to a revoked client before dropping it.
const REVOKED_SHUTDOWN: Duration = Duration::from_secs(5);

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
    /// Reason for room-not-found; the desktop's broker reads it (broker.cpp).
    #[serde(skip_serializing_if = "str::is_empty")]
    message: &'a str,
}

struct Room {
    user_id: String,
    participants: Vec<String>, // MQTT client ids
    created: Instant,
    last_activity: Instant,
}

struct Client {
    /// Tells a registration apart from a later one reusing its client id.
    session: u64,
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
    next_session: std::sync::atomic::AtomicU64,
    /// How often a connection re-checks its device's registration, as a backstop for a
    /// missed revocation event ([`SESSION_RECHECK`]; shorter in tests).
    session_recheck: Duration,
}

fn acl_allows(user_id: &str, topic: &str, write: bool) -> bool {
    topic.starts_with(&format!("user/{user_id}/"))
        || topic.starts_with(&format!("{SIGNALING_PREFIX}{user_id}/"))
        || (!write && topic == "remarkable/screenshare/signaling")
}

impl Broker {
    /// `ice_servers`: list for `room-joined` (xochitl wants each entry's key as singular `url`).
    pub fn new(devices: DeviceManager, ice_servers: Value) -> Self {
        Self::with_session_recheck(devices, ice_servers, SESSION_RECHECK)
    }

    fn with_session_recheck(
        devices: DeviceManager,
        ice_servers: Value,
        session_recheck: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                devices,
                ice_servers,
                clients: Mutex::default(),
                rooms: Mutex::default(),
                next_session: Default::default(),
                session_recheck,
            }),
        }
    }

    /// Deliver to every subscribed, authorised client (QoS = min(publish, subscription)).
    pub fn publish(&self, topic: &str, payload: Vec<u8>, qos: QoS) {
        let clients = self.inner.clients.lock();
        for (id, c) in clients.iter() {
            let Some(sub_qos) = c
                .subscriptions
                .iter()
                .filter(|(f, _)| matches(topic, f))
                .map(|(_, q)| *q)
                .max_by_key(|q| *q as u8)
            else {
                continue;
            };
            if !acl_allows(&c.user_id, topic, false) {
                continue;
            }
            let qos = if (qos as u8) < (sub_qos as u8) {
                qos
            } else {
                sub_qos
            };
            if c.tx
                .try_send(Publish::new(topic, qos, payload.clone()))
                .is_err()
            {
                tracing::warn!(client = %id, %topic, "screenshare: client queue full, dropping message");
            }
        }
    }

    fn reply(&self, topic: String, reply: &Reply, qos: QoS) {
        if let Ok(body) = serde_json::to_vec(reply) {
            self.publish(&topic, body, qos);
        }
    }

    /// Like [`active_room`](Self::active_room), with how long ago it was created.
    pub fn active_room_age(&self, user_id: &str) -> Option<(String, Duration)> {
        let id = self.active_room(user_id)?;
        let created = self.inner.rooms.lock().get(&id)?.created;
        Some((id, created.elapsed()))
    }

    /// Newest room of `user_id`, if any.
    pub fn active_room(&self, user_id: &str) -> Option<String> {
        self.inner
            .rooms
            .lock()
            .iter()
            .filter(|(_, r)| r.user_id == user_id)
            .max_by_key(|(_, r)| r.created)
            .map(|(id, _)| id.clone())
    }

    fn touch_user_room(&self, user_id: &str) {
        if let Some(id) = self.active_room(user_id) {
            if let Some(r) = self.inner.rooms.lock().get_mut(&id) {
                r.last_activity = Instant::now();
            }
        }
    }

    fn join(&self, room_id: &str, client_id: &str) -> bool {
        let mut rooms = self.inner.rooms.lock();
        let Some(r) = rooms.get_mut(room_id) else {
            return false;
        };
        if !r.participants.iter().any(|p| p == client_id) {
            r.participants.push(client_id.into());
        }
        r.last_activity = Instant::now();
        true
    }

    fn peers(&self, room_id: &str, except: &str) -> Vec<String> {
        self.inner
            .rooms
            .lock()
            .get(room_id)
            .map(|r| {
                r.participants
                    .iter()
                    .filter(|p| *p != except)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn handle_signal(&self, user_id: &str, sender: &str, msg: Signal, qos: QoS) {
        match msg.kind.as_str() {
            "create-room" => {
                let room_id = match self.active_room(user_id) {
                    Some(existing) => {
                        self.join(&existing, sender);
                        existing
                    }
                    None => {
                        let id = uuid::Uuid::new_v4().to_string();
                        let now = Instant::now();
                        self.inner.rooms.lock().insert(
                            id.clone(),
                            Room {
                                user_id: user_id.into(),
                                participants: vec![sender.into()],
                                created: now,
                                last_activity: now,
                            },
                        );
                        tracing::info!(room = %id, client = %sender, "screenshare room created");
                        id
                    }
                };
                self.reply(
                    format!("user/{user_id}/signaling"),
                    &Reply {
                        kind: "room-created",
                        room: &msg.room,
                        room_id: &room_id,
                        ice_servers: None,
                        message: "",
                    },
                    qos,
                );
            }
            "join-auth-room" | "join-active-room" => {
                let room_id = if msg.room_id.is_empty() {
                    self.active_room(user_id).unwrap_or_default()
                } else {
                    msg.room_id
                };
                if room_id.is_empty() || !self.join(&room_id, sender) {
                    self.reply(
                        format!("user/{user_id}/client/{sender}/signaling/{room_id}"),
                        &Reply {
                            kind: "room-not-found",
                            room: "",
                            room_id: "",
                            ice_servers: None,
                            message: "no active screen share room",
                        },
                        qos,
                    );
                    return;
                }
                let ice = json!({ "ice_servers": self.inner.ice_servers });
                self.reply(
                    format!("user/{user_id}/client/{sender}/signaling/room/{room_id}"),
                    &Reply {
                        kind: "room-joined",
                        room: "",
                        room_id: &room_id,
                        ice_servers: Some(ice),
                        message: "",
                    },
                    qos,
                );
            }
            "broadcast" => {
                let room_id = if msg.room_id.is_empty() {
                    self.active_room(user_id).unwrap_or_default()
                } else {
                    msg.room_id
                };
                let body = serde_json::to_vec(
                    &json!({ "type": "broadcast", "clientId": sender, "payload": msg.payload }),
                )
                .unwrap_or_default();
                for peer in self.peers(&room_id, sender) {
                    self.publish(
                        &format!("user/{user_id}/client/{peer}/signaling/{room_id}"),
                        body.clone(),
                        qos,
                    );
                }
            }
            "direct" => {
                if msg.client_id.is_empty() {
                    tracing::warn!(client = %sender, "screenshare direct message without clientId");
                    return;
                }
                let room_id = if msg.room_id.is_empty() {
                    self.active_room(user_id).unwrap_or_default()
                } else {
                    msg.room_id
                };
                let body = serde_json::to_vec(
                    &json!({ "type": "direct", "clientId": sender, "payload": msg.payload }),
                )
                .unwrap_or_default();
                self.publish(
                    &format!(
                        "user/{user_id}/client/{}/signaling/{room_id}",
                        msg.client_id
                    ),
                    body,
                    qos,
                );
            }
            other => {
                tracing::warn!(kind = other, client = %sender, "unknown screenshare signaling message")
            }
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
                    Err(e) => {
                        tracing::warn!(topic = %p.topic, "bad screenshare signaling payload: {e}")
                    }
                }
            }
        }
        self.publish(&p.topic, p.payload.to_vec(), p.qos);
    }

    fn new_session(&self) -> u64 {
        self.inner
            .next_session
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Unregister `client_id` if it is still registration `session`. A client
    /// that reconnected with the same id has replaced it, and keeps its
    /// registration and room memberships.
    fn remove_client(&self, client_id: &str, session: u64) {
        // Hold the clients lock through the room cleanup, so a replacement
        // can't register in between and lose its memberships. (Lock order is
        // always clients, then rooms.)
        let mut clients = self.inner.clients.lock();
        if clients.get(client_id).is_none_or(|c| c.session != session) {
            return;
        }
        clients.remove(client_id);
        let mut rooms = self.inner.rooms.lock();
        for r in rooms.values_mut() {
            r.participants.retain(|p| p != client_id);
        }
        rooms.retain(|id, r| {
            let keep = !r.participants.is_empty();
            if !keep {
                tracing::info!(room = %id, "screenshare room closed (no participants)");
            }
            keep
        });
    }

    /// Attach a client that lives in this process rather than on an MQTT
    /// connection, e.g. the server's own screen share viewer. It is subject to
    /// the same ACL as a remote client of `user_id`, and leaves the broker
    /// (and any rooms) when dropped.
    pub fn local_client(&self, user_id: &str, client_id: &str, filters: &[String]) -> LocalClient {
        let (tx, rx) = mpsc::channel::<Publish>(CLIENT_QUEUE);
        let subscriptions = filters
            .iter()
            .filter(|f| {
                acl_allows(user_id, &f.replace(['+', '#'], "x"), false)
                    || f.starts_with(&format!("user/{user_id}/"))
            })
            .map(|f| (f.clone(), QoS::AtLeastOnce))
            .collect();
        let session = self.new_session();
        self.inner.clients.lock().insert(
            client_id.into(),
            Client {
                session,
                user_id: user_id.into(),
                subscriptions,
                tx,
            },
        );
        LocalClient {
            broker: self.clone(),
            session,
            user_id: user_id.into(),
            client_id: client_id.into(),
            rx,
        }
    }

    /// Drop rooms that have been idle for [`ROOM_TIMEOUT`] and have nobody
    /// connected. A sharing tablet can stay quiet for longer than that between
    /// viewers; its room lives as long as its MQTT session (`remove_client`
    /// closes rooms whose participants have all left).
    fn sweep_rooms(&self) {
        let connected: std::collections::HashSet<String> =
            self.inner.clients.lock().keys().cloned().collect();
        self.inner.rooms.lock().retain(|id, r| {
            let keep = r.participants.iter().any(|p| connected.contains(p))
                || r.last_activity.elapsed() < ROOM_TIMEOUT;
            if !keep {
                tracing::info!(room = %id, "screenshare room expired");
            }
            keep
        });
    }

    /// Accept TLS connections on `bind` until the process exits.
    pub async fn serve(
        self,
        bind: SocketAddr,
        tls: tokio_rustls::TlsAcceptor,
    ) -> std::io::Result<()> {
        let listener = crate::bind_when_available(bind).await?;
        tracing::info!(%bind, "screenshare MQTT broker listening (TLS)");
        let sweeper = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(ROOM_SWEEP);
            loop {
                tick.tick().await;
                sweeper.sweep_rooms();
            }
        });
        loop {
            let (tcp, peer) = listener.accept().await?;
            let (broker, tls) = (self.clone(), tls.clone());
            tokio::spawn(async move {
                match tls.accept(tcp).await {
                    Ok(stream) => {
                        if let Err(e) = broker.session(stream).await {
                            tracing::debug!(%peer, "mqtt session ended: {e}");
                        }
                    }
                    Err(e) => tracing::warn!(%peer, "mqtt TLS handshake failed: {e}"),
                }
            });
        }
    }

    async fn session<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        &self,
        mut stream: S,
    ) -> anyhow::Result<()> {
        let mut buf = BytesMut::with_capacity(4096);
        let mut out = BytesMut::new();

        // CONNECT (must be first)
        let connect = loop {
            match v4::read(&mut buf, MAX_PACKET) {
                Ok(Packet::Connect(c)) => break c,
                Ok(other) => anyhow::bail!("expected CONNECT, got {other:?}"),
                Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {
                    if stream.read_buf(&mut buf).await? == 0 {
                        anyhow::bail!("closed before CONNECT");
                    }
                }
                Err(e) => anyhow::bail!("bad packet: {e:?}"),
            }
        };
        let token = connect
            .login
            .as_ref()
            .map(|l| {
                if l.password.is_empty() {
                    l.username.clone()
                } else {
                    l.password.clone()
                }
            })
            .unwrap_or_default();
        // Same acceptance as `validate_token`, keeping the device and epoch as well.
        let Ok(identity) = self
            .inner
            .devices
            .session_identity(&format!("Bearer {token}"))
        else {
            ConnAck::new(ConnectReturnCode::BadUserNamePassword, false).write(&mut out)?;
            stream.write_all(&out).await?;
            anyhow::bail!("auth failed for client {}", connect.client_id);
        };
        // Resolves once that device is revoked (its event, or the periodic re-check, where only
        // a definite "not registered" counts: a DB hiccup keeps the session). It subscribes to
        // revocations here, before its first registration check, so none slips in between.
        let revoked = self
            .inner
            .devices
            .session_revoked_every(&identity, self.inner.session_recheck);
        tokio::pin!(revoked);
        let user_id = identity.user_id.clone();
        let client_id = if connect.client_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            connect.client_id.clone()
        };
        // CONNACK before joining the broker: past the insert, only `remove_client` below may end
        // the session, so a failed write here can't leave the client registered for good. Packets
        // the client sends meanwhile wait in `buf` or the socket until the loop below reads them.
        ConnAck::new(ConnectReturnCode::Success, false).write(&mut out)?;
        stream.write_all(&out).await?;
        let (tx, mut rx) = mpsc::channel::<Publish>(CLIENT_QUEUE);
        // A reconnect with the same client id replaces the old session.
        let session = self.new_session();
        self.inner.clients.lock().insert(
            client_id.clone(),
            Client {
                session,
                user_id: user_id.clone(),
                subscriptions: Vec::new(),
                tx,
            },
        );
        tracing::info!(client = %client_id, user = %user_id, device = %identity.device_id, "mqtt client connected");

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
                                    if let Some(c) = self.inner.clients.lock().get_mut(&client_id).filter(|c| c.session == session) {
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
                            if let Some(c) = self.inner.clients.lock().get_mut(&client_id).filter(|c| c.session == session) {
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

                // Biased so a revocation already known wins over queued deliveries and further
                // reads: a revoked device gets nothing more once its session could know.
                tokio::select! {
                    biased;
                    () = &mut revoked => {
                        tracing::info!(client = %client_id, device = %identity.device_id, "device revoked, closing screenshare mqtt session");
                        // MQTT 3.1.1 has no server DISCONNECT: closing the connection ends the
                        // session. Leaving the broker and its rooms happens below, as on any exit.
                        let _ = tokio::time::timeout(REVOKED_SHUTDOWN, stream.shutdown()).await;
                        return Ok(());
                    }
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

        self.remove_client(&client_id, session);
        tracing::info!(client = %client_id, "mqtt client disconnected");
        result
    }
}

/// An in-process broker client; see [`Broker::local_client`].
pub struct LocalClient {
    broker: Broker,
    session: u64,
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
        self.broker.on_publish(
            &self.user_id,
            &Publish::new(topic, QoS::AtLeastOnce, payload),
        );
        Ok(())
    }

    /// Next message delivered to this client's subscriptions.
    pub async fn recv(&mut self) -> Option<Publish> {
        self.rx.recv().await
    }
}

impl Drop for LocalClient {
    fn drop(&mut self) {
        self.broker.remove_client(&self.client_id, self.session);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> (Broker, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (Broker::new(devices, json!([])), tmp)
    }

    fn age_all_rooms(broker: &Broker) {
        let old = Instant::now().checked_sub(ROOM_TIMEOUT * 2).unwrap();
        for r in broker.inner.rooms.lock().values_mut() {
            r.last_activity = old;
        }
    }

    #[tokio::test]
    async fn sharing_tablets_room_survives_idle_sweeps() {
        let (broker, _tmp) = broker();
        let tablet = broker.local_client("u", "tablet", &["user/u/signaling".into()]);
        tablet
            .publish(
                "remarkable/screenshare/signaling/user/u/client/tablet",
                br#"{"type":"create-room"}"#.to_vec(),
            )
            .unwrap();
        assert!(broker.active_room("u").is_some());

        // Idle for longer than ROOM_TIMEOUT, but the tablet is still connected.
        age_all_rooms(&broker);
        broker.sweep_rooms();
        assert!(
            broker.active_room("u").is_some(),
            "room of a connected tablet was expired"
        );

        // The tablet disconnects: its room goes with it.
        drop(tablet);
        assert!(broker.active_room("u").is_none());
    }

    #[tokio::test]
    async fn stale_session_does_not_remove_its_replacement() {
        let (broker, _tmp) = broker();
        let old = broker.local_client("u", "tablet", &["user/u/signaling".into()]);
        // The tablet reconnects with the same client id before the old session ends.
        let new = broker.local_client("u", "tablet", &["user/u/signaling".into()]);
        new.publish(
            "remarkable/screenshare/signaling/user/u/client/tablet",
            br#"{"type":"create-room"}"#.to_vec(),
        )
        .unwrap();
        drop(old);
        assert!(
            broker.inner.clients.lock().contains_key("tablet"),
            "old session removed the new registration"
        );
        assert!(
            broker.active_room("u").is_some(),
            "old session closed the new session's room"
        );
        drop(new);
        assert!(broker.active_room("u").is_none());
    }

    fn broker_with_recheck(recheck: Duration) -> (Broker, DeviceManager, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let broker = Broker::with_session_recheck(devices.clone(), json!([]), recheck);
        (broker, devices, tmp)
    }

    /// Pair `device` to `user` the way the tablet does: (device token, user token).
    fn pair(dm: &DeviceManager, user: &str, device: &str) -> (String, String) {
        let code = dm.create_pairing_code(user).unwrap();
        dm.exchange_code(&code, device, "remarkable").unwrap()
    }

    const WAIT: Duration = Duration::from_secs(5);

    /// A remote broker client, as xochitl drives it, over an in-memory stream in place of
    /// the TLS connection (the session code is the same).
    struct Remote {
        io: tokio::io::DuplexStream,
        buf: BytesMut,
        session: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl Remote {
        /// CONNECT with `token` as the password; `None` if the broker refuses it.
        async fn connect(broker: &Broker, client_id: &str, token: &str) -> Option<Remote> {
            let (io, server) = tokio::io::duplex(64 * 1024);
            let broker = broker.clone();
            let session = tokio::spawn(async move { broker.session(server).await });
            let mut remote = Remote {
                io,
                buf: BytesMut::new(),
                session,
            };
            let mut connect = v4::Connect::new(client_id);
            connect.keep_alive = 600;
            connect.set_login("tablet", token);
            assert!(remote.send(|o| connect.write(o).map(drop)).await);
            match remote.next().await {
                Some(Packet::ConnAck(a)) if a.code == ConnectReturnCode::Success => Some(remote),
                Some(Packet::ConnAck(_)) => None,
                other => panic!("expected CONNACK, got {other:?}"),
            }
        }

        /// Write one packet; false once the broker has gone.
        async fn send(
            &mut self,
            write: impl FnOnce(&mut BytesMut) -> Result<(), rumqttc::mqttbytes::Error>,
        ) -> bool {
            let mut out = BytesMut::new();
            write(&mut out).unwrap();
            self.io.write_all(&out).await.is_ok()
        }

        /// Next packet from the broker; `None` once it has closed the connection.
        async fn next(&mut self) -> Option<Packet> {
            tokio::time::timeout(WAIT, async {
                loop {
                    match v4::read(&mut self.buf, MAX_PACKET) {
                        Ok(p) => return Some(p),
                        Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {}
                        Err(e) => panic!("bad packet from broker: {e:?}"),
                    }
                    if !matches!(self.io.read_buf(&mut self.buf).await, Ok(n) if n > 0) {
                        return None;
                    }
                }
            })
            .await
            .expect("broker neither answered nor closed the connection")
        }

        async fn subscribe(&mut self, filter: &str) {
            let mut sub = v4::Subscribe::new(filter, QoS::AtMostOnce);
            sub.pkid = 1;
            assert!(self.send(|o| sub.write(o).map(drop)).await);
            assert!(matches!(self.next().await, Some(Packet::SubAck(_))));
        }

        async fn publish(&mut self, topic: &str, payload: &[u8]) {
            let p = Publish::new(topic, QoS::AtMostOnce, payload.to_vec());
            assert!(self.send(|o| p.write(o).map(drop)).await);
        }

        /// Whether the session is still up: a PINGREQ is answered.
        async fn alive(&mut self) -> bool {
            if !self.send(|o| v4::PingReq.write(o).map(drop)).await {
                return false;
            }
            loop {
                match self.next().await {
                    Some(Packet::PingResp) => return true,
                    Some(_) => {} // a message for a subscription
                    None => return false,
                }
            }
        }

        /// The broker closes the connection, and the session (including leaving the broker and
        /// its rooms) ends without error.
        async fn closed(mut self) {
            while self.next().await.is_some() {}
            tokio::time::timeout(WAIT, self.session)
                .await
                .expect("session did not end after closing")
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn revoked_devices_session_is_closed_and_others_stay() {
        use remarkable_mqtt::screenshare::{signaling_topic, subscriptions};
        // No periodic re-check within the test: the revocation event alone must close it.
        let (broker, dm, _tmp) = broker_with_recheck(Duration::from_secs(3600));
        let uid = "local-user";
        let (_, ut_a) = pair(&dm, uid, "RM110-1");
        let (dt_b, _) = pair(&dm, uid, "RM110-2");
        let admin = dm.create_user_token(uid).unwrap();

        // Tablet A shares its screen (user token, as xochitl logs in)...
        let mut a = Remote::connect(&broker, "tablet-a", &ut_a)
            .await
            .expect("tablet a connects");
        a.subscribe(&format!("user/{uid}/signaling")).await;
        a.publish(
            &signaling_topic(uid, "tablet-a"),
            br#"{"type":"create-room"}"#,
        )
        .await;
        assert!(
            matches!(a.next().await, Some(Packet::Publish(p)) if p.topic == format!("user/{uid}/signaling"))
        );
        let room = broker.active_room(uid).expect("room created");
        // ...the in-process browser viewer (no token) watches it...
        let mut viewer = broker.local_client(uid, "viewer", &subscriptions(uid, "viewer"));
        viewer
            .publish(
                &signaling_topic("local-user", "viewer"),
                br#"{"type":"join-active-room"}"#.to_vec(),
            )
            .unwrap();
        let joined = tokio::time::timeout(WAIT, viewer.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&joined.payload).contains("room-joined"));
        // ...and another tablet (device token) and an admin-token client are connected too.
        let mut b = Remote::connect(&broker, "tablet-b", &dt_b).await.unwrap();
        let mut adm = Remote::connect(&broker, "admin", &admin).await.unwrap();
        // Tablet B's routine token churn (user-token refresh, 3.28 OAuth refresh and bundle, a
        // same-account re-pair) is not a revocation: its session stays up through all of it.
        dm.refresh_user_token(&dt_b).unwrap();
        dm.refresh_oauth(&dt_b).unwrap();
        dm.oauth_bundle(uid, "RM110-2", "remarkable").unwrap();
        pair(&dm, uid, "RM110-2");
        tokio::task::yield_now().await;
        assert!(b.alive().await, "token churn closed the tablet's session");

        assert!(dm.delete_device("RM110-1", None).unwrap());
        a.closed().await;
        assert!(!broker.inner.clients.lock().contains_key("tablet-a"));
        assert_eq!(
            broker.inner.rooms.lock()[&room].participants,
            ["viewer"],
            "the revoked tablet left its room; the viewer is still in it"
        );
        assert!(b.alive().await, "another device's session stays");
        assert!(adm.alive().await, "admin sessions are not tied to a device");
        // The viewer is unaffected: still in the broker and still receiving its signaling.
        b.publish(
            &signaling_topic(uid, "tablet-b"),
            format!(
                r#"{{"type":"direct","clientId":"viewer","roomId":"{room}","payload":{{"x":1}}}}"#
            )
            .as_bytes(),
        )
        .await;
        let direct = tokio::time::timeout(WAIT, viewer.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            direct.topic,
            format!("user/{uid}/client/viewer/signaling/{room}")
        );
        // The revoked token no longer connects.
        assert!(Remote::connect(&broker, "tablet-a", &ut_a).await.is_none());

        // Re-pairing tablet B to another account revokes it for this one.
        pair(&dm, "other-user", "RM110-2");
        b.closed().await;
        assert!(adm.alive().await);
        assert!(broker.inner.clients.lock().contains_key("viewer"));
        assert_eq!(broker.active_room(uid).as_deref(), Some(room.as_str()));
    }

    // Paused clock: each sleep below runs every 20 ms re-check tick inside it, however loaded the
    // machine (the clock only moves once all tasks are idle).
    #[tokio::test(start_paused = true)]
    async fn db_error_keeps_session_open_and_recheck_catches_silent_revocation() {
        let (broker, dm, tmp) = broker_with_recheck(Duration::from_millis(20));
        let (dt, _) = pair(&dm, "local-user", "RM110-1");
        let mut tablet = Remote::connect(&broker, "tablet", &dt).await.unwrap();
        let side = rusqlite::Connection::open(tmp.path().join("devices.db")).unwrap();

        // The ~10 re-checks in this window fail with "no such table": none may drop the tablet.
        side.execute_batch("ALTER TABLE devices RENAME TO devices_gone")
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(tablet.alive().await, "a DB error closed the session");
        // Re-checks that succeed again keep it too.
        side.execute_batch("ALTER TABLE devices_gone RENAME TO devices")
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(tablet.alive().await);

        // A registration that vanishes without an event is caught by the periodic re-check.
        side.execute("DELETE FROM devices WHERE device_id = 'RM110-1'", [])
            .unwrap();
        tablet.closed().await;
        assert!(broker.inner.clients.lock().is_empty());
    }

    #[tokio::test]
    async fn known_revocation_wins_over_queued_deliveries() {
        let (broker, dm, _tmp) = broker_with_recheck(Duration::from_secs(3600));
        let (dt, _) = pair(&dm, "local-user", "RM110-1");
        let mut tablet = Remote::connect(&broker, "tablet", &dt).await.unwrap();
        tablet.subscribe("user/local-user/signaling").await;
        // Single-threaded runtime: the session only runs again once this test awaits, and by then
        // the revocation and a backlog of messages for the device are both waiting.
        assert!(dm.delete_device("RM110-1", None).unwrap());
        for i in 0..16 {
            broker.publish(
                "user/local-user/signaling",
                format!("{i}").into_bytes(),
                QoS::AtMostOnce,
            );
        }
        if let Some(p) = tablet.next().await {
            panic!("a revoked device was sent {p:?}");
        }
        tablet.closed().await;
        assert!(broker.inner.clients.lock().is_empty());
    }

    #[tokio::test]
    async fn failed_connack_leaves_no_client_behind() {
        let (broker, dm, _tmp) = broker_with_recheck(Duration::from_secs(3600));
        let (dt, _) = pair(&dm, "local-user", "RM110-1");
        let mut a = Remote::connect(&broker, "tablet", &dt).await.unwrap();
        // A reconnect under the same client id that resets before its CONNACK goes out.
        let (mut io, server) = tokio::io::duplex(64 * 1024);
        let mut connect = v4::Connect::new("tablet");
        connect.set_login("tablet", &dt);
        let mut out = BytesMut::new();
        connect.write(&mut out).unwrap();
        io.write_all(&out).await.unwrap();
        drop(io);
        assert!(broker.session(server).await.is_err());
        // The failed session never joined, so the live one still holds the id and, on leaving,
        // takes it out of the broker.
        assert!(a.alive().await);
        assert!(a.send(|o| v4::Disconnect.write(o).map(drop)).await);
        a.closed().await;
        assert!(broker.inner.clients.lock().is_empty());
    }

    #[test]
    fn orphaned_idle_rooms_are_swept() {
        let (broker, _tmp) = broker();
        let now = Instant::now();
        broker.inner.rooms.lock().insert(
            "r".into(),
            Room {
                user_id: "u".into(),
                participants: vec!["gone".into()],
                created: now,
                last_activity: now,
            },
        );
        age_all_rooms(&broker);
        broker.sweep_rooms();
        assert!(broker.active_room("u").is_none());
    }
}
