//! Watch the tablet's screen share in a browser, served by this server.
//!
//! The server joins the tablet's screen share room as an in-process
//! participant of whichever broker the tablet uses, negotiates WebRTC with the
//! tablet, and decodes the frames with `remarkable-screenshare`. Browsers then
//! get the latest frame from `/screenshare/view`.
//!
//! Two signaling paths, carrying the same `PeerMessage`s:
//! - MQTT broker ([`Broker::local_client`]): xochitl up to 3.2x.
//! - REST rooms (`/screenshare/v1`, xochitl 3.27+/3.28): the tablet owns a room
//!   in [`RoomManager`] and exchanges `ScreenshareMessage` notifications whose
//!   base64 `data` is the peer message; the sender is `sourceDeviceID` and a
//!   direct message names its `targetClientId` (as the desktop app's
//!   `restbroker.cpp`/`roombroker.cpp` do).
//!
//! A session runs only while at least one browser is watching and ends shortly
//! after the last one leaves. The pages are protected by `ADMIN_TOKEN` (login
//! form → HttpOnly cookie, or an `x-admin-token` header).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use base64::Engine;
use parking_lot::Mutex;
use remarkable_mqtt::screenshare::{signaling_topic, subscriptions};
use remarkable_mqtt::{PeerMessage, SignalingEvent, SignalingRequest, WebRtcMessage};
use remarkable_screenshare::{pump_frames, Frame, PixelFormat, TransportConfig, Update, WebRtcHandler};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::notifications::WsMessage;
use crate::screenshare::{Broker, LocalClient};
use crate::screenshare_rest::RoomManager;

const COOKIE: &str = "rm_screen";
/// Default for [`ViewerConfig::idle_grace`].
pub const IDLE_GRACE: Duration = Duration::from_secs(20);
/// How often to look for a (new) room while screen share is off. This is an
/// in-process lookup, not a connection to the tablet.
const POLL_NOT_SHARING: Duration = Duration::from_secs(3);
/// Reconnect policy of the desktop app's Reconnector (Client ctor
/// 0x140169E50): delay min(1 s * 2^attempt, 30 s), at most 5 attempts.
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(30);
pub const MAX_RETRIES: u32 = 5;
/// The desktop gives up if the channel isn't active this long after joining
/// (handleNegotiationTimerEvent 0x14016F3D0).
const NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the viewer refreshes a REST room it is in, as the desktop app does.
const REST_KEEPALIVE: Duration = Duration::from_secs(20);
/// How long the tablet gets to answer each signaling step.
const SIGNALING_TIMEOUT: Duration = Duration::from_secs(15);

/// Viewer settings.
#[derive(Debug, Clone)]
pub struct ViewerConfig {
    /// Account whose tablet to watch (the paired account, `local-user`).
    pub user_id: String,
    /// Local WebRTC setup; `udp_ports` should match the firewall.
    pub transport: TransportConfig,
    /// Keep the tablet session this long after the last browser leaves.
    pub idle_grace: Duration,
}

impl Default for ViewerConfig {
    /// The paired account, host candidates only, 20 s idle grace.
    fn default() -> Self {
        Self { user_id: "local-user".into(), transport: TransportConfig::default(), idle_grace: IDLE_GRACE }
    }
}

/// The brokers a tablet may be using; at least one must be present.
#[derive(Clone, Default)]
pub struct Signaling {
    pub mqtt: Option<Broker>,
    pub rest: Option<RestRooms>,
}

/// The REST room broker's state (`AppState::screenshare` and `notification_tx`).
#[derive(Clone)]
pub struct RestRooms {
    pub rooms: RoomManager,
    pub notifications: broadcast::Sender<WsMessage>,
}

/// What the viewer is doing, sent to browsers as JSON.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Status {
    Idle,
    Connecting,
    /// Screen share is off on the tablet.
    NotSharing,
    /// The tablet answered the handshake but hasn't sent a picture yet. On
    /// connect it sends the whole current screen image (xochitl 3.29
    /// 0x459250 marks QImage::rect() dirty); with no image, only pings come.
    Connected,
    Streaming { width: u32, height: u32 },
    /// The last session failed; trying again in `in_secs`.
    Reconnecting { attempt: u32, max: u32, in_secs: u64, message: String },
    /// The tablet ended its screen share; waiting for it to start a new one.
    Stopped,
    /// Gave up after [`MAX_RETRIES`] failed attempts.
    Error { message: String },
}

/// Delay before retry `attempt` (1-based): 2, 4, 8, 16, 30 s.
fn retry_delay(attempt: u32) -> Duration {
    RETRY_BASE.saturating_mul(2u32.saturating_pow(attempt)).min(RETRY_CAP)
}

/// Shared viewer; clone freely.
#[derive(Clone)]
pub struct ScreenViewer {
    inner: Arc<Inner>,
}

struct Inner {
    signaling: Signaling,
    config: ViewerConfig,
    png: watch::Sender<Option<Arc<Vec<u8>>>>,
    status: watch::Sender<Status>,
    /// Pen position in the current frame's pixels; `None` hides the cursor.
    cursor: watch::Sender<Option<(u32, u32)>>,
    /// How many browsers are watching. A watch channel, so the supervisor
    /// can't miss the last one leaving between checking and waiting.
    watchers: watch::Sender<usize>,
    running: Mutex<bool>,
}

/// A browser watching the screen; the session stops some time after the last
/// one is dropped.
pub struct Watcher {
    viewer: ScreenViewer,
    pub png: watch::Receiver<Option<Arc<Vec<u8>>>>,
    pub status: watch::Receiver<Status>,
    pub cursor: watch::Receiver<Option<(u32, u32)>>,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.viewer.inner.watchers.send_modify(|n| *n -= 1);
    }
}

#[derive(Debug)]
enum SessionEnd {
    /// No active room: screen share is off.
    NotSharing,
    /// Everyone left.
    NoWatchers,
    /// The tablet ended its share (message 0x65): don't retry this room.
    Stopped { room_id: String },
    /// Worth retrying (the desktop's PingTimeout, NegotiationTimeout,
    /// ProtocolError, WebRtcFailure). `streamed` says whether it got as far
    /// as a live stream, which resets the retry budget.
    Failed { message: String, streamed: bool },
}

fn failed(message: impl Into<String>) -> SessionEnd {
    SessionEnd::Failed { message: message.into(), streamed: false }
}

impl ScreenViewer {
    pub fn new(signaling: Signaling, config: ViewerConfig) -> Self {
        let (png, _) = watch::channel(None);
        let (status, _) = watch::channel(Status::Idle);
        let (cursor, _) = watch::channel(None);
        Self {
            inner: Arc::new(Inner {
                signaling,
                config,
                png,
                status,
                cursor,
                watchers: watch::channel(0).0,
                running: Mutex::new(false),
            }),
        }
    }

    /// Start watching; starts a tablet session if none is running.
    pub fn watch(&self) -> Watcher {
        self.inner.watchers.send_modify(|n| *n += 1);
        let start = {
            let mut running = self.inner.running.lock();
            !std::mem::replace(&mut *running, true)
        };
        if start {
            let viewer = self.clone();
            tokio::spawn(async move { viewer.supervise().await });
        }
        Watcher {
            viewer: self.clone(),
            png: self.inner.png.subscribe(),
            status: self.inner.status.subscribe(),
            cursor: self.inner.cursor.subscribe(),
        }
    }

    fn watchers(&self) -> usize {
        *self.inner.watchers.borrow()
    }

    /// Resolves once nobody has been watching for [`IDLE_GRACE`].
    async fn idle(&self) {
        let mut watchers = self.inner.watchers.subscribe();
        loop {
            // `wait_for` checks the current value first, so no change is missed.
            if watchers.wait_for(|n| *n == 0).await.is_err() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(self.inner.config.idle_grace) => return,
                _ = watchers.wait_for(|n| *n > 0) => {}
            }
        }
    }

    /// Keep a session up while anyone is watching, retrying like the desktop
    /// app: failures back off (2, 4, 8, 16, 30 s, then give up), a stopped
    /// share waits for a new room, and screen share being off is polled.
    async fn supervise(self) {
        let mut attempt = 0;
        // Room of a share the tablet ended; not rejoined.
        let mut ended_room: Option<String> = None;
        loop {
            let wait = if let Some(room) = &ended_room {
                if self.active_room().is_some_and(|r| r != *room) {
                    ended_room = None;
                    continue;
                }
                POLL_NOT_SHARING
            } else {
                if attempt == 0 {
                    self.inner.status.send_replace(Status::Connecting);
                }
                let end = tokio::select! {
                    end = self.session() => end,
                    _ = self.idle() => SessionEnd::NoWatchers,
                };
                tracing::info!("screenshare viewer session ended: {end:?}");
                self.inner.cursor.send_replace(None);
                match end {
                    SessionEnd::NoWatchers => break,
                    SessionEnd::NotSharing => {
                        attempt = 0;
                        self.inner.status.send_replace(Status::NotSharing);
                        POLL_NOT_SHARING
                    }
                    SessionEnd::Stopped { room_id } => {
                        attempt = 0;
                        self.inner.status.send_replace(Status::Stopped);
                        ended_room = Some(room_id);
                        POLL_NOT_SHARING
                    }
                    SessionEnd::Failed { message, streamed } => {
                        attempt = if streamed { 1 } else { attempt + 1 };
                        if attempt > MAX_RETRIES {
                            self.inner.status.send_replace(Status::Error {
                                message: format!("gave up after {MAX_RETRIES} attempts: {message}"),
                            });
                            // Like the desktop, stop until someone opens the viewer again.
                            self.idle().await;
                            break;
                        }
                        let delay = retry_delay(attempt);
                        self.inner.status.send_replace(Status::Reconnecting {
                            attempt,
                            max: MAX_RETRIES,
                            in_secs: delay.as_secs(),
                            message,
                        });
                        delay
                    }
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.idle() => break,
            }
        }
        // Checked under the lock so a watcher arriving now restarts us.
        let mut running = self.inner.running.lock();
        *running = false;
        self.inner.status.send_replace(Status::Idle);
        self.inner.png.send_replace(None);
        self.inner.cursor.send_replace(None);
        if self.watchers() > 0 {
            *running = true;
            let viewer = self.clone();
            tokio::spawn(async move { viewer.supervise().await });
        }
    }

    /// The tablet's current room, without joining it: the newest one across
    /// both brokers, with whether it's on the REST rooms.
    fn newest_room(&self) -> Option<(String, bool)> {
        let uid = &self.inner.config.user_id;
        let signaling = &self.inner.signaling;
        let mqtt = signaling.mqtt.as_ref().and_then(|b| b.active_room_age(uid)).map(|(id, age)| (id, age, false));
        let rest = signaling.rest.as_ref().and_then(|r| r.rooms.active_room_age(uid)).map(|(id, age)| (id, age, true));
        // A stale room on one broker mustn't hide a fresh one on the other.
        [mqtt, rest].into_iter().flatten().min_by_key(|(_, age, _)| *age).map(|(id, _, rest)| (id, rest))
    }

    fn active_room(&self) -> Option<String> {
        self.newest_room().map(|(id, _)| id)
    }

    /// Find the tablet's room on whichever broker has one.
    async fn open_channel(&self, cid: &str) -> Result<Channel, SessionEnd> {
        let uid = &self.inner.config.user_id;
        let prefer_rest = self.newest_room().is_some_and(|(_, rest)| rest);
        let try_rest = || self.inner.signaling.rest.as_ref().and_then(|rest| Channel::join_rest(rest, uid, cid));
        if prefer_rest {
            if let Some(channel) = try_rest() {
                return Ok(channel);
            }
        }
        if let Some(broker) = &self.inner.signaling.mqtt {
            match Channel::join_mqtt(broker, uid, cid).await {
                Err(SessionEnd::NotSharing) => {}
                joined => return joined,
            }
        }
        try_rest().ok_or(SessionEnd::NotSharing)
    }

    /// One negotiation and stream with the tablet.
    async fn session(&self) -> SessionEnd {
        let cid = format!("server-viewer-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let mut channel = match self.open_channel(&cid).await {
            Ok(c) => c,
            Err(end) => return end,
        };
        if let Err(e) = channel.broadcast(PeerMessage::RequestOffer { id: Some(cid.clone()) }) {
            return failed(e.to_string());
        }

        // Wait for the tablet's offer, keeping any candidates that arrive first
        // along with their sender.
        let mut early = Vec::new();
        let (tablet, offer) = loop {
            match tokio::time::timeout(SIGNALING_TIMEOUT, channel.recv()).await {
                Ok(Some((from, PeerMessage::WebRtc { payload }))) => match payload {
                    WebRtcMessage::Offer { description } => break (from, description),
                    WebRtcMessage::Candidate { candidate, mid } => early.push((from, candidate, mid)),
                    WebRtcMessage::Answer { .. } => {}
                },
                Ok(Some(_)) => {}
                Ok(None) => return failed("signaling closed"),
                Err(_) => return failed("tablet did not send an offer in time"),
            }
        };

        let (webrtc, mut ice_rx, mut data_rx) = match WebRtcHandler::new(self.inner.config.transport.clone()).await {
            Ok(w) => w,
            Err(e) => return failed(format!("WebRTC setup failed: {e}")),
        };
        let answer = match webrtc.accept_offer(&offer).await {
            Ok(a) => a,
            Err(e) => return failed(format!("bad offer from tablet: {e}")),
        };
        if let Err(e) = channel.direct(&tablet, WebRtcMessage::Answer { description: answer }) {
            return failed(e.to_string());
        }
        for (_, candidate, mid) in early.into_iter().filter(|(from, ..)| *from == tablet) {
            let _ = webrtc.add_ice_candidate(&candidate, mid.as_deref(), Some(0)).await;
        }
        tracing::info!(room = %channel.room_id(), tablet = %tablet, via = channel.kind(), "screenshare viewer answered tablet offer");
        let channel_room = channel.room_id().to_owned();

        let png = &self.inner.png;
        let status = &self.inner.status;
        let cursor = &self.inner.cursor;
        let connected = std::sync::atomic::AtomicBool::new(false);
        let frames = pump_frames(&mut data_rx, |update| match update {
            Update::Connected { .. } => {
                connected.store(true, std::sync::atomic::Ordering::Relaxed);
                status.send_replace(Status::Connected);
            }
            Update::Frame(frame) => {
                status.send_if_modified(|s| {
                    let streaming = Status::Streaming { width: frame.width, height: frame.height };
                    (*s != streaming).then(|| *s = streaming).is_some()
                });
                match encode_png(&frame) {
                    Ok(bytes) => {
                        png.send_replace(Some(Arc::new(bytes)));
                    }
                    Err(e) => tracing::warn!("screenshare viewer: PNG encode failed: {e}"),
                }
            }
            Update::Cursor(point) => {
                cursor.send_replace(point);
            }
        });
        // The channel must be up and handshaken within NEGOTIATION_TIMEOUT.
        let deadline = async {
            tokio::time::sleep(NEGOTIATION_TIMEOUT).await;
            if connected.load(std::sync::atomic::Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
        };
        let trickle = async {
            let mut keepalive = tokio::time::interval(REST_KEEPALIVE);
            loop {
                tokio::select! {
                    _ = keepalive.tick() => channel.keepalive(),
                    Some(c) = ice_rx.recv() => {
                        let _ = channel.direct(&tablet, WebRtcMessage::Candidate {
                            candidate: c.candidate,
                            mid: Some(c.sdp_mid.unwrap_or_else(|| "0".into())),
                        });
                    }
                    msg = channel.recv() => match msg {
                        // Only the peer that made the offer is part of this session.
                        Some((from, PeerMessage::WebRtc { payload: WebRtcMessage::Candidate { candidate, mid } })) if from == tablet => {
                            let _ = webrtc.add_ice_candidate(&candidate, mid.as_deref(), Some(0)).await;
                        }
                        Some(_) => {}
                        None => break,
                    },
                }
            }
        };
        let end = tokio::select! {
            r = frames => match r {
                Ok(()) => SessionEnd::Stopped { room_id: channel_room.clone() },
                Err(e) => SessionEnd::Failed { message: format!("stream ended: {e}"), streamed: connected.load(std::sync::atomic::Ordering::Relaxed) },
            },
            _ = trickle => SessionEnd::Failed { message: "signaling ended".into(), streamed: connected.load(std::sync::atomic::Ordering::Relaxed) },
            _ = deadline => failed("tablet did not connect in time"),
        };
        let _ = webrtc.close().await;
        end
    }
}

/// The viewer's membership of the tablet's room on one broker.
enum Channel {
    Mqtt { client: LocalClient, topic: String, room_id: String },
    Rest { rest: RestRooms, rx: broadcast::Receiver<WsMessage>, user_id: String, client_id: String, room_id: String },
}

impl Channel {
    async fn join_mqtt(broker: &Broker, uid: &str, cid: &str) -> Result<Channel, SessionEnd> {
        let mut client = broker.local_client(uid, cid, &subscriptions(uid, cid));
        let topic = signaling_topic(uid, cid);
        let join = SignalingRequest::JoinActiveRoom { room: String::new(), room_id: String::new() };
        client
            .publish(&topic, serde_json::to_vec(&join).unwrap_or_default())
            .map_err(|e| failed(e.to_string()))?;
        loop {
            let p = match tokio::time::timeout(SIGNALING_TIMEOUT, client.recv()).await {
                Ok(Some(p)) => p,
                Ok(None) => return Err(failed("broker dropped the viewer")),
                Err(_) => return Err(failed("broker did not answer join-active-room")),
            };
            match SignalingEvent::from_bytes(&p.payload) {
                Some(SignalingEvent::RoomNotFound) => return Err(SessionEnd::NotSharing),
                Some(SignalingEvent::RoomJoined { room_id, .. }) => return Ok(Channel::Mqtt { client, topic, room_id }),
                _ => {}
            }
        }
    }

    fn join_rest(rest: &RestRooms, uid: &str, cid: &str) -> Option<Channel> {
        let room_id = rest.rooms.active_room(uid)?;
        // Subscribe before announcing ourselves so no reply is missed.
        let rx = rest.notifications.subscribe();
        rest.rooms.join(&room_id, cid, uid).then(|| Channel::Rest {
            rest: rest.clone(),
            rx,
            user_id: uid.into(),
            client_id: cid.into(),
            room_id,
        })
    }

    /// Keep a REST room alive while we are in it (MQTT rooms live as long as
    /// the tablet's session).
    fn keepalive(&self) {
        if let Channel::Rest { rest, room_id, user_id, .. } = self {
            rest.rooms.keepalive(room_id, user_id);
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Channel::Mqtt { .. } => "mqtt",
            Channel::Rest { .. } => "rest",
        }
    }

    fn room_id(&self) -> &str {
        match self {
            Channel::Mqtt { room_id, .. } | Channel::Rest { room_id, .. } => room_id,
        }
    }

    fn broadcast(&self, payload: PeerMessage) -> anyhow::Result<()> {
        match self {
            Channel::Mqtt { client, topic, room_id } => {
                let req = SignalingRequest::Broadcast { room_id: room_id.clone(), payload };
                client.publish(topic, serde_json::to_vec(&req)?)
            }
            Channel::Rest { rest, user_id, client_id, room_id, .. } => {
                rest_send(rest, user_id, client_id, room_id, None, &payload)
            }
        }
    }

    fn direct(&self, target: &str, msg: WebRtcMessage) -> anyhow::Result<()> {
        let payload = PeerMessage::WebRtc { payload: msg };
        match self {
            Channel::Mqtt { client, topic, room_id } => {
                let req = SignalingRequest::Direct { room_id: room_id.clone(), client_id: target.into(), payload };
                client.publish(topic, serde_json::to_vec(&req)?)
            }
            Channel::Rest { rest, user_id, client_id, room_id, .. } => {
                rest_send(rest, user_id, client_id, room_id, Some(target), &payload)
            }
        }
    }

    /// Next peer message addressed to us, with its sender's client id.
    async fn recv(&mut self) -> Option<(String, PeerMessage)> {
        match self {
            Channel::Mqtt { client, .. } => loop {
                let p = client.recv().await?;
                match SignalingEvent::from_bytes(&p.payload) {
                    Some(SignalingEvent::Direct { client_id, payload }) => return Some((client_id, payload)),
                    Some(SignalingEvent::Broadcast { client_id, payload }) => return Some((client_id, payload)),
                    _ => {}
                }
            },
            Channel::Rest { rx, client_id, room_id, .. } => loop {
                let msg = match rx.recv().await {
                    Ok(msg) => msg,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("screenshare viewer: skipped {n} notifications");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                };
                let a = &msg.message.attributes;
                let for_us = a.target_client_id.as_deref().is_none_or(|t| t == client_id.as_str());
                if a.event != "ScreenshareMessage" || a.source_device_id == *client_id || !for_us
                    || a.room_id.as_deref() != Some(room_id.as_str())
                {
                    continue;
                }
                let Some(data) = msg.message.data.as_deref() else { continue };
                let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data) else { continue };
                if let Ok(payload) = serde_json::from_slice::<PeerMessage>(&bytes) {
                    return Some((a.source_device_id.clone(), payload));
                }
            },
        }
    }
}

/// Relay like `POST /screenshare/v1/rooms/{id}/messages/{broadcast,direct}`.
fn rest_send(rest: &RestRooms, user_id: &str, client_id: &str, room_id: &str, target: Option<&str>, payload: &PeerMessage) -> anyhow::Result<()> {
    let data = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(payload)?);
    rest.notifications
        .send(WsMessage::screenshare_message(user_id, client_id, room_id, target, &data))
        .map_err(|_| anyhow::anyhow!("no notification subscribers"))?;
    Ok(())
}

impl Drop for Channel {
    fn drop(&mut self) {
        if let Channel::Rest { rest, client_id, room_id, .. } = self {
            rest.rooms.leave(room_id, client_id);
        }
    }
}

fn encode_png(frame: &Frame) -> Result<Vec<u8>, png::EncodingError> {
    let mut out = Vec::with_capacity(frame.data.len() / 8);
    let mut encoder = png::Encoder::new(&mut out, frame.width, frame.height);
    encoder.set_color(match frame.format {
        PixelFormat::Gray8 => png::ColorType::Grayscale,
        PixelFormat::Rgb8 => png::ColorType::Rgb,
    });
    encoder.set_depth(png::BitDepth::Eight);
    encoder.set_compression(png::Compression::Fast);
    encoder.write_header()?.write_image_data(&frame.data)?;
    Ok(out)
}

// --- HTTP ---

/// Routes under `/screenshare/view`. Only mount when `ADMIN_TOKEN` is set.
pub fn router(viewer: ScreenViewer) -> Router {
    Router::new()
        .route("/screenshare/view", get(page))
        .route("/screenshare/view/login", post(login))
        .route("/screenshare/view/ws", get(ws))
        .route("/screenshare/view/frame.png", get(frame))
        .with_state(viewer)
}

fn admin_token() -> Option<String> {
    std::env::var("ADMIN_TOKEN").ok().map(|t| t.trim().to_owned()).filter(|t| !t.is_empty())
}

/// Constant-time comparison, so response timing leaks nothing about the token.
fn token_matches(given: &str, expected: &str) -> bool {
    given.len() == expected.len()
        && given.bytes().zip(expected.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

fn authorized(headers: &HeaderMap) -> bool {
    let Some(expected) = admin_token() else { return false };
    let header = headers.get("x-admin-token").and_then(|v| v.to_str().ok());
    let cookie = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|c| c.trim().strip_prefix(&format!("{COOKIE}=")).map(str::to_owned))
        .next();
    header.into_iter().map(str::to_owned).chain(cookie).any(|t| token_matches(&t, &expected))
}

async fn page(headers: HeaderMap) -> Html<String> {
    if authorized(&headers) {
        Html(VIEWER_HTML.to_owned())
    } else {
        Html(LOGIN_HTML.replace("{error}", ""))
    }
}

#[derive(Deserialize)]
struct LoginForm {
    token: String,
}

async fn login(Form(form): Form<LoginForm>) -> Response {
    match admin_token() {
        Some(expected) if token_matches(form.token.trim(), &expected) => {
            let cookie = format!(
                "{COOKIE}={}; HttpOnly; Secure; SameSite=Strict; Path=/screenshare/view; Max-Age=2592000",
                form.token.trim()
            );
            ([(header::SET_COOKIE, cookie)], Redirect::to("/screenshare/view")).into_response()
        }
        _ => (StatusCode::UNAUTHORIZED, Html(LOGIN_HTML.replace("{error}", "<p class=err>Wrong token.</p>")))
            .into_response(),
    }
}

async fn frame(State(viewer): State<ScreenViewer>, headers: HeaderMap) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let mut watcher = viewer.watch();
    let latest = tokio::time::timeout(Duration::from_secs(20), watcher.png.wait_for(Option::is_some)).await;
    match latest.ok().and_then(|r| r.ok()).and_then(|p| p.clone()) {
        Some(png) => ([(header::CONTENT_TYPE, "image/png"), (header::CACHE_CONTROL, "no-store")], png.to_vec()).into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, format!("{:?}", *watcher.status.borrow())).into_response(),
    }
}

async fn ws(State(viewer): State<ScreenViewer>, headers: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    upgrade.on_upgrade(move |socket| stream(socket, viewer))
}

/// Send status changes and cursor moves as JSON text and each new frame as a
/// binary PNG. Cursor messages are `{"cursor":[x,y]}` or `{"cursor":null}`.
async fn stream(mut socket: WebSocket, viewer: ScreenViewer) {
    let mut watcher = viewer.watch();
    watcher.png.mark_changed();
    watcher.status.mark_changed();
    watcher.cursor.mark_changed();
    loop {
        tokio::select! {
            changed = watcher.cursor.changed() => {
                if changed.is_err() { break }
                let point = *watcher.cursor.borrow_and_update();
                let msg = serde_json::json!({ "cursor": point.map(|(x, y)| [x, y]) }).to_string();
                if socket.send(Message::Text(msg.into())).await.is_err() { break }
            }
            changed = watcher.status.changed() => {
                if changed.is_err() { break }
                let status = serde_json::to_string(&*watcher.status.borrow_and_update()).unwrap_or_default();
                if socket.send(Message::Text(status.into())).await.is_err() { break }
            }
            changed = watcher.png.changed() => {
                if changed.is_err() { break }
                let Some(png) = watcher.png.borrow_and_update().clone() else { continue };
                if socket.send(Message::Binary(png.to_vec().into())).await.is_err() { break }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
        }
    }
}

const LOGIN_HTML: &str = r#"<!doctype html><html lang=en><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Tablet screen</title>
<style>body{font:16px system-ui;display:grid;place-items:center;min-height:100vh;margin:0;background:#f4f4f2}
form{display:grid;gap:.75rem;padding:2rem;background:#fff;border-radius:12px;box-shadow:0 1px 4px #0002;width:min(22rem,90vw)}
input,button{font:inherit;padding:.6rem;border-radius:8px;border:1px solid #bbb}button{background:#222;color:#fff;border:0}.err{color:#b00;margin:0}</style>
<form method=post action=/screenshare/view/login><h1 style="margin:0;font-size:1.2rem">Tablet screen</h1>{error}
<input type=password name=token placeholder="Admin token" autocomplete=current-password required autofocus>
<button>Sign in</button></form></html>"#;

const VIEWER_HTML: &str = r#"<!doctype html><html lang=en><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Tablet screen</title>
<style>html,body{margin:0;height:100%;background:#1b1b1b;color:#ddd;font:14px system-ui}
#stage{position:relative;width:fit-content;margin:auto}
#screen{display:block;max-width:100vw;max-height:calc(100vh - 2.2rem);background:#fff}
#pen{position:absolute;border-radius:50%;background:#e33;pointer-events:none;display:none;transform:translate(-50%,-50%)}
#bar{height:2.2rem;display:flex;align-items:center;gap:1rem;padding:0 1rem}
#dot{width:.6rem;height:.6rem;border-radius:50%;background:#888}#dot.live{background:#3c3}#dot.warn{background:#d93}#dot.bad{background:#d33}
button{margin-left:auto;background:none;color:inherit;border:1px solid #555;border-radius:6px;padding:.2rem .6rem;font:inherit}</style>
<div id=bar><span id=dot></span><span id=status>Connecting…</span><button onclick="document.documentElement.requestFullscreen()">Full screen</button></div>
<div id=stage><img id=screen alt="Tablet screen"><div id=pen></div></div>
<script>
const img=document.getElementById('screen'),pen=document.getElementById('pen'),status=document.getElementById('status'),dot=document.getElementById('dot');
// Pen marker: a 15 px filled circle in tablet pixels, as the desktop app draws it.
const PEN=15;let cursor=null;
function drawPen(){
  if(!cursor||!img.naturalWidth){pen.style.display='none';return}
  const k=img.clientWidth/img.naturalWidth;
  pen.style.display='block';pen.style.width=pen.style.height=Math.max(4,PEN*k)+'px';
  pen.style.left=cursor[0]*k+'px';pen.style.top=cursor[1]*k+'px';
}
new ResizeObserver(drawPen).observe(img);img.addEventListener('load',drawPen);
function describe(s){
  switch(s.state){
    case 'idle':return['Idle',''];
    case 'connecting':return['Connecting to tablet…',''];
    case 'not-sharing':return['Screen share is off on the tablet','warn'];
    case 'connected':return['Connected, waiting for the tablet\'s picture (open a document on the tablet if nothing appears)','live'];
    case 'streaming':return['Live','live'];
    case 'reconnecting':return[`Reconnecting (${s.attempt}/${s.max}) in ${s.in_secs}s: ${s.message}`,'warn'];
    case 'stopped':return['The tablet stopped sharing; waiting for a new share','warn'];
    case 'error':return['Stopped: '+s.message+' (reload to try again)','bad'];
    default:return[s.state,''];
  }
}
function connect(){
  const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/screenshare/view/ws');
  ws.binaryType='blob';
  ws.onmessage=e=>{
    if(typeof e.data==='string'){const m=JSON.parse(e.data);
      if('cursor' in m){cursor=m.cursor;drawPen();return}
      const [t,c]=describe(m);status.textContent=t;dot.className=c;
      if(m.state!=='streaming'){cursor=null;drawPen()}
      return}
    const url=URL.createObjectURL(e.data);const old=img.src;img.src=url;if(old)URL.revokeObjectURL(old)};
  ws.onclose=()=>{status.textContent='Disconnected, retrying…';dot.className='warn';setTimeout(connect,2000)};
}
connect();
</script></html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delays_match_the_desktop() {
        let delays: Vec<u64> = (1..=5).map(|n| retry_delay(n).as_secs()).collect();
        assert_eq!(delays, [2, 4, 8, 16, 30]);
    }

    #[test]
    fn token_comparison() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abd", "abc"));
        assert!(!token_matches("ab", "abc"));
    }

    #[test]
    fn png_round_trip_size() {
        for (format, bytes, color) in [(PixelFormat::Gray8, 1, png::ColorType::Grayscale), (PixelFormat::Rgb8, 3, png::ColorType::Rgb)] {
            let frame = Frame { data: vec![255; 4 * 3 * bytes], width: 4, height: 3, format, timestamp: std::time::Instant::now() };
            let png = encode_png(&frame).unwrap();
            let reader = png::Decoder::new(std::io::Cursor::new(png)).read_info().unwrap();
            assert_eq!((reader.info().width, reader.info().height, reader.info().color_type), (4, 3, color));
        }
    }
}
