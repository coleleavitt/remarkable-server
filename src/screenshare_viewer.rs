//! Watch the tablet's screen share in a browser, served by this server.
//!
//! The server joins the tablet's screen share room as a participant of its own
//! MQTT broker (through [`Broker::local_client`], no network hop), negotiates
//! WebRTC with the tablet, and decodes the frames with `remarkable-screenshare`.
//! Browsers then get the latest frame from `/screenshare/view`.
//!
//! A session runs only while at least one browser is watching and ends shortly
//! after the last one leaves. The pages are protected by `ADMIN_TOKEN` (login
//! form → HttpOnly cookie, or an `x-admin-token` header).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use parking_lot::Mutex;
use remarkable_mqtt::screenshare::{signaling_topic, subscriptions};
use remarkable_mqtt::{PeerMessage, SignalingEvent, SignalingRequest, WebRtcMessage};
use remarkable_screenshare::{pump_frames, Frame, TransportConfig, WebRtcHandler};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::screenshare::{Broker, LocalClient};

const COOKIE: &str = "rm_screen";
/// Keep the tablet session this long after the last browser leaves.
const IDLE_GRACE: Duration = Duration::from_secs(20);
/// Wait between attempts while screen share is off on the tablet.
const RETRY_NOT_SHARING: Duration = Duration::from_secs(3);
const RETRY_ERROR: Duration = Duration::from_secs(5);
/// How long the tablet gets to answer each signaling step.
const SIGNALING_TIMEOUT: Duration = Duration::from_secs(15);

/// Viewer settings.
#[derive(Debug, Clone)]
pub struct ViewerConfig {
    /// Account whose tablet to watch (the paired account, `local-user`).
    pub user_id: String,
    /// Local WebRTC setup; `udp_ports` should match the firewall.
    pub transport: TransportConfig,
}

/// What the viewer is doing, sent to browsers as JSON.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Status {
    Idle,
    Connecting,
    /// Screen share is off on the tablet.
    NotSharing,
    Streaming { width: u32, height: u32 },
    Error { message: String },
}

/// Shared viewer; clone freely.
#[derive(Clone)]
pub struct ScreenViewer {
    inner: Arc<Inner>,
}

struct Inner {
    broker: Broker,
    config: ViewerConfig,
    png: watch::Sender<Option<Arc<Vec<u8>>>>,
    status: watch::Sender<Status>,
    watchers: AtomicUsize,
    /// Wakes the supervisor when watchers come and go.
    watchers_changed: tokio::sync::Notify,
    running: Mutex<bool>,
}

/// A browser watching the screen; the session stops some time after the last
/// one is dropped.
pub struct Watcher {
    viewer: ScreenViewer,
    pub png: watch::Receiver<Option<Arc<Vec<u8>>>>,
    pub status: watch::Receiver<Status>,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.viewer.inner.watchers.fetch_sub(1, Ordering::SeqCst);
        self.viewer.inner.watchers_changed.notify_waiters();
    }
}

#[derive(Debug)]
enum SessionEnd {
    /// No active room: screen share is off.
    NotSharing,
    /// Everyone left.
    NoWatchers,
    /// The tablet stopped sharing.
    Stopped,
    Failed(String),
}

impl ScreenViewer {
    pub fn new(broker: Broker, config: ViewerConfig) -> Self {
        let (png, _) = watch::channel(None);
        let (status, _) = watch::channel(Status::Idle);
        Self {
            inner: Arc::new(Inner {
                broker,
                config,
                png,
                status,
                watchers: AtomicUsize::new(0),
                watchers_changed: tokio::sync::Notify::new(),
                running: Mutex::new(false),
            }),
        }
    }

    /// Start watching; starts a tablet session if none is running.
    pub fn watch(&self) -> Watcher {
        self.inner.watchers.fetch_add(1, Ordering::SeqCst);
        self.inner.watchers_changed.notify_waiters();
        let start = {
            let mut running = self.inner.running.lock();
            !std::mem::replace(&mut *running, true)
        };
        if start {
            let viewer = self.clone();
            tokio::spawn(async move { viewer.supervise().await });
        }
        Watcher { viewer: self.clone(), png: self.inner.png.subscribe(), status: self.inner.status.subscribe() }
    }

    fn watchers(&self) -> usize {
        self.inner.watchers.load(Ordering::SeqCst)
    }

    /// Resolves once nobody has been watching for [`IDLE_GRACE`].
    async fn idle(&self) {
        loop {
            while self.watchers() > 0 {
                self.inner.watchers_changed.notified().await;
            }
            tokio::select! {
                _ = tokio::time::sleep(IDLE_GRACE) => if self.watchers() == 0 { return },
                _ = self.inner.watchers_changed.notified() => {}
            }
        }
    }

    /// Keep a session up while anyone is watching.
    async fn supervise(self) {
        loop {
            self.inner.status.send_replace(Status::Connecting);
            let end = tokio::select! {
                end = self.session() => end,
                _ = self.idle() => SessionEnd::NoWatchers,
            };
            tracing::info!("screenshare viewer session ended: {end:?}");
            let wait = match end {
                SessionEnd::NoWatchers => break,
                SessionEnd::NotSharing | SessionEnd::Stopped => {
                    self.inner.status.send_replace(Status::NotSharing);
                    RETRY_NOT_SHARING
                }
                SessionEnd::Failed(message) => {
                    self.inner.status.send_replace(Status::Error { message });
                    RETRY_ERROR
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
        if self.watchers() > 0 {
            *running = true;
            let viewer = self.clone();
            tokio::spawn(async move { viewer.supervise().await });
        }
    }

    /// One negotiation and stream with the tablet.
    async fn session(&self) -> SessionEnd {
        let uid = &self.inner.config.user_id;
        let cid = format!("server-viewer-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let mut client = self.inner.broker.local_client(uid, &cid, &subscriptions(uid, &cid));
        let signal = Signal { topic: signaling_topic(uid, &cid) };

        let (room_id, tablet, offer, early) = match negotiate(&mut client, &signal, &cid).await {
            Ok(n) => n,
            Err(end) => return end,
        };

        let (webrtc, mut ice_rx, mut data_rx) = match WebRtcHandler::new(self.inner.config.transport.clone()).await {
            Ok(w) => w,
            Err(e) => return SessionEnd::Failed(format!("WebRTC setup failed: {e}")),
        };
        let answer = match webrtc.accept_offer(&offer).await {
            Ok(a) => a,
            Err(e) => return SessionEnd::Failed(format!("bad offer from tablet: {e}")),
        };
        let direct = |msg: WebRtcMessage| SignalingRequest::Direct {
            room_id: room_id.clone(),
            client_id: tablet.clone(),
            payload: PeerMessage::WebRtc { payload: msg },
        };
        if let Err(e) = signal.send(&client, &direct(WebRtcMessage::Answer { description: answer })) {
            return SessionEnd::Failed(e.to_string());
        }
        for (candidate, mid) in early {
            let _ = webrtc.add_ice_candidate(&candidate, mid.as_deref(), Some(0)).await;
        }
        tracing::info!(room = %room_id, tablet = %tablet, "screenshare viewer answered tablet offer");

        let png = &self.inner.png;
        let status = &self.inner.status;
        let frames = pump_frames(&mut data_rx, |frame| {
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
        });
        let trickle = async {
            loop {
                tokio::select! {
                    Some(c) = ice_rx.recv() => {
                        let msg = direct(WebRtcMessage::Candidate {
                            candidate: c.candidate,
                            mid: Some(c.sdp_mid.unwrap_or_else(|| "0".into())),
                        });
                        let _ = signal.send(&client, &msg);
                    }
                    Some(p) = client.recv() => {
                        if let Some(SignalingEvent::Direct {
                            payload: PeerMessage::WebRtc { payload: WebRtcMessage::Candidate { candidate, mid } }, ..
                        }) = SignalingEvent::from_bytes(&p.payload) {
                            let _ = webrtc.add_ice_candidate(&candidate, mid.as_deref(), Some(0)).await;
                        }
                    }
                    else => break,
                }
            }
        };
        let end = tokio::select! {
            r = frames => match r {
                Ok(()) => SessionEnd::Stopped,
                Err(e) => SessionEnd::Failed(format!("stream ended: {e}")),
            },
            _ = trickle => SessionEnd::Failed("signaling ended".into()),
        };
        let _ = webrtc.close().await;
        end
    }
}

struct Signal {
    topic: String,
}

impl Signal {
    fn send(&self, client: &LocalClient, request: &SignalingRequest) -> anyhow::Result<()> {
        client.publish(&self.topic, serde_json::to_vec(request)?)
    }
}

type Negotiated = (String, String, String, Vec<(String, Option<String>)>);

/// Join the active room and wait for the tablet's offer.
async fn negotiate(client: &mut LocalClient, signal: &Signal, cid: &str) -> Result<Negotiated, SessionEnd> {
    let fail = |e: anyhow::Error| SessionEnd::Failed(e.to_string());
    signal
        .send(client, &SignalingRequest::JoinActiveRoom { room: String::new(), room_id: String::new() })
        .map_err(fail)?;
    let mut room_id = None;
    let mut early = Vec::new();
    loop {
        let p = match tokio::time::timeout(SIGNALING_TIMEOUT, client.recv()).await {
            Ok(Some(p)) => p,
            Ok(None) => return Err(SessionEnd::Failed("broker dropped the viewer".into())),
            Err(_) => {
                let step = if room_id.is_some() { "an offer" } else { "a room" };
                return Err(SessionEnd::Failed(format!("tablet did not send {step} in time")));
            }
        };
        match SignalingEvent::from_bytes(&p.payload) {
            Some(SignalingEvent::RoomNotFound) => return Err(SessionEnd::NotSharing),
            Some(SignalingEvent::RoomJoined { room_id: r, .. }) if room_id.is_none() => {
                signal
                    .send(client, &SignalingRequest::Broadcast {
                        room_id: r.clone(),
                        payload: PeerMessage::RequestOffer { id: cid.into() },
                    })
                    .map_err(fail)?;
                room_id = Some(r);
            }
            Some(SignalingEvent::Direct { client_id, payload: PeerMessage::WebRtc { payload } }) => match payload {
                WebRtcMessage::Offer { description } => {
                    let room = room_id.ok_or_else(|| SessionEnd::Failed("offer before room".into()))?;
                    return Ok((room, client_id, description, early));
                }
                WebRtcMessage::Candidate { candidate, mid } => early.push((candidate, mid)),
                WebRtcMessage::Answer { .. } => {}
            },
            _ => {}
        }
    }
}

fn encode_png(frame: &Frame) -> Result<Vec<u8>, png::EncodingError> {
    let mut out = Vec::with_capacity(frame.data.len() / 8);
    let mut encoder = png::Encoder::new(&mut out, frame.width, frame.height);
    encoder.set_color(png::ColorType::Grayscale);
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
    std::env::var("ADMIN_TOKEN").ok().filter(|t| !t.is_empty())
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

/// Send status changes as JSON text and each new frame as a binary PNG.
async fn stream(mut socket: WebSocket, viewer: ScreenViewer) {
    let mut watcher = viewer.watch();
    watcher.png.mark_changed();
    watcher.status.mark_changed();
    loop {
        tokio::select! {
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
#screen{display:block;margin:auto;max-width:100vw;max-height:calc(100vh - 2.2rem);background:#fff}
#bar{height:2.2rem;display:flex;align-items:center;gap:1rem;padding:0 1rem}
#dot{width:.6rem;height:.6rem;border-radius:50%;background:#888}#dot.live{background:#3c3}#dot.warn{background:#d93}
button{margin-left:auto;background:none;color:inherit;border:1px solid #555;border-radius:6px;padding:.2rem .6rem;font:inherit}</style>
<div id=bar><span id=dot></span><span id=status>Connecting…</span><button onclick="document.documentElement.requestFullscreen()">Full screen</button></div>
<img id=screen alt="Tablet screen">
<script>
const img=document.getElementById('screen'),status=document.getElementById('status'),dot=document.getElementById('dot');
const text={idle:'Idle',connecting:'Connecting to tablet…','not-sharing':'Screen share is off on the tablet',streaming:'Live',error:'Error'};
function connect(){
  const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/screenshare/view/ws');
  ws.binaryType='blob';
  ws.onmessage=e=>{
    if(typeof e.data==='string'){const s=JSON.parse(e.data);
      status.textContent=(text[s.state]||s.state)+(s.message?': '+s.message:'');
      dot.className=s.state==='streaming'?'live':(s.state==='error'||s.state==='not-sharing'?'warn':'');return}
    const url=URL.createObjectURL(e.data);const old=img.src;img.src=url;if(old)URL.revokeObjectURL(old)};
  ws.onclose=()=>{status.textContent='Disconnected, retrying…';dot.className='warn';setTimeout(connect,2000)};
}
connect();
</script></html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abd", "abc"));
        assert!(!token_matches("ab", "abc"));
    }

    #[test]
    fn png_round_trip_size() {
        let frame = Frame { data: vec![255; 4 * 3], width: 4, height: 3, timestamp: std::time::Instant::now() };
        let png = encode_png(&frame).unwrap();
        let decoder = png::Decoder::new(std::io::Cursor::new(png));
        let reader = decoder.read_info().unwrap();
        assert_eq!((reader.info().width, reader.info().height), (4, 3));
    }
}
