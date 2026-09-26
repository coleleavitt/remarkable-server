//! What `/mqtt` logs for a SUBSCRIBE, captured from the route as `main.rs` serves it
//! with `MQTT_WS_NOTIFICATIONS=1`, at the deployed `RUST_LOG=remarkable_server=info`.
//!
//! This test has a binary, and so a process, of its own. tracing caches each callsite's
//! interest process-wide, and while one `Dispatch` exists it is decided by the default
//! subscriber of whichever thread reaches the callsite first (tracing-core 0.1.36,
//! `callsite.rs`, `Rebuilder::JustOne`). As a lib unit test, with a subscriber scoped to
//! its session's future, it missed its SUBSCRIBE lines about once in a thousand runs: a
//! `/mqtt` session in another test, on a thread with no subscriber, reached the callsite
//! first and cached it as never. Running the session once beforehand does not help, as
//! no callsite registers before the first `Dispatch` exists (the max level is off until
//! then). Here the capture is the global default, which every thread asks, and no other
//! test runs in the process.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use remarkable_server::mqtt_ws::{self, LOGGED_TOPIC_BYTES, MAX_MESSAGE_SIZE, MAX_SUBSCRIPTIONS};
use remarkable_server::{AppState, DeviceManager, Storage, create_router};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// SUBACK return code for a refused filter (MQTT 3.1.1 §3.9.3).
const SUBACK_FAILURE: u8 = 0x80;

/// Everything the process logs.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Log {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// An MQTT packet: fixed header byte `header`, remaining length (§2.2.3), `body`.
fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut p = vec![header];
    let mut len = body.len();
    loop {
        let digit = (len % 128) as u8;
        len /= 128;
        if len == 0 {
            p.push(digit);
            break;
        }
        p.push(digit | 0x80);
    }
    p.extend_from_slice(body);
    p
}

/// A SUBSCRIBE (packet id `id`) of `filters`, each at QoS 0.
fn subscribe<T: AsRef<str>>(id: u16, filters: impl IntoIterator<Item = T>) -> Message {
    let mut body = id.to_be_bytes().to_vec();
    for f in filters {
        let f = f.as_ref().as_bytes();
        body.extend_from_slice(&(f.len() as u16).to_be_bytes());
        body.extend_from_slice(f);
        body.push(0);
    }
    Message::Binary(packet(0x82, &body).into())
}

fn suback(id: u16, codes: &[u8]) -> Vec<u8> {
    packet(0x90, &[&id.to_be_bytes()[..], codes].concat())
}

/// Log volume is per SUBSCRIBE packet, not per filter, and a client's topics reach the
/// log cut short and escaped. A SUBSCRIBE is up to [`MAX_MESSAGE_SIZE`], room for tens of
/// thousands of filters or a few 64 KiB ones; logging each filter let one frame write
/// megabytes, enough for journald's per-service rate limit to drop the server's other
/// lines (tablet sync included). A raw newline in a topic would forge a log line.
#[tokio::test]
async fn a_subscribe_is_logged_once_whatever_its_filters() {
    let log = Log::default();
    let writer = log.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter("remarkable_server=info")
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish(),
    )
    .expect("the only subscriber in this process");

    let tmp = tempfile::TempDir::new().unwrap();
    let devices = DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
    let token = devices.create_user_token("u1@test").unwrap();
    let state = AppState::new(Storage::new(tmp.path().join("storage")).unwrap(), devices);
    let app = create_router(state.clone())
        .merge(mqtt_ws::router_if_enabled(state, Some("1")).expect("enabled"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut req = format!("ws://{}{}", listener.local_addr().unwrap(), mqtt_ws::PATH)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();

    let mut fill: Vec<String> = (0..MAX_SUBSCRIPTIONS - 2)
        .map(|i| format!("t{i}"))
        .collect();
    fill.push("evil\n2026-01-01T00:00:00Z  INFO forged".into());
    fill.push("L".repeat(5000)); // concrete, stored, too long to log whole
    let flood = (MAX_MESSAGE_SIZE - 8) / 4; // "z" repeated, each past the cap
    let wildcard = format!("#{}", "A".repeat(65_000));
    // CONNECT, MQTT 3.1.1, clean session, keepalive 60, client id "c"; the token is the
    // upgrade's header.
    let connect = [
        0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'c',
    ];
    for frame in [
        Message::Binary(connect.to_vec().into()),
        subscribe(1, &fill),
        subscribe(2, std::iter::repeat_n("z", flood)),
        subscribe(3, [&wildcard, &wildcard, &wildcard]),
    ] {
        ws.send(frame).await.unwrap();
    }

    // The CONNACK, then the SUBACKs, unchanged: all 64 stored, then every filter
    // refused. The catch-up PUBLISHes after the first are skipped.
    let mut written = Vec::new();
    while written.iter().filter(|p: &&Vec<u8>| p[0] == 0x90).count() < 3 {
        match tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("timed out")
        {
            Some(Ok(Message::Binary(b))) if b[0] != 0x30 => written.push(b.to_vec()),
            Some(Ok(Message::Binary(_))) => {}
            other => panic!("session ended early: {other:?}"),
        }
    }
    // Each SUBACK is written after its SUBSCRIBE's log line, so all three are in.
    let log = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert_eq!(
        written,
        [
            vec![0x20, 2, 0, 0],
            suback(1, &[0; MAX_SUBSCRIPTIONS]),
            suback(2, &vec![SUBACK_FAILURE; flood]),
            suback(3, &[SUBACK_FAILURE; 3]),
        ]
    );

    let lines: Vec<&str> = log.lines().collect();
    assert!(
        lines.iter().all(|l| l
            .split_whitespace()
            .nth(2)
            .is_some_and(|target| target.starts_with("remarkable_server::"))),
        "every line is a whole event of this server, none forged:\n{log}"
    );
    let subscribe_lines: Vec<&&str> = lines
        .iter()
        .filter(|l| l.contains("MQTT SUBSCRIBE"))
        .collect();
    assert_eq!(subscribe_lines.len(), 3, "one line per SUBSCRIBE:\n{log}");
    assert!(
        subscribe_lines[0].contains("stored=64") && subscribe_lines[0].contains("\\n2026"),
        "stored topics are listed, escaped: {}",
        subscribe_lines[0]
    );
    assert!(
        subscribe_lines[1].contains(&format!("refused_cap={flood}")),
        "{}",
        subscribe_lines[1]
    );
    assert!(
        subscribe_lines[2].contains("refused_wildcard=3"),
        "{}",
        subscribe_lines[2]
    );
    assert!(
        !log.contains(&"L".repeat(LOGGED_TOPIC_BYTES + 1))
            && !log.contains(&"A".repeat(LOGGED_TOPIC_BYTES + 1)),
        "topics are cut to {LOGGED_TOPIC_BYTES} bytes"
    );
    assert!(
        log.len() < 16 * 1024,
        "{} bytes logged for ~{} KiB of SUBSCRIBEs",
        log.len(),
        (MAX_MESSAGE_SIZE + 3 * 65_000) / 1024
    );
}
