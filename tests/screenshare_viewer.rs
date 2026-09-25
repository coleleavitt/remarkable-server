//! End to end: the server's screen share viewer negotiates with a fake tablet
//! through the in-process broker and turns its frames into PNGs.

use std::sync::Arc;
use std::time::Duration;

use remarkable_mqtt::screenshare::{signaling_topic, subscriptions};
use remarkable_mqtt::{PeerMessage, SignalingEvent, SignalingRequest, WebRtcMessage};
use remarkable_server::screenshare::{Broker, LocalClient};
use remarkable_server::screenshare_viewer::{ScreenViewer, Status, ViewerConfig};
use remarkable_server::DeviceManager;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

const USER: &str = "local-user";
const TABLET: &str = "tablet-1";
const W: u16 = 8;
const H: u16 = 4;

fn broker() -> (Broker, tempfile::TempDir) {
    // Both aws-lc-rs and ring are linked in; main() picks aws-lc-rs the same way.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let tmp = tempfile::tempdir().unwrap();
    let devices = DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
    (Broker::new(devices, serde_json::json!([])), tmp)
}

fn send(client: &LocalClient, request: &SignalingRequest) {
    client.publish(&signaling_topic(USER, client.client_id()), serde_json::to_vec(request).unwrap()).unwrap();
}

async fn next_event(client: &mut LocalClient) -> SignalingEvent {
    loop {
        let p = tokio::time::timeout(Duration::from_secs(10), client.recv()).await.expect("signaling timeout").unwrap();
        if let Some(event) = SignalingEvent::from_bytes(&p.payload) {
            return event;
        }
    }
}

/// Server messages the way xochitl writes them: handshake, then one full
/// update of black pixels in a partial-flushed zlib stream.
fn tablet_stream() -> Vec<u8> {
    let mut m = vec![0x68];
    for v in [2u16, W, H] {
        m.extend(v.to_be_bytes());
    }
    let mut raw = Vec::new();
    for v in [0u16, 0, W, H] {
        raw.extend(v.to_be_bytes());
    }
    raw.extend((u32::from(W) * u32::from(H) * 2).to_be_bytes());
    raw.resize(raw.len() + usize::from(W) * usize::from(H) * 2, 0);
    let mut z = flate2::Compress::new(flate2::Compression::default(), true);
    let mut zdata = Vec::with_capacity(raw.len() + 64);
    z.compress_vec(&raw, &mut zdata, flate2::FlushCompress::Partial).unwrap();
    m.push(0x00);
    m.extend(1u16.to_be_bytes());
    m.extend((zdata.len() as u32).to_be_bytes());
    m.extend(zdata);
    m
}

/// Plays the tablet: owns the room, answers request-offer with a WebRTC offer
/// and streams one frame once the viewer's handshake arrives.
async fn fake_tablet(broker: Broker) {
    let mut client = broker.local_client(USER, TABLET, &subscriptions(USER, TABLET));
    send(&client, &SignalingRequest::CreateRoom { room: String::new() });
    let room_id = match next_event(&mut client).await {
        SignalingEvent::RoomCreated { room_id, .. } => room_id,
        other => panic!("expected room-created, got {other:?}"),
    };

    let viewer = loop {
        match next_event(&mut client).await {
            SignalingEvent::Broadcast { payload: PeerMessage::RequestOffer { id }, .. } => break id,
            _ => {}
        }
    };

    let pc = Arc::new(APIBuilder::new().build().new_peer_connection(RTCConfiguration::default()).await.unwrap());
    let dc = pc.create_data_channel("screenshare", None).await.unwrap();
    let dc2 = Arc::clone(&dc);
    dc.on_message(Box::new(move |msg: DataChannelMessage| {
        let dc = Arc::clone(&dc2);
        Box::pin(async move {
            assert_eq!(&msg.data[..], b"reMarkable\x00\x02", "viewer handshake");
            dc.send(&bytes::Bytes::from(tablet_stream())).await.unwrap();
        })
    }));

    let offer = pc.create_offer(None).await.unwrap();
    let mut gathered = pc.gathering_complete_promise().await;
    pc.set_local_description(offer).await.unwrap();
    let _ = gathered.recv().await;
    let description = pc.local_description().await.unwrap().sdp;
    let direct = |payload| SignalingRequest::Direct {
        room_id: room_id.clone(),
        client_id: viewer.clone(),
        payload: PeerMessage::WebRtc { payload },
    };
    send(&client, &direct(WebRtcMessage::Offer { description }));

    loop {
        match next_event(&mut client).await {
            SignalingEvent::Direct { payload: PeerMessage::WebRtc { payload }, .. } => match payload {
                WebRtcMessage::Answer { description } => {
                    pc.set_remote_description(RTCSessionDescription::answer(description).unwrap()).await.unwrap();
                }
                WebRtcMessage::Candidate { candidate, mid } => {
                    let init = RTCIceCandidateInit { candidate, sdp_mid: mid, sdp_mline_index: Some(0), ..Default::default() };
                    let _ = pc.add_ice_candidate(init).await;
                }
                WebRtcMessage::Offer { .. } => {}
            },
            SignalingEvent::Broadcast { client_id, .. } => {
                assert_ne!(client_id, TABLET, "tablet received its own broadcast");
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn viewer_streams_frames_from_tablet() {
    let (broker, _tmp) = broker();
    let tablet = tokio::spawn(fake_tablet(broker.clone()));
    // Let the tablet create its room first.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let viewer = ScreenViewer::new(broker, ViewerConfig { user_id: USER.into(), transport: Default::default() });
    let mut watcher = viewer.watch();
    let png = tokio::time::timeout(Duration::from_secs(20), watcher.png.wait_for(Option::is_some))
        .await
        .expect("no frame from tablet")
        .unwrap()
        .clone()
        .unwrap();

    let info = png::Decoder::new(std::io::Cursor::new(png.to_vec())).read_info().unwrap().info().clone();
    assert_eq!((info.width, info.height), (u32::from(W), u32::from(H)));
    assert_eq!(*watcher.status.borrow(), Status::Streaming { width: u32::from(W), height: u32::from(H) });
    tablet.abort();
}

#[tokio::test]
async fn viewer_reports_when_screen_share_is_off() {
    let (broker, _tmp) = broker();
    let viewer = ScreenViewer::new(broker, ViewerConfig { user_id: USER.into(), transport: Default::default() });
    let mut watcher = viewer.watch();
    tokio::time::timeout(Duration::from_secs(10), watcher.status.wait_for(|s| *s == Status::NotSharing))
        .await
        .expect("viewer never reported not-sharing")
        .unwrap();
}

#[tokio::test]
async fn broadcast_is_not_echoed_to_sender() {
    let (broker, _tmp) = broker();
    let mut tablet = broker.local_client(USER, TABLET, &subscriptions(USER, TABLET));
    send(&tablet, &SignalingRequest::CreateRoom { room: String::new() });
    let room_id = match next_event(&mut tablet).await {
        SignalingEvent::RoomCreated { room_id, .. } => room_id,
        other => panic!("expected room-created, got {other:?}"),
    };
    let mut viewer = broker.local_client(USER, "viewer-1", &subscriptions(USER, "viewer-1"));
    send(&viewer, &SignalingRequest::JoinActiveRoom { room: String::new(), room_id: String::new() });
    assert!(matches!(next_event(&mut viewer).await, SignalingEvent::RoomJoined { .. }));

    send(&viewer, &SignalingRequest::Broadcast { room_id, payload: PeerMessage::RequestOffer { id: "viewer-1".into() } });
    assert!(matches!(next_event(&mut tablet).await, SignalingEvent::Broadcast { .. }));
    let echo = tokio::time::timeout(Duration::from_millis(300), viewer.recv()).await;
    assert!(echo.is_err(), "viewer received its own broadcast: {echo:?}");
}
