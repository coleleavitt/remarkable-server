//! gentree/v1 delta sync for rm-sync (software 3.28+).
//!
//! rm-sync drives sync with an RPC-style API under `/gentree/v1/` (endpoints recovered
//! from the rm-sync initializer sub_38700 and its QString table). It also still speaks
//! `/sync/v3/*` and `/sync/v4/root`, which remain the reliable path. gentree is a
//! generation-based tree: `EntrySession` batches entry ops under an optimistic
//! generation lock, files move by hash via `PutFile`/`GetFile`.
//!
//! Confidence: endpoints/vocabulary HIGH; per-field shapes MEDIUM (reconstructed).
//! Untested against a real 3.28 device. Read paths reuse the existing content store.

use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::api::AppState;
use crate::error::{Result, ServerError};
use crate::json_scan::{Member, Scanned, Streamed};
use crate::storage::is_valid_hash;

/// JSON field carrying an inline blob body in PutFile.
const BLOB_FIELD: &str = "payload";

fn authz(h: &HeaderMap) -> Result<&str> {
    h.get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::Unauthorized)
}

struct IndexEntry {
    hash: String,
    id: String,
    size: u64,
}

/// Parse a sync index blob: first line is the schema version, then `hash:type:id:subfiles:size`.
fn parse_index(bytes: &[u8]) -> Vec<IndexEntry> {
    String::from_utf8_lossy(bytes)
        .lines()
        .skip(1)
        .filter_map(|l| {
            let mut it = l.split(':');
            let hash = it.next()?.to_string();
            let _ty = it.next()?;
            let id = it.next()?.to_string();
            let _sub = it.next();
            let size = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            if hash.is_empty() {
                None
            } else {
                Some(IndexEntry { hash, id, size })
            }
        })
        .collect()
}

fn req_hash<'a>(body: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|k| body.get(*k).and_then(|v| v.as_str()))
}

/// `POST /gentree/v1/GetEntries` -> the server's entry tree (one entry per document).
pub async fn get_entries(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let root = state.storage.get_root();
    let generation = root.generation;
    let mut entries = Vec::new();
    if !root.hash.is_empty() {
        if let Ok(idx) = state.storage.get(&root.hash) {
            for e in parse_index(&idx) {
                let uuid = e.id.strip_suffix(".docSchema").unwrap_or(&e.id).to_string();
                entries.push(json!({
                    "uuid": uuid, "hash": e.hash, "generation": generation,
                    "path": e.id, "sizeBytes": e.size, "deletedAt": Value::Null,
                }));
            }
        }
    }
    Ok(Json(json!({"library": {
        "generation": generation, "lastPurgeGeneration": 0, "entries": entries,
    }})))
}

/// `POST /gentree/v1/GetFiles` req `{"hash": <docIndexHash>}` -> the files listed in that index.
pub async fn get_files(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let h = req_hash(&body, &["hash", "fileHash"]).unwrap_or_default();
    if !is_valid_hash(h) {
        return Err(ServerError::InvalidHash(h.into()));
    }
    let idx = state.storage.get(h)?;
    let files: Vec<Value> = parse_index(&idx)
        .into_iter()
        .map(|e| json!({"fileHash": e.hash, "filePath": e.id, "sizeBytes": e.size}))
        .collect();
    Ok(Json(json!({"files": files})))
}

/// `POST /gentree/v1/GetFile` req `{"hash": <blobHash>}` -> the raw blob.
pub async fn get_file(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response> {
    state.auth_user(&headers)?;
    let h = req_hash(&body, &["hash", "fileHash"]).unwrap_or_default();
    if !is_valid_hash(h) {
        return Err(ServerError::InvalidHash(h.into()));
    }
    let data = state.storage.get(h)?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CONTENT_LENGTH, &data.len().to_string()),
        ],
        data,
    )
        .into_response())
}

/// PutFile members read besides the blob (indices into [`Scanned::captured`]).
const PUT_FIELDS: &[&str] = &["fileHash", "filePath"];
const FILE_HASH: usize = 0;
const FILE_PATH: usize = 1;

/// `POST /gentree/v1/PutFile` req `{fileHash, filePath, sizeBytes, <blob>: b64}` -> stores the blob.
///
/// The blob arrives base64 inside JSON. The body is read incrementally and the blob
/// decoded straight to a staged file as it arrives ([`crate::upload::stage_json_base64`]),
/// so neither a 1 GiB body nor its decoded blob is ever held in memory. Once the body is
/// in, the checks are those of the `Json<Value>` handler this replaced, in its order
/// (fileHash, blob present as a string, valid base64), with the same responses; only
/// auth now comes before the body rather than after it.
pub async fn put_file(State(state): State<AppState>, headers: HeaderMap, body: Body) -> Response {
    if let Some(rejection) = crate::upload::json_content_type_rejection(&headers).await {
        return rejection;
    }
    put_file_streamed(&state, &headers, body)
        .await
        .into_response()
}

async fn put_file_streamed(
    state: &AppState,
    headers: &HeaderMap,
    body: Body,
) -> Result<Json<Value>> {
    state.auth_user(headers)?;
    let (scanned, blob) = crate::upload::stage_json_base64(
        &state.storage,
        headers,
        body,
        crate::MAX_BLOB_BYTES as u64,
        BLOB_FIELD,
        PUT_FIELDS,
    )
    .await?;
    let file_hash = put_file_hash(&scanned)?;
    let blob = match (scanned.streamed, blob) {
        (Streamed::Text, Some(blob)) => blob,
        _ => return Err(ServerError::MissingHeader(BLOB_FIELD.into())),
    };
    let staged = blob
        .finish()
        .await?
        .ok_or_else(|| ServerError::MissingHeader(format!("{BLOB_FIELD} (base64)")))?;
    let file_path = put_file_path(&scanned)?;
    let size = staged.commit(&state.storage, &file_hash, &file_path)?;
    Ok(Json(
        json!({"fileHash": file_hash, "sizeBytes": size, "state": "stored"}),
    ))
}

/// `fileHash` as `body.get("fileHash").and_then(Value::as_str).unwrap_or_default()`,
/// which must be a valid blob hash.
fn put_file_hash(scanned: &Scanned) -> Result<String> {
    let hash = match &scanned.captured[FILE_HASH] {
        Member::TooLong => {
            return Err(ServerError::InvalidHash(format!(
                "(longer than {} bytes)",
                crate::upload::MAX_JSON_FIELD
            )));
        }
        m => m.as_str().unwrap_or_default().to_owned(),
    };
    if !is_valid_hash(&hash) {
        return Err(ServerError::InvalidHash(hash));
    }
    Ok(hash)
}

/// `filePath` as `body.get("filePath").and_then(Value::as_str).unwrap_or("gentree-blob")`.
fn put_file_path(scanned: &Scanned) -> Result<String> {
    match &scanned.captured[FILE_PATH] {
        Member::TooLong => Err(ServerError::PayloadTooLarge(format!(
            "filePath longer than {} bytes",
            crate::upload::MAX_JSON_FIELD
        ))),
        m => Ok(m.as_str().unwrap_or("gentree-blob").to_owned()),
    }
}

/// `POST /gentree/v1/DeleteEntry` req `{entryUuid}` -> tombstone acknowledgement.
pub async fn delete_entry(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let uuid = body
        .get("entryUuid")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Ok(Json(json!({"entryUuid": uuid, "state": "deleted"})))
}

/// `POST /gentree/v1/EntrySession` -> commit a batch of entry ops under an optimistic
/// generation lock. `preEntryGeneration` must equal the current generation (like sync v3
/// root CAS); `postEntryHash` is the already-uploaded new root index.
pub async fn entry_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>> {
    let (user_id, device_id, _) = state.devices.caller(authz(&headers)?)?;
    let session_id = body
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let pre_gen = body.get("preEntryGeneration").and_then(|v| v.as_u64());
    let post_hash = body
        .get("postEntryHash")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let generation = if is_valid_hash(&post_hash) {
        if !state.storage.exists(&post_hash) {
            return Err(ServerError::NotFound(format!(
                "root index {post_hash} not uploaded"
            )));
        }
        let root = state.storage.set_root_if(post_hash, pre_gen)?;
        let _ = state
            .notification_tx
            .send(crate::notifications::WsMessage::sync_complete(
                root.generation,
                &device_id,
                &user_id,
            ));
        root.generation
    } else {
        // No new root supplied: report current state (a no-op session).
        state.storage.get_root().generation
    };

    Ok(Json(json!({"session": {
        "sessionId": session_id, "state": "completed", "generation": generation,
    }})))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use base64::Engine;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    fn setup() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        (AppState::new(storage, devices), tk, tmp)
    }
    fn hdrs(tk: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {tk}")).unwrap(),
        );
        h
    }
    fn put_body(hash: &str, path: &str, data: &[u8]) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("fileHash".into(), Value::String(hash.into()));
        m.insert("filePath".into(), Value::String(path.into()));
        m.insert(
            "payload".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(data)),
        );
        Value::Object(m)
    }
    /// PutFile `body` (as JSON) straight to the handler; the response must be a 200.
    async fn put(state: &AppState, tk: &str, body: &Value) -> Value {
        let mut h = hdrs(tk);
        h.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let resp = put_file(State(state.clone()), h, Body::from(body.to_string())).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn blob_roundtrip_and_session() {
        let (state, tk, _tmp) = setup();

        // PutFile then GetFile returns the same bytes
        let blob_hash = "a".repeat(64);
        let payload = b"hello gentree";
        let r = put(&state, &tk, &put_body(&blob_hash, "doc/1.rm", payload)).await;
        assert_eq!(r["state"], "stored");
        let mut m = serde_json::Map::new();
        m.insert("hash".into(), Value::String(blob_hash.clone()));
        let resp = get_file(State(state.clone()), hdrs(&tk), Json(Value::Object(m)))
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], payload);

        // Upload a root index that references one document, then commit it via EntrySession
        let root_index = format!(
            "3\n{}:80000000:doc-abc.docSchema:1:{}\n",
            blob_hash,
            payload.len()
        );
        let root_hash = "b".repeat(64);
        put(
            &state,
            &tk,
            &put_body(&root_hash, "root.docSchema", root_index.as_bytes()),
        )
        .await;

        let mut s = serde_json::Map::new();
        s.insert("sessionId".into(), Value::String("sess-1".into()));
        s.insert("preEntryGeneration".into(), json!(0));
        s.insert("postEntryHash".into(), Value::String(root_hash.clone()));
        let Json(r) = entry_session(
            State(state.clone()),
            hdrs(&tk),
            Json(Value::Object(s.clone())),
        )
        .await
        .unwrap();
        assert_eq!(r["session"]["state"], "completed");
        assert_eq!(r["session"]["generation"], 1);

        // GetEntries now reports that one document
        let Json(e) = get_entries(State(state.clone()), hdrs(&tk)).await.unwrap();
        let entries = e["library"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["uuid"], "doc-abc");
        assert_eq!(entries[0]["hash"], blob_hash);
        assert_eq!(e["library"]["generation"], 1);

        // GetFiles on the root index lists the child file
        let mut gf = serde_json::Map::new();
        gf.insert("hash".into(), Value::String(root_hash.clone()));
        let Json(fr) = get_files(State(state.clone()), hdrs(&tk), Json(Value::Object(gf)))
            .await
            .unwrap();
        assert_eq!(fr["files"].as_array().unwrap()[0]["fileHash"], blob_hash);

        // A stale preEntryGeneration is rejected (optimistic lock -> 412)
        assert!(
            entry_session(State(state.clone()), hdrs(&tk), Json(Value::Object(s)))
                .await
                .is_err()
        );

        // Missing auth is rejected
        assert!(
            entry_session(State(state.clone()), HeaderMap::new(), Json(json!({})))
                .await
                .is_err()
        );
    }
}

/// Streaming PutFile against the buffered handler it replaced, through the real router.
#[cfg(test)]
mod put_file_tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::DefaultBodyLimit;
    use axum::http::Request;
    use axum::routing::post;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, TestRunner};
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    const URI: &str = "/gentree/v1/PutFile";

    /// The PutFile handler before streaming (`Json<Value>` body, whole-string decode),
    /// kept as the oracle for today's behaviour.
    async fn buffered_put_file(
        State(state): State<AppState>,
        headers: HeaderMap,
        Json(mut body): Json<Value>,
    ) -> Result<Json<Value>> {
        state.auth_user(&headers)?;
        let file_hash = body
            .get("fileHash")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        if !is_valid_hash(&file_hash) {
            return Err(ServerError::InvalidHash(file_hash));
        }
        let b64 = match body.get_mut(BLOB_FIELD).map(Value::take) {
            Some(Value::String(b64)) => b64,
            _ => return Err(ServerError::MissingHeader(BLOB_FIELD.into())),
        };
        let data = STANDARD
            .decode(&b64)
            .map_err(|_| ServerError::MissingHeader(format!("{BLOB_FIELD} (base64)")))?;
        let file_path = body
            .get("filePath")
            .and_then(|v| v.as_str())
            .unwrap_or("gentree-blob");
        state.storage.put_with_hash(&data, &file_hash, file_path)?;
        Ok(Json(
            json!({"fileHash": file_hash, "sizeBytes": data.len(), "state": "stored"}),
        ))
    }

    fn setup() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        (AppState::new(storage, devices), format!("Bearer {tk}"), tmp)
    }

    fn request(auth: &str, body: Body) -> Request<Body> {
        Request::post(URI)
            .header("authorization", auth)
            .header("content-type", "application/json")
            .body(body)
            .unwrap()
    }

    /// `doc` as a body arriving in frames cut at `cuts`.
    fn framed(doc: &[u8], cuts: &[usize]) -> Body {
        let mut cuts: Vec<usize> = cuts.iter().copied().filter(|&c| c <= doc.len()).collect();
        cuts.extend([0, doc.len()]);
        cuts.sort_unstable();
        cuts.dedup();
        let frames: Vec<std::io::Result<Bytes>> = cuts
            .windows(2)
            .map(|w| Ok(Bytes::copy_from_slice(&doc[w[0]..w[1]])))
            .collect();
        Body::from_stream(futures_util::stream::iter(frames))
    }

    async fn send(router: &Router, req: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
        let resp = router.clone().oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (parts.status, parts.headers, body)
    }

    fn staged_bytes(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    fn staging_empty(state: &AppState) -> bool {
        std::fs::read_dir(state.storage.staging_dir())
            .map(|d| d.count() == 0)
            .unwrap_or(true)
    }

    /// Names in the staging directory, sorted.
    fn staged_names(dir: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Staging-directory contents, one entry per frame the body was asked for.
    type Seen = Arc<Mutex<Vec<Vec<String>>>>;

    /// `frames` as a body that, whenever a frame is asked for (so once the previous one
    /// has been handled), records the staging directory's contents.
    fn watched(state: &AppState, frames: Vec<Vec<u8>>) -> (Body, Seen) {
        let dir = state.storage.staging_dir();
        let seen = Seen::default();
        let log = seen.clone();
        let stream = futures_util::stream::iter(frames).map(move |frame| {
            log.lock().unwrap().push(staged_names(&dir));
            Ok::<_, std::io::Error>(Bytes::from(frame))
        });
        (Body::from_stream(stream), seen)
    }

    /// JSON string literal for `s`, each char escaped or not as `choices` says: raw,
    /// short escape (`\/` included) or `\u` (surrogate pair past the BMP).
    fn render_str(s: &str, choices: &[u8]) -> String {
        let mut out = String::from("\"");
        for (i, c) in s.chars().enumerate() {
            let r = choices.get(i % choices.len().max(1)).copied().unwrap_or(0);
            let short = match c {
                '"' => Some("\\\""),
                '\\' => Some("\\\\"),
                '/' => Some("\\/"),
                '\u{8}' => Some("\\b"),
                '\u{c}' => Some("\\f"),
                '\n' => Some("\\n"),
                '\r' => Some("\\r"),
                '\t' => Some("\\t"),
                _ => None,
            };
            let must = matches!(c, '"' | '\\') || (c as u32) < 0x20;
            match (r % 6, short) {
                (0..=2, _) if !must => out.push(c),
                (3 | 4, Some(esc)) => out.push_str(esc),
                _ => {
                    let mut units = [0u16; 2];
                    for u in c.encode_utf16(&mut units) {
                        if r & 0x80 == 0 {
                            out.push_str(&format!("\\u{u:04x}"));
                        } else {
                            out.push_str(&format!("\\u{u:04X}"));
                        }
                    }
                }
            }
        }
        out.push('"');
        out
    }

    /// One PutFile member value: a string (escaped per `choices`) or other JSON.
    #[derive(Debug, Clone)]
    enum Val {
        Str(String),
        Raw(String),
    }

    fn any_json() -> impl Strategy<Value = String> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i32>().prop_map(Value::from),
            any::<f64>().prop_map(Value::from),
            "\\PC{0,6}".prop_map(Value::String),
        ];
        leaf.prop_recursive(3, 12, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{0,4}", inner), 0..4)
                    .prop_map(|kv| Value::Object(kv.into_iter().collect())),
            ]
        })
        .prop_map(|v| v.to_string())
    }

    fn member() -> impl Strategy<Value = (String, Val)> {
        let hash = prop_oneof![
            4 => "[0-9a-f]{64}",
            1 => "[0-9a-fA-F/]{62,65}",
        ];
        let blob = prop_oneof![
            6 => prop::collection::vec(any::<u8>(), 0..160).prop_map(|d| STANDARD.encode(d)),
            1 => "[A-Za-z0-9+/=]{0,12}",
            1 => "\\PC{0,4}",
        ];
        prop_oneof![
            3 => hash.prop_map(|h| ("fileHash".to_owned(), Val::Str(h))),
            3 => blob.prop_map(|b| ("payload".to_owned(), Val::Str(b))),
            2 => "(\\PC|/){0,16}".prop_map(|p| ("filePath".to_owned(), Val::Str(p))),
            1 => any::<u32>().prop_map(|n| ("sizeBytes".to_owned(), Val::Raw(n.to_string()))),
            1 => (
                prop_oneof![Just("fileHash"), Just("payload"), Just("filePath"), Just("other")],
                any_json()
            ).prop_map(|(k, v)| (k.to_owned(), Val::Raw(v))),
            1 => ("\\PC{0,6}", any_json()).prop_map(|(k, v)| (k, Val::Raw(v))),
        ]
    }

    #[derive(Debug, Clone)]
    struct Case {
        doc: Vec<u8>,
        cuts: Vec<usize>,
    }

    /// PutFile bodies: an object of random members in random order (duplicates
    /// included), random whitespace and escapes; sometimes another top-level value;
    /// sometimes damaged by a few byte edits. Split into random frames.
    fn case() -> impl Strategy<Value = Case> {
        // A well-formed request plus a few random members, in any order.
        let request = (
            "[0-9a-f]{64}",
            prop::collection::vec(any::<u8>(), 0..160),
            prop::option::of("(\\PC|/){0,16}"),
            prop::collection::vec(member(), 0..3),
        )
            .prop_flat_map(|(hash, data, path, mut members)| {
                members.push(("fileHash".to_owned(), Val::Str(hash)));
                members.push(("payload".to_owned(), Val::Str(STANDARD.encode(data))));
                if let Some(path) = path {
                    members.push(("filePath".to_owned(), Val::Str(path)));
                }
                Just(members).prop_shuffle()
            });
        let members = prop_oneof![1 => prop::collection::vec(member(), 0..6), 2 => request];
        let object = (
            members,
            prop::collection::vec(any::<u8>(), 1..32),
            prop::collection::vec(0usize..5, 1..16),
        )
            .prop_map(|(members, choices, ws)| {
                let sp = |i: usize| ["", " ", "\n", "\t ", "\r\n  "][ws[i % ws.len()]];
                let mut doc = format!("{}{{", sp(0));
                for (i, (k, v)) in members.iter().enumerate() {
                    if i > 0 {
                        doc.push(',');
                    }
                    let v = match v {
                        Val::Str(s) => render_str(s, &choices[i % choices.len()..]),
                        Val::Raw(r) => r.clone(),
                    };
                    let key = render_str(k, &choices[(i * 7) % choices.len()..]);
                    doc.push_str(&format!(
                        "{}{key}{}:{}{v}{}",
                        sp(i + 1),
                        sp(i + 2),
                        sp(i + 3),
                        sp(i + 4)
                    ));
                }
                doc.push_str(&format!("}}{}", sp(members.len() + 5)));
                doc.into_bytes()
            });
        let top = prop_oneof![9 => object, 1 => any_json().prop_map(String::into_bytes)];
        let edit = (
            any::<prop::sample::Index>(),
            0u8..4,
            prop::sample::select(b"\"\\{}[]:,u0e-. a=/\x00\xc3\xff".to_vec()),
        );
        (
            top,
            prop_oneof![3 => Just(Vec::new()), 1 => prop::collection::vec(edit, 1..3)],
            prop_oneof![
                6 => prop::collection::vec(0usize..600, 0..8),
                1 => Just((0..600).collect::<Vec<usize>>()),
            ],
        )
            .prop_map(|(mut doc, edits, cuts)| {
                for (at, op, byte) in edits {
                    let i = at.index(doc.len() + 1);
                    match op {
                        0 if i < doc.len() => {
                            doc.remove(i);
                        }
                        1 => doc.insert(i, byte),
                        2 if i < doc.len() => doc[i] = byte,
                        _ => doc.truncate(i),
                    }
                }
                Case { doc, cuts }
            })
    }

    /// Byte-for-byte the old handler's responses and stored blobs, whatever the body.
    #[test]
    fn streamed_put_file_matches_buffered_handler() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let auth = format!("Bearer {}", devices.create_user_token("u@test").unwrap());
        let old = AppState::new(
            Storage::new(tmp.path().join("old")).unwrap(),
            devices.clone(),
        );
        let new = AppState::new(Storage::new(tmp.path().join("new")).unwrap(), devices);
        let old_router = Router::new()
            .route(
                URI,
                post(buffered_put_file).layer(DefaultBodyLimit::max(crate::MAX_BLOB_BYTES)),
            )
            .with_state(old.clone());
        let new_router = crate::create_router(new.clone());

        let (ok, json_err) = (std::cell::Cell::new(0), std::cell::Cell::new(0));
        let mut runner = TestRunner::new(Config {
            cases: 512,
            failure_persistence: None,
            ..Config::default()
        });
        let result = runner.run(&case(), |Case { doc, cuts }| {
            rt.block_on(async {
                let (s_old, h_old, b_old) =
                    send(&old_router, request(&auth, Body::from(doc.clone()))).await;
                let (s_new, _, b_new) =
                    send(&new_router, request(&auth, framed(&doc, &cuts))).await;
                let shown = String::from_utf8_lossy(&doc).into_owned();
                prop_assert_eq!(s_new, s_old, "{:?}: {:?} vs {:?}", shown, b_new, b_old);
                if h_old[header::CONTENT_TYPE] == "application/json" {
                    // Success, or one of the handler's own errors: same body.
                    prop_assert_eq!(&b_new, &b_old, "{:?}", shown);
                } else {
                    // axum's JSON syntax rejection (plain text): ours is a 400 too.
                    json_err.set(json_err.get() + 1);
                    prop_assert!(String::from_utf8_lossy(&b_new).contains("bad_request"));
                }
                if s_old == StatusCode::OK {
                    ok.set(ok.get() + 1);
                    let r: Value = serde_json::from_slice(&b_old).unwrap();
                    let hash = r["fileHash"].as_str().unwrap();
                    prop_assert!(new.storage.get(hash).unwrap() == old.storage.get(hash).unwrap());
                    prop_assert_eq!(
                        new.storage.filename_for_hash(hash),
                        old.storage.filename_for_hash(hash)
                    );
                }
                prop_assert!(staging_empty(&new), "staged file left behind");
                Ok(())
            })
        });
        if let Err(e) = result {
            panic!("{e}");
        }
        // The generator must reach both stores and refusals.
        let (ok, json_err) = (ok.get(), json_err.get());
        assert!(ok > 100 && json_err > 50, "ok {ok}, json errors {json_err}");
    }

    #[tokio::test]
    async fn blob_decodes_while_the_body_is_still_arriving() {
        let (state, auth, _tmp) = setup();
        let data: Vec<u8> = (0..16 * 1024 * 1024u32)
            .map(|i| i.wrapping_mul(2_654_435_761).to_be_bytes()[0])
            .collect();
        let hash = hex::encode(Sha256::digest(&data));
        let b64 = STANDARD.encode(&data);
        let (first, rest) = b64.as_bytes().split_at(b64.len() / 2);
        // Blob first, fileHash after it: validated only once the body is in.
        let mut frames = vec![br#"{"filePath":"doc/big.rm","payload":""#.to_vec()];
        frames.extend(first.chunks(64 * 1024).map(<[u8]>::to_vec));
        let second_half = frames.len();
        frames.extend(rest.chunks(64 * 1024).map(<[u8]>::to_vec));
        let tail = format!(r#"","fileHash":"{hash}","sizeBytes":{}}}"#, data.len());
        frames.push(tail.into_bytes());
        // Bytes on disk in staging as each frame is asked for (the handler finishes with
        // one frame before asking for the next).
        let dir = state.storage.staging_dir();
        let on_disk = Arc::new(Mutex::new(Vec::new()));
        let log = on_disk.clone();
        let body = Body::from_stream(futures_util::stream::iter(frames).map(move |frame| {
            log.lock().unwrap().push(staged_bytes(&dir));
            Ok::<_, std::io::Error>(Bytes::from(frame))
        }));
        let router = crate::create_router(state.clone());
        let (status, _, body) = send(&router, request(&auth, body)).await;
        // When the second half of the blob was asked for, with the body still open, the
        // first half (8 MiB decoded, less what the write buffers and the decoder hold) was
        // already on disk: the body isn't buffered first.
        let seen = on_disk.lock().unwrap()[second_half];
        assert!(seen >= data.len() as u64 / 4, "{seen} bytes staged");
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"fileHash": hash, "sizeBytes": data.len(), "state": "stored"})
        );
        assert!(state.storage.get(&hash).unwrap() == data);
        assert_eq!(
            state.storage.filename_for_hash(&hash).as_deref(),
            Some("doc/big.rm")
        );
        assert!(staging_empty(&state));
    }

    #[tokio::test]
    async fn refusals_before_and_while_streaming() {
        let (state, auth, _tmp) = setup();
        let router = crate::create_router(state.clone());
        let hash = "e".repeat(64);
        let doc = format!(r#"{{"fileHash":"{hash}","payload":"QUJD"}}"#);

        // No JSON content type: axum's own 415, as the `Json` extractor gave.
        let (s, _, b) = send(
            &router,
            Request::post(URI)
                .header("authorization", &auth)
                .body(Body::from(doc.clone()))
                .unwrap(),
        )
        .await;
        assert_eq!(s, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            b,
            "Expected request with `Content-Type: application/json`".as_bytes()
        );

        // Unauthenticated: refused before the body is read, so nothing is staged.
        let (body, seen) = watched(&state, vec![doc.clone().into_bytes()]);
        let (s, _, _) = send(&router, request("Bearer nope", body)).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert!(seen.lock().unwrap().is_empty(), "body read before auth");
        assert!(staging_empty(&state));

        // Declared over the 1 GiB limit: 413 up front.
        let mut req = request(&auth, Body::from(doc.clone()));
        req.headers_mut().insert(
            header::CONTENT_LENGTH,
            (crate::MAX_BLOB_BYTES as u64 + 1).into(),
        );
        assert_eq!(send(&router, req).await.0, StatusCode::PAYLOAD_TOO_LARGE);

        // A filePath too long to hold: 413, nothing stored.
        let long = "p".repeat(crate::upload::MAX_JSON_FIELD + 1);
        let big = format!(r#"{{"fileHash":"{hash}","filePath":"{long}","payload":"QUJD"}}"#);
        assert_eq!(
            send(&router, request(&auth, Body::from(big))).await.0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert!(!state.storage.exists(&hash));

        // The client goes away mid-blob: 400, staged file removed.
        let frames: Vec<std::io::Result<Bytes>> = vec![
            Ok(Bytes::from(format!(r#"{{"fileHash":"{hash}","payload":""#))),
            Ok(Bytes::from(vec![b'Q'; 1024 * 1024])),
            Err(std::io::Error::other("client went away")),
        ];
        let body = Body::from_stream(futures_util::stream::iter(frames));
        assert_eq!(
            send(&router, request(&auth, body)).await.0,
            StatusCode::BAD_REQUEST
        );
        assert!(!state.storage.exists(&hash));
        assert!(staging_empty(&state));

        // A numeral too long to hold: 413.
        let digits = "1".repeat(crate::json_scan::MAX_NUMBER + 1);
        let body = format!(r#"{{"fileHash":"{hash}","sizeBytes":{digits},"payload":"QUJD"}}"#);
        assert_eq!(
            send(&router, request(&auth, Body::from(body))).await.0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        // A fileHash too long to hold: the 400 invalid_hash any bad hash gets.
        let long = "a".repeat(crate::upload::MAX_JSON_FIELD + 1);
        let body = format!(r#"{{"fileHash":"{long}","payload":"QUJD"}}"#);
        let (s, _, b) = send(&router, request(&auth, Body::from(body))).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(
            String::from_utf8_lossy(&b).contains("invalid_hash"),
            "{b:?}"
        );
        assert!(!state.storage.exists(&hash));
        assert!(staging_empty(&state));

        // Still fine afterwards.
        let (s, _, _) = send(&router, request(&auth, Body::from(doc))).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(state.storage.get(&hash).unwrap(), b"ABC");

        // A filePath of exactly the capture cap is still held and catalogued.
        let path = "p".repeat(crate::upload::MAX_JSON_FIELD);
        let other = "9".repeat(64);
        let body = format!(r#"{{"fileHash":"{other}","filePath":"{path}","payload":"QUJD"}}"#);
        let (s, _, b) = send(&router, request(&auth, Body::from(body))).await;
        assert_eq!(s, StatusCode::OK, "{b:?}");
        assert_eq!(
            state.storage.filename_for_hash(&other).as_deref(),
            Some(path.as_str())
        );
    }

    #[tokio::test]
    async fn body_limit_applies_while_streaming() {
        let (state, _auth, _tmp) = setup();
        let data: Vec<u8> = (0..6000u32).map(|i| (i * 7 % 251) as u8).collect();
        let doc = format!(
            r#"{{"fileHash":"{}","payload":"{}"}}"#,
            "f".repeat(64),
            STANDARD.encode(&data)
        );
        let start = doc.find(r#""payload":""#).unwrap() + r#""payload":""#.len();
        // Cut inside the blob, so it is being staged when the last frame trips the limit.
        let frames = |doc: &str| {
            let (a, b) = (start + 8, start + 4096);
            vec![
                doc.as_bytes()[..a].to_vec(),
                doc.as_bytes()[a..b].to_vec(),
                doc.as_bytes()[b..].to_vec(),
            ]
        };
        let (body, seen) = watched(&state, frames(&doc));
        let staged = crate::upload::stage_json_base64(
            &state.storage,
            &HeaderMap::new(),
            body,
            doc.len() as u64 - 1,
            BLOB_FIELD,
            PUT_FIELDS,
        )
        .await;
        assert!(matches!(staged, Err(ServerError::PayloadTooLarge(_))));
        // A staged file existed when the over-limit frame was asked for, and is gone.
        assert_eq!(
            seen.lock().unwrap()[2].len(),
            1,
            "{:?}",
            seen.lock().unwrap()
        );
        assert!(staging_empty(&state));

        let (body, _) = watched(&state, frames(&doc));
        let (scanned, blob) = crate::upload::stage_json_base64(
            &state.storage,
            &HeaderMap::new(),
            body,
            doc.len() as u64,
            BLOB_FIELD,
            PUT_FIELDS,
        )
        .await
        .unwrap();
        assert_eq!(scanned.streamed, Streamed::Text);
        let staged = blob.unwrap().finish().await.unwrap().unwrap();
        assert_eq!(staged.crc32c(), crc32c::crc32c(&data));
        drop(staged);
        assert!(staging_empty(&state));
    }

    /// `frames` through [`crate::upload::stage_json_base64`]; the staged blob, committed
    /// as `hash`, and what the staging directory held as each frame was asked for.
    async fn stage_frames(state: &AppState, frames: Vec<Vec<u8>>, hash: &str) -> Vec<Vec<String>> {
        let (body, seen) = watched(state, frames);
        let (scanned, blob) = crate::upload::stage_json_base64(
            &state.storage,
            &HeaderMap::new(),
            body,
            crate::MAX_BLOB_BYTES as u64,
            BLOB_FIELD,
            PUT_FIELDS,
        )
        .await
        .unwrap();
        assert_eq!(scanned.streamed, Streamed::Text);
        let staged = blob.unwrap().finish().await.unwrap().unwrap();
        // Exactly one staged file remains: the blob's.
        assert_eq!(staged_names(&state.storage.staging_dir()).len(), 1);
        staged.commit(&state.storage, hash, "dup").unwrap();
        assert!(staging_empty(state));
        Arc::try_unwrap(seen).unwrap().into_inner().unwrap()
    }

    #[tokio::test]
    async fn duplicate_blob_keys_reuse_the_staged_file() {
        let (state, _auth, _tmp) = setup();

        // Many short duplicates, one per frame: the last wins, and they all share the
        // first staged file instead of creating and deleting one each.
        let mut frames = vec![br#"{"payload":"QUJD""#.to_vec()];
        frames.extend(std::iter::repeat_n(br#","payload":"WFla""#.to_vec(), 200));
        frames.push(br#","payload":"REVG"}"#.to_vec());
        let hash = "1".repeat(64);
        let seen = stage_frames(&state, frames, &hash).await;
        assert!(seen[0].is_empty());
        let files: std::collections::BTreeSet<&String> = seen[1..].iter().flatten().collect();
        assert_eq!(files.len(), 1, "{files:?}");
        assert!(seen[1..].iter().all(|names| names.len() == 1));
        assert_eq!(state.storage.get(&hash).unwrap(), b"DEF");

        // An occurrence long enough to have been written is replaced by a new file (the
        // old one removed), and a later short one still wins.
        let big: Vec<u8> = (0..200_000u32).map(|i| (i * 13 % 251) as u8).collect();
        let frames = vec![
            format!(r#"{{"payload":"{}""#, STANDARD.encode(&big)).into_bytes(),
            br#","payload":"QUJD""#.to_vec(),
            br#"}"#.to_vec(),
        ];
        let hash = "2".repeat(64);
        let seen = stage_frames(&state, frames, &hash).await;
        assert_eq!(seen[1].len(), 1);
        assert_eq!(seen[2].len(), 1);
        assert_ne!(
            seen[1], seen[2],
            "the written file should have been replaced"
        );
        assert_eq!(state.storage.get(&hash).unwrap(), b"ABC");

        // And the other way round: a short occurrence, then a long one.
        let frames = vec![
            br#"{"payload":"QUJD""#.to_vec(),
            format!(r#","payload":"{}"}}"#, STANDARD.encode(&big)).into_bytes(),
        ];
        let hash = "3".repeat(64);
        stage_frames(&state, frames, &hash).await;
        assert!(state.storage.get(&hash).unwrap() == big);
    }
}
