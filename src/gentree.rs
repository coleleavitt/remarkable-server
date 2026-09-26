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
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use serde_json::{Value, json};

use crate::api::AppState;
use crate::error::{Result, ServerError};
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

/// `POST /gentree/v1/PutFile` req `{fileHash, filePath, sizeBytes, <blob>: b64}` -> stores the blob.
pub async fn put_file(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let file_hash = body
        .get("fileHash")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !is_valid_hash(file_hash) {
        return Err(ServerError::InvalidHash(file_hash.into()));
    }
    let b64 = body
        .get(BLOB_FIELD)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ServerError::MissingHeader(BLOB_FIELD.into()))?;
    let data = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|_| ServerError::MissingHeader(format!("{BLOB_FIELD} (base64)")))?;
    let file_path = body
        .get("filePath")
        .and_then(|v| v.as_str())
        .unwrap_or("gentree-blob");
    state.storage.put_with_hash(&data, file_hash, file_path)?;
    Ok(Json(
        json!({"fileHash": file_hash, "sizeBytes": data.len(), "state": "stored"}),
    ))
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

    #[tokio::test]
    async fn blob_roundtrip_and_session() {
        let (state, tk, _tmp) = setup();

        // PutFile then GetFile returns the same bytes
        let blob_hash = "a".repeat(64);
        let payload = b"hello gentree";
        let Json(r) = put_file(
            State(state.clone()),
            hdrs(&tk),
            Json(put_body(&blob_hash, "doc/1.rm", payload)),
        )
        .await
        .unwrap();
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
        put_file(
            State(state.clone()),
            hdrs(&tk),
            Json(put_body(
                &root_hash,
                "root.docSchema",
                root_index.as_bytes(),
            )),
        )
        .await
        .unwrap();

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
