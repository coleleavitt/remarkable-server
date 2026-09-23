//! Server-side document creation ("Read on reMarkable", desktop/web uploads).
//!
//! A document in the sync tree is a set of blobs (`<id>.metadata`, `<id>.content`,
//! `<id>.pdf|.epub`) listed in a document index, which is itself listed in the root
//! index. Index format (schema 3): first line `3`, then `hash:type:name:subfiles:size`.
//! An index's hash is sha256 over its entries' binary hashes, sorted by name
//! (verified against device-written trees).

use axum::{body::Bytes, extract::{Multipart, State}, http::{header, HeaderMap, StatusCode}};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{api::AppState, error::{Result, ServerError}, notifications::WsMessage, storage::Storage};

const SCHEMA: &str = "3";
const FILE_TYPE: &str = "0";
const DOC_TYPE: &str = "80000000";
/// Retries when another client moves the root while we're inserting.
const ROOT_RETRIES: usize = 5;

struct Entry { hash: String, kind: String, name: String, subfiles: u64, size: u64 }

impl Entry {
    fn parse(line: &str) -> Option<Self> {
        let mut f = line.split(':');
        Some(Self {
            hash: f.next()?.to_owned(),
            kind: f.next()?.to_owned(),
            name: f.next()?.to_owned(),
            subfiles: f.next()?.parse().ok()?,
            size: f.next()?.parse().ok()?,
        })
    }
}

fn index_hash(entries: &mut [Entry]) -> Result<String> {
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let mut hasher = Sha256::new();
    for e in entries.iter() {
        hasher.update(hex::decode(&e.hash).map_err(|_| ServerError::InvalidHash(e.hash.clone()))?);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn render_index(entries: &[Entry]) -> Vec<u8> {
    let mut out = format!("{SCHEMA}\n");
    for e in entries {
        out.push_str(&format!("{}:{}:{}:{}:{}\n", e.hash, e.kind, e.name, e.subfiles, e.size));
    }
    out.into_bytes()
}

/// Store a leaf blob under its content hash and return its index entry.
fn put_leaf(storage: &Storage, name: String, data: &[u8]) -> Result<Entry> {
    let hash = hex::encode(Sha256::digest(data));
    storage.put_with_hash(data, &hash, &name)?;
    Ok(Entry { hash, kind: FILE_TYPE.into(), name, subfiles: 0, size: data.len() as u64 })
}

/// Map an upload content type to the document's file extension.
fn file_type(content_type: &str) -> Result<&'static str> {
    match content_type.split(';').next().unwrap_or_default().trim() {
        "application/pdf" => Ok("pdf"),
        "application/epub+zip" => Ok("epub"),
        other => Err(ServerError::Config(format!("unsupported content type {other:?} (pdf or epub only)"))),
    }
}

/// Add a new PDF/EPUB document at the top level and commit a new root. Returns the document id.
pub fn create_document(storage: &Storage, name: &str, ext: &str, data: &[u8]) -> Result<(String, u64)> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp_millis().to_string();
    let metadata = serde_json::json!({
        "createdTime": now, "lastModified": now, "lastOpened": "", "lastOpenedPage": 0,
        "parent": "", "pinned": false, "type": "DocumentType", "visibleName": name,
    });
    let content = serde_json::json!({
        "fileType": ext, "coverPageNumber": 0, "extraMetadata": {}, "fontName": "",
        "lineHeight": -1, "margins": 125, "orientation": "portrait", "pageCount": 0,
        "pages": [], "textScale": 1,
    });

    let mut files = vec![
        put_leaf(storage, format!("{id}.metadata"), &serde_json::to_vec_pretty(&metadata)?)?,
        put_leaf(storage, format!("{id}.content"), &serde_json::to_vec_pretty(&content)?)?,
        put_leaf(storage, format!("{id}.{ext}"), data)?,
    ];
    let doc_hash = index_hash(&mut files)?;
    storage.put_with_hash(&render_index(&files), &doc_hash, &format!("{id}.docSchema"))?;
    let doc_entry = || Entry {
        hash: doc_hash.clone(), kind: DOC_TYPE.into(), name: id.clone(),
        subfiles: files.len() as u64, size: files.iter().map(|f| f.size).sum(),
    };

    for _ in 0..ROOT_RETRIES {
        let root = storage.get_root();
        let mut entries: Vec<Entry> = if root.hash.is_empty() {
            Vec::new()
        } else {
            String::from_utf8_lossy(&storage.get(&root.hash)?).lines().skip(1).filter_map(Entry::parse).collect()
        };
        entries.push(doc_entry());
        let root_hash = index_hash(&mut entries)?;
        storage.put_with_hash(&render_index(&entries), &root_hash, "root.docSchema")?;
        match storage.set_root_if(root_hash, Some(root.generation)) {
            Ok(new_root) => return Ok((id, new_root.generation)),
            Err(ServerError::GenerationMismatch { .. }) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(ServerError::Internal("root kept changing while adding document".into()))
}

fn finish(state: &AppState, user_id: &str, name: &str, ext: &str, data: &[u8]) -> Result<StatusCode> {
    let (id, generation) = create_document(&state.storage, name, ext, data)?;
    tracing::info!(%id, name, ext, bytes = data.len(), generation, "document uploaded");
    // Tell connected devices to pull the new root.
    let _ = state.notification_tx.send(WsMessage::sync_complete(generation, "local-server", user_id));
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct UploadMeta { file_name: String }

/// `POST /doc/v1/files`: multipart form with `meta` (JSON `{"file_name": ...}`) and `file`.
pub async fn upload_v1(State(state): State<AppState>, headers: HeaderMap, mut form: Multipart) -> Result<StatusCode> {
    let user_id = state.auth_user(&headers)?;
    let (mut meta, mut file) = (None, None);
    while let Some(field) = form.next_field().await.map_err(|e| ServerError::Config(e.to_string()))? {
        match field.name() {
            Some("meta") => meta = Some(field.text().await.map_err(|e| ServerError::Config(e.to_string()))?),
            Some("file") => {
                let ct = field.content_type().unwrap_or_default().to_owned();
                file = Some((ct, field.bytes().await.map_err(|e| ServerError::Config(e.to_string()))?));
            }
            _ => {}
        }
    }
    let meta: UploadMeta = serde_json::from_str(&meta.ok_or_else(|| ServerError::Config("missing 'meta'".into()))?)?;
    let (ct, data) = file.ok_or_else(|| ServerError::Config("missing 'file'".into()))?;
    finish(&state, &user_id, &meta.file_name, file_type(&ct)?, &data)
}

/// `POST /doc/v2/files`: raw body, `rm-meta` header = base64 JSON `{"file_name": ...}`.
pub async fn upload_v2(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<StatusCode> {
    let user_id = state.auth_user(&headers)?;
    let meta = headers.get("rm-meta").and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("rm-meta".into()))?;
    let meta = STANDARD.decode(meta).map_err(|_| ServerError::Config("rm-meta is not base64".into()))?;
    let meta: UploadMeta = serde_json::from_slice(&meta)?;
    let ct = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).ok_or_else(|| ServerError::MissingHeader("content-type".into()))?;
    finish(&state, &user_id, &meta.file_name, file_type(ct)?, &body)
}
