//! Server-side document creation ("Read on reMarkable", desktop/web uploads).
//!
//! A document in the sync tree is a set of blobs (`<id>.metadata`, `<id>.content`,
//! `<id>.pdf|.epub`) listed in a document index, which is itself listed in the root
//! index. Index format (schema 3): first line `3`, then `hash:type:name:subfiles:size`.
//! An index's hash is sha256 over its entries' binary hashes, sorted by name
//! (verified against device-written trees).

use axum::body::Body;
use axum::extract::{Multipart, State};
use axum::http::{HeaderMap, StatusCode, header};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::api::AppState;
use crate::error::{Result, ServerError};
use crate::notifications::WsMessage;
use crate::storage::{Storage, is_valid_hash};

const SCHEMA: &str = "3";
const FILE_TYPE: &str = "0";
const DOC_TYPE: &str = "80000000";
/// Retries when another client moves the root while we're inserting.
const ROOT_RETRIES: usize = 5;

#[derive(Clone)]
struct Entry {
    hash: String,
    kind: String,
    name: String,
    subfiles: u64,
    size: u64,
}

impl Entry {
    /// Strict: exactly `hash:type:name:subfiles:size` with a valid hash and non-empty fields.
    fn parse(line: &str) -> Option<Self> {
        let mut f = line.split(':');
        let e = Self {
            hash: f.next().filter(|h| is_valid_hash(h))?.to_owned(),
            kind: f.next().filter(|k| !k.is_empty())?.to_owned(),
            name: f.next().filter(|n| !n.is_empty())?.to_owned(),
            subfiles: f.next()?.parse().ok()?,
            size: f.next()?.parse().ok()?,
        };
        if f.next().is_some() {
            return None;
        }
        Some(e)
    }

    /// The node id of a root entry; some trees name it `<id>.docSchema` (cf. gentree).
    fn id(&self) -> &str {
        self.name.strip_suffix(".docSchema").unwrap_or(&self.name)
    }
}

/// Parse the current root index for rewriting. Refuses anything not fully understood
/// (other schema, e.g. 4 with its `0:.:count:size` summary line whose hashing we haven't
/// verified; or any unparseable line): rewriting a root with lines dropped would delete
/// those documents from the tablet on its next sync.
fn parse_root(data: &[u8]) -> std::result::Result<Vec<Entry>, String> {
    let text = std::str::from_utf8(data).map_err(|_| "root index is not UTF-8".to_string())?;
    let mut lines = text.lines();
    match lines.next() {
        Some(SCHEMA) => {}
        other => {
            return Err(format!(
                "unsupported root index schema {other:?} (only {SCHEMA:?})"
            ));
        }
    }
    lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| Entry::parse(l).ok_or_else(|| format!("unparseable root index line {l:?}")))
        .collect()
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
        out.push_str(&format!(
            "{}:{}:{}:{}:{}\n",
            e.hash, e.kind, e.name, e.subfiles, e.size
        ));
    }
    out.into_bytes()
}

/// Store a leaf blob under its content hash and return its index entry.
fn put_leaf(storage: &Storage, name: String, data: &[u8]) -> Result<Entry> {
    let hash = hex::encode(Sha256::digest(data));
    storage.put_with_hash(data, &hash, &name)?;
    Ok(Entry {
        hash,
        kind: FILE_TYPE.into(),
        name,
        subfiles: 0,
        size: data.len() as u64,
    })
}

/// A new document's file: bytes in memory, or an upload streamed to a staged file
/// (staged with its sha256, which names the blob).
pub(crate) enum DocFile<'a> {
    Bytes(&'a [u8]),
    Staged(crate::upload::Staged),
}

impl DocFile<'_> {
    fn len(&self) -> u64 {
        match self {
            Self::Bytes(d) => d.len() as u64,
            Self::Staged(s) => s.len(),
        }
    }
}

/// [`put_leaf`] for a [`DocFile`]; a staged file is renamed into the store, not copied.
fn put_doc_file(storage: &Storage, name: String, file: DocFile<'_>) -> Result<Entry> {
    let staged = match file {
        DocFile::Bytes(data) => return put_leaf(storage, name, data),
        DocFile::Staged(staged) => staged,
    };
    let hash = staged
        .sha256_hex()
        .ok_or_else(|| ServerError::Internal("staged document has no sha256".into()))?
        .to_owned();
    let size = staged.commit(storage, &hash, &name)?;
    Ok(Entry {
        hash,
        kind: FILE_TYPE.into(),
        name,
        subfiles: 0,
        size,
    })
}

/// Map an upload content type to the document's file extension.
fn file_type(content_type: &str) -> Result<&'static str> {
    match content_type.split(';').next().unwrap_or_default().trim() {
        "application/pdf" => Ok("pdf"),
        "application/epub+zip" => Ok("epub"),
        other => Err(ServerError::Config(format!(
            "unsupported content type {other:?} (pdf or epub only)"
        ))),
    }
}

/// The current root and its entries, or an error if the root index isn't one we can safely rewrite.
fn current_root_entries(storage: &Storage) -> Result<(crate::types::SyncRoot, Vec<Entry>)> {
    let root = storage.get_root();
    if root.hash.is_empty() {
        return Ok((root, Vec::new()));
    }
    let entries = parse_root(&storage.get(&root.hash)?).map_err(|why| {
        tracing::error!(root = %root.hash, generation = root.generation, %why, "refusing to add document: root index not understood");
        ServerError::Internal(format!("refusing to modify root index: {why}"))
    })?;
    Ok((root, entries))
}

/// Add a new PDF/EPUB document at the top level and commit a new root. Returns the document id.
pub fn create_document(
    storage: &Storage,
    name: &str,
    ext: &str,
    data: &[u8],
) -> Result<(String, u64)> {
    create_document_in(storage, name, ext, data, "")
}

/// Like [`create_document`], but inside the collection `parent` (a CollectionType id; "" = top level).
pub fn create_document_in(
    storage: &Storage,
    name: &str,
    ext: &str,
    data: &[u8],
    parent: &str,
) -> Result<(String, u64)> {
    create_document_from(storage, name, ext, DocFile::Bytes(data), parent)
}

/// [`create_document_in`] for a [`DocFile`], so uploads needn't be held in memory.
pub(crate) fn create_document_from(
    storage: &Storage,
    name: &str,
    ext: &str,
    file: DocFile<'_>,
    parent: &str,
) -> Result<(String, u64)> {
    let id = uuid::Uuid::new_v4().to_string();
    // Refuse up front, before writing any blobs, so a root we won't rewrite doesn't leave
    // an orphaned document behind on every rejected upload. (Re-checked on each attempt below.)
    current_root_entries(storage)?;
    let entry = put_document(storage, &id, name, ext, file, parent)?;
    let generation = commit_to_root(storage, std::slice::from_ref(&entry))?;
    Ok((id, generation))
}

/// Store a new document's blobs (`<id>.metadata`, `<id>.content`, `<id>.<ext>`) and its index,
/// inside the collection `parent` ("" = top level); returns its root entry. Nothing refers to
/// the blobs until [`commit_to_root`] adds the entry.
fn put_document(
    storage: &Storage,
    id: &str,
    name: &str,
    ext: &str,
    file: DocFile<'_>,
    parent: &str,
) -> Result<Entry> {
    let now = chrono::Utc::now().timestamp_millis().to_string();
    let metadata = serde_json::json!({
        "createdTime": now, "lastModified": now, "lastOpened": "", "lastOpenedPage": 0,
        "parent": parent, "pinned": false, "type": "DocumentType", "visibleName": name,
    });
    let content = serde_json::json!({
        "fileType": ext, "coverPageNumber": 0, "extraMetadata": {}, "fontName": "",
        "lineHeight": -1, "margins": 125, "orientation": "portrait", "pageCount": 0,
        "pages": [], "textScale": 1,
    });
    let mut files = vec![
        put_leaf(
            storage,
            format!("{id}.metadata"),
            &serde_json::to_vec_pretty(&metadata)?,
        )?,
        put_leaf(
            storage,
            format!("{id}.content"),
            &serde_json::to_vec_pretty(&content)?,
        )?,
        put_doc_file(storage, format!("{id}.{ext}"), file)?,
    ];
    let doc_hash = index_hash(&mut files)?;
    storage.put_with_hash(&render_index(&files), &doc_hash, &format!("{id}.docSchema"))?;
    Ok(Entry {
        hash: doc_hash,
        kind: DOC_TYPE.into(),
        name: id.to_owned(),
        subfiles: files.len() as u64,
        size: files.iter().map(|f| f.size).sum(),
    })
}

/// Add `new` (document entries whose blobs are stored) to the current root in one commit and
/// return the new generation. Each attempt re-reads and strictly re-parses the root, so a
/// root that became one we won't rewrite is refused, and a root another client moved meanwhile
/// (generation mismatch) is retried on top of its new entries, up to [`ROOT_RETRIES`] times.
fn commit_to_root(storage: &Storage, new: &[Entry]) -> Result<u64> {
    for _ in 0..ROOT_RETRIES {
        let (root, mut entries) = current_root_entries(storage)?;
        entries.extend(new.iter().cloned());
        let root_hash = index_hash(&mut entries)?;
        storage.put_with_hash(&render_index(&entries), &root_hash, "root.docSchema")?;
        match storage.set_root_if(root_hash, Some(root.generation)) {
            Ok(new_root) => return Ok(new_root.generation),
            Err(ServerError::GenerationMismatch { .. }) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(ServerError::Internal(match new.len() {
        1 => "root kept changing while adding document".into(),
        n => format!("root kept changing while adding {n} documents"),
    }))
}

/// A new PDF/EPUB document for [`stage_documents`]: its visible name, file extension (`pdf`
/// or `epub`, also its `fileType`) and file.
pub struct NewDocument<'a> {
    pub name: &'a str,
    pub ext: &'a str,
    pub data: &'a [u8],
}

/// New documents whose blobs are stored, to be added to the root together by
/// [`commit`](Self::commit). Their ids are known before the commit, so a caller can record where
/// each is going first.
pub struct DocumentBatch {
    entries: Vec<Entry>,
}

impl DocumentBatch {
    /// The documents' ids, in the order they were staged.
    pub fn ids(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.name.clone()).collect()
    }

    /// Add every document to the root in one commit (one generation bump, whatever the number
    /// of documents), with the same strict parsing and retries as [`create_document_in`], and
    /// return the new generation. A root not fully understood is refused and left as it is.
    /// An empty batch commits nothing and returns the current generation.
    pub fn commit(self, storage: &Storage) -> Result<u64> {
        if self.entries.is_empty() {
            return Ok(storage.get_root().generation);
        }
        commit_to_root(storage, &self.entries)
    }
}

/// Store the blobs of `docs`, each a new document inside the collection `parent` ("" = top
/// level), for [`DocumentBatch::commit`] to add in one root commit. Like [`create_document_in`],
/// refuses a root it doesn't fully understand before writing any blob.
pub fn stage_documents(
    storage: &Storage,
    docs: &[NewDocument<'_>],
    parent: &str,
) -> Result<DocumentBatch> {
    current_root_entries(storage)?;
    let entries = docs
        .iter()
        .map(|doc| {
            let id = uuid::Uuid::new_v4().to_string();
            put_document(
                storage,
                &id,
                doc.name,
                doc.ext,
                DocFile::Bytes(doc.data),
                parent,
            )
        })
        .collect::<Result<_>>()?;
    Ok(DocumentBatch { entries })
}

/// The ids of the nodes (documents and collections) listed in the current root, read with the
/// same strict parsing as the document writers; an error for a root they would refuse.
pub fn root_node_ids(storage: &Storage) -> Result<std::collections::HashSet<String>> {
    let (_, entries) = current_root_entries(storage)?;
    Ok(entries.iter().map(|e| e.id().to_owned()).collect())
}

/// Resolve `folder` to a collection id: "" is the top level; otherwise a live
/// CollectionType whose id equals `folder`, else one whose visible name equals it
/// (top-level ones first). If none exists, a top-level collection named `folder`
/// is created. Names are matched whole; "/" is not treated as a path separator.
pub fn ensure_folder(storage: &Storage, folder: &str) -> Result<String> {
    if folder.is_empty() {
        return Ok(String::new());
    }
    for _ in 0..ROOT_RETRIES {
        let root = storage.get_root();
        let entries = root_entries(storage, &root.hash)?;
        let collections: Vec<(String, serde_json::Value)> = entries
            .iter()
            .filter(|e| e.kind == DOC_TYPE)
            .filter_map(|e| Some((e.id().to_owned(), node_metadata(storage, e)?)))
            .filter(|(_, m)| {
                m["type"] == "CollectionType" && m["deleted"] != true && m["parent"] != "trash"
            })
            .collect();
        if collections.iter().any(|(id, _)| id == folder) {
            return Ok(folder.to_owned());
        }
        let mut named: Vec<&(String, serde_json::Value)> = collections
            .iter()
            .filter(|(_, m)| m["visibleName"] == folder)
            .collect();
        named.sort_by_key(|(_, m)| m["parent"].as_str().unwrap_or_default() != "");
        if let Some((id, _)) = named.first() {
            return Ok(id.clone());
        }

        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis().to_string();
        let metadata = serde_json::json!({
            "createdTime": now, "lastModified": now, "parent": "", "pinned": false,
            "type": "CollectionType", "visibleName": folder,
        });
        let mut files = vec![
            put_leaf(
                storage,
                format!("{id}.metadata"),
                &serde_json::to_vec_pretty(&metadata)?,
            )?,
            put_leaf(storage, format!("{id}.content"), br#"{"tags": []}"#)?,
        ];
        let hash = index_hash(&mut files)?;
        storage.put_with_hash(&render_index(&files), &hash, &format!("{id}.docSchema"))?;
        let mut entries = entries;
        entries.push(Entry {
            hash,
            kind: DOC_TYPE.into(),
            name: id.clone(),
            subfiles: files.len() as u64,
            size: files.iter().map(|f| f.size).sum(),
        });
        let root_hash = index_hash(&mut entries)?;
        storage.put_with_hash(&render_index(&entries), &root_hash, "root.docSchema")?;
        match storage.set_root_if(root_hash, Some(root.generation)) {
            Ok(_) => {
                tracing::info!(%id, folder, "created collection");
                return Ok(id);
            }
            // Re-scan: whoever moved the root may have created the folder.
            Err(ServerError::GenerationMismatch { .. }) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(ServerError::Internal(
        "root kept changing while adding folder".into(),
    ))
}

fn root_entries(storage: &Storage, root_hash: &str) -> Result<Vec<Entry>> {
    if root_hash.is_empty() {
        return Ok(Vec::new());
    }
    // Same strict rule as create_document: never rewrite a root with lines we'd drop.
    parse_root(&storage.get(root_hash)?).map_err(|why| {
        tracing::error!(%root_hash, %why, "refusing to rewrite root index: not understood");
        ServerError::Internal(format!("refusing to modify root index: {why}"))
    })
}

/// A node's parsed `<id>.metadata`, if its index and metadata blob are readable.
fn node_metadata(storage: &Storage, node: &Entry) -> Option<serde_json::Value> {
    let index = storage.get(&node.hash).ok()?;
    let name = format!("{}.metadata", node.id());
    let meta = String::from_utf8_lossy(&index)
        .lines()
        .skip(1)
        .filter_map(Entry::parse)
        .find(|e| e.name == name)?;
    serde_json::from_slice(&storage.get(&meta.hash).ok()?).ok()
}

fn finish(
    state: &AppState,
    user_id: &str,
    name: &str,
    ext: &str,
    file: DocFile<'_>,
) -> Result<StatusCode> {
    let bytes = file.len();
    let (id, generation) = create_document_from(&state.storage, name, ext, file, "")?;
    tracing::info!(%id, name, ext, bytes, generation, "document uploaded");
    // Tell connected devices to pull the new root.
    let _ = state.notification_tx.send(WsMessage::sync_complete(
        generation,
        "local-server",
        user_id,
    ));
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct UploadMeta {
    file_name: String,
}

/// `POST /doc/v1/files`: multipart form with `meta` (JSON `{"file_name": ...}`) and `file`.
pub async fn upload_v1(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut form: Multipart,
) -> Result<StatusCode> {
    let user_id = state.auth_user(&headers)?;
    let (mut meta, mut file) = (None, None);
    while let Some(field) = form
        .next_field()
        .await
        .map_err(|e| ServerError::Config(e.to_string()))?
    {
        match field.name() {
            Some("meta") => {
                meta = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| ServerError::Config(e.to_string()))?,
                )
            }
            Some("file") => {
                let ct = field.content_type().unwrap_or_default().to_owned();
                // Streamed to disk (hashed on the way) rather than buffered.
                file = Some((
                    ct,
                    crate::upload::stage_field(&state.storage, field, true).await?,
                ));
            }
            _ => {}
        }
    }
    let meta: UploadMeta =
        serde_json::from_str(&meta.ok_or_else(|| ServerError::Config("missing 'meta'".into()))?)?;
    let (ct, data) = file.ok_or_else(|| ServerError::Config("missing 'file'".into()))?;
    finish(
        &state,
        &user_id,
        &meta.file_name,
        file_type(&ct)?,
        DocFile::Staged(data),
    )
}

/// `POST /doc/v2/files`: raw body, `rm-meta` header = base64 JSON `{"file_name": ...}`.
pub async fn upload_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode> {
    let user_id = state.auth_user(&headers)?;
    let meta = headers
        .get("rm-meta")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::MissingHeader("rm-meta".into()))?;
    let meta = STANDARD
        .decode(meta)
        .map_err(|_| ServerError::Config("rm-meta is not base64".into()))?;
    let meta: UploadMeta = serde_json::from_slice(&meta)?;
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::MissingHeader("content-type".into()))?;
    // Validated before the body is read, so an unsupported upload is never staged.
    let ext = file_type(ct)?;
    let staged = crate::upload::stage_body(
        &state.storage,
        &headers,
        body,
        crate::MAX_BLOB_BYTES as u64,
        true,
    )
    .await?;
    finish(
        &state,
        &user_id,
        &meta.file_name,
        ext,
        DocFile::Staged(staged),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(storage: &Storage, id: &str) -> serde_json::Value {
        let root = storage.get_root();
        let node = root_entries(storage, &root.hash)
            .unwrap()
            .into_iter()
            .find(|e| e.name == id)
            .expect("node in root");
        node_metadata(storage, &node).expect("metadata")
    }

    #[test]
    fn unparseable_root_is_never_rewritten() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let index = b"3\nnot a valid entry\n";
        let hash = "a".repeat(64);
        storage
            .put_with_hash(index, &hash, "root.docSchema")
            .unwrap();
        let before = storage.set_root(hash.clone()).unwrap();

        assert!(create_document(&storage, "a", "pdf", b"%PDF").is_err());
        assert!(ensure_folder(&storage, "News").is_err());
        let after = storage.get_root();
        assert_eq!((after.hash, after.generation), (hash, before.generation));
    }

    #[test]
    fn create_document_stays_top_level() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let (id, _) = create_document(&storage, "a", "pdf", b"%PDF").unwrap();
        assert_eq!(meta(&storage, &id)["parent"], "");
    }

    #[test]
    fn ensure_folder_creates_once_then_resolves_by_name_or_id() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        assert_eq!(ensure_folder(&storage, "").unwrap(), "");
        let folder = ensure_folder(&storage, "News").unwrap();
        let m = meta(&storage, &folder);
        assert_eq!(
            (
                m["type"].as_str(),
                m["visibleName"].as_str(),
                m["parent"].as_str()
            ),
            (Some("CollectionType"), Some("News"), Some(""))
        );
        let generation = storage.get_root().generation;
        assert_eq!(
            ensure_folder(&storage, "News").unwrap(),
            folder,
            "resolved by visible name"
        );
        assert_eq!(
            ensure_folder(&storage, &folder).unwrap(),
            folder,
            "resolved by id"
        );
        assert_eq!(
            storage.get_root().generation,
            generation,
            "no new folder committed"
        );

        let (doc, _) = create_document_in(&storage, "article", "epub", b"PK", &folder).unwrap();
        assert_eq!(meta(&storage, &doc)["parent"], folder.as_str());
        // A document with the folder's name isn't a folder.
        assert_ne!(ensure_folder(&storage, "article").unwrap(), doc);
    }

    #[test]
    fn ensure_folder_finds_collection_listed_with_docschema_suffix() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let id = "11111111-2222-3333-4444-555555555555";
        let metadata =
            serde_json::json!({"parent": "", "type": "CollectionType", "visibleName": "News"});
        let mut files = vec![
            put_leaf(
                &storage,
                format!("{id}.metadata"),
                &serde_json::to_vec(&metadata).unwrap(),
            )
            .unwrap(),
        ];
        let hash = index_hash(&mut files).unwrap();
        storage
            .put_with_hash(&render_index(&files), &hash, &format!("{id}.docSchema"))
            .unwrap();
        let mut entries = vec![Entry {
            hash,
            kind: DOC_TYPE.into(),
            name: format!("{id}.docSchema"),
            subfiles: 1,
            size: files[0].size,
        }];
        let root_hash = index_hash(&mut entries).unwrap();
        storage
            .put_with_hash(&render_index(&entries), &root_hash, "root.docSchema")
            .unwrap();
        let generation = storage.set_root(root_hash).unwrap().generation;

        assert_eq!(
            ensure_folder(&storage, "News").unwrap(),
            id,
            "resolved by visible name"
        );
        assert_eq!(ensure_folder(&storage, id).unwrap(), id, "resolved by id");
        assert_eq!(
            storage.get_root().generation,
            generation,
            "no duplicate folder committed"
        );
    }

    fn storage_with_root(index: &str) -> (Storage, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let root_hash = hex::encode(Sha256::digest(index.as_bytes()));
        storage
            .put_with_hash(index.as_bytes(), &root_hash, "root.docSchema")
            .unwrap();
        storage.set_root(root_hash.clone()).unwrap();
        (storage, root_hash, tmp)
    }

    fn entry_line(c: char, name: &str) -> String {
        format!("{}:{DOC_TYPE}:{name}:3:100", c.to_string().repeat(64))
    }

    #[test]
    fn refuses_root_with_unparseable_line() {
        let bad = [
            format!("3\n{}\nnot-an-entry\n", entry_line('a', "doc-a")),
            format!("4\n0:.:1:100\n{}\n", entry_line('a', "doc-a")),
            format!("3\n{}:extra\n", entry_line('a', "doc-a")),
            format!("3\n{}\n", entry_line('a', "doc-a").replacen('a', "A", 1)),
        ];
        for index in bad {
            let (storage, root_hash, _tmp) = storage_with_root(&index);
            let (before, blobs) = (storage.get_root(), storage.list_hashes().unwrap().len());
            assert!(
                create_document(&storage, "Book", "pdf", b"%PDF-1.4").is_err(),
                "{index:?}"
            );
            assert_eq!(
                storage.list_hashes().unwrap().len(),
                blobs,
                "rejected upload must not leave orphan blobs"
            );
            let after = storage.get_root();
            assert_eq!(
                (after.hash.as_str(), after.generation),
                (root_hash.as_str(), before.generation),
                "root must be unchanged"
            );
            assert_eq!(storage.get(&root_hash).unwrap(), index.as_bytes());
        }
    }

    #[test]
    fn adds_document_and_keeps_existing_entries() {
        let (a, b) = (entry_line('a', "doc-a"), entry_line('b', "doc-b"));
        let (storage, _, _tmp) = storage_with_root(&format!("3\n{a}\n{b}\n"));
        let gen = storage.get_root().generation;
        let (id, new_gen) = create_document(&storage, "Book", "pdf", b"%PDF-1.4").unwrap();
        assert_eq!(new_gen, gen + 1);
        let entries = parse_root(&storage.get(&storage.get_root().hash).unwrap()).unwrap();
        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names.len(), 3);
        assert!(
            names.contains(&"doc-a") && names.contains(&"doc-b") && names.contains(&id.as_str())
        );
        let text = String::from_utf8(storage.get(&storage.get_root().hash).unwrap()).unwrap();
        assert!(text.contains(&a) && text.contains(&b));
    }

    fn new_docs(names: &[&'static str]) -> Vec<NewDocument<'static>> {
        names
            .iter()
            .map(|&name| NewDocument {
                name,
                ext: "epub",
                data: b"PK",
            })
            .collect()
    }

    /// A batch of documents lands in one root commit (one generation bump), inside the folder,
    /// next to the entries already there, under the ids it was staged with.
    #[test]
    fn a_batch_is_added_in_one_commit() {
        let (a, b) = (entry_line('a', "doc-a"), entry_line('b', "doc-b"));
        let (storage, _, _tmp) = storage_with_root(&format!("3\n{a}\n{b}\n"));
        let folder = ensure_folder(&storage, "News").unwrap();
        let generation = storage.get_root().generation;

        let batch =
            stage_documents(&storage, &new_docs(&["One", "Two", "Three"]), &folder).unwrap();
        let ids = batch.ids();
        assert_eq!(
            storage.get_root().generation,
            generation,
            "staging commits nothing"
        );
        assert_eq!(batch.commit(&storage).unwrap(), generation + 1);

        assert_eq!(storage.get_root().generation, generation + 1);
        let listed = root_node_ids(&storage).unwrap();
        assert_eq!(listed.len(), 3 + 3, "{listed:?}");
        assert!(
            ["doc-a", "doc-b", folder.as_str()]
                .iter()
                .all(|id| listed.contains(*id))
        );
        for (id, name) in ids.iter().zip(["One", "Two", "Three"]) {
            assert!(listed.contains(id));
            let m = meta(&storage, id);
            assert_eq!(
                (m["visibleName"].as_str(), m["parent"].as_str()),
                (Some(name), Some(folder.as_str()))
            );
        }
        let text = String::from_utf8(storage.get(&storage.get_root().hash).unwrap()).unwrap();
        assert!(text.contains(&a) && text.contains(&b));

        let empty = stage_documents(&storage, &[], "").unwrap();
        assert_eq!(empty.commit(&storage).unwrap(), generation + 1);
        assert_eq!(storage.get_root().generation, generation + 1);
    }

    /// A batch is refused, before any blob is written, by a root the writers don't fully
    /// understand; and one that becomes such a root before the commit is refused then, and left
    /// as it is.
    #[test]
    fn a_batch_never_rewrites_a_root_it_does_not_understand() {
        let bad = format!("3\n{}\nnot-an-entry\n", entry_line('a', "doc-a"));
        let (storage, root_hash, _tmp) = storage_with_root(&bad);
        let (before, blobs) = (storage.get_root(), storage.list_hashes().unwrap().len());
        assert!(stage_documents(&storage, &new_docs(&["One", "Two"]), "").is_err());
        assert_eq!(
            storage.list_hashes().unwrap().len(),
            blobs,
            "no orphan blobs"
        );
        assert_eq!(storage.get_root().generation, before.generation);
        assert!(root_node_ids(&storage).is_err());

        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let batch = stage_documents(&storage, &new_docs(&["One", "Two"]), "").unwrap();
        storage
            .put_with_hash(bad.as_bytes(), &root_hash, "root.docSchema")
            .unwrap();
        let before = storage.set_root(root_hash.clone()).unwrap();
        assert!(batch.commit(&storage).is_err());
        let after = storage.get_root();
        assert_eq!(
            (after.hash, after.generation),
            (root_hash, before.generation)
        );
    }

    /// A root another client moved between staging and the commit is re-read: the commit lands
    /// on top of it, keeping what it added.
    #[test]
    fn a_batch_commit_keeps_what_landed_meanwhile() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let batch = stage_documents(&storage, &new_docs(&["One", "Two"]), "").unwrap();
        let (other, generation) = create_document(&storage, "Upload", "pdf", b"%PDF").unwrap();
        assert_eq!(batch.commit(&storage).unwrap(), generation + 1);
        let listed = root_node_ids(&storage).unwrap();
        assert_eq!(listed.len(), 3);
        assert!(listed.contains(&other));
    }

    #[test]
    fn creates_root_when_none_exists() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let (id, _) = create_document(&storage, "Book", "epub", b"PK").unwrap();
        let entries = parse_root(&storage.get(&storage.get_root().hash).unwrap()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, id);
    }
}
