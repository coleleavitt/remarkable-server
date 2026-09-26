//! Content-addressed blob storage with a SQLite sync index.
//!
//! ```text
//! storage/
//!   ab/
//!     abcd1234...  blob bytes, named by hash (2-char prefix directory)
//!   sync.db        root pointer, blob metadata, parsed index tree (source of truth)
//!   root.json      mirror of the root row for humans and rollback; not read once sync.db exists
//!   meta/          legacy `{hash}.meta` filename files; imported once, no longer written
//! ```
//!
//! Index blobs (the root index and each `.docSchema`) are served back byte-for-byte:
//! the `indexes`/`entries` tables are a projection parsed from them, never what the
//! tablet reads. A format the parser doesn't know is recorded as unparsed and sync
//! carries on unaffected.

use crate::error::{Result, ServerError};
use crate::types::SyncRoot;
use parking_lot::{Mutex, RwLock};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Monotonic counter for unique temp-file names within this process.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Bump when `parse_index` learns a new format so stored "unparsed" verdicts are retried.
const PARSER_VERSION: i64 = 1;
/// Index entry type of a leaf file; anything else (e.g. `80000000`, a document) is an index.
const FILE_KIND: &str = "0";

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS root (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    hash           TEXT    NOT NULL,
    generation     INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    previous_hash  TEXT    NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS blobs (
    hash       TEXT    PRIMARY KEY,
    filename   TEXT    NOT NULL,
    size       INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS blobs_filename ON blobs(filename);
CREATE TABLE IF NOT EXISTS indexes (
    hash           TEXT    PRIMARY KEY,
    schema         TEXT,
    ok             INTEGER NOT NULL,
    error          TEXT,
    parser_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS entries (
    index_hash TEXT    NOT NULL REFERENCES indexes(hash) ON DELETE CASCADE,
    name       TEXT    NOT NULL,
    hash       TEXT    NOT NULL,
    kind       TEXT    NOT NULL,
    subfiles   INTEGER NOT NULL,
    size       INTEGER NOT NULL,
    PRIMARY KEY (index_hash, name)
);
CREATE INDEX IF NOT EXISTS entries_hash ON entries(hash);
";

/// Write `data` to `path` atomically: write to a temp file in the *same*
/// directory (rename is only atomic within one filesystem), fsync it so the
/// bytes are durable, then rename it over `path`.
///
/// A crash may leave the old file or a stray temp file, but never a
/// half-written `path`.
fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("tmp");
    let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.tmp.{}.{seq}", std::process::id()));

    let write = || -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?; // durable on disk before we rename over the target
        Ok(())
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    // Best-effort: fsync the directory so the rename itself survives a crash.
    // Not all platforms permit opening a directory for fsync; ignore failures.
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Blob hashes are 64-char lowercase hex (sha256). Anything else is rejected
/// before it can be used as a path component.
pub fn is_valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn unix_now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

/// One line of a sync index: `hash:type:name:subfiles:size`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub hash: String,
    pub kind: String,
    pub name: String,
    pub subfiles: u64,
    pub size: u64,
}

impl IndexEntry {
    /// Whether this entry points at another index (a document) rather than a leaf file.
    pub fn is_index(&self) -> bool {
        self.kind != FILE_KIND || self.subfiles > 0
    }
}

/// Parse an index blob strictly: schema `3` or `4`, then `hash:type:name:subfiles:size`
/// lines. Schema 4's summary line (a field that is just `.`, e.g. `0:.:<count>:<size>`)
/// is skipped. Any other shape is an error, so the projection is never partially wrong.
pub fn parse_index(bytes: &[u8]) -> std::result::Result<(String, Vec<IndexEntry>), String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "index is not UTF-8".to_string())?;
    let mut lines = text.lines();
    let schema = lines.next().ok_or("empty index")?.trim().to_string();
    if schema != "3" && schema != "4" {
        return Err(format!("unsupported index schema {schema:?}"));
    }
    let mut entries = Vec::new();
    for line in lines.filter(|l| !l.trim().is_empty()) {
        let fields: Vec<&str> = line.split(':').collect();
        if schema == "4" && fields.contains(&".") {
            continue;
        }
        let [hash, kind, name, subfiles, size] = fields[..] else {
            return Err(format!("malformed index line {line:?}"));
        };
        if !is_valid_hash(hash) {
            return Err(format!("invalid hash in index line {line:?}"));
        }
        let number = |s: &str| s.parse::<u64>().map_err(|_| format!("bad number in index line {line:?}"));
        entries.push(IndexEntry {
            hash: hash.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            subfiles: number(subfiles)?,
            size: number(size)?,
        });
    }
    Ok((schema, entries))
}

/// Child hashes of an index blob, read leniently (any first line, first field of each
/// line if it is a hash). Used where the strict parser gave up.
fn lenient_children(bytes: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .skip(1)
        .filter_map(|l| l.split(':').next())
        .filter(|h| is_valid_hash(h))
        .map(str::to_owned)
        .collect()
}

/// Hash-based storage backend
#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    /// Base directory for storage
    base_path: PathBuf,
    db: Mutex<Connection>,
    /// Last root read from or written to the database; served if a read fails.
    root: RwLock<SyncRoot>,
}

impl Storage {
    /// Open storage at `path`, creating `sync.db` and importing legacy `root.json` /
    /// `meta/` on first run. Every open reconciles the blob table with the files on disk.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;

        let conn = Connection::open(base_path.join("sync.db"))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(SCHEMA)?;

        let root = Self::load_or_import_root(&conn, &base_path)?;
        let root = Self::adopt_newer_root_json(&conn, &base_path, root)?;
        let storage = Self {
            inner: Arc::new(StorageInner { base_path, db: Mutex::new(conn), root: RwLock::new(root.clone()) }),
        };
        storage.reconcile_blobs()?;
        if let Err(e) = storage.index_tree(&root.hash) {
            tracing::warn!(error = %e, "could not index the current sync tree");
        }
        Ok(storage)
    }

    /// The root row, created from `root.json` (or empty) the first time.
    fn load_or_import_root(conn: &Connection, base_path: &Path) -> Result<SyncRoot> {
        let exists: Option<i64> = conn.query_row("SELECT id FROM root WHERE id = 1", [], |r| r.get(0)).optional()?;
        if exists.is_some() {
            return Self::select_root(conn);
        }
        let root = Self::read_root_json(base_path)?.unwrap_or_else(SyncRoot::empty);
        // Another process may be opening the same fresh directory; the first insert wins.
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO root (id, hash, generation, schema_version) VALUES (1, ?1, ?2, ?3)",
            params![root.hash, root.generation as i64, root.schema_version],
        )?;
        if inserted == 1 && !root.hash.is_empty() {
            tracing::info!(generation = root.generation, "imported root.json into sync.db");
        }
        Self::select_root(conn)
    }

    fn select_root(conn: &Connection) -> Result<SyncRoot> {
        Ok(conn.query_row("SELECT hash, generation, schema_version FROM root WHERE id = 1", [], |r| {
            Ok(SyncRoot { hash: r.get(0)?, generation: r.get::<_, i64>(1)? as u64, schema_version: r.get(2)? })
        })?)
    }

    fn read_root_json(base_path: &Path) -> Result<Option<SyncRoot>> {
        let root_path = base_path.join("root.json");
        if !root_path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&fs::read_to_string(&root_path)?)?))
    }

    /// `root.json` is only a mirror, so it can't be *ahead* of `sync.db` unless a build
    /// without `sync.db` committed roots here (a rollback). Adopt that root rather than
    /// serve an older tree, which would make the tablet drop what it synced meanwhile.
    fn adopt_newer_root_json(conn: &Connection, base_path: &Path, db_root: SyncRoot) -> Result<SyncRoot> {
        let Some(mirror) = Self::read_root_json(base_path)? else { return Ok(db_root) };
        if mirror.generation > db_root.generation {
            tracing::warn!(
                db_generation = db_root.generation, json_generation = mirror.generation,
                "root.json is ahead of sync.db (rolled back to an older build?); adopting root.json"
            );
            conn.execute(
                "UPDATE root SET hash = ?1, generation = ?2, schema_version = ?3, previous_hash = hash WHERE id = 1 AND generation = ?4",
                params![mirror.hash, mirror.generation as i64, mirror.schema_version, db_root.generation as i64],
            )?;
            return Self::select_root(conn);
        }
        if mirror.generation == db_root.generation && mirror.hash != db_root.hash && !mirror.hash.is_empty() {
            return Err(ServerError::Config(format!(
                "root.json and sync.db disagree at generation {} ({} vs {}); fix one by hand before starting",
                db_root.generation, mirror.hash, db_root.hash
            )));
        }
        Ok(db_root)
    }

    /// Make the `blobs` table match the blob files on disk: add rows for files it
    /// doesn't know (named from a legacy `meta/{hash}.meta` if there is one) and drop
    /// rows whose file is gone. Blob files are the truth; the table is their catalogue.
    fn reconcile_blobs(&self) -> Result<()> {
        let on_disk = self.scan_disk()?;
        let meta_dir = self.inner.base_path.join("meta");
        let mut db = self.inner.db.lock();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let known: HashSet<String> = {
            let mut stmt = tx.prepare("SELECT hash FROM blobs")?;
            let rows = stmt.query_map([], |r| r.get(0))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let (mut added, mut dropped) = (0usize, 0usize);
        for (hash, (size, mtime)) in &on_disk {
            if known.contains(hash) {
                continue;
            }
            let filename = fs::read_to_string(meta_dir.join(format!("{hash}.meta")))
                .map(|s| s.trim().to_string())
                .unwrap_or_default(); // unknown name, as with the old meta-less blobs
            tx.execute(
                "INSERT INTO blobs (hash, filename, size, updated_at) VALUES (?1, ?2, ?3, ?4)",
                params![hash, filename, *size as i64, mtime],
            )?;
            added += 1;
        }
        // Re-check: another process may have written the file after our scan.
        for hash in known.iter().filter(|h| !on_disk.contains_key(*h) && !self.hash_path(h).exists()) {
            tx.execute("DELETE FROM blobs WHERE hash = ?1", [hash])?;
            dropped += 1;
        }
        tx.commit()?;
        if added + dropped > 0 {
            tracing::info!(added, dropped, "reconciled blob table with storage directory");
        }
        Ok(())
    }

    /// Every blob file on disk with its size and modification time (unix seconds).
    fn scan_disk(&self) -> Result<HashMap<String, (u64, i64)>> {
        let mut blobs = HashMap::new();
        for entry in fs::read_dir(&self.inner.base_path)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.len() != 2 || !entry.file_type()?.is_dir() {
                continue;
            }
            for file in fs::read_dir(entry.path())? {
                let file = file?;
                let Some(hash) = file.file_name().to_str().map(str::to_owned) else { continue };
                if !is_valid_hash(&hash) || !hash.starts_with(&*name.to_string_lossy()) {
                    continue; // a temp file left by a crash mid-write, or a misplaced file get() can't serve
                }
                let meta = file.metadata()?;
                let mtime = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map_or_else(unix_now, |d| d.as_secs() as i64);
                blobs.insert(hash, (meta.len(), mtime));
            }
        }
        Ok(blobs)
    }

    /// Directory holding the blobs (and server-side indexes kept alongside them).
    pub fn base_path(&self) -> &Path {
        &self.inner.base_path
    }

    /// Get current root
    pub fn get_root(&self) -> SyncRoot {
        let row = self.inner.db.lock().query_row(
            "SELECT hash, generation, schema_version FROM root WHERE id = 1",
            [],
            |r| Ok(SyncRoot { hash: r.get(0)?, generation: r.get::<_, i64>(1)? as u64, schema_version: r.get(2)? }),
        );
        match row {
            Ok(root) => {
                *self.inner.root.write() = root.clone();
                root
            }
            Err(e) => {
                tracing::error!(error = %e, "reading root from sync.db failed; serving last known root");
                self.inner.root.read().clone()
            }
        }
    }

    /// Set new root hash and increment generation
    pub fn set_root(&self, hash: String) -> Result<SyncRoot> {
        self.set_root_if(hash, None)
    }

    /// Set the root hash only if the current generation equals `expected`
    /// (GCS `x-goog-if-generation-match` semantics). `None` skips the check.
    ///
    /// The check and the write are one SQLite transaction, so they hold across
    /// processes sharing the storage directory, not just within this one.
    pub fn set_root_if(&self, hash: String, expected: Option<u64>) -> Result<SyncRoot> {
        let root = {
            let mut db = self.inner.db.lock();
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (generation, schema_version): (i64, u32) =
                tx.query_row("SELECT generation, schema_version FROM root WHERE id = 1", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
            let generation = generation as u64;
            if expected.is_some_and(|expected| expected != generation) {
                return Err(ServerError::GenerationMismatch { current: generation });
            }
            let root = SyncRoot { hash, generation: generation + 1, schema_version };
            tx.execute(
                "UPDATE root SET previous_hash = hash, hash = ?1, generation = ?2 WHERE id = 1",
                params![root.hash, root.generation as i64],
            )?;
            tx.commit()?;
            self.write_mirror(&root);
            root
        };
        *self.inner.root.write() = root.clone();

        if let Err(e) = self.index_tree(&root.hash) {
            tracing::warn!(error = %e, hash = %root.hash, "root committed but its tree could not be indexed");
        }
        Ok(root)
    }

    /// Mirror the root to `root.json` for humans and for rolling back to a build without
    /// sync.db. Written under the db lock, and never over a newer generation another
    /// process already mirrored.
    fn write_mirror(&self, root: &SyncRoot) {
        let path = self.inner.base_path.join("root.json");
        if Self::read_root_json(&self.inner.base_path).ok().flatten().is_some_and(|m| m.generation > root.generation) {
            return;
        }
        let written = serde_json::to_string_pretty(root).map_err(std::io::Error::other).and_then(|m| atomic_write(&path, m.as_bytes()));
        if let Err(e) = written {
            tracing::warn!(error = %e, "root committed but root.json mirror not updated");
        }
    }

    /// Get file path for a hash (2-char prefix directory)
    fn hash_path(&self, hash: &str) -> PathBuf {
        if hash.len() < 2 {
            return self.inner.base_path.join(hash);
        }
        let prefix = &hash[..2];
        self.inner.base_path.join(prefix).join(hash)
    }

    /// Check if a hash exists
    pub fn exists(&self, hash: &str) -> bool {
        is_valid_hash(hash) && self.hash_path(hash).exists()
    }

    /// Get file by hash
    pub fn get(&self, hash: &str) -> Result<Vec<u8>> {
        if !is_valid_hash(hash) {
            return Err(ServerError::InvalidHash(hash.to_string()));
        }
        let path = self.hash_path(hash);
        if !path.exists() {
            return Err(ServerError::NotFound(hash.to_string()));
        }
        Ok(fs::read(path)?)
    }

    /// Get file by filename (the most recently stored blob with that name)
    pub fn get_by_filename(&self, filename: &str) -> Result<Vec<u8>> {
        let hash = self.hash_for_filename(filename).ok_or_else(|| ServerError::NotFound(filename.to_string()))?;
        self.get(&hash)
    }

    /// Get hash for filename
    pub fn hash_for_filename(&self, filename: &str) -> Option<String> {
        self.inner
            .db
            .lock()
            .query_row(
                "SELECT hash FROM blobs WHERE filename = ?1 ORDER BY updated_at DESC, rowid DESC LIMIT 1",
                [filename],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "blob lookup by filename failed");
                None
            })
    }

    /// Get filename for a given hash (reverse lookup)
    pub fn filename_for_hash(&self, hash: &str) -> Option<String> {
        self.inner
            .db
            .lock()
            .query_row("SELECT filename FROM blobs WHERE hash = ?1 AND filename != ''", [hash], |r| r.get(0))
            .optional()
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "blob lookup by hash failed");
                None
            })
    }

    /// Store file and return its hash
    pub fn put(&self, data: &[u8], filename: &str) -> Result<String> {
        let hash = hex::encode(Sha256::digest(data));
        self.put_with_hash(data, &hash, filename)?;
        Ok(hash)
    }

    /// Store a blob under the client-supplied hash.
    ///
    /// The hash is not recomputed from `data`: in sync v3 the hash of an index
    /// (root, `.docSchema`) is derived from its entries' hashes, not its bytes,
    /// so it can't be verified here. Integrity is checked via `x-goog-hash` by the caller.
    ///
    /// The file is written before its row, so a crash in between leaves a file the
    /// next start's reconcile picks up, never a row without bytes.
    pub fn put_with_hash(&self, data: &[u8], hash: &str, filename: &str) -> Result<()> {
        if !is_valid_hash(hash) {
            return Err(ServerError::InvalidHash(hash.to_string()));
        }
        let path = self.hash_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        atomic_write(&path, data)?;

        self.inner.db.lock().execute(
            "INSERT INTO blobs (hash, filename, size, updated_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(hash) DO UPDATE SET filename = excluded.filename, size = excluded.size, updated_at = excluded.updated_at",
            params![hash, filename, data.len() as i64, unix_now()],
        )?;
        // New bytes under this hash: forget any earlier parse of it.
        self.inner.db.lock().execute("DELETE FROM indexes WHERE hash = ?1", [hash])?;
        Ok(())
    }

    /// Mark stored blobs as just used. A client told a blob is present won't upload it
    /// again, so this keeps it inside the unreachable report's grace period until the
    /// root that references it is committed.
    pub fn touch(&self, hashes: &[String]) -> Result<()> {
        let mut db = self.inner.db.lock();
        let tx = db.transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE blobs SET updated_at = ?1 WHERE hash = ?2")?;
            let now = unix_now();
            for hash in hashes {
                stmt.execute(params![now, hash])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Delete file by hash
    pub fn delete(&self, hash: &str) -> Result<()> {
        if !is_valid_hash(hash) {
            return Err(ServerError::InvalidHash(hash.to_string()));
        }
        let path = self.hash_path(hash);
        if path.exists() {
            fs::remove_file(path)?;
        }
        let meta_path = self.inner.base_path.join("meta").join(format!("{hash}.meta"));
        if meta_path.exists() {
            fs::remove_file(meta_path)?;
        }
        let db = self.inner.db.lock();
        db.execute("DELETE FROM blobs WHERE hash = ?1", [hash])?;
        db.execute("DELETE FROM indexes WHERE hash = ?1", [hash])?;
        Ok(())
    }

    /// Parse `root_hash` and the document indexes it lists into the projection.
    /// Indexes are immutable (content-addressed), so each is parsed once.
    fn index_tree(&self, root_hash: &str) -> Result<()> {
        if !self.ensure_indexed(root_hash)? {
            return Ok(());
        }
        for doc in self.index_entries(root_hash)?.unwrap_or_default().iter().filter(|e| e.is_index()) {
            self.ensure_indexed(&doc.hash)?;
        }
        Ok(())
    }

    /// Parse and record the index blob `hash` if it hasn't been with the current parser.
    /// Returns whether it is parsed. A blob not yet uploaded is left for a later call.
    fn ensure_indexed(&self, hash: &str) -> Result<bool> {
        if !is_valid_hash(hash) {
            return Ok(false);
        }
        let seen: Option<bool> = self
            .inner
            .db
            .lock()
            .query_row("SELECT ok FROM indexes WHERE hash = ?1 AND parser_version >= ?2", params![hash, PARSER_VERSION], |r| r.get(0))
            .optional()?;
        if let Some(ok) = seen {
            return Ok(ok);
        }
        if !self.exists(hash) {
            return Ok(false);
        }
        let parsed = parse_index(&self.get(hash)?);

        let mut db = self.inner.db.lock();
        let tx = db.transaction()?;
        tx.execute("DELETE FROM entries WHERE index_hash = ?1", [hash])?;
        let ok = match &parsed {
            Ok((schema, entries)) => {
                tx.execute(
                    "INSERT INTO indexes (hash, schema, ok, error, parser_version) VALUES (?1, ?2, 1, NULL, ?3)
                     ON CONFLICT(hash) DO UPDATE SET schema = excluded.schema, ok = 1, error = NULL, parser_version = excluded.parser_version",
                    params![hash, schema, PARSER_VERSION],
                )?;
                let mut insert = tx.prepare(
                    "INSERT OR REPLACE INTO entries (index_hash, name, hash, kind, subfiles, size) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )?;
                for e in entries {
                    insert.execute(params![hash, e.name, e.hash, e.kind, e.subfiles as i64, e.size as i64])?;
                }
                true
            }
            Err(error) => {
                tracing::warn!(%hash, %error, "index blob not parsed; serving it verbatim regardless");
                tx.execute(
                    "INSERT INTO indexes (hash, schema, ok, error, parser_version) VALUES (?1, NULL, 0, ?2, ?3)
                     ON CONFLICT(hash) DO UPDATE SET schema = NULL, ok = 0, error = excluded.error, parser_version = excluded.parser_version",
                    params![hash, error, PARSER_VERSION],
                )?;
                false
            }
        };
        tx.commit()?;
        Ok(ok)
    }

    /// The parsed entries of index blob `hash`, or `None` if it isn't (successfully) parsed.
    pub fn index_entries(&self, hash: &str) -> Result<Option<Vec<IndexEntry>>> {
        let db = self.inner.db.lock();
        let ok: Option<bool> = db.query_row("SELECT ok FROM indexes WHERE hash = ?1", [hash], |r| r.get(0)).optional()?;
        if ok != Some(true) {
            return Ok(None);
        }
        let mut stmt = db.prepare("SELECT hash, kind, name, subfiles, size FROM entries WHERE index_hash = ?1 ORDER BY name")?;
        let rows = stmt.query_map([hash], |r| {
            Ok(IndexEntry {
                hash: r.get(0)?,
                kind: r.get(1)?,
                name: r.get(2)?,
                subfiles: r.get::<_, i64>(3)? as u64,
                size: r.get::<_, i64>(4)? as u64,
            })
        })?;
        Ok(Some(rows.collect::<rusqlite::Result<_>>()?))
    }

    /// Child hashes of an index blob: from the projection when parsed, else read leniently.
    fn children(&self, hash: &str) -> Result<Vec<String>> {
        if let Some(entries) = self.index_entries(hash)? {
            return Ok(entries.into_iter().map(|e| e.hash).collect());
        }
        Ok(lenient_children(&self.get(hash)?))
    }

    /// Walk the sync tree from the current root (root index -> document indexes ->
    /// files) and return every referenced hash that isn't stored.
    pub fn missing_from_root(&self) -> Result<Vec<String>> {
        let root = self.get_root();
        if root.hash.is_empty() {
            return Ok(Vec::new());
        }
        if !self.exists(&root.hash) {
            return Ok(vec![root.hash]);
        }
        let mut missing = Vec::new();
        for doc in self.children(&root.hash)? {
            if self.exists(&doc) {
                missing.extend(self.children(&doc)?.into_iter().filter(|f| !self.exists(f)));
            } else {
                missing.push(doc);
            }
        }
        Ok(missing)
    }

    /// Stored blobs not reachable from the current root and not written within `grace`
    /// (an in-flight sync uploads blobs before it commits the root that references them).
    ///
    /// Live means: the current root's tree, the previous root's tree (a tablet mid-sync
    /// may still be reading it), and blobs recorded in the version history.
    ///
    /// Read-only: nothing is deleted, and the answer is a snapshot — anything acting on
    /// it must re-check against the root generation at that time. Errors rather than
    /// guess if an index in the current tree is missing or unparsed, since its children
    /// would otherwise look unreachable.
    pub fn unreachable_blobs(&self, grace: Duration) -> Result<Vec<String>> {
        let root = self.get_root();
        let previous: String =
            self.inner.db.lock().query_row("SELECT previous_hash FROM root WHERE id = 1", [], |r| r.get(0))?;
        let mut live = self.version_hashes()?;
        self.add_tree(&root.hash, true, &mut live)?;
        // The previous root is best effort: it may predate this server or be gone.
        self.add_tree(&previous, false, &mut live)?;

        let grace = i64::try_from(grace.as_secs()).unwrap_or(i64::MAX);
        let cutoff = unix_now().saturating_sub(grace);
        let db = self.inner.db.lock();
        let mut stmt = db.prepare("SELECT hash FROM blobs WHERE updated_at <= ?1 ORDER BY hash")?;
        let rows = stmt.query_map([cutoff], |r| r.get::<_, String>(0))?;
        let mut unreachable = Vec::new();
        for hash in rows {
            let hash = hash?;
            if !live.contains(&hash) {
                unreachable.push(hash);
            }
        }
        Ok(unreachable)
    }

    /// Add the tree under `root_hash` to `live`. With `strict`, a missing or unparsed
    /// index is an error; otherwise its subtree is read leniently when possible.
    fn add_tree(&self, root_hash: &str, strict: bool, live: &mut HashSet<String>) -> Result<()> {
        if root_hash.is_empty() {
            return Ok(());
        }
        live.insert(root_hash.to_string());
        self.index_tree(root_hash)?;
        let unparsed = |hash: &str| {
            ServerError::Internal(format!("index {hash} is missing or unparsed; refusing to report unreachable blobs"))
        };
        let docs = match self.index_entries(root_hash)? {
            Some(docs) => docs,
            None if strict => return Err(unparsed(root_hash)),
            None => {
                if let Ok(bytes) = self.get(root_hash) {
                    for doc in lenient_children(&bytes) {
                        if let Ok(doc_bytes) = self.get(&doc) {
                            live.extend(lenient_children(&doc_bytes));
                        }
                        live.insert(doc);
                    }
                }
                return Ok(());
            }
        };
        for doc in docs {
            if doc.is_index() {
                match self.index_entries(&doc.hash)? {
                    Some(files) => live.extend(files.into_iter().map(|f| f.hash)),
                    None if strict => return Err(unparsed(&doc.hash)),
                    None => {
                        if let Ok(bytes) = self.get(&doc.hash) {
                            live.extend(lenient_children(&bytes));
                        }
                    }
                }
            }
            live.insert(doc.hash);
        }
        Ok(())
    }

    /// Whether the current or previous root's tree references `hash`. Answers `true`
    /// when the tree can't be read, so callers deleting on `false` stay safe.
    pub fn is_referenced(&self, hash: &str) -> bool {
        let root = self.get_root();
        let previous = self.inner.db.lock().query_row("SELECT previous_hash FROM root WHERE id = 1", [], |r| r.get::<_, String>(0));
        let mut live = HashSet::new();
        let walked = previous
            .map_err(ServerError::from)
            .and_then(|previous| {
                self.add_tree(&root.hash, true, &mut live)?;
                self.add_tree(&previous, false, &mut live)
            });
        match walked {
            Ok(()) => live.contains(hash),
            Err(e) => {
                tracing::warn!(error = %e, "sync tree unreadable; treating {hash} as referenced");
                true
            }
        }
    }

    /// Content hashes the version history (`versions/versions.db`) may read back from storage.
    fn version_hashes(&self) -> Result<HashSet<String>> {
        let path = self.inner.base_path.join("versions").join("versions.db");
        if !path.exists() {
            return Ok(HashSet::new());
        }
        let conn = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut stmt = conn.prepare("SELECT DISTINCT content_hash FROM versions")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// List all hashes in storage
    pub fn list_hashes(&self) -> Result<Vec<String>> {
        let db = self.inner.db.lock();
        let mut stmt = db.prepare("SELECT hash FROM blobs ORDER BY hash")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        let (file_count, total_bytes) = self
            .inner
            .db
            .lock()
            .query_row("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM blobs", [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "reading storage stats failed");
                (0, 0)
            });
        let root = self.get_root();
        StorageStats {
            file_count: file_count as usize,
            total_bytes: total_bytes as u64,
            root_hash: root.hash,
            generation: root.generation,
        }
    }

    /// Clear all storage
    pub fn clear(&self) -> Result<()> {
        // Remove all hash directories
        for entry in fs::read_dir(&self.inner.base_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.len() == 2 || name == "meta" {
                    fs::remove_dir_all(&path)?;
                }
            }
        }

        {
            let mut db = self.inner.db.lock();
            let tx = db.transaction()?;
            tx.execute_batch("DELETE FROM entries; DELETE FROM indexes; DELETE FROM blobs;")?;
            tx.execute("UPDATE root SET hash = '', generation = 0 WHERE id = 1", [])?;
            tx.commit()?;
        }
        *self.inner.root.write() = SyncRoot::empty();

        let root_path = self.inner.base_path.join("root.json");
        if root_path.exists() {
            fs::remove_file(root_path)?;
        }

        Ok(())
    }

    /// List all stored files with hash, filename, and size
    pub fn list(&self) -> Vec<(String, String, usize)> {
        let db = self.inner.db.lock();
        let rows = db.prepare("SELECT hash, filename, size FROM blobs ORDER BY filename").and_then(|mut stmt| {
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? as usize)))?
                .collect::<rusqlite::Result<Vec<_>>>()
        });
        rows.unwrap_or_else(|e| {
            tracing::error!(error = %e, "listing blobs failed");
            Vec::new()
        })
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub file_count: usize,
    pub total_bytes: u64,
    pub root_hash: String,
    pub generation: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_storage_put_get() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();

        let data = b"hello world";
        let hash = storage.put(data, "test.txt").unwrap();

        // Verify hash is SHA-256
        assert_eq!(hash.len(), 64);

        // Retrieve
        let retrieved = storage.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    fn temp_residue(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp."))
            .collect()
    }

    #[test]
    fn atomic_write_overwrites_without_temp_residue() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("root.json");
        atomic_write(&path, b"first").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"first");
        // Overwrite with different-length content: never truncated, fully replaced.
        atomic_write(&path, b"second-and-longer").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second-and-longer");
        assert!(temp_residue(tmp.path()).is_empty(), "temp files left behind");
    }

    #[test]
    fn set_root_persists_atomically_and_reloads() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let h = "a".repeat(64);
        storage.set_root(h.clone()).unwrap();
        // A fresh Storage over the same dir reads the persisted root back.
        let reloaded = Storage::new(tmp.path()).unwrap();
        assert_eq!(reloaded.get_root().hash, h);
        assert!(temp_residue(tmp.path()).is_empty(), "temp files left behind");
        // root.json is kept as a mirror of the committed root.
        let mirror: SyncRoot = serde_json::from_str(&fs::read_to_string(tmp.path().join("root.json")).unwrap()).unwrap();
        assert_eq!((mirror.hash, mirror.generation), (h, 1));
    }

    #[test]
    fn test_storage_by_filename() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();

        let data = b"document content";
        storage.put(data, "doc-uuid.content").unwrap();

        let retrieved = storage.get_by_filename("doc-uuid.content").unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_storage_root() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();

        // Initial state
        let root = storage.get_root();
        assert!(root.hash.is_empty());
        assert_eq!(root.generation, 0);

        // Set root
        storage.set_root("abc123".to_string()).unwrap();
        let root = storage.get_root();
        assert_eq!(root.hash, "abc123");
        assert_eq!(root.generation, 1);
    }

    #[test]
    fn test_storage_delete() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();

        let data = b"to be deleted";
        let hash = storage.put(data, "temp.txt").unwrap();

        assert!(storage.exists(&hash));
        storage.delete(&hash).unwrap();
        assert!(!storage.exists(&hash));
        assert!(storage.filename_for_hash(&hash).is_none());
        assert!(matches!(storage.delete("../../etc"), Err(ServerError::InvalidHash(_))));
    }

    #[test]
    fn list_and_stats_report_stored_blobs() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let a = storage.put(b"aaaa", "a.pdf").unwrap();
        let b = storage.put(b"bb", "b.content").unwrap();

        let mut listed = storage.list();
        listed.sort();
        let mut expected = vec![(a.clone(), "a.pdf".to_string(), 4), (b.clone(), "b.content".to_string(), 2)];
        expected.sort();
        assert_eq!(listed, expected);

        let stats = storage.stats();
        assert_eq!((stats.file_count, stats.total_bytes), (2, 6));
        assert_eq!(storage.filename_for_hash(&a).as_deref(), Some("a.pdf"));
    }

    /// Storage written by the pre-sync.db layout: root.json, blob files, meta/*.meta.
    fn legacy_layout(dir: &Path) -> String {
        let data = b"legacy blob";
        let hash = hex::encode(Sha256::digest(data));
        fs::create_dir_all(dir.join(&hash[..2])).unwrap();
        fs::write(dir.join(&hash[..2]).join(&hash), data).unwrap();
        fs::create_dir_all(dir.join("meta")).unwrap();
        fs::write(dir.join("meta").join(format!("{hash}.meta")), "doc.pdf").unwrap();
        let root = SyncRoot::new("c".repeat(64), 21);
        fs::write(dir.join("root.json"), serde_json::to_string(&root).unwrap()).unwrap();
        hash
    }

    #[test]
    fn imports_legacy_root_and_meta() {
        let tmp = TempDir::new().unwrap();
        let hash = legacy_layout(tmp.path());

        let storage = Storage::new(tmp.path()).unwrap();
        assert!(tmp.path().join("sync.db").exists());
        let root = storage.get_root();
        assert_eq!((root.hash.as_str(), root.generation), ("c".repeat(64).as_str(), 21));
        assert_eq!(storage.filename_for_hash(&hash).as_deref(), Some("doc.pdf"));
        assert_eq!(storage.get_by_filename("doc.pdf").unwrap(), b"legacy blob");
        assert_eq!(storage.list_hashes().unwrap(), vec![hash.clone()]);

        // Once imported, sync.db wins over a stale root.json.
        fs::write(tmp.path().join("root.json"), serde_json::to_string(&SyncRoot::new("d".repeat(64), 3)).unwrap()).unwrap();
        assert_eq!(Storage::new(tmp.path()).unwrap().get_root().generation, 21);
    }

    #[test]
    fn reconcile_follows_blob_files_on_disk() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let kept = storage.put(b"kept", "kept.rm").unwrap();
        let removed = storage.put(b"removed", "removed.rm").unwrap();
        drop(storage);

        // A blob vanishes and one appears without a row (crash between file and row).
        fs::remove_file(tmp.path().join(&removed[..2]).join(&removed)).unwrap();
        let orphan = hex::encode(Sha256::digest(b"orphan"));
        fs::create_dir_all(tmp.path().join(&orphan[..2])).unwrap();
        fs::write(tmp.path().join(&orphan[..2]).join(&orphan), b"orphan").unwrap();
        // Crash residue is not a blob.
        fs::write(tmp.path().join(&kept[..2]).join(format!(".{kept}.tmp.1.0")), b"x").unwrap();

        let storage = Storage::new(tmp.path()).unwrap();
        let mut expected = vec![kept, orphan];
        expected.sort();
        assert_eq!(storage.list_hashes().unwrap(), expected);
    }

    #[test]
    fn root_cas_holds_across_instances() {
        // Two Storage values over one directory stand in for two server processes.
        let tmp = TempDir::new().unwrap();
        let first = Storage::new(tmp.path()).unwrap();
        let second = Storage::new(tmp.path()).unwrap();

        first.set_root_if("a".repeat(64), Some(0)).unwrap();
        let lost = second.set_root_if("b".repeat(64), Some(0));
        assert!(matches!(lost, Err(ServerError::GenerationMismatch { current: 1 })));
        assert_eq!(second.get_root().hash, "a".repeat(64));

        second.set_root_if("b".repeat(64), Some(1)).unwrap();
        assert_eq!(first.get_root().generation, 2);
    }

    #[test]
    fn parses_schema_3_and_4_and_rejects_unknown() {
        let h = "e".repeat(64);
        let (schema, entries) = parse_index(format!("3\n{h}:0:doc.content:0:12\n").as_bytes()).unwrap();
        assert_eq!(schema, "3");
        assert_eq!(entries, vec![IndexEntry { hash: h.clone(), kind: "0".into(), name: "doc.content".into(), subfiles: 0, size: 12 }]);

        let (schema, entries) = parse_index(format!("4\n0:.:1:12\n{h}:0:doc.content:0:12\n").as_bytes()).unwrap();
        assert_eq!((schema.as_str(), entries.len()), ("4", 1));

        assert!(parse_index(b"9\nwhatever").is_err());
        assert!(parse_index(format!("3\n{h}:0:doc.content:0\n").as_bytes()).is_err());
        assert!(parse_index(b"3\nnot-a-hash:0:x:0:1\n").is_err());
    }

    #[test]
    fn projects_committed_tree_and_serves_index_verbatim() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let (id, _) = crate::documents::create_document(&storage, "Paper", "pdf", b"%PDF-1.4 test").unwrap();
        let first_root = storage.get_root().hash;

        let docs = storage.index_entries(&first_root).unwrap().expect("root parsed");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].name, id);
        assert!(docs[0].is_index());
        let files = storage.index_entries(&docs[0].hash).unwrap().expect("doc index parsed");
        let names: Vec<_> = files.iter().map(|f| f.name.clone()).collect();
        assert_eq!(names, vec![format!("{id}.content"), format!("{id}.metadata"), format!("{id}.pdf")]);
        assert!(storage.missing_from_root().unwrap().is_empty());

        // The index bytes the tablet fetches are exactly what was stored.
        let stored = storage.get(&first_root).unwrap();
        assert!(stored.starts_with(b"3\n"));

        // A second document moves the root; the old root stays fetchable.
        crate::documents::create_document(&storage, "Second", "pdf", b"%PDF-1.4 two").unwrap();
        assert_ne!(storage.get_root().hash, first_root);
        assert_eq!(storage.get(&first_root).unwrap(), stored);
        assert_eq!(storage.index_entries(&storage.get_root().hash).unwrap().unwrap().len(), 2);

        // Deleting a leaf shows up as missing.
        let pdf = files.iter().find(|f| f.name.ends_with(".pdf")).unwrap();
        storage.delete(&pdf.hash).unwrap();
        assert_eq!(storage.missing_from_root().unwrap(), vec![pdf.hash.clone()]);
    }

    #[test]
    fn unknown_index_format_never_breaks_commit() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let leaf = storage.put(b"leaf", "leaf.rm").unwrap();
        let index = format!("7\n{leaf}:future:field:layout\n");
        let index_hash = "f".repeat(64);
        storage.put_with_hash(index.as_bytes(), &index_hash, "root.docSchema").unwrap();

        let root = storage.set_root_if(index_hash.clone(), Some(0)).unwrap();
        assert_eq!(root.generation, 1);
        assert_eq!(storage.get(&index_hash).unwrap(), index.as_bytes());
        assert!(storage.index_entries(&index_hash).unwrap().is_none());
        // Missing-blob detection falls back to the lenient reader.
        assert!(storage.missing_from_root().unwrap().is_empty());
        // The unreachable report refuses rather than guessing.
        assert!(storage.unreachable_blobs(Duration::ZERO).is_err());
    }

    #[test]
    fn rollback_then_upgrade_adopts_newer_root_json() {
        let tmp = TempDir::new().unwrap();
        Storage::new(tmp.path()).unwrap().set_root("a".repeat(64)).unwrap();
        // An older build (no sync.db) commits two more roots, updating only root.json.
        fs::write(tmp.path().join("root.json"), serde_json::to_string(&SyncRoot::new("b".repeat(64), 3)).unwrap()).unwrap();
        let root = Storage::new(tmp.path()).unwrap().get_root();
        assert_eq!((root.hash, root.generation), ("b".repeat(64), 3));

        // Same generation, different hash: ambiguous, refuse to start.
        fs::write(tmp.path().join("root.json"), serde_json::to_string(&SyncRoot::new("c".repeat(64), 3)).unwrap()).unwrap();
        assert!(matches!(Storage::new(tmp.path()), Err(ServerError::Config(_))));
    }

    #[test]
    fn is_referenced_follows_tree() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        crate::documents::create_document(&storage, "Doc", "pdf", b"%PDF-1.4 doc").unwrap();
        let root = storage.get_root().hash;
        let doc = &storage.index_entries(&root).unwrap().unwrap()[0];
        let leaf = &storage.index_entries(&doc.hash).unwrap().unwrap()[0];
        assert!(storage.is_referenced(&root) && storage.is_referenced(&doc.hash) && storage.is_referenced(&leaf.hash));
        let orphan = storage.put(b"orphan", "o.rm").unwrap();
        assert!(!storage.is_referenced(&orphan));
    }

    #[test]
    fn mirror_never_goes_backwards() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        storage.write_mirror(&SyncRoot::new("a".repeat(64), 5));
        storage.write_mirror(&SyncRoot::new("b".repeat(64), 4));
        assert_eq!(Storage::read_root_json(tmp.path()).unwrap().unwrap().generation, 5);
    }

    #[test]
    fn reupload_forgets_old_parse() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let h = "f".repeat(64);
        storage.put_with_hash(b"7\nunknown", &h, "root.docSchema").unwrap();
        storage.set_root(h.clone()).unwrap();
        assert!(storage.index_entries(&h).unwrap().is_none());
        storage.put_with_hash(b"3\n", &h, "root.docSchema").unwrap();
        storage.index_tree(&h).unwrap();
        assert_eq!(storage.index_entries(&h).unwrap(), Some(vec![]));
    }

    #[test]
    fn unreachable_keeps_previous_root_versions_and_touched() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        crate::documents::create_document(&storage, "One", "pdf", b"%PDF-1.4 one").unwrap();
        let first_tree: Vec<String> = storage.list_hashes().unwrap();
        crate::documents::create_document(&storage, "Two", "pdf", b"%PDF-1.4 two").unwrap();
        // Root one is now the previous root: its tree is still live.
        let unreachable = storage.unreachable_blobs(Duration::ZERO).unwrap();
        assert!(first_tree.iter().all(|h| !unreachable.contains(h)), "{unreachable:?}");

        // A version-history blob is live.
        let kept = storage.put(b"old content", "doc.content").unwrap();
        let versions = tmp.path().join("versions");
        fs::create_dir_all(&versions).unwrap();
        let db = Connection::open(versions.join("versions.db")).unwrap();
        db.execute_batch("CREATE TABLE versions (content_hash TEXT NOT NULL)").unwrap();
        db.execute("INSERT INTO versions VALUES (?1)", [&kept]).unwrap();
        assert!(!storage.unreachable_blobs(Duration::ZERO).unwrap().contains(&kept));

        // Huge grace saturates instead of wrapping into "report everything".
        assert!(storage.unreachable_blobs(Duration::from_secs(u64::MAX)).unwrap().is_empty());

        // Touching resets the clock.
        let orphan = storage.put(b"orphan", "x.rm").unwrap();
        storage.inner.db.lock().execute("UPDATE blobs SET updated_at = 0 WHERE hash = ?1", [&orphan]).unwrap();
        assert!(storage.unreachable_blobs(Duration::from_secs(60)).unwrap().contains(&orphan));
        storage.touch(&[orphan.clone()]).unwrap();
        assert!(!storage.unreachable_blobs(Duration::from_secs(60)).unwrap().contains(&orphan));
    }

    #[test]
    fn unreachable_report_lists_only_orphans_past_grace() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        crate::documents::create_document(&storage, "Live", "pdf", b"%PDF-1.4 live").unwrap();
        let orphan = storage.put(b"superseded page", "old.rm").unwrap();

        assert_eq!(storage.unreachable_blobs(Duration::ZERO).unwrap(), vec![orphan.clone()]);
        assert!(storage.unreachable_blobs(Duration::from_secs(3600)).unwrap().is_empty());
        // Report only: nothing was deleted.
        assert!(storage.exists(&orphan));
    }
}
