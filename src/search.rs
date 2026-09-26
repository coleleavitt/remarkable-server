//! FTS5 full-text search for remarkable-server
//!
//! Provides:
//! - FTS5 virtual table for document content
//! - Document/folder name indexing
//! - PDF text extraction via pdftotext
//! - Highlight matches
//! - Incremental indexing on sync

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::error::{Result, ServerError};
use crate::storage::Storage;

/// Cap on extracted text stored per document, in bytes.
const MAX_INDEXED_TEXT: usize = 100_000;

/// Whether `hash` is the blob `filename` currently resolves to. A superseded version (the
/// same filename re-uploaded with new content) is not, whether or not the storage layer still
/// remembers a name for the old hash.
fn is_current(storage: &Storage, hash: &str, filename: &str) -> bool {
    storage.hash_for_filename(filename).as_deref() == Some(hash)
}

/// Longest prefix of `text` of at most `max` bytes that ends on a char boundary.
fn truncate_on_char_boundary(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Search index using SQLite FTS5
#[derive(Clone)]
pub struct SearchIndex {
    inner: Arc<SearchIndexInner>,
}

struct SearchIndexInner {
    conn: Mutex<Connection>,
}

/// Indexed document entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedDocument {
    pub hash: String,
    pub filename: String,
    pub doc_type: DocumentType,
    pub content: Option<String>,
    pub indexed_at: i64,
}

/// Document type for indexing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DocumentType {
    Document,
    Folder,
    Pdf,
    Epub,
    Unknown,
}

impl DocumentType {
    fn from_filename(filename: &str) -> Self {
        let lower = filename.to_lowercase();
        if lower.ends_with(".pdf") {
            Self::Pdf
        } else if lower.ends_with(".epub") {
            Self::Epub
        } else if lower.ends_with(".content") || lower.ends_with(".metadata") {
            Self::Document
        } else if lower.contains("folder") || filename.ends_with("/") {
            Self::Folder
        } else {
            Self::Unknown
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Folder => "folder",
            Self::Pdf => "pdf",
            Self::Epub => "epub",
            Self::Unknown => "unknown",
        }
    }
}

/// Search result with highlight
#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub hash: String,
    pub filename: String,
    pub doc_type: DocumentType,
    pub snippet: String,
    pub rank: f64,
}

/// Search query options
#[derive(Debug, Clone, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub doc_type: Option<DocumentType>,
}

fn default_limit() -> usize {
    20
}

impl SearchIndex {
    /// Create new search index at path
    pub fn new<P: AsRef<Path>>(storage_path: P) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let db_path = storage_path.join("search.db");

        let conn = Connection::open(&db_path)
            .map_err(|e| ServerError::Database(format!("Failed to open search DB: {}", e)))?;

        // Enable FTS5
        Self::init_fts5(&conn)?;

        info!("Search index initialized at {:?}", db_path);

        Ok(Self {
            inner: Arc::new(SearchIndexInner {
                conn: Mutex::new(conn),
            }),
        })
    }

    /// Initialize FTS5 virtual table
    fn init_fts5(conn: &Connection) -> Result<()> {
        // Main documents table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS documents (
                hash TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                doc_type TEXT NOT NULL,
                content TEXT,
                indexed_at INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| ServerError::Database(e.to_string()))?;

        // FTS5 virtual table for full-text search
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                filename,
                content,
                content='documents',
                content_rowid='rowid'
            )",
            [],
        )
        .map_err(|e| ServerError::Database(e.to_string()))?;

        // Triggers to keep FTS in sync
        conn.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
                INSERT INTO documents_fts(rowid, filename, content)
                VALUES (new.rowid, new.filename, new.content);
            END;
            
            CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, filename, content)
                VALUES ('delete', old.rowid, old.filename, old.content);
            END;
            
            CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, filename, content)
                VALUES ('delete', old.rowid, old.filename, old.content);
                INSERT INTO documents_fts(rowid, filename, content)
                VALUES (new.rowid, new.filename, new.content);
            END;",
        )
        .map_err(|e| ServerError::Database(e.to_string()))?;

        // Index for fast lookups
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_filename ON documents(filename)",
            [],
        )
        .map_err(|e| ServerError::Database(e.to_string()))?;

        Ok(())
    }

    /// Index a document
    pub fn index_document(&self, hash: &str, filename: &str, content: Option<&str>) -> Result<()> {
        let conn = self.inner.conn.lock();
        let doc_type = DocumentType::from_filename(filename);
        let now = chrono::Utc::now().timestamp();

        conn.execute(
            "INSERT OR REPLACE INTO documents (hash, filename, doc_type, content, indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![hash, filename, doc_type.as_str(), content, now],
        )
        .map_err(|e| ServerError::Database(e.to_string()))?;

        debug!("Indexed document: {} ({})", filename, hash);
        Ok(())
    }

    /// Index a PDF by extracting text with pdftotext
    pub fn index_pdf(&self, hash: &str, filename: &str, pdf_data: &[u8]) -> Result<()> {
        let content = Self::extract_pdf_text(hash, filename, pdf_data);
        self.index_document(hash, filename, content.as_deref())
    }

    /// Extract a PDF's text with pdftotext (None if it fails or isn't installed).
    fn extract_pdf_text(hash: &str, filename: &str, pdf_data: &[u8]) -> Option<String> {
        // Stage the PDF in a unique temp file (removed on drop), so concurrent extractions of
        // the same hash can't clobber or delete each other's input.
        let staged = tempfile::Builder::new()
            .prefix(&format!("{hash}-"))
            .suffix(".pdf")
            .tempfile()
            .and_then(|mut f| std::io::Write::write_all(&mut f, pdf_data).map(|_| f));
        let temp_pdf = match staged {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "Failed to stage PDF {} for text extraction: {}",
                    filename, e
                );
                return None;
            }
        };

        // Extract text with pdftotext
        let output = Command::new("pdftotext")
            .args(["-layout", "-enc", "UTF-8"])
            .arg(temp_pdf.path())
            .arg("-")
            .output();
        drop(temp_pdf);

        match output {
            Ok(out) if out.status.success() => {
                // Truncate to reasonable size for indexing
                Some(
                    truncate_on_char_boundary(
                        &String::from_utf8_lossy(&out.stdout),
                        MAX_INDEXED_TEXT,
                    )
                    .to_string(),
                )
            }
            Ok(out) => {
                warn!(
                    "pdftotext failed for {}: {}",
                    filename,
                    String::from_utf8_lossy(&out.stderr)
                );
                None
            }
            Err(e) => {
                warn!("pdftotext not available: {}", e);
                None
            }
        }
    }

    /// Remove document from index
    pub fn remove_document(&self, hash: &str) -> Result<()> {
        let conn = self.inner.conn.lock();
        conn.execute("DELETE FROM documents WHERE hash = ?1", params![hash])
            .map_err(|e| ServerError::Database(e.to_string()))?;
        debug!("Removed from index: {}", hash);
        Ok(())
    }

    /// Search documents with FTS5
    pub fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let conn = self.inner.conn.lock();

        // Escape query for FTS5 (wrap terms in quotes for phrase matching if needed)
        let fts_query = Self::prepare_fts_query(&query.q);

        // Build and execute query based on type filter
        let mut results = Vec::new();

        if let Some(ref dt) = query.doc_type {
            let sql = "SELECT d.hash, d.filename, d.doc_type,
                              snippet(documents_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                              bm25(documents_fts) as rank
                       FROM documents_fts fts
                       JOIN documents d ON fts.rowid = d.rowid
                       WHERE documents_fts MATCH ?1 AND d.doc_type = ?2
                       ORDER BY rank
                       LIMIT ?3 OFFSET ?4";

            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| ServerError::Database(e.to_string()))?;

            let rows = stmt
                .query_map(
                    params![
                        &fts_query,
                        dt.as_str(),
                        query.limit as i64,
                        query.offset as i64
                    ],
                    |row| Self::row_to_result(row),
                )
                .map_err(|e| ServerError::Database(e.to_string()))?;

            for row in rows {
                if let Ok(r) = row {
                    results.push(r);
                }
            }
        } else {
            let sql = "SELECT d.hash, d.filename, d.doc_type,
                              snippet(documents_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
                              bm25(documents_fts) as rank
                       FROM documents_fts fts
                       JOIN documents d ON fts.rowid = d.rowid
                       WHERE documents_fts MATCH ?1
                       ORDER BY rank
                       LIMIT ?2 OFFSET ?3";

            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| ServerError::Database(e.to_string()))?;

            let rows = stmt
                .query_map(
                    params![&fts_query, query.limit as i64, query.offset as i64],
                    |row| Self::row_to_result(row),
                )
                .map_err(|e| ServerError::Database(e.to_string()))?;

            for row in rows {
                if let Ok(r) = row {
                    results.push(r);
                }
            }
        }

        Ok(results)
    }

    /// Convert a database row to SearchResult
    fn row_to_result(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchResult> {
        Ok(SearchResult {
            hash: row.get(0)?,
            filename: row.get(1)?,
            doc_type: Self::parse_doc_type(&row.get::<_, String>(2)?),
            // NULL for name-only rows (no extracted text); still a match on the filename.
            snippet: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            rank: row.get(4)?,
        })
    }

    /// Prepare query for FTS5 (handle special characters)
    fn prepare_fts_query(query: &str) -> String {
        // If query contains special FTS operators, use as-is
        if query.contains('"')
            || query.contains('*')
            || query.contains("OR")
            || query.contains("AND")
        {
            return query.to_string();
        }

        // Otherwise, prefix-match each word for partial matching
        query
            .split_whitespace()
            .map(|word| format!("{}*", word))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn parse_doc_type(s: &str) -> DocumentType {
        match s {
            "document" => DocumentType::Document,
            "folder" => DocumentType::Folder,
            "pdf" => DocumentType::Pdf,
            "epub" => DocumentType::Epub,
            _ => DocumentType::Unknown,
        }
    }

    /// Get index statistics
    pub fn stats(&self) -> Result<IndexStats> {
        let conn = self.inner.conn.lock();

        let total_docs: i64 = conn
            .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
            .map_err(|e| ServerError::Database(e.to_string()))?;

        let indexed_content: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE content IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .map_err(|e| ServerError::Database(e.to_string()))?;

        let pdf_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE doc_type = 'pdf'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| ServerError::Database(e.to_string()))?;

        Ok(IndexStats {
            total_documents: total_docs as usize,
            documents_with_content: indexed_content as usize,
            pdf_count: pdf_count as usize,
        })
    }

    /// Rebuild entire index from storage. Only blobs reachable from the current root's
    /// tree are indexed, so documents deleted on the tablet (still stored until garbage
    /// collection) drop out of search; if that tree can't be fully read, every stored blob
    /// is indexed as before rather than wiping search. Rows are upserted one at a time as
    /// text is extracted (hashes are content addresses, so re-indexing one is idempotent and
    /// no library-wide batch of text is held in memory), then rows whose blob is gone,
    /// unreachable, or no longer mapped to any filename are pruned, so deleted and superseded
    /// documents (e.g. the old blob left behind when a filename is re-uploaded with new
    /// content) drop out of search. A live, mapped blob that can't be read this time keeps its
    /// previous row, and rows written meanwhile by an overlapping (re)index survive.
    pub fn rebuild_from_storage(&self, storage: &Storage) -> Result<usize> {
        info!("Rebuilding search index from storage...");

        let mut indexed = 0;
        for hash in Self::live_hashes(storage)? {
            let Some(filename) = storage.filename_for_hash(&hash) else {
                continue;
            };
            if !is_current(storage, &hash, &filename) {
                continue;
            }
            let content = match DocumentType::from_filename(&filename) {
                DocumentType::Pdf => match storage.get(&hash) {
                    Ok(data) => Self::extract_pdf_text(&hash, &filename, &data),
                    Err(ServerError::NotFound(_)) => continue,
                    Err(e) => {
                        warn!(
                            "Keeping previous index row for unreadable PDF {}: {}",
                            filename, e
                        );
                        continue;
                    }
                },
                _ => None,
            };
            self.index_document(&hash, &filename, content.as_deref())?;
            indexed += 1;
        }
        let pruned = self.prune_missing(storage)?;

        info!(
            "Indexed {} documents, pruned {} stale rows",
            indexed, pruned
        );
        Ok(indexed)
    }

    /// Stored blobs search may index: those in the current root's tree, or every stored blob
    /// when that tree can't be fully read (no root yet, or an index missing or unparsed).
    fn live_hashes(storage: &Storage) -> Result<Vec<String>> {
        let stored = storage.list_hashes()?;
        Ok(match storage.reachable_from_root() {
            Some(reachable) => stored
                .into_iter()
                .filter(|h| reachable.contains(h))
                .collect(),
            None => {
                warn!("sync tree not fully readable; search covers every stored blob");
                stored
            }
        })
    }

    /// Delete rows whose blob is no longer in storage (or, when the current tree is readable,
    /// no longer reachable from it) or no longer mapped to a filename, and
    /// repoint rows whose own filename moved on to another blob while a different filename
    /// still currently maps their hash (so an unreadable blob can't keep a stale name). The
    /// listing and mapping lookups happen while holding the DB lock, so every row already
    /// written refers to a blob and mapping they can see. The row's own filename is checked
    /// first (O(1)); only on a mismatch is the hash reverse-looked-up.
    fn prune_missing(&self, storage: &Storage) -> Result<usize> {
        let db = |e: rusqlite::Error| ServerError::Database(e.to_string());
        let mut conn = self.inner.conn.lock();
        let live: std::collections::HashSet<String> =
            Self::live_hashes(storage)?.into_iter().collect();
        let tx = conn.transaction().map_err(db)?;
        let (mut stale, mut renamed) = (Vec::new(), Vec::new());
        {
            let mut stmt = tx
                .prepare("SELECT hash, filename FROM documents")
                .map_err(db)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(db)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(db)?;
            for (h, f) in rows {
                if live.contains(&h) && is_current(storage, &h, &f) {
                    continue;
                }
                match storage
                    .filename_for_hash(&h)
                    .filter(|g| live.contains(&h) && is_current(storage, &h, g))
                {
                    Some(g) => renamed.push((h, g)),
                    None => stale.push(h),
                }
            }
        }
        for hash in &stale {
            tx.execute("DELETE FROM documents WHERE hash = ?1", params![hash])
                .map_err(db)?;
        }
        for (hash, filename) in &renamed {
            tx.execute(
                "UPDATE documents SET filename = ?2, doc_type = ?3 WHERE hash = ?1",
                params![
                    hash,
                    filename,
                    DocumentType::from_filename(filename).as_str()
                ],
            )
            .map_err(db)?;
        }
        tx.commit().map_err(db)?;
        Ok(stale.len())
    }

    /// Incremental index: index new/changed documents since last sync
    pub fn index_incremental(&self, storage: &Storage, changed_hashes: &[String]) -> Result<usize> {
        let mut indexed = 0;

        for hash in changed_hashes {
            if let Some(filename) = storage.filename_for_hash(hash) {
                let doc_type = DocumentType::from_filename(&filename);

                match doc_type {
                    DocumentType::Pdf => {
                        if let Ok(data) = storage.get(hash) {
                            self.index_pdf(hash, &filename, &data)?;
                            indexed += 1;
                        }
                    }
                    _ => {
                        self.index_document(hash, &filename, None)?;
                        indexed += 1;
                    }
                }
            }
        }

        debug!("Incrementally indexed {} documents", indexed);
        Ok(indexed)
    }

    /// Check if document is indexed
    pub fn is_indexed(&self, hash: &str) -> bool {
        let conn = self.inner.conn.lock();
        conn.query_row(
            "SELECT 1 FROM documents WHERE hash = ?1",
            params![hash],
            |_| Ok(()),
        )
        .is_ok()
    }
}

/// Index statistics
#[derive(Debug, Clone, Serialize)]
pub struct IndexStats {
    pub total_documents: usize,
    pub documents_with_content: usize,
    pub pdf_count: usize,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_search_index_basic() {
        let tmp = TempDir::new().unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();

        // Index documents
        index
            .index_document(
                "hash1",
                "Meeting Notes.content",
                Some("Important meeting about Q4 planning"),
            )
            .unwrap();
        index
            .index_document(
                "hash2",
                "Project Ideas.content",
                Some("Ideas for the new project"),
            )
            .unwrap();
        index.index_document("hash3", "Archive/", None).unwrap();

        // Search
        let results = index
            .search(&SearchQuery {
                q: "meeting".to_string(),
                limit: 10,
                offset: 0,
                doc_type: None,
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].hash, "hash1");
        assert!(results[0].snippet.contains("<mark>"));
    }

    #[test]
    fn test_search_prefix_matching() {
        let tmp = TempDir::new().unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();

        index
            .index_document(
                "hash1",
                "development.content",
                Some("Software development notes"),
            )
            .unwrap();

        // Partial match should work
        let results = index
            .search(&SearchQuery {
                q: "develop".to_string(),
                limit: 10,
                offset: 0,
                doc_type: None,
            })
            .unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_type_filter() {
        let tmp = TempDir::new().unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();

        index
            .index_document("hash1", "doc.content", Some("document content"))
            .unwrap();
        index
            .index_document("hash2", "file.pdf", Some("pdf content"))
            .unwrap();

        // Filter by type
        let results = index
            .search(&SearchQuery {
                q: "content".to_string(),
                limit: 10,
                offset: 0,
                doc_type: Some(DocumentType::Pdf),
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_type, DocumentType::Pdf);
    }

    #[test]
    fn test_rebuild_prunes_deleted_blobs() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("store")).unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        let (keep, gone) = ("a".repeat(64), "b".repeat(64));
        storage
            .put_with_hash(b"{}", &keep, "keepme.metadata")
            .unwrap();
        storage
            .put_with_hash(b"{}", &gone, "goneaway.metadata")
            .unwrap();
        // A stale row for a blob storage never had (e.g. left by an older rebuild).
        index
            .index_document(&"c".repeat(64), "phantom.metadata", None)
            .unwrap();
        let q = |q: &str| {
            index
                .search(&SearchQuery {
                    q: q.into(),
                    limit: 10,
                    offset: 0,
                    doc_type: None,
                })
                .unwrap()
                .len()
        };

        assert_eq!(index.rebuild_from_storage(&storage).unwrap(), 2);
        assert_eq!((q("keepme"), q("goneaway"), q("phantom")), (1, 1, 0));

        storage.delete(&gone).unwrap();
        assert_eq!(index.rebuild_from_storage(&storage).unwrap(), 1);
        assert_eq!((q("keepme"), q("goneaway")), (1, 0));
        assert!(!index.is_indexed(&gone));
        assert_eq!(index.stats().unwrap().total_documents, 1);
    }

    #[test]
    fn test_rebuild_keeps_rows_it_did_not_rewrite_for_live_blobs() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("store")).unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        let q = |q: &str| {
            index
                .search(&SearchQuery {
                    q: q.into(),
                    limit: 10,
                    offset: 0,
                    doc_type: None,
                })
                .unwrap()
                .len()
        };
        let book = "d".repeat(64);
        storage
            .put_with_hash(b"%PDF-1.4", &book, "book.pdf")
            .unwrap();
        index
            .index_document(&book, "book.pdf", Some("zanzibar"))
            .unwrap();
        assert_eq!(index.rebuild_from_storage(&storage).unwrap(), 1);

        // A blob added and indexed after a rebuild listed storage (overlapping reindex) survives the prune.
        let late = "e".repeat(64);
        storage
            .put_with_hash(b"{}", &late, "latecomer.metadata")
            .unwrap();
        index
            .index_document(&late, "latecomer.metadata", None)
            .unwrap();
        assert_eq!(index.prune_missing(&storage).unwrap(), 0);
        assert_eq!(q("latecomer"), 1);

        // An unreadable (not deleted) PDF keeps its previous row and extracted text.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            index
                .index_document(&book, "book.pdf", Some("zanzibar"))
                .unwrap();
            let path = tmp.path().join("store").join(&book[..2]).join(&book);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
            if std::fs::read(&path).is_err() {
                // not root
                index.rebuild_from_storage(&storage).unwrap();
                assert_eq!(q("zanzibar"), 1, "read error must not erase the row");
            }
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
    }

    #[test]
    fn test_rebuild_prunes_blobs_whose_filename_was_repointed() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("store")).unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        let q = |q: &str| {
            index
                .search(&SearchQuery {
                    q: q.into(),
                    limit: 10,
                    offset: 0,
                    doc_type: None,
                })
                .unwrap()
        };
        let old = storage.put(b"{\"v\":1}", "notes.metadata").unwrap();
        assert_eq!(index.rebuild_from_storage(&storage).unwrap(), 1);
        assert!(index.is_indexed(&old));

        // Same filename, new content: the mapping moves to the new hash, the old blob stays on disk.
        let new = storage.put(b"{\"v\":2}", "notes.metadata").unwrap();
        assert_ne!(old, new);
        assert!(
            storage.exists(&old)
                && storage.hash_for_filename("notes.metadata").as_deref() == Some(new.as_str())
        );
        assert_eq!(index.rebuild_from_storage(&storage).unwrap(), 1);
        assert!(
            !index.is_indexed(&old),
            "unmapped blob's row must be pruned"
        );
        assert!(index.is_indexed(&new));
        assert_eq!(
            q("notes")
                .iter()
                .map(|r| r.hash.clone())
                .collect::<Vec<_>>(),
            vec![new]
        );
    }

    #[test]
    fn test_prune_repoints_row_to_other_current_filename() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("store")).unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        let q = |q: &str| {
            index
                .search(&SearchQuery {
                    q: q.into(),
                    limit: 10,
                    offset: 0,
                    doc_type: None,
                })
                .unwrap()
        };
        // Two filenames share one blob; the row was written under alpha (e.g. its last readable indexing).
        let old = storage.put(b"{\"v\":1}", "alpha.metadata").unwrap();
        assert_eq!(storage.put(b"{\"v\":1}", "beta.metadata").unwrap(), old);
        index
            .index_document(&old, "alpha.metadata", Some("zanzibar"))
            .unwrap();

        // alpha moves to new content; beta still maps the old blob, so its row is repointed, not kept stale.
        let new = storage.put(b"{\"v\":2}", "alpha.metadata").unwrap();
        index.index_document(&new, "alpha.metadata", None).unwrap();
        assert_eq!(index.prune_missing(&storage).unwrap(), 0);
        let hits = q("zanzibar");
        assert_eq!(
            hits.iter()
                .map(|r| (r.hash.clone(), r.filename.clone()))
                .collect::<Vec<_>>(),
            vec![(old.clone(), "beta.metadata".into())]
        );
        assert!(
            q("alpha").iter().all(|r| r.hash == new),
            "no row may keep alpha's stale name"
        );
        assert_eq!(
            q("beta").iter().map(|r| r.hash.clone()).collect::<Vec<_>>(),
            vec![old]
        );
    }

    /// Commit a new root that lists every document of the current one except `doc_id`,
    /// as the tablet does when a document is deleted. Returns the hashes that left the tree.
    fn delete_from_root(storage: &Storage, doc_id: &str) -> Vec<String> {
        let root = storage.get_root().hash;
        let doc = storage
            .index_entries(&root)
            .unwrap()
            .unwrap()
            .into_iter()
            .find(|e| e.name == doc_id)
            .unwrap();
        let mut dropped: Vec<String> = storage
            .index_entries(&doc.hash)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|f| f.hash)
            .collect();
        dropped.push(doc.hash.clone());
        let index: String = String::from_utf8(storage.get(&root).unwrap())
            .unwrap()
            .lines()
            .filter(|l| !l.starts_with(&doc.hash))
            .map(|l| format!("{l}\n"))
            .collect();
        let new_root = storage.put(index.as_bytes(), "root.docSchema").unwrap();
        storage.set_root(new_root).unwrap();
        // Identical files (e.g. two PDFs' `.content`) are one blob; keep only what left the tree.
        let still = storage.reachable_from_root().unwrap();
        dropped.retain(|h| !still.contains(h));
        dropped
    }

    #[test]
    fn test_rebuild_drops_documents_deleted_from_the_root() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("store")).unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        let (kept, _) =
            crate::documents::create_document(&storage, "Kept", "pdf", b"%PDF-1.4 kept").unwrap();
        let (gone, _) =
            crate::documents::create_document(&storage, "Gone", "pdf", b"%PDF-1.4 gone").unwrap();
        index.rebuild_from_storage(&storage).unwrap();
        let file_hash =
            |id: &str, ext: &str| storage.hash_for_filename(&format!("{id}.{ext}")).unwrap();
        let gone_pdf = file_hash(&gone, "pdf");
        assert!(index.is_indexed(&gone_pdf) && index.is_indexed(&file_hash(&kept, "pdf")));

        let dropped = delete_from_root(&storage, &gone);
        assert!(dropped.contains(&gone_pdf));
        // Still stored (nothing is garbage-collected yet), but no longer in the tree.
        assert!(storage.exists(&gone_pdf));
        index.rebuild_from_storage(&storage).unwrap();
        for hash in &dropped {
            assert!(
                !index.is_indexed(hash),
                "deleted document's {hash} still searchable"
            );
        }
        for ext in ["pdf", "metadata"] {
            assert!(index.is_indexed(&file_hash(&kept, ext)), "{ext}");
        }
        // Blobs outside the tree (e.g. an uncommitted upload) aren't indexed either.
        let stray = storage.put(b"{}", "stray.metadata").unwrap();
        index.rebuild_from_storage(&storage).unwrap();
        assert!(!index.is_indexed(&stray));
    }

    #[test]
    fn test_rebuild_indexes_everything_when_tree_is_unparsed() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("store")).unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        let doc = storage.put(b"{}", "notes.metadata").unwrap();
        index.index_document(&doc, "notes.metadata", None).unwrap();
        let root = "f".repeat(64);
        storage
            .put_with_hash(
                format!("7\n{doc}:future\n").as_bytes(),
                &root,
                "root.docSchema",
            )
            .unwrap();
        storage.set_root(root).unwrap();
        assert!(storage.reachable_from_root().is_none());

        // Search isn't wiped: every stored, current blob stays indexed.
        assert_eq!(index.rebuild_from_storage(&storage).unwrap(), 2);
        assert!(index.is_indexed(&doc));
    }

    #[test]
    fn test_truncate_on_char_boundary() {
        assert_eq!(truncate_on_char_boundary("abc", 10), "abc");
        assert_eq!(truncate_on_char_boundary("abc", 2), "ab");
        // 'é' is 2 bytes, '€' is 3: cutting inside either backs off to the previous boundary.
        assert_eq!(truncate_on_char_boundary("aé", 2), "a");
        assert_eq!(truncate_on_char_boundary("€€", 4), "€");
        let long = "€".repeat(MAX_INDEXED_TEXT); // 3 bytes each; MAX isn't a multiple of 3
        let cut = truncate_on_char_boundary(&long, MAX_INDEXED_TEXT);
        assert!(cut.len() <= MAX_INDEXED_TEXT && cut.len() > MAX_INDEXED_TEXT - 4);
    }

    #[test]
    fn test_document_type_detection() {
        assert_eq!(DocumentType::from_filename("notes.pdf"), DocumentType::Pdf);
        assert_eq!(
            DocumentType::from_filename("doc.content"),
            DocumentType::Document
        );
        assert_eq!(DocumentType::from_filename("Books/"), DocumentType::Folder);
        assert_eq!(
            DocumentType::from_filename("random.txt"),
            DocumentType::Unknown
        );
    }
}
