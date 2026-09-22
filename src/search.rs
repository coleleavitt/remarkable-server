//! FTS5 full-text search for remarkable-server
//!
//! Provides:
//! - FTS5 virtual table for document content
//! - Document/folder name indexing
//! - PDF text extraction via pdftotext
//! - Highlight matches
//! - Incremental indexing on sync

use crate::error::{Result, ServerError};
use crate::storage::Storage;
use rusqlite::{Connection, params};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Search index using SQLite FTS5
#[derive(Clone)]
pub struct SearchIndex {
    inner: Arc<SearchIndexInner>,
}

struct SearchIndexInner {
    conn: Mutex<Connection>,
    storage_path: PathBuf,
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

fn default_limit() -> usize { 20 }

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
                storage_path,
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
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
        // FTS5 virtual table for full-text search
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                filename,
                content,
                content='documents',
                content_rowid='rowid'
            )",
            [],
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
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
            END;"
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
        // Index for fast lookups
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_filename ON documents(filename)",
            [],
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
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
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
        debug!("Indexed document: {} ({})", filename, hash);
        Ok(())
    }
    
    /// Index a PDF by extracting text with pdftotext
    pub fn index_pdf(&self, hash: &str, filename: &str, pdf_data: &[u8]) -> Result<()> {
        // Write PDF to temp file
        let temp_dir = std::env::temp_dir();
        let temp_pdf = temp_dir.join(format!("{}.pdf", hash));
        std::fs::write(&temp_pdf, pdf_data)?;
        
        // Extract text with pdftotext
        let output = Command::new("pdftotext")
            .args(["-layout", "-enc", "UTF-8"])
            .arg(&temp_pdf)
            .arg("-")
            .output();
        
        // Clean up temp file
        let _ = std::fs::remove_file(&temp_pdf);
        
        let content = match output {
            Ok(out) if out.status.success() => {
                let text = String::from_utf8_lossy(&out.stdout).to_string();
                // Truncate to reasonable size for indexing
                if text.len() > 100_000 {
                    Some(text[..100_000].to_string())
                } else {
                    Some(text)
                }
            }
            Ok(out) => {
                warn!("pdftotext failed for {}: {}", filename, String::from_utf8_lossy(&out.stderr));
                None
            }
            Err(e) => {
                warn!("pdftotext not available: {}", e);
                None
            }
        };
        
        self.index_document(hash, filename, content.as_deref())
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
            
            let mut stmt = conn.prepare(sql)
                .map_err(|e| ServerError::Database(e.to_string()))?;
            
            let rows = stmt.query_map(
                params![&fts_query, dt.as_str(), query.limit as i64, query.offset as i64],
                |row| Self::row_to_result(row),
            ).map_err(|e| ServerError::Database(e.to_string()))?;
            
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
            
            let mut stmt = conn.prepare(sql)
                .map_err(|e| ServerError::Database(e.to_string()))?;
            
            let rows = stmt.query_map(
                params![&fts_query, query.limit as i64, query.offset as i64],
                |row| Self::row_to_result(row),
            ).map_err(|e| ServerError::Database(e.to_string()))?;
            
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
            snippet: row.get(3)?,
            rank: row.get(4)?,
        })
    }
    
    /// Prepare query for FTS5 (handle special characters)
    fn prepare_fts_query(query: &str) -> String {
        // If query contains special FTS operators, use as-is
        if query.contains('"') || query.contains('*') || query.contains("OR") || query.contains("AND") {
            return query.to_string();
        }
        
        // Otherwise, prefix-match each word for partial matching
        query.split_whitespace()
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
        
        let total_docs: i64 = conn.query_row(
            "SELECT COUNT(*) FROM documents",
            [],
            |row| row.get(0),
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
        let indexed_content: i64 = conn.query_row(
            "SELECT COUNT(*) FROM documents WHERE content IS NOT NULL",
            [],
            |row| row.get(0),
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
        let pdf_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM documents WHERE doc_type = 'pdf'",
            [],
            |row| row.get(0),
        ).map_err(|e| ServerError::Database(e.to_string()))?;
        
        Ok(IndexStats {
            total_documents: total_docs as usize,
            documents_with_content: indexed_content as usize,
            pdf_count: pdf_count as usize,
        })
    }
    
    /// Rebuild entire index from storage
    pub fn rebuild_from_storage(&self, storage: &Storage) -> Result<usize> {
        info!("Rebuilding search index from storage...");
        
        let hashes = storage.list_hashes()?;
        let mut indexed = 0;
        
        for hash in &hashes {
            if let Some(filename) = storage.filename_for_hash(hash) {
                let doc_type = DocumentType::from_filename(&filename);
                
                match doc_type {
                    DocumentType::Pdf => {
                        if let Ok(data) = storage.get(hash) {
                            if let Err(e) = self.index_pdf(hash, &filename, &data) {
                                warn!("Failed to index PDF {}: {}", filename, e);
                            } else {
                                indexed += 1;
                            }
                        }
                    }
                    DocumentType::Document | DocumentType::Folder => {
                        // Index just the filename for non-PDF documents
                        if let Err(e) = self.index_document(hash, &filename, None) {
                            warn!("Failed to index {}: {}", filename, e);
                        } else {
                            indexed += 1;
                        }
                    }
                    _ => {
                        // Index filename for other types too
                        let _ = self.index_document(hash, &filename, None);
                        indexed += 1;
                    }
                }
            }
        }
        
        info!("Indexed {} documents", indexed);
        Ok(indexed)
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
        ).is_ok()
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
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_search_index_basic() {
        let tmp = TempDir::new().unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        
        // Index documents
        index.index_document("hash1", "Meeting Notes.content", Some("Important meeting about Q4 planning")).unwrap();
        index.index_document("hash2", "Project Ideas.content", Some("Ideas for the new project")).unwrap();
        index.index_document("hash3", "Archive/", None).unwrap();
        
        // Search
        let results = index.search(&SearchQuery {
            q: "meeting".to_string(),
            limit: 10,
            offset: 0,
            doc_type: None,
        }).unwrap();
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].hash, "hash1");
        assert!(results[0].snippet.contains("<mark>"));
    }

    #[test]
    fn test_search_prefix_matching() {
        let tmp = TempDir::new().unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        
        index.index_document("hash1", "development.content", Some("Software development notes")).unwrap();
        
        // Partial match should work
        let results = index.search(&SearchQuery {
            q: "develop".to_string(),
            limit: 10,
            offset: 0,
            doc_type: None,
        }).unwrap();
        
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_type_filter() {
        let tmp = TempDir::new().unwrap();
        let index = SearchIndex::new(tmp.path()).unwrap();
        
        index.index_document("hash1", "doc.content", Some("document content")).unwrap();
        index.index_document("hash2", "file.pdf", Some("pdf content")).unwrap();
        
        // Filter by type
        let results = index.search(&SearchQuery {
            q: "content".to_string(),
            limit: 10,
            offset: 0,
            doc_type: Some(DocumentType::Pdf),
        }).unwrap();
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_type, DocumentType::Pdf);
    }

    #[test]
    fn test_document_type_detection() {
        assert_eq!(DocumentType::from_filename("notes.pdf"), DocumentType::Pdf);
        assert_eq!(DocumentType::from_filename("doc.content"), DocumentType::Document);
        assert_eq!(DocumentType::from_filename("Books/"), DocumentType::Folder);
        assert_eq!(DocumentType::from_filename("random.txt"), DocumentType::Unknown);
    }
}
