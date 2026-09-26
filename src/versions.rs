//! Document version history for remarkable-server
//!
//! Stores document versions on each sync, supporting:
//! - Configurable retention (last N versions, or time-based)
//! - Version diff (show changes between versions)
//! - Restore to previous version
//! - Version metadata (timestamp, device, size)
//!
//! Storage layout:
//! ```text
//! storage/
//!   versions.db       (SQLite metadata)
//!   versions/
//!     {doc_id}/
//!       {version}.content  (archived content snapshots)
//! ```

use crate::error::{Result, ServerError};
use crate::storage::Storage;
use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path as StdPath, PathBuf};
use std::sync::{Arc, Mutex};

/// Version retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RetentionPolicy {
    /// Keep last N versions per document
    Count { max_versions: usize },
    /// Keep versions newer than duration
    TimeBased { max_age_days: u32 },
    /// Keep both: last N versions AND anything within time window
    Combined { max_versions: usize, max_age_days: u32 },
    /// Keep everything (no automatic cleanup)
    Unlimited,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::Count { max_versions: 10 }
    }
}

/// Configuration for version manager
#[derive(Debug, Clone)]
pub struct VersionConfig {
    pub retention: RetentionPolicy,
    /// Whether to store full content or just metadata (content stays in main storage)
    pub store_content_snapshots: bool,
}

impl Default for VersionConfig {
    fn default() -> Self {
        Self {
            retention: RetentionPolicy::default(),
            store_content_snapshots: true,
        }
    }
}

/// Metadata for a single version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    /// Version number (monotonically increasing per document)
    pub version: u64,
    /// Document ID
    pub doc_id: String,
    /// Content hash at this version
    pub content_hash: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Device ID that created this version
    pub device_id: Option<String>,
    /// Size in bytes
    pub size: u64,
    /// Optional commit message / change description
    pub message: Option<String>,
}

/// Summary of changes between two versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionDiff {
    pub from_version: u64,
    pub to_version: u64,
    pub doc_id: String,
    /// Size difference in bytes (positive = grew, negative = shrunk)
    pub size_delta: i64,
    /// Whether content actually changed
    pub content_changed: bool,
    /// Byte-level diff stats
    pub diff_stats: DiffStats,
}

/// Statistics about byte-level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffStats {
    /// Bytes added
    pub bytes_added: usize,
    /// Bytes removed
    pub bytes_removed: usize,
    /// Bytes unchanged
    pub bytes_unchanged: usize,
    /// Similarity ratio (0.0 to 1.0)
    pub similarity: f64,
}

/// Version manager handles document versioning
#[derive(Clone)]
pub struct VersionManager {
    inner: Arc<VersionManagerInner>,
}

struct VersionManagerInner {
    /// Database connection (Mutex for thread safety)
    db: Mutex<Connection>,
    /// Base path for version storage
    base_path: PathBuf,
    /// Configuration
    config: RwLock<VersionConfig>,
    /// Reference to main storage
    storage: Storage,
}

// Safety: VersionManagerInner is Send+Sync because:
// - Mutex<Connection> provides thread-safe access to the non-Sync Connection
// - RwLock<VersionConfig> is Send+Sync
// - Storage is Clone (and Send+Sync)
// - PathBuf is Send+Sync
unsafe impl Send for VersionManagerInner {}
unsafe impl Sync for VersionManagerInner {}

impl VersionManager {
    /// Create new version manager
    pub fn new<P: AsRef<StdPath>>(path: P, storage: Storage, config: VersionConfig) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        fs::create_dir_all(base_path.join("versions"))?;

        let db_path = base_path.join("versions.db");
        let db = Connection::open(&db_path)?;

        // Initialize schema
        db.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                device_id TEXT,
                size INTEGER NOT NULL,
                message TEXT,
                has_snapshot INTEGER DEFAULT 0,
                UNIQUE(doc_id, version)
            );
            
            CREATE INDEX IF NOT EXISTS idx_versions_doc_id ON versions(doc_id);
            CREATE INDEX IF NOT EXISTS idx_versions_created_at ON versions(created_at);
            CREATE INDEX IF NOT EXISTS idx_versions_doc_version ON versions(doc_id, version DESC);
            "#,
        )?;

        Ok(Self {
            inner: Arc::new(VersionManagerInner {
                db: Mutex::new(db),
                base_path,
                config: RwLock::new(config),
                storage,
            }),
        })
    }

    /// Create a new version for a document
    pub fn create_version(
        &self,
        doc_id: &str,
        content: &[u8],
        device_id: Option<&str>,
        message: Option<&str>,
    ) -> Result<VersionInfo> {
        // Lock order: never hold `config` while acquiring (or holding) `db`.
        // Copy what we need out of the config first so a queued
        // `set_retention` writer can never deadlock against `apply_retention`.
        let store_content_snapshots = self.inner.config.read().store_content_snapshots;
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;

        // Calculate content hash
        let mut hasher = Sha256::new();
        hasher.update(content);
        let content_hash = hex::encode(hasher.finalize());

        // Get next version number
        let next_version: u64 = db
            .query_row(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM versions WHERE doc_id = ?",
                params![doc_id],
                |row| row.get(0),
            )
            .unwrap_or(1);

        let now = Utc::now();
        let size = content.len() as u64;

        // Check if content actually changed from last version
        let last_hash: Option<String> = db
            .query_row(
                "SELECT content_hash FROM versions WHERE doc_id = ? ORDER BY version DESC LIMIT 1",
                params![doc_id],
                |row| row.get(0),
            )
            .optional()?;

        // Skip if content identical to previous version
        if let Some(ref last) = last_hash {
            if last == &content_hash {
                drop(db);
                // Return existing version info
                return self.get_version(doc_id, next_version - 1);
            }
        }

        // Store content snapshot if configured
        let has_snapshot = if store_content_snapshots {
            let version_path = self.version_path(doc_id, next_version);
            if let Some(parent) = version_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&version_path, content)?;
            true
        } else {
            false
        };

        // Insert version record
        db.execute(
            r#"
            INSERT INTO versions (doc_id, version, content_hash, created_at, device_id, size, message, has_snapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                doc_id,
                next_version,
                content_hash,
                now.to_rfc3339(),
                device_id,
                size as i64,
                message,
                has_snapshot as i32
            ],
        )?;

        drop(db);

        // Apply retention policy
        self.apply_retention(doc_id)?;

        Ok(VersionInfo {
            version: next_version,
            doc_id: doc_id.to_string(),
            content_hash,
            created_at: now,
            device_id: device_id.map(String::from),
            size,
            message: message.map(String::from),
        })
    }

    /// Get path for a version snapshot
    fn version_path(&self, doc_id: &str, version: u64) -> PathBuf {
        self.inner
            .base_path
            .join("versions")
            .join(doc_id)
            .join(format!("{}.content", version))
    }

    /// List all versions for a document
    pub fn list_versions(&self, doc_id: &str) -> Result<Vec<VersionInfo>> {
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;
        let mut stmt = db.prepare(
            r#"
            SELECT version, doc_id, content_hash, created_at, device_id, size, message
            FROM versions
            WHERE doc_id = ?
            ORDER BY version DESC
            "#,
        )?;

        let versions = stmt
            .query_map(params![doc_id], |row| {
                Ok(VersionInfo {
                    version: row.get(0)?,
                    doc_id: row.get(1)?,
                    content_hash: row.get(2)?,
                    created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    device_id: row.get(4)?,
                    size: row.get::<_, i64>(5)? as u64,
                    message: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(versions)
    }

    /// Get a specific version
    pub fn get_version(&self, doc_id: &str, version: u64) -> Result<VersionInfo> {
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;
        db.query_row(
            r#"
            SELECT version, doc_id, content_hash, created_at, device_id, size, message
            FROM versions
            WHERE doc_id = ? AND version = ?
            "#,
            params![doc_id, version],
            |row| {
                Ok(VersionInfo {
                    version: row.get(0)?,
                    doc_id: row.get(1)?,
                    content_hash: row.get(2)?,
                    created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    device_id: row.get(4)?,
                    size: row.get::<_, i64>(5)? as u64,
                    message: row.get(6)?,
                })
            },
        )
        .map_err(|_| ServerError::NotFound(format!("version {} for doc {}", version, doc_id)))
    }

    /// Get content for a specific version
    pub fn get_version_content(&self, doc_id: &str, version: u64) -> Result<Vec<u8>> {
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;

        // Get version info
        let (content_hash, has_snapshot): (String, bool) = db
            .query_row(
                "SELECT content_hash, has_snapshot FROM versions WHERE doc_id = ? AND version = ?",
                params![doc_id, version],
                |row| Ok((row.get(0)?, row.get::<_, i32>(1)? != 0)),
            )
            .map_err(|_| ServerError::NotFound(format!("version {} for doc {}", version, doc_id)))?;

        drop(db);

        // Try snapshot first
        if has_snapshot {
            let path = self.version_path(doc_id, version);
            if path.exists() {
                return Ok(fs::read(path)?);
            }
        }

        // Fall back to main storage by hash
        self.inner.storage.get(&content_hash)
    }

    /// Compute diff between two versions
    pub fn diff_versions(&self, doc_id: &str, from_version: u64, to_version: u64) -> Result<VersionDiff> {
        let from = self.get_version(doc_id, from_version)?;
        let to = self.get_version(doc_id, to_version)?;

        let content_changed = from.content_hash != to.content_hash;
        let size_delta = to.size as i64 - from.size as i64;

        // Compute byte-level diff stats if content changed
        let diff_stats = if content_changed {
            let from_content = self.get_version_content(doc_id, from_version)?;
            let to_content = self.get_version_content(doc_id, to_version)?;
            compute_diff_stats(&from_content, &to_content)
        } else {
            DiffStats {
                bytes_added: 0,
                bytes_removed: 0,
                bytes_unchanged: from.size as usize,
                similarity: 1.0,
            }
        };

        Ok(VersionDiff {
            from_version,
            to_version,
            doc_id: doc_id.to_string(),
            size_delta,
            content_changed,
            diff_stats,
        })
    }

    /// Restore a document to a previous version
    pub fn restore_version(&self, doc_id: &str, version: u64) -> Result<VersionInfo> {
        // Get the content from the version to restore
        let content = self.get_version_content(doc_id, version)?;

        // Create a new version with the restored content
        let restored = self.create_version(
            doc_id,
            &content,
            None,
            Some(&format!("Restored from version {}", version)),
        )?;

        // Also update main storage
        let filename = format!("{}.content", doc_id);
        self.inner.storage.put(&content, &filename)?;

        Ok(restored)
    }

    /// Apply retention policy to a document's versions; returns how many were deleted
    fn apply_retention(&self, doc_id: &str) -> Result<usize> {
        // Snapshot the policy and release the config lock before taking `db`
        // (see lock-order note in `create_version`).
        let retention = self.inner.config.read().retention.clone();
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;
        let mut deleted = 0;

        match &retention {
            RetentionPolicy::Count { max_versions } => {
                // Delete versions beyond the limit
                let mut stmt = db.prepare(
                    r#"
                    SELECT version FROM versions
                    WHERE doc_id = ?
                    ORDER BY version DESC
                    LIMIT -1 OFFSET ?
                    "#,
                )?;
                let versions_to_delete: Vec<u64> = stmt
                    .query_map(params![doc_id, *max_versions], |row| row.get(0))?
                    .collect::<std::result::Result<_, _>>()?;
                drop(stmt);

                for v in versions_to_delete {
                    self.delete_version_internal(&db, doc_id, v)?;
                    deleted += 1;
                }
            }
            RetentionPolicy::TimeBased { max_age_days } => {
                let cutoff = Utc::now() - Duration::days(*max_age_days as i64);
                let mut stmt = db.prepare(
                    r#"
                    SELECT version FROM versions
                    WHERE doc_id = ? AND created_at < ?
                    "#,
                )?;
                let versions_to_delete: Vec<u64> = stmt
                    .query_map(params![doc_id, cutoff.to_rfc3339()], |row| row.get(0))?
                    .collect::<std::result::Result<_, _>>()?;
                drop(stmt);

                for v in versions_to_delete {
                    self.delete_version_internal(&db, doc_id, v)?;
                    deleted += 1;
                }
            }
            RetentionPolicy::Combined { max_versions, max_age_days } => {
                let cutoff = Utc::now() - Duration::days(*max_age_days as i64);
                
                // Keep versions that are either in the last N OR within time window
                let mut stmt = db.prepare(
                    r#"
                    SELECT version FROM versions
                    WHERE doc_id = ?
                    AND version NOT IN (
                        SELECT version FROM versions
                        WHERE doc_id = ?
                        ORDER BY version DESC
                        LIMIT ?
                    )
                    AND created_at < ?
                    "#,
                )?;
                let versions_to_delete: Vec<u64> = stmt
                    .query_map(
                        params![doc_id, doc_id, *max_versions, cutoff.to_rfc3339()],
                        |row| row.get(0),
                    )?
                    .collect::<std::result::Result<_, _>>()?;
                drop(stmt);

                for v in versions_to_delete {
                    self.delete_version_internal(&db, doc_id, v)?;
                    deleted += 1;
                }
            }
            RetentionPolicy::Unlimited => {
                // Do nothing
            }
        }

        Ok(deleted)
    }

    /// Delete a version (internal, assumes db lock held)
    fn delete_version_internal(&self, db: &Connection, doc_id: &str, version: u64) -> Result<()> {
        // Delete snapshot file if exists
        let path = self.version_path(doc_id, version);
        if path.exists() {
            fs::remove_file(path)?;
        }

        // Delete from database
        db.execute(
            "DELETE FROM versions WHERE doc_id = ? AND version = ?",
            params![doc_id, version],
        )?;

        Ok(())
    }

    /// Update retention policy
    pub fn set_retention(&self, policy: RetentionPolicy) {
        self.inner.config.write().retention = policy;
    }

    /// Get current retention policy
    pub fn get_retention(&self) -> RetentionPolicy {
        self.inner.config.read().retention.clone()
    }

    /// Get all document IDs with versions
    pub fn list_documents(&self) -> Result<Vec<String>> {
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;
        let mut stmt = db.prepare("SELECT DISTINCT doc_id FROM versions ORDER BY doc_id")?;
        let docs = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(docs)
    }

    /// Get version count for a document
    pub fn version_count(&self, doc_id: &str) -> Result<usize> {
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;
        let count: i64 = db.query_row(
            "SELECT COUNT(*) FROM versions WHERE doc_id = ?",
            params![doc_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Get storage statistics
    pub fn stats(&self) -> Result<VersionStats> {
        let db = self.inner.db.lock().map_err(|e| ServerError::Internal(e.to_string()))?;

        let total_versions: i64 =
            db.query_row("SELECT COUNT(*) FROM versions", [], |row| row.get(0))?;

        let total_documents: i64 = db.query_row(
            "SELECT COUNT(DISTINCT doc_id) FROM versions",
            [],
            |row| row.get(0),
        )?;

        let total_size: i64 = db.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM versions WHERE has_snapshot = 1",
            [],
            |row| row.get(0),
        )?;

        let oldest: Option<String> = db
            .query_row(
                "SELECT MIN(created_at) FROM versions",
                [],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        let newest: Option<String> = db
            .query_row(
                "SELECT MAX(created_at) FROM versions",
                [],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        drop(db);

        Ok(VersionStats {
            total_versions: total_versions as usize,
            total_documents: total_documents as usize,
            total_snapshot_bytes: total_size as u64,
            oldest_version: oldest.and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
            newest_version: newest.and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.with_timezone(&Utc))),
            retention_policy: self.inner.config.read().retention.clone(),
        })
    }

    /// Prune all documents according to retention policy
    pub fn prune_all(&self) -> Result<usize> {
        let docs = self.list_documents()?;
        let mut pruned = 0;

        for doc_id in docs {
            // Count deletions directly: a before/after count across separate
            // lock acquisitions underflows if a concurrent sync adds versions.
            pruned += self.apply_retention(&doc_id)?;
        }

        Ok(pruned)
    }
}

/// Version storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionStats {
    pub total_versions: usize,
    pub total_documents: usize,
    pub total_snapshot_bytes: u64,
    pub oldest_version: Option<DateTime<Utc>>,
    pub newest_version: Option<DateTime<Utc>>,
    pub retention_policy: RetentionPolicy,
}

/// Compute diff statistics between two byte arrays
fn compute_diff_stats(from: &[u8], to: &[u8]) -> DiffStats {
    // Position-aware byte matching
    let mut matched = 0;
    let min_len = from.len().min(to.len());
    for i in 0..min_len {
        if from[i] == to[i] {
            matched += 1;
        }
    }
    
    let total_bytes = from.len().max(to.len());
    let similarity = if total_bytes > 0 {
        matched as f64 / total_bytes as f64
    } else {
        1.0
    };

    // Calculate actual added/removed based on size difference
    let bytes_added = if to.len() > from.len() {
        to.len() - from.len()
    } else {
        (0..min_len).filter(|&i| from[i] != to[i]).count()
    };

    let bytes_removed = if from.len() > to.len() {
        from.len() - to.len()
    } else {
        (0..min_len).filter(|&i| from[i] != to[i]).count()
    };

    DiffStats {
        bytes_added,
        bytes_removed,
        bytes_unchanged: matched,
        similarity,
    }
}

// ============================================================================
// API Handlers
// ============================================================================

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

/// Shared state for version API
#[derive(Clone)]
pub struct VersionState {
    pub manager: VersionManager,
}

/// Response for version list
#[derive(Debug, Serialize)]
pub struct VersionListResponse {
    pub doc_id: String,
    pub versions: Vec<VersionInfo>,
    pub total: usize,
}

/// Response for restore operation
#[derive(Debug, Serialize)]
pub struct RestoreResponse {
    pub restored_from: u64,
    pub new_version: VersionInfo,
}

/// GET /versions/v1/{doc_id} - list versions
pub async fn list_versions(
    State(state): State<VersionState>,
    Path(doc_id): Path<String>,
) -> Result<Json<VersionListResponse>> {
    let versions = state.manager.list_versions(&doc_id)?;
    let total = versions.len();
    
    Ok(Json(VersionListResponse {
        doc_id,
        versions,
        total,
    }))
}

/// GET /versions/v1/{doc_id}/{version} - get specific version
pub async fn get_version(
    State(state): State<VersionState>,
    Path((doc_id, version)): Path<(String, u64)>,
) -> Result<Json<VersionInfo>> {
    let info = state.manager.get_version(&doc_id, version)?;
    Ok(Json(info))
}

/// GET /versions/v1/{doc_id}/{version}/content - download version content
pub async fn get_version_content(
    State(state): State<VersionState>,
    Path((doc_id, version)): Path<(String, u64)>,
) -> Result<Vec<u8>> {
    state.manager.get_version_content(&doc_id, version)
}

/// POST /versions/v1/{doc_id}/restore/{version} - restore to version
pub async fn restore_version(
    State(state): State<VersionState>,
    Path((doc_id, version)): Path<(String, u64)>,
) -> Result<Json<RestoreResponse>> {
    let new_version = state.manager.restore_version(&doc_id, version)?;
    
    Ok(Json(RestoreResponse {
        restored_from: version,
        new_version,
    }))
}

/// GET /versions/v1/{doc_id}/diff/{v1}/{v2} - diff between versions
pub async fn diff_versions(
    State(state): State<VersionState>,
    Path((doc_id, v1, v2)): Path<(String, u64, u64)>,
) -> Result<Json<VersionDiff>> {
    let diff = state.manager.diff_versions(&doc_id, v1, v2)?;
    Ok(Json(diff))
}

/// GET /versions/v1/stats - get version storage stats
pub async fn get_stats(State(state): State<VersionState>) -> Result<Json<VersionStats>> {
    let stats = state.manager.stats()?;
    Ok(Json(stats))
}

/// GET /versions/v1/retention - get retention policy
pub async fn get_retention(State(state): State<VersionState>) -> Json<RetentionPolicy> {
    Json(state.manager.get_retention())
}

/// PUT /versions/v1/retention - set retention policy
pub async fn set_retention(
    State(state): State<VersionState>,
    Json(policy): Json<RetentionPolicy>,
) -> StatusCode {
    state.manager.set_retention(policy);
    StatusCode::OK
}

/// POST /versions/v1/prune - prune all documents
pub async fn prune_all(State(state): State<VersionState>) -> Result<Json<PruneResponse>> {
    let pruned = state.manager.prune_all()?;
    Ok(Json(PruneResponse { versions_pruned: pruned }))
}

#[derive(Debug, Serialize)]
pub struct PruneResponse {
    pub versions_pruned: usize,
}

/// Create router for version API
pub fn version_router(state: VersionState) -> axum::Router {
    use axum::routing::{get, post, put};
    
    axum::Router::new()
        .route("/stats", get(get_stats))
        .route("/retention", get(get_retention))
        .route("/retention", put(set_retention))
        .route("/prune", post(prune_all))
        .route("/{doc_id}", get(list_versions))
        .route("/{doc_id}/{version}", get(get_version))
        .route("/{doc_id}/{version}/content", get(get_version_content))
        .route("/{doc_id}/restore/{version}", post(restore_version))
        .route("/{doc_id}/diff/{v1}/{v2}", get(diff_versions))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, VersionManager) {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let config = VersionConfig::default();
        let manager = VersionManager::new(tmp.path().join("versions"), storage, config).unwrap();
        (tmp, manager)
    }

    #[test]
    fn test_create_version() {
        let (_tmp, manager) = setup();

        let content = b"hello world";
        let v = manager
            .create_version("doc1", content, Some("device1"), Some("initial"))
            .unwrap();

        assert_eq!(v.version, 1);
        assert_eq!(v.doc_id, "doc1");
        assert_eq!(v.size, 11);
        assert_eq!(v.device_id, Some("device1".to_string()));
        assert_eq!(v.message, Some("initial".to_string()));
    }

    #[test]
    fn test_version_dedup() {
        let (_tmp, manager) = setup();

        let content = b"same content";
        let v1 = manager.create_version("doc1", content, None, None).unwrap();
        let v2 = manager.create_version("doc1", content, None, None).unwrap();

        // Same content should return same version
        assert_eq!(v1.version, v2.version);
    }

    #[test]
    fn test_list_versions() {
        let (_tmp, manager) = setup();

        manager.create_version("doc1", b"v1", None, None).unwrap();
        manager.create_version("doc1", b"v2", None, None).unwrap();
        manager.create_version("doc1", b"v3", None, None).unwrap();

        let versions = manager.list_versions("doc1").unwrap();
        assert_eq!(versions.len(), 3);
        // Should be ordered newest first
        assert_eq!(versions[0].version, 3);
        assert_eq!(versions[2].version, 1);
    }

    #[test]
    fn test_get_version_content() {
        let (_tmp, manager) = setup();

        let content = b"test content";
        manager.create_version("doc1", content, None, None).unwrap();

        let retrieved = manager.get_version_content("doc1", 1).unwrap();
        assert_eq!(retrieved, content);
    }

    #[test]
    fn test_diff_versions() {
        let (_tmp, manager) = setup();

        manager.create_version("doc1", b"hello", None, None).unwrap();
        manager.create_version("doc1", b"hello world", None, None).unwrap();

        let diff = manager.diff_versions("doc1", 1, 2).unwrap();
        assert!(diff.content_changed);
        assert_eq!(diff.size_delta, 6); // " world" = 6 bytes
    }

    #[test]
    fn test_restore_version() {
        let (_tmp, manager) = setup();

        manager.create_version("doc1", b"original", None, None).unwrap();
        manager.create_version("doc1", b"modified", None, None).unwrap();

        let restored = manager.restore_version("doc1", 1).unwrap();
        assert_eq!(restored.version, 3);

        let content = manager.get_version_content("doc1", 3).unwrap();
        assert_eq!(content, b"original");
    }

    #[test]
    fn test_retention_count() {
        let (_tmp, manager) = setup();
        manager.set_retention(RetentionPolicy::Count { max_versions: 2 });

        for i in 0..5 {
            manager
                .create_version("doc1", format!("content {}", i).as_bytes(), None, None)
                .unwrap();
        }

        let versions = manager.list_versions("doc1").unwrap();
        assert_eq!(versions.len(), 2);
        // Should keep newest
        assert_eq!(versions[0].version, 5);
        assert_eq!(versions[1].version, 4);
    }

    #[test]
    fn test_stats() {
        let (_tmp, manager) = setup();

        manager.create_version("doc1", b"content1", None, None).unwrap();
        manager.create_version("doc1", b"content2", None, None).unwrap();
        manager.create_version("doc2", b"other", None, None).unwrap();

        let stats = manager.stats().unwrap();
        assert_eq!(stats.total_versions, 3);
        assert_eq!(stats.total_documents, 2);
    }

    /// Regression: create_version used to take db -> config while
    /// apply_retention took config -> db; with a queued set_retention writer
    /// (parking_lot is writer-fair) that deadlocked. Hammer all three paths
    /// concurrently and fail (instead of hanging) if they don't finish.
    #[test]
    fn test_concurrent_create_and_set_retention_no_deadlock() {
        let (_tmp, manager) = setup();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut handles = Vec::new();
        for t in 0..4 {
            let m = manager.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..40 {
                    m.create_version(&format!("doc{}", t % 2), format!("c{} {}", t, i).as_bytes(), None, None).unwrap();
                }
            }));
        }
        for t in 0..2 {
            let m = manager.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..300 {
                    let p = if (i + t) % 2 == 0 { RetentionPolicy::Count { max_versions: 3 } } else { RetentionPolicy::Unlimited };
                    m.set_retention(p);
                    let _ = m.get_retention();
                    let _ = m.stats().unwrap();
                }
            }));
        }
        let m = manager.clone();
        handles.push(std::thread::spawn(move || { for _ in 0..10 { m.prune_all().unwrap(); } }));
        std::thread::spawn(move || { let ok = handles.into_iter().all(|h| h.join().is_ok()); let _ = done_tx.send(ok); });
        let ok = done_rx.recv_timeout(std::time::Duration::from_secs(60)).expect("create_version/set_retention deadlocked");
        assert!(ok, "a worker thread panicked");
    }
}
