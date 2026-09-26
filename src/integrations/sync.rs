//! Bidirectional sync engine
//!
//! Handles sync between local remarkable storage and cloud providers.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::integrations::conflict::{
    Conflict,
    ConflictResolution,
    ConflictResolver,
    ConflictStrategy,
    ConflictType,
};
use crate::integrations::{CloudFile, CloudProvider, IntegrationError, Result, SyncFolderConfig};

/// Longest path segment, in bytes, that a local path may have: `NAME_MAX` on the usual Linux
/// filesystems (ext4, XFS, Btrfs). Dropbox and OneDrive allow names of 255 *characters*, so
/// a name with multi-byte characters can be valid remotely but impossible to create here.
pub(crate) const MAX_NAME_BYTES: usize = 255;

/// Split a *relative* sync path into components, rejecting anything that could escape
/// the sync root: absolute paths, `..`/`.`/empty segments, backslashes, NUL, drive prefixes.
/// Remote file names are attacker-controlled, so every local path is built from this.
/// Segments longer than [`MAX_NAME_BYTES`] are rejected too: they could never be written
/// locally, and rejecting them up front makes that a permanent failure rather than a write
/// error on every sync.
pub(crate) fn safe_components(path: &str) -> Result<Vec<&str>> {
    let bad = |why: &str| {
        Err(IntegrationError::InvalidPath(format!(
            "{:?}: {}",
            path, why
        )))
    };
    if path.is_empty() {
        return bad("empty");
    }
    if path.contains('\0') {
        return bad("NUL byte");
    }
    if path.contains('\\') {
        return bad("backslash");
    }
    if path.starts_with('/') {
        return bad("absolute path");
    }
    let parts: Vec<&str> = path.split('/').collect();
    if let Some(first) = parts.first() {
        let b = first.as_bytes();
        if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
            return bad("drive prefix");
        }
    }
    for part in &parts {
        if part.is_empty() || *part == "." || *part == ".." {
            return bad("empty, '.' or '..' segment");
        }
        if part.len() > MAX_NAME_BYTES {
            return bad("segment longer than 255 bytes");
        }
        let mut comps = Path::new(part).components();
        if !matches!(
            (comps.next(), comps.next()),
            (Some(std::path::Component::Normal(_)), None)
        ) {
            return bad("not a plain path segment");
        }
    }
    Ok(parts)
}

/// Whether a remote item name is usable as one local path segment. Providers build listing
/// paths from names, so an item (or a folder on its path) failing this is skipped.
pub(crate) fn is_safe_name(name: &str) -> bool {
    matches!(safe_components(name).as_deref(), Ok([_]))
}

/// Deepest relative path (in components) a provider listing or change feed returns; deeper
/// items are skipped, so a pathological tree can't make a sync walk forever.
pub(crate) const MAX_LIST_DEPTH: usize = 64;

/// Components of a provider path. Providers root paths at `/` (e.g. `/Notes/a.pdf`), meaning
/// the sync root, so exactly one leading slash is stripped before validation.
pub(crate) fn cloud_path_components(cloud_path: &str) -> Result<Vec<&str>> {
    safe_components(cloud_path.strip_prefix('/').unwrap_or(cloud_path))
}

/// Local destination for a provider path, guaranteed (lexically) to stay under `base`.
pub(crate) fn local_path_for(base: &Path, cloud_path: &str) -> Result<PathBuf> {
    let joined = cloud_path_components(cloud_path)?
        .iter()
        .fold(base.to_path_buf(), |p, c| p.join(c));
    if !joined.starts_with(base) || joined == base {
        return Err(IntegrationError::InvalidPath(format!(
            "{:?} escapes sync root",
            cloud_path
        )));
    }
    Ok(joined)
}

/// Create `root/rel` one level at a time from the canonical root, resolving each existing
/// entry and refusing (`Ok(None)`) any that lands outside the root or isn't a directory, so
/// a symlinked subdirectory can't make us create directories elsewhere. Returns the
/// canonical directory. `rel` must already be validated (plain `Normal` components).
pub(crate) async fn create_dirs_within(root: &Path, rel: &Path) -> Result<Option<PathBuf>> {
    let root = fs::canonicalize(root).await?;
    let mut cur = root.clone();
    for c in rel.components() {
        let next = cur.join(c);
        match fs::create_dir(&next).await {
            Ok(()) => {
                cur = next;
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let real = fs::canonicalize(&next).await?;
        if !real.starts_with(&root) || !fs::metadata(&real).await?.is_dir() {
            return Ok(None);
        }
        cur = real;
    }
    Ok(Some(cur))
}

/// Replace `target` (in `dir`) via a fresh temp file + rename: `create_new` never follows a
/// symlink and `rename` replaces the directory entry rather than writing through it, so a
/// symlink swapped in after our checks can't redirect the content. Also makes writes atomic.
async fn write_replace(dir: &Path, target: &Path, content: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let tmp = dir.join(format!(".rms-sync-{}.tmp", uuid::Uuid::new_v4()));
    let res = async {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        f.write_all(content).await?;
        f.sync_all().await?;
        fs::rename(&tmp, target).await
    }
    .await;
    if res.is_err() {
        let _ = fs::remove_file(&tmp).await;
    }
    Ok(res?)
}

/// Classify a failed local write of the remote file at `cloud_path`: errors that come back the
/// same on every retry (something that isn't a directory where one is needed or the other way
/// round, a name the filesystem rejects) become the permanent
/// [`LocalPathUnusable`](IntegrationError::LocalPathUnusable), so they don't hold a delta
/// cursor forever. Other errors (disk full, permission denied) are left as they are.
fn local_write_error(cloud_path: &str, e: IntegrationError) -> IntegrationError {
    use std::io::ErrorKind::{InvalidFilename, IsADirectory, NotADirectory};
    match e {
        IntegrationError::Io(io)
            if matches!(io.kind(), IsADirectory | NotADirectory | InvalidFilename) =>
        {
            IntegrationError::LocalPathUnusable(format!("{:?}: {}", cloud_path, io))
        }
        e => e,
    }
}

/// Sync direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirection {
    /// Upload only (local → cloud)
    Upload,
    /// Download only (cloud → local)
    Download,
    /// Bidirectional sync
    Bidirectional,
}

impl Default for SyncDirection {
    fn default() -> Self {
        Self::Bidirectional
    }
}

/// Sync operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub status: SyncStatus,
    pub uploaded: usize,
    pub downloaded: usize,
    pub deleted: usize,
    pub conflicts: Vec<Conflict>,
    pub errors: Vec<String>,
    pub duration_ms: u64,
}

/// Sync status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    Success,
    PartialSuccess,
    Failed,
    Cancelled,
}

/// Sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Local base path for sync
    pub local_path: PathBuf,
    /// Cloud folder ID or path (None = root)
    pub cloud_folder: Option<String>,
    /// Sync direction
    pub direction: SyncDirection,
    /// Conflict resolution strategy
    pub conflict_strategy: ConflictStrategy,
    /// Selective folder configs
    pub folder_configs: Vec<SyncFolderConfig>,
    /// File patterns to exclude (glob)
    pub exclude_patterns: Vec<String>,
    /// Maximum file size to sync (bytes)
    pub max_file_size: Option<u64>,
    /// Sync hidden files (starting with .)
    pub sync_hidden: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            local_path: PathBuf::from("."),
            cloud_folder: None,
            direction: SyncDirection::Bidirectional,
            conflict_strategy: ConflictStrategy::NewerWins,
            folder_configs: Vec::new(),
            exclude_patterns: vec![
                "*.tmp".into(),
                "*.temp".into(),
                ".DS_Store".into(),
                "Thumbs.db".into(),
            ],
            max_file_size: Some(100 * 1024 * 1024), // 100MB default
            sync_hidden: false,
        }
    }
}

/// Sync state for tracking changes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncState {
    /// Last sync timestamp
    pub last_sync: Option<i64>,
    /// Provider-specific cursor for delta sync
    pub cursor: Option<String>,
    /// Map of local path to cloud file ID
    pub file_map: HashMap<String, String>,
    /// Map of local path to last known hash
    pub hash_map: HashMap<String, String>,
    /// Map of local path to last sync modification time
    pub mtime_map: HashMap<String, i64>,
}

/// Cloud sync engine
pub struct CloudSync<P: CloudProvider> {
    provider: P,
    config: SyncConfig,
    state: SyncState,
    conflict_resolver: ConflictResolver,
}

impl<P: CloudProvider> CloudSync<P> {
    pub fn new(provider: P, config: SyncConfig) -> Self {
        let conflict_resolver = ConflictResolver::new(config.conflict_strategy);
        Self {
            provider,
            config,
            state: SyncState::default(),
            conflict_resolver,
        }
    }

    pub fn with_state(provider: P, config: SyncConfig, state: SyncState) -> Self {
        let conflict_resolver = ConflictResolver::new(config.conflict_strategy);
        Self {
            provider,
            config,
            state,
            conflict_resolver,
        }
    }

    /// Get current sync state
    pub fn state(&self) -> &SyncState {
        &self.state
    }

    /// Perform full sync
    pub async fn sync(&mut self) -> Result<SyncResult> {
        Ok(self.reconcile(LocalOnly::Upload).await?.result)
    }

    /// Whether `f` is a file over `max_file_size`. Such remote files are never fetched: a
    /// download is held in memory whole and lands on the server's own disk (the local scan
    /// skips oversized files the same way).
    fn too_large(&self, f: &CloudFile) -> bool {
        !f.is_folder && self.config.max_file_size.is_some_and(|max| f.size > max)
    }

    /// Full sync: compare the whole listing with the local tree and transfer what differs.
    async fn reconcile(&mut self, local_only: LocalOnly) -> Result<Reconciled> {
        let start = std::time::Instant::now();
        let mut result = SyncResult {
            status: SyncStatus::Success,
            uploaded: 0,
            downloaded: 0,
            deleted: 0,
            conflicts: Vec::new(),
            errors: Vec::new(),
            duration_ms: 0,
        };

        // Get cloud files
        let cloud_files = match self
            .provider
            .list_files(self.config.cloud_folder.as_deref())
            .await
        {
            Ok(files) => files,
            Err(e) => {
                result.status = SyncStatus::Failed;
                result
                    .errors
                    .push(format!("Failed to list cloud files: {}", e));
                result.duration_ms = start.elapsed().as_millis() as u64;
                return Ok(Reconciled {
                    result,
                    retry_needed: true,
                });
            }
        };

        // Build cloud file map
        let mut cloud_map: HashMap<String, CloudFile> = cloud_files
            .into_iter()
            .map(|f| (f.path.clone(), f))
            .collect();

        // Get local files
        let mut local_files = match self.list_local_files().await {
            Ok(files) => files,
            Err(e) => {
                result.status = SyncStatus::Failed;
                result
                    .errors
                    .push(format!("Failed to list local files: {}", e));
                result.duration_ms = start.elapsed().as_millis() as u64;
                return Ok(Reconciled {
                    result,
                    retry_needed: true,
                });
            }
        };

        // A path whose remote file is too large is left alone in both directions: not
        // downloaded, and a local file there isn't uploaded over it either.
        cloud_map.retain(|path, f| {
            let keep = !self.too_large(f);
            if !keep {
                tracing::warn!(
                    "cloud sync: skipping {:?}: {} bytes is over the size limit",
                    path,
                    f.size
                );
                local_files.remove(path);
            }
            keep
        });

        // Set when a remote file wasn't fetched for a reason that may go away; a resync then
        // keeps its old cursor so the fetch is retried (see `resync`).
        let mut retry_needed = false;

        // Build sets for comparison
        let local_paths: HashSet<String> = local_files.keys().cloned().collect();
        let cloud_paths: HashSet<String> = cloud_map.keys().cloned().collect();

        // Files only in local (need upload)
        let upload_paths: Vec<&String> = local_paths.difference(&cloud_paths).collect();

        // Files only in cloud (need download)
        let download_paths: Vec<&String> = cloud_paths.difference(&local_paths).collect();

        // Files in both (need comparison)
        let common_paths: Vec<&String> = local_paths.intersection(&cloud_paths).collect();

        match self.config.direction {
            SyncDirection::Upload | SyncDirection::Bidirectional
                if local_only == LocalOnly::Upload =>
            {
                // Upload new local files
                for path in &upload_paths {
                    let local_info = &local_files[*path];
                    match self.upload_file(path, local_info).await {
                        Ok(_) => result.uploaded += 1,
                        Err(e) => {
                            result.errors.push(format!("Upload {} failed: {}", path, e));
                        }
                    }
                }
            }
            _ => {}
        }

        match self.config.direction {
            SyncDirection::Download | SyncDirection::Bidirectional => {
                // Download new cloud files
                for path in &download_paths {
                    let cloud_file = &cloud_map[*path];
                    if !cloud_file.is_folder {
                        match self.download_file(cloud_file).await {
                            Ok(_) => result.downloaded += 1,
                            Err(e) => {
                                retry_needed |= !e.is_permanent();
                                result
                                    .errors
                                    .push(format!("Download {} failed: {}", path, e));
                            }
                        }
                    }
                }
            }
            SyncDirection::Upload => {}
        }

        // Handle files that exist in both
        for path in common_paths {
            let local_info = &local_files[path];
            let cloud_file = &cloud_map[path];

            // Check for conflicts
            let conflict = ConflictResolver::detect_conflict(
                &local_info.0,
                local_info.1,
                local_info.2,
                true,
                Some(cloud_file),
                self.state.last_sync,
            );

            if let Some(mut conflict) = conflict {
                let resolution = self.conflict_resolver.resolve(&mut conflict);

                match resolution {
                    ConflictResolution::UseLocal => {
                        if self.config.direction != SyncDirection::Download {
                            match self.upload_file(path, local_info).await {
                                Ok(_) => result.uploaded += 1,
                                Err(e) => {
                                    result.errors.push(format!("Upload {} failed: {}", path, e))
                                }
                            }
                        }
                    }
                    ConflictResolution::UseCloud => {
                        if self.config.direction != SyncDirection::Upload {
                            match self.download_file(cloud_file).await {
                                Ok(_) => result.downloaded += 1,
                                Err(e) => {
                                    retry_needed |= !e.is_permanent();
                                    result
                                        .errors
                                        .push(format!("Download {} failed: {}", path, e));
                                }
                            }
                        }
                    }
                    ConflictResolution::KeepBoth { renamed_to } => {
                        // Download cloud version with new name
                        let mut renamed_file = cloud_file.clone();
                        renamed_file.name = renamed_to;
                        renamed_file.path = format!(
                            "{}/{}",
                            cloud_file
                                .path
                                .rsplit_once('/')
                                .map(|(p, _)| p)
                                .unwrap_or(""),
                            renamed_file.name
                        );

                        if self.config.direction != SyncDirection::Upload {
                            match self.download_file(&renamed_file).await {
                                Ok(_) => result.downloaded += 1,
                                Err(e) => {
                                    retry_needed |= !e.is_permanent();
                                    result
                                        .errors
                                        .push(format!("Download conflict copy failed: {}", e));
                                }
                            }
                        }
                    }
                    ConflictResolution::ManualRequired => {
                        result.conflicts.push(conflict);
                    }
                    _ => {}
                }
            } else {
                // No conflict - sync based on modification time
                let last_sync = self.state.mtime_map.get(path).copied().unwrap_or(0);

                if local_info.1 > last_sync && self.config.direction != SyncDirection::Download {
                    // Local is newer
                    match self.upload_file(path, local_info).await {
                        Ok(_) => result.uploaded += 1,
                        Err(e) => result.errors.push(format!("Upload {} failed: {}", path, e)),
                    }
                } else if cloud_file.modified_at > last_sync
                    && self.config.direction != SyncDirection::Upload
                {
                    // Cloud is newer
                    match self.download_file(cloud_file).await {
                        Ok(_) => result.downloaded += 1,
                        Err(e) => {
                            retry_needed |= !e.is_permanent();
                            result
                                .errors
                                .push(format!("Download {} failed: {}", path, e));
                        }
                    }
                }
            }
        }

        // Update state
        self.state.last_sync = Some(chrono::Utc::now().timestamp());

        // Determine final status
        if !result.errors.is_empty() {
            result.status = if result.uploaded > 0 || result.downloaded > 0 {
                SyncStatus::PartialSuccess
            } else {
                SyncStatus::Failed
            };
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(Reconciled {
            result,
            retry_needed,
        })
    }

    /// List local files with metadata
    /// Returns map of relative path to (full path, mtime, size)
    async fn list_local_files(&self) -> Result<HashMap<String, (PathBuf, i64, u64)>> {
        let mut files = HashMap::new();
        self.scan_directory(&self.config.local_path, &self.config.local_path, &mut files)
            .await?;
        Ok(files)
    }

    /// Recursively scan directory
    async fn scan_directory(
        &self,
        base: &Path,
        dir: &Path,
        files: &mut HashMap<String, (PathBuf, i64, u64)>,
    ) -> Result<()> {
        let mut entries = fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Skip hidden files if configured
            if !self.config.sync_hidden && name.starts_with('.') {
                continue;
            }

            // Check exclude patterns
            if self.should_exclude(&name) {
                continue;
            }

            let metadata = entry.metadata().await?;

            if metadata.is_dir() {
                // Check if folder is in selective sync
                if self.should_sync_folder(&path) {
                    Box::pin(self.scan_directory(base, &path, files)).await?;
                }
            } else {
                // Check file size limit
                if let Some(max_size) = self.config.max_file_size {
                    if metadata.len() > max_size {
                        continue;
                    }
                }

                let relative_path = path
                    .strip_prefix(base)
                    .map(|p| format!("/{}", p.to_string_lossy()))
                    .unwrap_or_else(|_| path.to_string_lossy().to_string());

                let mtime = metadata
                    .modified()
                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64)
                    .unwrap_or(0);

                files.insert(relative_path, (path, mtime, metadata.len()));
            }
        }

        Ok(())
    }

    /// Check if a path matches exclude patterns
    fn should_exclude(&self, name: &str) -> bool {
        for pattern in &self.config.exclude_patterns {
            if Self::glob_match(pattern, name) {
                return true;
            }
        }
        false
    }

    /// Simple glob matching (supports * and ?)
    fn glob_match(pattern: &str, name: &str) -> bool {
        let mut pattern_chars = pattern.chars().peekable();
        let mut name_chars = name.chars().peekable();

        while let Some(p) = pattern_chars.next() {
            match p {
                '*' => {
                    // Match any sequence
                    if pattern_chars.peek().is_none() {
                        return true; // Trailing * matches everything
                    }
                    // Try matching remaining pattern at each position
                    let remaining: String = pattern_chars.collect();
                    let remaining_name: String = name_chars.collect();
                    for i in 0..=remaining_name.len() {
                        if Self::glob_match(&remaining, &remaining_name[i..]) {
                            return true;
                        }
                    }
                    return false;
                }
                '?' => {
                    // Match any single character
                    if name_chars.next().is_none() {
                        return false;
                    }
                }
                c => {
                    // Match exact character
                    if name_chars.next() != Some(c) {
                        return false;
                    }
                }
            }
        }

        name_chars.next().is_none()
    }

    /// Check if a folder should be synced based on selective sync config
    fn should_sync_folder(&self, path: &Path) -> bool {
        if self.config.folder_configs.is_empty() {
            return true; // Sync everything if no selective config
        }

        for config in &self.config.folder_configs {
            if path.starts_with(&config.local_path) || config.local_path.starts_with(path) {
                return true;
            }
        }

        false
    }

    /// Upload a file to cloud
    async fn upload_file(&mut self, path: &str, local_info: &(PathBuf, i64, u64)) -> Result<()> {
        // Keep the relative path (not just the basename) so nested files with the same
        // name don't collide remotely; validated like downloads.
        let components = cloud_path_components(path)?;
        let content = fs::read(&local_info.0).await?;

        // Determine parent folder
        let parent_id = self.config.cloud_folder.as_deref();

        let cloud_file = self
            .provider
            .upload_file_at(parent_id, &components, &content, None)
            .await?;

        // Update state
        self.state.file_map.insert(path.to_string(), cloud_file.id);
        if let Some(hash) = cloud_file.content_hash {
            self.state.hash_map.insert(path.to_string(), hash);
        }
        self.state.mtime_map.insert(path.to_string(), local_info.1);

        Ok(())
    }

    /// Download a file from cloud
    async fn download_file(&mut self, cloud_file: &CloudFile) -> Result<()> {
        // Validate before touching the network or the filesystem.
        let local_path =
            local_path_for(&self.config.local_path, &cloud_file.path).inspect_err(|e| {
                tracing::error!(
                    "cloud sync: skipping download of {:?}: {}",
                    cloud_file.path,
                    e
                );
            })?;
        // A local directory where the file goes (the remote folder was replaced by a file;
        // remote deletions aren't applied locally, so the directory stays) would fail the write
        // every time. Catch it before fetching the body rather than after.
        if fs::symlink_metadata(&local_path)
            .await
            .is_ok_and(|m| m.is_dir())
        {
            return Err(IntegrationError::LocalPathUnusable(format!(
                "{:?}: a local directory is in the way",
                cloud_file.path
            )));
        }
        let content = self.provider.download_file(&cloud_file.id).await?;

        // Symlinks already inside the sync root must not redirect the write (or any mkdir) elsewhere.
        let escape = || {
            tracing::error!(
                "cloud sync: {:?} resolves outside sync root, skipping",
                cloud_file.path
            );
            IntegrationError::InvalidPath(format!(
                "{:?} resolves outside sync root",
                cloud_file.path
            ))
        };
        let (rel_dir, name) = match (
            local_path
                .parent()
                .and_then(|p| p.strip_prefix(&self.config.local_path).ok()),
            local_path.file_name(),
        ) {
            (Some(d), Some(n)) => (d, n),
            _ => return Err(escape()),
        };
        let unusable = |e| local_write_error(&cloud_file.path, e);
        let dir = create_dirs_within(&self.config.local_path, rel_dir)
            .await
            .map_err(unusable)?
            .ok_or_else(escape)?;
        let target = dir.join(name);
        if fs::symlink_metadata(&target)
            .await
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(escape());
        }
        write_replace(&dir, &target, &content)
            .await
            .map_err(unusable)?;

        // Update state
        self.state
            .file_map
            .insert(cloud_file.path.clone(), cloud_file.id.clone());
        if let Some(ref hash) = cloud_file.content_hash {
            self.state
                .hash_map
                .insert(cloud_file.path.clone(), hash.clone());
        }
        self.state
            .mtime_map
            .insert(cloud_file.path.clone(), cloud_file.modified_at);

        Ok(())
    }

    /// Perform delta sync using provider's change API: download what changed remotely. Remote
    /// deletions are reported by the provider but not applied, and local changes wait for a
    /// full [`sync`](Self::sync). Without a usable cursor (none yet, or rejected by the
    /// provider) it runs a [`resync`](Self::resync) instead.
    ///
    /// With [`SyncDirection::Upload`] there is nothing to do: the change feed only brings
    /// remote changes down. The provider isn't asked, and the cursor is neither taken nor
    /// advanced. It marks how far remote changes have been applied, and none were, so a
    /// direction widened later still gets them (or a resync, if the cursor has expired by then).
    pub async fn delta_sync(&mut self) -> Result<SyncResult> {
        if self.config.direction == SyncDirection::Upload {
            tracing::debug!("cloud sync: upload-only, so a delta sync has nothing to download");
            return Ok(SyncResult {
                status: SyncStatus::Success,
                uploaded: 0,
                downloaded: 0,
                deleted: 0,
                conflicts: Vec::new(),
                errors: Vec::new(),
                duration_ms: 0,
            });
        }
        let Some(cursor) = self.state.cursor.clone() else {
            // A change feed only lists what changes after its cursor, so the files already
            // there have to come from a full listing first.
            tracing::info!("cloud sync: no delta cursor yet; running a full sync");
            return self.resync().await;
        };

        let start = std::time::Instant::now();
        let mut result = SyncResult {
            status: SyncStatus::Success,
            uploaded: 0,
            downloaded: 0,
            deleted: 0,
            conflicts: Vec::new(),
            errors: Vec::new(),
            duration_ms: 0,
        };

        // Get changes since last cursor
        let (changes, new_cursor) = match self
            .provider
            .get_changes_in(self.config.cloud_folder.as_deref(), Some(&cursor))
            .await
        {
            Ok(page) => page,
            Err(IntegrationError::ResyncRequired(why)) => {
                tracing::warn!(
                    "cloud sync: delta cursor rejected ({}); running a full sync",
                    why
                );
                return self.resync().await;
            }
            Err(e) => return Err(e),
        };

        // Set when a change failed for a reason that may go away (network, I/O, rate limit);
        // the cursor is then held so the provider re-sends the page. Re-applying the changes
        // that did succeed is idempotent (atomic overwrite with the same content).
        let mut retry_needed = false;
        let mut record = |result: &mut SyncResult, what: &str, e: IntegrationError| {
            if !e.is_permanent() {
                retry_needed = true;
            }
            result.errors.push(format!("{}: {}", what, e));
        };

        for cloud_file in changes {
            if cloud_file.deleted {
                // Not propagated: a local copy may hold edits the remote never saw, and the
                // local mtime can't tell us (downloads are stamped with the write time).
                tracing::info!(
                    "cloud sync: {:?} was deleted remotely; keeping the local copy",
                    cloud_file.path
                );
                continue;
            }
            if cloud_file.is_folder {
                continue;
            }
            if self.too_large(&cloud_file) {
                // Permanent until the limit changes, so it doesn't hold the cursor.
                tracing::warn!(
                    "cloud sync: skipping change {:?}: {} bytes is over the size limit",
                    cloud_file.path,
                    cloud_file.size
                );
                continue;
            }

            let local_path = match local_path_for(&self.config.local_path, &cloud_file.path) {
                Ok(p) => p,
                Err(e) => {
                    // Validation reject: permanent, so it must not block the cursor forever.
                    tracing::error!("cloud sync: skipping change {:?}: {}", cloud_file.path, e);
                    record(&mut result, &format!("Skipped {}", cloud_file.path), e);
                    continue;
                }
            };

            // Check if local file exists
            let local_exists = local_path.exists();

            let outcome = if local_exists {
                // Check for conflict
                let metadata = match fs::metadata(&local_path).await {
                    Ok(m) => m,
                    Err(e) => {
                        record(&mut result, &format!("Stat {}", cloud_file.path), e.into());
                        continue;
                    }
                };
                let local_mtime = metadata
                    .modified()
                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64)
                    .unwrap_or(0);

                let last_sync_time = self.state.mtime_map.get(&cloud_file.path).copied();

                if local_mtime > last_sync_time.unwrap_or(0) {
                    // Local modified since last sync - conflict
                    let mut conflict = Conflict {
                        local_path: local_path.clone(),
                        cloud_file: cloud_file.clone(),
                        local_modified_at: local_mtime,
                        local_size: metadata.len(),
                        conflict_type: ConflictType::BothModified,
                        resolution: None,
                    };

                    // Conflicts are reported, not retried: re-fetching the change can't
                    // resolve them, so they don't hold the cursor.
                    match self.conflict_resolver.resolve(&mut conflict) {
                        ConflictResolution::UseCloud => Some(self.download_file(&cloud_file).await),
                        ConflictResolution::ManualRequired => {
                            result.conflicts.push(conflict);
                            None
                        }
                        _ => None,
                    }
                } else {
                    // Cloud is newer, download
                    Some(self.download_file(&cloud_file).await)
                }
            } else {
                // New file from cloud
                Some(self.download_file(&cloud_file).await)
            };

            match outcome {
                Some(Ok(())) => result.downloaded += 1,
                Some(Err(e)) => record(&mut result, "Download failed", e),
                None => {}
            }
        }

        // Only advance past this page once every change in it was applied or failed
        // permanently; otherwise the transiently failed files would never be retried.
        if retry_needed {
            tracing::warn!("cloud sync: holding delta cursor; some changes will be retried");
        } else {
            self.state.cursor = new_cursor;
        }
        self.state.last_sync = Some(chrono::Utc::now().timestamp());

        if !result.errors.is_empty() {
            result.status = SyncStatus::PartialSuccess;
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Catch up when there is no usable delta cursor (none yet, or the provider rejected it):
    /// take a fresh cursor, then do a full sync. The cursor is taken first so changes made
    /// during the full sync are replayed by the next delta rather than missed. It is only
    /// stored once nothing remote is left to retry: if the listing failed or a download failed
    /// for a reason that may go away, the old cursor is kept, so the next delta runs the resync
    /// again instead of silently skipping those files (a fresh cursor never lists changes made
    /// before it). Permanent failures (deleted, not downloadable, unsafe or over-long name, a
    /// local directory in the way) don't hold it, and neither do failed uploads, which no
    /// cursor covers.
    ///
    /// Like the delta it stands in for, a resync never uploads files that exist only locally
    /// (see [`LocalOnly::Keep`]).
    async fn resync(&mut self) -> Result<SyncResult> {
        let (_, fresh) = self
            .provider
            .get_changes_in(self.config.cloud_folder.as_deref(), None)
            .await?;
        let Reconciled {
            result,
            retry_needed,
        } = self.reconcile(LocalOnly::Keep).await?;
        if retry_needed {
            tracing::warn!(
                "cloud sync: keeping the old delta cursor; the full sync will be retried"
            );
        } else {
            self.state.cursor = fresh;
        }
        Ok(result)
    }
}

/// What a full sync does with files that exist only locally, in an uploading direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalOnly {
    /// Upload them: a requested full sync.
    Upload,
    /// Leave them. A resync stands in for a delta, which reports remote deletions but keeps
    /// the local copies; a file missing from the listing may be one of those, and uploading it
    /// would bring back what was deleted remotely. A later full sync uploads new local files.
    Keep,
}

/// Outcome of a full sync ([`CloudSync::reconcile`]).
struct Reconciled {
    result: SyncResult,
    /// Something remote wasn't fetched for a reason that may go away (a listing failed, or a
    /// download failed with a non-permanent error).
    retry_needed: bool,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::integrations::{CloudFolder, OAuthToken, ProviderType, StorageQuota};

    /// In-memory provider: serves `files` (content = id bytes) and records upload names and
    /// downloaded ids. Downloads of ids in `fail_ids` fail with a (transient) network error,
    /// of ids in `gone_ids` with a (permanent) not-found; `get_changes` hands out
    /// `next_cursor`, except for `stale_cursor`, which it rejects as expired. While
    /// `list_fails` is set, the full listing fails.
    #[derive(Default)]
    struct MockProvider {
        files: Vec<CloudFile>,
        uploads: Mutex<Vec<String>>,
        downloads: Mutex<Vec<String>>,
        fail_ids: Mutex<HashSet<String>>,
        gone_ids: HashSet<String>,
        next_cursor: Option<String>,
        stale_cursor: Option<String>,
        list_fails: std::sync::atomic::AtomicBool,
        /// How often `list_files` and `get_changes` were called.
        list_calls: std::sync::atomic::AtomicUsize,
        changes_calls: std::sync::atomic::AtomicUsize,
    }

    fn cf(id: &str, path: &str) -> CloudFile {
        CloudFile {
            id: id.into(),
            name: path.rsplit('/').next().unwrap().into(),
            mime_type: None,
            size: 1,
            modified_at: 1,
            content_hash: None,
            parent_id: None,
            is_folder: false,
            path: path.into(),
            deleted: false,
        }
    }

    #[async_trait]
    impl CloudProvider for MockProvider {
        fn provider_type(&self) -> ProviderType {
            ProviderType::Dropbox
        }
        fn is_authenticated(&self) -> bool {
            true
        }
        fn get_token(&self) -> Option<&OAuthToken> {
            None
        }
        fn set_token(&mut self, _: OAuthToken) {}
        async fn refresh_token(&mut self) -> Result<()> {
            Ok(())
        }
        async fn list_files(&self, _: Option<&str>) -> Result<Vec<CloudFile>> {
            self.list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.list_fails.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(IntegrationError::Network("listing failed".into()));
            }
            Ok(self.files.clone())
        }
        async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
            Ok(vec![])
        }
        async fn get_file_metadata(&self, id: &str) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        async fn download_file(&self, id: &str) -> Result<Vec<u8>> {
            self.downloads.lock().unwrap().push(id.into());
            if self.fail_ids.lock().unwrap().contains(id) {
                return Err(IntegrationError::Network("connection reset".into()));
            }
            if self.gone_ids.contains(id) {
                return Err(IntegrationError::NotFound(id.into()));
            }
            Ok(id.as_bytes().to_vec())
        }
        async fn upload_file(
            &self,
            _: Option<&str>,
            name: &str,
            _: &[u8],
            _: Option<&str>,
        ) -> Result<CloudFile> {
            self.uploads.lock().unwrap().push(name.into());
            Ok(cf(name, &format!("/{}", name)))
        }
        async fn create_folder(&self, _: Option<&str>, _: &str) -> Result<CloudFolder> {
            unimplemented!()
        }
        async fn delete(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn move_file(&self, id: &str, _: &str, _: Option<&str>) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        async fn get_changes(
            &self,
            cursor: Option<&str>,
        ) -> Result<(Vec<CloudFile>, Option<String>)> {
            self.changes_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if cursor.is_some() && cursor == self.stale_cursor.as_deref() {
                return Err(IntegrationError::ResyncRequired("expired".into()));
            }
            Ok((self.files.clone(), self.next_cursor.clone()))
        }
        async fn get_quota(&self) -> Result<StorageQuota> {
            Ok(StorageQuota {
                used: 0,
                total: None,
                trash: None,
            })
        }
    }

    fn cfg(dir: &Path, direction: SyncDirection) -> SyncConfig {
        SyncConfig {
            local_path: dir.to_path_buf(),
            direction,
            ..Default::default()
        }
    }

    const EVIL: &[&str] = &[
        "../x",
        "/../x",
        "//etc/x",
        "a/../../x",
        "..\\x",
        "a\\..\\..\\x",
        "C:/x",
        "c:x",
        "a/./b",
        "a//b",
        "",
        "/",
        "x\0y",
        "..",
    ];

    #[test]
    fn traversal_names_rejected() {
        for p in [
            "../x",
            "/etc/x",
            "a/../../x",
            "..\\x",
            "C:\\x",
            "a\0b",
            "./x",
            "a//b",
            "",
            "..",
        ] {
            assert!(safe_components(p).is_err(), "accepted {:?}", p);
        }
        let base = Path::new("/srv/sync");
        for p in EVIL {
            assert!(local_path_for(base, p).is_err(), "accepted {:?}", p);
        }
        // A provider-rooted path maps under the base, never to the real /etc.
        assert_eq!(local_path_for(base, "/etc/x").unwrap(), base.join("etc/x"));
        assert_eq!(
            local_path_for(base, "/Notes/a b.pdf").unwrap(),
            base.join("Notes").join("a b.pdf")
        );
        assert_eq!(
            safe_components("a/b/c.pdf").unwrap(),
            vec!["a", "b", "c.pdf"]
        );
    }

    #[tokio::test]
    async fn download_skips_traversal_and_keeps_syncing() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let mut files: Vec<CloudFile> = EVIL
            .iter()
            .enumerate()
            .map(|(i, p)| cf(&format!("evil{}", i), p))
            .collect();
        files.push(cf("good", "/sub/ok.txt"));
        let mut sync = CloudSync::new(
            MockProvider {
                files: files.clone(),
                ..Default::default()
            },
            cfg(&root, SyncDirection::Download),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!(r.downloaded, 1);
        assert_eq!(r.status, SyncStatus::PartialSuccess);
        assert_eq!(r.errors.len(), EVIL.len());
        assert_eq!(std::fs::read(root.join("sub/ok.txt")).unwrap(), b"good");
        assert!(!outer.path().join("x").exists());
        // Delta sync applies the same validation (fresh root so nothing conflicts).
        let root2 = outer.path().join("root2");
        std::fs::create_dir(&root2).unwrap();
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::with_state(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root2, SyncDirection::Download),
            state,
        );
        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.downloaded, 1);
        assert_eq!(r.errors.len(), EVIL.len());
        assert!(root2.join("sub/ok.txt").exists());
        assert!(!outer.path().join("x").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_refuses_symlinked_dir_escape() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::os::unix::fs::symlink(outer.path(), root.join("link")).unwrap();
        let files = vec![cf("evil", "/link/pwned.txt")];
        let mut sync = CloudSync::new(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root, SyncDirection::Download),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!(r.downloaded, 0);
        assert!(!outer.path().join("pwned.txt").exists());
    }

    /// No directory may be created through a symlinked subdirectory, a symlinked target file
    /// is never written through, and symlinks that stay inside the root still work.
    #[cfg(unix)]
    #[tokio::test]
    async fn download_never_creates_or_writes_through_escaping_symlinks() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir_all(root.join("real")).unwrap();
        std::os::unix::fs::symlink(outer.path(), root.join("link")).unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join("alias")).unwrap();
        // Hidden, so the local scan skips it and the cloud copy is a pure download.
        std::fs::write(outer.path().join("victim.txt"), "orig").unwrap();
        std::os::unix::fs::symlink(outer.path().join("victim.txt"), root.join(".f.txt")).unwrap();
        let files = vec![
            cf("evil1", "/link/new/deep/pwned.txt"),
            cf("evil2", "/.f.txt"),
            cf("ok", "/alias/sub/ok.txt"),
        ];
        let mut sync = CloudSync::new(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root, SyncDirection::Download),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 2), "{:?}", r.errors);
        assert!(
            !outer.path().join("new").exists(),
            "mkdir escaped through symlink"
        );
        assert_eq!(
            std::fs::read(outer.path().join("victim.txt")).unwrap(),
            b"orig"
        );
        assert_eq!(std::fs::read(root.join("real/sub/ok.txt")).unwrap(), b"ok");
        // Temp files from the atomic write don't linger.
        assert!(
            std::fs::read_dir(root.join("real/sub"))
                .unwrap()
                .all(|e| e.unwrap().file_name() == "ok.txt")
        );
    }

    /// A transient download failure holds the delta cursor so the change is re-fetched; once
    /// it succeeds the cursor advances. Permanent (validation) rejects never hold it.
    #[tokio::test]
    async fn delta_cursor_held_until_transient_failures_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("ok", "/ok.txt"), cf("flaky", "/sub/flaky.txt")],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        provider.fail_ids.lock().unwrap().insert("flaky".into());
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync =
            CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Download), state);

        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 1), "{:?}", r.errors);
        assert_eq!(r.status, SyncStatus::PartialSuccess);
        assert_eq!(
            sync.state().cursor.as_deref(),
            Some("c1"),
            "cursor advanced past a failure"
        );
        assert!(!dir.path().join("sub/flaky.txt").exists());

        // The failure clears (e.g. network back): the retried page applies and the cursor moves.
        sync.provider.fail_ids.lock().unwrap().clear();
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.status, SyncStatus::Success);
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        assert_eq!(
            std::fs::read(dir.path().join("sub/flaky.txt")).unwrap(),
            b"flaky"
        );
    }

    #[tokio::test]
    async fn delta_cursor_advances_past_permanent_rejects() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let mut files: Vec<CloudFile> = EVIL
            .iter()
            .enumerate()
            .map(|(i, p)| cf(&format!("evil{}", i), p))
            .collect();
        files.push(cf("good", "/good.txt"));
        let provider = MockProvider {
            files,
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::with_state(provider, cfg(&root, SyncDirection::Download), state);
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, EVIL.len()));
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        assert!(!outer.path().join("x").exists());
    }

    /// Remote deletions are never downloaded and never remove the local copy, and they don't
    /// hold the cursor.
    #[tokio::test]
    async fn delta_skips_remote_deletions() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.txt"), "mine").unwrap();
        let mut gone = cf("", "/gone.txt");
        gone.deleted = true;
        let provider = MockProvider {
            files: vec![gone, cf("new", "/new.txt")],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync =
            CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Download), state);
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.downloaded, r.deleted), (1, 0));
        assert_eq!(std::fs::read(dir.path().join("gone.txt")).unwrap(), b"mine");
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
    }

    /// A rejected cursor triggers a full sync; the fresh cursor is only stored once that
    /// sync works, so a failed attempt is retried by the next delta instead of skipping the
    /// changes made while the cursor was stale.
    #[tokio::test]
    async fn delta_resync_holds_cursor_until_full_sync_works() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("a", "/a.txt"), cf("b", "/sub/b.txt")],
            next_cursor: Some("fresh".into()),
            stale_cursor: Some("stale".into()),
            list_fails: true.into(),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("stale".into()),
            ..Default::default()
        };
        let mut sync =
            CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Download), state);

        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.status, SyncStatus::Failed, "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("stale"));

        sync.provider
            .list_fails
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.downloaded, 2);
        assert_eq!(std::fs::read(dir.path().join("sub/b.txt")).unwrap(), b"b");
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));
    }

    fn stale(
        provider: MockProvider,
        dir: &Path,
        direction: SyncDirection,
    ) -> CloudSync<MockProvider> {
        let state = SyncState {
            cursor: Some("stale".into()),
            ..Default::default()
        };
        let provider = MockProvider {
            next_cursor: Some("fresh".into()),
            stale_cursor: Some("stale".into()),
            ..provider
        };
        CloudSync::with_state(provider, cfg(dir, direction), state)
    }

    /// A resync keeps the old cursor while a download failed transiently, whatever the
    /// status (here PartialSuccess): the fresh cursor would never list that file again.
    #[tokio::test]
    async fn resync_holds_cursor_on_transient_download_failure() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("ok", "/ok.txt"), cf("flaky", "/sub/flaky.txt")],
            ..Default::default()
        };
        provider.fail_ids.lock().unwrap().insert("flaky".into());
        let mut sync = stale(provider, dir.path(), SyncDirection::Download);

        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.status, r.downloaded), (SyncStatus::PartialSuccess, 1));
        assert_eq!(sync.state().cursor.as_deref(), Some("stale"));

        sync.provider.fail_ids.lock().unwrap().clear();
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));
        assert_eq!(
            std::fs::read(dir.path().join("sub/flaky.txt")).unwrap(),
            b"flaky"
        );
    }

    /// Only permanent failures and nothing else to transfer is status Failed, yet the resync
    /// did all it ever can: the fresh cursor is stored instead of resyncing on every delta.
    #[tokio::test]
    async fn resync_advances_cursor_past_permanent_failures() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("gone", "/gone.txt")],
            gone_ids: HashSet::from(["gone".to_string()]),
            ..Default::default()
        };
        let mut sync = stale(provider, dir.path(), SyncDirection::Download);
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.status, r.errors.len()), (SyncStatus::Failed, 1));
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));
    }

    /// A resync (bidirectional here) never uploads files missing from the listing: they may be
    /// remote deletions that delta sync reported and deliberately kept locally. A requested
    /// full sync still uploads them.
    #[tokio::test]
    async fn resync_does_not_upload_local_only_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("deleted-remotely.txt"), "mine").unwrap();
        let provider = MockProvider {
            files: vec![cf("a", "/a.txt")],
            ..Default::default()
        };
        let mut sync = stale(provider, dir.path(), SyncDirection::Bidirectional);
        sync.state
            .file_map
            .insert("/deleted-remotely.txt".into(), "old-id".into());

        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.downloaded, r.uploaded), (1, 0));
        assert!(sync.provider.uploads.lock().unwrap().is_empty());
        assert_eq!(
            std::fs::read(dir.path().join("deleted-remotely.txt")).unwrap(),
            b"mine"
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));

        sync.sync().await.unwrap();
        assert!(
            sync.provider
                .uploads
                .lock()
                .unwrap()
                .contains(&"deleted-remotely.txt".to_string())
        );
    }

    /// With no cursor yet, delta sync takes one and then runs a full sync, so files that
    /// existed before the cursor aren't silently skipped.
    #[tokio::test]
    async fn delta_without_cursor_runs_a_full_sync_first() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("local-only.txt"), "mine").unwrap();
        let provider = MockProvider {
            files: vec![cf("a", "/a.txt"), cf("b", "/sub/b.txt")],
            next_cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::new(provider, cfg(dir.path(), SyncDirection::Bidirectional));
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.downloaded, r.uploaded), (2, 0));
        assert_eq!(std::fs::read(dir.path().join("sub/b.txt")).unwrap(), b"b");
        assert_eq!(sync.state().cursor.as_deref(), Some("c1"));
    }

    /// Upload-only: a delta sync downloads nothing and doesn't ask the provider for changes or
    /// a listing, with a cursor, an expired one or none. The cursor stays as it was (a resync
    /// would store one past remote changes that were never applied), so widening the direction
    /// later still brings those changes down. Local changes wait for a full sync, which does
    /// upload them.
    #[tokio::test]
    async fn upload_only_delta_sync_downloads_nothing_and_keeps_the_cursor() {
        use std::sync::atomic::Ordering::SeqCst;
        for cursor in [Some("c1"), Some("stale"), None] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("local.txt"), "mine").unwrap();
            let provider = MockProvider {
                files: vec![cf("r", "/remote.txt")],
                next_cursor: Some("c2".into()),
                stale_cursor: Some("stale".into()),
                ..Default::default()
            };
            let state = SyncState {
                cursor: cursor.map(Into::into),
                ..Default::default()
            };
            let mut sync =
                CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Upload), state);

            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{cursor:?}: {:?}", r.errors);
            assert_eq!(
                (r.status, r.downloaded, r.uploaded),
                (SyncStatus::Success, 0, 0),
                "{cursor:?}"
            );
            assert!(sync.provider.downloads.lock().unwrap().is_empty());
            assert!(!dir.path().join("remote.txt").exists());
            assert_eq!(sync.state().cursor.as_deref(), cursor);
            assert_eq!(sync.provider.changes_calls.load(SeqCst), 0, "{cursor:?}");
            assert_eq!(sync.provider.list_calls.load(SeqCst), 0, "{cursor:?}");

            sync.sync().await.unwrap();
            assert_eq!(*sync.provider.uploads.lock().unwrap(), vec!["local.txt"]);
            assert!(!dir.path().join("remote.txt").exists());

            sync.config.direction = SyncDirection::Download;
            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{cursor:?}: {:?}", r.errors);
            assert_eq!(std::fs::read(dir.path().join("remote.txt")).unwrap(), b"r");
            assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        }
    }

    /// Remote files over `max_file_size` are never downloaded, by full or delta sync, and a
    /// local file at such a path isn't uploaded over the remote one; neither is an error.
    #[tokio::test]
    async fn oversized_cloud_files_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("big.bin"), "small local").unwrap();
        let mut big = cf("big", "/big.bin");
        big.size = 1 << 20;
        let mut huge = cf("huge", "/sub/huge.bin");
        huge.size = u64::MAX;
        let provider = MockProvider {
            files: vec![big, huge, cf("ok", "/ok.txt")],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let config = SyncConfig {
            max_file_size: Some(1024),
            ..cfg(dir.path(), SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(provider, config);

        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(
            (r.status, r.downloaded, r.uploaded),
            (SyncStatus::Success, 1, 0)
        );
        assert_eq!(*sync.provider.downloads.lock().unwrap(), vec!["ok"]);
        assert_eq!(
            std::fs::read(dir.path().join("big.bin")).unwrap(),
            b"small local"
        );
        assert!(!dir.path().join("sub/huge.bin").exists());

        sync.state.cursor = Some("c1".into());
        sync.provider.downloads.lock().unwrap().clear();
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(
            !sync
                .provider
                .downloads
                .lock()
                .unwrap()
                .iter()
                .any(|id| id != "ok"),
            "{:?}",
            sync.provider.downloads
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
    }

    /// A remote folder replaced by a file of the same name leaves the local directory in place
    /// (remote deletions aren't applied). Writing the file there fails every time, so it is a
    /// permanent failure: caught before any download, and neither a resync nor a delta holds
    /// its cursor for it.
    #[tokio::test]
    async fn remote_file_over_local_directory_does_not_hold_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("Dir")).unwrap();
        std::fs::write(dir.path().join("Dir/kept.txt"), "mine").unwrap();
        let provider = MockProvider {
            files: vec![cf("was-a-folder", "/Dir"), cf("ok", "/ok.txt")],
            next_cursor: Some("fresh".into()),
            stale_cursor: Some("stale".into()),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("stale".into()),
            ..Default::default()
        };
        // CloudWins so the delta below tries to replace the (newer) local directory.
        let config = SyncConfig {
            conflict_strategy: ConflictStrategy::CloudWins,
            ..cfg(dir.path(), SyncDirection::Download)
        };
        let mut sync = CloudSync::with_state(provider, config, state);

        // Resync (stale cursor): the file is skipped as unusable, the rest syncs, and the fresh
        // cursor is stored instead of repeating the full listing on every delta.
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 1), "{:?}", r.errors);
        assert!(
            r.errors[0].contains("Local path unusable"),
            "{:?}",
            r.errors
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));

        // Delta: the same change fails permanently again, and the cursor still advances.
        sync.provider.next_cursor = Some("c3".into());
        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert!(
            r.errors[0].contains("Local path unusable"),
            "{:?}",
            r.errors
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("c3"));

        // The body was never fetched, and the local directory is untouched.
        assert!(
            !sync
                .provider
                .downloads
                .lock()
                .unwrap()
                .contains(&"was-a-folder".to_string())
        );
        assert_eq!(
            std::fs::read(dir.path().join("Dir/kept.txt")).unwrap(),
            b"mine"
        );
    }

    /// Dropbox and OneDrive allow 255 characters per name, which can be more than the 255 bytes
    /// a local name may have. Such a file (or folder) is rejected up front like an unsafe name:
    /// never downloaded, and the cursor from a first sync is stored and then advanced.
    #[tokio::test]
    async fn names_too_long_for_the_local_filesystem_do_not_hold_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let long = format!("{}.pdf", "\u{6587}".repeat(90)); // 94 characters, 274 bytes
        assert!(long.chars().count() <= 255 && long.len() > MAX_NAME_BYTES);
        let provider = MockProvider {
            files: vec![
                cf("long-file", &format!("/{}", long)),
                cf("in-long-folder", &format!("/{}/x.pdf", long)),
                cf("ok", "/ok.txt"),
            ],
            next_cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::new(provider, cfg(dir.path(), SyncDirection::Download));

        // No cursor yet: the full sync runs and its fresh cursor is kept.
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 2), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("c1"));

        sync.provider.next_cursor = Some("c2".into());
        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.errors.len(), 2, "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        assert!(
            sync.provider
                .downloads
                .lock()
                .unwrap()
                .iter()
                .all(|id| id == "ok")
        );
    }

    #[test]
    fn overlong_segments_rejected() {
        let max = "a".repeat(MAX_NAME_BYTES);
        let over = "a".repeat(MAX_NAME_BYTES + 1);
        // Measured in bytes, not characters: 86 three-byte characters are 258 bytes.
        let wide = "\u{6587}".repeat(86);
        assert!(is_safe_name(&max));
        assert!(safe_components(&format!("{max}/{max}")).is_ok());
        for bad in [&over, &wide] {
            assert!(!is_safe_name(bad), "accepted {} bytes", bad.len());
            assert!(safe_components(&format!("ok/{bad}/x")).is_err());
            assert!(local_path_for(Path::new("/srv/sync"), &format!("/{bad}")).is_err());
        }
    }

    /// Local write failures that come back on every retry are classified permanent; the
    /// others (fixable on the server) stay transient. Checked against real filesystem errors.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_write_errors_classified() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/x"), "x").unwrap();

        // A file renamed over a (non-empty) directory: IsADirectory.
        let e = write_replace(dir.path(), &dir.path().join("sub"), b"new")
            .await
            .unwrap_err();
        let e = local_write_error("/sub", e);
        assert!(matches!(e, IntegrationError::LocalPathUnusable(_)), "{e:?}");
        assert!(e.is_permanent());
        assert_eq!(std::fs::read(dir.path().join("sub/x")).unwrap(), b"x");

        // A name the filesystem refuses (ENAMETOOLONG): InvalidFilename.
        let long = dir.path().join("a".repeat(MAX_NAME_BYTES + 1));
        let e = write_replace(dir.path(), &long, b"new").await.unwrap_err();
        let e = local_write_error("/aaa", e);
        assert!(e.is_permanent(), "{e:?}");

        // No temp files are left behind by the failed writes.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::StorageFull,
            std::io::ErrorKind::Other,
        ] {
            let e = local_write_error("/x", IntegrationError::Io(kind.into()));
            assert!(
                matches!(e, IntegrationError::Io(_)) && !e.is_permanent(),
                "{e:?}"
            );
        }
        let e = local_write_error("/x", IntegrationError::Network("reset".into()));
        assert!(matches!(e, IntegrationError::Network(_)));
    }

    #[test]
    fn error_permanence() {
        assert!(IntegrationError::InvalidPath("x".into()).is_permanent());
        assert!(IntegrationError::NotFound("x".into()).is_permanent());
        assert!(IntegrationError::NotDownloadable("x".into()).is_permanent());
        assert!(IntegrationError::LocalPathUnusable("x".into()).is_permanent());
        for e in [
            IntegrationError::Network("x".into()),
            IntegrationError::Io(std::io::Error::other("x")),
            IntegrationError::RateLimited {
                retry_after_secs: 1,
            },
            IntegrationError::TokenExpired,
            IntegrationError::Api("500".into()),
            IntegrationError::ResyncRequired("410".into()),
        ] {
            assert!(!e.is_permanent(), "{e}");
        }
    }

    #[tokio::test]
    async fn upload_preserves_nested_paths() {
        let dir = tempfile::tempdir().unwrap();
        for sub in ["a", "b/c"] {
            std::fs::create_dir_all(dir.path().join(sub)).unwrap();
            std::fs::write(dir.path().join(sub).join("same.pdf"), sub).unwrap();
        }
        std::fs::write(dir.path().join("top.pdf"), "t").unwrap();
        let mut sync = CloudSync::new(
            MockProvider::default(),
            cfg(dir.path(), SyncDirection::Upload),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!(r.uploaded, 3, "{:?}", r.errors);
        let mut names = sync.provider.uploads.lock().unwrap().clone();
        names.sort();
        assert_eq!(names, vec!["a/same.pdf", "b/c/same.pdf", "top.pdf"]);
    }

    #[test]
    fn test_glob_match() {
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*.txt", "test.txt"
            )
        );
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*.txt", "file.txt"
            )
        );
        assert!(
            !CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*.txt", "test.pdf"
            )
        );
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "test?", "test1"
            )
        );
        assert!(
            !CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "test?", "test12"
            )
        );
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*", "anything"
            )
        );
    }
}
