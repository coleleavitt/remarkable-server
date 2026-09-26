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

/// Split a *relative* sync path into components, rejecting anything that could escape
/// the sync root: absolute paths, `..`/`.`/empty segments, backslashes, NUL, drive prefixes.
/// Remote file names are attacker-controlled, so every local path is built from this.
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
                return Ok(result);
            }
        };

        // Build cloud file map
        let cloud_map: HashMap<String, CloudFile> = cloud_files
            .into_iter()
            .map(|f| (f.path.clone(), f))
            .collect();

        // Get local files
        let local_files = match self.list_local_files().await {
            Ok(files) => files,
            Err(e) => {
                result.status = SyncStatus::Failed;
                result
                    .errors
                    .push(format!("Failed to list local files: {}", e));
                result.duration_ms = start.elapsed().as_millis() as u64;
                return Ok(result);
            }
        };

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
            SyncDirection::Upload | SyncDirection::Bidirectional => {
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
            SyncDirection::Download => {}
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
                                Err(e) => result
                                    .errors
                                    .push(format!("Download {} failed: {}", path, e)),
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
                                Err(e) => result
                                    .errors
                                    .push(format!("Download conflict copy failed: {}", e)),
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
                        Err(e) => result
                            .errors
                            .push(format!("Download {} failed: {}", path, e)),
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
        Ok(result)
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
        let dir = create_dirs_within(&self.config.local_path, rel_dir)
            .await?
            .ok_or_else(escape)?;
        let target = dir.join(name);
        if fs::symlink_metadata(&target)
            .await
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(escape());
        }
        write_replace(&dir, &target, &content).await?;

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

    /// Perform delta sync using provider's change API
    pub async fn delta_sync(&mut self) -> Result<SyncResult> {
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
        let (changes, new_cursor) = self
            .provider
            .get_changes(self.state.cursor.as_deref())
            .await?;

        for cloud_file in changes {
            if cloud_file.is_folder {
                continue;
            }

            let local_path = match local_path_for(&self.config.local_path, &cloud_file.path) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("cloud sync: skipping change {:?}: {}", cloud_file.path, e);
                    result
                        .errors
                        .push(format!("Skipped {}: {}", cloud_file.path, e));
                    continue;
                }
            };

            // Check if local file exists
            let local_exists = local_path.exists();

            if local_exists {
                // Check for conflict
                let metadata = fs::metadata(&local_path).await?;
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

                    let resolution = self.conflict_resolver.resolve(&mut conflict);
                    match resolution {
                        ConflictResolution::UseCloud => {
                            match self.download_file(&cloud_file).await {
                                Ok(_) => result.downloaded += 1,
                                Err(e) => result.errors.push(format!("Download failed: {}", e)),
                            }
                        }
                        ConflictResolution::ManualRequired => {
                            result.conflicts.push(conflict);
                        }
                        _ => {}
                    }
                } else {
                    // Cloud is newer, download
                    match self.download_file(&cloud_file).await {
                        Ok(_) => result.downloaded += 1,
                        Err(e) => result.errors.push(format!("Download failed: {}", e)),
                    }
                }
            } else {
                // New file from cloud
                match self.download_file(&cloud_file).await {
                    Ok(_) => result.downloaded += 1,
                    Err(e) => result.errors.push(format!("Download failed: {}", e)),
                }
            }
        }

        // Update cursor
        self.state.cursor = new_cursor;
        self.state.last_sync = Some(chrono::Utc::now().timestamp());

        if !result.errors.is_empty() {
            result.status = SyncStatus::PartialSuccess;
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::integrations::{CloudFolder, OAuthToken, ProviderType, StorageQuota};

    /// In-memory provider: serves `files` (content = id bytes) and records upload names.
    #[derive(Default)]
    struct MockProvider {
        files: Vec<CloudFile>,
        uploads: Mutex<Vec<String>>,
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
            Ok(self.files.clone())
        }
        async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
            Ok(vec![])
        }
        async fn get_file_metadata(&self, id: &str) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        async fn download_file(&self, id: &str) -> Result<Vec<u8>> {
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
        async fn get_changes(&self, _: Option<&str>) -> Result<(Vec<CloudFile>, Option<String>)> {
            Ok((self.files.clone(), None))
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
        let mut sync = CloudSync::new(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root2, SyncDirection::Download),
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
