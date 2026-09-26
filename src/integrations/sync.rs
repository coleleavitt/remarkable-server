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
use crate::integrations::{CloudFile, CloudProvider, Result, SyncFolderConfig};

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
        let content = fs::read(&local_info.0).await?;

        let name = local_info
            .0
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".into());

        // Determine parent folder
        let parent_id = self.config.cloud_folder.as_deref();

        let cloud_file = self
            .provider
            .upload_file(parent_id, &name, &content, None)
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
        let content = self.provider.download_file(&cloud_file.id).await?;

        let local_path = self
            .config
            .local_path
            .join(cloud_file.path.trim_start_matches('/'));

        // Create parent directories
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::write(&local_path, &content).await?;

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

            let local_path = self
                .config
                .local_path
                .join(cloud_file.path.trim_start_matches('/'));

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
    use super::*;

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
