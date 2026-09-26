//! Conflict resolution for bidirectional sync
//!
//! Handles cases where both local and cloud have changes to the same file.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::integrations::CloudFile;

/// How a conflict was resolved
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Use the local version
    UseLocal,
    /// Use the cloud version
    UseCloud,
    /// Keep both (rename one)
    KeepBoth {
        /// New name for the conflicting file
        renamed_to: String,
    },
    /// Merge content (for supported file types)
    Merged,
    /// User must manually resolve
    ManualRequired,
    /// Skip this file
    Skip,
}

/// Strategy for automatic conflict resolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Always use the newer modification time
    NewerWins,
    /// Always prefer local changes
    LocalWins,
    /// Always prefer cloud changes
    CloudWins,
    /// Keep both versions (create conflict copies)
    KeepBoth,
    /// Ask user for each conflict
    AskUser,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        Self::NewerWins
    }
}

/// A detected conflict between local and cloud versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    /// Local file path
    pub local_path: PathBuf,
    /// Cloud file metadata
    pub cloud_file: CloudFile,
    /// Local modification time (Unix timestamp)
    pub local_modified_at: i64,
    /// Local file size
    pub local_size: u64,
    /// Type of conflict
    pub conflict_type: ConflictType,
    /// How it was resolved (if resolved)
    pub resolution: Option<ConflictResolution>,
}

/// Types of conflicts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictType {
    /// Both modified since last sync
    BothModified,
    /// Local deleted, cloud modified
    LocalDeletedCloudModified,
    /// Local modified, cloud deleted
    LocalModifiedCloudDeleted,
    /// File type changed (e.g., file became folder)
    TypeChanged,
    /// Content hash mismatch with same modification time
    HashMismatch,
}

/// Conflict resolver with configurable strategy
pub struct ConflictResolver {
    strategy: ConflictStrategy,
    /// Conflicts that need manual resolution
    pending_conflicts: Vec<Conflict>,
}

impl ConflictResolver {
    pub fn new(strategy: ConflictStrategy) -> Self {
        Self {
            strategy,
            pending_conflicts: Vec::new(),
        }
    }

    /// Resolve a conflict using the configured strategy
    pub fn resolve(&mut self, conflict: &mut Conflict) -> ConflictResolution {
        let resolution = match self.strategy {
            ConflictStrategy::NewerWins => self.resolve_by_time(conflict),
            ConflictStrategy::LocalWins => ConflictResolution::UseLocal,
            ConflictStrategy::CloudWins => ConflictResolution::UseCloud,
            ConflictStrategy::KeepBoth => {
                let renamed = self.generate_conflict_name(&conflict.cloud_file.name);
                ConflictResolution::KeepBoth {
                    renamed_to: renamed,
                }
            }
            ConflictStrategy::AskUser => {
                self.pending_conflicts.push(conflict.clone());
                ConflictResolution::ManualRequired
            }
        };

        conflict.resolution = Some(resolution.clone());
        resolution
    }

    /// Resolve based on modification time
    fn resolve_by_time(&self, conflict: &Conflict) -> ConflictResolution {
        match conflict.conflict_type {
            ConflictType::BothModified | ConflictType::HashMismatch => {
                if conflict.local_modified_at > conflict.cloud_file.modified_at {
                    ConflictResolution::UseLocal
                } else if conflict.local_modified_at < conflict.cloud_file.modified_at {
                    ConflictResolution::UseCloud
                } else {
                    // Same time, keep both
                    let renamed = self.generate_conflict_name(&conflict.cloud_file.name);
                    ConflictResolution::KeepBoth {
                        renamed_to: renamed,
                    }
                }
            }
            ConflictType::LocalDeletedCloudModified => {
                // Cloud has newer changes, restore from cloud
                ConflictResolution::UseCloud
            }
            ConflictType::LocalModifiedCloudDeleted => {
                // Local has changes, upload to cloud
                ConflictResolution::UseLocal
            }
            ConflictType::TypeChanged => {
                // Keep both and let user sort it out
                let renamed = self.generate_conflict_name(&conflict.cloud_file.name);
                ConflictResolution::KeepBoth {
                    renamed_to: renamed,
                }
            }
        }
    }

    /// Generate a conflict filename
    fn generate_conflict_name(&self, original: &str) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");

        // Split extension
        if let Some(dot_pos) = original.rfind('.') {
            let (name, ext) = original.split_at(dot_pos);
            format!("{}_conflict_{}{}", name, timestamp, ext)
        } else {
            format!("{}_conflict_{}", original, timestamp)
        }
    }

    /// Get pending conflicts that need manual resolution
    pub fn pending_conflicts(&self) -> &[Conflict] {
        &self.pending_conflicts
    }

    /// Clear pending conflicts
    pub fn clear_pending(&mut self) {
        self.pending_conflicts.clear();
    }

    /// Manually resolve a pending conflict
    pub fn resolve_manual(
        &mut self,
        index: usize,
        resolution: ConflictResolution,
    ) -> Option<Conflict> {
        if index < self.pending_conflicts.len() {
            let mut conflict = self.pending_conflicts.remove(index);
            conflict.resolution = Some(resolution);
            Some(conflict)
        } else {
            None
        }
    }

    /// Detect if there's a conflict between local and cloud versions
    pub fn detect_conflict(
        local_path: &PathBuf,
        local_modified_at: i64,
        local_size: u64,
        local_exists: bool,
        cloud_file: Option<&CloudFile>,
        last_sync_time: Option<i64>,
    ) -> Option<Conflict> {
        match (local_exists, cloud_file) {
            (true, Some(cloud)) => {
                // Both exist - check if both modified since last sync
                let last_sync = last_sync_time.unwrap_or(0);

                let local_changed = local_modified_at > last_sync;
                let cloud_changed = cloud.modified_at > last_sync;

                if local_changed && cloud_changed {
                    Some(Conflict {
                        local_path: local_path.clone(),
                        cloud_file: cloud.clone(),
                        local_modified_at,
                        local_size,
                        conflict_type: ConflictType::BothModified,
                        resolution: None,
                    })
                } else {
                    None
                }
            }
            (false, Some(cloud)) => {
                // Local deleted, cloud exists
                let last_sync = last_sync_time.unwrap_or(0);
                if cloud.modified_at > last_sync {
                    Some(Conflict {
                        local_path: local_path.clone(),
                        cloud_file: cloud.clone(),
                        local_modified_at: 0,
                        local_size: 0,
                        conflict_type: ConflictType::LocalDeletedCloudModified,
                        resolution: None,
                    })
                } else {
                    None // Local delete should propagate
                }
            }
            (true, None) => {
                // Local exists, cloud deleted
                let last_sync = last_sync_time.unwrap_or(0);
                if local_modified_at > last_sync {
                    // Need a placeholder cloud file for the conflict
                    Some(Conflict {
                        local_path: local_path.clone(),
                        cloud_file: CloudFile {
                            id: String::new(),
                            name: local_path
                                .file_name()
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_default(),
                            mime_type: None,
                            size: 0,
                            modified_at: 0,
                            content_hash: None,
                            parent_id: None,
                            is_folder: false,
                            path: local_path.to_string_lossy().into_owned(),
                        },
                        local_modified_at,
                        local_size,
                        conflict_type: ConflictType::LocalModifiedCloudDeleted,
                        resolution: None,
                    })
                } else {
                    None // Cloud delete should propagate
                }
            }
            (false, None) => None, // Both don't exist, no conflict
        }
    }
}

/// Merge result for content-aware merging
#[derive(Debug)]
pub enum MergeResult {
    /// Successfully merged
    Success(Vec<u8>),
    /// Merge not possible, need manual resolution
    CannotMerge,
    /// File type doesn't support merging
    Unsupported,
}

/// Attempt to merge two versions of a file.
///
/// Performs a line-based 3-way (diff3) merge of `local_content` and
/// `cloud_content` against their common ancestor `base_content`. Only
/// text-like MIME types are merged. Without a base version a true 3-way
/// merge is impossible, so divergent content is reported as `CannotMerge`.
pub fn attempt_merge(
    local_content: &[u8],
    cloud_content: &[u8],
    base_content: Option<&[u8]>,
    mime_type: Option<&str>,
) -> MergeResult {
    // Only attempt text merges
    let is_text = mime_type
        .map(|m| {
            m.starts_with("text/")
                || m == "application/json"
                || m == "application/xml"
                || m == "application/javascript"
        })
        .unwrap_or(false);

    if !is_text {
        return MergeResult::Unsupported;
    }

    // Identical content: nothing to merge.
    if local_content == cloud_content {
        return MergeResult::Success(local_content.to_vec());
    }

    let Some(base) = base_content else {
        // No common ancestor: cannot tell which side changed what.
        return MergeResult::CannotMerge;
    };

    // Only one side changed relative to the base: take that side.
    if base == local_content {
        return MergeResult::Success(cloud_content.to_vec());
    }
    if base == cloud_content {
        return MergeResult::Success(local_content.to_vec());
    }

    // Both sides changed: line-based diff3. `Err` carries conflict markers,
    // which we never write back automatically.
    match diffy::merge_bytes(base, local_content, cloud_content) {
        Ok(merged) => MergeResult::Success(merged),
        Err(_) => MergeResult::CannotMerge,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_non_overlapping_text_edits() {
        let base = b"line1\nline2\nline3\nline4\n";
        let local = b"LINE1\nline2\nline3\nline4\n";
        let cloud = b"line1\nline2\nline3\nLINE4\n";
        match attempt_merge(local, cloud, Some(base), Some("text/plain")) {
            MergeResult::Success(m) => assert_eq!(m, b"LINE1\nline2\nline3\nLINE4\n"),
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[test]
    fn merge_overlapping_text_edits_conflicts() {
        let base = b"a\nb\nc\n";
        let local = b"a\nLOCAL\nc\n";
        let cloud = b"a\nCLOUD\nc\n";
        assert!(matches!(
            attempt_merge(local, cloud, Some(base), Some("text/plain")),
            MergeResult::CannotMerge
        ));
    }

    #[test]
    fn merge_one_side_changed_and_edge_cases() {
        let base = b"x\n";
        let changed = b"y\n";
        match attempt_merge(base, changed, Some(base), Some("application/json")) {
            MergeResult::Success(m) => assert_eq!(m, changed),
            other => panic!("expected Success, got {:?}", other),
        }
        // No base and divergent content -> cannot merge
        assert!(matches!(
            attempt_merge(b"a\n", b"b\n", None, Some("text/plain")),
            MergeResult::CannotMerge
        ));
        // Binary types are never merged
        assert!(matches!(
            attempt_merge(b"a", b"b", Some(b"c"), Some("application/pdf")),
            MergeResult::Unsupported
        ));
    }

    #[test]
    fn test_conflict_resolution_newer_wins() {
        let mut resolver = ConflictResolver::new(ConflictStrategy::NewerWins);

        let mut conflict = Conflict {
            local_path: PathBuf::from("/test.txt"),
            cloud_file: CloudFile {
                id: "123".into(),
                name: "test.txt".into(),
                mime_type: Some("text/plain".into()),
                size: 100,
                modified_at: 1000,
                content_hash: None,
                parent_id: None,
                is_folder: false,
                path: "/test.txt".into(),
            },
            local_modified_at: 2000, // Local is newer
            local_size: 150,
            conflict_type: ConflictType::BothModified,
            resolution: None,
        };

        let resolution = resolver.resolve(&mut conflict);
        assert!(matches!(resolution, ConflictResolution::UseLocal));
    }

    #[test]
    fn test_conflict_name_generation() {
        let resolver = ConflictResolver::new(ConflictStrategy::KeepBoth);

        let name = resolver.generate_conflict_name("document.pdf");
        assert!(name.starts_with("document_conflict_"));
        assert!(name.ends_with(".pdf"));

        let name_no_ext = resolver.generate_conflict_name("README");
        assert!(name_no_ext.starts_with("README_conflict_"));
    }
}
