//! Hash-based file storage for sync server
//!
//! Files are stored by their SHA-256 hash in a content-addressable layout:
//! ```text
//! storage/
//!   ab/
//!     abcd1234...  (first 2 chars as directory)
//!   root.json       (current root state)
//!   meta/
//!     {hash}.meta   (filename → hash mapping)
//! ```

use crate::error::{Result, ServerError};
use crate::types::SyncRoot;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Blob hashes are 64-char lowercase hex (sha256). Anything else is rejected
/// before it can be used as a path component.
pub fn is_valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Hash-based storage backend
#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    /// Base directory for storage
    base_path: PathBuf,
    /// Current root state (cached)
    root: RwLock<SyncRoot>,
    /// Filename to hash mapping (for rm-filename lookups)
    filename_map: RwLock<HashMap<String, String>>,
}

impl Storage {
    /// Create new storage at the given path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        
        // Create directories
        fs::create_dir_all(&base_path)?;
        fs::create_dir_all(base_path.join("meta"))?;
        
        // Load or create root state
        let root_path = base_path.join("root.json");
        let root = if root_path.exists() {
            let data = fs::read_to_string(&root_path)?;
            serde_json::from_str(&data)?
        } else {
            SyncRoot::empty()
        };
        
        // Load filename mappings
        let filename_map = Self::load_filename_map(&base_path)?;
        
        Ok(Self {
            inner: Arc::new(StorageInner {
                base_path,
                root: RwLock::new(root),
                filename_map: RwLock::new(filename_map),
            }),
        })
    }
    
    /// Load filename→hash mappings from meta files
    fn load_filename_map(base_path: &Path) -> Result<HashMap<String, String>> {
        let mut map = HashMap::new();
        let meta_dir = base_path.join("meta");
        
        if meta_dir.exists() {
            for entry in fs::read_dir(&meta_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "meta") {
                    let hash = path.file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    let filename = fs::read_to_string(&path)?;
                    map.insert(filename.trim().to_string(), hash);
                }
            }
        }
        
        Ok(map)
    }
    
    /// Directory holding the blobs (and server-side indexes kept alongside them).
    pub fn base_path(&self) -> &Path {
        &self.inner.base_path
    }

    /// Get current root
    pub fn get_root(&self) -> SyncRoot {
        self.inner.root.read().clone()
    }
    
    /// Set new root hash and increment generation
    pub fn set_root(&self, hash: String) -> Result<SyncRoot> {
        self.set_root_if(hash, None)
    }

    /// Set the root hash only if the current generation equals `expected`
    /// (GCS `x-goog-if-generation-match` semantics). `None` skips the check.
    pub fn set_root_if(&self, hash: String, expected: Option<u64>) -> Result<SyncRoot> {
        let mut root = self.inner.root.write();
        if let Some(expected) = expected {
            if expected != root.generation {
                return Err(ServerError::GenerationMismatch { current: root.generation });
            }
        }
        root.hash = hash;
        root.generation += 1;
        
        // Persist
        let root_path = self.inner.base_path.join("root.json");
        let data = serde_json::to_string_pretty(&*root)?;
        fs::write(root_path, data)?;
        
        Ok(root.clone())
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
    
    /// Get file by filename (using rm-filename header mapping)
    pub fn get_by_filename(&self, filename: &str) -> Result<Vec<u8>> {
        let map = self.inner.filename_map.read();
        let hash = map.get(filename)
            .ok_or_else(|| ServerError::NotFound(filename.to_string()))?;
        self.get(hash)
    }
    
    /// Get hash for filename
    pub fn hash_for_filename(&self, filename: &str) -> Option<String> {
        self.inner.filename_map.read().get(filename).cloned()
    }
    
    /// Get filename for a given hash (reverse lookup)
    pub fn filename_for_hash(&self, hash: &str) -> Option<String> {
        let map = self.inner.filename_map.read();
        map.iter()
            .find(|(_, h)| h.as_str() == hash)
            .map(|(f, _)| f.clone())
    }
    
    /// Store file and return its hash
    pub fn put(&self, data: &[u8], filename: &str) -> Result<String> {
        // Calculate SHA-256 hash
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hex::encode(hasher.finalize());
        
        // Create directory and write file
        let path = self.hash_path(&hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;
        
        // Store filename mapping
        let meta_path = self.inner.base_path.join("meta").join(format!("{}.meta", hash));
        fs::write(meta_path, filename)?;
        
        // Update in-memory mapping
        self.inner.filename_map.write().insert(filename.to_string(), hash.clone());
        
        Ok(hash)
    }
    
    /// Store file with explicit hash (for uploads with known hash)
    /// Store a blob under the client-supplied hash.
    ///
    /// The hash is not recomputed from `data`: in sync v3 the hash of an index
    /// (root, `.docSchema`) is derived from its entries' hashes, not its bytes,
    /// so it can't be verified here. Integrity is checked via `x-goog-hash` by the caller.
    pub fn put_with_hash(&self, data: &[u8], hash: &str, filename: &str) -> Result<()> {
        if !is_valid_hash(hash) {
            return Err(ServerError::InvalidHash(hash.to_string()));
        }

        // Write file
        let path = self.hash_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;
        
        // Store mapping
        let meta_path = self.inner.base_path.join("meta").join(format!("{}.meta", hash));
        fs::write(meta_path, filename)?;
        self.inner.filename_map.write().insert(filename.to_string(), hash.to_string());
        
        Ok(())
    }
    
    /// Delete file by hash
    pub fn delete(&self, hash: &str) -> Result<()> {
        let path = self.hash_path(hash);
        if path.exists() {
            fs::remove_file(path)?;
        }
        
        // Remove metadata
        let meta_path = self.inner.base_path.join("meta").join(format!("{}.meta", hash));
        if meta_path.exists() {
            // Get filename before removing
            if let Ok(filename) = fs::read_to_string(&meta_path) {
                self.inner.filename_map.write().remove(filename.trim());
            }
            fs::remove_file(meta_path)?;
        }
        
        Ok(())
    }
    
    /// List all hashes in storage
    /// Walk the sync tree from the current root (root index -> document indexes ->
    /// files) and return every referenced hash that isn't stored.
    ///
    /// Index format: first line is the schema version, then `hash:type:id:subfiles:size`.
    pub fn missing_from_root(&self) -> Result<Vec<String>> {
        let root = self.get_root();
        if root.hash.is_empty() {
            return Ok(Vec::new());
        }
        let entries = |index: &[u8]| -> Vec<String> {
            String::from_utf8_lossy(index)
                .lines()
                .skip(1)
                .filter_map(|l| l.split(':').next())
                .filter(|h| is_valid_hash(h))
                .map(str::to_owned)
                .collect()
        };
        let mut missing = Vec::new();
        let Ok(root_index) = self.get(&root.hash) else {
            return Ok(vec![root.hash]);
        };
        for doc in entries(&root_index) {
            match self.get(&doc) {
                Ok(doc_index) => missing.extend(entries(&doc_index).into_iter().filter(|f| !self.exists(f))),
                Err(_) => missing.push(doc),
            }
        }
        Ok(missing)
    }

    pub fn list_hashes(&self) -> Result<Vec<String>> {
        let mut hashes = Vec::new();
        
        for entry in fs::read_dir(&self.inner.base_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() && entry.file_name().to_string_lossy().len() == 2 {
                // This is a hash prefix directory
                for file in fs::read_dir(&path)? {
                    let file = file?;
                    if file.path().is_file() {
                        if let Some(name) = file.file_name().to_str() {
                            hashes.push(name.to_string());
                        }
                    }
                }
            }
        }
        
        Ok(hashes)
    }
    
    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        let hashes = self.list_hashes().unwrap_or_default();
        let total_size: u64 = hashes.iter()
            .filter_map(|h| self.get(h).ok())
            .map(|data| data.len() as u64)
            .sum();
        
        StorageStats {
            file_count: hashes.len(),
            total_bytes: total_size,
            root_hash: self.get_root().hash,
            generation: self.get_root().generation,
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
        
        // Reset root
        *self.inner.root.write() = SyncRoot::empty();
        self.inner.filename_map.write().clear();
        
        // Recreate meta directory
        fs::create_dir_all(self.inner.base_path.join("meta"))?;
        
        // Remove root.json
        let root_path = self.inner.base_path.join("root.json");
        if root_path.exists() {
            fs::remove_file(root_path)?;
        }
        
        Ok(())
    }
    
    /// List all stored files with hash, filename, and size
    pub fn list(&self) -> Vec<(String, String, usize)> {
        let filename_map = self.inner.filename_map.read();
        filename_map.iter().filter_map(|(filename, hash)| {
            let path = self.inner.base_path.join("blobs").join(&hash[..2]).join(hash);
            if let Ok(metadata) = std::fs::metadata(&path) {
                Some((hash.clone(), filename.clone(), metadata.len() as usize))
            } else {
                None
            }
        }).collect()
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
    }
}
