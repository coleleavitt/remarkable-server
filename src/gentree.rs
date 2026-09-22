//! Document tree generation for the gentree API
//!
//! Builds a hierarchical document tree from storage.

use crate::error::{Result, ServerError};
use crate::storage::Storage;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

/// Document tree node representing either a document or folder
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeNode {
    /// Document/folder UUID
    pub id: String,
    /// Display name
    pub visible_name: String,
    /// Type: "DocumentType" or "CollectionType"
    #[serde(rename = "type")]
    pub node_type: String,
    /// Parent UUID (empty for root items)
    pub parent: String,
    /// Content hash (for documents)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// Last modification timestamp
    pub last_modified: String,
    /// Whether the document/folder is pinned
    pub pinned: bool,
    /// Whether deleted (soft delete)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
    /// Child nodes (for folders)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<TreeNode>,
    /// Page count (for documents)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_count: Option<u32>,
    /// File type (notebook, pdf, epub)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_type: Option<String>,
}

/// Full document tree response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentTree {
    /// Tree generation (matches sync generation)
    pub generation: u64,
    /// Root hash for the tree
    pub hash: String,
    /// Schema version
    pub schema_version: u32,
    /// Root-level items (documents and folders)
    pub items: Vec<TreeNode>,
    /// Total document count
    pub document_count: usize,
    /// Total folder count
    pub folder_count: usize,
}

/// Cached document tree with invalidation tracking
#[derive(Clone)]
pub struct TreeCache {
    inner: Arc<TreeCacheInner>,
}

struct TreeCacheInner {
    /// Cached tree
    tree: RwLock<Option<CachedTree>>,
}

struct CachedTree {
    /// The computed tree
    tree: DocumentTree,
    /// Generation at computation time
    generation: u64,
}

impl TreeCache {
    /// Create a new tree cache
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TreeCacheInner {
                tree: RwLock::new(None),
            }),
        }
    }
    
    /// Get or compute the document tree
    pub fn get_or_compute(&self, storage: &Storage) -> Result<DocumentTree> {
        let current_gen = storage.get_root().generation;
        
        // Check cache
        {
            let cache = self.inner.tree.read();
            if let Some(ref cached) = *cache {
                if cached.generation == current_gen {
                    return Ok(cached.tree.clone());
                }
            }
        }
        
        // Compute fresh tree
        let tree = compute_tree(storage)?;
        
        // Update cache
        {
            let mut cache = self.inner.tree.write();
            *cache = Some(CachedTree {
                tree: tree.clone(),
                generation: current_gen,
            });
        }
        
        Ok(tree)
    }
    
    /// Invalidate the cache (call on storage changes)
    pub fn invalidate(&self) {
        *self.inner.tree.write() = None;
    }
}

impl Default for TreeCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Document metadata from .metadata files
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct MetadataFile {
    #[serde(default)]
    created_time: Option<String>,
    #[serde(default)]
    last_modified: Option<String>,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    pinned: Option<bool>,
    #[serde(rename = "type", default)]
    doc_type: Option<String>,
    #[serde(default)]
    visible_name: Option<String>,
    #[serde(default)]
    deleted: Option<bool>,
}

/// Document content from .content files
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContentFile {
    page_count: Option<u32>,
    file_type: Option<String>,
    #[serde(rename = "cPages")]
    c_pages: Option<CPages>,
    pages: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct CPages {
    pages: Option<Vec<serde_json::Value>>,
}

/// Compute the document tree from storage
pub fn compute_tree(storage: &Storage) -> Result<DocumentTree> {
    let root = storage.get_root();
    let hashes = storage.list_hashes()?;
    
    // Build index of all metadata by doc_id
    let mut metadata_map: HashMap<String, MetadataFile> = HashMap::new();
    let mut content_map: HashMap<String, ContentFile> = HashMap::new();
    let mut hash_map: HashMap<String, String> = HashMap::new();
    
    for hash in &hashes {
        if let Ok(data) = storage.get(hash) {
            if let Some(filename) = storage.filename_for_hash(hash) {
                if filename.ends_with(".metadata") {
                    let doc_id = filename.trim_end_matches(".metadata").to_string();
                    if let Ok(meta) = serde_json::from_slice::<MetadataFile>(&data) {
                        metadata_map.insert(doc_id, meta);
                    }
                } else if filename.ends_with(".content") {
                    let doc_id = filename.trim_end_matches(".content").to_string();
                    if let Ok(content) = serde_json::from_slice::<ContentFile>(&data) {
                        content_map.insert(doc_id.clone(), content);
                        hash_map.insert(doc_id, hash.clone());
                    }
                }
            }
        }
    }
    
    // Build flat node list
    let mut nodes: HashMap<String, TreeNode> = HashMap::new();
    let mut document_count = 0;
    let mut folder_count = 0;
    
    for (doc_id, meta) in &metadata_map {
        let content = content_map.get(doc_id);
        let hash = hash_map.get(doc_id).cloned();
        let is_collection = meta.doc_type.as_deref() == Some("CollectionType");
        
        if is_collection {
            folder_count += 1;
        } else {
            document_count += 1;
        }
        
        let page_count = content.and_then(|c| {
            c.page_count.or_else(|| {
                c.c_pages.as_ref()
                    .and_then(|cp| cp.pages.as_ref())
                    .map(|p| p.len() as u32)
                    .or_else(|| c.pages.as_ref().map(|p| p.len() as u32))
            })
        });
        
        nodes.insert(doc_id.clone(), TreeNode {
            id: doc_id.clone(),
            visible_name: meta.visible_name.clone().unwrap_or_default(),
            node_type: meta.doc_type.clone().unwrap_or_else(|| "DocumentType".to_string()),
            parent: meta.parent.clone().unwrap_or_default(),
            hash,
            last_modified: meta.last_modified.clone().unwrap_or_default(),
            pinned: meta.pinned.unwrap_or(false),
            deleted: meta.deleted.unwrap_or(false),
            children: vec![],
            page_count: if is_collection { None } else { page_count },
            file_type: if is_collection { None } else { content.and_then(|c| c.file_type.clone()) },
        });
    }
    
    // Build tree recursively
    fn build_node_with_children(id: &str, nodes: &HashMap<String, TreeNode>) -> TreeNode {
        let mut node = nodes.get(id).cloned().unwrap_or_else(|| TreeNode {
            id: id.to_string(),
            visible_name: String::new(),
            node_type: "DocumentType".to_string(),
            parent: String::new(),
            hash: None,
            last_modified: String::new(),
            pinned: false,
            deleted: false,
            children: vec![],
            page_count: None,
            file_type: None,
        });
        
        // Find all children of this node
        node.children = nodes.values()
            .filter(|n| n.parent == id && !n.deleted)
            .map(|n| build_node_with_children(&n.id, nodes))
            .collect();
        
        // Sort children: folders first, then by name
        node.children.sort_by(|a, b| {
            match (a.node_type.as_str(), b.node_type.as_str()) {
                ("CollectionType", "DocumentType") => std::cmp::Ordering::Less,
                ("DocumentType", "CollectionType") => std::cmp::Ordering::Greater,
                _ => a.visible_name.to_lowercase().cmp(&b.visible_name.to_lowercase()),
            }
        });
        
        node
    }
    
    // Collect root-level items
    let mut root_items: Vec<TreeNode> = nodes.values()
        .filter(|n| {
            let parent = &n.parent;
            !n.deleted && parent != "trash" && 
            (parent.is_empty() || !nodes.contains_key(parent))
        })
        .map(|n| build_node_with_children(&n.id, &nodes))
        .collect();
    
    // Sort root items
    root_items.sort_by(|a, b| {
        match (a.node_type.as_str(), b.node_type.as_str()) {
            ("CollectionType", "DocumentType") => std::cmp::Ordering::Less,
            ("DocumentType", "CollectionType") => std::cmp::Ordering::Greater,
            _ => a.visible_name.to_lowercase().cmp(&b.visible_name.to_lowercase()),
        }
    });
    
    // Compute tree hash
    let tree_hash = compute_tree_hash(&root_items);
    
    Ok(DocumentTree {
        generation: root.generation,
        hash: tree_hash,
        schema_version: 3,
        items: root_items,
        document_count,
        folder_count,
    })
}

/// Compute hash for the entire tree
fn compute_tree_hash(items: &[TreeNode]) -> String {
    let mut hasher = Sha256::new();
    
    for item in items {
        hasher.update(item.id.as_bytes());
        hasher.update(item.visible_name.as_bytes());
        hasher.update(item.last_modified.as_bytes());
        hash_children(&mut hasher, &item.children);
    }
    
    hex::encode(hasher.finalize())
}

fn hash_children(hasher: &mut Sha256, children: &[TreeNode]) {
    for child in children {
        hasher.update(child.id.as_bytes());
        hasher.update(child.visible_name.as_bytes());
        hasher.update(child.last_modified.as_bytes());
        hash_children(hasher, &child.children);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_tree_cache() {
        let cache = TreeCache::new();
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        
        // First call computes
        let tree1 = cache.get_or_compute(&storage).unwrap();
        assert_eq!(tree1.generation, 0);
        
        // Second call returns cached
        let tree2 = cache.get_or_compute(&storage).unwrap();
        assert_eq!(tree1.hash, tree2.hash);
    }

    #[test]
    fn test_empty_tree() {
        let tmp = TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        
        let tree = compute_tree(&storage).unwrap();
        assert_eq!(tree.document_count, 0);
        assert_eq!(tree.folder_count, 0);
        assert!(tree.items.is_empty());
    }
}

