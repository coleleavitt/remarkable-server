//! Protocol types for reMarkable sync API

use serde::{Deserialize, Serialize};


/// Root response from GET /sync/v3/root
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRoot {
    /// SHA-256 hash of the root index
    pub hash: String,
    /// Monotonic generation counter
    pub generation: u64,
    /// Schema version (always 3 for sync v3)
    #[serde(rename = "schemaVersion", default = "default_schema_version")]
    pub schema_version: u32,
}

fn default_schema_version() -> u32 {
    3
}

impl SyncRoot {
    pub fn new(hash: String, generation: u64) -> Self {
        Self {
            hash,
            generation,
            schema_version: 3,
        }
    }
    
    /// Empty root for initial state
    pub fn empty() -> Self {
        Self {
            hash: String::new(),
            generation: 0,
            schema_version: 3,
        }
    }
}

/// Document entry in root index
/// Format in root: `{hash}:{type}:{uuid}:{version}:{size}`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocEntry {
    /// SHA-256 hash of document schema
    pub hash: String,
    /// Entry type (80000000 for document, etc.)
    #[serde(rename = "type")]
    pub entry_type: String,
    /// Document UUID
    pub uuid: String,
    /// Version number
    pub version: String,
    /// Size in bytes
    pub size: String,
}

impl DocEntry {
    /// Parse from line format: `hash:type:uuid:version:size`
    pub fn parse(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() >= 5 {
            Some(Self {
                hash: parts[0].to_string(),
                entry_type: parts[1].to_string(),
                uuid: parts[2].to_string(),
                version: parts[3].to_string(),
                size: parts[4].to_string(),
            })
        } else {
            None
        }
    }
    
    /// Serialize to line format
    pub fn to_line(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.hash, self.entry_type, self.uuid, self.version, self.size
        )
    }
}

/// File entry in document schema
/// Format: `{hash}:{flags}:{filename}:{offset}:{size}`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// SHA-256 hash of file content
    pub hash: String,
    /// Flags (typically 0)
    pub flags: String,
    /// Filename within document (e.g., "uuid.metadata", "uuid/page.rm")
    pub filename: String,
    /// Offset (typically 0)
    pub offset: String,
    /// Size in bytes
    pub size: u64,
}

impl FileEntry {
    /// Parse from line format: `hash:flags:filename:offset:size`
    pub fn parse(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() >= 5 {
            Some(Self {
                hash: parts[0].to_string(),
                flags: parts[1].to_string(),
                filename: parts[2].to_string(),
                offset: parts[3].to_string(),
                size: parts[4].parse().unwrap_or(0),
            })
        } else {
            None
        }
    }
    
    /// Serialize to line format
    pub fn to_line(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.hash, self.flags, self.filename, self.offset, self.size
        )
    }
}

/// Document schema (content of {doc_uuid}.docSchema)
#[derive(Debug, Clone)]
pub struct DocumentSchema {
    /// Schema version (always 3)
    pub version: u32,
    /// Files in this document
    pub files: Vec<FileEntry>,
}

impl DocumentSchema {
    /// Parse schema from text format
    /// 
    /// Format:
    /// ```text
    /// 3
    /// {count}
    /// {hash}:{flags}:{filename}:{offset}:{size}
    /// ...
    /// ```
    pub fn parse(data: &str) -> Option<Self> {
        let lines: Vec<&str> = data.lines().collect();
        if lines.len() < 2 {
            return None;
        }
        
        let version: u32 = lines[0].trim().parse().ok()?;
        let count: usize = lines[1].trim().parse().ok()?;
        
        let mut files = Vec::with_capacity(count);
        for line in lines.iter().skip(2) {
            if let Some(entry) = FileEntry::parse(line) {
                files.push(entry);
            }
        }
        
        Some(Self { version, files })
    }
    
    /// Serialize to text format
    pub fn to_string(&self) -> String {
        let mut output = format!("{}\n{}\n", self.version, self.files.len());
        for file in &self.files {
            output.push_str(&file.to_line());
            output.push('\n');
        }
        output
    }
}

/// Root index (content of root hash file)
#[derive(Debug, Clone)]
pub struct RootIndex {
    /// Schema version
    pub version: u32,
    /// Document entries
    pub docs: Vec<DocEntry>,
}

impl RootIndex {
    /// Parse from text format
    pub fn parse(data: &str) -> Option<Self> {
        let lines: Vec<&str> = data.lines().collect();
        if lines.is_empty() {
            return Some(Self {
                version: 3,
                docs: vec![],
            });
        }
        
        let version: u32 = lines[0].trim().parse().ok()?;
        
        let mut docs = Vec::new();
        for line in lines.iter().skip(1) {
            if !line.trim().is_empty() {
                if let Some(entry) = DocEntry::parse(line) {
                    docs.push(entry);
                }
            }
        }
        
        Some(Self { version, docs })
    }
    
    /// Serialize to text format
    pub fn to_string(&self) -> String {
        let mut output = format!("{}\n", self.version);
        for doc in &self.docs {
            output.push_str(&doc.to_line());
            output.push('\n');
        }
        output
    }
    
    /// Create empty index
    pub fn empty() -> Self {
        Self {
            version: 3,
            docs: vec![],
        }
    }
}

/// Token request body for /token/json/2/user/new
#[derive(Debug, Deserialize)]
pub struct TokenRefreshRequest {
    // No body required, uses Authorization header
}

/// Token response
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub token: String,
}

/// Device registration request for /token/json/2/device/new
#[derive(Debug, Deserialize)]
pub struct DeviceRegisterRequest {
    pub code: String,
    #[serde(rename = "deviceDesc")]
    pub device_desc: String,
    #[serde(rename = "deviceID")]
    pub device_id: String,
}

/// Upload response
#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub hash: String,
    pub size: u64,
}

/// Upload headers required for PUT /sync/v3/files/{hash}
#[derive(Debug, Clone)]
pub struct UploadHeaders {
    /// rm-filename: filename within document
    pub filename: String,
    /// rm-parent-hash: parent hash for tree structure
    pub parent_hash: Option<String>,
    /// rm-sync-id: sync session ID
    pub sync_id: Option<String>,
    /// rm-batch-number: batch number for multi-file uploads
    pub batch_number: Option<u32>,
    /// rm-expect-version: expected version for conflict detection
    pub expect_version: Option<u64>,
    /// x-goog-hash: CRC32C checksum
    pub goog_hash: Option<String>,
}

impl UploadHeaders {
    pub fn new(filename: String) -> Self {
        Self {
            filename,
            parent_hash: None,
            sync_id: None,
            batch_number: None,
            expect_version: None,
            goog_hash: None,
        }
    }
}
