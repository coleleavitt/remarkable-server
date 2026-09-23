//! Firmware OTA server for reMarkable devices
//! 
//! Serves firmware updates from a local archive, supporting:
//! - Version checking and compatibility
//! - Firmware downloads  
//! - Changelog generation
//! - Rollback support (serving older versions)
//! - Delta updates (if delta files exist)

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::Arc,
};
use tokio::{fs::File, io::{AsyncReadExt, AsyncSeekExt}};
use tokio_util::io::ReaderStream;

use crate::error::{Result, ServerError};

/// Device type identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeviceType {
    Rm1,      // reMarkable 1
    Rm2,      // reMarkable 2
    Ferrari,  // Paper Pro (codename)
    Chiappa,  // reMarkable 2 variant
    Tatsu,    // Newest device
}

impl DeviceType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "rm1" | "remarkable1" | "remarkable 1" => Some(Self::Rm1),
            "rm2" | "remarkable2" | "remarkable 2" => Some(Self::Rm2),
            "ferrari" | "paper_pro" | "paperpro" => Some(Self::Ferrari),
            "chiappa" => Some(Self::Chiappa),
            "tatsu" => Some(Self::Tatsu),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Rm1 => "rm1",
            Self::Rm2 => "rm2", 
            Self::Ferrari => "ferrari",
            Self::Chiappa => "chiappa",
            Self::Tatsu => "tatsu",
        }
    }
    
    /// Returns compatible device families (for firmware that works across models)
    fn compatible_with(&self) -> Vec<DeviceType> {
        match self {
            Self::Rm1 => vec![Self::Rm1],
            Self::Rm2 => vec![Self::Rm2, Self::Chiappa], // rm2 and chiappa share firmware
            Self::Ferrari => vec![Self::Ferrari],
            Self::Chiappa => vec![Self::Chiappa, Self::Rm2],
            Self::Tatsu => vec![Self::Tatsu],
        }
    }
}

/// Parsed firmware version (major.minor.patch.build)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FirmwareVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub build: u32,
}

impl FirmwareVersion {
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 4 {
            return None;
        }
        Some(Self {
            major: parts[0].parse().ok()?,
            minor: parts[1].parse().ok()?,
            patch: parts[2].parse().ok()?,
            build: parts[3].parse().ok()?,
        })
    }
    
    pub fn to_string(&self) -> String {
        format!("{}.{}.{}.{}", self.major, self.minor, self.patch, self.build)
    }
    
    /// Compare versions, returning ordering
    pub fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.major, self.minor, self.patch, self.build)
            .cmp(&(other.major, other.minor, other.patch, other.build))
    }
    
    pub fn is_newer_than(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Greater
    }
}

impl Serialize for FirmwareVersion {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for FirmwareVersion {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).ok_or_else(|| serde::de::Error::custom("invalid version format"))
    }
}

/// Firmware image metadata
#[derive(Debug, Clone, Serialize)]
pub struct FirmwareInfo {
    pub version: FirmwareVersion,
    pub device: DeviceType,
    pub filename: String,
    pub size: u64,
    pub checksum: Option<String>,
    pub release_type: ReleaseType,
    pub download_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReleaseType {
    Production,
    Memfault,    // Debug/diagnostic builds
    Prototype,   // CT prototype builds
    Beta,
}

impl ReleaseType {
    fn from_str(s: &str) -> Self {
        match s {
            "production" => Self::Production,
            "memfault" => Self::Memfault,
            "ct-prototype" => Self::Prototype,
            "beta" => Self::Beta,
            _ => Self::Production,
        }
    }
}

/// Delta update info (if available)
#[derive(Debug, Clone, Serialize)]
pub struct DeltaUpdate {
    pub from_version: FirmwareVersion,
    pub to_version: FirmwareVersion,
    pub filename: String,
    pub size: u64,
}

/// Firmware manager - scans and serves firmware files
#[derive(Clone)]
pub struct FirmwareManager {
    archive_path: PathBuf,
    /// All firmware indexed by (device, version)
    firmware: Arc<HashMap<(DeviceType, String), FirmwareInfo>>,
    /// Latest version per device
    latest: Arc<HashMap<DeviceType, FirmwareVersion>>,
    /// All versions per device (sorted newest first)
    versions: Arc<HashMap<DeviceType, Vec<FirmwareVersion>>>,
    /// Delta updates indexed by (device, from_version, to_version)
    deltas: Arc<HashMap<(DeviceType, String, String), DeltaUpdate>>,
    /// Base URL for downloads
    base_url: String,
}

impl FirmwareManager {
    /// Create new firmware manager, scanning the archive directory
    pub fn new(archive_path: impl Into<PathBuf>, base_url: &str) -> Result<Self> {
        let archive_path = archive_path.into();
        if !archive_path.exists() {
            return Err(ServerError::NotFound(format!(
                "Firmware archive not found: {}",
                archive_path.display()
            )));
        }

        let mut firmware = HashMap::new();
        let mut versions_map: HashMap<DeviceType, Vec<FirmwareVersion>> = HashMap::new();
        let deltas = HashMap::new(); // TODO: Scan for delta files

        // Scan for .swu files
        // Pattern: remarkable-{type}-image-{version}-{device}-public.swu
        let re = regex::Regex::new(
            r"remarkable-(\w+(?:-\w+)?)-image-(\d+\.\d+\.\d+\.\d+)-(\w+)-public\.swu"
        ).unwrap();

        for entry in fs::read_dir(&archive_path).map_err(|e| ServerError::Storage(e))? {
            let entry = entry.map_err(|e| ServerError::Storage(e))?;
            let path = entry.path();
            
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(caps) = re.captures(filename) {
                    let release_type = ReleaseType::from_str(&caps[1]);
                    let version_str = &caps[2];
                    let device_str = &caps[3];
                    
                    if let (Some(version), Some(device)) = (
                        FirmwareVersion::parse(version_str),
                        DeviceType::from_str(device_str),
                    ) {
                        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                        let key = (device, version_str.to_string());
                        
                        let info = FirmwareInfo {
                            version: version.clone(),
                            device,
                            filename: filename.to_string(),
                            size,
                            checksum: None, // Could compute SHA256 on startup
                            release_type,
                            download_url: format!("{}/firmware/v1/download/{}", base_url, version_str),
                        };
                        
                        // Only keep production builds as primary, but track all
                        if release_type == ReleaseType::Production {
                            firmware.insert(key, info);
                            versions_map.entry(device).or_default().push(version);
                        }
                    }
                }
            }
        }

        // Sort versions and find latest
        let mut latest = HashMap::new();
        for (device, versions) in versions_map.iter_mut() {
            versions.sort_by(|a, b| b.cmp(a)); // Newest first
            if let Some(v) = versions.first() {
                latest.insert(*device, v.clone());
            }
        }

        tracing::info!(
            "Firmware manager initialized: {} images across {} devices",
            firmware.len(),
            latest.len()
        );
        for (device, version) in &latest {
            tracing::info!("  {}: latest {}", device.as_str(), version.to_string());
        }

        Ok(Self {
            archive_path,
            firmware: Arc::new(firmware),
            latest: Arc::new(latest),
            versions: Arc::new(versions_map),
            deltas: Arc::new(deltas),
            base_url: base_url.to_string(),
        })
    }

    /// Get the latest firmware for a device
    pub fn get_latest(&self, device: DeviceType) -> Option<&FirmwareInfo> {
        let version = self.latest.get(&device)?;
        self.firmware.get(&(device, version.to_string()))
    }

    /// Get specific firmware version
    pub fn get_version(&self, device: DeviceType, version: &str) -> Option<&FirmwareInfo> {
        self.firmware.get(&(device, version.to_string()))
    }

    /// Get all versions for a device (newest first)
    pub fn get_versions(&self, device: DeviceType) -> Vec<&FirmwareInfo> {
        self.versions
            .get(&device)
            .map(|versions| {
                versions
                    .iter()
                    .filter_map(|v| self.firmware.get(&(device, v.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if update is available
    pub fn check_update(&self, device: DeviceType, current: &str) -> UpdateCheckResult {
        let current_version = match FirmwareVersion::parse(current) {
            Some(v) => v,
            None => {
                return UpdateCheckResult {
                    update_available: false,
                    current_version: current.to_string(),
                    latest_version: None,
                    firmware: None,
                    delta: None,
                    rollback_versions: vec![],
                }
            }
        };

        let latest = self.latest.get(&device);
        let update_available = latest
            .map(|l| l.is_newer_than(&current_version))
            .unwrap_or(false);

        let firmware = if update_available {
            latest.and_then(|v| self.firmware.get(&(device, v.to_string())).cloned())
        } else {
            None
        };

        // Check for delta update
        let delta = if update_available {
            if let Some(latest_version) = latest {
                self.deltas
                    .get(&(device, current.to_string(), latest_version.to_string()))
                    .cloned()
            } else {
                None
            }
        } else {
            None
        };

        // Get rollback versions (older than current)
        let rollback_versions: Vec<String> = self
            .versions
            .get(&device)
            .map(|versions| {
                versions
                    .iter()
                    .filter(|v| current_version.is_newer_than(v))
                    .take(5) // Limit to 5 rollback options
                    .map(|v| v.to_string())
                    .collect()
            })
            .unwrap_or_default();

        UpdateCheckResult {
            update_available,
            current_version: current.to_string(),
            latest_version: latest.map(|v| v.to_string()),
            firmware,
            delta,
            rollback_versions,
        }
    }

    /// Get path to firmware file
    pub fn get_firmware_path(&self, device: DeviceType, version: &str) -> Option<PathBuf> {
        self.firmware
            .get(&(device, version.to_string()))
            .map(|info| self.archive_path.join(&info.filename))
    }

    /// Generate changelog between versions
    pub fn get_changelog(&self, device: DeviceType, from: Option<&str>, to: Option<&str>) -> ChangelogResponse {
        let versions = self.get_versions(device);
        
        let entries: Vec<ChangelogEntry> = versions
            .iter()
            .filter(|info| {
                // Filter by from/to if specified
                let v = &info.version;
                let after_from = from
                    .and_then(FirmwareVersion::parse)
                    .map(|f| v.is_newer_than(&f) || v == &f)
                    .unwrap_or(true);
                let before_to = to
                    .and_then(FirmwareVersion::parse)
                    .map(|t| t.is_newer_than(v) || v == &t)
                    .unwrap_or(true);
                after_from && before_to
            })
            .map(|info| ChangelogEntry {
                version: info.version.to_string(),
                release_type: info.release_type,
                size: info.size,
                // TODO: Read actual changelog from firmware metadata
                changes: generate_synthetic_changelog(&info.version),
            })
            .collect();

        ChangelogResponse {
            device: device.as_str().to_string(),
            entries,
        }
    }
}

/// Generate synthetic changelog (in production, extract from firmware metadata)
fn generate_synthetic_changelog(version: &FirmwareVersion) -> Vec<String> {
    // This would ideally be extracted from the firmware's embedded changelog
    vec![
        format!("System update {}", version.to_string()),
        "Bug fixes and performance improvements".to_string(),
    ]
}

// === API Types ===

#[derive(Serialize)]
pub struct UpdateCheckResult {
    pub update_available: bool,
    pub current_version: String,
    pub latest_version: Option<String>,
    pub firmware: Option<FirmwareInfo>,
    pub delta: Option<DeltaUpdate>,
    pub rollback_versions: Vec<String>,
}

#[derive(Serialize)]
pub struct ChangelogEntry {
    pub version: String,
    pub release_type: ReleaseType,
    pub size: u64,
    pub changes: Vec<String>,
}

#[derive(Serialize)]
pub struct ChangelogResponse {
    pub device: String,
    pub entries: Vec<ChangelogEntry>,
}

#[derive(Deserialize)]
pub struct CheckQuery {
    pub device: String,
    pub version: String,
}

#[derive(Deserialize)]
pub struct DownloadQuery {
    pub device: Option<String>,
}

#[derive(Deserialize)]
pub struct ChangelogQuery {
    pub device: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

// === API State ===

#[derive(Clone)]
pub struct FirmwareState {
    pub manager: FirmwareManager,
}

impl FirmwareState {
    pub fn new(manager: FirmwareManager) -> Self {
        Self { manager }
    }
}

// === API Handlers ===

/// GET /firmware/v1/check?device={type}&version={current}
/// Check for firmware updates
pub async fn check_update(
    State(state): State<FirmwareState>,
    Query(query): Query<CheckQuery>,
) -> Result<Json<UpdateCheckResult>> {
    let device = DeviceType::from_str(&query.device)
        .ok_or_else(|| ServerError::NotFound(format!("Unknown device type: {}", query.device)))?;
    
    let result = state.manager.check_update(device, &query.version);
    Ok(Json(result))
}

/// GET /firmware/v1/download/{version}?device={type}
/// Download firmware file
pub async fn download_firmware(
    State(state): State<FirmwareState>,
    Path(version): Path<String>,
    Query(query): Query<DownloadQuery>,
    headers: HeaderMap,
) -> Result<Response> {
    // Try to find firmware for any device if not specified
    let devices = if let Some(d) = &query.device {
        vec![DeviceType::from_str(d)
            .ok_or_else(|| ServerError::NotFound(format!("Unknown device: {}", d)))?]
    } else {
        vec![DeviceType::Rm1, DeviceType::Rm2, DeviceType::Ferrari, DeviceType::Chiappa, DeviceType::Tatsu]
    };

    // Find the firmware
    let mut firmware_path = None;
    let mut filename = String::new();
    let mut size = 0u64;
    
    for device in devices {
        if let Some(info) = state.manager.get_version(device, &version) {
            if let Some(path) = state.manager.get_firmware_path(device, &version) {
                if path.exists() {
                    firmware_path = Some(path);
                    filename = info.filename.clone();
                    size = info.size;
                    break;
                }
            }
        }
    }

    let path = firmware_path
        .ok_or_else(|| ServerError::NotFound(format!("Firmware version not found: {}", version)))?;

    // Stream the file, honouring a single `Range: bytes=` request so interrupted
    // update downloads can resume.
    let mut file = File::open(&path).await.map_err(ServerError::Storage)?;
    let total = file.metadata().await.map_err(ServerError::Storage)?.len();
    let _ = size; // index size may be stale; trust the file on disk
    let disposition = format!("attachment; filename=\"{}\"", filename);

    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|v| parse_byte_range(v, total));

    match range {
        Some(Some((start, end))) => {
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(ServerError::Storage)?;
            let len = end - start + 1;
            let body = Body::from_stream(ReaderStream::new(file.take(len)));
            Ok((
                StatusCode::PARTIAL_CONTENT,
                [
                    (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                    (header::CONTENT_LENGTH, len.to_string()),
                    (header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, total)),
                    (header::ACCEPT_RANGES, "bytes".to_string()),
                    (header::CONTENT_DISPOSITION, disposition),
                ],
                body,
            )
                .into_response())
        }
        Some(None) => Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{}", total))],
        )
            .into_response()),
        None => {
            let body = Body::from_stream(ReaderStream::new(file));
            Ok((
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                    (header::CONTENT_LENGTH, total.to_string()),
                    (header::ACCEPT_RANGES, "bytes".to_string()),
                    (header::CONTENT_DISPOSITION, disposition),
                ],
                body,
            )
                .into_response())
        }
    }
}

/// Parse a single-range `bytes=` header against a resource of `total` bytes.
/// Returns inclusive `(start, end)`, or `None` if unsatisfiable/unsupported.
fn parse_byte_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let spec = value.trim().strip_prefix("bytes=")?;
    if spec.contains(',') || total == 0 {
        return None; // multi-range not supported
    }
    let (a, b) = spec.split_once('-')?;
    let (a, b) = (a.trim(), b.trim());
    let (start, end) = if a.is_empty() {
        // suffix range: last N bytes
        let n: u64 = b.parse().ok()?;
        if n == 0 {
            return None;
        }
        (total.saturating_sub(n), total - 1)
    } else {
        let start: u64 = a.parse().ok()?;
        let end = if b.is_empty() { total - 1 } else { b.parse::<u64>().ok()?.min(total - 1) };
        (start, end)
    };
    (start <= end && start < total).then_some((start, end))
}

/// GET /firmware/v1/changelog?device={type}&from={version}&to={version}
/// Get changelog between versions
pub async fn get_changelog(
    State(state): State<FirmwareState>,
    Query(query): Query<ChangelogQuery>,
) -> Result<Json<ChangelogResponse>> {
    let device = query
        .device
        .as_ref()
        .and_then(|d| DeviceType::from_str(d))
        .unwrap_or(DeviceType::Rm2);
    
    let changelog = state.manager.get_changelog(
        device,
        query.from.as_deref(),
        query.to.as_deref(),
    );
    
    Ok(Json(changelog))
}

/// GET /firmware/v1/versions?device={type}
/// List all available versions for a device
pub async fn list_versions(
    State(state): State<FirmwareState>,
    Query(query): Query<DownloadQuery>,
) -> Result<Json<Vec<FirmwareInfo>>> {
    let device = query
        .device
        .as_ref()
        .and_then(|d| DeviceType::from_str(d))
        .unwrap_or(DeviceType::Rm2);
    
    let versions: Vec<FirmwareInfo> = state.manager.get_versions(device)
        .into_iter()
        .cloned()
        .collect();
    
    Ok(Json(versions))
}

/// GET /firmware/v1/devices
/// List supported devices and their latest versions
pub async fn list_devices(
    State(state): State<FirmwareState>,
) -> Json<HashMap<String, Option<String>>> {
    let devices = [
        DeviceType::Rm1,
        DeviceType::Rm2,
        DeviceType::Ferrari,
        DeviceType::Chiappa,
        DeviceType::Tatsu,
    ];
    
    let mut result = HashMap::new();
    for device in devices {
        let latest = state.manager.get_latest(device).map(|f| f.version.to_string());
        result.insert(device.as_str().to_string(), latest);
    }
    
    Json(result)
}

/// Create firmware router
pub fn firmware_router(state: FirmwareState) -> axum::Router {
    use axum::routing::get;
    
    axum::Router::new()
        .route("/check", get(check_update))
        .route("/download/{version}", get(download_firmware))
        .route("/changelog", get(get_changelog))
        .route("/versions", get(list_versions))
        .route("/devices", get(list_devices))
        .with_state(state)
}

#[cfg(test)]
mod tests {

    #[test]
    fn byte_range_parsing() {
        assert_eq!(super::parse_byte_range("bytes=0-1023", 5000), Some((0, 1023)));
        assert_eq!(super::parse_byte_range("bytes=100-", 5000), Some((100, 4999)));
        assert_eq!(super::parse_byte_range("bytes=-500", 5000), Some((4500, 4999)));
        assert_eq!(super::parse_byte_range("bytes=0-99999", 5000), Some((0, 4999)));
        assert_eq!(super::parse_byte_range("bytes=6000-", 5000), None);
        assert_eq!(super::parse_byte_range("bytes=0-1,5-9", 5000), None);
        assert_eq!(super::parse_byte_range("items=0-1", 5000), None);
    }
    use super::*;

    #[test]
    fn test_version_parsing() {
        let v = FirmwareVersion::parse("3.28.0.172").unwrap();
        assert_eq!(v.major, 3);
        assert_eq!(v.minor, 28);
        assert_eq!(v.patch, 0);
        assert_eq!(v.build, 172);
        
        assert!(FirmwareVersion::parse("invalid").is_none());
        assert!(FirmwareVersion::parse("1.2.3").is_none());
    }

    #[test]
    fn test_version_comparison() {
        let v1 = FirmwareVersion::parse("3.27.0.97").unwrap();
        let v2 = FirmwareVersion::parse("3.28.0.172").unwrap();
        let v3 = FirmwareVersion::parse("3.28.0.172").unwrap();
        
        assert!(v2.is_newer_than(&v1));
        assert!(!v1.is_newer_than(&v2));
        assert!(!v2.is_newer_than(&v3));
    }

    #[test]
    fn test_device_type_parsing() {
        assert_eq!(DeviceType::from_str("rm1"), Some(DeviceType::Rm1));
        assert_eq!(DeviceType::from_str("remarkable2"), Some(DeviceType::Rm2));
        assert_eq!(DeviceType::from_str("ferrari"), Some(DeviceType::Ferrari));
        assert_eq!(DeviceType::from_str("unknown"), None);
    }
}
