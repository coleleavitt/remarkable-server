use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRoot {
    pub hash: String,
    pub generation: u64,
    #[serde(rename = "schemaVersion", default = "default_schema")]
    pub schema_version: u32,
}
fn default_schema() -> u32 {
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
    pub fn empty() -> Self {
        Self {
            hash: String::new(),
            generation: 0,
            schema_version: 3,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeviceRegisterRequest {
    pub code: String,
    #[serde(rename = "deviceDesc")]
    pub device_desc: String,
    #[serde(rename = "deviceID")]
    pub device_id: String,
}

#[derive(Debug, Serialize)]
pub struct PairingCodeResponse {
    pub code: String,
    pub expires_in: u64,
}

#[derive(Debug, Serialize)]
pub struct DeviceInfo {
    #[serde(rename = "deviceId")]
    pub device_id: String,
    #[serde(rename = "deviceDesc")]
    pub device_desc: String,
    #[serde(rename = "registeredAt")]
    pub registered_at: String,
    #[serde(rename = "lastActivity")]
    pub last_activity: String,
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub hash: String,
    pub size: u64,
}
