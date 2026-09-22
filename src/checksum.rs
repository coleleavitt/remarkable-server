//! CRC32C checksum utilities for Google Cloud Storage compatibility
//!
//! reMarkable sync API uses GCS which requires CRC32C in x-goog-hash header.

use base64::{Engine as _, engine::general_purpose::STANDARD};

/// Calculate CRC32C checksum using the crc32c crate
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Encode CRC32C as base64 for x-goog-hash header (big-endian, 4 bytes)
pub fn crc32c_base64(data: &[u8]) -> String {
    let crc = crc32c(data);
    STANDARD.encode(crc.to_be_bytes())
}

/// Format for x-goog-hash header value
pub fn format_goog_hash(data: &[u8]) -> String {
    format!("crc32c={}", crc32c_base64(data))
}

/// Parse x-goog-hash header to extract CRC32C value
/// Format: "crc32c=BASE64_VALUE" or "crc32c=BASE64,md5=..."
pub fn parse_goog_hash(header: &str) -> Option<u32> {
    for part in header.split(',') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("crc32c=") {
            let bytes = STANDARD.decode(value.trim()).ok()?;
            if bytes.len() == 4 {
                return Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
            }
        }
    }
    None
}

/// Verify checksum matches expected value
pub fn verify_checksum(data: &[u8], expected_header: &str) -> bool {
    if let Some(expected) = parse_goog_hash(expected_header) {
        crc32c(data) == expected
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c_known_vector() {
        // Standard test vector: "123456789" should produce 0xE3069283
        let result = crc32c(b"123456789");
        assert_eq!(result, 0xE3069283);
    }

    #[test]
    fn test_crc32c_base64() {
        let result = crc32c_base64(b"123456789");
        // 0xE3069283 in big-endian base64
        assert_eq!(result, "4waSgw==");
    }

    #[test]
    fn test_format_goog_hash() {
        let header = format_goog_hash(b"123456789");
        assert_eq!(header, "crc32c=4waSgw==");
    }

    #[test]
    fn test_parse_goog_hash() {
        assert_eq!(parse_goog_hash("crc32c=4waSgw=="), Some(0xE3069283));
        assert_eq!(parse_goog_hash("crc32c=4waSgw==,md5=xxx"), Some(0xE3069283));
        assert_eq!(parse_goog_hash("invalid"), None);
    }

    #[test]
    fn test_verify_checksum() {
        assert!(verify_checksum(b"123456789", "crc32c=4waSgw=="));
        assert!(!verify_checksum(b"wrong data", "crc32c=4waSgw=="));
    }
}
