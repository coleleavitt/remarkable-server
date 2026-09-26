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

/// Strict parse of a request's `x-goog-hash`: every comma-separated part must be
/// `key=value`, and exactly one must be a well-formed `crc32c=` (other keys such as
/// `md5=` are accepted alongside, unchecked). `None` means the header is malformed.
pub fn parse_goog_hash_strict(header: &str) -> Option<u32> {
    let mut crc = None;
    for part in header.split(',') {
        let (key, value) = part.trim().split_once('=')?;
        if value.trim().is_empty() { return None; }
        if key.trim().eq_ignore_ascii_case("crc32c") {
            let bytes: [u8; 4] = STANDARD.decode(value.trim()).ok()?.try_into().ok()?;
            if crc.replace(u32::from_be_bytes(bytes)).is_some() { return None; }
        }
    }
    crc
}

/// Check an upload body against its `x-goog-hash` header. Absent header: nothing to
/// check (GCS semantics). Present but without a usable crc32c: 400, never silently skipped.
pub fn verify_goog_hash_header(headers: &axum::http::HeaderMap, body: &[u8]) -> crate::error::Result<()> {
    use crate::error::ServerError;
    let Some(raw) = headers.get("x-goog-hash") else { return Ok(()) };
    let goog = raw.to_str().ok().filter(|v| !v.trim().is_empty());
    let Some(crc) = goog.and_then(parse_goog_hash_strict) else {
        tracing::warn!(header = ?raw, "rejected malformed x-goog-hash");
        return Err(ServerError::InvalidHeader(format!("x-goog-hash: {raw:?} (expected crc32c=<base64>)")));
    };
    if crc != crc32c(body) {
        return Err(ServerError::ChecksumMismatch { expected: goog.unwrap_or_default().to_string(), actual: format_goog_hash(body) });
    }
    Ok(())
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
    fn test_parse_goog_hash_strict() {
        assert_eq!(parse_goog_hash_strict("crc32c=4waSgw=="), Some(0xE3069283));
        assert_eq!(parse_goog_hash_strict("crc32c=4waSgw==,md5=XUFAKrxLKna5cZ2REBfFkg=="), Some(0xE3069283));
        assert_eq!(parse_goog_hash_strict("md5=XUFAKrxLKna5cZ2REBfFkg==, crc32c=4waSgw=="), Some(0xE3069283));
        for bad in ["", "invalid", "crc32c=", "crc32c=!!!", "crc32c=AAAAAAAA", "md5=XUFAKrxLKna5cZ2REBfFkg==",
                    "crc32c=4waSgw==,crc32c=4waSgw==", "crc32c=4waSgw==,junk"] {
            assert_eq!(parse_goog_hash_strict(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn test_verify_goog_hash_header() {
        use axum::http::{HeaderMap, HeaderValue};
        let mut h = HeaderMap::new();
        assert!(verify_goog_hash_header(&h, b"123456789").is_ok(), "absent header is unconditional");
        h.insert("x-goog-hash", HeaderValue::from_static("crc32c=4waSgw==,md5=xxx"));
        assert!(verify_goog_hash_header(&h, b"123456789").is_ok());
        assert!(matches!(verify_goog_hash_header(&h, b"other"), Err(crate::error::ServerError::ChecksumMismatch { .. })));
        h.insert("x-goog-hash", HeaderValue::from_static("crc32c=garbage"));
        assert!(matches!(verify_goog_hash_header(&h, b"123456789"), Err(crate::error::ServerError::InvalidHeader(_))));
    }

    #[test]
    fn test_verify_checksum() {
        assert!(verify_checksum(b"123456789", "crc32c=4waSgw=="));
        assert!(!verify_checksum(b"wrong data", "crc32c=4waSgw=="));
    }
}
