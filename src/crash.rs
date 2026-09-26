//! Crash-report sink for the device crash uploader (software 3.28).
//!
//! crashuploader POSTs minidump/json crash reports to
//! `https://backtrace-proxy.cloud.remarkable.engineering/post?token=..&format=minidump|json&..`
//! as multipart (`upload_file` + `attachment_*`). The device only needs a 200 back.
//! We accept and store each report under `CRASH_DIR` (default `./crash-dumps/<uuid>/`)
//! plus a `meta.json` of the query params. No auth (the query token is ignored).

use std::collections::HashMap;
use std::path::Path;

use axum::extract::{Multipart, Query};
use axum::http::StatusCode;

pub async fn upload(
    Query(params): Query<HashMap<String, String>>,
    mut mp: Multipart,
) -> StatusCode {
    let dir = std::env::var("CRASH_DIR").unwrap_or_else(|_| "./crash-dumps".into());
    let id = uuid::Uuid::new_v4().to_string();
    let base = Path::new(&dir).join(&id);
    if std::fs::create_dir_all(&base).is_err() {
        return StatusCode::OK; // never make the device retry forever on our storage error
    }
    if let Ok(meta) = serde_json::to_vec(&params) {
        let _ = std::fs::write(base.join("meta.json"), meta);
    }
    let mut parts = 0usize;
    while let Ok(Some(field)) = mp.next_field().await {
        let raw = field
            .file_name()
            .or(field.name())
            .unwrap_or("part")
            .to_string();
        // sanitise: no path separators / traversal
        let safe: String = raw
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(c, '.' | '_' | '-'))
            .collect();
        let name = if safe.is_empty() || safe == "." || safe == ".." {
            format!("part{parts}")
        } else {
            safe
        };
        if let Ok(bytes) = field.bytes().await {
            let _ = std::fs::write(base.join(&name), &bytes);
            parts += 1;
        }
    }
    tracing::info!(
        "crash report stored id={id} parts={parts} format={:?}",
        params.get("format")
    );
    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filename_sanitised() {
        let raw = "../../etc/passwd";
        let safe: String = raw
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(c, '.' | '_' | '-'))
            .collect();
        assert!(!safe.contains('/'));
    }
}
