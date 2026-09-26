//! Crash-report sink for the device crash uploader (software 3.28).
//!
//! crashuploader POSTs minidump/json crash reports to
//! `https://backtrace-proxy.cloud.remarkable.engineering/post?token=..&format=minidump|json&..`
//! as multipart (`upload_file` + `attachment_*`). The device only needs a 200 back.
//! We accept and store each report under `CRASH_DIR` (default `./crash-dumps/<uuid>/`)
//! plus a `meta.json` of the query params. No auth: the query token is a Backtrace
//! project token baked into the device, not one of our credentials, so requiring our
//! auth would break the uploader.
//!
//! Because the route is unauthenticated, disk use is bounded here instead:
//! - the request body is capped at [`MAX_BODY`] (route layer in `lib.rs`);
//! - each part is streamed to disk and truncated at [`MAX_PART_BYTES`];
//! - at most [`MAX_PARTS`] parts are kept per report (the rest are drained);
//! - after each report, the whole crash dir is trimmed to `CRASH_MAX_TOTAL_BYTES`
//!   (default [`DEFAULT_MAX_TOTAL_BYTES`]) and `CRASH_MAX_REPORTS` (default
//!   [`DEFAULT_MAX_REPORTS`]) by deleting the *oldest* reports. New reports are never
//!   rejected, so the device still gets its 200 and stops retrying, while the newest
//!   crashes (the interesting ones) are kept.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use axum::extract::{Multipart, Query};
use axum::http::StatusCode;
use tokio::io::AsyncWriteExt;

/// Body limit for `/post`. xochitl minidumps are a few MiB (thread stacks + module
/// list) and the json/log attachments are KiB, so 32 MiB leaves ample headroom while
/// keeping a single anonymous request far from the 1 GiB blob limit.
pub const MAX_BODY: usize = 32 * 1024 * 1024;
/// Per-part cap; bytes beyond it are dropped (the part is kept, truncated).
pub const MAX_PART_BYTES: u64 = 16 * 1024 * 1024;
/// Parts stored per report; later parts are read and discarded.
pub const MAX_PARTS: usize = 16;
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_MAX_REPORTS: usize = 200;

#[derive(Debug, Clone)]
struct Limits {
    dir: PathBuf,
    max_part_bytes: u64,
    max_parts: usize,
    max_total_bytes: u64,
    max_reports: usize,
}

impl Limits {
    fn from_env() -> Self {
        fn env<T: std::str::FromStr>(key: &str) -> Option<T> {
            std::env::var(key).ok().and_then(|v| v.trim().parse().ok())
        }
        Self {
            dir: std::env::var("CRASH_DIR")
                .unwrap_or_else(|_| "./crash-dumps".into())
                .into(),
            max_part_bytes: MAX_PART_BYTES,
            max_parts: MAX_PARTS,
            max_total_bytes: env("CRASH_MAX_TOTAL_BYTES").unwrap_or(DEFAULT_MAX_TOTAL_BYTES),
            max_reports: env("CRASH_MAX_REPORTS").unwrap_or(DEFAULT_MAX_REPORTS),
        }
    }
}

pub async fn upload(Query(params): Query<HashMap<String, String>>, mp: Multipart) -> StatusCode {
    store(&Limits::from_env(), params, mp).await
}

async fn store(limits: &Limits, params: HashMap<String, String>, mut mp: Multipart) -> StatusCode {
    let id = uuid::Uuid::new_v4().to_string();
    let base = limits.dir.join(&id);
    if tokio::fs::create_dir_all(&base).await.is_err() {
        return StatusCode::OK; // never make the device retry forever on our storage error
    }
    if let Ok(meta) = serde_json::to_vec(&params) {
        let _ = tokio::fs::write(base.join("meta.json"), meta).await;
    }
    let mut used: HashSet<String> = HashSet::from(["meta.json".to_string()]);
    let mut parts = 0usize;
    let mut truncated = 0usize;
    let mut dropped = 0usize;
    while let Ok(Some(mut field)) = mp.next_field().await {
        if parts >= limits.max_parts {
            // Drain (bounded by MAX_BODY) so the device still gets a clean 200.
            while let Ok(Some(_)) = field.chunk().await {}
            dropped += 1;
            continue;
        }
        let raw = field.file_name().or(field.name()).unwrap_or("part");
        let name = unique_name(sanitize(raw, parts), &mut used);
        let Ok(mut file) = tokio::fs::File::create(base.join(&name)).await else {
            break;
        };
        let mut written = 0u64;
        let mut cut = false;
        while let Ok(Some(chunk)) = field.chunk().await {
            let room = limits.max_part_bytes.saturating_sub(written);
            let take = (chunk.len() as u64).min(room) as usize;
            if take < chunk.len() {
                cut = true;
            }
            if take > 0 && file.write_all(&chunk[..take]).await.is_err() {
                break;
            }
            written += take as u64;
        }
        let _ = file.flush().await;
        parts += 1;
        truncated += cut as usize;
    }
    tracing::info!(
        "crash report stored id={id} parts={parts} truncated={truncated} dropped={dropped} format={:?}",
        params.get("format")
    );
    let (dir, max_bytes, max_reports) = (
        limits.dir.clone(),
        limits.max_total_bytes,
        limits.max_reports,
    );
    let _ = tokio::task::spawn_blocking(move || enforce_quota(&dir, max_bytes, max_reports)).await;
    StatusCode::OK
}

/// Keep only `[A-Za-z0-9._-]` (no separators / traversal); fall back to `part{n}`.
fn sanitize(raw: &str, n: usize) -> String {
    let safe: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .take(128)
        .collect();
    if safe.is_empty() || safe.chars().all(|c| c == '.') {
        format!("part{n}")
    } else {
        safe
    }
}

/// Two parts may sanitize to the same name (or to `meta.json`); suffix later ones
/// (`name-1.ext`, `name-2.ext`, ...) instead of overwriting.
fn unique_name(name: String, used: &mut HashSet<String>) -> String {
    if used.insert(name.clone()) {
        return name;
    }
    let (stem, ext) = match name.rfind('.') {
        Some(i) if i > 0 => (&name[..i], &name[i..]),
        _ => (name.as_str(), ""),
    };
    let mut i = 1usize;
    loop {
        let candidate = format!("{stem}-{i}{ext}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        i += 1;
    }
}

fn dir_size(path: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(path) else {
        return 0;
    };
    rd.flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_size(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Delete the oldest report dirs (by mtime, then name) until the crash dir holds at
/// most `max_reports` reports and `max_bytes` bytes. Only subdirectories are touched.
fn enforce_quota(dir: &Path, max_bytes: u64, max_reports: usize) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut reports: Vec<(std::time::SystemTime, PathBuf, u64)> = rd
        .flatten()
        .filter_map(|e| {
            let m = e.metadata().ok()?;
            if !m.is_dir() {
                return None;
            }
            let path = e.path();
            let size = dir_size(&path);
            Some((m.modified().unwrap_or(std::time::UNIX_EPOCH), path, size))
        })
        .collect();
    reports.sort();
    let mut total: u64 = reports.iter().map(|r| r.2).sum();
    let mut count = reports.len();
    for (_, path, size) in reports {
        if count <= max_reports && total <= max_bytes {
            break;
        }
        if std::fs::remove_dir_all(&path).is_ok() {
            tracing::info!("crash quota: evicted {}", path.display());
        }
        count -= 1;
        total = total.saturating_sub(size);
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::FromRequest;

    use super::*;

    fn limits(dir: &Path) -> Limits {
        Limits {
            dir: dir.to_path_buf(),
            max_part_bytes: MAX_PART_BYTES,
            max_parts: MAX_PARTS,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
            max_reports: DEFAULT_MAX_REPORTS,
        }
    }

    async fn multipart(parts: &[(&str, Option<&str>, Vec<u8>)]) -> Multipart {
        const B: &str = "XBOUNDARYX";
        let mut body = Vec::new();
        for (name, file, data) in parts {
            body.extend_from_slice(format!("--{B}\r\n").as_bytes());
            let disp = match file {
                Some(f) => format!("form-data; name=\"{name}\"; filename=\"{f}\""),
                None => format!("form-data; name=\"{name}\""),
            };
            body.extend_from_slice(format!("Content-Disposition: {disp}\r\n\r\n").as_bytes());
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{B}--\r\n").as_bytes());
        let req = axum::http::Request::post("/post")
            .header("content-type", format!("multipart/form-data; boundary={B}"))
            .body(axum::body::Body::from(body))
            .unwrap();
        Multipart::from_request(req, &()).await.unwrap()
    }

    fn report_dirs(dir: &Path) -> Vec<PathBuf> {
        let mut v: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        v.sort();
        v
    }

    fn files(dir: &Path) -> Vec<String> {
        let mut v: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn filename_sanitised() {
        assert_eq!(sanitize("../../etc/passwd", 0), "....etcpasswd");
        assert_eq!(sanitize("..", 3), "part3");
        assert_eq!(sanitize("/", 1), "part1");
        assert_eq!(sanitize("ünï.dmp", 0), "n.dmp");
    }

    #[test]
    fn unique_names_get_suffixes() {
        let mut used = HashSet::from(["meta.json".to_string()]);
        assert_eq!(unique_name("a.dmp".into(), &mut used), "a.dmp");
        assert_eq!(unique_name("a.dmp".into(), &mut used), "a-1.dmp");
        assert_eq!(unique_name("a.dmp".into(), &mut used), "a-2.dmp");
        assert_eq!(unique_name("meta.json".into(), &mut used), "meta-1.json");
        assert_eq!(unique_name("noext".into(), &mut used), "noext");
        assert_eq!(unique_name("noext".into(), &mut used), "noext-1");
    }

    #[tokio::test]
    async fn colliding_part_names_are_all_kept() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mp = multipart(&[
            ("upload_file", Some("a/b.dmp"), b"one".to_vec()),
            ("x", Some("a\\b.dmp"), b"two".to_vec()),
            ("meta.json", None, b"three".to_vec()),
        ])
        .await;
        assert_eq!(
            store(&limits(tmp.path()), HashMap::new(), mp).await,
            StatusCode::OK
        );
        let d = &report_dirs(tmp.path())[0];
        assert_eq!(files(d), ["ab-1.dmp", "ab.dmp", "meta-1.json", "meta.json"]);
        assert_eq!(std::fs::read(d.join("ab.dmp")).unwrap(), b"one");
        assert_eq!(std::fs::read(d.join("ab-1.dmp")).unwrap(), b"two");
        assert_eq!(std::fs::read(d.join("meta-1.json")).unwrap(), b"three");
        assert_eq!(std::fs::read(d.join("meta.json")).unwrap(), b"{}");
    }

    #[tokio::test]
    async fn oversize_part_is_truncated_and_extra_parts_dropped() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut l = limits(tmp.path());
        l.max_part_bytes = 1000;
        l.max_parts = 2;
        let mp = multipart(&[
            ("upload_file", Some("big.dmp"), vec![7u8; 5000]),
            ("attachment_log", Some("log.txt"), b"small".to_vec()),
            ("attachment_x", Some("extra.txt"), b"dropped".to_vec()),
        ])
        .await;
        assert_eq!(store(&l, HashMap::new(), mp).await, StatusCode::OK);
        let d = &report_dirs(tmp.path())[0];
        assert_eq!(std::fs::metadata(d.join("big.dmp")).unwrap().len(), 1000);
        assert_eq!(std::fs::read(d.join("log.txt")).unwrap(), b"small");
        assert!(!d.join("extra.txt").exists());
    }

    #[tokio::test]
    async fn quota_evicts_oldest_reports() {
        let tmp = tempfile::TempDir::new().unwrap();
        // Three pre-existing reports with distinct, old mtimes.
        let now = std::time::SystemTime::now();
        for (i, name) in ["old0", "old1", "old2"].iter().enumerate() {
            let d = tmp.path().join(name);
            std::fs::create_dir(&d).unwrap();
            std::fs::write(d.join("f"), vec![0u8; 100]).unwrap();
            let age = std::time::Duration::from_secs(3600 * (10 - i as u64));
            std::fs::File::open(&d)
                .unwrap()
                .set_modified(now - age)
                .unwrap();
        }
        let mut l = limits(tmp.path());
        l.max_reports = 2;
        let mp = multipart(&[("upload_file", Some("new.dmp"), b"new".to_vec())]).await;
        assert_eq!(store(&l, HashMap::new(), mp).await, StatusCode::OK);
        let names: Vec<_> = report_dirs(tmp.path())
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(names.contains(&"old2".to_string()), "{names:?}");
        assert!(!names.contains(&"old0".to_string()));
        assert!(!names.contains(&"old1".to_string()));

        // Byte quota: old2 (100) + old3 (100) + the new report (~5) exceed 150, so the
        // oldest (old2) goes and the rest fit.
        let d = tmp.path().join("old3");
        std::fs::create_dir(&d).unwrap();
        std::fs::write(d.join("f"), vec![0u8; 100]).unwrap();
        std::fs::File::open(&d)
            .unwrap()
            .set_modified(now - std::time::Duration::from_secs(1))
            .unwrap();
        enforce_quota(tmp.path(), 150, 100);
        assert!(!tmp.path().join("old2").exists(), "oldest evicted by size");
        assert!(tmp.path().join("old3").exists());
        assert_eq!(report_dirs(tmp.path()).len(), 2);
    }
}
