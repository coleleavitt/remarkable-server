//! Telemetry the tablet posts (`ping.remarkable.com`: `/v1/reports`,
//! `/analytics/v2/events`, …). Kept in `reports.jsonl` next to the blobs so
//! usage (screen share sessions included) can be looked at, instead of being
//! dropped. Bounded: bodies are capped and the file rotates once.

use std::io::Write;
use std::path::{Path, PathBuf};

use axum::body::Bytes;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::{require_admin, AppState};
use crate::error::Result;

/// Largest body kept per report. Also the request body limit on the telemetry
/// routes (see `lib.rs`), so nothing larger than we would store is accepted.
pub(crate) const MAX_BODY: usize = 64 * 1024;
/// `reports.jsonl` is moved to `reports.jsonl.1` past this size.
const MAX_FILE: u64 = 8 * 1024 * 1024;
static WRITE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn log_path(base: &Path) -> PathBuf {
    base.join("reports.jsonl")
}

/// Where `log_path` is moved on rotation; also read back by `recent`.
fn rotated_path(base: &Path) -> PathBuf {
    log_path(base).with_extension("jsonl.1")
}

/// Append one report. Failures are logged, never returned: telemetry must
/// not break the tablet.
fn append(base: &Path, path: &str, body: &[u8]) {
    let body = &body[..body.len().min(MAX_BODY)];
    let parsed = serde_json::from_slice::<Value>(body).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()));
    let line = json!({ "at": chrono::Utc::now().to_rfc3339(), "path": path, "body": parsed }).to_string();
    let file = log_path(base);
    let _guard = WRITE.lock();
    if std::fs::metadata(&file).is_ok_and(|m| m.len() > MAX_FILE) {
        let _ = std::fs::rename(&file, rotated_path(base));
    }
    let result = std::fs::OpenOptions::new().create(true).append(true).open(&file).and_then(|mut f| writeln!(f, "{line}"));
    if let Err(e) = result {
        tracing::warn!("could not record report from {path}: {e}");
    }
}

/// `POST /v1/reports`, `/v2/reports`, `/report/v1`, `/v2/events`, `/sync/reports/v1` -> 200.
pub async fn store(State(state): State<AppState>, OriginalUri(uri): OriginalUri, body: Bytes) -> StatusCode {
    append(state.storage.base_path(), uri.path(), &body);
    StatusCode::OK
}

/// `POST /analytics/v2/events` -> 201 `{"message":"Success"}`.
pub async fn store_analytics(State(state): State<AppState>, OriginalUri(uri): OriginalUri, body: Bytes) -> (StatusCode, Json<Value>) {
    append(state.storage.base_path(), uri.path(), &body);
    (StatusCode::CREATED, Json(json!({ "message": "Success" })))
}

#[derive(Deserialize)]
pub struct ListQuery {
    /// Newest entries to return (default 100).
    limit: Option<usize>,
    /// Keep entries whose JSON contains this text, e.g. `screenshare`.
    contains: Option<String>,
}

/// Newest `limit` entries (optionally only those containing `contains`,
/// case-insensitive) from `reports.jsonl` and its rotated `reports.jsonl.1`,
/// oldest first.
pub fn recent(base: &Path, limit: usize, contains: Option<&str>) -> Vec<Value> {
    let needle = contains.map(str::to_lowercase);
    // The current file holds the newest entries; the rotated `.1` (if present)
    // holds the older ones. Walk both newest-first so rotation doesn't drop
    // history, and parse before `take` so unparseable/torn lines don't eat the
    // quota.
    //
    // Both files are read under the same lock `append` takes for rotation, so
    // the two-file snapshot is consistent: without it, a rotation landing
    // between the reads could show the pre-rotation current file as both the
    // current and the `.1` (duplicated entries) while the new current is missed.
    let (current, rotated) = {
        let _guard = WRITE.lock();
        (std::fs::read_to_string(log_path(base)).unwrap_or_default(), std::fs::read_to_string(rotated_path(base)).unwrap_or_default())
    };
    let mut hits: Vec<Value> = current
        .lines()
        .rev()
        .chain(rotated.lines().rev())
        .filter(|l| needle.as_deref().is_none_or(|n| l.to_lowercase().contains(n)))
        .filter_map(|l| serde_json::from_str(l).ok())
        .take(limit)
        .collect();
    hits.reverse();
    hits
}

/// `GET /admin/reports?limit=&contains=` (requires `x-admin-token`).
pub async fn list(State(state): State<AppState>, headers: HeaderMap, Query(q): Query<ListQuery>) -> Result<Json<Vec<Value>>> {
    require_admin(&headers)?;
    Ok(Json(recent(state.storage.base_path(), q.limit.unwrap_or(100).min(1000), q.contains.as_deref())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_are_kept_and_filtered() {
        let tmp = tempfile::tempdir().unwrap();
        append(tmp.path(), "/v1/reports", br#"{"event":"screenshare-client-connected","roomId":"r1"}"#);
        append(tmp.path(), "/v1/reports", b"not json");
        append(tmp.path(), "/v2/events", br#"{"event":"sync"}"#);
        let all = recent(tmp.path(), 10, None);
        assert_eq!(all.len(), 3);
        assert_eq!(all[1]["body"], "not json");
        let share = recent(tmp.path(), 10, Some("ScreenShare"));
        assert_eq!(share.len(), 1);
        assert_eq!(share[0]["body"]["roomId"], "r1");
        assert_eq!(recent(tmp.path(), 1, None)[0]["path"], "/v2/events");
    }

    #[test]
    fn large_bodies_are_capped() {
        let tmp = tempfile::tempdir().unwrap();
        append(tmp.path(), "/v1/reports", &vec![b'x'; MAX_BODY * 2]);
        let kept = recent(tmp.path(), 1, None);
        assert_eq!(kept[0]["body"].as_str().unwrap().len(), MAX_BODY);
    }

    #[test]
    fn rotated_file_is_still_read() {
        let tmp = tempfile::tempdir().unwrap();
        append(tmp.path(), "/v1/reports", br#"{"n":1}"#);
        append(tmp.path(), "/v1/reports", br#"{"n":2}"#);
        // Simulate rotation: the current file becomes the older `.1`.
        std::fs::rename(log_path(tmp.path()), rotated_path(tmp.path())).unwrap();
        append(tmp.path(), "/v1/reports", br#"{"n":3}"#);
        // History across both files, oldest first.
        let all = recent(tmp.path(), 10, None);
        assert_eq!(all.len(), 3);
        assert_eq!([&all[0]["body"]["n"], &all[1]["body"]["n"], &all[2]["body"]["n"]], [&json!(1), &json!(2), &json!(3)]);
        // The limit spans both files and keeps the newest.
        let newest = recent(tmp.path(), 2, None);
        assert_eq!(newest.len(), 2);
        assert_eq!([&newest[0]["body"]["n"], &newest[1]["body"]["n"]], [&json!(2), &json!(3)]);
    }

    #[test]
    fn torn_lines_do_not_consume_limit() {
        let tmp = tempfile::tempdir().unwrap();
        append(tmp.path(), "/v1/reports", br#"{"a":1}"#);
        // A write torn BEFORE its newline (a crash mid-write): note there is no
        // trailing '\n', so the next appended report is concatenated onto it and
        // the two become a single invalid-JSON line.
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(log_path(tmp.path())).unwrap();
            write!(f, "{{\"at\":\"x\",\"pa").unwrap();
        }
        append(tmp.path(), "/v1/reports", br#"{"b":2}"#);
        // The corrupted joined line is skipped (its swallowed report is lost),
        // but the intact earlier report still comes back...
        let all = recent(tmp.path(), 10, None);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0]["body"]["a"], json!(1));
        // ...and the skipped line never crowds the valid one out of the limit.
        let one = recent(tmp.path(), 1, None);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0]["body"]["a"], json!(1));
    }
}
