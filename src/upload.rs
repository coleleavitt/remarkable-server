//! Streaming uploads: a request body (or multipart field, or a base64 string inside a
//! JSON body) is written to a temp file under [`Storage::staging_dir`] as it arrives,
//! with its crc32c (and sha256 when the caller needs a content hash) computed on the
//! way. Nothing larger than one write buffer is held in memory, so concurrent large
//! uploads can't exhaust RAM.
//!
//! A [`Staged`] file is removed when dropped unless it was handed to storage
//! ([`Staged::commit`]) or moved elsewhere ([`Staged::persist`]), so every error path
//! (bad checksum, client hang-up, size limit, storage error) cleans up after itself.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::extract::FromRequest;
use axum::extract::multipart::Field;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::error::{Result, ServerError};
use crate::json_scan::{JsonScanner, Output, ScanError, Scanned};
use crate::storage::Storage;

/// Axum's `DefaultBodyLimit` default (2 MiB): what routes without an explicit limit
/// accepted when they still buffered the body with `Bytes`.
pub const DEFAULT_LIMIT: u64 = 2 * 1024 * 1024;

/// Write buffer between the body stream and the temp file.
const WRITE_BUF: usize = 256 * 1024;

/// Base64 characters decoded per step (a multiple of 4, so every step is whole quads).
const B64_STEP: usize = 64 * 1024;

/// Staged files older than this are leftovers from a crash and are swept.
const STALE_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

/// A fully received upload sitting in the staging directory.
#[derive(Debug)]
pub struct Staged {
    path: Option<PathBuf>,
    len: u64,
    crc: u32,
    sha256: Option<String>,
    head: Vec<u8>,
}

impl Staged {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// crc32c of the whole upload.
    pub fn crc32c(&self) -> u32 {
        self.crc
    }

    /// Lowercase hex sha256 of the upload, if it was requested when staging.
    pub fn sha256_hex(&self) -> Option<&str> {
        self.sha256.as_deref()
    }

    /// The first few bytes (magic-number checks).
    pub fn head(&self) -> &[u8] {
        &self.head
    }

    /// Store as blob `hash` (renamed into place, catalogued). Removed on failure.
    pub fn commit(mut self, storage: &Storage, hash: &str, filename: &str) -> Result<u64> {
        // `path` is only cleared by `commit`/`persist`, which consume `self`.
        let path = self.path.as_deref().expect("staged file present");
        let size = storage.put_file_with_hash(path, hash, filename)?;
        self.path = None;
        Ok(size)
    }

    /// Move to `dest` (must be on the store's filesystem). Removed on failure.
    pub fn persist(mut self, dest: &Path) -> Result<()> {
        // `path` is only cleared by `commit`/`persist`, which consume `self`.
        let path = self.path.as_deref().expect("staged file present");
        std::fs::rename(path, dest)?;
        self.path = None;
        Ok(())
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Incremental writer behind every `stage_*` function.
struct Stager {
    out: BufWriter<tokio::fs::File>,
    /// Temp file path; `None` once handed to a [`Staged`] (which then owns cleanup).
    path: Option<PathBuf>,
    len: u64,
    limit: u64,
    crc: u32,
    sha: Option<Sha256>,
    head: Vec<u8>,
}

impl Stager {
    async fn new(storage: &Storage, limit: u64, want_sha: bool) -> Result<Self> {
        let dir = storage.staging_dir();
        tokio::fs::create_dir_all(&dir).await?;
        sweep_stale(&dir);
        let path = dir.join(format!("{}.part", uuid::Uuid::new_v4()));
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        Ok(Self {
            out: BufWriter::with_capacity(WRITE_BUF, file),
            path: Some(path),
            len: 0,
            limit,
            crc: 0,
            sha: want_sha.then(Sha256::new),
            head: Vec::new(),
        })
    }

    async fn write(&mut self, chunk: &[u8]) -> Result<()> {
        self.len += chunk.len() as u64;
        if self.len > self.limit {
            return Err(too_large(self.limit));
        }
        if self.head.len() < 16 {
            let take = (16 - self.head.len()).min(chunk.len());
            self.head.extend_from_slice(&chunk[..take]);
        }
        self.crc = crc32c::crc32c_append(self.crc, chunk);
        if let Some(sha) = &mut self.sha {
            sha.update(chunk);
        }
        self.out.write_all(chunk).await?;
        Ok(())
    }

    /// Flush and fsync (the file may be renamed straight into the store), then hand over.
    async fn finish(mut self) -> Result<Staged> {
        self.out.flush().await?;
        self.out.get_mut().sync_all().await?;
        Ok(Staged {
            path: self.path.take(),
            len: self.len,
            crc: self.crc,
            sha256: self.sha.take().map(|s| hex::encode(s.finalize())),
            head: std::mem::take(&mut self.head),
        })
    }
}

impl Drop for Stager {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn too_large(limit: u64) -> ServerError {
    ServerError::PayloadTooLarge(format!("request body exceeds {limit} bytes"))
}

/// Best-effort: remove staged files a crash left behind. In-flight uploads are far younger.
fn sweep_stale(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| now.duration_since(t).ok())
            .is_some_and(|age| age > STALE_AFTER);
        if old {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Stream a raw request body to a staged file, refusing more than `limit` bytes (413,
/// up front when `content-length` already says so). A body read error is a 400, as
/// axum's `Bytes` extractor reported it.
pub async fn stage_body(
    storage: &Storage,
    headers: &HeaderMap,
    body: Body,
    limit: u64,
    want_sha: bool,
) -> Result<Staged> {
    check_declared_length(headers, limit)?;
    let mut stager = Stager::new(storage, limit, want_sha).await?;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        stager.write(&chunk.map_err(body_read_error)?).await?;
    }
    stager.finish().await
}

/// 413 up front when `content-length` already exceeds `limit`.
fn check_declared_length(headers: &HeaderMap, limit: u64) -> Result<()> {
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > limit) {
        return Err(too_large(limit));
    }
    Ok(())
}

/// A body read error is a 400, as axum's `Bytes` extractor reported it.
fn body_read_error(e: axum::Error) -> ServerError {
    ServerError::BadRequest(format!("failed to read request body: {e}"))
}

/// Stream a multipart field to a staged file. Field read errors map to
/// `ServerError::Config` (400), as the handlers' `field.bytes()` calls did; the overall
/// size is still bounded by the route's `DefaultBodyLimit`, which `Multipart` enforces.
pub async fn stage_field(
    storage: &Storage,
    mut field: Field<'_>,
    want_sha: bool,
) -> Result<Staged> {
    let mut stager = Stager::new(storage, u64::MAX, want_sha).await?;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| ServerError::Config(e.to_string()))?
    {
        stager.write(&chunk).await?;
    }
    stager.finish().await
}

/// Standard (padded) base64 decoded as it arrives, a step of whole quads at a time, so
/// the decoded blob never has to exist in memory. Accepts exactly what `STANDARD.decode`
/// of the whole input accepts, with the same bytes: padding may only end the input, and
/// the input is cut into the same `step`-sized pieces whatever the pushes look like, each
/// decoded like the corresponding part of a whole-input decode.
struct Base64Decoder {
    step: usize,
    /// Input not decoded yet (less than one step).
    pending: Vec<u8>,
    /// What the last `push`/`finish` decoded.
    decoded: Vec<u8>,
    pad_seen: bool,
    valid: bool,
}

impl Base64Decoder {
    /// `step` must be a non-zero multiple of 4.
    fn new(step: usize) -> Self {
        debug_assert!(step > 0 && step % 4 == 0);
        Self {
            step,
            pending: Vec::new(),
            decoded: Vec::new(),
            pad_seen: false,
            valid: true,
        }
    }

    /// Take the next part of the input; returns the bytes it completed (none once the
    /// input is known to be invalid).
    fn push(&mut self, mut chars: &[u8]) -> &[u8] {
        self.decoded.clear();
        if !self.valid {
            return &[];
        }
        // After the first '=' only more '=' may follow.
        let pad_from = if self.pad_seen {
            Some(0)
        } else {
            chars.iter().position(|&c| c == b'=')
        };
        if let Some(p) = pad_from {
            self.pad_seen = true;
            if chars[p..].iter().any(|&c| c != b'=') {
                return self.invalid();
            }
        }
        if !self.pending.is_empty() {
            let take = (self.step - self.pending.len()).min(chars.len());
            self.pending.extend_from_slice(&chars[..take]);
            chars = &chars[take..];
            if self.pending.len() < self.step {
                return &[];
            }
            if STANDARD
                .decode_vec(&self.pending, &mut self.decoded)
                .is_err()
            {
                return self.invalid();
            }
            self.pending.clear();
        }
        while chars.len() >= self.step {
            let (quads, rest) = chars.split_at(self.step);
            if STANDARD.decode_vec(quads, &mut self.decoded).is_err() {
                return self.invalid();
            }
            chars = rest;
        }
        self.pending.extend_from_slice(chars);
        &self.decoded
    }

    /// End of input: the remaining bytes, or `None` if it wasn't valid base64.
    fn finish(&mut self) -> Option<&[u8]> {
        self.decoded.clear();
        if self.valid
            && STANDARD
                .decode_vec(&self.pending, &mut self.decoded)
                .is_err()
        {
            self.invalid();
        }
        self.valid.then_some(&self.decoded[..])
    }

    fn invalid(&mut self) -> &[u8] {
        self.valid = false;
        self.pending = Vec::new();
        self.decoded.clear();
        &[]
    }
}

/// A base64 string decoded into a staged file as its characters arrive.
pub(crate) struct Base64Stager {
    dec: Base64Decoder,
    stager: Stager,
}

impl Base64Stager {
    pub(crate) async fn new(storage: &Storage, want_sha: bool) -> Result<Self> {
        Ok(Self {
            dec: Base64Decoder::new(B64_STEP),
            stager: Stager::new(storage, u64::MAX, want_sha).await?,
        })
    }

    /// The next base64 characters (any split). Invalid input is noted, not an error:
    /// see [`Self::finish`].
    pub(crate) async fn push(&mut self, chars: &[u8]) -> Result<()> {
        let decoded = self.dec.push(chars);
        if !decoded.is_empty() {
            self.stager.write(decoded).await?;
        }
        Ok(())
    }

    /// The staged blob, or `None` if the input wasn't valid base64 (the file is removed).
    pub(crate) async fn finish(self) -> Result<Option<Staged>> {
        let Self {
            mut dec,
            mut stager,
        } = self;
        let Some(tail) = dec.finish() else {
            return Ok(None);
        };
        if !tail.is_empty() {
            stager.write(tail).await?;
        }
        stager.finish().await.map(Some)
    }
}

/// Largest unescaped `capture` member [`stage_json_base64`] holds in memory.
pub(crate) const MAX_JSON_FIELD: usize = 4096;

/// Input handed to the JSON scanner at a time, so one huge body frame doesn't mean an
/// equally huge unescaped copy.
const FEED_STEP: usize = 64 * 1024;

/// Read a JSON request body whose top-level `key` member is a base64 string, decoding
/// that string into a staged file as the body arrives (see [`crate::json_scan`]); the
/// `capture` members come back as strings of at most [`MAX_JSON_FIELD`] bytes. Neither
/// the body nor the blob is held whole. The body is refused past `limit` bytes (413,
/// up front when `content-length` says so); a read error is a 400 as in [`stage_body`];
/// invalid JSON is a 400.
///
/// The staged blob is the last occurrence of `key`, if that was a string (see
/// [`Scanned::streamed`]); call [`Base64Stager::finish`] to get it.
pub(crate) async fn stage_json_base64(
    storage: &Storage,
    headers: &HeaderMap,
    body: Body,
    limit: u64,
    key: &'static str,
    capture: &'static [&'static str],
) -> Result<(Scanned, Option<Base64Stager>)> {
    check_declared_length(headers, limit)?;
    let mut scanner = JsonScanner::new(key, capture, MAX_JSON_FIELD);
    let mut out = Output::default();
    let mut blob: Option<Base64Stager> = None;
    let mut received = 0u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(body_read_error)?;
        received += chunk.len() as u64;
        if received > limit {
            return Err(too_large(limit));
        }
        for piece in chunk.chunks(FEED_STEP) {
            scanner.feed(piece, &mut out).map_err(scan_error)?;
            if std::mem::take(&mut out.restart) {
                // A (new) occurrence of `key`: whatever an earlier one staged is dropped.
                blob = Some(Base64Stager::new(storage, false).await?);
            }
            if let Some(blob) = &mut blob {
                blob.push(&out.bytes).await?;
            }
            out.bytes.clear();
        }
    }
    let scanned = scanner.finish().map_err(scan_error)?;
    Ok((scanned, blob))
}

fn scan_error(e: ScanError) -> ServerError {
    match e {
        ScanError::Syntax { .. } => ServerError::BadRequest(e.to_string()),
        ScanError::TooLong { .. } => ServerError::PayloadTooLarge(e.to_string()),
    }
}

/// The 415 axum's `Json` extractor gives a request without a JSON `content-type`, for
/// handlers that read a JSON body themselves. Runs axum's own check on the headers.
pub(crate) async fn json_content_type_rejection(headers: &HeaderMap) -> Option<Response> {
    let mut probe = axum::extract::Request::new(Body::empty());
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        probe.headers_mut().insert(header::CONTENT_TYPE, ct.clone());
    }
    match axum::Json::<serde::de::IgnoredAny>::from_request(probe, &()).await {
        Err(rejection @ JsonRejection::MissingJsonContentType(_)) => {
            Some(rejection.into_response())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn residue(storage: &Storage) -> usize {
        std::fs::read_dir(storage.staging_dir())
            .map(|d| d.count())
            .unwrap_or(0)
    }

    /// `input` pushed through a [`Base64Decoder`] in pieces cut at `cuts` (offsets,
    /// any order, out of range ignored).
    fn decode_in_pieces(input: &[u8], step: usize, cuts: &[usize]) -> Option<Vec<u8>> {
        let mut cuts: Vec<usize> = cuts.iter().copied().filter(|&c| c <= input.len()).collect();
        cuts.push(0);
        cuts.push(input.len());
        cuts.sort_unstable();
        let mut dec = Base64Decoder::new(step);
        let mut out = Vec::new();
        for w in cuts.windows(2) {
            out.extend_from_slice(dec.push(&input[w[0]..w[1]]));
        }
        out.extend_from_slice(dec.finish()?);
        Some(out)
    }

    #[tokio::test]
    async fn base64_stager_matches_whole_decode() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let data: Vec<u8> = (0..(B64_STEP * 3 + 7))
            .map(|i| (i * 31 % 251) as u8)
            .collect();
        let b64 = STANDARD.encode(&data);
        // Pushed in uneven pieces, so steps straddle pushes.
        let mut blob = Base64Stager::new(&storage, true).await.unwrap();
        let mut rest = b64.as_bytes();
        for n in [1, 3, 5, 7, 4093, 65_537].into_iter().cycle() {
            if rest.is_empty() {
                break;
            }
            let (piece, tail) = rest.split_at(n.min(rest.len()));
            blob.push(piece).await.unwrap();
            rest = tail;
        }
        let staged = blob.finish().await.unwrap().unwrap();
        assert_eq!(staged.len(), data.len() as u64);
        assert_eq!(staged.crc32c(), crc32c::crc32c(&data));
        assert_eq!(
            staged.sha256_hex().unwrap(),
            hex::encode(Sha256::digest(&data))
        );
        assert!(std::fs::read(staged.path.as_deref().unwrap()).unwrap() == data);
        drop(staged);
        for s in ["", "QQ==", "QUI=", "QUJD"] {
            let mut blob = Base64Stager::new(&storage, false).await.unwrap();
            blob.push(s.as_bytes()).await.unwrap();
            let ok = blob.finish().await.unwrap();
            assert_eq!(
                ok.map(|s| s.len() as usize),
                STANDARD.decode(s).ok().map(|d| d.len()),
                "{s:?}"
            );
        }
        for bad in ["QQ", "QQ==QUJD", "QU J", "Q===", "QR==", "!!!!"] {
            assert!(STANDARD.decode(bad).is_err(), "{bad:?}");
            let mut blob = Base64Stager::new(&storage, false).await.unwrap();
            blob.push(bad.as_bytes()).await.unwrap();
            assert!(blob.finish().await.unwrap().is_none(), "{bad:?}");
        }
        assert_eq!(residue(&storage), 0);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(1024))]
        /// Incremental decoding accepts exactly what a whole-string decode accepts, with
        /// the same bytes, whatever the pushes and across step boundaries (4- and 8-char
        /// steps).
        #[test]
        fn base64_decoder_agrees_with_whole_decode(
            s in "[A-Za-z0-9+/=]{0,40}",
            data in proptest::collection::vec(proptest::num::u8::ANY, 0..40),
            cuts in proptest::collection::vec(0usize..60, 0..6),
        ) {
            for input in [s, STANDARD.encode(&data)] {
                let want = STANDARD.decode(&input).ok();
                for step in [4, 8] {
                    let got = decode_in_pieces(input.as_bytes(), step, &cuts);
                    proptest::prop_assert_eq!(&got, &want, "{:?} step {} cuts {:?}", input, step, cuts);
                }
            }
        }
    }

    #[tokio::test]
    async fn limit_and_drop_leave_no_residue() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let body = Body::from(vec![7u8; 100]);
        let err = stage_body(&storage, &HeaderMap::new(), body, 99, false)
            .await
            .unwrap_err();
        assert!(matches!(err, ServerError::PayloadTooLarge(_)));
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_LENGTH, "1000".parse().unwrap());
        let err = stage_body(&storage, &h, Body::empty(), 99, false)
            .await
            .unwrap_err();
        assert!(matches!(err, ServerError::PayloadTooLarge(_)));
        let staged = stage_body(&storage, &HeaderMap::new(), Body::from("abc"), 99, false)
            .await
            .unwrap();
        assert_eq!(residue(&storage), 1);
        drop(staged);
        assert_eq!(residue(&storage), 0);
    }
}

/// Each converted route, end to end through the real router, with a body big enough to
/// span many chunks: identical bytes stored under the right hash, bad checksums refused,
/// and nothing left in the staging directory either way.
#[cfg(test)]
mod route_tests {
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::api::AppState;
    use crate::device::DeviceManager;

    const BIG: usize = 20 * 1024 * 1024;

    fn setup() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        (AppState::new(storage, devices), format!("Bearer {tk}"), tmp)
    }

    fn data(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_be_bytes()[0] ^ seed)
            .collect()
    }

    fn sha(d: &[u8]) -> String {
        hex::encode(Sha256::digest(d))
    }

    fn goog(d: &[u8]) -> String {
        crate::checksum::format_goog_hash(d)
    }

    /// A body delivered in 64 KiB frames, as a network body would be.
    fn chunked(d: &[u8]) -> Body {
        let frames: Vec<std::result::Result<bytes::Bytes, std::io::Error>> = d
            .chunks(64 * 1024)
            .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
            .collect();
        Body::from_stream(futures_util::stream::iter(frames))
    }

    async fn send(state: &AppState, req: Request<Body>) -> (StatusCode, bytes::Bytes) {
        let resp = crate::create_router(state.clone())
            .oneshot(req)
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, body)
    }

    fn clean(state: &AppState) {
        let left = std::fs::read_dir(state.storage.staging_dir())
            .map(|d| d.count())
            .unwrap_or(0);
        assert_eq!(left, 0, "staging directory not empty");
    }

    #[tokio::test]
    async fn sync_v3_put_file_streams() {
        let (state, auth, _tmp) = setup();
        let d = data(BIG, 1);
        let hash = sha(&d);
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/sync/v3/files/{hash}"))
            .header("authorization", &auth)
            .header("rm-filename", "doc/page.rm")
            .header("x-goog-hash", goog(&d))
            .body(chunked(&d))
            .unwrap();
        let (status, body) = send(&state, req).await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp["hash"], hash.as_str());
        assert_eq!(resp["size"], BIG as u64);
        assert!(state.storage.get(&hash).unwrap() == d);
        assert_eq!(
            state.storage.filename_for_hash(&hash).as_deref(),
            Some("doc/page.rm")
        );
        assert_eq!(
            state.storage.hash_for_filename("doc/page.rm").as_deref(),
            Some(hash.as_str())
        );
        clean(&state);

        // Wrong crc: 400 checksum_mismatch, nothing stored, no temp residue.
        let other = data(BIG, 2);
        let other_hash = sha(&other);
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/sync/v3/files/{other_hash}"))
            .header("authorization", &auth)
            .header("rm-filename", "doc/other.rm")
            .header("x-goog-hash", goog(b"something else"))
            .body(chunked(&other))
            .unwrap();
        let (status, body) = send(&state, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(String::from_utf8_lossy(&body).contains("checksum_mismatch"));
        assert!(!state.storage.exists(&other_hash));
        assert!(state.storage.hash_for_filename("doc/other.rm").is_none());
        clean(&state);

        // Malformed header: 400 before the body is read.
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/sync/v3/files/{other_hash}"))
            .header("authorization", &auth)
            .header("rm-filename", "doc/other.rm")
            .header("x-goog-hash", "crc32c=nope")
            .body(chunked(&other))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::BAD_REQUEST);
        // Invalid hash: 400, staged file removed.
        let req = Request::builder()
            .method(Method::PUT)
            .uri("/sync/v3/files/not-a-hash")
            .header("authorization", &auth)
            .header("rm-filename", "x")
            .body(chunked(b"abc"))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::BAD_REQUEST);
        clean(&state);
    }

    #[tokio::test]
    async fn aborted_body_leaves_nothing_behind() {
        let (state, auth, _tmp) = setup();
        let hash = "d".repeat(64);
        let frames: Vec<std::result::Result<bytes::Bytes, std::io::Error>> = vec![
            Ok(bytes::Bytes::from(vec![1u8; 1024 * 1024])),
            Err(std::io::Error::other("client went away")),
        ];
        let req = Request::put(format!("/sync/v3/files/{hash}"))
            .header("authorization", &auth)
            .header("rm-filename", "x.rm")
            .body(Body::from_stream(futures_util::stream::iter(frames)))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::BAD_REQUEST);
        assert!(!state.storage.exists(&hash));
        clean(&state);
    }

    #[tokio::test]
    async fn blobstorage_put_streams_and_root_still_works() {
        let (state, _auth, _tmp) = setup();
        let d = data(BIG, 3);
        let hash = sha(&d);
        let (token, _) = state.devices.sign_blob(&hash, true).unwrap();
        let uri = format!(
            "/blobstorage?blob={hash}&token={}",
            urlencoding::encode(&token)
        );
        let req = Request::put(&uri)
            .header("x-goog-hash", goog(&d))
            .body(chunked(&d))
            .unwrap();
        let (status, body) = send(&state, req).await;
        assert_eq!((status, &body[..]), (StatusCode::OK, &b"{}"[..]));
        assert!(state.storage.get(&hash).unwrap() == d);
        assert_eq!(
            state.storage.filename_for_hash(&hash).as_deref(),
            Some(hash.as_str())
        );
        clean(&state);

        // Same blob, corrupted in transit: refused, stored copy untouched.
        let mut bad = d.clone();
        bad[BIG / 2] ^= 0xff;
        let req = Request::put(&uri)
            .header("x-goog-hash", goog(&d))
            .body(chunked(&bad))
            .unwrap();
        let (status, body) = send(&state, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(String::from_utf8_lossy(&body).contains("checksum_mismatch"));
        assert!(state.storage.get(&hash).unwrap() == d);
        clean(&state);

        // The root blob (a hash as text) with its generation header.
        let (token, _) = state.devices.sign_blob("root", true).unwrap();
        let uri = format!(
            "/blobstorage?blob=root&token={}",
            urlencoding::encode(&token)
        );
        let req = Request::put(&uri)
            .header("x-goog-hash", goog(hash.as_bytes()))
            .body(Body::from(hash.clone()))
            .unwrap();
        let resp = crate::create_router(state.clone())
            .oneshot(req)
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["x-goog-generation"], "1");
        assert_eq!(state.storage.get_root().hash, hash);
        // An oversized root body is the same 400 an unparseable one always was.
        let req = Request::put(&uri)
            .body(Body::from(vec![b'a'; 8192]))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::BAD_REQUEST);
        assert_eq!(state.storage.get_root().hash, hash);
        clean(&state);
    }

    #[tokio::test]
    async fn gentree_put_file_decodes_to_disk() {
        let (state, auth, _tmp) = setup();
        let d = data(BIG, 4);
        let hash = sha(&d);
        let body = serde_json::json!({
            "fileHash": hash, "filePath": "doc/big.pdf", "sizeBytes": BIG,
            "payload": STANDARD.encode(&d),
        });
        let req = Request::post("/gentree/v1/PutFile")
            .header("authorization", &auth)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let (status, resp) = send(&state, req).await;
        assert_eq!(status, StatusCode::OK, "{resp:?}");
        let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(resp["sizeBytes"], BIG as u64);
        assert_eq!(resp["state"], "stored");
        assert!(state.storage.get(&hash).unwrap() == d);
        assert_eq!(
            state.storage.filename_for_hash(&hash).as_deref(),
            Some("doc/big.pdf")
        );
        // Bad base64: the same 400 as before, nothing stored.
        let other = "c".repeat(64);
        let req = Request::post("/gentree/v1/PutFile")
            .header("authorization", &auth)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"fileHash": other, "payload": "QQ==QUJD"}).to_string(),
            ))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::BAD_REQUEST);
        assert!(!state.storage.exists(&other));
        clean(&state);
    }

    #[tokio::test]
    async fn v2_v4_put_stream_with_the_old_2mib_cap() {
        let (state, auth, _tmp) = setup();
        for v in ["v2", "v4"] {
            let d = data(DEFAULT_LIMIT as usize, 5);
            let hash = sha(&d);
            let req = Request::put(format!("/sync/{v}/files/{hash}"))
                .header("authorization", &auth)
                .header("rm-filename", format!("{v}.rm"))
                .body(chunked(&d))
                .unwrap();
            assert_eq!(send(&state, req).await.0, StatusCode::OK, "{v}");
            assert!(state.storage.get(&hash).unwrap() == d);
            assert_eq!(
                state.storage.filename_for_hash(&hash).as_deref(),
                Some(format!("{v}.rm").as_str())
            );
            // One byte over axum's default limit: 413, as with the `Bytes` extractor.
            let d = data(DEFAULT_LIMIT as usize + 1, 6);
            let hash = sha(&d);
            let req = Request::put(format!("/sync/{v}/files/{hash}"))
                .header("authorization", &auth)
                .body(chunked(&d))
                .unwrap();
            assert_eq!(
                send(&state, req).await.0,
                StatusCode::PAYLOAD_TOO_LARGE,
                "{v}"
            );
            assert!(!state.storage.exists(&hash));
            clean(&state);
        }
    }

    /// The stored `<id>.pdf` leaf of the only document in the root.
    fn stored_pdf(state: &AppState, d: &[u8]) -> Vec<u8> {
        let hash = sha(d);
        let name = state.storage.filename_for_hash(&hash).expect("leaf stored");
        assert!(name.ends_with(".pdf"), "{name}");
        state.storage.get(&hash).unwrap()
    }

    #[tokio::test]
    async fn doc_v2_upload_streams() {
        let (state, auth, _tmp) = setup();
        let mut d = b"%PDF-1.4\n".to_vec();
        d.extend(data(BIG, 7));
        let meta = STANDARD.encode(br#"{"file_name":"Big"}"#);
        let req = Request::post("/doc/v2/files")
            .header("authorization", &auth)
            .header("rm-meta", meta)
            .header("content-type", "application/pdf")
            .body(chunked(&d))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::OK);
        assert!(stored_pdf(&state, &d) == d);
        assert_eq!(state.storage.get_root().generation, 1);
        clean(&state);
    }

    #[tokio::test]
    async fn doc_v1_multipart_upload_streams() {
        let (state, auth, _tmp) = setup();
        let mut d = b"%PDF-1.4\n".to_vec();
        d.extend(data(BIG, 8));
        let b = "XyZboundary";
        let mut body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"meta\"\r\n\r\n{{\"file_name\":\"Big\"}}\r\n--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"big.pdf\"\r\nContent-Type: application/pdf\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(&d);
        body.extend_from_slice(format!("\r\n--{b}--\r\n").as_bytes());
        let req = Request::post("/doc/v1/files")
            .header("authorization", &auth)
            .header("content-type", format!("multipart/form-data; boundary={b}"))
            .body(chunked(&body))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::OK);
        assert!(stored_pdf(&state, &d) == d);
        clean(&state);

        // Unsupported type: refused, the staged file doesn't linger.
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"meta\"\r\n\r\n{{\"file_name\":\"x\"}}\r\n--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x.txt\"\r\nContent-Type: text/plain\r\n\r\nhello\r\n--{b}--\r\n"
        );
        let req = Request::post("/doc/v1/files")
            .header("authorization", &auth)
            .header("content-type", format!("multipart/form-data; boundary={b}"))
            .body(Body::from(body))
            .unwrap();
        assert_eq!(send(&state, req).await.0, StatusCode::BAD_REQUEST);
        assert_eq!(state.storage.get_root().generation, 1);
        clean(&state);
    }

    #[tokio::test]
    async fn share_link_page_streams() {
        let (state, auth, _tmp) = setup();
        let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
        png.extend(data(BIG, 9));
        let b = "pngboundary";
        let mut body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n{{\"DocID\":\"d\"}}\r\n--{b}\r\nContent-Disposition: form-data; name=\"page\"; filename=\"p.png\"\r\nContent-Type: image/png\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(&png);
        body.extend_from_slice(format!("\r\n--{b}--\r\n").as_bytes());
        let req = Request::post("/share/v1/link")
            .header("authorization", &auth)
            .header("content-type", format!("multipart/form-data; boundary={b}"))
            .body(chunked(&body))
            .unwrap();
        let (status, resp) = send(&state, req).await;
        assert_eq!(status, StatusCode::OK);
        let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        let link = resp["link"].as_str().unwrap();
        let name = link.rsplit('/').next().unwrap();
        let (status, got) = send(
            &state,
            Request::get(format!("/share/v1/link/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(got[..] == png[..]);
        clean(&state);
    }
}
