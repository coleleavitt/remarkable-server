//! Sync 1.5 ("signed URL") protocol, used by current xochitl firmware.
//!
//! The device asks for a signed URL per blob (`/sync/v2/signed-urls/{downloads,uploads}`),
//! then GETs/PUTs the blob at that URL. The special blob `root` holds the root hash as text
//! and is versioned with GCS-style generations (`x-goog-generation`,
//! `x-goog-if-generation-match`) so concurrent writers get a 412 instead of a lost update.
//! Mirrors rmfakecloud's `blobStorageDownload`/`blobStorageUpload`/`/blobstorage`.

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::checksum;
use crate::error::{Result, ServerError};
use crate::notifications::WsMessage;
use crate::storage::is_valid_hash;

const ROOT_BLOB: &str = "root";
const GENERATION_HEADER: &str = "x-goog-generation";
const GENERATION_MATCH_HEADER: &str = "x-goog-if-generation-match";
/// Most a root blob upload may be: a 64-char hash plus whatever whitespace a client adds.
const ROOT_BODY_LIMIT: usize = 4096;

#[derive(Deserialize)]
pub struct SignedUrlRequest {
    relative_path: String,
    #[serde(default)]
    initial_sync: bool,
}

#[derive(Serialize)]
pub struct SignedUrlResponse {
    expires: String,
    method: &'static str,
    relative_path: String,
    url: String,
}

#[derive(Deserialize)]
pub struct BlobQuery {
    blob: String,
    token: String,
}

#[derive(Deserialize)]
pub struct SyncCompleteRequest {
    #[serde(default)]
    generation: u64,
}

#[derive(Serialize)]
pub struct SyncCompleteResponse {
    id: String,
}

fn check_blob_id(blob: &str) -> Result<()> {
    if blob == ROOT_BLOB || is_valid_hash(blob) {
        return Ok(());
    }
    tracing::warn!(blob, "rejected blob id");
    Err(ServerError::InvalidHash(blob.to_string()))
}

/// Parse a signed-URL request, logging the raw body if it doesn't match what we expect.
fn parse_signed_request(body: &Bytes) -> Result<SignedUrlRequest> {
    serde_json::from_slice(body).map_err(|e| {
        tracing::warn!(error = %e, body = %String::from_utf8_lossy(body), "bad signed-url request");
        ServerError::Json(e)
    })
}

fn signed_url(
    state: &AppState,
    headers: &HeaderMap,
    req: SignedUrlRequest,
    write: bool,
) -> Result<Json<SignedUrlResponse>> {
    state.auth_user(headers)?;
    check_blob_id(&req.relative_path)?;
    if write && req.initial_sync {
        tracing::info!("initial sync upload");
    }
    let (token, exp) = state.devices.sign_blob(&req.relative_path, write)?;
    let url = format!(
        "https://{}/blobstorage?blob={}&token={}",
        state.devices.get_endpoint(),
        urlencoding::encode(&req.relative_path),
        urlencoding::encode(&token),
    );
    Ok(Json(SignedUrlResponse {
        expires: exp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        method: if write { "PUT" } else { "GET" },
        relative_path: req.relative_path,
        url,
    }))
}

pub async fn signed_download(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<SignedUrlResponse>> {
    signed_url(&state, &headers, parse_signed_request(&body)?, false)
}

pub async fn signed_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<SignedUrlResponse>> {
    signed_url(&state, &headers, parse_signed_request(&body)?, true)
}

pub async fn blob_get(
    State(state): State<AppState>,
    Query(q): Query<BlobQuery>,
) -> Result<Response> {
    state.devices.verify_blob(&q.token, &q.blob, false)?;
    check_blob_id(&q.blob)?;

    let (data, generation) = if q.blob == ROOT_BLOB {
        let root = state.storage.get_root();
        if root.hash.is_empty() {
            return Err(ServerError::NotFound(ROOT_BLOB.into()));
        }
        (root.hash.into_bytes(), Some(root.generation))
    } else {
        (state.storage.get(&q.blob)?, None)
    };

    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        [(
            header::HeaderName::from_static("x-goog-hash"),
            checksum::format_goog_hash(&data),
        )],
        data,
    )
        .into_response();
    if let Some(generation) = generation {
        resp.headers_mut()
            .insert(GENERATION_HEADER, HeaderValue::from(generation));
    }
    Ok(resp)
}

/// `x-goog-if-generation-match` as GCS reads it: absent = unconditional write (the
/// tablet's first root upload may omit it), present = must be a u64 or the request is
/// rejected. A garbled header must never degrade into an unguarded root overwrite.
fn generation_precondition(headers: &HeaderMap) -> Result<Option<u64>> {
    let Some(raw) = headers.get(GENERATION_MATCH_HEADER) else {
        return Ok(None);
    };
    match raw.to_str().ok().and_then(|v| v.trim().parse::<u64>().ok()) {
        Some(generation) => Ok(Some(generation)),
        None => {
            tracing::warn!(header = ?raw, "rejected malformed {GENERATION_MATCH_HEADER}");
            Err(ServerError::InvalidHeader(format!(
                "{GENERATION_MATCH_HEADER}: {raw:?}"
            )))
        }
    }
}

pub async fn blob_put(
    State(state): State<AppState>,
    Query(q): Query<BlobQuery>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    state.devices.verify_blob(&q.token, &q.blob, true)?;
    check_blob_id(&q.blob)?;

    // Parsed before the body is read, so a malformed header is refused up front.
    let expected = checksum::GoogHash::from_headers(&headers)?;

    if q.blob != ROOT_BLOB {
        let staged = crate::upload::stage_body(
            &state.storage,
            &headers,
            body,
            crate::MAX_BLOB_BYTES as u64,
            false,
        )
        .await?;
        if let Some(expected) = &expected {
            expected.verify(staged.crc32c())?;
        }
        staged.commit(&state.storage, &q.blob, &q.blob)?;
        return Ok(Json(serde_json::json!({})).into_response());
    }

    // The root blob is just a hash as text: buffer it, but never more than a small cap.
    // Anything longer can't be a valid hash, so it gets the same 400 it always did.
    let body = axum::body::to_bytes(body, ROOT_BODY_LIMIT)
        .await
        .map_err(|_| ServerError::InvalidHash("root body too large or unreadable".into()))?;
    if let Some(expected) = &expected {
        expected.verify(checksum::crc32c(&body))?;
    }

    let hash = std::str::from_utf8(&body)
        .map(str::trim)
        .unwrap_or_default();
    if !is_valid_hash(hash) {
        return Err(ServerError::InvalidHash(hash.to_string()));
    }
    let expected = generation_precondition(&headers)?;
    let root = state.storage.set_root_if(hash.to_string(), expected)?;
    tracing::info!(generation = root.generation, hash = %root.hash, "root updated");

    let mut resp = Json(serde_json::json!({})).into_response();
    resp.headers_mut()
        .insert(GENERATION_HEADER, HeaderValue::from(root.generation));
    Ok(resp)
}

pub async fn sync_complete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SyncCompleteRequest>,
) -> Result<Json<SyncCompleteResponse>> {
    let (user_id, device_id, _) = state.devices.caller(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
    )?;
    tracing::info!(generation = req.generation, device = %device_id, "sync complete");
    // Attributed to the pushing device so it skips its own notification (xochitl 3.28 C.2).
    let msg = WsMessage::sync_complete(req.generation, &device_id, &user_id);
    let id = uuid::Uuid::new_v4().to_string();
    // No subscribers is fine; the device just isn't listening right now.
    let _ = state.notification_tx.send(msg);
    Ok(Json(SyncCompleteResponse { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    fn setup() -> (AppState, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        (AppState::new(storage, devices), tmp)
    }

    async fn put(
        state: &AppState,
        blob: &str,
        hdrs: &[(&'static str, &'static str)],
        body: &'static [u8],
    ) -> Result<Response> {
        let (token, _) = state.devices.sign_blob(blob, true).unwrap();
        let mut h = HeaderMap::new();
        for (k, v) in hdrs {
            h.insert(*k, HeaderValue::from_static(v));
        }
        blob_put(
            State(state.clone()),
            Query(BlobQuery {
                blob: blob.into(),
                token,
            }),
            h,
            Body::from(body),
        )
        .await
    }

    const H1: &[u8] = b"1111111111111111111111111111111111111111111111111111111111111111";
    const H2: &[u8] = b"2222222222222222222222222222222222222222222222222222222222222222";

    #[tokio::test]
    async fn root_generation_header_semantics() {
        let (state, _tmp) = setup();
        // Absent header: unconditional (GCS semantics).
        put(&state, ROOT_BLOB, &[], H1).await.unwrap();
        assert_eq!(state.storage.get_root().generation, 1);
        // Stale generation: 412, root untouched.
        let err = put(&state, ROOT_BLOB, &[(GENERATION_MATCH_HEADER, "0")], H2)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::GenerationMismatch { current: 1 }
        ));
        // Present but malformed: 400, never an unguarded overwrite.
        for bad in ["", "abc", "-1", "1.0", "1,2"] {
            let err = put(&state, ROOT_BLOB, &[(GENERATION_MATCH_HEADER, bad)], H2)
                .await
                .unwrap_err();
            assert!(matches!(err, ServerError::InvalidHeader(_)), "{bad:?}");
            assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
        }
        let root = state.storage.get_root();
        assert_eq!((root.hash.as_bytes(), root.generation), (H1, 1));
        // Matching generation: accepted.
        let resp = put(&state, ROOT_BLOB, &[(GENERATION_MATCH_HEADER, "1")], H2)
            .await
            .unwrap();
        assert_eq!(resp.headers()[GENERATION_HEADER], "2");
        assert_eq!(state.storage.get_root().hash.as_bytes(), H2);
    }

    #[tokio::test]
    async fn blob_goog_hash_validation() {
        let (state, _tmp) = setup();
        let blob = "a".repeat(64);
        // crc32c("123456789") = 4waSgw==; md5 alongside is fine.
        put(
            &state,
            &blob,
            &[(
                "x-goog-hash",
                "crc32c=4waSgw==,md5=JfnnlDI7RTiF9RgfG2JNCw==",
            )],
            b"123456789",
        )
        .await
        .unwrap();
        put(&state, &blob, &[], b"123456789").await.unwrap();
        let err = put(
            &state,
            &blob,
            &[("x-goog-hash", "crc32c=4waSgw==")],
            b"12345678X",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServerError::ChecksumMismatch { .. }));
        for bad in ["crc32c=nope", "garbage", "md5=JfnnlDI7RTiF9RgfG2JNCw=="] {
            let err = put(&state, &blob, &[("x-goog-hash", bad)], b"123456789")
                .await
                .unwrap_err();
            assert!(matches!(err, ServerError::InvalidHeader(_)), "{bad:?}");
        }
        // Root puts are checked too, before any root change.
        let err = put(&state, ROOT_BLOB, &[("x-goog-hash", "crc32c=nope")], H1)
            .await
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidHeader(_)));
        assert!(state.storage.get_root().hash.is_empty());
    }
}
