//! Integration tests for the sync server

use remarkable_server::{create_router, AppState, DeviceManager, Storage};
use axum::http::{Request, StatusCode};
use axum::body::Body;
use tower::ServiceExt;
use tempfile::TempDir;

/// Helper to create test app
fn test_app() -> (axum::Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::new(tmp.path()).unwrap();
    let db_path = tmp.path().join("devices.db");
    let devices = DeviceManager::new(&db_path, "local", "test").unwrap();
    let state = AppState::new(storage, devices);
    let router = create_router(state);
    (router, tmp)
}

#[tokio::test]
async fn test_health_endpoint() {
    let (app, _tmp) = test_app();
    
    let response = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(json["status"], "ok");
    assert_eq!(json["storage"]["file_count"], 0);
}

#[tokio::test]
async fn test_get_root_empty() {
    let (app, _tmp) = test_app();
    
    let response = app
        .oneshot(Request::builder().uri("/sync/v3/root").body(Body::empty()).unwrap())
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(json["hash"], "");
    assert_eq!(json["generation"], 0);
    assert_eq!(json["schemaVersion"], 3);
}

#[tokio::test]
async fn test_put_and_get_file() {
    let (app, _tmp) = test_app();
    
    let data = b"hello world";
    let hash = sha2_hash(data);
    let upload_uri = format!("/sync/v3/files/{}", hash);
    
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&upload_uri)
                .header("rm-filename", "test.txt")
                .header("content-type", "application/octet-stream")
                .body(Body::from(data.to_vec()))
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let response = app
        .oneshot(
            Request::builder()
                .uri(&upload_uri)
                .header("rm-filename", "test.txt")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-goog-hash"));
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), data);
}

#[tokio::test]
async fn test_get_file_missing_header() {
    let (app, _tmp) = test_app();
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/sync/v3/files/abc123")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_file_not_found() {
    let (app, _tmp) = test_app();
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/sync/v3/files/nonexistent")
                .header("rm-filename", "test.txt")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_checksum_validation() {
    let (app, _tmp) = test_app();
    
    let data = b"test data";
    let hash = sha2_hash(data);
    let correct_checksum = format!("crc32c={}", base64_crc32c(data));
    
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/sync/v3/files/{}", hash))
                .header("rm-filename", "test.bin")
                .header("x-goog-hash", &correct_checksum)
                .body(Body::from(data.to_vec()))
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let wrong_hash = sha2_hash(b"wrong");
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/sync/v3/files/{}", wrong_hash))
                .header("rm-filename", "bad.bin")
                .header("x-goog-hash", "crc32c=AAAA")
                .body(Body::from(b"wrong".to_vec()))
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_token_refresh() {
    let (app, _tmp) = test_app();
    
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/token/json/2/user/new")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    
    // With invalid token, should return unauthorized
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_discovery() {
    let (app, _tmp) = test_app();
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/discovery/v1/endpoints")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    
    assert_eq!(json["Status"], "OK");
}

#[tokio::test]
async fn test_list_files() {
    let (app, _tmp) = test_app();
    
    let response = app.clone()
        .oneshot(Request::builder().uri("/debug/files").body(Body::empty()).unwrap())
        .await
        .unwrap();
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let files: Vec<String> = serde_json::from_slice(&body).unwrap();
    assert!(files.is_empty());
    
    let data = b"content";
    let hash = sha2_hash(data);
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/sync/v3/files/{}", hash))
                .header("rm-filename", "doc.txt")
                .body(Body::from(data.to_vec()))
                .unwrap()
        )
        .await
        .unwrap();
    
    let response = app
        .oneshot(Request::builder().uri("/debug/files").body(Body::empty()).unwrap())
        .await
        .unwrap();
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let files: Vec<String> = serde_json::from_slice(&body).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0], hash);
}

#[tokio::test]
async fn test_clear_storage() {
    let (app, _tmp) = test_app();
    
    let data = b"to be cleared";
    let hash = sha2_hash(data);
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/sync/v3/files/{}", hash))
                .header("rm-filename", "temp.txt")
                .body(Body::from(data.to_vec()))
                .unwrap()
        )
        .await
        .unwrap();
    
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/debug/clear")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    
    let response = app
        .oneshot(Request::builder().uri("/debug/files").body(Body::empty()).unwrap())
        .await
        .unwrap();
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let files: Vec<String> = serde_json::from_slice(&body).unwrap();
    assert!(files.is_empty());
}

fn sha2_hash(data: &[u8]) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn base64_crc32c(data: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let crc = crc32c::crc32c(data);
    STANDARD.encode(crc.to_be_bytes())
}
