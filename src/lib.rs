pub mod api;
pub mod calendar;
pub mod calendar_api;
pub mod checksum;
pub mod crash;
pub mod device;
pub mod documents;
pub mod email;
pub mod email_api;
pub mod error;
pub mod feeds;
pub mod firmware;
pub mod gentree;
pub mod handwriting;
pub mod hw_search;
pub mod integrations;
pub mod mdm;
pub mod mqtt_ws;
pub mod notifications;
pub mod oauth;
pub mod passcode;
pub mod protocol;
pub mod readlater;
pub mod readlater_api;
pub mod reports;
pub mod screenshare;
pub mod screenshare_rest;
pub mod screenshare_viewer;
pub mod search;
pub mod search_api;
pub mod service;
pub mod share_email;
pub mod share_link;
pub mod storage;
pub mod sync15;
pub mod types;
pub mod upload;
pub mod versions;

use std::path::Path;

pub use api::AppState;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post, put};
pub use calendar::{Calendar, CalendarConfig, CalendarManager, CalendarProvider};
pub use calendar_api::CalendarState;
pub use device::DeviceManager;
pub use error::{Result, ServerError};
pub use integrations::{
    CloudProvider,
    ConflictResolution,
    ConflictResolver,
    ConflictStrategy,
    IntegrationManager,
    IntegrationState,
    OAuthConfig,
    OAuthToken,
    PkceFlow,
    ProviderType,
    SyncConfig,
    SyncDirection,
    SyncResult,
    SyncStatus,
    integration_router,
};
pub use readlater::{
    Article,
    ArticleFormat,
    ProviderAccount,
    ProviderConfig,
    ReadLaterManager,
    ReadLaterProvider,
    ReadStatus,
    SyncResult as ReadLaterSyncResult,
    SyncSettings,
};
pub use readlater_api::{ReadLaterState, readlater_router};
pub use storage::Storage;
use tower_http::trace::TraceLayer;

/// Max upload body for blob routes. Axum's 2 MB default rejects PDFs/EPUBs and large
/// notebook pages with 413, which the device reports as "Failed uploading".
pub(crate) const MAX_BLOB_BYTES: usize = 1024 * 1024 * 1024;

/// Bind a TCP listener, waiting while the address doesn't exist yet (e.g. the tablet's
/// USB network is down because it's asleep or unplugged) instead of failing startup.
pub async fn bind_when_available(
    addr: std::net::SocketAddr,
) -> std::io::Result<tokio::net::TcpListener> {
    let mut warned = false;
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Err(e) if e.kind() == std::io::ErrorKind::AddrNotAvailable => {
                if !warned {
                    tracing::warn!(%addr, "address not present yet (tablet asleep/unplugged?); waiting for it");
                    warned = true;
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            other => return other,
        }
    }
}

/// How often the feed scheduler looks for subscriptions due a refresh (each has its own interval).
const FEED_CHECK_SECS: u64 = 300;

pub fn create_router(state: AppState) -> Router {
    Router::new()
        // V1 Protocol (document-storage JSON API - firmware 1.x-2.x)
        .route("/document-storage/json/2/docs", get(protocol::v1_list_docs))
        .route(
            "/document-storage/json/2/upload/request",
            put(protocol::v1_upload_request),
        )
        .route(
            "/document-storage/json/2/upload/update-status",
            put(protocol::v1_update_status),
        )
        .route("/document-storage/json/2/delete", put(protocol::v1_delete))
        // V1.5 Protocol (batch operations)
        .route("/sync/v1.5/batch", post(protocol::v15_batch_sync))
        // V2 Protocol (binary with metadata)
        .route("/sync/v2/root", get(protocol::v2_get_root))
        .route("/sync/v2/files/{hash}", get(protocol::v2_get_file))
        .route("/sync/v2/files/{hash}", put(protocol::v2_put_file))
        // Sync 1.5 (signed URLs - current xochitl firmware)
        .route(
            "/sync/v2/signed-urls/downloads",
            post(sync15::signed_download),
        )
        .route("/sync/v2/signed-urls/uploads", post(sync15::signed_upload))
        .route("/sync/v2/sync-complete", post(sync15::sync_complete))
        .route(
            "/api/v1/signed-urls/downloads",
            post(sync15::signed_download),
        )
        .route("/api/v1/signed-urls/uploads", post(sync15::signed_upload))
        .route("/api/v1/sync-complete", post(sync15::sync_complete))
        // Handwriting conversion (local tesseract; see handwriting.rs)
        .route(
            "/convert/v1/handwriting",
            post(handwriting::convert).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/handwriting/v1/search", get(hw_search::search))
        .route(
            "/api/v1/page",
            post(handwriting::convert).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        // Share a page as a link (3.27+)
        .route(
            "/share/v1/link",
            post(share_link::create).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/share/v1/link/{name}", get(share_link::get))
        // Send by email (SMTP via env)
        .route(
            "/share/v1/email",
            post(share_email::send).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        // Read on reMarkable / desktop uploads (PDF, EPUB)
        .route(
            "/doc/v1/files",
            post(documents::upload_v1).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route(
            "/doc/v2/files",
            post(documents::upload_v2).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route(
            "/blobstorage",
            get(sync15::blob_get)
                .put(sync15::blob_put)
                .layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        // V3 Protocol (hash-based CRDT - current production)
        .route("/sync/v3/root", get(api::get_root).put(api::put_root))
        .route("/sync/v3/check-files", post(api::check_files))
        .route("/sync/v3/missing", get(api::missing_blobs))
        .route("/sync/v3/files-list", get(api::files_list))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route(
            "/sync/v3/files/{hash}",
            put(api::put_file).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        // V4 Protocol (extended metadata - future)
        // gentree/v1 delta sync (rm-sync, software 3.28+); /sync/v3/{missing,check-files} are reused above.
        .route("/gentree/v1/GetEntries", post(gentree::get_entries))
        .route("/gentree/v1/GetFiles", post(gentree::get_files))
        .route("/gentree/v1/GetFile", post(gentree::get_file))
        .route(
            "/gentree/v1/PutFile",
            post(gentree::put_file).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/gentree/v1/DeleteEntry", post(gentree::delete_entry))
        .route("/gentree/v1/EntrySession", post(gentree::entry_session))
        .route("/sync/v4/root", get(protocol::v4_get_root))
        .route("/sync/v4/files/{hash}", get(protocol::v4_get_file))
        .route("/sync/v4/files/{hash}", put(protocol::v4_put_file))
        // Device management
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route(
            "/token/json/2/device/delete",
            post(api::delete_device_token),
        )
        .route(
            "/token/json/3/device/delete",
            post(api::delete_device_token),
        )
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/discovery/v1/webapp", get(service::discovery_webapp))
        .route("/service/json/1/{service}", get(api::service_locator))
        .route("/admin/create-user", post(api::create_test_user))
        .route(
            "/admin/passcode/resets/{uuid}/approve",
            post(passcode::approve),
        )
        .route(
            "/passcode/v1/resets/{uuid}",
            post(passcode::create).get(passcode::get),
        )
        .route(
            "/passcode/v1/reset/{uuid}/approve",
            post(passcode::device_approve),
        )
        .route(
            "/passcode/v1/reset/{uuid}/deny",
            post(passcode::device_deny),
        )
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        // Sync push notifications (JSON over WebSocket; what the tablet uses). The MQTT-over-
        // WebSocket variant (`/mqtt`) is opt-in: see `mqtt_ws::router_if_enabled`.
        .route(
            "/notifications/ws/json/1",
            get(notifications::notifications_ws),
        )
        // Screenshare REST room broker (xochitl 3.27+/3.28)
        .route("/screenshare/v1/rooms", post(screenshare_rest::create_room))
        .route(
            "/screenshare/v1/rooms/join-active",
            post(screenshare_rest::join_active),
        )
        .route(
            "/screenshare/v1/rooms/{roomId}",
            get(screenshare_rest::get_room).delete(screenshare_rest::delete_room),
        )
        .route(
            "/screenshare/v1/rooms/{roomId}/join",
            post(screenshare_rest::join_room),
        )
        .route(
            "/screenshare/v1/rooms/{roomId}/keepalive",
            post(screenshare_rest::keepalive),
        )
        .route(
            "/screenshare/v1/rooms/{roomId}/messages/broadcast",
            post(screenshare_rest::broadcast),
        )
        .route(
            "/screenshare/v1/rooms/{roomId}/messages/direct",
            post(screenshare_rest::direct),
        )
        // OAuth2 device-flow login + legacy migration (software 3.28)
        .route("/oauth/device/code", post(oauth::device_code))
        .route("/oauth/token", post(oauth::token))
        .route("/oauth/revoke", post(oauth::revoke))
        .route("/oauth/verify", get(oauth::verify_page).post(oauth::verify))
        .route("/admin/oauth/approve", post(oauth::admin_approve))
        .route(
            "/token/json/4/device/exchange",
            post(oauth::device_exchange),
        )
        // Settings and updates
        .route(
            "/settings/v1/beta",
            get(service::get_beta)
                .post(service::post_beta)
                .delete(service::delete_beta),
        )
        // Search index settings / client error reports (3.27+)
        .route(
            "/search/v1/settings",
            get(service::get_search_settings).patch(service::patch_search_settings),
        )
        .route("/search/v1/error", post(service::search_error))
        // mdm-agent polling: nothing to do
        // MDM instruction queue (enterprise device management) + crash-report sink
        .route("/mdm/v1/instruction", get(mdm::get_instruction))
        .route("/mdm/devices/v0/instruction", get(mdm::get_instruction))
        .route("/mdm/v1/instruction/status", post(mdm::post_status))
        .route("/admin/mdm/enqueue", post(mdm::admin_enqueue))
        .route("/admin/mdm/instructions", get(mdm::admin_list))
        .route(
            "/post",
            post(crash::upload).layer(DefaultBodyLimit::max(crash::MAX_BODY)),
        )
        .route("/settings/v1/features", get(service::get_beta))
        // Telemetry / analytics (ping.remarkable.com), kept in reports.jsonl.
        // Bounded to what we would store (Axum's 2 MiB default otherwise applies).
        .route(
            "/v1/reports",
            post(reports::store).layer(DefaultBodyLimit::max(reports::MAX_BODY)),
        )
        .route(
            "/v2/reports",
            post(reports::store).layer(DefaultBodyLimit::max(reports::MAX_BODY)),
        )
        .route(
            "/report/v1",
            post(reports::store).layer(DefaultBodyLimit::max(reports::MAX_BODY)),
        )
        .route(
            "/v2/events",
            post(reports::store).layer(DefaultBodyLimit::max(reports::MAX_BODY)),
        )
        .route(
            "/sync/reports/v1",
            post(reports::store).layer(DefaultBodyLimit::max(reports::MAX_BODY)),
        )
        .route(
            "/analytics/v2/events",
            post(reports::store_analytics).layer(DefaultBodyLimit::max(reports::MAX_BODY)),
        )
        .route("/admin/reports", get(reports::list))
        .route("/admin/storage/unreachable", get(api::unreachable_blobs))
        .route("/admin/storage/gc", post(api::storage_gc))
        // Third-party integrations (none configured)
        .route("/integrations/v1/", get(service::list_integrations))
        .route(
            "/integrations/v2/instances",
            get(service::list_integrations),
        )
        .route(
            "/integrations/v2/messaging/{instance_id}/message",
            post(service::send_integration_message),
        )
        .route("/updates/v1/check", get(api::check_updates))
        .route("/updates/check", get(api::check_updates))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

fn calendar_router(state: CalendarState) -> Router {
    Router::new()
        .route("/", get(calendar_api::list_calendars))
        .route("/", post(calendar_api::add_calendar))
        .route("/upcoming", get(calendar_api::get_upcoming_events))
        .route("/sync-all", post(calendar_api::sync_all_calendars))
        .route("/{id}", get(calendar_api::get_calendar))
        .route("/{id}", delete(calendar_api::delete_calendar))
        .route("/{id}/events", get(calendar_api::get_events))
        .route("/{id}/sync", post(calendar_api::sync_calendar_endpoint))
        .route("/{id}/meeting-notes", get(calendar_api::list_meeting_notes))
        .route(
            "/{id}/events/{event_id}/meeting-notes",
            post(calendar_api::create_meeting_note),
        )
        .route("/webhook", post(calendar_api::calendar_webhook))
        .with_state(state)
}

pub fn create_router_with_all(
    state: AppState,
    calendar_state: CalendarState,
    readlater_state: ReadLaterState,
) -> Router {
    let sync_router = Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route(
            "/sync/v3/files/{hash}",
            put(api::put_file).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route(
            "/token/json/3/device/delete",
            post(api::delete_device_token),
        )
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/{service}", get(api::service_locator))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state);

    sync_router
        .nest(
            "/integrations/v2/calendars",
            calendar_router(calendar_state),
        )
        .nest(
            "/integrations/v2/readlater",
            readlater_router(readlater_state),
        )
        .layer(TraceLayer::new_for_http())
}

pub fn create_router_with_calendar(state: AppState, calendar_state: CalendarState) -> Router {
    let sync_router = Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route(
            "/sync/v3/files/{hash}",
            put(api::put_file).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route(
            "/token/json/3/device/delete",
            post(api::delete_device_token),
        )
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/{service}", get(api::service_locator))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state);

    sync_router
        .nest(
            "/integrations/v2/calendars",
            calendar_router(calendar_state),
        )
        .layer(TraceLayer::new_for_http())
}

/// Create router with cloud storage integrations
pub fn create_router_with_integrations(
    state: AppState,
    integration_state: IntegrationState,
) -> Router {
    Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route(
            "/sync/v3/files/{hash}",
            put(api::put_file).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route(
            "/token/json/3/device/delete",
            post(api::delete_device_token),
        )
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/{service}", get(api::service_locator))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state)
        .nest(
            "/integrations/v2/cloud",
            integration_router(integration_state.clone()),
        )
        .nest(
            "/integrations/v2/storage",
            integration_router(integration_state),
        )
        .layer(TraceLayer::new_for_http())
}

/// Create full router with calendar and cloud integrations
pub fn create_full_router(
    state: AppState,
    calendar_state: CalendarState,
    integration_state: IntegrationState,
) -> Router {
    Router::new()
        .route("/sync/v3/root", get(api::get_root))
        .route("/sync/v3/files/{hash}", get(api::get_file))
        .route(
            "/sync/v3/files/{hash}",
            put(api::put_file).layer(DefaultBodyLimit::max(MAX_BLOB_BYTES)),
        )
        .route("/devices/v1", post(api::create_pairing_code))
        .route("/devices/v1", get(api::list_devices))
        .route("/devices/v1/{id}", delete(api::delete_device))
        .route("/token/json/2/user/new", post(api::refresh_token))
        .route("/token/json/2/device/new", post(api::register_device))
        .route(
            "/token/json/3/device/delete",
            post(api::delete_device_token),
        )
        .route("/discovery/v1/endpoints", get(api::discovery))
        .route("/service/json/1/{service}", get(api::service_locator))
        .route("/admin/create-user", post(api::create_test_user))
        .route("/health", get(api::health))
        .route("/debug/files", get(api::list_files))
        .route("/debug/clear", delete(api::clear_storage))
        .with_state(state)
        .nest(
            "/integrations/v2/calendars",
            calendar_router(calendar_state),
        )
        .nest(
            "/integrations/v2/cloud",
            integration_router(integration_state.clone()),
        )
        .nest(
            "/integrations/v2/storage",
            integration_router(integration_state),
        )
        .layer(TraceLayer::new_for_http())
}

pub fn init_calendar_manager(storage_path: &Path) -> anyhow::Result<CalendarManager> {
    let db_path = storage_path.join("calendars.db");
    Ok(CalendarManager::new(&db_path)?)
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    /// Public hostname the device reaches us by (discovery + JWT issuer).
    pub host: String,
    pub storage_path: String,
    pub db_path: String,
    pub region: String,
    pub enable_calendar: bool,
    pub enable_cloud_integrations: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".into(),
            host: "local.tectonic.remarkable.com".into(),
            storage_path: "./remarkable-storage".into(),
            db_path: "./remarkable-storage/devices.db".into(),
            region: "local".into(),
            enable_calendar: true,
            enable_cloud_integrations: true,
            cert_path: None,
            key_path: None,
        }
    }
}

pub fn init_readlater_manager(storage_path: &Path) -> anyhow::Result<ReadLaterManager> {
    let db_path = storage_path.join("readlater.db");
    std::fs::create_dir_all(storage_path.join("articles"))?;
    Ok(ReadLaterManager::new(&db_path, storage_path)?)
}

/// Reject requests without a valid device/user token. Applied to every feature API
/// below, whose handlers don't authenticate on their own.
async fn require_auth(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> std::result::Result<axum::response::Response, ServerError> {
    state.auth_user(req.headers())?;
    Ok(next.run(req).await)
}

/// Optional feature APIs (search, versions, calendars, read-later, cloud integrations,
/// inbound email status), all behind token auth. `email` is the inbound mail server,
/// passed in only when it's enabled.
pub fn feature_routes(
    state: AppState,
    storage_path: &Path,
    email: Option<email::EmailServer>,
) -> anyhow::Result<Router> {
    let search = search_api::SearchState::new(
        search::SearchIndex::new(storage_path)?,
        state.storage.clone(),
    );
    let search_routes = Router::new()
        .route("/query", get(search_api::search))
        .route("/stats", get(search_api::stats))
        .route("/reindex", post(search_api::reindex))
        .route("/suggest", get(search_api::suggest))
        .with_state(search);

    let versions = versions::VersionState {
        manager: versions::VersionManager::new(
            storage_path.join("versions"),
            state.storage.clone(),
            versions::VersionConfig::default(),
        )?,
    };

    let feeds = std::sync::Arc::new(feeds::FeedManager::new(
        &storage_path.join("feeds.db"),
        state.storage.clone(),
        &storage_path.join("feeds-epub"),
    )?);
    // Periodic refresh of due subscriptions; only inside a Tokio runtime (not in unit tests).
    let scheduler = tokio::runtime::Handle::try_current()
        .is_ok()
        .then(|| feeds.clone().start_scheduler(FEED_CHECK_SECS));

    // Cloud syncs may only touch directories under <storage>/integrations.
    let cloud = IntegrationState::with_sync_base(storage_path.join("integrations"));
    let mut router = Router::new()
        .nest(
            "/feeds/v1",
            feeds::feeds_router(feeds::FeedState {
                manager: feeds,
                scheduler,
                notification_tx: state.notification_tx.clone(),
            }),
        )
        .nest("/search/v1", search_routes)
        .nest("/versions/v1", versions::version_router(versions))
        .nest(
            "/integrations/v2/calendars",
            calendar_router(CalendarState::new(init_calendar_manager(storage_path)?)),
        )
        .nest(
            "/integrations/v2/readlater",
            readlater_router(ReadLaterState::new(init_readlater_manager(storage_path)?)),
        )
        // xochitl 3.29 uses /storage/; older builds use /cloud. Share one state so both see the same accounts.
        .nest(
            "/integrations/v2/cloud",
            integrations::integration_api_router(cloud.clone()),
        )
        .nest(
            "/integrations/v2/storage",
            integrations::integration_api_router(cloud.clone()),
        );

    if let Some(server) = email {
        router = router.nest(
            "/email/v1",
            Router::new()
                .route("/emails", get(email_api::list_emails))
                .route("/stats", get(email_api::stats))
                .route("/config", get(email_api::config))
                .route("/health", get(email_api::health))
                .with_state(email_api::EmailState::new(server)),
        );
    }

    // Optional OTA archive (versions/changelogs/downloads). Enabled when FIRMWARE_ARCHIVE points at a directory.
    if let Ok(dir) = std::env::var("FIRMWARE_ARCHIVE") {
        let base = std::env::var("PUBLIC_URL").unwrap_or_else(|_| "http://localhost:3000".into());
        match firmware::FirmwareManager::new(&dir, &base) {
            Ok(m) => {
                router = router.nest(
                    "/firmware/v1",
                    firmware::firmware_router(firmware::FirmwareState::new(m)),
                )
            }
            Err(e) => tracing::warn!("firmware archive disabled ({dir}): {e}"),
        }
    }

    // The OAuth callback/success pages are hit by the user's browser (no device token), so they
    // are merged after the auth layer; the callback is authenticated by its one-time PKCE state.
    let oauth = Router::new()
        .nest(
            "/integrations/v2/cloud",
            integrations::integration_oauth_router(cloud.clone()),
        )
        .nest(
            "/integrations/v2/storage",
            integrations::integration_oauth_router(cloud),
        );
    Ok(router
        .layer(axum::middleware::from_fn_with_state(state, require_auth))
        .merge(oauth))
}

#[cfg(test)]
mod router_tests {
    use super::*;

    /// axum panics at router construction on bad route syntax (e.g. old `/:id` captures),
    /// which would crash the server at startup. Build every router here instead.
    #[test]
    fn all_routers_build() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let state = AppState::new(storage, devices);
        // Everything main.rs merges with `MQTT_WS_NOTIFICATIONS=1`, in its order: `merge`
        // panics on overlapping routes, so a `/mqtt` added elsewhere would only crash
        // deployments that enabled the flag.
        let _ = create_router(state.clone())
            .merge(feature_routes(state.clone(), tmp.path(), None).unwrap())
            .merge(mqtt_ws::router_if_enabled(state, Some("1")).expect("enabled"));
    }

    /// The API router with `/mqtt` enabled, as main.rs builds it when
    /// `MQTT_WS_NOTIFICATIONS=1`.
    fn with_mqtt_ws(state: &AppState) -> Router {
        create_router(state.clone())
            .merge(mqtt_ws::router_if_enabled(state.clone(), Some("1")).expect("enabled"))
    }

    /// `/mqtt` is off unless `MQTT_WS_NOTIFICATIONS` is `1`/`true`/`on`: no tablet uses it
    /// (GAP_ANALYSIS.md), so by default it must not be reachable, not even with a valid
    /// token, while `/notifications/ws/json/1` (what the tablet uses) still is.
    #[tokio::test]
    async fn mqtt_ws_route_is_off_by_default() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let token = devices.create_user_token("u1@test").unwrap();
        let state = AppState::new(storage, devices);
        for flag in [None, Some(""), Some("0"), Some("false"), Some("yes")] {
            assert!(
                mqtt_ws::router_if_enabled(state.clone(), flag).is_none(),
                "{flag:?}"
            );
        }
        for flag in ["1", "true", "on"] {
            assert!(
                mqtt_ws::router_if_enabled(state.clone(), Some(flag)).is_some(),
                "{flag}"
            );
        }
        // Built as main.rs builds it when `router_if_enabled` returns `None`.
        let app =
            create_router(state.clone()).merge(feature_routes(state, tmp.path(), None).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        let connect = |path: &str| {
            let mut req = format!("{base}{path}").into_client_request().unwrap();
            req.headers_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            tokio_tungstenite::connect_async(req)
        };
        match connect("/mqtt").await {
            Err(tokio_tungstenite::tungstenite::Error::Http(r)) => {
                assert_eq!(r.status(), axum::http::StatusCode::NOT_FOUND)
            }
            other => panic!("/mqtt must not be served, got {:?}", other.map(|_| ())),
        }
        assert!(connect("/notifications/ws/json/1").await.is_ok());
    }

    /// With `MQTT_WS_NOTIFICATIONS=1`, `/mqtt` is served, refuses clients without a valid
    /// token, and pushes an authenticated client its own user's SyncComplete but not
    /// another user's.
    #[tokio::test]
    async fn mqtt_ws_route_requires_auth_and_filters_by_user() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let token = devices.create_user_token("u1@test").unwrap();
        let state = AppState::new(Storage::new(tmp.path().join("storage")).unwrap(), devices);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/mqtt", listener.local_addr().unwrap());
        let router = with_mqtt_ws(&state);
        tokio::spawn(async move { axum::serve(listener, router).await });
        let connect = |auth: Option<&str>| {
            let mut req = url.as_str().into_client_request().unwrap();
            if let Some(a) = auth {
                req.headers_mut()
                    .insert("authorization", a.parse().unwrap());
            }
            tokio_tungstenite::connect_async(req)
        };
        async fn recv<
            S: futures_util::Stream<
                    Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>,
                > + Unpin,
        >(
            ws: &mut S,
        ) -> Option<Vec<u8>> {
            match tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
                .await
                .expect("timed out")
            {
                Some(Ok(Message::Binary(b))) => Some(b.to_vec()),
                _ => None,
            }
        }
        const CONNECT: [u8; 15] = [
            0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'c',
        ];

        match connect(Some("Bearer not-a-token")).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(r)) => assert_eq!(r.status(), 401),
            other => panic!("bad token must be refused, got {:?}", other.map(|_| ())),
        }
        let (mut anon, _) = connect(None).await.unwrap();
        anon.send(Message::Binary(CONNECT.to_vec().into()))
            .await
            .unwrap();
        assert_eq!(
            recv(&mut anon).await,
            Some(vec![0x20, 2, 0, 5]),
            "CONNACK not authorized"
        );
        let _ = state
            .notification_tx
            .send(notifications::WsMessage::sync_complete(1, "d", "u1@test"));
        assert_eq!(recv(&mut anon).await, None, "closed, nothing published");

        // A valid header token does not excuse a malformed CONNECT (here an empty body).
        let (mut bad, _) = connect(Some(&format!("Bearer {token}"))).await.unwrap();
        bad.send(Message::Binary(vec![0x10, 0].into()))
            .await
            .unwrap();
        assert_eq!(recv(&mut bad).await, None, "closed without a CONNACK");

        let (mut ws, _) = connect(Some(&format!("Bearer {token}"))).await.unwrap();
        ws.send(Message::Binary(CONNECT.to_vec().into()))
            .await
            .unwrap();
        assert_eq!(recv(&mut ws).await, Some(vec![0x20, 2, 0, 0]));
        ws.send(Message::Binary(vec![0x82, 6, 0, 1, 0, 1, b't', 0].into()))
            .await
            .unwrap(); // SUBSCRIBE "t"
        assert_eq!(recv(&mut ws).await.unwrap()[0], 0x90);
        assert_eq!(
            recv(&mut ws).await.unwrap()[0],
            0x30,
            "catch-up SyncComplete"
        );
        state
            .notification_tx
            .send(notifications::WsMessage::sync_complete(
                2,
                "d",
                "other@test",
            ))
            .unwrap();
        state
            .notification_tx
            .send(notifications::WsMessage::sync_complete(3, "d", "u1@test"))
            .unwrap();
        let publish = recv(&mut ws).await.unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&publish[publish.iter().position(|&b| b == b'{').unwrap()..])
                .unwrap(); // after topic "t"
        assert_eq!(
            body["message"]["attributes"]["auth0UserID"], "u1@test",
            "other user's SyncComplete was not forwarded"
        );
        assert_eq!(body["message"]["attributes"]["event"], "SyncComplete");
    }

    /// Open notification sessions (JSON WebSocket and, when enabled, `/mqtt`, token in the
    /// header or in the MQTT CONNECT) close when their device is deleted; another device's
    /// sessions stay open.
    #[tokio::test]
    async fn notification_sessions_close_when_their_device_is_revoked() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        type Ws = tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >;
        let tmp = tempfile::TempDir::new().unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        // A: user token from an OAuth bundle; B: its device token.
        let (a_user, ..) = devices
            .oauth_bundle("u1@test", "RM110-A", "remarkable")
            .unwrap();
        let (_, b_device, _) = devices
            .oauth_bundle("u1@test", "RM110-B", "remarkable")
            .unwrap();
        let state = AppState::new(Storage::new(tmp.path().join("storage")).unwrap(), devices);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("ws://{}", listener.local_addr().unwrap());
        let router = with_mqtt_ws(&state);
        tokio::spawn(async move { axum::serve(listener, router).await });
        let open = |path: &str, token: Option<&str>| {
            let mut req = format!("{base}{path}").into_client_request().unwrap();
            if let Some(t) = token {
                req.headers_mut()
                    .insert("authorization", format!("Bearer {t}").parse().unwrap());
            }
            async move { tokio_tungstenite::connect_async(req).await.unwrap().0 }
        };
        async fn next(ws: &mut Ws) -> Option<Message> {
            match tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
                .await
                .expect("timed out")
            {
                Some(Ok(m)) => Some(m),
                _ => None,
            }
        }
        /// The session ends: a Close frame (or EOF) and nothing else.
        async fn closed(ws: &mut Ws) {
            loop {
                match next(ws).await {
                    None | Some(Message::Close(_)) => return,
                    Some(Message::Text(t)) => panic!("unexpected {t}"),
                    Some(_) => {}
                }
            }
        }
        async fn mqtt_connect(ws: &mut Ws, password: Option<&str>) {
            let mut p = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'c'];
            if let Some(pw) = password {
                p[7] = 0xC2; // username + password
                p.extend_from_slice(&[0, 1, b'u']);
                p.extend_from_slice(&(pw.len() as u16).to_be_bytes());
                p.extend_from_slice(pw.as_bytes());
            }
            let mut packet = vec![0x10];
            let mut len = p.len();
            loop {
                let byte = (len % 128) as u8;
                len /= 128;
                packet.push(if len > 0 { byte | 0x80 } else { byte });
                if len == 0 {
                    break;
                }
            }
            packet.extend_from_slice(&p);
            ws.send(Message::Binary(packet.into())).await.unwrap();
            assert_eq!(
                next(ws).await,
                Some(Message::Binary(vec![0x20, 2, 0, 0].into())),
                "CONNACK accepted"
            );
        }
        async fn mqtt_alive(ws: &mut Ws) {
            ws.send(Message::Binary(vec![0xC0, 0].into()))
                .await
                .unwrap(); // PINGREQ
            assert_eq!(
                next(ws).await,
                Some(Message::Binary(vec![0xD0, 0].into())),
                "PINGRESP"
            );
        }

        let mut json_a = open("/notifications/ws/json/1", Some(&a_user)).await;
        let mut json_b = open("/notifications/ws/json/1", Some(&b_device)).await;
        for ws in [&mut json_a, &mut json_b] {
            assert!(
                matches!(next(ws).await, Some(Message::Text(_))),
                "initial SyncComplete"
            );
        }
        let mut mqtt_a_header = open("/mqtt", Some(&a_user)).await;
        mqtt_connect(&mut mqtt_a_header, None).await;
        let mut mqtt_a_connect = open("/mqtt", None).await;
        mqtt_connect(&mut mqtt_a_connect, Some(&a_user)).await;
        let mut mqtt_b = open("/mqtt", Some(&b_device)).await;
        mqtt_connect(&mut mqtt_b, None).await;

        assert!(state.devices.delete_device("RM110-A", None).unwrap());

        closed(&mut json_a).await;
        closed(&mut mqtt_a_header).await;
        closed(&mut mqtt_a_connect).await;
        // B's sessions are untouched.
        mqtt_alive(&mut mqtt_b).await;
        state
            .notification_tx
            .send(notifications::WsMessage::sync_complete(9, "d", "u1@test"))
            .unwrap();
        match next(&mut json_b).await {
            Some(Message::Text(t)) => assert!(t.contains("SyncComplete")),
            other => panic!("B's JSON session should still be open, got {other:?}"),
        }
        // Revoked A can't open a new session either.
        let mut req = format!("{base}/notifications/ws/json/1")
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert("authorization", format!("Bearer {a_user}").parse().unwrap());
        match tokio_tungstenite::connect_async(req).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(r)) => assert_eq!(r.status(), 401),
            other => panic!("revoked token must be refused, got {:?}", other.map(|_| ())),
        }
    }

    /// A browser returning from the OAuth provider carries no device token: the callback and
    /// success page must be reachable through the production auth layer; the API must not.
    #[tokio::test]
    async fn oauth_browser_routes_bypass_device_auth() {
        use tower::ServiceExt;
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path()).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let state = AppState::new(storage, devices);
        // Merged exactly as main.rs does, so any route conflict would panic here too.
        let app =
            create_router(state.clone()).merge(feature_routes(state, tmp.path(), None).unwrap());
        let get = |uri: &str| {
            axum::http::Request::get(uri)
                .body(axum::body::Body::empty())
                .unwrap()
        };
        for mount in ["/integrations/v2/cloud", "/integrations/v2/storage"] {
            let status = |uri: String| {
                let app = app.clone();
                async move { app.oneshot(get(&uri)).await.unwrap().status() }
            };
            assert_eq!(
                status(format!("{mount}/providers/dropbox/success")).await,
                axum::http::StatusCode::OK
            );
            // Reaches the handler (unknown PKCE state -> 400), not the auth layer (401).
            assert_eq!(
                status(format!("{mount}/callback?code=c&state=bogus")).await,
                axum::http::StatusCode::BAD_REQUEST
            );
            assert_eq!(
                status(format!("{mount}/providers")).await,
                axum::http::StatusCode::UNAUTHORIZED
            );
            assert_eq!(
                status(format!("{mount}/providers/dropbox/status")).await,
                axum::http::StatusCode::UNAUTHORIZED
            );
        }
    }
}
