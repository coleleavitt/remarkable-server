//! Calendar API endpoints

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use futures_util::future::{BoxFuture, FutureExt, Shared};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::calendar::{
    Calendar,
    CalendarConfig,
    CalendarError,
    CalendarEvent,
    CalendarManager,
    CalendarProvider,
    CreateMeetingNoteRequest,
    EventQuery,
    MeetingNote,
    SyncConfig,
    parse_ics_file,
};
use crate::calendar_providers::{Fetched, ProviderEndpoints, RemoteSync, SyncWindow};
use crate::error::{Result, ServerError};

#[derive(Clone)]
pub struct CalendarState {
    pub manager: Arc<Mutex<CalendarManager>>,
    pub sync_config: SyncConfig,
    /// HTTP client and API endpoints for CalDAV / Google / Microsoft Graph calendars.
    pub remote: RemoteSync,
    /// The remote calendar syncs running, by calendar id. Two syncs of one calendar never
    /// overlap (each would refresh the OAuth token from, and save back, its own copy of the
    /// credentials), and a request for a calendar that is already syncing gets that sync's
    /// result instead of queueing another full sync behind it.
    syncs: SingleFlight<String, SyncResponse>,
    /// The `/sync-all` run in progress, if any; later requests share its result.
    sync_all: SingleFlight<(), Vec<SyncResponse>>,
}

impl CalendarState {
    pub fn new(manager: CalendarManager) -> Self {
        Self {
            manager: Arc::new(Mutex::new(manager)),
            sync_config: SyncConfig::default(),
            remote: RemoteSync::default(),
            syncs: SingleFlight::default(),
            sync_all: SingleFlight::default(),
        }
    }

    /// Use other Google / Microsoft API endpoints (tests, sovereign clouds).
    pub fn with_endpoints(mut self, endpoints: ProviderEndpoints) -> Self {
        self.remote = RemoteSync::new(endpoints);
        self
    }
}

/// A running task's result, shared by everyone awaiting it; `Err` when the task panicked.
type Flight<V> = Shared<BoxFuture<'static, std::result::Result<V, String>>>;

/// Runs at most one task per key at a time: a caller arriving while one runs awaits that
/// task's result instead of starting (or queueing) another. Each task runs on a task of its
/// own, so it finishes, and saves what it fetched, even when every caller has gone away.
struct SingleFlight<K, V> {
    running: Arc<Mutex<HashMap<K, Flight<V>>>>,
}

impl<K, V> Clone for SingleFlight<K, V> {
    fn clone(&self) -> Self {
        Self {
            running: self.running.clone(),
        }
    }
}

impl<K, V> Default for SingleFlight<K, V> {
    fn default() -> Self {
        Self {
            running: Arc::default(),
        }
    }
}

/// Takes a finished task's entry out of the map, also when the task panics.
struct Landed<K: Eq + Hash, V> {
    running: Arc<Mutex<HashMap<K, Flight<V>>>>,
    key: K,
}

impl<K: Eq + Hash, V> Drop for Landed<K, V> {
    fn drop(&mut self) {
        self.running.lock().remove(&self.key);
    }
}

impl<K, V> SingleFlight<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// The running task for `key`, or a new one running `task()`.
    fn join_or_start<F>(&self, key: K, task: impl FnOnce() -> F) -> Flight<V>
    where
        F: Future<Output = V> + Send + 'static,
    {
        let mut running = self.running.lock();
        if let Some(flight) = running.get(&key) {
            return flight.clone();
        }
        let task = task();
        let (map, landed_key) = (self.running.clone(), key.clone());
        let handle = tokio::spawn(async move {
            // Made inside the task, not captured by it: a future tokio drops unpolled (spawned
            // during shutdown) must not take the map lock held below. Its drop waits for that
            // lock, so the entry is in the map before it is removed; only this task removes
            // it, and no other task for the key starts while it is there.
            let _landed = Landed {
                running: map,
                key: landed_key,
            };
            task.await
        });
        let flight = async move { handle.await.map_err(|e| e.to_string()) }
            .boxed()
            .shared();
        running.insert(key, flight.clone());
        flight
    }
}

#[derive(Serialize)]
pub struct CalendarListResponse {
    pub calendars: Vec<CalendarResponse>,
}

#[derive(Serialize)]
pub struct CalendarResponse {
    pub id: String,
    pub name: String,
    pub color: Option<String>,
    pub provider: String,
    pub primary: bool,
    pub read_only: bool,
    pub last_sync: Option<String>,
}

impl From<Calendar> for CalendarResponse {
    fn from(c: Calendar) -> Self {
        Self {
            id: c.id,
            name: c.name,
            color: c.color,
            provider: c.provider.to_string(),
            primary: c.primary,
            read_only: c.read_only,
            last_sync: c.last_sync.map(|dt| dt.to_rfc3339()),
        }
    }
}

#[derive(Serialize)]
pub struct EventListResponse {
    pub events: Vec<EventResponse>,
    pub total: usize,
}

#[derive(Serialize)]
pub struct EventResponse {
    pub id: String,
    pub calendar_id: String,
    pub summary: String,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: String,
    pub end: String,
    pub all_day: bool,
    pub meeting_url: Option<String>,
    pub status: String,
}

impl From<CalendarEvent> for EventResponse {
    fn from(e: CalendarEvent) -> Self {
        Self {
            id: e.id,
            calendar_id: e.calendar_id,
            summary: e.summary,
            description: e.description,
            location: e.location,
            start: e.start.to_rfc3339(),
            end: e.end.to_rfc3339(),
            all_day: e.all_day,
            meeting_url: e.meeting_url,
            status: format!("{:?}", e.status).to_lowercase(),
        }
    }
}

#[derive(Serialize)]
pub struct MeetingNoteResponse {
    pub id: String,
    pub event_id: String,
    pub calendar_id: String,
    pub document_id: Option<String>,
    pub title: String,
    pub created: String,
}

impl From<MeetingNote> for MeetingNoteResponse {
    fn from(n: MeetingNote) -> Self {
        Self {
            id: n.id,
            event_id: n.event_id,
            calendar_id: n.calendar_id,
            document_id: n.document_id,
            title: n.title,
            created: n.created.to_rfc3339(),
        }
    }
}

#[derive(Deserialize)]
pub struct AddCalendarRequest {
    pub name: String,
    pub color: Option<String>,
    pub provider: String,
    #[serde(default)]
    pub primary: bool,
    pub config: CalendarConfigRequest,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CalendarConfigRequest {
    Ics {
        path: String,
        #[serde(default)]
        watch: bool,
    },
    /// `url` is the calendar collection or a URL to discover it from (server root, principal
    /// or calendar home). Basic auth with `username`/`password`, or `bearer_token`.
    Caldav {
        url: String,
        #[serde(default)]
        username: String,
        password: Option<String>,
        #[serde(default)]
        bearer_token: Option<String>,
    },
    /// Google Calendar API v3. `refresh_token` plus `client_id` (and usually `client_secret`)
    /// let the server renew the access token by itself.
    Google {
        calendar_id: String,
        access_token: Option<String>,
        refresh_token: Option<String>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<String>,
    },
    /// On-premises Exchange (EWS); stored but not synced.
    Exchange {
        server: String,
        username: String,
        password: Option<String>,
        #[serde(default)]
        use_ews: bool,
    },
    /// Microsoft Graph (Exchange Online / Microsoft 365). `calendar_id` defaults to the
    /// user's default calendar.
    Office365 {
        tenant_id: String,
        access_token: Option<String>,
        refresh_token: Option<String>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<String>,
        #[serde(default)]
        calendar_id: Option<String>,
    },
}

impl From<CalendarConfigRequest> for CalendarConfig {
    fn from(c: CalendarConfigRequest) -> Self {
        match c {
            CalendarConfigRequest::Ics { path, watch } => CalendarConfig::Ics {
                path: path.into(),
                watch,
            },
            CalendarConfigRequest::Caldav {
                url,
                username,
                password,
                bearer_token,
            } => CalendarConfig::Caldav {
                url,
                username,
                password,
                bearer_token,
                collection_url: None,
            },
            CalendarConfigRequest::Google {
                calendar_id,
                access_token,
                refresh_token,
                client_id,
                client_secret,
            } => CalendarConfig::Google {
                calendar_id,
                access_token,
                refresh_token,
                client_id,
                client_secret,
                token_expires_at: None,
            },
            CalendarConfigRequest::Exchange {
                server,
                username,
                password,
                use_ews,
            } => CalendarConfig::Exchange {
                server,
                username,
                password,
                use_ews,
            },
            CalendarConfigRequest::Office365 {
                tenant_id,
                access_token,
                refresh_token,
                client_id,
                client_secret,
                calendar_id,
            } => CalendarConfig::Office365 {
                tenant_id,
                access_token,
                refresh_token,
                client_id,
                client_secret,
                calendar_id,
                token_expires_at: None,
            },
        }
    }
}

#[derive(Deserialize)]
pub struct EventQueryParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub expand_recurring: bool,
}

impl From<EventQueryParams> for EventQuery {
    fn from(p: EventQueryParams) -> Self {
        Self {
            start: p
                .start
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            end: p
                .end
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            limit: p.limit,
            expand_recurring: p.expand_recurring,
        }
    }
}

#[derive(Deserialize)]
pub struct CreateMeetingNoteApiRequest {
    pub template_id: Option<String>,
    pub folder_id: Option<String>,
}

impl From<CreateMeetingNoteApiRequest> for CreateMeetingNoteRequest {
    fn from(r: CreateMeetingNoteApiRequest) -> Self {
        Self {
            template_id: r.template_id,
            folder_id: r.folder_id,
        }
    }
}

impl From<CalendarError> for ServerError {
    fn from(e: CalendarError) -> Self {
        match e {
            CalendarError::NotFound(id) => ServerError::NotFound(id),
            CalendarError::Backend(msg)
            | CalendarError::Database(msg)
            | CalendarError::Parse(msg)
            | CalendarError::Network(msg) => ServerError::Database(msg),
            CalendarError::AuthRequired(_) => ServerError::Unauthorized,
            CalendarError::Io(e) => ServerError::Storage(e),
        }
    }
}

pub async fn list_calendars(
    State(state): State<CalendarState>,
) -> Result<Json<CalendarListResponse>> {
    let calendars = state
        .manager
        .lock()
        .list_calendars()
        .into_iter()
        .map(CalendarResponse::from)
        .collect();
    Ok(Json(CalendarListResponse { calendars }))
}

pub async fn add_calendar(
    State(state): State<CalendarState>,
    Json(request): Json<AddCalendarRequest>,
) -> Result<impl IntoResponse> {
    let provider = match request.provider.as_str() {
        "ics" => CalendarProvider::Ics,
        "caldav" => CalendarProvider::Caldav,
        "google" => CalendarProvider::Google,
        "exchange" => CalendarProvider::Exchange,
        "office365" => CalendarProvider::Office365,
        _ => {
            return Err(ServerError::Database(format!(
                "Unknown provider: {}",
                request.provider
            )));
        }
    };
    if let CalendarConfigRequest::Caldav { url, .. } = &request.config {
        let valid = reqwest::Url::parse(url.trim())
            .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.has_host());
        if !valid {
            return Err(ServerError::BadRequest(format!(
                "caldav url must be an http(s) URL: {:?}",
                url
            )));
        }
    }
    let calendar = Calendar {
        id: uuid::Uuid::new_v4().to_string(),
        name: request.name,
        color: request.color,
        provider,
        primary: request.primary,
        read_only: false,
        sync_token: None,
        last_sync: None,
        config: request.config.into(),
    };
    let response = CalendarResponse::from(calendar.clone());
    state.manager.lock().add_calendar(calendar)?;
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn get_calendar(
    State(state): State<CalendarState>,
    Path(id): Path<String>,
) -> Result<Json<CalendarResponse>> {
    let calendar = state
        .manager
        .lock()
        .get_calendar(&id)
        .ok_or_else(|| ServerError::NotFound(id))?;
    Ok(Json(CalendarResponse::from(calendar)))
}

pub async fn delete_calendar(
    State(state): State<CalendarState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse> {
    if state.manager.lock().remove_calendar(&id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ServerError::NotFound(id))
    }
}

pub async fn get_events(
    State(state): State<CalendarState>,
    Path(id): Path<String>,
    Query(params): Query<EventQueryParams>,
) -> Result<Json<EventListResponse>> {
    let events = state.manager.lock().get_events(&id, &params.into())?;
    let total = events.len();
    Ok(Json(EventListResponse {
        events: events.into_iter().map(EventResponse::from).collect(),
        total,
    }))
}

pub async fn create_meeting_note(
    State(state): State<CalendarState>,
    Path((calendar_id, event_id)): Path<(String, String)>,
    Json(request): Json<CreateMeetingNoteApiRequest>,
) -> Result<impl IntoResponse> {
    let note =
        state
            .manager
            .lock()
            .create_meeting_note(&calendar_id, &event_id, &request.into())?;
    Ok((StatusCode::CREATED, Json(MeetingNoteResponse::from(note))))
}

pub async fn list_meeting_notes(
    State(state): State<CalendarState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<MeetingNoteResponse>>> {
    let notes = state.manager.lock().get_meeting_notes(&id, None)?;
    Ok(Json(
        notes.into_iter().map(MeetingNoteResponse::from).collect(),
    ))
}

#[derive(Clone, Serialize)]
pub struct SyncResponse {
    pub calendar_id: String,
    pub events_synced: usize,
    /// Stored events in the sync window that the provider no longer returns (deleted or
    /// moved upstream) and were removed. Omitted when zero.
    #[serde(skip_serializing_if = "is_zero")]
    pub events_removed: usize,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl SyncResponse {
    fn new(calendar_id: String, events_synced: usize, error: Option<String>) -> Self {
        Self {
            calendar_id,
            events_synced,
            events_removed: 0,
            success: error.is_none(),
            error,
        }
    }
}

pub async fn sync_calendar_endpoint(
    State(state): State<CalendarState>,
    Path(id): Path<String>,
) -> Result<Json<SyncResponse>> {
    let calendar = state
        .manager
        .lock()
        .get_calendar(&id)
        .ok_or_else(|| ServerError::NotFound(id.clone()))?;
    let count = match &calendar.config {
        CalendarConfig::Ics { path, .. } => {
            let events = parse_ics_file(path, &calendar.id).map_err(ServerError::from)?;
            let count = events.len();
            let mut mgr = state.manager.lock();
            for event in events {
                mgr.upsert_event(&event)?;
            }
            mgr.set_last_sync(&id, Utc::now())?;
            count
        }
        _ => {
            // Provider problems (including rejected credentials) are reported in the body, never
            // as an HTTP error status of this server.
            let result = sync_remote(&state, &id).await;
            if let Some(err) = &result.error {
                tracing::warn!("calendar {} sync failed: {}", result.calendar_id, err);
            }
            return Ok(Json(result));
        }
    };
    Ok(Json(SyncResponse::new(id, count, None)))
}

/// Sync one remote calendar, or wait for the sync of it already running and share its result.
///
/// The sync runs on a task of its own that outlives the request: when the client (or nginx,
/// after its read timeout) gives up, the handler's future is dropped, and tokens refreshed or
/// rotated by then, a discovered collection and the fetched events must still be saved.
/// Requests that keep coming while a slow provider holds one sync up join it rather than
/// piling up full syncs to run one after another.
async fn sync_remote(state: &CalendarState, id: &str) -> SyncResponse {
    let flight = state.syncs.join_or_start(id.to_string(), || {
        let state = state.clone();
        let id = id.to_string();
        async move { fetch_and_store(&state, &id).await }
    });
    flight.await.unwrap_or_else(|e| {
        SyncResponse::new(id.to_string(), 0, Some(format!("sync task failed: {}", e)))
    })
}

/// Fetch a remote (CalDAV / Google / Microsoft Graph) calendar and store what it returned in
/// the sync window. Every failure is reported in the response rather than returned.
async fn fetch_and_store(state: &CalendarState, id: &str) -> SyncResponse {
    // The stored calendar, not a copy taken before the request joined or started this sync:
    // a sync that just finished may have refreshed its tokens or found its collection.
    let Some(calendar) = state.manager.lock().get_calendar(id) else {
        return SyncResponse::new(
            id.to_string(),
            0,
            Some(format!("calendar {} no longer exists", id)),
        );
    };
    let window = SyncWindow::around(Utc::now());
    let mut config = calendar.config.clone();
    let fetched = state
        .remote
        .fetch_events(&calendar, &mut config, window)
        .await;
    let mut errors = Vec::new();
    let mut mgr = state.manager.lock();
    // Refreshed/rotated tokens and discovered URLs are saved even when the fetch failed later.
    if config != calendar.config {
        if let Err(e) = mgr.update_config(&calendar.id, config) {
            errors.push(format!("saving updated credentials failed: {}", e));
        }
    }
    let (stored, removed) = match fetched {
        Ok(Fetched {
            events,
            incomplete: None,
        }) => mgr
            .replace_events_in_range(&calendar.id, window.start, window.end, &events)
            .unwrap_or_else(|e| {
                errors.push(format!("saving events failed: {}", e));
                (0, 0)
            }),
        // Store what came, but an event missing from a partial answer was not deleted.
        Ok(Fetched {
            events,
            incomplete: Some(reason),
        }) => {
            errors.insert(0, reason);
            match mgr.upsert_events(&calendar.id, &events) {
                Ok(stored) => (stored, 0),
                Err(e) => {
                    errors.push(format!("saving events failed: {}", e));
                    (0, 0)
                }
            }
        }
        Err(e) => {
            errors.insert(0, e.to_string());
            (0, 0)
        }
    };
    if errors.is_empty() {
        if let Err(e) = mgr.set_last_sync(&calendar.id, Utc::now()) {
            errors.push(format!("saving sync time failed: {}", e));
        }
    }
    drop(mgr);
    let error = (!errors.is_empty()).then(|| errors.join("; "));
    SyncResponse {
        events_removed: removed,
        ..SyncResponse::new(calendar.id, stored, error)
    }
}

/// Load an ICS calendar's file; failures are reported in the result.
fn sync_ics(state: &CalendarState, calendar: &Calendar, path: &std::path::Path) -> SyncResponse {
    let (count, error) = match parse_ics_file(path, &calendar.id) {
        Ok(events) => {
            let total = events.len();
            let mut mgr = state.manager.lock();
            let failures: Vec<String> = events
                .iter()
                .filter_map(|e| {
                    mgr.upsert_event(e)
                        .err()
                        .map(|err| format!("{}: {}", e.uid, err))
                })
                .collect();
            let error = if failures.is_empty() {
                // Reported like a remote sync's: a sync whose time was not saved did not
                // fully succeed.
                mgr.set_last_sync(&calendar.id, Utc::now())
                    .err()
                    .map(|e| format!("saving sync time failed: {}", e))
            } else {
                Some(format!(
                    "{} of {} events failed to save: {}",
                    failures.len(),
                    total,
                    failures.join("; ")
                ))
            };
            (total - failures.len(), error)
        }
        Err(e) => (0, Some(format!("ICS load failed: {}", e))),
    };
    SyncResponse::new(calendar.id.clone(), count, error)
}

pub async fn sync_all_calendars(
    State(state): State<CalendarState>,
) -> Result<Json<Vec<SyncResponse>>> {
    // On a task of its own, like a single remote sync: the remaining calendars still sync (and
    // save refreshed tokens) when the client stops waiting. At most one runs; a request made
    // while it does gets its results.
    let flight = state.sync_all.join_or_start((), {
        let state = state.clone();
        move || sync_all(state)
    });
    let results = flight
        .await
        .map_err(|e| ServerError::Internal(format!("calendar sync task failed: {}", e)))?;
    Ok(Json(results))
}

async fn sync_all(state: CalendarState) -> Vec<SyncResponse> {
    let calendars = state.manager.lock().list_calendars();
    let mut results = Vec::new();
    for calendar in calendars {
        // Per-calendar failures are reported in that calendar's entry (success=false + error)
        // instead of being swallowed; the rest still sync.
        let result = match &calendar.config {
            CalendarConfig::Ics { path, .. } => sync_ics(&state, &calendar, path),
            _ => sync_remote(&state, &calendar.id).await,
        };
        if let Some(err) = &result.error {
            tracing::warn!("calendar {} sync failed: {}", result.calendar_id, err);
        }
        results.push(result);
    }
    results
}

pub async fn get_upcoming_events(
    State(state): State<CalendarState>,
    Query(params): Query<EventQueryParams>,
) -> Result<Json<EventListResponse>> {
    let calendars = state.manager.lock().list_calendars();
    let query: EventQuery = params.into();
    let mut all_events = Vec::new();
    for calendar in calendars {
        if let Ok(events) = state.manager.lock().get_events(&calendar.id, &query) {
            all_events.extend(events);
        }
    }
    all_events.sort_by(|a, b| a.start.cmp(&b.start));
    if let Some(limit) = query.limit {
        all_events.truncate(limit);
    }
    let total = all_events.len();
    Ok(Json(EventListResponse {
        events: all_events.into_iter().map(EventResponse::from).collect(),
        total,
    }))
}

#[derive(Deserialize)]
pub struct WebhookPayload {
    pub resource_id: Option<String>,
}

pub async fn calendar_webhook(
    State(_state): State<CalendarState>,
    Json(_payload): Json<WebhookPayload>,
) -> Result<impl IntoResponse> {
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn single_flight_shares_a_run_and_forgets_it_when_it_lands() {
        let flights = SingleFlight::<&'static str, usize>::default();
        let runs = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let run = || {
            let (runs, gate) = (runs.clone(), gate.clone());
            move || async move {
                gate.acquire().await.unwrap().forget();
                runs.fetch_add(1, Ordering::SeqCst) + 1
            }
        };
        let a = flights.join_or_start("k", run());
        let b = flights.join_or_start("k", run());
        let other = flights.join_or_start("j", run());
        gate.add_permits(3);
        let (a, b, other) = (a.await.unwrap(), b.await.unwrap(), other.await.unwrap());
        assert_eq!(a, b);
        assert_ne!(a, other);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        // Removed before the result was handed out: the next call runs again.
        assert!(flights.running.lock().is_empty());
        gate.add_permits(1);
        assert_eq!(flights.join_or_start("k", run()).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn single_flight_recovers_from_a_panicking_run() {
        let flights = SingleFlight::<(), u8>::default();
        let err = flights
            .join_or_start((), || async { panic!("provider exploded") })
            .await
            .unwrap_err();
        assert!(err.contains("panic"), "{}", err);
        assert!(flights.running.lock().is_empty());
        assert_eq!(flights.join_or_start((), || async { 7 }).await, Ok(7));
    }

    fn ics_calendar(id: &str, path: std::path::PathBuf) -> Calendar {
        Calendar {
            id: id.into(),
            name: id.into(),
            color: None,
            provider: CalendarProvider::Ics,
            primary: false,
            read_only: false,
            sync_token: None,
            last_sync: None,
            config: CalendarConfig::Ics { path, watch: false },
        }
    }

    #[tokio::test]
    async fn sync_all_reports_an_ics_sync_time_that_was_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let ics = dir.path().join("good.ics");
        std::fs::write(
            &ics,
            "BEGIN:VEVENT\nUID:1\nDTSTART:20250101T100000Z\nEND:VEVENT\n",
        )
        .unwrap();
        let db = dir.path().join("cal.db");
        let mut mgr = CalendarManager::new(&db).unwrap();
        mgr.add_calendar(ics_calendar("ics", ics)).unwrap();
        // The database refuses to record sync times from now on.
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER no_sync_time BEFORE UPDATE OF last_sync ON calendars \
                 BEGIN SELECT RAISE(ABORT, 'disk is full'); END;",
            )
            .unwrap();
        let state = CalendarState::new(mgr);
        let Json(results) = sync_all_calendars(State(state.clone())).await.unwrap();
        let result = &results[0];
        assert!(!result.success);
        assert_eq!(result.events_synced, 1);
        let error = result.error.as_deref().unwrap();
        assert!(
            error.contains("saving sync time failed") && error.contains("disk is full"),
            "{}",
            error
        );
        assert!(
            state
                .manager
                .lock()
                .get_calendar("ics")
                .unwrap()
                .last_sync
                .is_none()
        );
    }

    #[tokio::test]
    async fn sync_all_reports_per_calendar_failures() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.ics");
        std::fs::write(&good, "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:1\nSUMMARY:x\nDTSTART:20250101T100000Z\nEND:VEVENT\nEND:VCALENDAR\n").unwrap();
        let mut mgr = CalendarManager::new(&dir.path().join("cal.db")).unwrap();
        mgr.add_calendar(ics_calendar("good", good)).unwrap();
        mgr.add_calendar(ics_calendar("missing", dir.path().join("nope.ics")))
            .unwrap();
        mgr.add_calendar(Calendar {
            id: "o365".into(),
            name: "o365".into(),
            color: None,
            provider: CalendarProvider::Office365,
            primary: false,
            read_only: false,
            sync_token: None,
            last_sync: None,
            config: CalendarConfig::Office365 {
                tenant_id: "t".into(),
                access_token: None,
                refresh_token: None,
                client_id: None,
                client_secret: None,
                calendar_id: None,
                token_expires_at: None,
            },
        })
        .unwrap();
        let Json(results) = sync_all_calendars(State(CalendarState::new(mgr)))
            .await
            .unwrap();
        let good = results.iter().find(|r| r.calendar_id == "good").unwrap();
        assert!(good.success && good.error.is_none());
        assert_eq!(good.events_synced, 1);
        let missing = results.iter().find(|r| r.calendar_id == "missing").unwrap();
        assert!(!missing.success);
        assert!(
            missing
                .error
                .as_deref()
                .unwrap()
                .contains("ICS load failed")
        );
        let o365 = results.iter().find(|r| r.calendar_id == "o365").unwrap();
        assert!(!o365.success && o365.events_synced == 0);
        assert!(
            o365.error
                .as_deref()
                .unwrap()
                .contains("no usable access token"),
            "{:?}",
            o365.error
        );
        // Backward-compatible shape: existing fields still present, `error` only when set.
        let json = serde_json::to_value(good).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"calendar_id": "good", "events_synced": 1, "success": true})
        );
    }
}
