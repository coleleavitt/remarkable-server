//! Calendar API endpoints

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
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
use crate::error::{Result, ServerError};

#[derive(Clone)]
pub struct CalendarState {
    pub manager: Arc<Mutex<CalendarManager>>,
    pub sync_config: SyncConfig,
}

impl CalendarState {
    pub fn new(manager: CalendarManager) -> Self {
        Self {
            manager: Arc::new(Mutex::new(manager)),
            sync_config: SyncConfig::default(),
        }
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
    Caldav {
        url: String,
        username: String,
        password: Option<String>,
    },
    Google {
        calendar_id: String,
        access_token: Option<String>,
        refresh_token: Option<String>,
    },
    Exchange {
        server: String,
        username: String,
        password: Option<String>,
        #[serde(default)]
        use_ews: bool,
    },
    Office365 {
        tenant_id: String,
        access_token: Option<String>,
        refresh_token: Option<String>,
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
            } => CalendarConfig::Caldav {
                url,
                username,
                password,
            },
            CalendarConfigRequest::Google {
                calendar_id,
                access_token,
                refresh_token,
            } => CalendarConfig::Google {
                calendar_id,
                access_token,
                refresh_token,
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
            } => CalendarConfig::Office365 {
                tenant_id,
                access_token,
                refresh_token,
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

#[derive(Serialize)]
pub struct SyncResponse {
    pub calendar_id: String,
    pub events_synced: usize,
    pub success: bool,
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
            count
        }
        _ => 0,
    };
    Ok(Json(SyncResponse {
        calendar_id: id,
        events_synced: count,
        success: true,
    }))
}

pub async fn sync_all_calendars(
    State(state): State<CalendarState>,
) -> Result<Json<Vec<SyncResponse>>> {
    let calendars = state.manager.lock().list_calendars();
    let mut results = Vec::new();
    for calendar in calendars {
        let count = match &calendar.config {
            CalendarConfig::Ics { path, .. } => match parse_ics_file(path, &calendar.id) {
                Ok(events) => {
                    let c = events.len();
                    let mut mgr = state.manager.lock();
                    for e in events {
                        let _ = mgr.upsert_event(&e);
                    }
                    c
                }
                Err(_) => 0,
            },
            _ => 0,
        };
        results.push(SyncResponse {
            calendar_id: calendar.id,
            events_synced: count,
            success: true,
        });
    }
    Ok(Json(results))
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
