//! Remote calendar sync against local mock servers: Google Calendar API v3, Microsoft Graph
//! and CalDAV. Checks the auth each provider sends, paging, OAuth token refresh and its
//! persistence, all-day mapping, and per-calendar error reporting.

use std::collections::HashMap;
use std::path::Path as FsPath;
use std::sync::Arc;

use axum::body::to_bytes;
use axum::extract::{Form, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use parking_lot::Mutex;
use remarkable_server::calendar::{
    Calendar,
    CalendarConfig,
    CalendarEvent,
    CalendarManager,
    CalendarProvider,
    EventQuery,
    EventStatus,
};
use remarkable_server::calendar_api::{
    self,
    CalendarState,
    sync_all_calendars,
    sync_calendar_endpoint,
};
use remarkable_server::calendar_providers::ProviderEndpoints;
use serde_json::{Value, json};

/// Serve the router `make` builds (it gets its own base URL) on an ephemeral local port.
async fn serve(make: impl FnOnce(String) -> Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = make(base.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    base
}

fn calendar(id: &str, name: &str, provider: CalendarProvider, config: CalendarConfig) -> Calendar {
    Calendar {
        id: id.into(),
        name: name.into(),
        color: None,
        provider,
        primary: false,
        read_only: false,
        sync_token: None,
        last_sync: None,
        config,
    }
}

fn all_events(state: &CalendarState, id: &str) -> Vec<CalendarEvent> {
    let now = Utc::now();
    state
        .manager
        .lock()
        .get_events(
            id,
            &EventQuery {
                start: Some(now - Duration::days(400)),
                end: Some(now + Duration::days(800)),
                ..Default::default()
            },
        )
        .unwrap()
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn midnight(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(0, 0, 0).unwrap().and_utc()
}

fn reopen(db: &FsPath, id: &str) -> CalendarConfig {
    CalendarManager::new(db)
        .unwrap()
        .get_calendar(id)
        .unwrap()
        .config
}

// ---------------------------------------------------------------------------------------
// Google Calendar API v3
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct GoogleLog {
    token_forms: Vec<HashMap<String, String>>,
    /// (Authorization header, query) per events.list call.
    lists: Vec<(Option<String>, HashMap<String, String>)>,
    calendar_ids: Vec<String>,
}

fn google_mock(log: Arc<Mutex<GoogleLog>>, day: NaiveDate) -> Router {
    let token_log = log.clone();
    Router::new()
        .route(
            "/token",
            post(move |Form(form): Form<HashMap<String, String>>| async move {
                let ok = form.get("grant_type").map(String::as_str) == Some("refresh_token")
                    && form.get("refresh_token").map(String::as_str) == Some("refresh-1")
                    && form.get("client_id").map(String::as_str) == Some("cid")
                    && form.get("client_secret").map(String::as_str) == Some("csecret");
                token_log.lock().token_forms.push(form);
                if ok {
                    Json(json!({"access_token": "fresh-token", "expires_in": 3599, "token_type": "Bearer"}))
                        .into_response()
                } else {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "invalid_grant", "error_description": "Token has been expired or revoked."})),
                    )
                        .into_response()
                }
            }),
        )
        .route(
            "/calendar/v3/calendars/{id}/events",
            get(
                move |Path(id): Path<String>,
                      Query(query): Query<HashMap<String, String>>,
                      headers: HeaderMap| async move {
                    let auth = header(&headers, "authorization");
                    let page = query.get("pageToken").cloned();
                    {
                        let mut log = log.lock();
                        log.lists.push((auth.clone(), query));
                        log.calendar_ids.push(id);
                    }
                    if auth.as_deref() != Some("Bearer fresh-token") {
                        return (
                            StatusCode::UNAUTHORIZED,
                            Json(json!({"error": {"code": 401, "message": "Invalid Credentials"}})),
                        )
                            .into_response();
                    }
                    let date = |d: i64| (day + Duration::days(d)).format("%Y-%m-%d").to_string();
                    match page.as_deref() {
                        None => Json(json!({
                            "items": [
                                {"id": "allday1", "status": "confirmed", "summary": "Holiday",
                                 "iCalUID": "allday1@google.com",
                                 "start": {"date": date(2)}, "end": {"date": date(3)},
                                 "etag": "\"e1\""},
                                {"id": "timed1", "summary": "Design review",
                                 "location": "Room 7", "description": "Bring notes",
                                 "start": {"dateTime": format!("{}T09:00:00-07:00", date(4)), "timeZone": "America/Los_Angeles"},
                                 "end": {"dateTime": format!("{}T10:30:00-07:00", date(4))},
                                 "hangoutLink": "https://meet.google.com/abc-defg-hij",
                                 "organizer": {"email": "boss@example.com", "displayName": "Boss"},
                                 "attendees": [
                                     {"email": "me@example.com", "responseStatus": "accepted", "self": true},
                                     {"email": "opt@example.com", "responseStatus": "tentative", "optional": true}
                                 ]},
                                {"id": "cancelled-stub", "status": "cancelled"}
                            ],
                            "nextPageToken": "page-2"
                        }))
                        .into_response(),
                        Some("page-2") => Json(json!({
                            "items": [
                                {"id": "timed2_20260101", "summary": "Weekly 1:1",
                                 "start": {"dateTime": format!("{}T16:00:00Z", date(5))},
                                 "end": {"dateTime": format!("{}T16:30:00Z", date(5))},
                                 "conferenceData": {"entryPoints": [
                                     {"entryPointType": "phone", "uri": "tel:+1-555"},
                                     {"entryPointType": "video", "uri": "https://zoom.example/j/1"}
                                 ]}}
                            ]
                        }))
                        .into_response(),
                        Some(other) => (StatusCode::BAD_REQUEST, format!("bad page {}", other))
                            .into_response(),
                    }
                },
            ),
        )
}

#[tokio::test]
async fn google_refreshes_rejected_token_follows_pages_and_persists_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("calendars.db");
    let log = Arc::new(Mutex::new(GoogleLog::default()));
    let today = Utc::now().date_naive();
    let base = serve({
        let log = log.clone();
        move |_| google_mock(log, today)
    })
    .await;

    let mut mgr = CalendarManager::new(&db).unwrap();
    mgr.add_calendar(calendar(
        "g1",
        "Team",
        CalendarProvider::Google,
        CalendarConfig::Google {
            calendar_id: "team@group.calendar.google.com".into(),
            access_token: Some("stale-token".into()),
            refresh_token: Some("refresh-1".into()),
            client_id: Some("cid".into()),
            client_secret: Some("csecret".into()),
            token_expires_at: None,
        },
    ))
    .unwrap();
    // An event deleted upstream since the last sync, and one far outside the window.
    let stale = |uid: &str, days: i64| {
        let mut e = remarkable_server::calendar::parse_ics_str(
            &format!(
                "BEGIN:VEVENT\nUID:{}\nSUMMARY:old\nDTSTART:{}\nEND:VEVENT\n",
                uid,
                (Utc::now() + Duration::days(days)).format("%Y%m%dT%H%M%SZ")
            ),
            "g1",
        );
        e.pop().unwrap()
    };
    mgr.upsert_event(&stale("deleted-upstream", 1)).unwrap();
    mgr.upsert_event(&stale("long-ago", -300)).unwrap();
    let state = CalendarState::new(mgr).with_endpoints(ProviderEndpoints {
        google_api: format!("{}/calendar/v3", base),
        google_token_url: format!("{}/token", base),
        ..Default::default()
    });

    let axum::Json(result) = sync_calendar_endpoint(State(state.clone()), Path("g1".into()))
        .await
        .unwrap();
    assert!(result.success, "{:?}", result.error);
    assert_eq!(result.events_synced, 3);
    assert_eq!(result.events_removed, 1);
    let json = serde_json::to_value(&result).unwrap();
    assert_eq!(json["events_removed"], 1);

    {
        let log = log.lock();
        // Stale token rejected once, refreshed once, then both pages with the new token.
        let auths: Vec<_> = log.lists.iter().map(|(a, _)| a.clone().unwrap()).collect();
        assert_eq!(
            auths,
            [
                "Bearer stale-token",
                "Bearer fresh-token",
                "Bearer fresh-token"
            ]
        );
        assert_eq!(log.token_forms.len(), 1);
        assert!(
            log.calendar_ids
                .iter()
                .all(|id| id == "team@group.calendar.google.com")
        );
        let (_, first) = &log.lists[1];
        assert_eq!(first["singleEvents"], "true");
        assert_eq!(first["orderBy"], "startTime");
        let time_min = DateTime::parse_from_rfc3339(&first["timeMin"]).unwrap();
        let time_max = DateTime::parse_from_rfc3339(&first["timeMax"]).unwrap();
        assert!((time_max - time_min).num_days() >= 390);
        assert!(!first.contains_key("pageToken"));
        assert_eq!(log.lists[2].1["pageToken"], "page-2");
    }

    let events = all_events(&state, "g1");
    let by_id = |id: &str| events.iter().find(|e| e.id == id).unwrap();
    let holiday = by_id("g1:allday1");
    assert!(holiday.all_day);
    assert_eq!(holiday.uid, "allday1@google.com");
    assert_eq!(holiday.start, midnight(today + Duration::days(2)));
    assert_eq!(holiday.end, midnight(today + Duration::days(3)));
    assert_eq!(holiday.etag.as_deref(), Some("\"e1\""));
    let review = by_id("g1:timed1");
    assert!(!review.all_day);
    assert_eq!(
        review.start,
        midnight(today + Duration::days(4)) + Duration::hours(16)
    );
    assert_eq!(review.end - review.start, Duration::minutes(90));
    assert_eq!(review.location.as_deref(), Some("Room 7"));
    assert_eq!(
        review.meeting_url.as_deref(),
        Some("https://meet.google.com/abc-defg-hij")
    );
    assert_eq!(review.attendees.len(), 2);
    assert_eq!(review.organizer.as_ref().unwrap().email, "boss@example.com");
    assert_eq!(
        by_id("g1:timed2_20260101").meeting_url.as_deref(),
        Some("https://zoom.example/j/1")
    );
    assert!(events.iter().all(|e| e.uid != "deleted-upstream"));
    assert!(events.iter().any(|e| e.uid == "long-ago"));
    assert!(
        state
            .manager
            .lock()
            .get_calendar("g1")
            .unwrap()
            .last_sync
            .is_some()
    );

    // The refreshed token survives a restart; the refresh token and client are kept.
    drop(state);
    match reopen(&db, "g1") {
        CalendarConfig::Google {
            access_token,
            refresh_token,
            client_secret,
            token_expires_at,
            ..
        } => {
            assert_eq!(access_token.as_deref(), Some("fresh-token"));
            assert_eq!(refresh_token.as_deref(), Some("refresh-1"));
            assert_eq!(client_secret.as_deref(), Some("csecret"));
            let left = token_expires_at.unwrap() - Utc::now();
            assert!(left > Duration::minutes(55) && left <= Duration::hours(1));
        }
        other => panic!("{:?}", other),
    }
}

// ---------------------------------------------------------------------------------------
// Microsoft Graph
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct GraphLog {
    token_requests: Vec<(String, HashMap<String, String>)>,
    views: Vec<(Option<String>, Option<String>, String)>,
}

fn graph_mock(base: String, log: Arc<Mutex<GraphLog>>, day: NaiveDate) -> Router {
    let token_log = log.clone();
    Router::new()
        .route(
            "/login/{tenant}/oauth2/v2.0/token",
            post(
                move |Path(tenant): Path<String>, Form(form): Form<HashMap<String, String>>| async move {
                    token_log.lock().token_requests.push((tenant, form));
                    // expires_in as a string, as some Microsoft endpoints send it.
                    Json(json!({
                        "token_type": "Bearer",
                        "access_token": "ms-access-2",
                        "refresh_token": "ms-refresh-2",
                        "expires_in": "3600"
                    }))
                },
            ),
        )
        .route(
            "/v1.0/me/calendars/{id}/calendarView",
            get(
                move |Path(id): Path<String>,
                      Query(query): Query<HashMap<String, String>>,
                      headers: HeaderMap| async move {
                    let auth = header(&headers, "authorization");
                    let prefer = header(&headers, "prefer");
                    log.lock().views.push((auth.clone(), prefer, id.clone()));
                    if auth.as_deref() != Some("Bearer ms-access-2") {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    let date = |d: i64| (day + Duration::days(d)).format("%Y-%m-%d").to_string();
                    if !query.contains_key("$skiptoken") {
                        assert!(query.contains_key("startDateTime") && query.contains_key("endDateTime"));
                        Json(json!({
                            "value": [
                                {"id": "AAMk-1", "iCalUId": "040000008200E001", "subject": "Offsite",
                                 "isAllDay": true, "isCancelled": false, "showAs": "oof",
                                 "start": {"dateTime": format!("{}T00:00:00.0000000", date(1)), "timeZone": "UTC"},
                                 "end": {"dateTime": format!("{}T00:00:00.0000000", date(3)), "timeZone": "UTC"}},
                                {"id": "AAMk-2", "subject": "Sprint planning", "bodyPreview": "Agenda",
                                 "location": {"displayName": "Teams"}, "isAllDay": false,
                                 "showAs": "tentative",
                                 "start": {"dateTime": format!("{}T14:30:00.0000000", date(2)), "timeZone": "UTC"},
                                 "end": {"dateTime": format!("{}T15:00:00.0000000", date(2)), "timeZone": "UTC"},
                                 "onlineMeeting": {"joinUrl": "https://teams.example/l/meetup"},
                                 "organizer": {"emailAddress": {"name": "Lead", "address": "lead@contoso.com"}},
                                 "attendees": [
                                     {"type": "required", "status": {"response": "accepted"}, "emailAddress": {"name": "Me", "address": "me@contoso.com"}},
                                     {"type": "optional", "status": {"response": "tentativelyAccepted"}, "emailAddress": {"address": "opt@contoso.com"}}
                                 ]}
                            ],
                            "@odata.nextLink": format!("{}/v1.0/me/calendars/{}/calendarView?$skiptoken=abc", base, id)
                        }))
                        .into_response()
                    } else {
                        Json(json!({
                            "value": [
                                {"id": "AAMk-3", "subject": "Canceled: Retro", "isAllDay": false, "isCancelled": true,
                                 "start": {"dateTime": format!("{}T08:00:00.0000000", date(6)), "timeZone": "UTC"},
                                 "end": {"dateTime": format!("{}T09:00:00.0000000", date(6)), "timeZone": "UTC"}}
                            ]
                        }))
                        .into_response()
                    }
                },
            ),
        )
}

#[tokio::test]
async fn graph_refreshes_missing_token_follows_next_link_and_persists_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("calendars.db");
    let log = Arc::new(Mutex::new(GraphLog::default()));
    let today = Utc::now().date_naive();
    let base = serve({
        let log = log.clone();
        move |base| graph_mock(base, log, today)
    })
    .await;

    let mut mgr = CalendarManager::new(&db).unwrap();
    // `exchange` provider backed by a Graph config: Exchange Online syncs through Graph.
    mgr.add_calendar(calendar(
        "m1",
        "Work",
        CalendarProvider::Exchange,
        CalendarConfig::Office365 {
            tenant_id: "contoso.onmicrosoft.com".into(),
            access_token: None,
            refresh_token: Some("ms-refresh-1".into()),
            client_id: Some("app-id".into()),
            client_secret: None,
            calendar_id: Some("AAMkWork=".into()),
            token_expires_at: None,
        },
    ))
    .unwrap();
    let state = CalendarState::new(mgr).with_endpoints(ProviderEndpoints {
        graph_api: format!("{}/v1.0", base),
        microsoft_login: format!("{}/login", base),
        ..Default::default()
    });

    let axum::Json(results) = sync_all_calendars(State(state.clone())).await.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].success, "{:?}", results[0].error);
    assert_eq!(results[0].events_synced, 3);

    {
        let log = log.lock();
        assert_eq!(log.token_requests.len(), 1);
        let (tenant, form) = &log.token_requests[0];
        assert_eq!(tenant, "contoso.onmicrosoft.com");
        assert_eq!(form["grant_type"], "refresh_token");
        assert_eq!(form["refresh_token"], "ms-refresh-1");
        assert_eq!(form["client_id"], "app-id");
        assert!(
            form["scope"].contains("Calendars.Read") && form["scope"].contains("offline_access")
        );
        assert!(!form.contains_key("client_secret"));
        // Token fetched up front (none stored), then two pages, both with the UTC preference.
        assert_eq!(log.views.len(), 2);
        for (auth, prefer, id) in &log.views {
            assert_eq!(auth.as_deref(), Some("Bearer ms-access-2"));
            assert_eq!(prefer.as_deref(), Some("outlook.timezone=\"UTC\""));
            assert_eq!(id, "AAMkWork=");
        }
    }

    let events = all_events(&state, "m1");
    let by_id = |id: &str| events.iter().find(|e| e.id == id).unwrap();
    let offsite = by_id("m1:AAMk-1");
    assert!(offsite.all_day);
    assert_eq!(offsite.uid, "040000008200E001");
    assert_eq!(offsite.start, midnight(today + Duration::days(1)));
    assert_eq!(offsite.end, midnight(today + Duration::days(3)));
    let planning = by_id("m1:AAMk-2");
    assert!(!planning.all_day);
    assert_eq!(
        planning.start,
        midnight(today + Duration::days(2)) + Duration::minutes(14 * 60 + 30)
    );
    assert_eq!(planning.status, EventStatus::Tentative);
    assert_eq!(planning.location.as_deref(), Some("Teams"));
    assert_eq!(planning.description.as_deref(), Some("Agenda"));
    assert_eq!(
        planning.meeting_url.as_deref(),
        Some("https://teams.example/l/meetup")
    );
    assert_eq!(planning.attendees.len(), 2);
    assert_eq!(by_id("m1:AAMk-3").status, EventStatus::Cancelled);

    drop(state);
    match reopen(&db, "m1") {
        CalendarConfig::Office365 {
            access_token,
            refresh_token,
            token_expires_at,
            ..
        } => {
            assert_eq!(access_token.as_deref(), Some("ms-access-2"));
            // Microsoft rotates refresh tokens; the new one must be kept.
            assert_eq!(refresh_token.as_deref(), Some("ms-refresh-2"));
            assert!(token_expires_at.unwrap() > Utc::now() + Duration::minutes(55));
        }
        other => panic!("{:?}", other),
    }
}

// ---------------------------------------------------------------------------------------
// CalDAV
// ---------------------------------------------------------------------------------------

#[derive(Default)]
struct DavLog {
    /// (method, path, Depth, Authorization, body)
    requests: Vec<(String, String, Option<String>, Option<String>, String)>,
}

fn multistatus(inner: &str) -> Response {
    (
        StatusCode::MULTI_STATUS,
        [("content-type", "application/xml; charset=utf-8")],
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">{}</d:multistatus>"#,
            inner
        ),
    )
        .into_response()
}

fn ics_escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('\r', "&#13;")
}

/// Server with discovery: `/` is not DAV, `/.well-known/caldav` redirects to `/dav/`, which
/// names the principal, whose calendar home holds a "Work" event calendar and a task list.
fn discovery_dav(state: Arc<Mutex<DavLog>>, day: NaiveDate) -> Router {
    Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: String| async move {
            let auth = header(&headers, "authorization");
            state.lock().requests.push((
                method.to_string(),
                uri.path().to_string(),
                header(&headers, "depth"),
                auth.clone(),
                body.clone(),
            ));
            // "alice:s3cret"
            if auth.as_deref() != Some("Basic YWxpY2U6czNjcmV0") {
                return (StatusCode::UNAUTHORIZED, "who are you").into_response();
            }
            let d = |n: i64| (day + Duration::days(n)).format("%Y%m%d").to_string();
            match (method.as_str(), uri.path()) {
                ("PROPFIND", "/") => StatusCode::NOT_FOUND.into_response(),
                ("PROPFIND", "/.well-known/caldav") => {
                    (StatusCode::MOVED_PERMANENTLY, [("location", "/dav/")]).into_response()
                }
                ("PROPFIND", "/dav/") => multistatus(
                    r#"<d:response><d:href>/dav/</d:href><d:propstat><d:prop>
                        <d:resourcetype><d:collection/></d:resourcetype>
                        <d:current-user-principal><d:href>/dav/principals/alice/</d:href></d:current-user-principal>
                       </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>
                       <d:propstat><d:prop><cal:calendar-home-set/></d:prop><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>
                       </d:response>"#,
                ),
                ("PROPFIND", "/dav/principals/alice/") => multistatus(
                    r#"<d:response><d:href>/dav/principals/alice/</d:href><d:propstat><d:prop>
                        <cal:calendar-home-set><d:href>/dav/calendars/alice/</d:href></cal:calendar-home-set>
                       </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"#,
                ),
                ("PROPFIND", "/dav/calendars/alice/") => multistatus(
                    r#"<d:response><d:href>/dav/calendars/alice/</d:href><d:propstat><d:prop>
                          <d:resourcetype><d:collection/></d:resourcetype></d:prop>
                          <d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
                       <d:response><d:href>/dav/calendars/alice/work/</d:href><d:propstat><d:prop>
                          <d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
                          <d:displayname>Work</d:displayname>
                          <cal:supported-calendar-component-set><cal:comp name="VEVENT"/></cal:supported-calendar-component-set>
                          </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
                       <d:response><d:href>/dav/calendars/alice/tasks/</d:href><d:propstat><d:prop>
                          <d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
                          <d:displayname>Tasks</d:displayname>
                          <cal:supported-calendar-component-set><cal:comp name="VTODO"/></cal:supported-calendar-component-set>
                          </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"#,
                ),
                ("REPORT", "/dav/calendars/alice/work/") => {
                    let holiday = format!(
                        "BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nUID:holiday@dav\nSUMMARY:Company holiday\nDTSTART;VALUE=DATE:{}\nDTEND;VALUE=DATE:{}\nBEGIN:VALARM\nACTION:DISPLAY\nDESCRIPTION:Reminder\nTRIGGER:-PT15M\nEND:VALARM\nEND:VEVENT\nEND:VCALENDAR\n",
                        d(3),
                        d(4)
                    );
                    let standup = format!(
                        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:standup@dav\r\nRECURRENCE-ID:{a}T090000Z\r\nSUMMARY:Standup\\, team <A&B>\r\nDTSTART:{a}T090000Z\r\nDTEND:{a}T091500Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:standup@dav\r\nRECURRENCE-ID:{b}T090000Z\r\nSUMMARY:Standup\\, team <A&B>\r\nDTSTART:{b}T090000Z\r\nDURATION:PT15M\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
                        a = d(1),
                        b = d(2)
                    );
                    multistatus(&format!(
                        r#"<d:response><d:href>/dav/calendars/alice/work/holiday.ics</d:href><d:propstat><d:prop>
                              <d:getetag>"h1"</d:getetag>
                              <cal:calendar-data><![CDATA[{}]]></cal:calendar-data>
                              </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
                           <d:response><d:href>/dav/calendars/alice/work/standup.ics</d:href><d:propstat><d:prop>
                              <d:getetag>"s7"</d:getetag>
                              <cal:calendar-data>{}</cal:calendar-data>
                              </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
                           <d:response><d:href>/dav/calendars/alice/work/private.ics</d:href><d:propstat><d:prop>
                              <cal:calendar-data/></d:prop><d:status>HTTP/1.1 403 Forbidden</d:status></d:propstat></d:response>"#,
                        holiday,
                        ics_escape_xml(&standup)
                    ))
                }
                _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
            }
        },
    )
}

#[tokio::test]
async fn caldav_discovers_collection_with_basic_auth_and_parses_report() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("calendars.db");
    let log = Arc::new(Mutex::new(DavLog::default()));
    let today = Utc::now().date_naive();
    let base = serve({
        let log = log.clone();
        move |_| discovery_dav(log, today)
    })
    .await;

    let mut mgr = CalendarManager::new(&db).unwrap();
    mgr.add_calendar(calendar(
        "d1",
        "Work",
        CalendarProvider::Caldav,
        CalendarConfig::Caldav {
            url: format!("{}/", base),
            username: "alice".into(),
            password: Some("s3cret".into()),
            bearer_token: None,
            collection_url: None,
        },
    ))
    .unwrap();
    let state = CalendarState::new(mgr);

    let axum::Json(result) = sync_calendar_endpoint(State(state.clone()), Path("d1".into()))
        .await
        .unwrap();
    assert!(result.success, "{:?}", result.error);
    assert_eq!(result.events_synced, 3);

    {
        let log = log.lock();
        let steps: Vec<_> = log
            .requests
            .iter()
            .map(|(m, p, depth, ..)| format!("{} {} {}", m, p, depth.as_deref().unwrap_or("-")))
            .collect();
        assert_eq!(
            steps,
            [
                "PROPFIND / 0",
                "PROPFIND /.well-known/caldav 0",
                "PROPFIND /dav/ 0",
                "PROPFIND /dav/principals/alice/ 0",
                "PROPFIND /dav/calendars/alice/ 1",
                "REPORT /dav/calendars/alice/work/ 1",
            ]
        );
        let report = &log.requests.last().unwrap().4;
        assert!(report.contains("calendar-query") && report.contains("<C:calendar-data>"));
        assert!(report.contains("<C:time-range start=\"") && report.contains("<C:expand"));
    }

    let events = all_events(&state, "d1");
    assert_eq!(events.len(), 3);
    let holiday = events.iter().find(|e| e.uid == "holiday@dav").unwrap();
    assert!(holiday.all_day);
    assert_eq!(holiday.start, midnight(today + Duration::days(3)));
    assert_eq!(holiday.end, midnight(today + Duration::days(4)));
    assert_eq!(holiday.etag.as_deref(), Some("\"h1\""));
    assert_eq!(holiday.description, None, "VALARM text must not leak");
    let standups: Vec<_> = events.iter().filter(|e| e.uid == "standup@dav").collect();
    assert_eq!(
        standups.len(),
        2,
        "each expanded occurrence is its own event"
    );
    for s in &standups {
        assert_eq!(s.summary, "Standup, team <A&B>");
        assert_eq!(s.end - s.start, Duration::minutes(15));
        assert_eq!(s.etag.as_deref(), Some("\"s7\""));
    }

    // The discovered collection is remembered, so the next sync goes straight to REPORT.
    let axum::Json(again) = sync_calendar_endpoint(State(state.clone()), Path("d1".into()))
        .await
        .unwrap();
    assert!(again.success && again.events_synced == 3 && again.events_removed == 0);
    assert_eq!(log.lock().requests.len(), 7);
    drop(state);
    match reopen(&db, "d1") {
        CalendarConfig::Caldav {
            collection_url,
            password,
            ..
        } => {
            assert_eq!(
                collection_url.as_deref(),
                Some(format!("{}/dav/calendars/alice/work/", base).as_str())
            );
            assert_eq!(password.as_deref(), Some("s3cret"));
        }
        other => panic!("{:?}", other),
    }
}

#[tokio::test]
async fn caldav_bearer_token_direct_collection_and_expand_fallback() {
    let log = Arc::new(Mutex::new(DavLog::default()));
    let day = Utc::now().date_naive() + Duration::days(10);
    let base = serve({
        let log = log.clone();
        move |_| {
            Router::new().fallback(
                move |method: Method, uri: Uri, headers: HeaderMap, body: String| async move {
                    let auth = header(&headers, "authorization");
                    log.lock().requests.push((
                        method.to_string(),
                        uri.path().to_string(),
                        header(&headers, "depth"),
                        auth.clone(),
                        body.clone(),
                    ));
                    if auth.as_deref() != Some("Bearer dav-token") {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    match method.as_str() {
                        "PROPFIND" => multistatus(
                            r#"<d:response><d:href>/cal/</d:href><d:propstat><d:prop>
                                <d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
                               </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"#,
                        ),
                        // No `expand` support: reject that query, answer the plain one.
                        "REPORT" if body.contains("expand") => {
                            (StatusCode::NOT_IMPLEMENTED, "expand unsupported").into_response()
                        }
                        "REPORT" => multistatus(&format!(
                            r#"<d:response><d:href>/cal/e.ics</d:href><d:propstat><d:prop>
                                <cal:calendar-data>BEGIN:VCALENDAR
BEGIN:VEVENT
UID:e1
SUMMARY:Dentist
DTSTART:{}T150000Z
END:VEVENT
END:VCALENDAR
</cal:calendar-data></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"#,
                            day.format("%Y%m%d")
                        )),
                        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
                    }
                },
            )
        }
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = CalendarManager::new(&dir.path().join("calendars.db")).unwrap();
    mgr.add_calendar(calendar(
        "d2",
        "Personal",
        CalendarProvider::Caldav,
        CalendarConfig::Caldav {
            url: format!("{}/cal/", base),
            username: String::new(),
            password: None,
            bearer_token: Some("dav-token".into()),
            collection_url: None,
        },
    ))
    .unwrap();
    let state = CalendarState::new(mgr);
    let axum::Json(result) = sync_calendar_endpoint(State(state.clone()), Path("d2".into()))
        .await
        .unwrap();
    assert!(result.success, "{:?}", result.error);
    assert_eq!(result.events_synced, 1);
    let log = log.lock();
    let methods: Vec<_> = log.requests.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(methods, ["PROPFIND", "REPORT", "REPORT"]);
    assert!(
        log.requests
            .iter()
            .all(|r| r.3.as_deref() == Some("Bearer dav-token"))
    );
    assert!(!log.requests[2].4.contains("expand"));
    let events = all_events(&state, "d2");
    assert_eq!(events[0].start, midnight(day) + Duration::hours(15));
}

// ---------------------------------------------------------------------------------------
// Error reporting and secrecy
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn sync_all_reports_each_provider_failure_without_leaking_secrets() {
    let base = serve(|_| {
        Router::new()
            .route(
                "/token",
                post(|| async {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "invalid_grant", "error_description": "Token has been expired or revoked."})),
                    )
                }),
            )
            .route(
                "/v1.0/me/calendar/calendarView",
                get(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": {"code": "ServiceNotAvailable", "message": "try later"}})),
                    )
                }),
            )
            .fallback(|| async { (StatusCode::UNAUTHORIZED, "bad credentials") })
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let ics = dir.path().join("ok.ics");
    std::fs::write(
        &ics,
        "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:1\nSUMMARY:x\nDTSTART:20250101T100000Z\nEND:VEVENT\nEND:VCALENDAR\n",
    )
    .unwrap();
    let mut mgr = CalendarManager::new(&dir.path().join("calendars.db")).unwrap();
    let cals = [
        calendar(
            "ics",
            "ics",
            CalendarProvider::Ics,
            CalendarConfig::Ics {
                path: ics,
                watch: false,
            },
        ),
        calendar(
            "dav",
            "dav",
            CalendarProvider::Caldav,
            CalendarConfig::Caldav {
                url: format!("{}/cal/", base),
                username: "alice".into(),
                password: Some("wrong-password".into()),
                bearer_token: None,
                collection_url: None,
            },
        ),
        calendar(
            "google",
            "google",
            CalendarProvider::Google,
            CalendarConfig::Google {
                calendar_id: "primary".into(),
                access_token: None,
                refresh_token: Some("revoked-refresh".into()),
                client_id: Some("cid".into()),
                client_secret: Some("csecret".into()),
                token_expires_at: None,
            },
        ),
        calendar(
            "graph",
            "graph",
            CalendarProvider::Office365,
            CalendarConfig::Office365 {
                tenant_id: String::new(),
                access_token: Some("valid-looking".into()),
                refresh_token: None,
                client_id: None,
                client_secret: None,
                calendar_id: None,
                token_expires_at: None,
            },
        ),
        calendar(
            "ews",
            "ews",
            CalendarProvider::Exchange,
            CalendarConfig::Exchange {
                server: "mail.example.com".into(),
                username: "u".into(),
                password: Some("ews-password".into()),
                use_ews: true,
            },
        ),
    ];
    for c in cals {
        mgr.add_calendar(c).unwrap();
    }
    let state = CalendarState::new(mgr).with_endpoints(ProviderEndpoints {
        google_api: format!("{}/calendar/v3", base),
        google_token_url: format!("{}/token", base),
        graph_api: format!("{}/v1.0", base),
        microsoft_login: format!("{}/login", base),
    });
    let axum::Json(results) = sync_all_calendars(State(state.clone())).await.unwrap();
    let get = |id: &str| results.iter().find(|r| r.calendar_id == id).unwrap();
    assert!(get("ics").success && get("ics").events_synced == 1);
    let expect = [
        ("dav", "HTTP 401"),
        ("google", "invalid_grant"),
        ("graph", "HTTP 503"),
        ("ews", "office365"),
    ];
    for (id, needle) in expect {
        let r = get(id);
        assert!(!r.success && r.events_synced == 0, "{}", id);
        let err = r.error.as_deref().unwrap();
        assert!(err.contains(needle), "{}: {}", id, err);
    }
    let body = serde_json::to_string(
        &results
            .iter()
            .map(|r| serde_json::to_value(r).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    for secret in [
        "wrong-password",
        "revoked-refresh",
        "csecret",
        "valid-looking",
        "ews-password",
    ] {
        assert!(!body.contains(secret), "{} leaked: {}", secret, body);
    }
    // A failed sync does not stamp last_sync.
    assert!(
        state
            .manager
            .lock()
            .get_calendar("google")
            .unwrap()
            .last_sync
            .is_none()
    );
}

#[tokio::test]
async fn calendar_api_never_returns_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let state = CalendarState::new(CalendarManager::new(&dir.path().join("calendars.db")).unwrap());
    let request: calendar_api::AddCalendarRequest = serde_json::from_value(json!({
        "name": "Work",
        "provider": "office365",
        "config": {
            "type": "office365",
            "tenant_id": "contoso",
            "access_token": "at-SECRET",
            "refresh_token": "rt-SECRET",
            "client_id": "app",
            "client_secret": "cs-SECRET"
        }
    }))
    .unwrap();
    let response = calendar_api::add_calendar(State(state.clone()), axum::Json(request))
        .await
        .unwrap()
        .into_response();
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    let axum::Json(list) = calendar_api::list_calendars(State(state.clone()))
        .await
        .unwrap();
    let axum::Json(one) = calendar_api::get_calendar(State(state.clone()), Path(id.clone()))
        .await
        .unwrap();
    assert_eq!(list.calendars.len(), 1);
    let rendered = [
        created.to_string(),
        serde_json::to_string(&list).unwrap(),
        serde_json::to_string(&one).unwrap(),
    ]
    .join("\n");
    assert!(!rendered.contains("SECRET"), "{}", rendered);
    // ... while the manager kept them for syncing.
    match state.manager.lock().get_calendar(&id).unwrap().config {
        CalendarConfig::Office365 {
            access_token,
            refresh_token,
            client_secret,
            ..
        } => {
            assert_eq!(access_token.as_deref(), Some("at-SECRET"));
            assert_eq!(refresh_token.as_deref(), Some("rt-SECRET"));
            assert_eq!(client_secret.as_deref(), Some("cs-SECRET"));
        }
        other => panic!("{:?}", other),
    }

    // A CalDAV calendar needs an http(s) URL.
    let bad: calendar_api::AddCalendarRequest = serde_json::from_value(json!({
        "name": "Bad", "provider": "caldav",
        "config": {"type": "caldav", "url": "file:///etc/passwd", "username": "u"}
    }))
    .unwrap();
    let err = calendar_api::add_calendar(State(state), axum::Json(bad))
        .await
        .err()
        .unwrap()
        .into_response();
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn caldav_picks_calendar_by_name_when_home_has_several() {
    let base = serve(|_| {
        Router::new().fallback(|method: Method, uri: Uri| async move {
            match (method.as_str(), uri.path()) {
                ("PROPFIND", "/home/") => multistatus(
                    r#"<d:response><d:href>/home/</d:href><d:propstat><d:prop>
                          <cal:calendar-home-set><d:href>/home/</d:href></cal:calendar-home-set>
                          <d:resourcetype><d:collection/></d:resourcetype></d:prop>
                          <d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
                       <d:response><d:href>/home/personal/</d:href><d:propstat><d:prop>
                          <d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
                          <d:displayname>Personal</d:displayname></d:prop>
                          <d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
                       <d:response><d:href>/home/work/</d:href><d:propstat><d:prop>
                          <d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
                          <d:displayname>Work</d:displayname></d:prop>
                          <d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"#,
                ),
                ("REPORT", "/home/work/") => multistatus(""),
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        })
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = CalendarManager::new(&dir.path().join("calendars.db")).unwrap();
    for (id, name) in [("work", "work"), ("other", "Other")] {
        mgr.add_calendar(calendar(
            id,
            name,
            CalendarProvider::Caldav,
            CalendarConfig::Caldav {
                url: format!("{}/home/", base),
                username: String::new(),
                password: None,
                bearer_token: None,
                collection_url: None,
            },
        ))
        .unwrap();
    }
    let state = CalendarState::new(mgr);
    let axum::Json(work) = sync_calendar_endpoint(State(state.clone()), Path("work".into()))
        .await
        .unwrap();
    assert!(work.success && work.events_synced == 0, "{:?}", work.error);
    match state.manager.lock().get_calendar("work").unwrap().config {
        CalendarConfig::Caldav { collection_url, .. } => {
            assert_eq!(collection_url, Some(format!("{}/home/work/", base)))
        }
        other => panic!("{:?}", other),
    }
    let axum::Json(other) = sync_calendar_endpoint(State(state.clone()), Path("other".into()))
        .await
        .unwrap();
    let err = other.error.unwrap();
    assert!(!other.success);
    assert!(
        err.contains("\"Personal\"") && err.contains("\"Work\"") && err.contains("/home/work/"),
        "{}",
        err
    );
}
