//! Google Calendar API v3: `events.list` on one calendar, recurring events expanded into
//! instances (`singleEvents=true`), following `nextPageToken`.

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::Deserialize;

use super::oauth::{self, Session};
use super::{
    MAX_BODY_BYTES,
    MAX_PAGES,
    SyncWindow,
    date_at_midnight,
    end_or_default,
    read_body,
    rfc3339_z,
};
use crate::calendar::{
    Attendee,
    AttendeeRole,
    AttendeeStatus,
    CalendarError,
    CalendarEvent,
    EventStatus,
    Result,
    UNTITLED_EVENT,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventsPage {
    #[serde(default)]
    items: Vec<Event>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    id: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    start: Option<EventTime>,
    #[serde(default)]
    end: Option<EventTime>,
    #[serde(default, rename = "iCalUID")]
    ical_uid: Option<String>,
    #[serde(default)]
    created: Option<String>,
    #[serde(default)]
    updated: Option<String>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    hangout_link: Option<String>,
    #[serde(default)]
    conference_data: Option<ConferenceData>,
    #[serde(default)]
    attendees: Vec<Person>,
    #[serde(default)]
    organizer: Option<Person>,
}

/// `date` (all-day) or `dateTime` (RFC 3339 with offset).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventTime {
    #[serde(default)]
    date: Option<String>,
    #[serde(default)]
    date_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConferenceData {
    #[serde(default)]
    entry_points: Vec<EntryPoint>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntryPoint {
    #[serde(default)]
    entry_point_type: Option<String>,
    #[serde(default)]
    uri: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Person {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    response_status: Option<String>,
    #[serde(default)]
    optional: bool,
    #[serde(default)]
    organizer: bool,
    #[serde(default)]
    resource: bool,
}

pub(super) async fn fetch(
    session: &mut Session<'_>,
    api_base: &str,
    remote_calendar_id: &str,
    calendar_id: &str,
    window: SyncWindow,
) -> Result<Vec<CalendarEvent>> {
    let url = format!(
        "{}/calendars/{}/events",
        api_base.trim_end_matches('/'),
        urlencoding::encode(remote_calendar_id)
    );
    let (time_min, time_max) = (rfc3339_z(window.start), rfc3339_z(window.end));
    let mut events = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut query = vec![
            ("timeMin", time_min.as_str()),
            ("timeMax", time_max.as_str()),
            ("singleEvents", "true"),
            ("orderBy", "startTime"),
            ("maxResults", "2500"),
        ];
        if let Some(token) = page_token.as_deref() {
            query.push(("pageToken", token));
        }
        let context = "google: events.list";
        let http = session.http();
        let response = session
            .send(context, |token| {
                http.get(&url).bearer_auth(token).query(&query)
            })
            .await?;
        let status = response.status();
        let body = read_body(response, MAX_BODY_BYTES, context).await?;
        if status != StatusCode::OK {
            return Err(oauth::api_error(context, status, &body));
        }
        let page: EventsPage = serde_json::from_str(&body)
            .map_err(|e| CalendarError::Parse(format!("{} response: {}", context, e)))?;
        events.extend(
            page.items
                .into_iter()
                .filter_map(|e| to_event(e, calendar_id)),
        );
        match page.next_page_token.filter(|t| !t.is_empty()) {
            Some(next) => page_token = Some(next),
            None => return Ok(events),
        }
    }
    Err(CalendarError::Backend(format!(
        "google: events.list returned more than {} pages",
        MAX_PAGES
    )))
}

/// `(start, all_day)` of an event time; `date` marks an all-day event.
fn parse_time(t: &EventTime) -> Option<(DateTime<Utc>, bool)> {
    if let Some(date) = t.date.as_deref() {
        return date_at_midnight(date).map(|d| (d, true));
    }
    let dt = DateTime::parse_from_rfc3339(t.date_time.as_deref()?).ok()?;
    Some((dt.with_timezone(&Utc), false))
}

fn parse_stamp(s: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s?)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn to_attendee(p: Person) -> Option<Attendee> {
    let status = match p.response_status.as_deref() {
        Some("accepted") => AttendeeStatus::Accepted,
        Some("declined") => AttendeeStatus::Declined,
        Some("tentative") => AttendeeStatus::Tentative,
        _ => AttendeeStatus::NeedsAction,
    };
    let role = if p.resource {
        AttendeeRole::NonParticipant
    } else if p.optional {
        AttendeeRole::Optional
    } else {
        AttendeeRole::Required
    };
    Some(Attendee {
        name: p.display_name,
        email: p.email?,
        status,
        role,
        organizer: p.organizer,
    })
}

/// Map one API event; events without a start (e.g. bare cancellation stubs) are skipped.
fn to_event(e: Event, calendar_id: &str) -> Option<CalendarEvent> {
    let (start, all_day) = parse_time(e.start.as_ref()?)?;
    let end = e.end.as_ref().and_then(parse_time).map(|(end, _)| end);
    let now = Utc::now();
    let meeting_url = e.hangout_link.or_else(|| {
        e.conference_data?
            .entry_points
            .into_iter()
            .find(|p| p.entry_point_type.as_deref() == Some("video"))?
            .uri
    });
    let organizer = e.organizer.and_then(|mut o| {
        o.organizer = true;
        o.response_status.get_or_insert_with(|| "accepted".into());
        let mut a = to_attendee(o)?;
        a.role = AttendeeRole::Chair;
        Some(a)
    });
    Some(CalendarEvent {
        id: format!("{}:{}", calendar_id, e.id),
        calendar_id: calendar_id.to_string(),
        uid: e.ical_uid.unwrap_or_else(|| e.id.clone()),
        summary: e
            .summary
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| UNTITLED_EVENT.to_string()),
        description: e.description,
        location: e.location,
        start,
        end: end_or_default(start, end, all_day),
        all_day,
        attendees: e.attendees.into_iter().filter_map(to_attendee).collect(),
        organizer,
        meeting_url,
        status: match e.status.as_deref() {
            Some("tentative") => EventStatus::Tentative,
            Some("cancelled") => EventStatus::Cancelled,
            _ => EventStatus::Confirmed,
        },
        created: parse_stamp(e.created.as_deref()).unwrap_or(now),
        updated: parse_stamp(e.updated.as_deref()).unwrap_or(now),
        etag: e.etag,
    })
}
