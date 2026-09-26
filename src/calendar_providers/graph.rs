//! Microsoft Graph: `calendarView` of one calendar (recurrences expanded by the server),
//! following `@odata.nextLink`, with times returned in UTC.

use chrono::{DateTime, NaiveDateTime, Utc};
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
    redact,
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

/// Scope requested on refresh: `.default` asks for every Microsoft Graph permission already
/// consented for this app (Calendars.Read, Calendars.ReadWrite, ...), so the refresh never
/// asks for one the grant lacks (AADSTS65001); `offline_access` keeps a refresh token coming.
pub(super) const SCOPE: &str = "https://graph.microsoft.com/.default offline_access";

/// Events per page.
const PAGE_SIZE: &str = "100";

/// Fields read from each event.
const SELECT: &str = "id,iCalUId,subject,bodyPreview,location,start,end,isAllDay,isCancelled,\
                      showAs,attendees,organizer,onlineMeeting,onlineMeetingUrl,createdDateTime,\
                      lastModifiedDateTime";

#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    value: Vec<Event>,
    #[serde(default, rename = "@odata.nextLink")]
    next_link: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    id: String,
    #[serde(default, rename = "iCalUId")]
    ical_uid: Option<String>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    body_preview: Option<String>,
    #[serde(default)]
    location: Option<Location>,
    #[serde(default)]
    start: Option<GraphDateTime>,
    #[serde(default)]
    end: Option<GraphDateTime>,
    #[serde(default)]
    is_all_day: bool,
    #[serde(default)]
    is_cancelled: bool,
    #[serde(default)]
    show_as: Option<String>,
    #[serde(default)]
    attendees: Vec<GraphAttendee>,
    #[serde(default)]
    organizer: Option<Recipient>,
    #[serde(default)]
    online_meeting: Option<OnlineMeeting>,
    #[serde(default)]
    online_meeting_url: Option<String>,
    #[serde(default)]
    created_date_time: Option<String>,
    #[serde(default)]
    last_modified_date_time: Option<String>,
    #[serde(default, rename = "@odata.etag")]
    etag: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Location {
    #[serde(default)]
    display_name: Option<String>,
}

/// Local date-time without offset (`2025-07-04T09:00:00.0000000`) in `time_zone`, which is
/// UTC because every request sends `Prefer: outlook.timezone="UTC"`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphDateTime {
    date_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphAttendee {
    #[serde(default)]
    email_address: Option<EmailAddress>,
    #[serde(default)]
    status: Option<ResponseStatus>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Recipient {
    #[serde(default)]
    email_address: Option<EmailAddress>,
}

#[derive(Deserialize)]
struct EmailAddress {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    address: Option<String>,
}

#[derive(Deserialize)]
struct ResponseStatus {
    #[serde(default)]
    response: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OnlineMeeting {
    #[serde(default)]
    join_url: Option<String>,
}

pub(super) async fn fetch(
    session: &mut Session<'_>,
    api_base: &str,
    remote_calendar_id: Option<&str>,
    calendar_id: &str,
    window: SyncWindow,
) -> Result<Vec<CalendarEvent>> {
    let base = api_base.trim_end_matches('/');
    let first = match remote_calendar_id.filter(|id| !id.is_empty()) {
        Some(id) => format!(
            "{}/me/calendars/{}/calendarView",
            base,
            urlencoding::encode(id)
        ),
        None => format!("{}/me/calendar/calendarView", base),
    };
    let first = reqwest::Url::parse_with_params(
        &first,
        [
            ("startDateTime", rfc3339_z(window.start).as_str()),
            ("endDateTime", rfc3339_z(window.end).as_str()),
            ("$top", PAGE_SIZE),
            ("$select", SELECT),
        ],
    )
    .map_err(|e| CalendarError::Backend(format!("microsoft graph: bad API URL: {}", e)))?;
    let api_origin = first.origin();
    let context = "microsoft graph: calendarView";
    let mut events = Vec::new();
    let mut url = first;
    for _ in 0..MAX_PAGES {
        let http = session.http();
        let response = session
            .send(context, |token| {
                http.get(url.clone())
                    .bearer_auth(token)
                    .header("Prefer", "outlook.timezone=\"UTC\"")
            })
            .await?;
        let status = response.status();
        let body = read_body(response, MAX_BODY_BYTES, context).await?;
        if status != StatusCode::OK {
            return Err(oauth::api_error(context, status, &body));
        }
        let page: Page = serde_json::from_str(&body)
            .map_err(|e| CalendarError::Parse(format!("{} response: {}", context, e)))?;
        events.extend(
            page.value
                .into_iter()
                .filter_map(|e| to_event(e, calendar_id)),
        );
        let Some(next) = page.next_link else {
            return Ok(events);
        };
        let next = reqwest::Url::parse(&next).map_err(|e| {
            CalendarError::Backend(format!("{}: bad @odata.nextLink: {}", context, e))
        })?;
        // The access token must only ever go to the Graph API itself.
        if next.origin() != api_origin {
            return Err(CalendarError::Backend(format!(
                "{}: @odata.nextLink points outside the Graph API ({})",
                context,
                redact(&next)
            )));
        }
        url = next;
    }
    Err(CalendarError::Backend(format!(
        "{}: more than {} pages",
        context, MAX_PAGES
    )))
}

fn parse_utc(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|dt| dt.and_utc())
}

fn to_attendee(email: Option<EmailAddress>) -> Option<(Option<String>, String)> {
    let email = email?;
    Some((email.name, email.address.filter(|a| !a.is_empty())?))
}

/// Map one calendarView event; events without a parseable start are skipped.
fn to_event(e: Event, calendar_id: &str) -> Option<CalendarEvent> {
    let start_raw = e.start.as_ref()?.date_time.as_str();
    let end_raw = e.end.as_ref().map(|t| t.date_time.as_str());
    let (start, end) = if e.is_all_day {
        (
            date_at_midnight(start_raw)?,
            end_raw.and_then(date_at_midnight),
        )
    } else {
        (parse_utc(start_raw)?, end_raw.and_then(parse_utc))
    };
    let now = Utc::now();
    let attendees = e
        .attendees
        .into_iter()
        .filter_map(|a| {
            let (name, email) = to_attendee(a.email_address)?;
            let response = a.status.and_then(|s| s.response);
            Some(Attendee {
                name,
                email,
                status: match response.as_deref() {
                    Some("accepted" | "organizer") => AttendeeStatus::Accepted,
                    Some("declined") => AttendeeStatus::Declined,
                    Some("tentativelyAccepted") => AttendeeStatus::Tentative,
                    _ => AttendeeStatus::NeedsAction,
                },
                role: match a.kind.as_deref() {
                    Some("optional") => AttendeeRole::Optional,
                    Some("resource") => AttendeeRole::NonParticipant,
                    _ => AttendeeRole::Required,
                },
                organizer: response.as_deref() == Some("organizer"),
            })
        })
        .collect();
    let organizer = e
        .organizer
        .and_then(|o| to_attendee(o.email_address))
        .map(|(name, email)| Attendee {
            name,
            email,
            status: AttendeeStatus::Accepted,
            role: AttendeeRole::Chair,
            organizer: true,
        });
    let status = if e.is_cancelled {
        EventStatus::Cancelled
    } else if e.show_as.as_deref() == Some("tentative") {
        EventStatus::Tentative
    } else {
        EventStatus::Confirmed
    };
    Some(CalendarEvent {
        id: format!("{}:{}", calendar_id, e.id),
        calendar_id: calendar_id.to_string(),
        uid: e.ical_uid.unwrap_or_else(|| e.id.clone()),
        summary: e
            .subject
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| UNTITLED_EVENT.to_string()),
        description: e.body_preview.filter(|s| !s.is_empty()),
        location: e
            .location
            .and_then(|l| l.display_name)
            .filter(|s| !s.is_empty()),
        start,
        end: end_or_default(start, end, e.is_all_day),
        all_day: e.is_all_day,
        attendees,
        organizer,
        meeting_url: e
            .online_meeting
            .and_then(|m| m.join_url)
            .or(e.online_meeting_url),
        status,
        created: e
            .created_date_time
            .as_deref()
            .and_then(parse_utc)
            .unwrap_or(now),
        updated: e
            .last_modified_date_time
            .as_deref()
            .and_then(parse_utc)
            .unwrap_or(now),
        etag: e.etag,
    })
}
