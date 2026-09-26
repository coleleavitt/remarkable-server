//! Calendar integration module

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CalendarError {
    #[error("Calendar not found: {0}")]
    NotFound(String),
    #[error("Backend error: {0}")]
    Backend(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Auth required for {0}")]
    AuthRequired(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Network error: {0}")]
    Network(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CalendarError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CalendarProvider {
    Ics,
    Caldav,
    Google,
    Exchange,
    Office365,
}

impl std::fmt::Display for CalendarProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ics => write!(f, "ics"),
            Self::Caldav => write!(f, "caldav"),
            Self::Google => write!(f, "google"),
            Self::Exchange => write!(f, "exchange"),
            Self::Office365 => write!(f, "office365"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attendee {
    pub name: Option<String>,
    pub email: String,
    pub status: AttendeeStatus,
    pub role: AttendeeRole,
    #[serde(default)]
    pub organizer: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AttendeeStatus {
    #[default]
    NeedsAction,
    Accepted,
    Declined,
    Tentative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AttendeeRole {
    #[default]
    Required,
    Optional,
    Chair,
    NonParticipant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarEvent {
    pub id: String,
    pub calendar_id: String,
    pub uid: String,
    pub summary: String,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(default)]
    pub all_day: bool,
    pub attendees: Vec<Attendee>,
    pub organizer: Option<Attendee>,
    pub meeting_url: Option<String>,
    pub status: EventStatus,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum EventStatus {
    #[default]
    Confirmed,
    Tentative,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Calendar {
    pub id: String,
    pub name: String,
    pub color: Option<String>,
    pub provider: CalendarProvider,
    #[serde(default)]
    pub primary: bool,
    #[serde(default)]
    pub read_only: bool,
    pub sync_token: Option<String>,
    pub last_sync: Option<DateTime<Utc>>,
    pub config: CalendarConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CalendarConfig {
    Ics {
        path: PathBuf,
        #[serde(default)]
        watch: bool,
    },
    Caldav {
        url: String,
        username: String,
        #[serde(skip_serializing)]
        password: Option<String>,
    },
    Google {
        calendar_id: String,
        #[serde(skip_serializing)]
        access_token: Option<String>,
        #[serde(skip_serializing)]
        refresh_token: Option<String>,
    },
    Exchange {
        server: String,
        username: String,
        #[serde(skip_serializing)]
        password: Option<String>,
        #[serde(default)]
        use_ews: bool,
    },
    Office365 {
        tenant_id: String,
        #[serde(skip_serializing)]
        access_token: Option<String>,
        #[serde(skip_serializing)]
        refresh_token: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeetingNote {
    pub id: String,
    pub event_id: String,
    pub calendar_id: String,
    pub document_id: Option<String>,
    pub title: String,
    pub event_summary: String,
    pub event_start: DateTime<Utc>,
    pub event_end: DateTime<Utc>,
    pub attendees: Vec<Attendee>,
    pub location: Option<String>,
    pub meeting_url: Option<String>,
    pub created: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateMeetingNoteRequest {
    pub template_id: Option<String>,
    pub folder_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct EventQuery {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub expand_recurring: bool,
}

pub struct CalendarManager {
    db: Connection,
    calendars: Arc<RwLock<HashMap<String, Calendar>>>,
}

impl CalendarManager {
    pub fn new(db_path: &Path) -> Result<Self> {
        let db = Connection::open(db_path).map_err(|e| CalendarError::Database(e.to_string()))?;
        Self::init_schema(&db)?;
        let mut mgr = Self {
            db,
            calendars: Arc::new(RwLock::new(HashMap::new())),
        };
        mgr.load_calendars()?;
        Ok(mgr)
    }

    fn init_schema(db: &Connection) -> Result<()> {
        db.execute_batch(r#"
            CREATE TABLE IF NOT EXISTS calendars (id TEXT PRIMARY KEY, name TEXT NOT NULL, color TEXT, provider TEXT NOT NULL, is_primary INTEGER DEFAULT 0, read_only INTEGER DEFAULT 0, sync_token TEXT, last_sync TEXT, config TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, calendar_id TEXT NOT NULL, uid TEXT NOT NULL, summary TEXT NOT NULL, description TEXT, location TEXT, start_time TEXT NOT NULL, end_time TEXT NOT NULL, all_day INTEGER DEFAULT 0, attendees TEXT, organizer TEXT, meeting_url TEXT, status TEXT DEFAULT 'confirmed', created TEXT NOT NULL, updated TEXT NOT NULL, etag TEXT);
            CREATE TABLE IF NOT EXISTS meeting_notes (id TEXT PRIMARY KEY, event_id TEXT NOT NULL, calendar_id TEXT NOT NULL, document_id TEXT, title TEXT NOT NULL, event_summary TEXT NOT NULL, event_start TEXT NOT NULL, event_end TEXT NOT NULL, attendees TEXT, location TEXT, meeting_url TEXT, created TEXT NOT NULL);
        "#).map_err(|e| CalendarError::Database(e.to_string()))?;
        Ok(())
    }

    fn load_calendars(&mut self) -> Result<()> {
        let mut stmt = self.db.prepare("SELECT id, name, color, provider, is_primary, read_only, sync_token, last_sync, config FROM calendars").map_err(|e| CalendarError::Database(e.to_string()))?;
        let calendars = stmt
            .query_map([], |row| {
                let config_str: String = row.get(8)?;
                let config: CalendarConfig =
                    serde_json::from_str(&config_str).unwrap_or(CalendarConfig::Ics {
                        path: PathBuf::new(),
                        watch: false,
                    });
                let provider_str: String = row.get(3)?;
                let provider = match provider_str.as_str() {
                    "ics" => CalendarProvider::Ics,
                    "caldav" => CalendarProvider::Caldav,
                    "google" => CalendarProvider::Google,
                    "exchange" => CalendarProvider::Exchange,
                    _ => CalendarProvider::Ics,
                };
                Ok(Calendar {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    color: row.get(2)?,
                    provider,
                    primary: row.get::<_, i32>(4)? != 0,
                    read_only: row.get::<_, i32>(5)? != 0,
                    sync_token: row.get(6)?,
                    last_sync: row
                        .get::<_, Option<String>>(7)?
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc)),
                    config,
                })
            })
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        let mut cal_map = self.calendars.write();
        for cal in calendars.flatten() {
            cal_map.insert(cal.id.clone(), cal);
        }
        Ok(())
    }

    pub fn list_calendars(&self) -> Vec<Calendar> {
        self.calendars.read().values().cloned().collect()
    }
    pub fn get_calendar(&self, id: &str) -> Option<Calendar> {
        self.calendars.read().get(id).cloned()
    }

    pub fn add_calendar(&mut self, calendar: Calendar) -> Result<()> {
        let config_json = serde_json::to_string(&calendar.config)
            .map_err(|e| CalendarError::Parse(e.to_string()))?;
        self.db.execute("INSERT INTO calendars (id, name, color, provider, is_primary, read_only, sync_token, last_sync, config) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![calendar.id, calendar.name, calendar.color, calendar.provider.to_string(), calendar.primary as i32, calendar.read_only as i32, calendar.sync_token, calendar.last_sync.map(|dt| dt.to_rfc3339()), config_json]).map_err(|e| CalendarError::Database(e.to_string()))?;
        self.calendars.write().insert(calendar.id.clone(), calendar);
        Ok(())
    }

    pub fn remove_calendar(&mut self, id: &str) -> Result<bool> {
        let deleted = self
            .db
            .execute("DELETE FROM calendars WHERE id = ?1", params![id])
            .map_err(|e| CalendarError::Database(e.to_string()))?
            > 0;
        if deleted {
            self.calendars.write().remove(id);
        }
        Ok(deleted)
    }

    pub fn get_events(&self, calendar_id: &str, query: &EventQuery) -> Result<Vec<CalendarEvent>> {
        let _cal = self
            .calendars
            .read()
            .get(calendar_id)
            .cloned()
            .ok_or_else(|| CalendarError::NotFound(calendar_id.to_string()))?;
        let start = query.start.unwrap_or_else(Utc::now);
        let end = query.end.unwrap_or_else(|| start + Duration::days(30));
        let mut stmt = self.db.prepare("SELECT id, calendar_id, uid, summary, description, location, start_time, end_time, all_day, attendees, organizer, meeting_url, status, created, updated, etag FROM events WHERE calendar_id = ?1 AND start_time >= ?2 AND start_time <= ?3 ORDER BY start_time").map_err(|e| CalendarError::Database(e.to_string()))?;
        let events = stmt
            .query_map(
                params![calendar_id, start.to_rfc3339(), end.to_rfc3339()],
                Self::map_event,
            )
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        let mut result: Vec<_> = events.flatten().collect();
        if let Some(limit) = query.limit {
            result.truncate(limit);
        }
        Ok(result)
    }

    fn map_event(row: &rusqlite::Row) -> rusqlite::Result<CalendarEvent> {
        let attendees_str: Option<String> = row.get(9)?;
        let organizer_str: Option<String> = row.get(10)?;
        let status_str: String = row
            .get::<_, Option<String>>(12)?
            .unwrap_or_else(|| "confirmed".to_string());
        Ok(CalendarEvent {
            id: row.get(0)?,
            calendar_id: row.get(1)?,
            uid: row.get(2)?,
            summary: row.get(3)?,
            description: row.get(4)?,
            location: row.get(5)?,
            start: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            end: DateTime::parse_from_rfc3339(&row.get::<_, String>(7)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            all_day: row.get::<_, i32>(8)? != 0,
            attendees: attendees_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            organizer: organizer_str.and_then(|s| serde_json::from_str(&s).ok()),
            meeting_url: row.get(11)?,
            status: match status_str.as_str() {
                "tentative" => EventStatus::Tentative,
                "cancelled" => EventStatus::Cancelled,
                _ => EventStatus::Confirmed,
            },
            created: DateTime::parse_from_rfc3339(&row.get::<_, String>(13)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated: DateTime::parse_from_rfc3339(&row.get::<_, String>(14)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            etag: row.get(15)?,
        })
    }

    pub fn upsert_event(&mut self, event: &CalendarEvent) -> Result<()> {
        let attendees_json = serde_json::to_string(&event.attendees).ok();
        let organizer_json = event
            .organizer
            .as_ref()
            .and_then(|o| serde_json::to_string(o).ok());
        self.db.execute("INSERT OR REPLACE INTO events (id, calendar_id, uid, summary, description, location, start_time, end_time, all_day, attendees, organizer, meeting_url, status, created, updated, etag) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![event.id, event.calendar_id, event.uid, event.summary, event.description, event.location, event.start.to_rfc3339(), event.end.to_rfc3339(), event.all_day as i32, attendees_json, organizer_json, event.meeting_url, format!("{:?}", event.status).to_lowercase(), event.created.to_rfc3339(), event.updated.to_rfc3339(), event.etag]).map_err(|e| CalendarError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn create_meeting_note(
        &mut self,
        calendar_id: &str,
        event_id: &str,
        _request: &CreateMeetingNoteRequest,
    ) -> Result<MeetingNote> {
        let mut stmt = self.db.prepare("SELECT id, calendar_id, uid, summary, description, location, start_time, end_time, all_day, attendees, organizer, meeting_url, status, created, updated, etag FROM events WHERE id = ?1 AND calendar_id = ?2").map_err(|e| CalendarError::Database(e.to_string()))?;
        let event = stmt
            .query_row(params![event_id, calendar_id], Self::map_event)
            .map_err(|_| CalendarError::NotFound(format!("Event {} not found", event_id)))?;
        let note = MeetingNote {
            id: uuid::Uuid::new_v4().to_string(),
            event_id: event.id.clone(),
            calendar_id: event.calendar_id.clone(),
            document_id: None,
            title: format!("Meeting Notes: {}", event.summary),
            event_summary: event.summary,
            event_start: event.start,
            event_end: event.end,
            attendees: event.attendees,
            location: event.location,
            meeting_url: event.meeting_url,
            created: Utc::now(),
        };
        let attendees_json = serde_json::to_string(&note.attendees).ok();
        self.db.execute("INSERT INTO meeting_notes (id, event_id, calendar_id, document_id, title, event_summary, event_start, event_end, attendees, location, meeting_url, created) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![note.id, note.event_id, note.calendar_id, note.document_id, note.title, note.event_summary, note.event_start.to_rfc3339(), note.event_end.to_rfc3339(), attendees_json, note.location, note.meeting_url, note.created.to_rfc3339()]).map_err(|e| CalendarError::Database(e.to_string()))?;
        Ok(note)
    }

    pub fn get_meeting_notes(
        &self,
        calendar_id: &str,
        event_id: Option<&str>,
    ) -> Result<Vec<MeetingNote>> {
        let query = if event_id.is_some() {
            "SELECT id, event_id, calendar_id, document_id, title, event_summary, event_start, event_end, attendees, location, meeting_url, created FROM meeting_notes WHERE calendar_id = ?1 AND event_id = ?2 ORDER BY created DESC"
        } else {
            "SELECT id, event_id, calendar_id, document_id, title, event_summary, event_start, event_end, attendees, location, meeting_url, created FROM meeting_notes WHERE calendar_id = ?1 ORDER BY created DESC"
        };
        let mut stmt = self
            .db
            .prepare(query)
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        let notes = if let Some(eid) = event_id {
            stmt.query_map(params![calendar_id, eid], Self::map_meeting_note)
        } else {
            stmt.query_map(params![calendar_id], Self::map_meeting_note)
        }
        .map_err(|e| CalendarError::Database(e.to_string()))?;
        Ok(notes.flatten().collect())
    }

    fn map_meeting_note(row: &rusqlite::Row) -> rusqlite::Result<MeetingNote> {
        let attendees_str: Option<String> = row.get(8)?;
        Ok(MeetingNote {
            id: row.get(0)?,
            event_id: row.get(1)?,
            calendar_id: row.get(2)?,
            document_id: row.get(3)?,
            title: row.get(4)?,
            event_summary: row.get(5)?,
            event_start: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            event_end: DateTime::parse_from_rfc3339(&row.get::<_, String>(7)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            attendees: attendees_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            location: row.get(9)?,
            meeting_url: row.get(10)?,
            created: DateTime::parse_from_rfc3339(&row.get::<_, String>(11)?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
        })
    }
}

/// Parse ICS file
pub fn parse_ics_file(path: &Path, calendar_id: &str) -> Result<Vec<CalendarEvent>> {
    let content = std::fs::read_to_string(path)?;
    let mut events = Vec::new();
    let mut in_vevent = false;
    let (mut uid, mut summary, mut dtstart, mut dtend) = (None, None, None, None);
    for line in content.lines() {
        let line = line.trim();
        if line == "BEGIN:VEVENT" {
            in_vevent = true;
            uid = None;
            summary = None;
            dtstart = None;
            dtend = None;
        } else if line == "END:VEVENT" && in_vevent {
            if let (Some(u), Some(s), Some(start)) = (uid.take(), summary.take(), dtstart.take()) {
                let end = dtend.take().unwrap_or_else(|| start + Duration::hours(1));
                let now = Utc::now();
                events.push(CalendarEvent {
                    id: format!("{}:{}", calendar_id, u),
                    calendar_id: calendar_id.to_string(),
                    uid: u,
                    summary: s,
                    description: None,
                    location: None,
                    start,
                    end,
                    all_day: false,
                    attendees: Vec::new(),
                    organizer: None,
                    meeting_url: None,
                    status: EventStatus::Confirmed,
                    created: now,
                    updated: now,
                    etag: None,
                });
            }
            in_vevent = false;
        } else if in_vevent {
            if let Some((key, value)) = line.split_once(':') {
                let key_base = key.split(';').next().unwrap_or(key);
                match key_base {
                    "UID" => uid = Some(value.to_string()),
                    "SUMMARY" => summary = Some(value.to_string()),
                    "DTSTART" => dtstart = parse_ics_datetime(value),
                    "DTEND" => dtend = parse_ics_datetime(value),
                    _ => {}
                }
            }
        }
    }
    Ok(events)
}

fn parse_ics_datetime(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    if value.ends_with('Z') {
        chrono::NaiveDateTime::parse_from_str(&value[..value.len() - 1], "%Y%m%dT%H%M%S")
            .ok()
            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
    } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S") {
        Some(DateTime::from_naive_utc_and_offset(dt, Utc))
    } else if let Ok(d) = chrono::NaiveDate::parse_from_str(value, "%Y%m%d") {
        d.and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
    } else {
        None
    }
}

#[derive(Debug, Clone, Default)]
pub struct SyncConfig {
    pub poll_interval_secs: u64,
    pub webhook_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_provider_display() {
        assert_eq!(CalendarProvider::Google.to_string(), "google");
    }
}
