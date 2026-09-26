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

/// Where a calendar's events come from.
///
/// Credential fields are `skip_serializing`, so they never appear in the `config` column or in
/// anything serialized for clients; [`CalendarManager`] persists them separately in the
/// `secrets` column (see [`CalendarSecrets`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CalendarConfig {
    Ics {
        path: PathBuf,
        #[serde(default)]
        watch: bool,
    },
    /// CalDAV (RFC 4791). `url` is the calendar collection itself, or any URL discovery can
    /// start from (server root, principal or calendar home).
    Caldav {
        url: String,
        /// Basic-auth user; empty when only a bearer token is used.
        #[serde(default)]
        username: String,
        #[serde(skip_serializing)]
        password: Option<String>,
        /// Sent as `Authorization: Bearer` instead of basic auth when set.
        #[serde(default, skip_serializing)]
        bearer_token: Option<String>,
        /// Calendar collection found by discovery on the first sync, reused afterwards.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collection_url: Option<String>,
    },
    /// Google Calendar API v3 with OAuth 2.0 tokens obtained out of band.
    Google {
        calendar_id: String,
        #[serde(skip_serializing)]
        access_token: Option<String>,
        #[serde(skip_serializing)]
        refresh_token: Option<String>,
        /// OAuth client the refresh token was issued to; needed to refresh the access token.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        #[serde(default, skip_serializing)]
        client_secret: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_expires_at: Option<DateTime<Utc>>,
    },
    /// On-premises Exchange via EWS. Not synced: EWS is not implemented. Exchange Online
    /// mailboxes sync through Microsoft Graph with an `office365` config (the provider may
    /// still be `exchange`).
    Exchange {
        server: String,
        username: String,
        #[serde(skip_serializing)]
        password: Option<String>,
        #[serde(default)]
        use_ews: bool,
    },
    /// Microsoft Graph (Exchange Online / Microsoft 365) with OAuth 2.0 tokens obtained out
    /// of band.
    Office365 {
        /// Azure AD tenant for token refresh (`common`/`organizations` work too).
        tenant_id: String,
        #[serde(skip_serializing)]
        access_token: Option<String>,
        #[serde(skip_serializing)]
        refresh_token: Option<String>,
        /// Application (client) id the refresh token was issued to.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        /// Only for confidential (web) app registrations.
        #[serde(default, skip_serializing)]
        client_secret: Option<String>,
        /// Graph calendar id; the user's default calendar when unset.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        calendar_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_expires_at: Option<DateTime<Utc>>,
    },
}

/// Credential fields of a [`CalendarConfig`], stored as JSON in the `secrets` column of
/// `calendars`. Never part of the `config` column or of any API response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct CalendarSecrets {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bearer_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_secret: Option<String>,
}

impl CalendarConfig {
    fn secrets(&self) -> CalendarSecrets {
        match self {
            Self::Ics { .. } => CalendarSecrets::default(),
            Self::Caldav {
                password,
                bearer_token,
                ..
            } => CalendarSecrets {
                password: password.clone(),
                bearer_token: bearer_token.clone(),
                ..Default::default()
            },
            Self::Exchange { password, .. } => CalendarSecrets {
                password: password.clone(),
                ..Default::default()
            },
            Self::Google {
                access_token,
                refresh_token,
                client_secret,
                ..
            }
            | Self::Office365 {
                access_token,
                refresh_token,
                client_secret,
                ..
            } => CalendarSecrets {
                access_token: access_token.clone(),
                refresh_token: refresh_token.clone(),
                client_secret: client_secret.clone(),
                ..Default::default()
            },
        }
    }

    /// Fill credential fields from stored secrets; a secret absent from the store leaves the
    /// field as deserialized.
    fn apply_secrets(&mut self, s: CalendarSecrets) {
        fn set(field: &mut Option<String>, v: Option<String>) {
            if v.is_some() {
                *field = v;
            }
        }
        match self {
            Self::Ics { .. } => {}
            Self::Caldav {
                password,
                bearer_token,
                ..
            } => {
                set(password, s.password);
                set(bearer_token, s.bearer_token);
            }
            Self::Exchange { password, .. } => set(password, s.password),
            Self::Google {
                access_token,
                refresh_token,
                client_secret,
                ..
            }
            | Self::Office365 {
                access_token,
                refresh_token,
                client_secret,
                ..
            } => {
                set(access_token, s.access_token);
                set(refresh_token, s.refresh_token);
                set(client_secret, s.client_secret);
            }
        }
    }

    /// `(config, secrets)` column values: the public config JSON and the credentials JSON
    /// (`None` when there are no credentials).
    fn to_columns(&self) -> Result<(String, Option<String>)> {
        let config =
            serde_json::to_string(self).map_err(|e| CalendarError::Parse(e.to_string()))?;
        let secrets = self.secrets();
        let secrets = if secrets == CalendarSecrets::default() {
            None
        } else {
            Some(serde_json::to_string(&secrets).map_err(|e| CalendarError::Parse(e.to_string()))?)
        };
        Ok((config, secrets))
    }
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
        // Provider credentials (JSON `CalendarSecrets`), kept out of `config` so they survive
        // restarts without being part of anything sent to clients.
        Self::ensure_column(db, "calendars", "secrets", "TEXT")?;
        Ok(())
    }

    /// `ALTER TABLE ... ADD COLUMN` unless the column already exists, so databases created by
    /// older versions are migrated in place.
    fn ensure_column(db: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
        let mut stmt = db
            .prepare(&format!("PRAGMA table_info({})", table))
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        let exists = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| CalendarError::Database(e.to_string()))?
            .filter_map(|name| name.ok())
            .any(|name| name == column);
        if !exists {
            db.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                table, column, decl
            ))
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        }
        Ok(())
    }

    fn load_calendars(&mut self) -> Result<()> {
        let mut stmt = self.db.prepare("SELECT id, name, color, provider, is_primary, read_only, sync_token, last_sync, config, secrets FROM calendars").map_err(|e| CalendarError::Database(e.to_string()))?;
        let calendars = stmt
            .query_map([], |row| {
                let config_str: String = row.get(8)?;
                let mut config: CalendarConfig =
                    serde_json::from_str(&config_str).unwrap_or(CalendarConfig::Ics {
                        path: PathBuf::new(),
                        watch: false,
                    });
                let secrets: Option<String> = row.get(9)?;
                if let Some(secrets) = secrets.and_then(|s| serde_json::from_str(&s).ok()) {
                    config.apply_secrets(secrets);
                }
                let provider_str: String = row.get(3)?;
                let provider = match provider_str.as_str() {
                    "ics" => CalendarProvider::Ics,
                    "caldav" => CalendarProvider::Caldav,
                    "google" => CalendarProvider::Google,
                    "exchange" => CalendarProvider::Exchange,
                    "office365" => CalendarProvider::Office365,
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
        let (config_json, secrets_json) = calendar.config.to_columns()?;
        self.db.execute("INSERT INTO calendars (id, name, color, provider, is_primary, read_only, sync_token, last_sync, config, secrets) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![calendar.id, calendar.name, calendar.color, calendar.provider.to_string(), calendar.primary as i32, calendar.read_only as i32, calendar.sync_token, calendar.last_sync.map(|dt| dt.to_rfc3339()), config_json, secrets_json]).map_err(|e| CalendarError::Database(e.to_string()))?;
        self.calendars.write().insert(calendar.id.clone(), calendar);
        Ok(())
    }

    /// Replace a calendar's config and credentials, e.g. after an OAuth token refresh or
    /// CalDAV collection discovery.
    pub fn update_config(&mut self, id: &str, config: CalendarConfig) -> Result<()> {
        let (config_json, secrets_json) = config.to_columns()?;
        let updated = self
            .db
            .execute(
                "UPDATE calendars SET config = ?1, secrets = ?2 WHERE id = ?3",
                params![config_json, secrets_json, id],
            )
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        if updated == 0 {
            return Err(CalendarError::NotFound(id.to_string()));
        }
        if let Some(cal) = self.calendars.write().get_mut(id) {
            cal.config = config;
        }
        Ok(())
    }

    pub fn set_last_sync(&mut self, id: &str, at: DateTime<Utc>) -> Result<()> {
        self.db
            .execute(
                "UPDATE calendars SET last_sync = ?1 WHERE id = ?2",
                params![at.to_rfc3339(), id],
            )
            .map_err(|e| CalendarError::Database(e.to_string()))?;
        if let Some(cal) = self.calendars.write().get_mut(id) {
            cal.last_sync = Some(at);
        }
        Ok(())
    }

    /// Store a complete remote snapshot of `[start, end]` in one transaction: upsert `events`,
    /// then delete this calendar's stored events starting in that range that the snapshot no
    /// longer has (deleted or moved away upstream). Returns `(stored, removed)`.
    pub fn replace_events_in_range(
        &mut self,
        calendar_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        events: &[CalendarEvent],
    ) -> Result<(usize, usize)> {
        // The calendar may have been deleted while its events were being fetched.
        if !self.calendars.read().contains_key(calendar_id) {
            return Err(CalendarError::NotFound(calendar_id.to_string()));
        }
        let db_err = |e: rusqlite::Error| CalendarError::Database(e.to_string());
        let tx = self.db.transaction().map_err(db_err)?;
        let mut keep = std::collections::HashSet::new();
        for event in events {
            upsert_event_in(&tx, event)?;
            keep.insert(event.id.as_str());
        }
        let stale: Vec<String> = {
            let mut stmt = tx
                .prepare(
                    "SELECT id FROM events WHERE calendar_id = ?1 AND start_time >= ?2 AND start_time <= ?3",
                )
                .map_err(db_err)?;
            let ids = stmt
                .query_map(
                    params![calendar_id, start.to_rfc3339(), end.to_rfc3339()],
                    |row| row.get::<_, String>(0),
                )
                .map_err(db_err)?;
            ids.filter_map(|id| id.ok())
                .filter(|id| !keep.contains(id.as_str()))
                .collect()
        };
        for id in &stale {
            tx.execute("DELETE FROM events WHERE id = ?1", params![id])
                .map_err(db_err)?;
        }
        tx.commit().map_err(db_err)?;
        Ok((keep.len(), stale.len()))
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
        upsert_event_in(&self.db, event)
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

fn upsert_event_in(db: &Connection, event: &CalendarEvent) -> Result<()> {
    let attendees_json = serde_json::to_string(&event.attendees).ok();
    let organizer_json = event
        .organizer
        .as_ref()
        .and_then(|o| serde_json::to_string(o).ok());
    db.execute("INSERT OR REPLACE INTO events (id, calendar_id, uid, summary, description, location, start_time, end_time, all_day, attendees, organizer, meeting_url, status, created, updated, etag) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![event.id, event.calendar_id, event.uid, event.summary, event.description, event.location, event.start.to_rfc3339(), event.end.to_rfc3339(), event.all_day as i32, attendees_json, organizer_json, event.meeting_url, format!("{:?}", event.status).to_lowercase(), event.created.to_rfc3339(), event.updated.to_rfc3339(), event.etag]).map_err(|e| CalendarError::Database(e.to_string()))?;
    Ok(())
}

/// Parse ICS file
pub fn parse_ics_file(path: &Path, calendar_id: &str) -> Result<Vec<CalendarEvent>> {
    Ok(parse_ics_str(&std::fs::read_to_string(path)?, calendar_id))
}

/// Undo RFC 5545 section 3.1 line folding: a line starting with a space or tab continues the
/// previous line, minus that one leading whitespace character.
fn unfold_ics_lines(content: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for raw in content.lines() {
        match (
            raw.strip_prefix(' ').or_else(|| raw.strip_prefix('\t')),
            lines.last_mut(),
        ) {
            (Some(rest), Some(prev)) => prev.push_str(rest),
            _ => lines.push(raw.to_string()),
        }
    }
    lines
}

/// Properties of the VEVENT being parsed.
#[derive(Default)]
struct VEventFields {
    uid: Option<String>,
    summary: Option<String>,
    description: Option<String>,
    location: Option<String>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    duration: Option<Duration>,
    all_day: bool,
    recurrence_id: Option<String>,
    status: EventStatus,
}

impl VEventFields {
    fn set(&mut self, key: &str, value: &str) {
        let name = key.split(';').next().unwrap_or(key).to_ascii_uppercase();
        match name.as_str() {
            "UID" => self.uid = Some(value.to_string()),
            "SUMMARY" => self.summary = Some(unescape_ics_text(value)),
            "DESCRIPTION" => self.description = Some(unescape_ics_text(value)),
            "LOCATION" => self.location = Some(unescape_ics_text(value)),
            "DTSTART" => {
                self.start = parse_ics_datetime(value);
                self.all_day = is_ics_date_only(key, value);
            }
            "DTEND" => self.end = parse_ics_datetime(value),
            "DURATION" => self.duration = parse_ics_duration(value),
            "RECURRENCE-ID" => self.recurrence_id = Some(value.trim().to_string()),
            "STATUS" => {
                self.status = match value.trim().to_ascii_uppercase().as_str() {
                    "CANCELLED" => EventStatus::Cancelled,
                    "TENTATIVE" => EventStatus::Tentative,
                    _ => EventStatus::Confirmed,
                }
            }
            _ => {}
        }
    }

    /// The event, if it has the required UID and DTSTART.
    fn into_event(self, calendar_id: &str) -> Option<CalendarEvent> {
        let (uid, start) = (self.uid?, self.start?);
        let default_length = if self.all_day {
            Duration::days(1)
        } else {
            Duration::hours(1)
        };
        // A malformed end before the start (or a negative DURATION) is ignored.
        let end = self
            .end
            .or_else(|| start.checked_add_signed(self.duration?))
            .filter(|end| *end >= start)
            .or_else(|| start.checked_add_signed(default_length))
            .unwrap_or(start);
        // Each occurrence of a recurring event (RECURRENCE-ID, e.g. from a CalDAV `expand`)
        // shares the series UID, so the occurrence is part of the id.
        let id = match &self.recurrence_id {
            Some(rid) => format!("{}:{}:{}", calendar_id, uid, rid),
            None => format!("{}:{}", calendar_id, uid),
        };
        let now = Utc::now();
        Some(CalendarEvent {
            id,
            calendar_id: calendar_id.to_string(),
            uid,
            summary: self.summary.unwrap_or_else(|| UNTITLED_EVENT.to_string()),
            description: self.description,
            location: self.location,
            start,
            end,
            all_day: self.all_day,
            attendees: Vec::new(),
            organizer: None,
            meeting_url: None,
            status: self.status,
            created: now,
            updated: now,
            etag: None,
        })
    }
}

/// Summary for events that have none (untitled events are valid in every provider).
pub const UNTITLED_EVENT: &str = "(no title)";

/// Parse VEVENTs from ICS text. A DTSTART with `VALUE=DATE` or a bare 8-digit date marks an
/// all-day event. Properties of components nested in a VEVENT (e.g. VALARM) are ignored.
pub fn parse_ics_str(content: &str, calendar_id: &str) -> Vec<CalendarEvent> {
    let mut events = Vec::new();
    let mut current: Option<VEventFields> = None;
    let mut nested = 0usize;
    for line in unfold_ics_lines(content) {
        let line = line.trim();
        let Some((key, value)) = split_ics_property(line) else {
            continue;
        };
        let key_upper = key.to_ascii_uppercase();
        let value_upper = value.trim().to_ascii_uppercase();
        match (key_upper.as_str(), value_upper.as_str(), current.as_mut()) {
            ("BEGIN", "VEVENT", None) => {
                current = Some(VEventFields::default());
                nested = 0;
            }
            ("END", "VEVENT", Some(_)) if nested == 0 => {
                events.extend(current.take().and_then(|f| f.into_event(calendar_id)));
            }
            ("BEGIN", _, Some(_)) => nested += 1,
            ("END", _, Some(_)) => nested = nested.saturating_sub(1),
            (_, _, Some(fields)) if nested == 0 => fields.set(key, value),
            _ => {}
        }
    }
    events
}

/// Split a content line into `(name;params, value)` at the first colon outside a quoted
/// parameter value (`DESCRIPTION;ALTREP="cid:x":text`).
fn split_ics_property(line: &str) -> Option<(&str, &str)> {
    let mut quoted = false;
    for (i, c) in line.char_indices() {
        match c {
            '"' => quoted = !quoted,
            ':' if !quoted => return Some((&line[..i], &line[i + 1..])),
            _ => {}
        }
    }
    None
}

/// Undo RFC 5545 TEXT escaping (`\n`, `\,`, `\;`, `\\`).
fn unescape_ics_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n' | 'N') => out.push('\n'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

/// RFC 5545 section 3.3.6 duration such as `PT1H30M`, `P1D` or `-P2W`.
fn parse_ics_duration(value: &str) -> Option<Duration> {
    let value = value.trim();
    let (negative, rest) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let rest = rest.strip_prefix('P')?;
    let mut total = Duration::zero();
    let mut digits = String::new();
    let mut in_time = false;
    let mut any = false;
    for c in rest.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        if c == 'T' && digits.is_empty() && !in_time {
            in_time = true;
            continue;
        }
        let n: i64 = digits.parse().ok()?;
        digits.clear();
        let part = match (c, in_time) {
            ('W', false) => Duration::try_weeks(n),
            ('D', false) => Duration::try_days(n),
            ('H', true) => Duration::try_hours(n),
            ('M', true) => Duration::try_minutes(n),
            ('S', true) => Duration::try_seconds(n),
            _ => None,
        }?;
        total = total.checked_add(&part)?;
        any = true;
    }
    if !any || !digits.is_empty() {
        return None;
    }
    Some(if negative { -total } else { total })
}

/// True for `DTSTART;VALUE=DATE:...` or a value that is exactly an 8-digit `YYYYMMDD` date.
fn is_ics_date_only(key: &str, value: &str) -> bool {
    let value = value.trim();
    key.split(';')
        .skip(1)
        .any(|p| p.eq_ignore_ascii_case("VALUE=DATE"))
        || (value.len() == 8 && value.bytes().all(|b| b.is_ascii_digit()))
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

    #[test]
    fn office365_provider_round_trips_through_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cal.db");
        let mut mgr = CalendarManager::new(&db).unwrap();
        let config = CalendarConfig::Office365 {
            tenant_id: "t".into(),
            access_token: None,
            refresh_token: None,
            client_id: None,
            client_secret: None,
            calendar_id: None,
            token_expires_at: None,
        };
        mgr.add_calendar(Calendar {
            id: "c1".into(),
            name: "Work".into(),
            color: None,
            provider: CalendarProvider::Office365,
            primary: false,
            read_only: false,
            sync_token: None,
            last_sync: None,
            config,
        })
        .unwrap();
        drop(mgr);
        let cal = CalendarManager::new(&db)
            .unwrap()
            .get_calendar("c1")
            .unwrap();
        assert_eq!(cal.provider, CalendarProvider::Office365);
        assert!(matches!(cal.config, CalendarConfig::Office365 { .. }));
    }

    #[test]
    fn date_only_events_are_all_day() {
        let ics = "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:a\nSUMMARY:Holiday\nDTSTART;VALUE=DATE:20250704\nDTEND;VALUE=DATE:20250705\nEND:VEVENT\nBEGIN:VEVENT\nUID:b\nSUMMARY:Bare date\nDTSTART:20250801\nEND:VEVENT\nBEGIN:VEVENT\nUID:c\nSUMMARY:Meeting\nDTSTART:20250801T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:d\nSUMMARY:Explicit datetime\nDTSTART;VALUE=DATE-TIME:20250801T090000\nEND:VEVENT\nEND:VCALENDAR\n";
        let events = parse_ics_str(ics, "cal");
        let by_uid = |u: &str| events.iter().find(|e| e.uid == u).unwrap();
        assert!(by_uid("a").all_day);
        assert_eq!(by_uid("a").end - by_uid("a").start, Duration::days(1));
        assert!(by_uid("b").all_day);
        assert_eq!(by_uid("b").end - by_uid("b").start, Duration::days(1));
        assert!(!by_uid("c").all_day);
        assert_eq!(by_uid("c").end - by_uid("c").start, Duration::hours(1));
        assert!(!by_uid("d").all_day);
    }

    #[test]
    fn folded_lines_are_unfolded() {
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:long-\r\n uid@example.com\r\nSUMMARY:Quarterly planning\r\n\t review: part 2\r\nDTSTART:20250801T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let events = parse_ics_str(ics, "cal");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].uid, "long-uid@example.com");
        assert_eq!(events[0].summary, "Quarterly planning review: part 2");
    }

    fn calendar(id: &str, config: CalendarConfig) -> Calendar {
        Calendar {
            id: id.into(),
            name: id.into(),
            color: None,
            provider: CalendarProvider::Caldav,
            primary: false,
            read_only: false,
            sync_token: None,
            last_sync: None,
            config,
        }
    }

    fn raw_columns(db: &Path, id: &str) -> (String, Option<String>) {
        Connection::open(db)
            .unwrap()
            .query_row(
                "SELECT config, secrets FROM calendars WHERE id = ?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
    }

    #[test]
    fn old_schema_db_is_migrated_and_credentials_persist() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("calendars.db");
        {
            // Schema and row exactly as written before the `secrets` column existed (the
            // password was `skip_serializing`, so it never reached the database).
            let old = Connection::open(&db).unwrap();
            old.execute_batch(r#"
                CREATE TABLE calendars (id TEXT PRIMARY KEY, name TEXT NOT NULL, color TEXT, provider TEXT NOT NULL, is_primary INTEGER DEFAULT 0, read_only INTEGER DEFAULT 0, sync_token TEXT, last_sync TEXT, config TEXT NOT NULL);
                CREATE TABLE events (id TEXT PRIMARY KEY, calendar_id TEXT NOT NULL, uid TEXT NOT NULL, summary TEXT NOT NULL, description TEXT, location TEXT, start_time TEXT NOT NULL, end_time TEXT NOT NULL, all_day INTEGER DEFAULT 0, attendees TEXT, organizer TEXT, meeting_url TEXT, status TEXT DEFAULT 'confirmed', created TEXT NOT NULL, updated TEXT NOT NULL, etag TEXT);
                INSERT INTO calendars (id, name, provider, config) VALUES ('old', 'Old', 'caldav', '{"type":"caldav","url":"https://dav.example/","username":"u"}');
                INSERT INTO calendars (id, name, provider, config) VALUES ('g', 'Old Google', 'google', '{"type":"google","calendar_id":"primary"}');
            "#).unwrap();
        }
        let mut mgr = CalendarManager::new(&db).unwrap();
        let cal = mgr.get_calendar("old").unwrap();
        assert_eq!(
            cal.config,
            CalendarConfig::Caldav {
                url: "https://dav.example/".into(),
                username: "u".into(),
                password: None,
                bearer_token: None,
                collection_url: None,
            }
        );
        assert!(matches!(
            mgr.get_calendar("g").unwrap().config,
            CalendarConfig::Google {
                client_id: None,
                token_expires_at: None,
                ..
            }
        ));
        let mut config = cal.config;
        if let CalendarConfig::Caldav {
            password,
            collection_url,
            ..
        } = &mut config
        {
            *password = Some("hunter2".into());
            *collection_url = Some("https://dav.example/cal/u/work/".into());
        }
        mgr.update_config("old", config.clone()).unwrap();
        assert!(matches!(
            mgr.update_config("missing", config.clone()),
            Err(CalendarError::NotFound(_))
        ));
        drop(mgr);

        // Re-opening runs the migration again without error and restores the password.
        let mgr = CalendarManager::new(&db).unwrap();
        assert_eq!(mgr.get_calendar("old").unwrap().config, config);
        let (config_json, secrets_json) = raw_columns(&db, "old");
        assert!(!config_json.contains("hunter2"), "{}", config_json);
        assert!(config_json.contains("https://dav.example/cal/u/work/"));
        assert_eq!(secrets_json.as_deref(), Some(r#"{"password":"hunter2"}"#));
        // Calendars without credentials store no secrets at all.
        assert_eq!(raw_columns(&db, "g").1, None);
    }

    #[test]
    fn oauth_secrets_round_trip_outside_config_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("calendars.db");
        let expires = Utc::now() + Duration::hours(1);
        let config = CalendarConfig::Google {
            calendar_id: "primary".into(),
            access_token: Some("at-secret".into()),
            refresh_token: Some("rt-secret".into()),
            client_id: Some("client.apps.googleusercontent.com".into()),
            client_secret: Some("cs-secret".into()),
            token_expires_at: Some(expires),
        };
        CalendarManager::new(&db)
            .unwrap()
            .add_calendar(calendar("g", config.clone()))
            .unwrap();
        let loaded = CalendarManager::new(&db)
            .unwrap()
            .get_calendar("g")
            .unwrap();
        assert_eq!(loaded.config, config);
        let (config_json, secrets_json) = raw_columns(&db, "g");
        for secret in ["at-secret", "rt-secret", "cs-secret"] {
            assert!(!config_json.contains(secret), "{}", config_json);
            assert!(secrets_json.as_deref().unwrap().contains(secret));
            // Serializing a calendar (what any client-facing JSON would be built from)
            // never includes credentials either.
            assert!(!serde_json::to_string(&loaded).unwrap().contains(secret));
        }
    }

    #[test]
    fn replace_events_in_range_prunes_only_inside_the_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = CalendarManager::new(&dir.path().join("calendars.db")).unwrap();
        mgr.add_calendar(calendar(
            "c",
            CalendarConfig::Ics {
                path: PathBuf::new(),
                watch: false,
            },
        ))
        .unwrap();
        let base = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let event = |uid: &str, days: i64| {
            let mut e = parse_ics_str(
                &format!(
                    "BEGIN:VEVENT\nUID:{}\nSUMMARY:{}\nDTSTART:{}\nEND:VEVENT\n",
                    uid,
                    uid,
                    (base + Duration::days(days)).format("%Y%m%dT%H%M%SZ")
                ),
                "c",
            );
            e.pop().unwrap()
        };
        for e in [event("kept", 1), event("gone", 2), event("outside", 100)] {
            mgr.upsert_event(&e).unwrap();
        }
        let (stored, removed) = mgr
            .replace_events_in_range(
                "c",
                base,
                base + Duration::days(10),
                &[event("kept", 1), event("new", 3), event("new", 3)],
            )
            .unwrap();
        assert_eq!((stored, removed), (2, 1));
        let all = mgr
            .get_events(
                "c",
                &EventQuery {
                    start: Some(base - Duration::days(1)),
                    end: Some(base + Duration::days(365)),
                    ..Default::default()
                },
            )
            .unwrap();
        let uids: Vec<_> = all.iter().map(|e| e.uid.as_str()).collect();
        assert_eq!(uids, ["kept", "new", "outside"]);
        assert!(matches!(
            mgr.replace_events_in_range("deleted", base, base, &[]),
            Err(CalendarError::NotFound(_))
        ));
    }

    #[test]
    fn caldav_style_components_parse() {
        // Expanded recurrence (shared UID, distinct RECURRENCE-ID), a VALARM whose
        // DESCRIPTION must not leak into the event, TEXT escapes, DURATION, STATUS, a quoted
        // parameter containing a colon, and an untitled event.
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID:20260105T090000Z\r\nSUMMARY:Standup\\, daily\r\nDTSTART:20260105T090000Z\r\nDURATION:PT15M\r\nDESCRIPTION;ALTREP=\"cid:part1@example.org\":Line one\\nLine two\\; done\r\nLOCATION:Room 1\r\nSTATUS:TENTATIVE\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nSUMMARY:Alarm\r\nTRIGGER:-PT5M\r\nEND:VALARM\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID:20260106T090000Z\r\nSUMMARY:Standup\r\nDTSTART:20260106T090000Z\r\nDTEND:20260106T091500Z\r\nstatus:cancelled\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:untitled\r\nDTSTART;VALUE=DATE:20260107\r\nDURATION:P2D\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let events = parse_ics_str(ics, "cal");
        assert_eq!(events.len(), 3);
        let first = &events[0];
        assert_eq!(first.id, "cal:series:20260105T090000Z");
        assert_eq!(first.summary, "Standup, daily");
        assert_eq!(
            first.description.as_deref(),
            Some("Line one\nLine two; done")
        );
        assert_eq!(first.location.as_deref(), Some("Room 1"));
        assert_eq!(first.end - first.start, Duration::minutes(15));
        assert_eq!(first.status, EventStatus::Tentative);
        assert_eq!(events[1].id, "cal:series:20260106T090000Z");
        assert_eq!(events[1].status, EventStatus::Cancelled);
        assert_eq!(events[2].summary, UNTITLED_EVENT);
        assert!(events[2].all_day);
        assert_eq!(events[2].end - events[2].start, Duration::days(2));
    }

    #[test]
    fn ics_durations() {
        assert_eq!(parse_ics_duration("PT1H30M"), Some(Duration::minutes(90)));
        assert_eq!(parse_ics_duration("P1W"), Some(Duration::weeks(1)));
        assert_eq!(
            parse_ics_duration("-P1DT2S"),
            Some(-(Duration::days(1) + Duration::seconds(2)))
        );
        for bad in [
            "",
            "P",
            "PT",
            "1H",
            "P1H",
            "PT1D",
            "P1",
            "PT99999999999999999999H",
        ] {
            assert_eq!(parse_ics_duration(bad), None, "{}", bad);
        }
    }

    proptest::proptest! {
        #[test]
        fn ics_parser_never_panics(s in "(BEGIN:VEVENT|END:VEVENT|UID:x|DTSTART:[0-9TZ]{0,16}|DTEND:[0-9TZ]{0,16}|DURATION:[-+PTWDHMS0-9]{0,12}|RECURRENCE-ID:[0-9]{0,8}|[ -~]{0,20}|\r?\n){0,40}") {
            for e in parse_ics_str(&s, "c") {
                proptest::prop_assert!(e.end >= e.start);
            }
        }
    }
}
