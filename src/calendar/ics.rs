//! iCalendar (RFC 5545) VEVENT parsing, for ICS files and CalDAV calendar data.
//!
//! `TZID=` times are converted to UTC with the VTIMEZONE components of the same object (see
//! [`super::timezone`]). A DATE value makes an all-day event at midnight UTC. Properties of
//! components nested in a VEVENT (VALARM) are ignored.
//!
//! [`parse_ics_str`] yields one event per VEVENT, a recurring event as its first occurrence.
//! [`parse_ics_expanded`] expands recurring events into their occurrences in a time window,
//! for CalDAV servers that return the master with its RRULE (or its list of RDATEs) instead of
//! expanding it. [`parse_ics_server_expanded`] reads an answer the server was asked to expand,
//! and names the data it cannot trust to be whole.
//!
//! Both are budgeted, so hostile calendar data costs bounded CPU and memory. A time whose
//! VTIMEZONE rules are too costly to go through (or whose TZID is too long to look up) is
//! never guessed: its event is left out, and the caller learns that the result is incomplete.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::rc::Rc;

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use super::recurrence::Rule;
use super::timezone::{Observance, Zone};
use super::{CalendarEvent, EventStatus, Result};

/// Summary for events that have none (untitled events are valid in every provider).
pub const UNTITLED_EVENT: &str = "(no title)";

/// Steps the recurrence rules of one CalDAV answer may take in all (see
/// [`Rule::walk`]: each period walked, each `BY` list entry it goes through and each date it
/// yields). A usual rule costs two to four steps a period. Rules skip to the window unless
/// they have a COUNT, so this is only reached by thousands of long-running COUNT rules, or by
/// a hostile server.
const MAX_RULE_STEPS: usize = 6_000_000;
/// Events one CalDAV answer may hold in all, expanded occurrences and events sent as they are
/// alike: some 250 daily series over the sync window. Each costs a few hundred bytes besides
/// its text, which [`MAX_EXPANDED_BYTES`] bounds.
const MAX_INSTANCES: usize = 100_000;
/// Bytes of text (ids, summaries, descriptions, locations, etags) the events of one CalDAV
/// answer may hold in all. Every occurrence carries a copy of its series' text (and of its
/// resource's etag), so without this a long DESCRIPTION would be multiplied by the number of
/// occurrences.
const MAX_EXPANDED_BYTES: usize = 64 << 20;
/// Steps the VTIMEZONE lookups of one CalDAV answer (or one ICS file) may take in all: one
/// per observance, plus the steps of walking its rule. A lookup in a usual zone costs about
/// 40, so this covers some 300,000 converted times; a hostile zone runs out of it instead of
/// the CPU.
const MAX_ZONE_STEPS: usize = 12_000_000;
/// Longest TZID a time is converted in. Real ones (Olson names, Outlook's display names, the
/// prefixed names of older exporters) are well under 100 bytes. Each conversion looks the
/// name up, and one without a VTIMEZONE is also noted, which takes time in proportion to its
/// length; values of one RDATE or EXDATE line share their TZID, so a long name on a long list
/// would cost hours of CPU without adding to any budget. A time in a longer one is treated as
/// one whose zone is too costly to go through: left out, and the result marked incomplete.
const MAX_TZID_BYTES: usize = 255;

/// TZIDs that name UTC itself, for calendar data that uses them without a VTIMEZONE.
const UTC_NAMES: &[&str] = &[
    "UTC",
    "Etc/UTC",
    "GMT",
    "Etc/GMT",
    "UCT",
    "Etc/UCT",
    "Universal",
    "Etc/Universal",
    "Zulu",
    "Z",
    "Coordinated Universal Time",
];

/// The events of an ICS text.
#[derive(Debug)]
pub struct IcsEvents {
    pub events: Vec<CalendarEvent>,
    /// Whether events are missing because the text's VTIMEZONE rules were too costly to go
    /// through, or a TZID too long to look up, so their `TZID=` times could not be converted
    /// (see [`MAX_ZONE_STEPS`] and [`MAX_TZID_BYTES`]).
    pub incomplete: bool,
}

/// Parse ICS file
pub fn parse_ics_file(path: &Path, calendar_id: &str) -> Result<IcsEvents> {
    Ok(parse_ics_str(&std::fs::read_to_string(path)?, calendar_id))
}

/// Parse VEVENTs from ICS text: one event per VEVENT, a recurring event as its first
/// occurrence.
pub fn parse_ics_str(content: &str, calendar_id: &str) -> IcsEvents {
    let doc = Document::parse(content, usize::MAX);
    let events = doc
        .events
        .iter()
        .filter_map(|e| e.event(calendar_id, &doc.zones))
        .collect();
    let unknown = doc.zones.unknown.into_inner();
    if !unknown.is_empty() {
        tracing::warn!(
            "calendar {}: time zones used without a VTIMEZONE ({}) are read as UTC",
            calendar_id,
            unknown.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
    let incomplete = doc.zones.exhausted.get();
    if incomplete {
        tracing::warn!(
            "calendar {}: too many time zone rules to go through (or a TZID too long to look \
             up); events whose times could not be converted were left out",
            calendar_id
        );
    }
    IcsEvents { events, incomplete }
}

/// How far [`parse_ics_expanded`] may expand recurring events: the window, and budgets shared
/// by every calendar object of one CalDAV sync (its answer, and the objects fetched again
/// whole, see [`parse_ics_server_expanded`]).
#[derive(Debug)]
pub struct Expansion {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    rule_steps_left: usize,
    instances_left: usize,
    bytes_left: usize,
    zone_steps_left: usize,
    truncated: bool,
    unknown_zones: BTreeSet<String>,
    /// Summaries of the recurring events whose RRULE is outside the supported subset, by
    /// UID.
    unexpanded: BTreeMap<String, String>,
}

impl Expansion {
    /// Expand occurrences overlapping `start..end`.
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            start,
            end,
            rule_steps_left: MAX_RULE_STEPS,
            instances_left: MAX_INSTANCES,
            bytes_left: MAX_EXPANDED_BYTES,
            zone_steps_left: MAX_ZONE_STEPS,
            truncated: false,
            unknown_zones: BTreeSet::new(),
            unexpanded: BTreeMap::new(),
        }
    }

    /// Whether a budget ran out, so events or occurrences in the window are missing.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// TZIDs the data used without a VTIMEZONE; their times were read as UTC.
    pub fn unknown_zones(&self) -> impl Iterator<Item = &str> {
        self.unknown_zones.iter().map(String::as_str)
    }

    /// Summaries of the recurring events whose RRULE is outside the supported subset (see
    /// [`super::recurrence`]), one per series: only their first occurrence is in the result
    /// (unless an EXDATE cancels it or it was moved), so later ones in the window are
    /// missing. The rest of the answer is complete.
    pub fn unexpanded(&self) -> impl ExactSizeIterator<Item = &str> {
        self.unexpanded.values().map(String::as_str)
    }

    /// Pay for one more event holding `bytes` of text; `false`, and the answer truncated,
    /// once either budget cannot.
    fn take(&mut self, bytes: usize) -> bool {
        if self.instances_left == 0 || self.bytes_left < bytes {
            self.truncated = true;
            return false;
        }
        self.instances_left -= 1;
        self.bytes_left -= bytes;
        true
    }
}

/// Parse CalDAV calendar data, expanding each recurring event (RRULE, RDATE, EXDATE and the
/// VEVENTs overriding single occurrences) into its occurrences that overlap the expansion
/// window. A series of RDATEs without an RRULE (irregular series, as Apple Calendar and
/// Outlook export them) is DTSTART and its RDATEs. An RRULE outside the supported subset (see
/// [`super::recurrence`]) leaves the event as its first occurrence (none when an EXDATE
/// cancels it or a VEVENT of its own moves it), and is listed in [`Expansion::unexpanded`].
/// Data a server already expanded has neither RRULE nor RDATE and is unaffected. Every event
/// gets `etag`, the etag of the resource the data came from, and is paid for from the
/// expansion's budgets.
pub fn parse_ics_expanded(
    content: &str,
    calendar_id: &str,
    etag: Option<&str>,
    expansion: &mut Expansion,
) -> Vec<CalendarEvent> {
    let doc = Document::parse_for(content, expansion);
    expand_document(doc, calendar_id, etag, expansion)
}

/// [`parse_ics_expanded`] for the calendar data of an answer the server was asked to expand
/// (a CalDAV `calendar-query` with `<C:expand>`): `None`, with nothing paid for, when the data
/// holds a series of RDATEs without an RRULE. A server that expands sends no series at all
/// (RFC 4791 section 9.6.5), so this one was sent as is, and possibly without the VEVENTs
/// that move or cancel its occurrences: Radicale sends such a series' master alone. Expanding
/// that master would store a moved occurrence at its old time, and a cancelled one as still
/// on. The caller fetches the object whole and reads it with [`parse_ics_expanded`] instead.
/// A series with an RRULE sent as is is expanded here, as [`parse_ics_expanded`] does: the
/// servers that send one unexpanded (those ignoring `expand`) send the whole object.
pub fn parse_ics_server_expanded(
    content: &str,
    calendar_id: &str,
    etag: Option<&str>,
    expansion: &mut Expansion,
) -> Option<Vec<CalendarEvent>> {
    let doc = Document::parse_for(content, expansion);
    if doc
        .events
        .iter()
        .any(|e| e.series_uid().is_some() && e.rrule.is_none())
    {
        return None;
    }
    Some(expand_document(doc, calendar_id, etag, expansion))
}

/// The events of `doc`, expanded as [`parse_ics_expanded`] says.
fn expand_document(
    doc: Document,
    calendar_id: &str,
    etag: Option<&str>,
    expansion: &mut Expansion,
) -> Vec<CalendarEvent> {
    expansion.truncated |= doc.events_dropped;
    doc.zones.steps_left.set(expansion.zone_steps_left);
    // Occurrences with a VEVENT of their own, by UID, and the series with one whose
    // RECURRENCE-ID could not be converted: which occurrence it replaces is unknown, so none
    // of the series' own occurrences are kept (and the answer is incomplete).
    let mut overridden: HashMap<&str, HashSet<String>> = HashMap::new();
    let mut unresolved: HashSet<&str> = HashSet::new();
    for e in &doc.events {
        if let (Some(uid), Some(rid)) = (&e.uid, &e.recurrence_id) {
            match rid.key(&doc.zones) {
                Some(key) => {
                    overridden.entry(uid.as_str()).or_default().insert(key);
                }
                None => {
                    unresolved.insert(uid.as_str());
                }
            }
        }
    }
    let mut events = Vec::new();
    for e in &doc.events {
        let Some(uid) = e.series_uid() else {
            // An event, or an occurrence with a VEVENT of its own.
            if let Some(event) = e.event(calendar_id, &doc.zones) {
                push_event(&mut events, event, etag, expansion);
            }
            continue;
        };
        let rule = e.rrule.as_deref().map(Rule::parse);
        if let Some(None) = rule {
            expansion
                .unexpanded
                .entry(uid.to_string())
                .or_insert_with(|| e.label());
        }
        if unresolved.contains(uid) {
            continue;
        }
        let skip = overridden.get(uid);
        match rule {
            // An RRULE outside the supported subset: the first occurrence only.
            Some(None) => {
                if let Some(event) = e.first_occurrence(calendar_id, &doc.zones, skip) {
                    push_event(&mut events, event, etag, expansion);
                }
            }
            // A supported RRULE, or RDATEs alone.
            rule => events.extend(e.expand(
                rule.flatten().as_ref(),
                calendar_id,
                etag,
                &doc.zones,
                skip,
                expansion,
            )),
        }
    }
    expansion.zone_steps_left = doc.zones.steps_left.get();
    expansion.truncated |= doc.zones.exhausted.get();
    expansion
        .unknown_zones
        .extend(doc.zones.unknown.into_inner());
    events
}

/// A DATE or DATE-TIME property value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum IcsTime {
    /// `VALUE=DATE` or a bare `YYYYMMDD`: an all-day date.
    Date(NaiveDate),
    /// A `Z`-suffixed UTC time.
    Utc(NaiveDateTime),
    /// A local time in the `TZID` zone, or floating (read as UTC) without one. The values of
    /// one RDATE or EXDATE list share their property's TZID, so a long one is not copied
    /// into each of them.
    Local(NaiveDateTime, Option<Rc<str>>),
}

/// The `TZID` parameter of a property, to be shared by the values it applies to.
fn tzid_param(params: &[(String, String)]) -> Option<Rc<str>> {
    param(params, "TZID").map(Rc::from)
}

impl IcsTime {
    fn parse(params: &[(String, String)], value: &str) -> Option<IcsTime> {
        Self::parse_in(params, &tzid_param(params), value)
    }

    /// `value` of a property with `params`, whose `TZID` is `tzid`.
    fn parse_in(
        params: &[(String, String)],
        tzid: &Option<Rc<str>>,
        value: &str,
    ) -> Option<IcsTime> {
        let value = value.trim();
        let date_only = param(params, "VALUE").is_some_and(|v| v.eq_ignore_ascii_case("DATE"))
            || (value.len() == 8 && value.bytes().all(|b| b.is_ascii_digit()));
        if date_only {
            if let Ok(date) = NaiveDate::parse_from_str(value, "%Y%m%d") {
                return Some(IcsTime::Date(date));
            }
        }
        if let Some(utc) = value.strip_suffix('Z') {
            return NaiveDateTime::parse_from_str(utc, "%Y%m%dT%H%M%S")
                .ok()
                .map(IcsTime::Utc);
        }
        NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S")
            .ok()
            .map(|local| IcsTime::Local(local, tzid.clone()))
    }

    fn is_date(&self) -> bool {
        matches!(self, IcsTime::Date(_))
    }

    /// The time as written, before any zone conversion; a date at midnight.
    fn written(&self) -> NaiveDateTime {
        match self {
            IcsTime::Date(date) => date.and_time(NaiveTime::MIN),
            IcsTime::Utc(at) | IcsTime::Local(at, _) => *at,
        }
    }

    /// Identifies an occurrence in event ids: the date of an all-day one, otherwise its start
    /// in UTC, which is what a server's own `expand` writes into RECURRENCE-ID. `None` when
    /// the time cannot be converted (see [`Zones::local_to_utc`]).
    fn key(&self, zones: &Zones) -> Option<String> {
        Some(match self {
            IcsTime::Date(date) => date.format("%Y%m%d").to_string(),
            other => occurrence_key(false, zones.utc(other)?),
        })
    }
}

/// One RDATE value: a DATE, a DATE-TIME, or a PERIOD (RFC 5545 section 3.3.9), which also
/// says how long that occurrence lasts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RDate {
    start: IcsTime,
    /// The end a PERIOD gives; boxed, as few values have one.
    period: Option<Box<PeriodEnd>>,
}

/// How a PERIOD ends: at a time (in its start's zone), or after a duration.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PeriodEnd {
    At(IcsTime),
    After(Duration),
}

impl RDate {
    /// `value` of an RDATE with `params`, whose `TZID` is `tzid`. A PERIOD whose end does not
    /// parse keeps its start, and lasts as long as the series' other occurrences.
    fn parse(params: &[(String, String)], tzid: &Option<Rc<str>>, value: &str) -> Option<RDate> {
        let (start, end) = match value.split_once('/') {
            Some((start, end)) => (start, Some(end.trim())),
            None => (value, None),
        };
        let period = end.and_then(|end| {
            Some(if end.trim_start_matches(['+', '-']).starts_with('P') {
                PeriodEnd::After(parse_ics_duration(end)?)
            } else {
                PeriodEnd::At(IcsTime::parse_in(params, tzid, end)?)
            })
        });
        Some(RDate {
            start: IcsTime::parse_in(params, tzid, start)?,
            period: period.map(Box::new),
        })
    }

    /// How long the occurrence lasts in wall-clock time, before any zone conversion: its
    /// PERIOD's length, else `series` (the length of the series' other occurrences).
    fn written_length(&self, series: Duration) -> Duration {
        match self.period.as_deref() {
            Some(PeriodEnd::After(length)) => *length,
            Some(PeriodEnd::At(end)) => end.written() - self.start.written(),
            None => series,
        }
    }

    /// Whether the occurrence cannot overlap `start..end` whatever its zone, as its wall-clock
    /// times are days outside it. `series` is the length of the series' other occurrences.
    ///
    /// A VTIMEZONE offset is under a day, so the occurrence starts less than a day from the
    /// time written, and ends less than a day from `written + reach`: a PERIOD's end is
    /// converted on its own, a length (the PERIOD's or `series`) is added to the start. Two
    /// days of slack, as for RRULE occurrences, leave room to spare.
    fn misses(&self, series: Duration, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        let slack = Duration::days(2);
        let written = self.start.written();
        let reach = self.written_length(series).max(series);
        let after = end
            .naive_utc()
            .checked_add_signed(slack)
            .is_some_and(|end| written >= end);
        let before = written
            .checked_add_signed(reach)
            .and_then(|at| at.checked_add_signed(slack))
            .is_some_and(|at| at <= start.naive_utc());
        after || before
    }

    /// When the occurrence starts in UTC and how long it lasts: its PERIOD's length, else
    /// `series`. A PERIOD that ends before it starts is ignored, as a DTEND before DTSTART is.
    /// `None` when a time cannot be converted (see [`Zones::local_to_utc`]).
    fn occurrence(&self, series: Duration, zones: &Zones) -> Option<(DateTime<Utc>, Duration)> {
        let start = zones.utc(&self.start)?;
        let length = match self.period.as_deref() {
            Some(PeriodEnd::After(length)) => Some(*length),
            Some(PeriodEnd::At(end)) => Some(zones.utc(end)? - start),
            None => None,
        };
        Some((
            start,
            length.filter(|l| *l >= Duration::zero()).unwrap_or(series),
        ))
    }
}

/// When an occurrence from `at` lasting `length` ends; `at` when that is out of range.
fn end_of(at: DateTime<Utc>, length: Duration) -> DateTime<Utc> {
    at.checked_add_signed(length).unwrap_or(at)
}

/// Whether an occurrence from `at` lasting `length` overlaps `start..end`, as a CalDAV
/// time-range does (RFC 4791 section 9.9).
fn overlaps(at: DateTime<Utc>, length: Duration, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    at < end && (end_of(at, length) > start || (length.is_zero() && at >= start))
}

/// Add `event`, from the resource with `etag`, to `events` if the expansion can pay for it.
fn push_event(
    events: &mut Vec<CalendarEvent>,
    mut event: CalendarEvent,
    etag: Option<&str>,
    expansion: &mut Expansion,
) {
    event.etag = etag.map(str::to_string);
    if expansion.take(text_bytes(&event)) {
        events.push(event);
    }
}

/// Bytes of text `event` holds, as the expansion's byte budget counts them.
fn text_bytes(event: &CalendarEvent) -> usize {
    let text = |t: &Option<String>| t.as_ref().map_or(0, String::len);
    event.id.len()
        + event.calendar_id.len()
        + event.uid.len()
        + event.summary.len()
        + text(&event.description)
        + text(&event.location)
        + text(&event.etag)
}

fn occurrence_key(all_day: bool, start: DateTime<Utc>) -> String {
    if all_day {
        start.format("%Y%m%d").to_string()
    } else {
        start.format("%Y%m%dT%H%M%SZ").to_string()
    }
}

/// The VTIMEZONEs of one iCalendar object, by TZID.
struct Zones {
    by_id: HashMap<String, Zone>,
    /// TZIDs used without a VTIMEZONE (their times are read as UTC).
    unknown: RefCell<BTreeSet<String>>,
    /// What lookups may still cost, see [`MAX_ZONE_STEPS`].
    steps_left: Cell<usize>,
    /// Whether that ran out, so later times in these zones could not be converted, or a time
    /// was in a zone whose name is too long to look up (see [`MAX_TZID_BYTES`]).
    exhausted: Cell<bool>,
}

impl Default for Zones {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            unknown: RefCell::default(),
            steps_left: Cell::new(MAX_ZONE_STEPS),
            exhausted: Cell::new(false),
        }
    }
}

impl Zones {
    /// `local` in zone `tzid` as UTC; floating times, and zones without a VTIMEZONE or a
    /// usable observance, are read as UTC. `None` when the zone's rules were too costly to go
    /// through, or its name is too long to look up (see [`MAX_TZID_BYTES`]): the offset is
    /// unknown, and reading the time as UTC would store it hours off.
    fn local_to_utc(&self, local: NaiveDateTime, tzid: Option<&str>) -> Option<NaiveDateTime> {
        let Some(tzid) = tzid else {
            return Some(local);
        };
        if tzid.len() > MAX_TZID_BYTES {
            self.exhausted.set(true);
            return None;
        }
        if let Some(zone) = self.by_id.get(tzid) {
            let mut budget = self.steps_left.get();
            let converted = zone.to_utc(local, &mut budget);
            self.steps_left.set(budget);
            match converted {
                Ok(Some(utc)) => return Some(utc),
                Ok(None) => {}
                Err(_) => {
                    self.exhausted.set(true);
                    return None;
                }
            }
        }
        if !UTC_NAMES.iter().any(|n| n.eq_ignore_ascii_case(tzid))
            && !self.unknown.borrow().contains(tzid)
        {
            self.unknown.borrow_mut().insert(tzid.to_string());
        }
        Some(local)
    }

    /// `time` in UTC; `None` when it cannot be converted (see [`Zones::local_to_utc`]).
    fn utc(&self, time: &IcsTime) -> Option<DateTime<Utc>> {
        Some(match time {
            IcsTime::Date(date) => date.and_time(NaiveTime::MIN).and_utc(),
            IcsTime::Utc(at) => at.and_utc(),
            IcsTime::Local(at, tzid) => self.local_to_utc(*at, tzid.as_deref())?.and_utc(),
        })
    }
}

/// Properties of one VEVENT.
#[derive(Debug, Default)]
struct VEventFields {
    uid: Option<String>,
    summary: Option<String>,
    description: Option<String>,
    location: Option<String>,
    start: Option<IcsTime>,
    end: Option<IcsTime>,
    duration: Option<Duration>,
    recurrence_id: Option<IcsTime>,
    /// RECURRENCE-ID as written, for one that does not parse.
    recurrence_id_raw: Option<String>,
    status: EventStatus,
    rrule: Option<String>,
    rdates: Vec<RDate>,
    exdates: Vec<IcsTime>,
}

impl VEventFields {
    fn set(&mut self, name: &str, params: &[(String, String)], value: &str) {
        match name {
            "UID" => self.uid = Some(value.to_string()),
            "SUMMARY" => self.summary = Some(unescape_ics_text(value)),
            "DESCRIPTION" => self.description = Some(unescape_ics_text(value)),
            "LOCATION" => self.location = Some(unescape_ics_text(value)),
            "DTSTART" => self.start = IcsTime::parse(params, value),
            "DTEND" => self.end = IcsTime::parse(params, value),
            "DURATION" => self.duration = parse_ics_duration(value),
            "RECURRENCE-ID" => {
                self.recurrence_id = IcsTime::parse(params, value);
                self.recurrence_id_raw = Some(value.trim().to_string());
            }
            "RRULE" => self.rrule = Some(value.to_string()),
            "RDATE" => {
                let tzid = tzid_param(params);
                self.rdates.extend(
                    value
                        .split(',')
                        .filter_map(|v| RDate::parse(params, &tzid, v)),
                );
            }
            "EXDATE" => {
                let tzid = tzid_param(params);
                self.exdates.extend(
                    value
                        .split(',')
                        .filter_map(|v| IcsTime::parse_in(params, &tzid, v)),
                );
            }
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

    /// Start and end in UTC, and whether the event is all-day; `None` without a DTSTART, or
    /// when a time cannot be converted. A malformed end before the start (or a negative
    /// DURATION) is ignored.
    fn times(&self, zones: &Zones) -> Option<(DateTime<Utc>, DateTime<Utc>, bool)> {
        let start_time = self.start.as_ref()?;
        let all_day = start_time.is_date();
        let start = zones.utc(start_time)?;
        let default_length = if all_day {
            Duration::days(1)
        } else {
            Duration::hours(1)
        };
        let end = match &self.end {
            Some(end) => Some(zones.utc(end)?),
            None => None,
        };
        let end = end
            .or_else(|| start.checked_add_signed(self.duration?))
            .filter(|end| *end >= start)
            .or_else(|| start.checked_add_signed(default_length))
            .unwrap_or(start);
        Some((start, end, all_day))
    }

    /// The UID of the series this VEVENT is the master of: one without a RECURRENCE-ID, with
    /// an RRULE, RDATEs, or both. `None` for an event, or an occurrence with a VEVENT of its
    /// own.
    fn series_uid(&self) -> Option<&str> {
        let series =
            self.recurrence_id.is_none() && (self.rrule.is_some() || !self.rdates.is_empty());
        self.uid.as_deref().filter(|_| series)
    }

    /// Bytes of text each occurrence of this event holds (see [`text_bytes`]): its id
    /// (`calendar:uid:key`), calendar id, UID, summary, description, location and `etag`.
    fn occurrence_bytes(&self, uid: &str, calendar_id: &str, etag: Option<&str>) -> usize {
        let text = |t: &Option<String>| t.as_ref().map_or(0, String::len);
        2 * (calendar_id.len() + uid.len())
            + 18
            + self.summary.as_deref().unwrap_or(UNTITLED_EVENT).len()
            + text(&self.description)
            + text(&self.location)
            + etag.map_or(0, str::len)
    }

    /// How the event is named in reports: the start of its summary, or its UID.
    fn label(&self) -> String {
        let name = self
            .summary
            .as_deref()
            .or(self.uid.as_deref())
            .unwrap_or(UNTITLED_EVENT);
        match name.char_indices().nth(60) {
            Some((end, _)) => format!("{}...", &name[..end]),
            None => name.to_string(),
        }
    }

    fn occurrence(
        &self,
        id: String,
        uid: &str,
        calendar_id: &str,
        etag: Option<&str>,
        (start, end, all_day): (DateTime<Utc>, DateTime<Utc>, bool),
    ) -> CalendarEvent {
        let now = Utc::now();
        CalendarEvent {
            id,
            calendar_id: calendar_id.to_string(),
            uid: uid.to_string(),
            summary: self
                .summary
                .clone()
                .unwrap_or_else(|| UNTITLED_EVENT.to_string()),
            description: self.description.clone(),
            location: self.location.clone(),
            start,
            end,
            all_day,
            attendees: Vec::new(),
            organizer: None,
            meeting_url: None,
            status: self.status,
            created: now,
            updated: now,
            etag: etag.map(str::to_string),
        }
    }

    /// The event, if it has the required UID and DTSTART and its times can be converted.
    fn event(&self, calendar_id: &str, zones: &Zones) -> Option<CalendarEvent> {
        let uid = self.uid.as_deref()?;
        let times = self.times(zones)?;
        // Each occurrence of a recurring event (RECURRENCE-ID, e.g. from a CalDAV `expand`)
        // shares the series UID, so the occurrence is part of the id.
        let rid = match (&self.recurrence_id, &self.recurrence_id_raw) {
            (Some(rid), _) => Some(rid.key(zones)?),
            (None, raw) => raw.clone(),
        };
        let id = match rid {
            Some(rid) => format!("{}:{}:{}", calendar_id, uid, rid),
            None => format!("{}:{}", calendar_id, uid),
        };
        Some(self.occurrence(id, uid, calendar_id, None, times))
    }

    /// EXDATE in UTC; `None` when one cannot be converted (see [`Zones::local_to_utc`]).
    fn excluded(&self, zones: &Zones) -> Option<HashSet<DateTime<Utc>>> {
        self.exdates.iter().map(|t| zones.utc(t)).collect()
    }

    /// What is left of a series whose RRULE is outside the supported subset: its first
    /// occurrence, as the event it has always been stored as (`calendar:uid`), unless an
    /// EXDATE cancels it or it is in `overridden` (it has a VEVENT of its own, which takes its
    /// place). `None` too when an EXDATE cannot be converted, as whether it cancels the
    /// occurrence is then unknown (and the answer is marked truncated).
    fn first_occurrence(
        &self,
        calendar_id: &str,
        zones: &Zones,
        overridden: Option<&HashSet<String>>,
    ) -> Option<CalendarEvent> {
        let event = self.event(calendar_id, zones)?;
        let excluded = self.excluded(zones)?;
        let moved = overridden
            .is_some_and(|keys| keys.contains(&occurrence_key(event.all_day, event.start)));
        (!excluded.contains(&event.start) && !moved).then_some(event)
    }

    /// The occurrences of this recurring event that overlap the expansion window: DTSTART,
    /// those `rule` (its RRULE, if it has one) yields and its RDATEs, except those in
    /// `overridden` (they have a VEVENT of their own) and in EXDATE, with each id once: an
    /// RDATE at the time of an earlier occurrence (in an all-day series, on its date) adds
    /// nothing. Each lasts as long as the first, except an RDATE PERIOD, which says how long
    /// its occurrence lasts. Occurrences whose time cannot be converted are left out, and so is
    /// the whole series when an EXDATE cannot be; the answer is then marked truncated, and
    /// nothing stored is pruned.
    fn expand(
        &self,
        rule: Option<&Rule>,
        calendar_id: &str,
        etag: Option<&str>,
        zones: &Zones,
        overridden: Option<&HashSet<String>>,
        expansion: &mut Expansion,
    ) -> Vec<CalendarEvent> {
        let (Some(uid), Some(start), Some((first_start, first_end, all_day))) =
            (self.uid.as_deref(), self.start.as_ref(), self.times(zones))
        else {
            return Vec::new();
        };
        let length = first_end - first_start;
        let Some(excluded) = self.excluded(zones) else {
            return Vec::new();
        };
        let (window_start, window_end) = (expansion.start, expansion.end);
        let skip = |at: DateTime<Utc>| {
            excluded.contains(&at)
                || overridden.is_some_and(|keys| keys.contains(&occurrence_key(all_day, at)))
        };
        let in_window =
            |at: DateTime<Utc>, length: Duration| overlaps(at, length, window_start, window_end);
        let mut starts: Vec<(DateTime<Utc>, Duration)> = Vec::new();
        let bytes = self.occurrence_bytes(uid, calendar_id, etag);
        if let Some(rule) = rule {
            let (anchor, tzid) = match start {
                IcsTime::Date(date) => (date.and_time(NaiveTime::MIN), None),
                IcsTime::Utc(at) => (*at, None),
                IcsTime::Local(at, tzid) => (*at, tzid.as_deref()),
            };
            // Local times run up to 14 hours ahead of UTC, and occurrences starting before the
            // window may still overlap it.
            let margin = Duration::days(2);
            let from = window_start
                .checked_sub_signed(length)
                .and_then(|at| at.checked_sub_signed(margin))
                .map(|at| at.date_naive());
            let end = window_end
                .checked_add_signed(margin)
                .unwrap_or(window_end)
                .naive_utc();
            let mut budget = expansion.rule_steps_left;
            let walked = rule.walk(
                anchor,
                from.filter(|_| !rule.has_count()),
                end,
                &mut budget,
                |local| {
                    // Occurrences days before the window, which a COUNT rule walks through from
                    // DTSTART, are only counted: they need no UTC time, and no zone lookup.
                    // (UNTIL, which RFC 5545 does not allow next to COUNT, is checked from the
                    // window on.)
                    if from.is_some_and(|from| local.date() < from) {
                        return true;
                    }
                    // Its zone's rules were too costly: this and later occurrences are unknown.
                    let Some(utc) = zones.local_to_utc(local, tzid) else {
                        return false;
                    };
                    if !rule.until_allows(local, utc) {
                        return false;
                    }
                    let at = utc.and_utc();
                    if at >= window_end {
                        return false;
                    }
                    if in_window(at, length) && !skip(at) {
                        if !expansion.take(bytes) {
                            return false;
                        }
                        starts.push((at, length));
                    }
                    true
                },
            );
            expansion.rule_steps_left = budget;
            expansion.truncated |= walked.is_err();
        } else if in_window(first_start, length) && !skip(first_start) {
            // Without an RRULE, the occurrences are DTSTART and the RDATEs (RFC 5545 section
            // 3.8.5.2).
            if expansion.take(bytes) {
                starts.push((first_start, length));
            }
        }
        // An occurrence's id holds its key, which is only its date in an all-day series: an
        // RDATE is left out when an occurrence before it (DTSTART, the rule's or an earlier
        // RDATE) has the same key, so that no id comes twice, not even from a time on a date
        // the series already has.
        let mut seen: HashSet<String> = starts
            .iter()
            .map(|(at, _)| occurrence_key(all_day, *at))
            .collect();
        for rdate in &self.rdates {
            // Not converted, so a long list of past dates costs no zone lookups.
            if rdate.misses(length, window_start, window_end) {
                continue;
            }
            let Some((at, length)) = rdate.occurrence(length, zones) else {
                continue;
            };
            if !in_window(at, length) || skip(at) || !seen.insert(occurrence_key(all_day, at)) {
                continue;
            }
            if !expansion.take(bytes) {
                break;
            }
            starts.push((at, length));
        }
        starts
            .into_iter()
            .map(|(at, length)| {
                let id = format!("{}:{}:{}", calendar_id, uid, occurrence_key(all_day, at));
                self.occurrence(
                    id,
                    uid,
                    calendar_id,
                    etag,
                    (at, end_of(at, length), all_day),
                )
            })
            .collect()
    }
}

/// The VEVENTs and VTIMEZONEs of an iCalendar text.
struct Document {
    events: Vec<VEventFields>,
    /// Whether VEVENTs past the most [`Document::parse`] was to keep were left out.
    events_dropped: bool,
    zones: Zones,
}

/// Where the parser is.
enum State {
    Outside,
    Event {
        fields: VEventFields,
        /// Depth of components (VALARM) nested in the VEVENT.
        nested: usize,
    },
    Timezone {
        tzid: Option<String>,
        zone: Zone,
        observance: Option<Observance>,
        nested: usize,
    },
}

impl Document {
    /// Parse `content` for `expansion`. Each VEVENT is an event, a moved occurrence (one event
    /// too) or a series (at least one occurrence, unless each is moved or cancelled): an object
    /// with more than twice the events still allowed would run out of them anyway. Its VEVENTs
    /// past that are not kept, so a server cannot make the parsed components ten times larger
    /// than its answer.
    fn parse_for(content: &str, expansion: &Expansion) -> Document {
        Self::parse(content, expansion.instances_left.saturating_mul(2))
    }

    /// Parse `content`, keeping its first `max_events` VEVENTs.
    fn parse(content: &str, max_events: usize) -> Document {
        let mut doc = Document {
            events: Vec::new(),
            events_dropped: false,
            zones: Zones::default(),
        };
        let mut state = State::Outside;
        for line in unfold_ics_lines(content) {
            let Some((key, value)) = split_ics_property(line.trim()) else {
                continue;
            };
            let (name, params) = split_name(key);
            // The component a BEGIN or END line names; other values can be long (DESCRIPTION)
            // and are not copied.
            let component = match name.as_str() {
                "BEGIN" | "END" => value.trim().to_ascii_uppercase(),
                _ => String::new(),
            };
            match (name.as_str(), component.as_str()) {
                // A VEVENT never nests, so a new one also ends an unterminated one (dropped)
                // or an unterminated VTIMEZONE (kept).
                ("BEGIN", "VEVENT") => {
                    doc.finish(std::mem::replace(&mut state, State::Outside));
                    state = State::Event {
                        fields: VEventFields::default(),
                        nested: 0,
                    };
                }
                // END:VEVENT always closes the event, even when a nested component (a VALARM
                // missing its END) is still open, so one broken alarm cannot swallow the
                // events that follow.
                ("END", "VEVENT") if matches!(state, State::Event { .. }) => {
                    if let State::Event { fields, .. } =
                        std::mem::replace(&mut state, State::Outside)
                    {
                        if doc.events.len() < max_events {
                            doc.events.push(fields);
                        } else {
                            doc.events_dropped = true;
                        }
                    }
                }
                ("BEGIN", "VTIMEZONE") if matches!(state, State::Outside) => {
                    state = State::Timezone {
                        tzid: None,
                        zone: Zone::default(),
                        observance: None,
                        nested: 0,
                    };
                }
                ("END", "VTIMEZONE") if matches!(state, State::Timezone { .. }) => {
                    doc.finish(std::mem::replace(&mut state, State::Outside));
                }
                _ => match &mut state {
                    State::Outside => {}
                    State::Event { fields, nested } => match name.as_str() {
                        "BEGIN" => *nested += 1,
                        "END" => *nested = nested.saturating_sub(1),
                        _ if *nested == 0 => fields.set(&name, &params, value),
                        _ => {}
                    },
                    State::Timezone {
                        tzid,
                        zone,
                        observance,
                        nested,
                    } => {
                        let is_observance = matches!(component.as_str(), "STANDARD" | "DAYLIGHT");
                        match name.as_str() {
                            "BEGIN" if is_observance && *nested == 0 && observance.is_none() => {
                                *observance = Some(Observance::default());
                            }
                            "END" if is_observance && *nested == 0 && observance.is_some() => {
                                zone.add(observance.take().unwrap_or_default());
                            }
                            "BEGIN" => *nested += 1,
                            "END" => *nested = nested.saturating_sub(1),
                            _ if *nested > 0 => {}
                            "TZID" if observance.is_none() => {
                                *tzid = Some(value.trim().to_string());
                            }
                            _ => {
                                if let Some(o) = observance {
                                    o.set(&name, value);
                                }
                            }
                        }
                    }
                },
            }
        }
        doc.finish(state);
        doc
    }

    /// Wrap up what `state` was parsing when it ended: a VTIMEZONE is kept, an unterminated
    /// VEVENT is dropped.
    fn finish(&mut self, state: State) {
        if let State::Timezone {
            tzid: Some(tzid),
            mut zone,
            observance,
            ..
        } = state
        {
            if let Some(o) = observance {
                zone.add(o);
            }
            self.zones.by_id.insert(tzid, zone);
        }
    }
}

/// The upper-cased property name of `name;params` and its parameters, values unquoted.
fn split_name(key: &str) -> (String, Vec<(String, String)>) {
    let mut parts = Vec::new();
    let (mut quoted, mut begin) = (false, 0);
    for (i, c) in key.char_indices() {
        match c {
            '"' => quoted = !quoted,
            ';' if !quoted => {
                parts.push(&key[begin..i]);
                begin = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&key[begin..]);
    let name = parts[0].trim().to_ascii_uppercase();
    let params = parts[1..]
        .iter()
        .filter_map(|p| {
            let (k, v) = p.split_once('=')?;
            Some((
                k.trim().to_ascii_uppercase(),
                v.trim().trim_matches('"').to_string(),
            ))
        })
        .collect();
    (name, params)
}

fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Undo RFC 5545 section 3.1 line folding: a line starting with a space or tab continues the
/// previous line, minus that one leading whitespace character.
///
/// Lines are unfolded one at a time as the parser asks for them, and only folded ones are
/// copied: a list of every line up front would cost some 30 times the text for text of short
/// lines (a gigabyte for a 32 MiB answer).
fn unfold_ics_lines(content: &str) -> impl Iterator<Item = Cow<'_, str>> {
    fn continuation(raw: &str) -> Option<&str> {
        raw.strip_prefix(' ').or_else(|| raw.strip_prefix('\t'))
    }
    let mut raw = content.lines().peekable();
    std::iter::from_fn(move || {
        let mut line = Cow::Borrowed(raw.next()?);
        while let Some(rest) = raw.peek().copied().and_then(continuation) {
            line.to_mut().push_str(rest);
            raw.next();
        }
        Some(line)
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Europe/Berlin as Outlook, Google and SabreDAV write it.
    const BERLIN: &str = "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nBEGIN:DAYLIGHT\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0200\r\nTZNAME:CEST\r\nDTSTART:19700329T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU\r\nEND:DAYLIGHT\r\nBEGIN:STANDARD\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nTZNAME:CET\r\nDTSTART:19701025T030000\r\nRRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n";

    #[test]
    fn date_only_events_are_all_day() {
        let ics = "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:a\nSUMMARY:Holiday\nDTSTART;VALUE=DATE:20250704\nDTEND;VALUE=DATE:20250705\nEND:VEVENT\nBEGIN:VEVENT\nUID:b\nSUMMARY:Bare date\nDTSTART:20250801\nEND:VEVENT\nBEGIN:VEVENT\nUID:c\nSUMMARY:Meeting\nDTSTART:20250801T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:d\nSUMMARY:Explicit datetime\nDTSTART;VALUE=DATE-TIME:20250801T090000\nEND:VEVENT\nEND:VCALENDAR\n";
        let events = parse_ics_str(ics, "cal").events;
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
        let events = parse_ics_str(ics, "cal").events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].uid, "long-uid@example.com");
        assert_eq!(events[0].summary, "Quarterly planning review: part 2");
    }

    #[test]
    fn caldav_style_components_parse() {
        // Expanded recurrence (shared UID, distinct RECURRENCE-ID), a VALARM whose
        // DESCRIPTION must not leak into the event, TEXT escapes, DURATION, STATUS, a quoted
        // parameter containing a colon, and an untitled event.
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID:20260105T090000Z\r\nSUMMARY:Standup\\, daily\r\nDTSTART:20260105T090000Z\r\nDURATION:PT15M\r\nDESCRIPTION;ALTREP=\"cid:part1@example.org\":Line one\\nLine two\\; done\r\nLOCATION:Room 1\r\nSTATUS:TENTATIVE\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nSUMMARY:Alarm\r\nTRIGGER:-PT5M\r\nEND:VALARM\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID:20260106T090000Z\r\nSUMMARY:Standup\r\nDTSTART:20260106T090000Z\r\nDTEND:20260106T091500Z\r\nstatus:cancelled\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:untitled\r\nDTSTART;VALUE=DATE:20260107\r\nDURATION:P2D\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let events = parse_ics_str(ics, "cal").events;
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
    fn an_unclosed_nested_component_does_not_swallow_later_events() {
        // The first event's VALARM never ends. END:VEVENT still closes the event (without the
        // alarm's DESCRIPTION), and the events after it are all there.
        let ics = "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:a\nSUMMARY:First\nDTSTART:20260105T090000Z\nBEGIN:VALARM\nACTION:DISPLAY\nDESCRIPTION:Alarm text\nSUMMARY:Alarm\nEND:VEVENT\nBEGIN:VEVENT\nUID:b\nSUMMARY:Second\nDTSTART:20260106T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:c\nSUMMARY:Third\nDTSTART:20260107T090000Z\nBEGIN:VALARM\nTRIGGER:-PT5M\nEND:VALARM\nDESCRIPTION:Real\nEND:VEVENT\nEND:VCALENDAR\n";
        let events = parse_ics_str(ics, "cal").events;
        let uids: Vec<_> = events.iter().map(|e| e.uid.as_str()).collect();
        assert_eq!(uids, ["a", "b", "c"]);
        assert_eq!(events[0].summary, "First");
        assert_eq!(events[0].description, None);
        assert_eq!(events[2].description.as_deref(), Some("Real"));
        // A VEVENT that never ends is dropped when the next one begins, as before.
        let unterminated = "BEGIN:VEVENT\nUID:x\nDTSTART:20260105T090000Z\nBEGIN:VEVENT\nUID:y\nDTSTART:20260106T090000Z\nEND:VEVENT\n";
        let events = parse_ics_str(unterminated, "cal").events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].uid, "y");
    }

    #[test]
    fn tzid_times_are_converted_with_the_vtimezone() {
        let ics = format!(
            "BEGIN:VCALENDAR\r\n{}BEGIN:VEVENT\r\nUID:winter\r\nDTSTART;TZID=Europe/Berlin:20260105T090000\r\nDTEND;TZID=\"Europe/Berlin\":20260105T100000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:summer\r\nDTSTART;TZID=Europe/Berlin:20260706T090000\r\nDURATION:PT30M\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260707T090000\r\nDTSTART;TZID=Europe/Berlin:20260707T110000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:elsewhere\r\nDTSTART;TZID=Mars/Olympus_Mons:20260105T090000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:utc-named\r\nDTSTART;TZID=Etc/UTC:20260105T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            BERLIN
        );
        let events = parse_ics_str(&ics, "cal").events;
        let by_uid = |u: &str| events.iter().find(|e| e.uid == u).unwrap();
        assert_eq!(by_uid("winter").start, utc("2026-01-05T08:00:00Z"));
        assert_eq!(by_uid("winter").end, utc("2026-01-05T09:00:00Z"));
        assert_eq!(by_uid("summer").start, utc("2026-07-06T07:00:00Z"));
        assert_eq!(by_uid("summer").end, utc("2026-07-06T07:30:00Z"));
        // The occurrence key is the original start in UTC, as a server's `expand` writes it.
        assert_eq!(by_uid("series").id, "cal:series:20260707T070000Z");
        assert_eq!(by_uid("series").start, utc("2026-07-07T09:00:00Z"));
        // A zone the data does not define is read as UTC (and logged).
        assert_eq!(by_uid("elsewhere").start, utc("2026-01-05T09:00:00Z"));
        assert_eq!(by_uid("utc-named").start, utc("2026-01-05T09:00:00Z"));
    }

    /// Weekly on Monday 09:00 Berlin time since 2025, with one occurrence cancelled (EXDATE),
    /// one moved (an overriding VEVENT) and one added (RDATE).
    fn weekly_series() -> String {
        format!(
            "BEGIN:VCALENDAR\r\n{}BEGIN:VEVENT\r\nUID:weekly\r\nSUMMARY:Planning\r\nDTSTART;TZID=Europe/Berlin:20250106T090000\r\nDTEND;TZID=Europe/Berlin:20250106T100000\r\nRRULE:FREQ=WEEKLY;BYDAY=MO\r\nEXDATE;TZID=Europe/Berlin:20260309T090000\r\nRDATE;TZID=Europe/Berlin:20260318T140000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:weekly\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260316T090000\r\nSUMMARY:Planning (moved)\r\nDTSTART;TZID=Europe/Berlin:20260317T090000\r\nDTEND;TZID=Europe/Berlin:20260317T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            BERLIN
        )
    }

    #[test]
    fn recurring_events_expand_within_the_window() {
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-07T00:00:00Z"));
        let events = parse_ics_expanded(&weekly_series(), "cal", None, &mut expansion);
        assert!(!expansion.truncated());
        let mut starts: Vec<_> = events
            .iter()
            .map(|e| (e.start.to_rfc3339(), e.summary.as_str(), e.id.as_str()))
            .collect();
        starts.sort();
        assert_eq!(
            starts,
            [
                // Mondays at 09:00 CET (08:00 UTC)...
                (
                    "2026-03-02T08:00:00+00:00".to_string(),
                    "Planning",
                    "cal:weekly:20260302T080000Z"
                ),
                // ... 9 March cancelled, 16 March moved to the 17th ...
                (
                    "2026-03-17T08:00:00+00:00".to_string(),
                    "Planning (moved)",
                    "cal:weekly:20260316T080000Z"
                ),
                // ... an extra one on Wednesday the 18th ...
                (
                    "2026-03-18T13:00:00+00:00".to_string(),
                    "Planning",
                    "cal:weekly:20260318T130000Z"
                ),
                (
                    "2026-03-23T08:00:00+00:00".to_string(),
                    "Planning",
                    "cal:weekly:20260323T080000Z"
                ),
                // ... and from 29 March at 09:00 CEST (07:00 UTC).
                (
                    "2026-03-30T07:00:00+00:00".to_string(),
                    "Planning",
                    "cal:weekly:20260330T070000Z"
                ),
                (
                    "2026-04-06T07:00:00+00:00".to_string(),
                    "Planning",
                    "cal:weekly:20260406T070000Z"
                ),
            ]
        );
        assert!(events.iter().all(|e| e.end - e.start == Duration::hours(1)));
        // Without expansion the series is its first occurrence, plus the moved one.
        let plain = parse_ics_str(&weekly_series(), "cal").events;
        assert_eq!(plain.len(), 2);
        assert_eq!(plain[0].id, "cal:weekly");
        assert_eq!(plain[0].start, utc("2025-01-06T08:00:00Z"));
    }

    #[test]
    fn expansion_honours_count_until_and_all_day_series() {
        let ics = "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:count\nDTSTART:20260301T120000Z\nRRULE:FREQ=DAILY;COUNT=3\nEND:VEVENT\nBEGIN:VEVENT\nUID:until\nDTSTART:20260301T120000Z\nDURATION:PT1H\nRRULE:FREQ=WEEKLY;UNTIL=20260315T120000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:birthday\nSUMMARY:Birthday\nDTSTART;VALUE=DATE:20000310\nDTEND;VALUE=DATE:20000311\nRRULE:FREQ=YEARLY\nEND:VEVENT\nBEGIN:VEVENT\nUID:odd\nDTSTART:20260302T120000Z\nRRULE:FREQ=YEARLY;BYWEEKNO=10\nEND:VEVENT\nBEGIN:VEVENT\nUID:close\nSUMMARY:Month-end close\nDTSTART:20250131T150000Z\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;WKST=SU\nEND:VEVENT\nEND:VCALENDAR\n";
        let mut expansion =
            Expansion::new(utc("2026-03-02T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(ics, "cal", None, &mut expansion);
        let ids = |uid: &str| {
            let mut ids: Vec<_> = events
                .iter()
                .filter(|e| e.uid == uid)
                .map(|e| e.id.clone())
                .collect();
            ids.sort();
            ids
        };
        // 1 March is before the window; COUNT still counts it.
        assert_eq!(
            ids("count"),
            ["cal:count:20260302T120000Z", "cal:count:20260303T120000Z"]
        );
        assert_eq!(
            ids("until"),
            ["cal:until:20260308T120000Z", "cal:until:20260315T120000Z"]
        );
        assert_eq!(ids("birthday"), ["cal:birthday:20260310"]);
        let birthday = events.iter().find(|e| e.uid == "birthday").unwrap();
        assert!(birthday.all_day);
        assert_eq!(birthday.start, utc("2026-03-10T00:00:00Z"));
        assert_eq!(birthday.end, utc("2026-03-11T00:00:00Z"));
        // The last weekday of the month, as Outlook writes it.
        assert_eq!(ids("close"), ["cal:close:20260331T150000Z"]);
        // A rule outside the supported subset stays its first occurrence, and is reported
        // (by UID, as it has no summary) without making the answer incomplete.
        assert_eq!(ids("odd"), ["cal:odd"]);
        assert_eq!(expansion.unexpanded().collect::<Vec<_>>(), ["odd"]);
        assert!(!expansion.truncated());
    }

    #[test]
    fn expansion_budgets_mark_the_answer_truncated() {
        let daily = "BEGIN:VEVENT\nUID:d\nDTSTART:20260101T090000Z\nRRULE:FREQ=DAILY\nEND:VEVENT\n";
        let window = (utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        let mut roomy = Expansion::new(window.0, window.1);
        assert_eq!(parse_ics_expanded(daily, "c", None, &mut roomy).len(), 365);
        assert!(!roomy.truncated());
        let mut few = Expansion {
            instances_left: 10,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(parse_ics_expanded(daily, "c", None, &mut few).len(), 10);
        assert!(few.truncated());
        // COUNT rules walk from DTSTART, so an old one costs periods, not occurrences.
        let old = "BEGIN:VEVENT\nUID:o\nDTSTART:19000101T090000Z\nRRULE:FREQ=DAILY;COUNT=100000\nEND:VEVENT\n";
        let mut short = Expansion {
            rule_steps_left: 2_000,
            ..Expansion::new(window.0, window.1)
        };
        assert!(parse_ics_expanded(old, "c", None, &mut short).is_empty());
        assert!(short.truncated());
        // The budgets are shared by every object of one answer.
        let mut shared = Expansion {
            instances_left: 400,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(parse_ics_expanded(daily, "c", None, &mut shared).len(), 365);
        assert_eq!(parse_ics_expanded(daily, "c", None, &mut shared).len(), 35);
        assert!(shared.truncated());
    }

    #[test]
    fn times_a_zone_is_too_costly_to_convert_are_left_out() {
        // Observances without end: every lookup goes through all 5,000 of them, so the budget
        // covers some 2,400 lookups.
        let observances: String = (0..5_000)
            .map(|i| {
                format!(
                    "BEGIN:STANDARD\nTZOFFSETFROM:+0100\nTZOFFSETTO:+0100\nDTSTART:{}0101T000000\nEND:STANDARD\n",
                    1000 + i
                )
            })
            .collect();
        let events: String = (0..3_000)
            .map(|i| {
                format!(
                    "BEGIN:VEVENT\nUID:{}\nDTSTART;TZID=Heavy:20260105T090000\nEND:VEVENT\n",
                    i
                )
            })
            .collect();
        let ics = format!(
            "BEGIN:VTIMEZONE\nTZID:Heavy\n{}END:VTIMEZONE\n{}BEGIN:VEVENT\nUID:utc\nDTSTART:20260105T090000Z\nEND:VEVENT\n",
            observances, events
        );
        let window = (utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        let mut expansion = Expansion::new(window.0, window.1);
        let parsed = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(expansion.truncated());
        // The events converted before the budget ran out are there, at the right time. The
        // rest are left out rather than read as UTC, an hour off. A UTC event after them is
        // unaffected.
        let converted = MAX_ZONE_STEPS / 5_000;
        assert_eq!(parsed.len(), converted + 1);
        assert!(
            parsed[..converted]
                .iter()
                .all(|e| e.start == utc("2026-01-05T08:00:00Z"))
        );
        assert_eq!(parsed[converted].uid, "utc");
        // An ICS file says so too.
        let file = parse_ics_str(&ics, "c");
        assert!(file.incomplete);
        assert_eq!(file.events.len(), converted + 1);
        assert!(
            file.events[..converted]
                .iter()
                .all(|e| e.start == utc("2026-01-05T08:00:00Z"))
        );
        assert!(!parse_ics_str(&weekly_series(), "c").incomplete);
    }

    #[test]
    fn a_series_cut_short_by_the_zone_budget_keeps_only_correct_occurrences() {
        let window = (utc("2026-03-01T00:00:00Z"), utc("2026-04-07T00:00:00Z"));
        let mut full = Expansion::new(window.0, window.1);
        let all = parse_ics_expanded(&weekly_series(), "cal", None, &mut full);
        assert!(!full.truncated());
        let key = |e: &CalendarEvent| (e.id.clone(), e.start, e.end, e.summary.clone());
        let all: HashSet<_> = all.iter().map(key).collect();
        let cost = MAX_ZONE_STEPS - full.zone_steps_left;
        // The budget runs out at every point of the series: at the moved occurrence's
        // RECURRENCE-ID, DTSTART, EXDATE, in the middle of the walk and at the RDATE. Whatever
        // is left out, nothing comes back at a wrong time or twice (the moved occurrence also
        // at its old time, or the cancelled one).
        for budget in 0..=cost {
            let mut expansion = Expansion {
                zone_steps_left: budget,
                ..Expansion::new(window.0, window.1)
            };
            let events = parse_ics_expanded(&weekly_series(), "cal", None, &mut expansion);
            let got: HashSet<_> = events.iter().map(key).collect();
            assert_eq!(got.len(), events.len(), "budget {}", budget);
            assert!(got.is_subset(&all), "budget {}: {:?}", budget, got);
            assert_eq!(expansion.truncated(), got != all, "budget {}", budget);
        }
    }

    #[test]
    fn occurrences_long_before_the_window_need_no_zone_lookup() {
        let window = (utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let expand = |start: &str| {
            let ics = format!(
                "{}BEGIN:VEVENT\nUID:d\nDTSTART;TZID=Europe/Berlin:{}T090000\nRRULE:FREQ=DAILY;COUNT=100000\nEND:VEVENT\n",
                BERLIN, start
            );
            let mut expansion = Expansion::new(window.0, window.1);
            let ids: Vec<String> = parse_ics_expanded(&ics, "c", None, &mut expansion)
                .into_iter()
                .map(|e| e.id)
                .collect();
            assert!(!expansion.truncated());
            (ids, MAX_ZONE_STEPS - expansion.zone_steps_left)
        };
        let (recent, recent_steps) = expand("20260225");
        let (old, old_steps) = expand("20150101");
        assert_eq!(recent.len(), 31);
        assert_eq!(recent[0], "c:d:20260301T080000Z");
        assert_eq!(old, recent);
        // Eleven years of daily occurrences before the window cost next to nothing: some
        // 4,000 zone lookups would be over 100,000 steps.
        assert!(
            old_steps < recent_steps + 100,
            "{} {}",
            old_steps,
            recent_steps
        );
    }

    #[test]
    fn expanded_text_is_bounded_by_the_byte_budget() {
        // One daily series with a 2 MiB description: a year of copies would be 730 MiB.
        let description = "x".repeat(2 << 20);
        let ics = format!(
            "BEGIN:VEVENT\nUID:d\nDTSTART:20260101T090000Z\nRRULE:FREQ=DAILY\nDESCRIPTION:{}\nEND:VEVENT\n",
            description
        );
        let window = (utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(expansion.truncated());
        let held: usize = events
            .iter()
            .map(|e| {
                e.id.len()
                    + e.uid.len()
                    + e.summary.len()
                    + e.description.as_ref().map_or(0, String::len)
            })
            .sum();
        assert!(held <= MAX_EXPANDED_BYTES, "{}", held);
        assert_eq!(events.len(), MAX_EXPANDED_BYTES / (2 << 20) - 1);
        // Short text costs little, and the budget is shared by the objects of one answer.
        let short = "BEGIN:VEVENT\nUID:s\nDTSTART:20260101T090000Z\nRRULE:FREQ=DAILY\nDESCRIPTION:Standup\nEND:VEVENT\n";
        // Id `c:s:20260101T090000Z`, calendar id, UID, "(no title)" and "Standup".
        let each = 20 + 1 + 1 + UNTITLED_EVENT.len() + "Standup".len();
        let mut shared = Expansion {
            bytes_left: 400 * each,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(parse_ics_expanded(short, "c", None, &mut shared).len(), 365);
        assert!(!shared.truncated());
        assert_eq!(parse_ics_expanded(short, "c", None, &mut shared).len(), 35);
        assert!(shared.truncated());
    }

    #[test]
    fn unknown_zones_are_reported_once() {
        let ics = "BEGIN:VEVENT\nUID:a\nDTSTART;TZID=Nowhere:20260105T090000\nEND:VEVENT\nBEGIN:VEVENT\nUID:b\nDTSTART;TZID=Nowhere:20260106T090000\nEND:VEVENT\n";
        let mut expansion =
            Expansion::new(utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        assert_eq!(parse_ics_expanded(ics, "c", None, &mut expansion).len(), 2);
        assert_eq!(expansion.unknown_zones().collect::<Vec<_>>(), ["Nowhere"]);
    }

    #[test]
    fn times_the_clocks_skip_take_the_offset_before_the_gap() {
        // Berlin skips 02:00-03:00 on 29 March 2026. RFC 5545 section 3.3.5 reads 02:30 with
        // the offset before the gap: 01:30 UTC, which is 03:30 CEST.
        let single = format!(
            "{}BEGIN:VEVENT\r\nUID:gap\r\nDTSTART;TZID=Europe/Berlin:20260329T023000\r\nEND:VEVENT\r\n",
            BERLIN
        );
        let events = parse_ics_str(&single, "c").events;
        assert_eq!(events[0].start, utc("2026-03-29T01:30:00Z"));
        assert_eq!(events[0].end, utc("2026-03-29T02:30:00Z"));

        // A nightly 02:30 series across the change, with the gap night cancelled or moved by
        // UTC times, which is how servers write EXDATE and RECURRENCE-ID.
        let series = |exdate: &str, moved: &str| {
            format!(
                "BEGIN:VCALENDAR\r\n{}BEGIN:VEVENT\r\nUID:nightly\r\nSUMMARY:Backup\r\nDTSTART;TZID=Europe/Berlin:20260325T023000\r\nDURATION:PT30M\r\nRRULE:FREQ=DAILY;COUNT=7\r\n{}END:VEVENT\r\n{}END:VCALENDAR\r\n",
                BERLIN, exdate, moved
            )
        };
        let expand = |ics: &str| {
            let mut expansion =
                Expansion::new(utc("2026-03-20T00:00:00Z"), utc("2026-04-10T00:00:00Z"));
            let mut events: Vec<_> = parse_ics_expanded(ics, "c", None, &mut expansion)
                .into_iter()
                .map(|e| (e.id, e.start.format("%dT%H%M").to_string()))
                .collect();
            events.sort();
            assert!(!expansion.truncated());
            events
        };
        let occurrence = |day: &str, utc: &str| (format!("c:nightly:202603{}Z", utc), day.into());
        let all = expand(&series("", ""));
        assert_eq!(
            all,
            [
                occurrence("25T0130", "25T013000"),
                occurrence("26T0130", "26T013000"),
                occurrence("27T0130", "27T013000"),
                occurrence("28T0130", "28T013000"),
                // The night of the change: 03:30 CEST.
                occurrence("29T0130", "29T013000"),
                // Then 02:30 CEST.
                occurrence("30T0030", "30T003000"),
                occurrence("31T0030", "31T003000"),
            ]
        );
        let cancelled = expand(&series("EXDATE:20260329T013000Z\r\n", ""));
        assert_eq!(cancelled.len(), 6);
        assert!(cancelled.iter().all(|(_, day)| !day.starts_with("29")));
        let moved = expand(&series(
            "",
            "BEGIN:VEVENT\r\nUID:nightly\r\nRECURRENCE-ID:20260329T013000Z\r\nSUMMARY:Backup (late)\r\nDTSTART:20260329T060000Z\r\nDURATION:PT30M\r\nEND:VEVENT\r\n",
        ));
        assert_eq!(moved.len(), 7);
        let gap_night: Vec<_> = moved
            .iter()
            .filter(|(_, day)| day.starts_with("29"))
            .collect();
        assert_eq!(gap_night, [&occurrence("29T0600", "29T013000")]);
        // The same cancellation written in local time matches too.
        let local = expand(&series("EXDATE;TZID=Europe/Berlin:20260329T023000\r\n", ""));
        assert_eq!(local, cancelled);
    }

    #[test]
    fn every_event_of_an_answer_is_paid_for() {
        let window = (utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        // Events sent as they are (what a server's `expand` answers with) count against the
        // instance budget like expanded occurrences.
        let plain: String = (0..5)
            .map(|i| {
                format!(
                    "BEGIN:VEVENT\nUID:p{}\nRECURRENCE-ID:2026010{}T090000Z\nDTSTART:2026010{}T090000Z\nEND:VEVENT\n",
                    i,
                    i + 1,
                    i + 1
                )
            })
            .collect();
        let mut few = Expansion {
            instances_left: 3,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(parse_ics_expanded(&plain, "c", None, &mut few).len(), 3);
        assert!(few.truncated());
        let mut roomy = Expansion::new(window.0, window.1);
        assert_eq!(parse_ics_expanded(&plain, "c", None, &mut roomy).len(), 5);
        assert!(!roomy.truncated());
        // And against the byte budget: id `c:p0:20260101T090000Z`, calendar id, UID and
        // "(no title)".
        let each = 21 + 1 + 2 + UNTITLED_EVENT.len();
        let mut short = Expansion {
            bytes_left: 4 * each,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(parse_ics_expanded(&plain, "c", None, &mut short).len(), 4);
        assert!(short.truncated());

        // Every event carries its resource's etag, and every copy of it is paid for: a daily
        // series with a 1 MiB etag cannot hold a year of copies (365 MiB).
        let etag = "e".repeat(1 << 20);
        let daily = "BEGIN:VEVENT\nUID:d\nDTSTART:20260101T090000Z\nRRULE:FREQ=DAILY\nEND:VEVENT\n";
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(daily, "c", Some(&etag), &mut expansion);
        assert!(expansion.truncated());
        // Id `c:d:20260101T090000Z`, calendar id, UID and "(no title)", then the etag.
        let each = 20 + 1 + 1 + UNTITLED_EVENT.len() + etag.len();
        assert_eq!(events.len(), MAX_EXPANDED_BYTES / each);
        let held: usize = events.iter().map(text_bytes).sum();
        assert!(held <= MAX_EXPANDED_BYTES, "{}", held);
        assert!(
            events
                .iter()
                .all(|e| e.etag.as_deref() == Some(etag.as_str()))
        );
        // Plain events carry it too.
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(&plain, "c", Some("\"v1\""), &mut expansion);
        assert!(events.iter().all(|e| e.etag.as_deref() == Some("\"v1\"")));

        // VEVENTs that could never all become events are not all parsed: past twice the
        // events still allowed, they are left out (and the answer is incomplete).
        let broken = "BEGIN:VEVENT\nSUMMARY:no UID\nEND:VEVENT\n".repeat(7);
        let mut expansion = Expansion {
            instances_left: 3,
            ..Expansion::new(window.0, window.1)
        };
        let events = parse_ics_expanded(&format!("{}{}", broken, plain), "c", None, &mut expansion);
        assert!(events.is_empty());
        assert!(expansion.truncated());
        assert_eq!(Document::parse(&broken, 6).events.len(), 6);
        assert!(Document::parse(&broken, 6).events_dropped);
        assert!(!Document::parse(&broken, 7).events_dropped);
        // A series whose occurrences are all moved yields none itself, which the margin
        // allows for.
        let moved = "BEGIN:VEVENT\nUID:m\nDTSTART:20260105T090000Z\nRRULE:FREQ=DAILY;COUNT=2\nEND:VEVENT\nBEGIN:VEVENT\nUID:m\nRECURRENCE-ID:20260105T090000Z\nDTSTART:20260105T100000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:m\nRECURRENCE-ID:20260106T090000Z\nDTSTART:20260106T100000Z\nEND:VEVENT\n";
        let mut expansion = Expansion {
            instances_left: 2,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(
            parse_ics_expanded(moved, "c", None, &mut expansion).len(),
            2
        );
        assert!(!expansion.truncated());
    }

    #[test]
    fn unexpanded_series_are_named_once() {
        let long = "A very long meeting title that goes on and on well past sixty characters";
        let ics = format!(
            "BEGIN:VEVENT\nUID:hourly\nSUMMARY:{}\nDTSTART:20260105T090000Z\nRRULE:FREQ=HOURLY\nEND:VEVENT\nBEGIN:VEVENT\nUID:hourly\nRECURRENCE-ID:20260105T100000Z\nSUMMARY:Moved\nDTSTART:20260105T103000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:weekno\nSUMMARY:Week 10\nDTSTART:20260302T090000Z\nRRULE:FREQ=YEARLY;BYWEEKNO=10\nEND:VEVENT\nBEGIN:VEVENT\nUID:fine\nDTSTART:20260105T090000Z\nRRULE:FREQ=WEEKLY;COUNT=2\nEND:VEVENT\n",
            long
        );
        let mut expansion =
            Expansion::new(utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        // First occurrences, the moved one and the supported series.
        assert_eq!(events.len(), 5);
        assert!(!expansion.truncated());
        let named: Vec<_> = expansion.unexpanded().collect();
        assert_eq!(named.len(), 2);
        assert_eq!(named[0], format!("{}...", &long[..60]));
        assert_eq!(named[1], "Week 10");
    }

    /// `(id, start, length, summary)` of each event, sorted by start.
    fn occurrences(events: &[CalendarEvent]) -> Vec<(String, String, Duration, String)> {
        let mut out: Vec<_> = events
            .iter()
            .map(|e| {
                (
                    e.id.clone(),
                    e.start.format("%Y-%m-%dT%H:%M").to_string(),
                    e.end - e.start,
                    e.summary.clone(),
                )
            })
            .collect();
        out.sort_by(|a, b| (&a.1, &a.0).cmp(&(&b.1, &b.0)));
        out
    }

    fn occurrence(
        id: &str,
        start: &str,
        length: Duration,
        summary: &str,
    ) -> (String, String, Duration, String) {
        (id.into(), start.into(), length, summary.into())
    }

    /// An irregular series as Apple Calendar exports it: no RRULE, its dates listed with RDATE
    /// (several on a line, and over several lines) in the series' zone, one of them cancelled
    /// (EXDATE) and one moved (an overriding VEVENT).
    fn irregular_series() -> String {
        format!(
            "BEGIN:VCALENDAR\r\nPRODID:-//Apple Inc.//macOS 15.0//EN\r\n{}BEGIN:VEVENT\r\nUID:4B1D-physio\r\nSUMMARY:Physio\r\nLOCATION:Clinic\r\nDTSTART;TZID=Europe/Berlin:20260303T170000\r\nDTEND;TZID=Europe/Berlin:20260303T174500\r\nRDATE;TZID=Europe/Berlin:20260310T170000,20260319T083000,20260326T170000\r\nRDATE;TZID=Europe/Berlin:20260402T170000\r\nRDATE;TZID=Europe/Berlin:20260416T170000\r\nEXDATE;TZID=Europe/Berlin:20260326T170000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:4B1D-physio\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260402T170000\r\nSUMMARY:Physio (moved)\r\nDTSTART;TZID=Europe/Berlin:20260403T090000\r\nDTEND;TZID=Europe/Berlin:20260403T094500\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            BERLIN
        )
    }

    #[test]
    fn series_of_rdates_alone_are_expanded() {
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-05-01T00:00:00Z"));
        let events = parse_ics_expanded(&irregular_series(), "cal", Some("\"e1\""), &mut expansion);
        assert!(!expansion.truncated());
        assert_eq!(expansion.unexpanded().len(), 0);
        let length = Duration::minutes(45);
        // 17:00 is 16:00 UTC in winter and 15:00 UTC from 29 March; 26 March is cancelled, and
        // 2 April moved to the morning of the 3rd. Occurrence ids are those an RRULE series
        // gets, so the moved one keeps its original start.
        assert_eq!(
            occurrences(&events),
            [
                occurrence(
                    "cal:4B1D-physio:20260303T160000Z",
                    "2026-03-03T16:00",
                    length,
                    "Physio"
                ),
                occurrence(
                    "cal:4B1D-physio:20260310T160000Z",
                    "2026-03-10T16:00",
                    length,
                    "Physio"
                ),
                occurrence(
                    "cal:4B1D-physio:20260319T073000Z",
                    "2026-03-19T07:30",
                    length,
                    "Physio"
                ),
                occurrence(
                    "cal:4B1D-physio:20260402T150000Z",
                    "2026-04-03T07:00",
                    length,
                    "Physio (moved)"
                ),
                occurrence(
                    "cal:4B1D-physio:20260416T150000Z",
                    "2026-04-16T15:00",
                    length,
                    "Physio"
                ),
            ]
        );
        assert!(
            events.iter().all(|e| !e.all_day
                && e.uid == "4B1D-physio"
                && e.etag.as_deref() == Some("\"e1\""))
        );
        assert!(
            events
                .iter()
                .filter(|e| e.summary == "Physio")
                .all(|e| e.location.as_deref() == Some("Clinic"))
        );
        // An ICS file still yields each VEVENT once, the series as its first occurrence.
        let plain = parse_ics_str(&irregular_series(), "cal").events;
        assert_eq!(plain.len(), 2);
        assert_eq!(plain[0].id, "cal:4B1D-physio");
        assert_eq!(plain[0].start, utc("2026-03-03T16:00:00Z"));

        // All-day series stay all-day: dates as keys, a day long unless DTEND says otherwise.
        let all_day = "BEGIN:VEVENT\nUID:market\nSUMMARY:Market\nDTSTART;VALUE=DATE:20260307\nRDATE;VALUE=DATE:20260321,20260404\nRDATE:20260418\nEND:VEVENT\nBEGIN:VEVENT\nUID:fair\nDTSTART;VALUE=DATE:20260310\nDTEND;VALUE=DATE:20260312\nRDATE;VALUE=DATE:20260410\nEND:VEVENT\n";
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-05-01T00:00:00Z"));
        let events = parse_ics_expanded(all_day, "cal", None, &mut expansion);
        assert!(events.iter().all(|e| e.all_day));
        let day = Duration::days(1);
        assert_eq!(
            occurrences(&events),
            [
                occurrence("cal:market:20260307", "2026-03-07T00:00", day, "Market"),
                occurrence(
                    "cal:fair:20260310",
                    "2026-03-10T00:00",
                    day * 2,
                    UNTITLED_EVENT
                ),
                occurrence("cal:market:20260321", "2026-03-21T00:00", day, "Market"),
                occurrence("cal:market:20260404", "2026-04-04T00:00", day, "Market"),
                occurrence(
                    "cal:fair:20260410",
                    "2026-04-10T00:00",
                    day * 2,
                    UNTITLED_EVENT
                ),
                occurrence("cal:market:20260418", "2026-04-18T00:00", day, "Market"),
            ]
        );
    }

    #[test]
    fn rdates_in_a_zone_follow_its_daylight_saving_changes() {
        // Berlin moves to CEST on 29 March 2026 (02:00-03:00 skipped) and back on 25 October
        // (02:00-03:00 repeated). Times are converted one by one, as RRULE occurrences are,
        // and a UTC value may sit in the same series.
        let ics = format!(
            "{}BEGIN:VEVENT\r\nUID:dst\r\nSUMMARY:Swim\r\nDTSTART;TZID=Europe/Berlin:20260328T093000\r\nDURATION:PT1H\r\nRDATE;TZID=Europe/Berlin:20260329T093000,20260329T023000,20261024T093000\r\nRDATE;TZID=Europe/Berlin:20261025T023000,20261025T093000\r\nRDATE:20260401T073000Z\r\nEND:VEVENT\r\n",
            BERLIN
        );
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-12-01T00:00:00Z"));
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        let hour = Duration::hours(1);
        assert_eq!(
            occurrences(&events),
            [
                // 09:30 CET...
                occurrence("c:dst:20260328T083000Z", "2026-03-28T08:30", hour, "Swim"),
                // ... 02:30 does not exist that night: the offset before the gap ...
                occurrence("c:dst:20260329T013000Z", "2026-03-29T01:30", hour, "Swim"),
                // ... 09:30 CEST ...
                occurrence("c:dst:20260329T073000Z", "2026-03-29T07:30", hour, "Swim"),
                occurrence("c:dst:20260401T073000Z", "2026-04-01T07:30", hour, "Swim"),
                occurrence("c:dst:20261024T073000Z", "2026-10-24T07:30", hour, "Swim"),
                // ... 02:30 happens twice: the first time, still CEST ...
                occurrence("c:dst:20261025T003000Z", "2026-10-25T00:30", hour, "Swim"),
                // ... and 09:30 CET again.
                occurrence("c:dst:20261025T083000Z", "2026-10-25T08:30", hour, "Swim"),
            ]
        );
    }

    /// RDATE PERIODs: explicit ends in UTC and in a zone (one across the change to summer
    /// time), durations, and periods whose end is unusable.
    fn period_series() -> String {
        format!(
            "{}BEGIN:VEVENT\r\nUID:periods\r\nSUMMARY:Office hours\r\nDTSTART:20260302T090000Z\r\nDTEND:20260302T100000Z\r\nRDATE;VALUE=PERIOD:20260303T090000Z/20260303T120000Z,20260304T130000Z/PT30M\r\nRDATE;VALUE=PERIOD;TZID=Europe/Berlin:20260305T090000/20260305T093000\r\nRDATE;VALUE=PERIOD;TZID=Europe/Berlin:20260328T220000/20260329T040000\r\nRDATE;VALUE=PERIOD:20260306T090000Z/20260306T080000Z,20260309T090000Z/-PT1H\r\nRDATE;VALUE=PERIOD:20260310T090000Z/soon\r\nRDATE:20260311T090000Z\r\nEND:VEVENT\r\n",
            BERLIN
        )
    }

    #[test]
    fn rdate_periods_give_their_occurrence_its_own_length() {
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(&period_series(), "c", None, &mut expansion);
        assert!(!expansion.truncated());
        let (hour, minutes) = (Duration::hours(1), Duration::minutes);
        let name = "Office hours";
        assert_eq!(
            occurrences(&events),
            [
                occurrence("c:periods:20260302T090000Z", "2026-03-02T09:00", hour, name),
                occurrence(
                    "c:periods:20260303T090000Z",
                    "2026-03-03T09:00",
                    hour * 3,
                    name
                ),
                occurrence(
                    "c:periods:20260304T130000Z",
                    "2026-03-04T13:00",
                    minutes(30),
                    name
                ),
                // 09:00-09:30 CET.
                occurrence(
                    "c:periods:20260305T080000Z",
                    "2026-03-05T08:00",
                    minutes(30),
                    name
                ),
                // An end before the start, a negative duration and an end that does not parse
                // leave the series' length.
                occurrence("c:periods:20260306T090000Z", "2026-03-06T09:00", hour, name),
                occurrence("c:periods:20260309T090000Z", "2026-03-09T09:00", hour, name),
                occurrence("c:periods:20260310T090000Z", "2026-03-10T09:00", hour, name),
                occurrence("c:periods:20260311T090000Z", "2026-03-11T09:00", hour, name),
                // 22:00 CET to 04:00 CEST: six hours on the clock, five in fact.
                occurrence(
                    "c:periods:20260328T210000Z",
                    "2026-03-28T21:00",
                    hour * 5,
                    name
                ),
            ]
        );
    }

    #[test]
    fn exdate_removes_rdates_and_dtstart() {
        let ics = format!(
            "{}BEGIN:VEVENT\r\nUID:ex\r\nDTSTART:20260302T090000Z\r\nRDATE:20260303T090000Z,20260304T090000Z,20260305T090000Z\r\nEXDATE:20260303T090000Z\r\nEXDATE;TZID=Europe/Berlin:20260305T100000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:ex-start\r\nSUMMARY:Market\r\nDTSTART;VALUE=DATE:20260302\r\nRDATE;VALUE=DATE:20260309,20260316\r\nEXDATE;VALUE=DATE:20260302\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:ex-period\r\nDTSTART:20260302T120000Z\r\nRDATE;VALUE=PERIOD:20260303T120000Z/PT3H\r\nEXDATE:20260303T120000Z\r\nEND:VEVENT\r\n",
            BERLIN
        );
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        let mut ids: Vec<_> = events.iter().map(|e| e.id.as_str()).collect();
        ids.sort();
        // The EXDATE in Berlin time is 09:00 UTC too; a cancelled DTSTART leaves only the
        // RDATEs, and a PERIOD is cancelled by its start.
        assert_eq!(
            ids,
            [
                "c:ex-period:20260302T120000Z",
                "c:ex-start:20260309",
                "c:ex-start:20260316",
                "c:ex:20260302T090000Z",
                "c:ex:20260304T090000Z",
            ]
        );
    }

    #[test]
    fn an_overriding_vevent_replaces_its_rdate_occurrence() {
        let ics = format!(
            "{}BEGIN:VEVENT\r\nUID:tasting\r\nSUMMARY:Tasting\r\nDTSTART;TZID=Europe/Berlin:20260303T190000\r\nDURATION:PT2H\r\nRDATE;TZID=Europe/Berlin:20260310T190000,20260317T190000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:tasting\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260310T190000\r\nSUMMARY:Tasting (moved)\r\nDTSTART;TZID=Europe/Berlin:20260311T200000\r\nDURATION:PT2H\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:tasting\r\nRECURRENCE-ID:20260317T180000Z\r\nSUMMARY:Tasting\r\nSTATUS:CANCELLED\r\nDTSTART:20260317T180000Z\r\nDURATION:PT2H\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:tasting\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260303T190000\r\nSUMMARY:Tasting (early)\r\nDTSTART;TZID=Europe/Berlin:20260303T180000\r\nDURATION:PT2H\r\nEND:VEVENT\r\n",
            BERLIN
        );
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        let two = Duration::hours(2);
        // Each occurrence once, as its own VEVENT says: DTSTART and an RDATE moved (matched
        // in local time or in UTC), and one cancelled.
        assert_eq!(
            occurrences(&events),
            [
                occurrence(
                    "c:tasting:20260303T180000Z",
                    "2026-03-03T17:00",
                    two,
                    "Tasting (early)"
                ),
                occurrence(
                    "c:tasting:20260310T180000Z",
                    "2026-03-11T19:00",
                    two,
                    "Tasting (moved)"
                ),
                occurrence(
                    "c:tasting:20260317T180000Z",
                    "2026-03-17T18:00",
                    two,
                    "Tasting"
                ),
            ]
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.status == EventStatus::Cancelled)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .find(|e| e.id == "c:tasting:20260317T180000Z")
                .unwrap()
                .status,
            EventStatus::Cancelled
        );
    }

    #[test]
    fn a_series_of_rdates_in_an_answer_meant_to_be_expanded_is_left_to_be_fetched_whole() {
        let window = || Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let spent = |e: &Expansion| {
            (
                e.instances_left,
                e.bytes_left,
                e.rule_steps_left,
                e.zone_steps_left,
                e.truncated,
            )
        };
        // Radicale's answer to `expand` for a series of RDATEs: its master alone (with the
        // VTIMEZONE), without the VEVENTs that move one occurrence and cancel another.
        // Expanded here, the moved one would come at its old time and the cancelled one as on.
        let master = format!(
            "{}BEGIN:VEVENT\r\nUID:tasting\r\nSUMMARY:Tasting\r\nDTSTART;TZID=Europe/Berlin:20260303T190000\r\nDURATION:PT2H\r\nRDATE;TZID=Europe/Berlin:20260310T190000,20260317T190000\r\nEND:VEVENT\r\n",
            BERLIN
        );
        let overrides = "BEGIN:VEVENT\r\nUID:tasting\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260310T190000\r\nSUMMARY:Tasting (moved)\r\nDTSTART;TZID=Europe/Berlin:20260311T200000\r\nDURATION:PT2H\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:tasting\r\nRECURRENCE-ID:20260317T180000Z\r\nSTATUS:CANCELLED\r\nDTSTART:20260317T180000Z\r\nEND:VEVENT\r\n";
        // Not read, and nothing paid for. With its VEVENTs sent along (a server ignoring
        // `expand`), the same: an answer cannot show whether any were left out.
        for ics in [master.clone(), format!("{}{}", master, overrides)] {
            let mut expansion = window();
            let before = spent(&expansion);
            assert!(parse_ics_server_expanded(&ics, "c", None, &mut expansion).is_none());
            assert_eq!(spent(&expansion), before);
            assert_eq!(expansion.unknown_zones().count(), 0);
        }
        // Whole, it is what `parse_ics_expanded` makes of it.
        let whole = parse_ics_expanded(
            &format!("{}{}", master, overrides),
            "c",
            None,
            &mut window(),
        );
        assert_eq!(
            occurrences(&whole),
            [
                occurrence(
                    "c:tasting:20260303T180000Z",
                    "2026-03-03T18:00",
                    Duration::hours(2),
                    "Tasting"
                ),
                occurrence(
                    "c:tasting:20260310T180000Z",
                    "2026-03-11T19:00",
                    Duration::hours(2),
                    "Tasting (moved)"
                ),
                occurrence(
                    "c:tasting:20260317T180000Z",
                    "2026-03-17T18:00",
                    Duration::hours(1),
                    UNTITLED_EVENT
                ),
            ]
        );

        // Everything else is read as `parse_ics_expanded` reads it: occurrences the server
        // expanded, a series with an RRULE sent as is (RDATEs too), an occurrence of its own
        // that lists an RDATE, an event, a zone whose observances use RDATE, and a series of
        // RDATEs without a UID (nothing to fetch; it has no events either way).
        let others = [
            "BEGIN:VEVENT\r\nUID:w\r\nRECURRENCE-ID:20260302T090000Z\r\nDTSTART:20260302T090000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:w\r\nRECURRENCE-ID:20260309T090000Z\r\nDTSTART:20260309T100000Z\r\nEND:VEVENT\r\n".to_string(),
            "BEGIN:VEVENT\r\nUID:r\r\nDTSTART:20260302T090000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nRDATE:20260320T090000Z\r\nEND:VEVENT\r\n".to_string(),
            "BEGIN:VEVENT\r\nUID:o\r\nRECURRENCE-ID:20260302T090000Z\r\nDTSTART:20260303T090000Z\r\nRDATE:20260320T090000Z\r\nEND:VEVENT\r\n".to_string(),
            "BEGIN:VEVENT\r\nUID:e\r\nDTSTART:20260305T090000Z\r\nEND:VEVENT\r\n".to_string(),
            "BEGIN:VTIMEZONE\r\nTZID:Old\r\nBEGIN:STANDARD\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0100\r\nDTSTART:19700101T000000\r\nRDATE:19800101T000000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:z\r\nDTSTART;TZID=Old:20260305T090000\r\nEND:VEVENT\r\n".to_string(),
            "BEGIN:VEVENT\r\nDTSTART:20260302T090000Z\r\nRDATE:20260320T090000Z\r\nEND:VEVENT\r\n".to_string(),
        ];
        for ics in others {
            let (mut here, mut there) = (window(), window());
            let expected = parse_ics_expanded(&ics, "c", Some("\"e1\""), &mut here);
            let read = parse_ics_server_expanded(&ics, "c", Some("\"e1\""), &mut there)
                .unwrap_or_else(|| panic!("left to be fetched: {}", ics));
            assert_eq!(occurrences(&read), occurrences(&expected), "{}", ics);
            assert!(read.iter().all(|e| e.etag.as_deref() == Some("\"e1\"")));
            assert_eq!(spent(&there), spent(&here), "{}", ics);
        }
    }

    #[test]
    fn rdate_occurrences_are_clipped_to_the_window() {
        let ics = "BEGIN:VEVENT\nUID:clip\nDTSTART:20250601T090000Z\nDURATION:PT2H\nRDATE:20260228T200000Z,20260228T235000Z,20260315T090000Z\nRDATE:20260331T235000Z,20260401T000000Z,20270101T090000Z\nRDATE;VALUE=PERIOD:20260220T000000Z/P10D,20260210T000000Z/P10D\nEND:VEVENT\nBEGIN:VEVENT\nUID:instant\nDTSTART:20260301T000000Z\nRDATE:20260228T235959Z,20260401T000000Z\nDURATION:PT0S\nEND:VEVENT\n";
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        let mut ids: Vec<_> = events.iter().map(|e| e.id.as_str()).collect();
        ids.sort();
        // What overlaps the window as a CalDAV time-range does: DTSTART a year before, what
        // ends before 1 March (20:00-22:00, a period to the 20th) and what starts at the end
        // or later are left out; what starts before and ends inside (23:50-01:50, a period
        // from 20 February to 2 March) is in. A zero-length occurrence counts at the start.
        assert_eq!(
            ids,
            [
                "c:clip:20260220T000000Z",
                "c:clip:20260228T235000Z",
                "c:clip:20260315T090000Z",
                "c:clip:20260331T235000Z",
                "c:instant:20260301T000000Z",
            ]
        );
    }

    #[test]
    fn a_huge_rdate_list_is_paid_for_from_the_budgets() {
        let window = (utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        // 200,000 occurrences a minute apart, all in the window.
        let first = utc("2026-01-01T00:00:00Z");
        let values: Vec<String> = (0..200_000)
            .map(|i| {
                (first + Duration::minutes(i))
                    .format("%Y%m%dT%H%M%SZ")
                    .to_string()
            })
            .collect();
        let ics = format!(
            "BEGIN:VEVENT\nUID:many\nDTSTART:20260101T000000Z\nRDATE:{}\nEND:VEVENT\n",
            values.join(",")
        );
        // Each costs an instance, as RRULE occurrences do...
        let mut expansion = Expansion::new(window.0, window.1);
        assert_eq!(
            parse_ics_expanded(&ics, "c", None, &mut expansion).len(),
            MAX_INSTANCES
        );
        assert!(expansion.truncated());
        let mut few = Expansion {
            instances_left: 1_000,
            ..Expansion::new(window.0, window.1)
        };
        assert_eq!(parse_ics_expanded(&ics, "c", None, &mut few).len(), 1_000);
        assert!(few.truncated());
        // ... and its text. Id `c:many:20260101T000000Z`, calendar id, UID and "(no title)".
        let each = 23 + 1 + 4 + UNTITLED_EVENT.len();
        let mut short = Expansion {
            bytes_left: 50 * each,
            ..Expansion::new(window.0, window.1)
        };
        let events = parse_ics_expanded(&ics, "c", None, &mut short);
        assert_eq!(events.len(), 50);
        assert!(short.truncated());
        assert_eq!(events.iter().map(text_bytes).sum::<usize>(), 50 * each);
        // The same date listed over and over is one occurrence.
        let repeated = format!(
            "BEGIN:VEVENT\nUID:same\nDTSTART:20260101T000000Z\nRDATE:{}\nEND:VEVENT\n",
            vec!["20260102T000000Z"; 200_000].join(",")
        );
        let mut expansion = Expansion::new(window.0, window.1);
        assert_eq!(
            parse_ics_expanded(&repeated, "c", None, &mut expansion).len(),
            2
        );
        assert!(!expansion.truncated());

        // Times in a zone cost zone steps: once they run out, the rest are left out rather
        // than read as UTC.
        let local: Vec<String> = (0..20_000)
            .map(|i| {
                (first + Duration::minutes(i))
                    .naive_utc()
                    .format("%Y%m%dT%H%M%S")
                    .to_string()
            })
            .collect();
        let zoned = format!(
            "{}BEGIN:VEVENT\nUID:zoned\nDTSTART:20260101T000000Z\nRDATE;TZID=Europe/Berlin:{}\nEND:VEVENT\n",
            BERLIN,
            local.join(",")
        );
        let mut expansion = Expansion {
            zone_steps_left: 20_000,
            ..Expansion::new(window.0, window.1)
        };
        let events = parse_ics_expanded(&zoned, "c", None, &mut expansion);
        assert!(expansion.truncated());
        assert!((100..20_000).contains(&events.len()), "{}", events.len());
        // January in Berlin is UTC+1, so each is an hour before the time as written (DTSTART,
        // in UTC, is also the listed 01:00).
        let written: HashSet<DateTime<Utc>> = (0..20_000)
            .map(|i| first + Duration::minutes(i) - Duration::hours(1))
            .collect();
        for e in &events {
            assert!(written.contains(&e.start), "{}", e.start);
            assert_eq!(
                e.id,
                format!("c:zoned:{}", e.start.format("%Y%m%dT%H%M%SZ"))
            );
        }
    }

    #[test]
    fn rdates_long_outside_the_window_need_no_zone_lookup() {
        let window = (utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let expand = |past: &str, future: &str| {
            let ics = format!(
                "{}BEGIN:VEVENT\nUID:r\nDTSTART;TZID=Europe/Berlin:20260302T090000\nRDATE;TZID=Europe/Berlin:{}20260309T090000,20300101T090000{}\nEND:VEVENT\n",
                BERLIN, past, future
            );
            let mut expansion = Expansion::new(window.0, window.1);
            let mut ids: Vec<String> = parse_ics_expanded(&ics, "c", None, &mut expansion)
                .into_iter()
                .map(|e| e.id)
                .collect();
            ids.sort();
            assert!(!expansion.truncated());
            (ids, MAX_ZONE_STEPS - expansion.zone_steps_left)
        };
        let (recent, recent_steps) = expand("", "");
        // Twenty years of dates every ten hours, before the window or after it: some 17,500
        // lookups would be over half a million steps.
        let every_ten_hours = |from: i32| {
            let first = NaiveDate::from_ymd_opt(from, 1, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap();
            (0..17_500).map(move |i| (first + Duration::hours(10 * i)).format("%Y%m%dT%H%M%S"))
        };
        let past: String = every_ten_hours(2005).map(|t| format!("{},", t)).collect();
        let future: String = every_ten_hours(2027).map(|t| format!(",{}", t)).collect();
        let (old, old_steps) = expand(&past, "");
        let (late, late_steps) = expand("", &future);
        assert_eq!(recent, ["c:r:20260302T080000Z", "c:r:20260309T080000Z"]);
        assert_eq!(old, recent);
        assert_eq!(old_steps, recent_steps);
        assert_eq!(late, recent);
        assert_eq!(late_steps, recent_steps);
    }

    #[test]
    fn rdate_series_cut_short_by_the_zone_budget_keep_only_correct_occurrences() {
        let window = (utc("2026-03-01T00:00:00Z"), utc("2026-05-01T00:00:00Z"));
        for ics in [irregular_series(), period_series()] {
            let mut full = Expansion::new(window.0, window.1);
            let all = parse_ics_expanded(&ics, "cal", None, &mut full);
            assert!(!full.truncated());
            let key = |e: &CalendarEvent| (e.id.clone(), e.start, e.end, e.summary.clone());
            let all: HashSet<_> = all.iter().map(key).collect();
            let cost = MAX_ZONE_STEPS - full.zone_steps_left;
            assert!(cost > 0);
            // Wherever the budget runs out (the moved occurrence's RECURRENCE-ID, DTSTART,
            // EXDATE, an RDATE or a PERIOD's end), nothing comes back at a wrong time, with a
            // wrong length or twice.
            for budget in 0..=cost {
                let mut expansion = Expansion {
                    zone_steps_left: budget,
                    ..Expansion::new(window.0, window.1)
                };
                let events = parse_ics_expanded(&ics, "cal", None, &mut expansion);
                let got: HashSet<_> = events.iter().map(key).collect();
                assert_eq!(got.len(), events.len(), "budget {}", budget);
                assert!(got.is_subset(&all), "budget {}: {:?}", budget, got);
                assert_eq!(expansion.truncated(), got != all, "budget {}", budget);
            }
        }
    }

    #[test]
    fn a_series_with_an_override_that_cannot_be_placed_is_left_out() {
        // The RECURRENCE-IDs need a zone lookup the budget cannot pay for: which occurrence
        // each VEVENT replaces is unknown, so its series is left out, whether it is a list of
        // RDATEs or the first occurrence of a rule that is not expanded (the answer is marked
        // incomplete, and nothing stored is pruned). A UTC event beside them is unaffected.
        let ics = format!(
            "{}BEGIN:VEVENT\nUID:r\nDTSTART:20260302T090000Z\nRDATE:20260303T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:r\nRECURRENCE-ID;TZID=Europe/Berlin:20260303T100000\nDTSTART:20260303T110000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:hourly\nDTSTART:20260302T090000Z\nRRULE:FREQ=HOURLY\nEND:VEVENT\nBEGIN:VEVENT\nUID:hourly\nRECURRENCE-ID;TZID=Europe/Berlin:20260302T100000\nDTSTART:20260302T100000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:plain\nDTSTART:20260302T120000Z\nEND:VEVENT\n",
            BERLIN
        );
        let mut expansion = Expansion {
            zone_steps_left: 0,
            ..Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"))
        };
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(expansion.truncated());
        assert_eq!(
            events.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["c:plain"]
        );
    }

    #[test]
    fn an_unexpanded_series_keeps_its_first_occurrence_unless_cancelled_or_moved() {
        let ics = format!(
            "{}BEGIN:VEVENT\nUID:cancelled\nSUMMARY:Week 10\nDTSTART:20260302T090000Z\nRRULE:FREQ=YEARLY;BYWEEKNO=10\nEXDATE:20260302T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:cancelled-local\nDTSTART;TZID=Europe/Berlin:20260302T090000\nRRULE:FREQ=HOURLY\nEXDATE;TZID=Europe/Berlin:20260302T090000\nEND:VEVENT\nBEGIN:VEVENT\nUID:moved\nDTSTART:20260302T090000Z\nRRULE:FREQ=YEARLY;BYWEEKNO=10\nEND:VEVENT\nBEGIN:VEVENT\nUID:moved\nRECURRENCE-ID:20260302T090000Z\nSUMMARY:Moved\nDTSTART:20260302T110000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:kept\nDTSTART;TZID=Europe/Berlin:20260302T090000\nRRULE:FREQ=YEARLY;BYWEEKNO=10\nEXDATE;TZID=Europe/Berlin:20270301T090000\nRDATE:20260303T090000Z\nEND:VEVENT\n",
            BERLIN
        );
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        // A cancelled first occurrence (in UTC or in local time) leaves nothing, a moved one
        // only its own VEVENT; one that is neither stays, under the id it always had.
        assert_eq!(
            occurrences(&events),
            [
                occurrence(
                    "c:kept",
                    "2026-03-02T08:00",
                    Duration::hours(1),
                    UNTITLED_EVENT
                ),
                occurrence(
                    "c:moved:20260302T090000Z",
                    "2026-03-02T11:00",
                    Duration::hours(1),
                    "Moved"
                ),
            ]
        );
        // Each is still named, as its later occurrences are missing, and the answer is
        // otherwise complete.
        assert_eq!(
            expansion.unexpanded().collect::<Vec<_>>(),
            ["Week 10", "cancelled-local", "kept", "moved"]
        );
        assert!(!expansion.truncated());
    }

    #[test]
    fn events_without_recurrence_are_unaffected() {
        // A one-off far outside the window, one with a stray EXDATE of its own start, and a
        // moved occurrence whose series is elsewhere: each is sent as it is, as before.
        let ics = "BEGIN:VEVENT\nUID:old\nDTSTART:20200101T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:stray\nDTSTART:20260302T090000Z\nEXDATE:20260302T090000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:lone\nRECURRENCE-ID:20260303T090000Z\nDTSTART:20260303T100000Z\nRDATE:20260304T090000Z\nEND:VEVENT\n";
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        assert_eq!(
            occurrences(&events),
            [
                occurrence(
                    "c:old",
                    "2020-01-01T09:00",
                    Duration::hours(1),
                    UNTITLED_EVENT
                ),
                occurrence(
                    "c:stray",
                    "2026-03-02T09:00",
                    Duration::hours(1),
                    UNTITLED_EVENT
                ),
                occurrence(
                    "c:lone:20260303T090000Z",
                    "2026-03-03T10:00",
                    Duration::hours(1),
                    UNTITLED_EVENT
                ),
            ]
        );
    }

    #[test]
    fn values_of_one_list_share_its_tzid() {
        // A long TZID is kept once per RDATE or EXDATE line, not once per value: 2,000 copies
        // of 64 KiB would be 128 MiB.
        let zone = "Z".repeat(1 << 16);
        let values = vec!["20260105T090000"; 1_000].join(",");
        let ics = format!(
            "BEGIN:VEVENT\nUID:a\nDTSTART:20260105T090000Z\nRDATE;TZID={z}:{v}\nEXDATE;TZID={z}:{v}\nEND:VEVENT\n",
            z = zone,
            v = values
        );
        let doc = Document::parse(&ics, usize::MAX);
        let event = &doc.events[0];
        let tzid = |t: &IcsTime| match t {
            IcsTime::Local(_, Some(tzid)) => tzid.clone(),
            other => panic!("{:?}", other),
        };
        let rdates: Vec<Rc<str>> = event.rdates.iter().map(|r| tzid(&r.start)).collect();
        let exdates: Vec<Rc<str>> = event.exdates.iter().map(tzid).collect();
        assert_eq!((rdates.len(), exdates.len()), (1_000, 1_000));
        assert_eq!(&*rdates[0], zone.as_str());
        assert!(rdates.iter().all(|t| Rc::ptr_eq(t, &rdates[0])));
        assert!(exdates.iter().all(|t| Rc::ptr_eq(t, &exdates[0])));
    }

    #[test]
    fn each_occurrence_id_comes_once_in_an_all_day_series() {
        // An all-day series keys its occurrences by date, so RDATE times on a date it already
        // has (two on one day, one on DTSTART's day, one on a day of its rule) would give an
        // id twice, and storing both would keep only the last. The first occurrence with an
        // id is kept: DTSTART and the rule's before the RDATEs, and RDATEs in their order.
        let ics = "BEGIN:VEVENT\nUID:m\nSUMMARY:Market\nDTSTART;VALUE=DATE:20260307\nRDATE:20260321T090000Z,20260321T150000Z\nRDATE:20260307T100000Z\nEND:VEVENT\nBEGIN:VEVENT\nUID:w\nDTSTART;VALUE=DATE:20260302\nRRULE:FREQ=WEEKLY;COUNT=3\nRDATE:20260309T120000Z,20260310T120000Z\nEND:VEVENT\n";
        let mut expansion =
            Expansion::new(utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let events = parse_ics_expanded(ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        let ids: HashSet<&str> = events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids.len(), events.len());
        let day = Duration::days(1);
        assert_eq!(
            occurrences(&events),
            [
                occurrence("c:w:20260302", "2026-03-02T00:00", day, UNTITLED_EVENT),
                occurrence("c:m:20260307", "2026-03-07T00:00", day, "Market"),
                occurrence("c:w:20260309", "2026-03-09T00:00", day, UNTITLED_EVENT),
                occurrence("c:w:20260310", "2026-03-10T12:00", day, UNTITLED_EVENT),
                occurrence("c:w:20260316", "2026-03-16T00:00", day, UNTITLED_EVENT),
                occurrence("c:m:20260321", "2026-03-21T09:00", day, "Market"),
            ]
        );
    }

    #[test]
    fn a_tzid_too_long_to_look_up_is_not_converted() {
        // Values of one list share their TZID, so a long one costs little memory; converting
        // each value would still look the name up (and note it, without a VTIMEZONE), so a
        // name too long for any real zone is refused before that, value by value, rather
        // than read as UTC. Its occurrences are unknown: left out, and the answer incomplete.
        let long = "Q".repeat(MAX_TZID_BYTES + 1);
        let values = vec!["20260105T090000"; 10_000].join(",");
        let window = (utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"));
        let ics = format!(
            "BEGIN:VEVENT\nUID:a\nDTSTART:20260105T090000Z\nRDATE;TZID={z}:{v}\nEXDATE;TZID={z}:20260106T090000\nEND:VEVENT\nBEGIN:VEVENT\nUID:b\nDTSTART:20260105T090000Z\nRDATE;TZID={z}:{v}\nEND:VEVENT\nBEGIN:VEVENT\nUID:plain\nDTSTART:20260105T100000Z\nEND:VEVENT\n",
            z = long,
            v = values
        );
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        // `a` cannot tell whether its EXDATE cancels anything and is left out; `b` keeps
        // DTSTART, in UTC.
        let mut ids: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, ["c:b:20260105T090000Z", "c:plain"]);
        assert!(expansion.truncated());
        assert_eq!(expansion.unknown_zones().count(), 0);
        assert_eq!(expansion.zone_steps_left, MAX_ZONE_STEPS);

        // Not even a VTIMEZONE of that name makes it one to convert in, in CalDAV data or in
        // an ICS file.
        let zoned = format!(
            "BEGIN:VTIMEZONE\nTZID:{z}\nBEGIN:STANDARD\nDTSTART:19700101T000000\nTZOFFSETFROM:+0100\nTZOFFSETTO:+0100\nEND:STANDARD\nEND:VTIMEZONE\nBEGIN:VEVENT\nUID:z\nDTSTART;TZID={z}:20260105T090000\nEND:VEVENT\n",
            z = long
        );
        let mut expansion = Expansion::new(window.0, window.1);
        assert!(parse_ics_expanded(&zoned, "c", None, &mut expansion).is_empty());
        assert!(expansion.truncated());
        let file = parse_ics_str(&zoned, "c");
        assert!(file.events.is_empty());
        assert!(file.incomplete);

        // One byte shorter is a zone name like any other: converted with its VTIMEZONE, or
        // read as UTC and reported without one.
        let longest = "Q".repeat(MAX_TZID_BYTES);
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(&zoned.replace(&long, &longest), "c", None, &mut expansion);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].start, utc("2026-01-05T08:00:00Z"));
        let unknown = format!(
            "BEGIN:VEVENT\nUID:u\nDTSTART;TZID={z}:20260105T090000\nRDATE;TZID={z}:{v}\nEND:VEVENT\n",
            z = longest,
            v = values
        );
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(&unknown, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].start, utc("2026-01-05T09:00:00Z"));
        assert_eq!(
            expansion.unknown_zones().collect::<Vec<_>>(),
            [longest.as_str()]
        );
    }

    #[test]
    fn an_unexpanded_series_whose_exdate_cannot_be_converted_is_left_out() {
        // A rule that is not expanded keeps its first occurrence unless an EXDATE cancels it.
        // When an EXDATE's zone cannot be gone through, whether it does is unknown: the
        // occurrence is left out rather than shown although it may be cancelled, and the
        // answer is incomplete, so a stored one is kept.
        let ics = format!(
            "{}BEGIN:VEVENT\nUID:cancelled\nDTSTART:20260302T080000Z\nRRULE:FREQ=HOURLY\nEXDATE;TZID=Europe/Berlin:20260302T090000\nEND:VEVENT\nBEGIN:VEVENT\nUID:kept\nDTSTART:20260302T080000Z\nRRULE:FREQ=HOURLY\nEXDATE;TZID=Europe/Berlin:20260303T090000\nEND:VEVENT\n",
            BERLIN
        );
        let window = (utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"));
        let mut expansion = Expansion::new(window.0, window.1);
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(!expansion.truncated());
        assert_eq!(
            events.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["c:kept"]
        );
        let mut expansion = Expansion {
            zone_steps_left: 0,
            ..Expansion::new(window.0, window.1)
        };
        let events = parse_ics_expanded(&ics, "c", None, &mut expansion);
        assert!(expansion.truncated());
        assert!(events.is_empty(), "{:?}", events);
        assert_eq!(
            expansion.unexpanded().collect::<Vec<_>>(),
            ["cancelled", "kept"]
        );
    }

    /// Line unfolding as it was done before it became lazy: every line up front.
    fn unfold_eagerly(content: &str) -> Vec<String> {
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

    #[test]
    fn unfolding_copies_only_folded_lines() {
        let content = " leading\r\n continued\r\nA:1\r\nB:2\r\n \r\n\tthree\r\n\r\nC:3";
        let lines: Vec<Cow<'_, str>> = unfold_ics_lines(content).collect();
        assert_eq!(lines, [" leadingcontinued", "A:1", "B:2three", "", "C:3"]);
        let copied: Vec<bool> = lines.iter().map(|l| matches!(l, Cow::Owned(_))).collect();
        assert_eq!(copied, [true, false, true, false, false]);
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
        fn unfolding_lazily_matches_unfolding_everything(s in "([ \t]?[A-Z:]{0,3}(\r?\n)?){0,12}") {
            let lazy: Vec<String> = unfold_ics_lines(&s).map(Cow::into_owned).collect();
            proptest::prop_assert_eq!(lazy, unfold_eagerly(&s));
        }

        #[test]
        fn ics_parser_never_panics(s in "(BEGIN:VEVENT|END:VEVENT|BEGIN:VALARM|END:VALARM|BEGIN:VTIMEZONE|END:VTIMEZONE|BEGIN:STANDARD|END:STANDARD|TZID:Z1|TZOFFSETTO:[-+][0-9]{4}|UID:x|DTSTART(;TZID=Z1)?:[0-9TZ]{0,16}|DTEND:[0-9TZ]{0,16}|DURATION:[-+PTWDHMS0-9]{0,12}|RECURRENCE-ID:[0-9]{0,8}|RRULE:FREQ=(DAILY|WEEKLY|MONTHLY|YEARLY)(;COUNT=[0-9]{1,3})?|EXDATE:[0-9TZ]{0,16}|DTSTART:20000[1-3][0-2][0-9]T[0-2][0-9]0000Z?|RDATE(;VALUE=PERIOD)?(;TZID=Z1)?:(20000[1-3][0-2][0-9](T[0-2][0-9]0000Z?)?(/([-+]?P(T[0-9]{1,3}H|[0-9]{1,8}W)|20000[1-3][0-2][0-9]T[0-2][0-9]0000Z?))?,?){0,3}|[ -~]{0,20}|\r?\n){0,40}") {
            for e in parse_ics_str(&s, "c").events {
                proptest::prop_assert!(e.end >= e.start);
            }
            let mut expansion = Expansion {
                rule_steps_left: 40_000,
                instances_left: 2_000,
                ..Expansion::new(utc("2000-01-01T00:00:00Z"), utc("2001-01-01T00:00:00Z"))
            };
            for e in parse_ics_expanded(&s, "c", None, &mut expansion) {
                proptest::prop_assert!(e.end >= e.start);
            }
        }

        #[test]
        fn rdates_left_unconverted_could_not_overlap_the_window(
            standard in -86_399i32..86_400,
            daylight in -86_399i32..86_400,
            start_minute in 0i64..(120 * 1440),
            period in proptest::option::of((proptest::bool::ANY, -3_000i64..20_000)),
            series_minutes in 0i64..5_000,
            window_minute in 0i64..(120 * 1440),
            window_minutes in 0i64..20_000,
        ) {
            // A zone at any offsets a VTIMEZONE may give, changing on the last Sundays of
            // March and October, so times in its gaps and overlaps come up too.
            let offset = |seconds: i32| {
                let a = seconds.unsigned_abs();
                format!(
                    "{}{:02}{:02}{:02}",
                    if seconds < 0 { '-' } else { '+' },
                    a / 3600,
                    a / 60 % 60,
                    a % 60
                )
            };
            let base = utc("2026-02-01T00:00:00Z").naive_utc();
            let local = base + Duration::minutes(start_minute);
            let written = local.format("%Y%m%dT%H%M%S");
            let value = match period {
                None => written.to_string(),
                Some((true, minutes)) if minutes >= 0 => format!("{}/PT{}M", written, minutes),
                Some((true, minutes)) => format!("{}/-PT{}M", written, -minutes),
                Some((false, minutes)) => format!(
                    "{}/{}",
                    written,
                    (local + Duration::minutes(minutes)).format("%Y%m%dT%H%M%S")
                ),
            };
            let ics = format!(
                "BEGIN:VTIMEZONE\nTZID:Z\nBEGIN:STANDARD\nDTSTART:20001029T030000\nTZOFFSETFROM:{d}\nTZOFFSETTO:{s}\nRRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\nEND:STANDARD\nBEGIN:DAYLIGHT\nDTSTART:20000326T020000\nTZOFFSETFROM:{s}\nTZOFFSETTO:{d}\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU\nEND:DAYLIGHT\nEND:VTIMEZONE\nBEGIN:VEVENT\nUID:p\nDTSTART:20200101T000000Z\nDURATION:PT{m}M\nRDATE;VALUE=PERIOD;TZID=Z:{v}\nEND:VEVENT\n",
                s = offset(standard),
                d = offset(daylight),
                m = series_minutes,
                v = value
            );
            let doc = Document::parse(&ics, usize::MAX);
            let event = &doc.events[0];
            let series = Duration::minutes(series_minutes);
            let window_start = (base + Duration::minutes(window_minute)).and_utc();
            let window_end = window_start + Duration::minutes(window_minutes);
            let rdate = &event.rdates[0];
            let (at, length) = rdate.occurrence(series, &doc.zones).unwrap();
            if rdate.misses(series, window_start, window_end) {
                proptest::prop_assert!(
                    !overlaps(at, length, window_start, window_end),
                    "{} lasting {} overlaps {}..{}",
                    at,
                    length,
                    window_start,
                    window_end
                );
            }
        }
    }
}
