//! CalDAV (RFC 4791): find the calendar collection, then run a `calendar-query` REPORT over
//! the sync window and feed each returned VCALENDAR through the ICS parser.
//!
//! Discovery (RFC 4791 section 7 / RFC 6764) starts at the configured URL: a calendar
//! collection is used as is; otherwise its `calendar-home-set` (directly or through
//! `current-user-principal`) is listed and the calendar holding events is chosen, by display
//! name when there are several. A bare server URL is tried at `/.well-known/caldav` first and
//! at its root second. The collection found is stored in the config so later syncs skip
//! discovery.
//!
//! Redirects and hrefs are followed, but credentials only go to the configured host and hosts
//! of the same site (iCloud serves calendar homes from per-user hosts), a hop to another
//! origin must stay on HTTPS, and it may not lead to another host's loopback or private
//! address.

use std::net::{IpAddr, Ipv4Addr};

use reqwest::header::{CONTENT_TYPE, LOCATION};
use reqwest::{Method, StatusCode, Url};

use super::xml::{self, CALDAV, DAV, Element};
use super::{Fetched, MAX_BODY_BYTES, SyncWindow, network_error, read_body, redact, snippet};
use crate::calendar::{Calendar, CalendarError, Result, parse_ics_str};

/// Redirect hops followed per request (e.g. `/.well-known/caldav` to the DAV root).
const MAX_REDIRECTS: usize = 5;

/// Connection settings from a `caldav` calendar config.
pub(super) struct Account<'a> {
    pub url: &'a str,
    pub username: &'a str,
    pub password: Option<&'a str>,
    pub bearer_token: Option<&'a str>,
}

struct Dav<'a> {
    http: &'a reqwest::Client,
    account: &'a Account<'a>,
    /// The configured URL; the credentials belong to its host.
    configured: &'a Url,
}

struct DavResponse {
    /// URL after redirects.
    url: Url,
    status: StatusCode,
    body: String,
}

impl Dav<'_> {
    /// Whether [`Dav::authorize`] adds an `Authorization` header.
    fn has_credentials(&self) -> bool {
        self.account.bearer_token.is_some()
            || self.account.password.is_some()
            || !self.account.username.is_empty()
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (self.account.bearer_token, self.account.password) {
            (Some(token), _) => request.bearer_auth(token),
            (None, Some(password)) => request.basic_auth(self.account.username, Some(password)),
            (None, None) if !self.account.username.is_empty() => {
                request.basic_auth(self.account.username, None::<&str>)
            }
            (None, None) => request,
        }
    }

    /// Send a WebDAV request, following redirects with the same method and body (reqwest
    /// would turn a redirected PROPFIND/REPORT into a GET).
    async fn send(&self, method: &str, url: &Url, depth: &str, body: &str) -> Result<DavResponse> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|e| CalendarError::Backend(format!("caldav: {}", e)))?;
        let mut url = url.clone();
        for _ in 0..=MAX_REDIRECTS {
            let context = format!("caldav: {} {}", method, redact(&url));
            if self.has_credentials() && !may_send_credentials(self.configured, &url) {
                return Err(CalendarError::Backend(format!(
                    "{}: not sending the credentials for {} to another site",
                    context,
                    self.configured.host_str().unwrap_or("")
                )));
            }
            let request = self
                .http
                .request(method.clone(), url.clone())
                .header("Depth", depth)
                .header(CONTENT_TYPE, "application/xml; charset=utf-8")
                .body(body.to_string());
            let response = self
                .authorize(request)
                .send()
                .await
                .map_err(|e| network_error(&context, e))?;
            let status = response.status();
            if status.is_redirection() {
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| {
                        CalendarError::Backend(format!(
                            "{}: HTTP {} without Location",
                            context,
                            status.as_u16()
                        ))
                    })?;
                url = follow(&url, location)?;
                continue;
            }
            let body = read_body(response, MAX_BODY_BYTES, &context).await?;
            return Ok(DavResponse { url, status, body });
        }
        Err(CalendarError::Backend(format!(
            "caldav: more than {} redirects from {}",
            MAX_REDIRECTS,
            redact(&url)
        )))
    }

    /// PROPFIND `url`, returning its multistatus resources. `Ok(None)` when the URL does not
    /// exist or is not a WebDAV resource.
    async fn propfind(
        &self,
        url: &Url,
        depth: &str,
        body: &str,
    ) -> Result<Option<(Url, Vec<Resource>)>> {
        let response = self.send("PROPFIND", url, depth, body).await?;
        match response.status {
            StatusCode::MULTI_STATUS => {
                let resources = parse_resources(&response.url, &response.body)?;
                Ok(Some((response.url, resources)))
            }
            StatusCode::NOT_FOUND
            | StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::NOT_IMPLEMENTED => Ok(None),
            status => Err(dav_error(
                &format!("caldav: PROPFIND {}", redact(&response.url)),
                status,
                &response.body,
            )),
        }
    }

    /// PROPFIND a URL that only might be a DAV resource (a bare server's well-known location
    /// or root): any answer but a multistatus means "not here" (a web page, a login redirect,
    /// a refusal), except 401, which says the credentials are wrong.
    async fn probe(&self, url: &Url) -> Result<Option<(Url, Vec<Resource>)>> {
        let response = self.send("PROPFIND", url, "0", PROPFIND_SELF).await?;
        match response.status {
            StatusCode::MULTI_STATUS => {
                let resources = parse_resources(&response.url, &response.body)?;
                Ok(Some((response.url, resources)))
            }
            StatusCode::UNAUTHORIZED => Err(dav_error(
                &format!("caldav: PROPFIND {}", redact(&response.url)),
                response.status,
                &response.body,
            )),
            _ => Ok(None),
        }
    }
}

/// Resolve a redirect or href. A hop may only leave the current origin for HTTPS (e.g. iCloud
/// serves calendar homes from per-user hosts), and never for another host's loopback, private
/// or link-local address: the server must not be able to aim this process at internal
/// services.
fn follow(from: &Url, target: &str) -> Result<Url> {
    let next = from.join(target).map_err(|e| {
        CalendarError::Backend(format!(
            "caldav: bad URL {:?} from {}: {}",
            target,
            redact(from),
            e
        ))
    })?;
    if next.origin() != from.origin() {
        if next.scheme() != "https" {
            return Err(CalendarError::Backend(format!(
                "caldav: refusing to follow {} to non-HTTPS {}",
                redact(from),
                redact(&next)
            )));
        }
        if next.host_str() != from.host_str() && is_internal(&next) {
            return Err(CalendarError::Backend(format!(
                "caldav: refusing to follow {} to internal address {}",
                redact(from),
                redact(&next)
            )));
        }
    }
    Ok(next)
}

/// `localhost`, or an IP literal that is not a public unicast address.
fn is_internal(url: &Url) -> bool {
    if let Some(domain) = url.domain() {
        let domain = domain.trim_end_matches('.');
        return domain.eq_ignore_ascii_case("localhost") || domain.ends_with(".localhost");
    }
    let host = url.host_str().unwrap_or("");
    let Ok(ip) = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    else {
        return true;
    };
    let internal_v4 = |v4: Ipv4Addr| {
        let [a, b, ..] = v4.octets();
        v4.is_loopback()
            || v4.is_private()
            || v4.is_link_local()
            || v4.is_unspecified()
            || v4.is_broadcast()
            || a == 0
            // 100.64.0.0/10, carrier-grade NAT.
            || (a == 100 && b & 0xc0 == 64)
    };
    match ip {
        IpAddr::V4(v4) => internal_v4(v4),
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.to_ipv4_mapped().is_some_and(internal_v4)
        }
    }
}

/// Whether the credentials configured for `configured` may go to `target`: always to the same
/// host, otherwise only over HTTPS to a host of the same [`site`].
fn may_send_credentials(configured: &Url, target: &Url) -> bool {
    if configured.host_str() == target.host_str() {
        return true;
    }
    match (configured.domain(), target.domain()) {
        (Some(a), Some(b)) => {
            target.scheme() == "https" && site(a).is_some_and(|s| site(b) == Some(s))
        }
        _ => false,
    }
}

/// The registrable domain of a DNS name, approximated without a public-suffix list: its last
/// two labels, or three under a two-letter country code with a short second level
/// (`example.co.uk`, `example.com.au`). Single-label names (`localhost`) have none. Where the
/// guess is too narrow, credentials are withheld rather than sent too widely.
fn site(domain: &str) -> Option<&str> {
    let domain = domain.trim_end_matches('.');
    let mut labels = domain.rsplit('.');
    let (tld, second) = (labels.next()?, labels.next()?);
    let keep = if tld.len() == 2 && second.len() <= 3 && labels.next().is_some() {
        3
    } else {
        2
    };
    let cut = domain
        .rmatch_indices('.')
        .nth(keep - 1)
        .map_or(0, |(i, _)| i + 1);
    Some(&domain[cut..])
}

fn dav_error(context: &str, status: StatusCode, body: &str) -> CalendarError {
    let message = format!(
        "{} failed (HTTP {}): {}",
        context,
        status.as_u16(),
        snippet(body)
    );
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => CalendarError::AuthRequired(message),
        StatusCode::NOT_FOUND => CalendarError::NotFound(message),
        _ => CalendarError::Backend(message),
    }
}

/// Properties of one `DAV:response`, from its `200 OK` propstats.
#[derive(Debug, Default)]
struct Resource {
    /// A non-2xx response-level `DAV:status` (e.g. 507 when a server truncates a REPORT's
    /// results, RFC 4918 section 14.28) or a 5xx propstat: the server could not answer fully.
    failure: Option<u16>,
    href: Option<Url>,
    is_calendar: bool,
    display_name: Option<String>,
    /// `supported-calendar-component-set`; `None` when not reported (all components).
    components: Option<Vec<String>>,
    principal: Option<Url>,
    calendar_home: Option<Url>,
    etag: Option<String>,
    calendar_data: Option<String>,
}

impl Resource {
    fn holds_events(&self) -> bool {
        self.is_calendar
            && self
                .components
                .as_ref()
                .is_none_or(|c| c.iter().any(|c| c.eq_ignore_ascii_case("VEVENT")))
    }
}

/// The code of a `DAV:status` child ("HTTP/1.1 200 OK"); `None` without one.
fn status_code(el: &Element) -> Option<Option<u16>> {
    let status = el.child(DAV, "status")?;
    Some(
        status
            .text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok()),
    )
}

fn href_in(el: Option<&Element>, base: &Url) -> Option<Url> {
    let href = el?.child(DAV, "href")?.text.trim().to_string();
    follow(base, &href).ok()
}

fn parse_resources(base: &Url, body: &str) -> Result<Vec<Resource>> {
    let root = xml::parse(body).map_err(|e| {
        CalendarError::Parse(format!(
            "caldav: bad multistatus from {}: {}",
            redact(base),
            e
        ))
    })?;
    if !root.is(DAV, "multistatus") {
        return Err(CalendarError::Parse(format!(
            "caldav: expected DAV:multistatus from {}, got {}",
            redact(base),
            root.name
        )));
    }
    let mut resources = Vec::new();
    for response in root.children_named(DAV, "response") {
        let mut resource = Resource {
            href: response
                .child(DAV, "href")
                .and_then(|h| follow(base, h.text.trim()).ok()),
            failure: match status_code(response) {
                Some(Some(code)) if (200..300).contains(&code) => None,
                // Unparseable status: treat it as a failure too.
                Some(code) => Some(code.unwrap_or(0)),
                None => None,
            },
            ..Default::default()
        };
        for propstat in response.children_named(DAV, "propstat") {
            // A propstat without status is taken as OK.
            match status_code(propstat) {
                None | Some(Some(200)) => {}
                Some(Some(code)) if code >= 500 => {
                    resource.failure.get_or_insert(code);
                    continue;
                }
                Some(_) => continue,
            }
            let Some(prop) = propstat.child(DAV, "prop") else {
                continue;
            };
            for p in &prop.children {
                match (p.ns.as_str(), p.name.as_str()) {
                    (DAV, "resourcetype") => {
                        resource.is_calendar |= p.child(CALDAV, "calendar").is_some();
                    }
                    (DAV, "displayname") => {
                        resource.display_name = Some(p.text.trim().to_string());
                    }
                    (DAV, "current-user-principal") => resource.principal = href_in(Some(p), base),
                    (DAV, "getetag") => resource.etag = Some(p.text.trim().to_string()),
                    (CALDAV, "calendar-home-set") => {
                        resource.calendar_home = href_in(Some(p), base)
                    }
                    (CALDAV, "supported-calendar-component-set") => {
                        resource.components = Some(
                            p.children_named(CALDAV, "comp")
                                .filter_map(|c| c.attr("name").map(str::to_string))
                                .collect(),
                        );
                    }
                    (CALDAV, "calendar-data") if !p.text.trim().is_empty() => {
                        resource.calendar_data = Some(p.text.clone());
                    }
                    _ => {}
                }
            }
        }
        resources.push(resource);
    }
    Ok(resources)
}

const PROPFIND_SELF: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:resourcetype/>
    <D:displayname/>
    <D:current-user-principal/>
    <C:calendar-home-set/>
    <C:supported-calendar-component-set/>
  </D:prop>
</D:propfind>"#;

const PROPFIND_CALENDARS: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:resourcetype/>
    <D:displayname/>
    <C:supported-calendar-component-set/>
  </D:prop>
</D:propfind>"#;

fn same_resource(a: &Url, b: &Url) -> bool {
    a.origin() == b.origin() && a.path().trim_end_matches('/') == b.path().trim_end_matches('/')
}

/// The calendar holding events under a calendar home, chosen by `name` when there are several.
async fn pick_calendar(dav: &Dav<'_>, home: &Url, name: &str) -> Result<Url> {
    let Some((home, resources)) = dav.propfind(home, "1", PROPFIND_CALENDARS).await? else {
        return Err(CalendarError::NotFound(format!(
            "caldav: calendar home {} does not exist",
            redact(home)
        )));
    };
    let calendars: Vec<(Url, Option<String>)> = resources
        .into_iter()
        .filter(|r| r.holds_events())
        .filter_map(|r| Some((r.href?, r.display_name)))
        .filter(|(href, _)| !same_resource(href, &home))
        .collect();
    let by_name = || {
        calendars.iter().find(|(_, n)| {
            n.as_deref()
                .is_some_and(|n| n.trim().eq_ignore_ascii_case(name.trim()))
        })
    };
    match calendars.as_slice() {
        [] => Err(CalendarError::NotFound(format!(
            "caldav: no event calendars under {}",
            redact(&home)
        ))),
        [(only, _)] => Ok(only.clone()),
        _ => match by_name() {
            Some((url, _)) => Ok(url.clone()),
            None => Err(CalendarError::Backend(format!(
                "caldav: {} calendars under {} and none is named {:?}; name the calendar after \
                 one of them or use its URL: {}",
                calendars.len(),
                redact(&home),
                name,
                calendars
                    .iter()
                    .map(|(u, n)| format!("{:?} {}", n.as_deref().unwrap_or(""), redact(u)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        },
    }
}

/// Find the calendar collection starting from `start`; `Ok(None)` when `start` leads nowhere.
/// A `guess` (a bare server's well-known location or root) that is not a DAV resource leads
/// nowhere whatever it answers, see [`Dav::probe`].
async fn discover_from(dav: &Dav<'_>, start: &Url, name: &str, guess: bool) -> Result<Option<Url>> {
    let found = if guess {
        dav.probe(start).await?
    } else {
        dav.propfind(start, "0", PROPFIND_SELF).await?
    };
    let Some((url, resources)) = found else {
        return Ok(None);
    };
    let Some(this) = resources.into_iter().next() else {
        return Ok(None);
    };
    if this.is_calendar {
        return Ok(Some(url));
    }
    if let Some(home) = this.calendar_home {
        return pick_calendar(dav, &home, name).await.map(Some);
    }
    let Some(principal) = this.principal else {
        return Ok(None);
    };
    let home = match dav.propfind(&principal, "0", PROPFIND_SELF).await? {
        Some((_, resources)) => resources.into_iter().next().and_then(|r| r.calendar_home),
        None => None,
    };
    match home {
        Some(home) => pick_calendar(dav, &home, name).await.map(Some),
        None => Err(CalendarError::NotFound(format!(
            "caldav: principal {} has no calendar-home-set",
            redact(&principal)
        ))),
    }
}

async fn discover(dav: &Dav<'_>, start: &Url, name: &str) -> Result<Url> {
    let nowhere = || {
        CalendarError::NotFound(format!(
            "caldav: {} is not a calendar collection and names no calendar home or principal \
             (nor does its /.well-known/caldav for a bare server URL); configure the calendar \
             collection URL",
            redact(start)
        ))
    };
    if !matches!(start.path(), "" | "/") {
        return discover_from(dav, start, name, false)
            .await?
            .ok_or_else(nowhere);
    }
    // RFC 6764 section 6: a bare server URL is bootstrapped through its well-known location.
    // The root comes second, for servers mounted there without one; it is often a web page.
    let well_known = follow(start, "/.well-known/caldav")?;
    let from_well_known = match discover_from(dav, &well_known, name, true).await {
        Ok(Some(found)) => return Ok(found),
        other => other,
    };
    match discover_from(dav, start, name, true).await {
        Ok(Some(found)) => Ok(found),
        // The well-known location is the more telling failure when both fail.
        from_root => Err(from_well_known
            .err()
            .or(from_root.err())
            .unwrap_or_else(nowhere)),
    }
}

fn calendar_query(window: SyncWindow, expand: bool) -> String {
    let (start, end) = (
        window.start.format("%Y%m%dT%H%M%SZ").to_string(),
        window.end.format("%Y%m%dT%H%M%SZ").to_string(),
    );
    // `expand` makes the server return each occurrence of a recurring event in the window
    // (with a RECURRENCE-ID, in UTC) instead of the master with an RRULE.
    let data = if expand {
        format!(
            r#"<C:calendar-data><C:expand start="{}" end="{}"/></C:calendar-data>"#,
            start, end
        )
    } else {
        "<C:calendar-data/>".to_string()
    };
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:getetag/>
    {}
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range start="{}" end="{}"/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>"#,
        data, start, end
    )
}

/// REPORT the collection's events in `window`.
async fn report(
    dav: &Dav<'_>,
    collection: &Url,
    calendar_id: &str,
    window: SyncWindow,
) -> Result<Fetched> {
    let mut response = dav
        .send("REPORT", collection, "1", &calendar_query(window, true))
        .await?;
    // Servers without `expand` support reject the query; ask for the plain data instead. Not
    // after a 5xx, which is usually transient: the plain data stores a recurring event as one
    // master instead of one event per occurrence, so the occurrences would be dropped until
    // the next sync.
    if matches!(
        response.status,
        StatusCode::BAD_REQUEST
            | StatusCode::FORBIDDEN
            | StatusCode::UNSUPPORTED_MEDIA_TYPE
            | StatusCode::UNPROCESSABLE_ENTITY
            | StatusCode::NOT_IMPLEMENTED
    ) {
        response = dav
            .send("REPORT", collection, "1", &calendar_query(window, false))
            .await?;
    }
    if response.status != StatusCode::MULTI_STATUS {
        return Err(dav_error(
            &format!("caldav: REPORT {}", redact(&response.url)),
            response.status,
            &response.body,
        ));
    }
    let resources = parse_resources(&response.url, &response.body)?;
    let incomplete = resources.iter().find_map(|r| {
        Some(format!(
            "caldav: REPORT {} answered HTTP {} for {}: results are incomplete, so stored \
             events it did not list were kept",
            redact(&response.url),
            r.failure?,
            r.href.as_ref().map_or_else(|| "a resource".into(), redact)
        ))
    });
    let mut events = Vec::new();
    for resource in resources {
        let Some(data) = resource.calendar_data else {
            continue;
        };
        for mut event in parse_ics_str(&data, calendar_id) {
            event.etag = resource.etag.clone();
            events.push(event);
        }
    }
    Ok(Fetched { events, incomplete })
}

pub(super) async fn fetch(
    http: &reqwest::Client,
    account: &Account<'_>,
    collection_url: &mut Option<String>,
    calendar: &Calendar,
    window: SyncWindow,
) -> Result<Fetched> {
    let start = Url::parse(account.url.trim()).map_err(|e| {
        CalendarError::Backend(format!("caldav: invalid url {:?}: {}", account.url, e))
    })?;
    let dav = Dav {
        http,
        account,
        configured: &start,
    };
    let remembered = collection_url.as_deref().and_then(|u| Url::parse(u).ok());
    let collection = match remembered.clone() {
        Some(url) => url,
        None => {
            let found = discover(&dav, &start, &calendar.name).await?;
            *collection_url = Some(found.to_string());
            found
        }
    };
    let result = report(&dav, &collection, &calendar.id, window).await;
    // A remembered collection that disappeared (renamed/recreated calendar) is rediscovered
    // on the next sync.
    if remembered.is_some() && matches!(result, Err(CalendarError::NotFound(_))) {
        *collection_url = None;
    }
    result
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn multistatus_resources_keep_only_ok_propstats() {
        let base = Url::parse("https://dav.example/cal/").unwrap();
        let body = r#"<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
          <d:response><d:href>/cal/work/</d:href>
            <d:propstat><d:prop><d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
              <d:displayname>Work</d:displayname>
              <cal:supported-calendar-component-set><cal:comp name="VEVENT"/></cal:supported-calendar-component-set>
            </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>
            <d:propstat><d:prop><cal:calendar-home-set><d:href>/evil/</d:href></cal:calendar-home-set></d:prop>
              <d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>
          </d:response>
          <d:response><d:href>/cal/tasks/</d:href>
            <d:propstat><d:prop><d:resourcetype><d:collection/><cal:calendar/></d:resourcetype>
              <cal:supported-calendar-component-set><cal:comp name="VTODO"/></cal:supported-calendar-component-set>
            </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>
          </d:response>
        </d:multistatus>"#;
        let resources = parse_resources(&base, body).unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(
            resources[0].href.as_ref().unwrap().as_str(),
            "https://dav.example/cal/work/"
        );
        assert!(resources[0].holds_events());
        assert_eq!(resources[0].display_name.as_deref(), Some("Work"));
        assert!(resources[0].calendar_home.is_none());
        assert!(!resources[1].holds_events());
    }

    #[test]
    fn follow_refuses_downgrade_to_foreign_http() {
        let from = Url::parse("https://dav.example/cal/").unwrap();
        assert!(follow(&from, "/other/").is_ok());
        assert!(follow(&from, "https://p42-caldav.example.net/1/calendars/").is_ok());
        assert!(follow(&from, "http://169.254.169.254/latest/").is_err());
    }

    #[test]
    fn follow_refuses_other_hosts_internal_addresses() {
        let from = Url::parse("https://dav.example/cal/").unwrap();
        for internal in [
            "https://127.0.0.1/",
            "https://10.0.0.5/",
            "https://192.168.1.2:8443/",
            "https://169.254.169.254/latest/",
            "https://100.64.0.1/",
            "https://0.0.0.0/",
            "https://[::1]/",
            "https://[fd00::1]/",
            "https://[fe80::1]/",
            "https://[::ffff:127.0.0.1]/",
            "https://localhost/",
            "https://api.localhost/",
        ] {
            let err = follow(&from, internal).unwrap_err().to_string();
            assert!(err.contains("internal address"), "{}: {}", internal, err);
        }
        assert!(follow(&from, "https://203.0.113.9/").is_ok());
        // A server configured on a private address may still move between its own ports.
        let lan = Url::parse("http://192.168.1.2:5232/").unwrap();
        assert!(follow(&lan, "/user/calendar/").is_ok());
        assert!(follow(&lan, "https://192.168.1.2:8443/user/").is_ok());
        assert!(follow(&lan, "https://192.168.1.3/user/").is_err());
    }

    #[test]
    fn site_approximates_the_registrable_domain() {
        assert_eq!(site("p42-caldav.icloud.com"), Some("icloud.com"));
        assert_eq!(site("icloud.com"), Some("icloud.com"));
        assert_eq!(site("dav.example.de."), Some("example.de"));
        assert_eq!(site("a.b.example.co.uk"), Some("example.co.uk"));
        assert_eq!(site("example.com.au"), Some("example.com.au"));
        assert_eq!(site("localhost"), None);
    }

    #[test]
    fn credentials_stay_with_the_configured_site() {
        let url = |s: &str| Url::parse(s).unwrap();
        let icloud = url("https://caldav.icloud.com/");
        assert!(may_send_credentials(
            &icloud,
            &url("https://p42-caldav.icloud.com/1/")
        ));
        assert!(may_send_credentials(
            &icloud,
            &url("https://caldav.icloud.com:8443/")
        ));
        assert!(!may_send_credentials(
            &icloud,
            &url("http://p42-caldav.icloud.com/")
        ));
        assert!(!may_send_credentials(
            &icloud,
            &url("https://evil.example/")
        ));
        assert!(!may_send_credentials(
            &icloud,
            &url("https://icloud.com.evil.example/")
        ));
        // Sibling registrations under a country-code second level are different sites.
        let uk = url("https://example.co.uk/dav/");
        assert!(!may_send_credentials(&uk, &url("https://evil.co.uk/")));
        let ip = url("http://127.0.0.1:5232/");
        assert!(may_send_credentials(
            &ip,
            &url("http://127.0.0.1:5232/user/")
        ));
        assert!(!may_send_credentials(&ip, &url("https://127.0.0.2/")));
    }

    #[test]
    fn multistatus_failures_are_flagged() {
        let base = Url::parse("https://dav.example/cal/").unwrap();
        let body = r#"<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
          <d:response><d:href>/cal/a.ics</d:href>
            <d:propstat><d:prop><d:getetag>"a"</d:getetag></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>
            <d:propstat><d:prop><cal:calendar-data/></d:prop><d:status>HTTP/1.1 403 Forbidden</d:status></d:propstat>
          </d:response>
          <d:response><d:href>/cal/b.ics</d:href>
            <d:propstat><d:prop><cal:calendar-data/></d:prop><d:status>HTTP/1.1 503 Service Unavailable</d:status></d:propstat>
          </d:response>
          <d:response><d:href>/cal/</d:href><d:status>HTTP/1.1 507 Insufficient Storage</d:status></d:response>
          <d:response><d:href>/cal/c.ics</d:href><d:status>HTTP/1.1 200 OK</d:status></d:response>
        </d:multistatus>"#;
        let failures: Vec<_> = parse_resources(&base, body)
            .unwrap()
            .iter()
            .map(|r| r.failure)
            .collect();
        // An unreadable (403) resource is skipped, not a sign of a partial answer.
        assert_eq!(failures, [None, Some(503), Some(507), None]);
    }

    #[test]
    fn calendar_query_carries_time_range_and_expand() {
        let window = SyncWindow {
            start: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap(),
        };
        let q = calendar_query(window, true);
        assert!(q.contains(r#"<C:time-range start="20260101T000000Z" end="20270101T000000Z"/>"#));
        assert!(q.contains("<C:expand"));
        assert!(!calendar_query(window, false).contains("<C:expand"));
        xml::parse(&q).unwrap();
    }
}
