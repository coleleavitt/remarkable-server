//! Remote calendar providers: CalDAV, Google Calendar API v3 and Microsoft Graph.
//!
//! [`RemoteSync::fetch_events`] returns every event of one calendar inside a [`SyncWindow`].
//! Providers that learn something worth keeping while fetching (a refreshed OAuth token, a
//! rotated refresh token, a discovered CalDAV collection) write it into the
//! [`CalendarConfig`] they were given, so the caller can persist it even when the fetch
//! itself fails afterwards.
//!
//! Response bodies are read with a size cap: this process also serves the tablets, so a
//! broken or hostile provider must not be able to exhaust its memory.
//!
//! Provider URLs (CalDAV servers) are admin-provided through the authenticated calendar API,
//! the same trust level as ICS file paths.

mod caldav;
mod dns;
mod google;
mod graph;
mod oauth;
mod xml;

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, SecondsFormat, Utc};

use crate::calendar::{Calendar, CalendarConfig, CalendarError, CalendarEvent, Result};

/// Base URLs of the hosted provider APIs. Overridable so tests (or sovereign clouds) can point
/// them elsewhere; CalDAV servers come from each calendar's own config.
#[derive(Debug, Clone)]
pub struct ProviderEndpoints {
    /// Google Calendar API v3 root.
    pub google_api: String,
    /// Google OAuth 2.0 token endpoint.
    pub google_token_url: String,
    /// Microsoft Graph v1.0 root.
    pub graph_api: String,
    /// Microsoft identity platform root; the token URL is
    /// `{microsoft_login}/{tenant}/oauth2/v2.0/token`.
    pub microsoft_login: String,
}

impl Default for ProviderEndpoints {
    fn default() -> Self {
        Self {
            google_api: "https://www.googleapis.com/calendar/v3".into(),
            google_token_url: "https://oauth2.googleapis.com/token".into(),
            graph_api: "https://graph.microsoft.com/v1.0".into(),
            microsoft_login: "https://login.microsoftonline.com".into(),
        }
    }
}

/// Time range a remote sync covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl SyncWindow {
    pub const PAST_DAYS: i64 = 30;
    pub const FUTURE_DAYS: i64 = 365;

    /// `now - 30 days .. now + 365 days`, truncated to whole seconds.
    pub fn around(now: DateTime<Utc>) -> Self {
        let now = DateTime::from_timestamp(now.timestamp(), 0).unwrap_or(now);
        Self {
            start: now - Duration::days(Self::PAST_DAYS),
            end: now + Duration::days(Self::FUTURE_DAYS),
        }
    }
}

/// Connect timeout for provider requests.
const CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(10);
/// Whole-request ceiling, so a hung provider cannot stall a sync (and the sync-all loop)
/// forever. Calendar responses are small next to cloud-storage transfers.
const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(60);
/// Upper bound on followed pages per sync, in case a provider keeps returning page links.
const MAX_PAGES: usize = 200;
/// Largest provider response body read (a CalDAV REPORT or one API page); a year of calendar
/// data is far below this.
const MAX_BODY_BYTES: usize = 32 << 20;
/// Largest OAuth token endpoint response read.
const MAX_TOKEN_BODY_BYTES: usize = 1 << 20;

/// Events a provider returned for a [`SyncWindow`].
#[derive(Debug)]
pub struct Fetched {
    pub events: Vec<CalendarEvent>,
    /// Why the answer may be missing events (a CalDAV server truncating its results, say).
    /// Stored events it does not list are then kept rather than removed as deleted upstream.
    pub incomplete: Option<String>,
}

impl Fetched {
    fn complete(events: Vec<CalendarEvent>) -> Self {
        Self {
            events,
            incomplete: None,
        }
    }
}

/// Shared HTTP client and endpoints for remote calendar syncs.
#[derive(Clone)]
pub struct RemoteSync {
    /// For the hosted APIs (Google, Microsoft Graph), whose hosts are fixed.
    http: reqwest::Client,
    endpoints: Arc<ProviderEndpoints>,
    /// Name resolution under the CalDAV clients, see [`caldav_client`].
    lookup: Arc<dyn dns::Lookup>,
}

/// Timeouts, no automatic redirects and the user agent, for every provider client.
fn client_builder() -> reqwest::ClientBuilder {
    // Redirects are not followed automatically: CalDAV needs them followed with the same
    // method and body (REPORT/PROPFIND), and the hosted APIs never redirect.
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("remarkable-server/", env!("CARGO_PKG_VERSION")))
}

/// A client for one CalDAV account configured at `configured_host`. Its names resolve
/// through [`dns::GuardedResolver`]: only the configured host (and the proxies from the
/// environment) may reach internal addresses, so a server cannot point this process at
/// internal services by redirecting it to a name that resolves to one.
fn caldav_client(configured_host: &str, lookup: Arc<dyn dns::Lookup>) -> Result<reqwest::Client> {
    let proxies = dns::proxy_hosts(|name| std::env::var(name).ok());
    let trusted = std::iter::once(configured_host).chain(proxies.iter().map(String::as_str));
    client_builder()
        .dns_resolver(Arc::new(dns::GuardedResolver::new(trusted, lookup)))
        .build()
        .map_err(|e| CalendarError::Backend(format!("caldav: cannot set up HTTP client: {}", e)))
}

impl Default for RemoteSync {
    fn default() -> Self {
        Self::new(ProviderEndpoints::default())
    }
}

impl RemoteSync {
    pub fn new(endpoints: ProviderEndpoints) -> Self {
        let http = client_builder()
            .build()
            // Only fails if the TLS backend cannot initialise, which every other client in
            // the process would hit as well.
            .expect("reqwest client with timeouts");
        Self {
            http,
            endpoints: Arc::new(endpoints),
            lookup: Arc::new(dns::SystemLookup),
        }
    }

    pub fn endpoints(&self) -> &ProviderEndpoints {
        &self.endpoints
    }

    /// Fetch `calendar`'s events in `window`. `config` starts as a copy of the calendar's
    /// config; refreshed tokens and discovered URLs are written back into it.
    pub async fn fetch_events(
        &self,
        calendar: &Calendar,
        config: &mut CalendarConfig,
        window: SyncWindow,
    ) -> Result<Fetched> {
        match config {
            CalendarConfig::Ics { .. } => Err(CalendarError::Backend(
                "ICS calendars are read from a local file, not a remote provider".into(),
            )),
            CalendarConfig::Caldav {
                url,
                username,
                password,
                bearer_token,
                collection_url,
            } => {
                let account = caldav::Account {
                    url,
                    username,
                    password: password.as_deref(),
                    bearer_token: bearer_token.as_deref(),
                };
                caldav::fetch(&self.lookup, &account, collection_url, calendar, window).await
            }
            CalendarConfig::Google {
                calendar_id,
                access_token,
                refresh_token,
                client_id,
                client_secret,
                token_expires_at,
            } => {
                let mut session = oauth::Session::new(
                    &self.http,
                    oauth::Client {
                        provider: "google",
                        token_url: self.endpoints.google_token_url.clone(),
                        client_id: client_id.as_deref(),
                        client_secret: client_secret.as_deref(),
                        scope: None,
                    },
                    oauth::Tokens {
                        access_token,
                        refresh_token,
                        expires_at: token_expires_at,
                    },
                );
                google::fetch(
                    &mut session,
                    &self.endpoints.google_api,
                    calendar_id,
                    &calendar.id,
                    window,
                )
                .await
                .map(Fetched::complete)
            }
            CalendarConfig::Office365 {
                tenant_id,
                access_token,
                refresh_token,
                client_id,
                client_secret,
                calendar_id,
                token_expires_at,
            } => {
                let tenant = match tenant_id.trim() {
                    "" => "common",
                    tenant => tenant,
                };
                let mut session = oauth::Session::new(
                    &self.http,
                    oauth::Client {
                        provider: "microsoft graph",
                        token_url: format!(
                            "{}/{}/oauth2/v2.0/token",
                            self.endpoints.microsoft_login.trim_end_matches('/'),
                            urlencoding::encode(tenant)
                        ),
                        client_id: client_id.as_deref(),
                        client_secret: client_secret.as_deref(),
                        scope: Some(graph::SCOPE),
                    },
                    oauth::Tokens {
                        access_token,
                        refresh_token,
                        expires_at: token_expires_at,
                    },
                );
                graph::fetch(
                    &mut session,
                    &self.endpoints.graph_api,
                    calendar_id.as_deref(),
                    &calendar.id,
                    window,
                )
                .await
                .map(Fetched::complete)
            }
            CalendarConfig::Exchange { .. } => {
                Err(CalendarError::Backend(EXCHANGE_UNSUPPORTED.into()))
            }
        }
    }
}

/// Why an `exchange` (EWS) config cannot sync, and what to use instead.
pub const EXCHANGE_UNSUPPORTED: &str = "on-premises Exchange (EWS) sync is not supported; \
     add Exchange Online / Microsoft 365 calendars with an \"office365\" config (Microsoft Graph)";

/// RFC 3339 UTC timestamp with a `Z` suffix, as the hosted APIs expect.
fn rfc3339_z(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// `scheme://host[:port]/path` without credentials or query, for error messages.
fn redact(url: &reqwest::Url) -> String {
    let mut shown = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
    if let Some(port) = url.port() {
        shown.push_str(&format!(":{}", port));
    }
    shown.push_str(url.path());
    shown
}

/// A transport error with its cause chain but without the request URL (which may carry
/// credentials or tokens in its query).
fn network_error(context: &str, err: reqwest::Error) -> CalendarError {
    let err = err.without_url();
    let mut message = format!("{}: {}", context, err);
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    CalendarError::Network(message)
}

/// `response`'s body as UTF-8 (invalid sequences replaced), or an error once it grows past
/// `limit` bytes.
async fn read_body(mut response: reqwest::Response, limit: usize, context: &str) -> Result<String> {
    let too_large = || {
        CalendarError::Backend(format!(
            "{}: response body larger than {} bytes",
            context, limit
        ))
    };
    if response
        .content_length()
        .is_some_and(|len| len > limit as u64)
    {
        return Err(too_large());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| network_error(context, e))?
    {
        if body.len() + chunk.len() > limit {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8(body)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
}

/// First few hundred characters of a response body, whitespace-collapsed, for error messages.
fn snippet(body: &str) -> String {
    const MAX: usize = 300;
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    match collapsed.char_indices().nth(MAX) {
        Some((cut, _)) => format!("{}...", &collapsed[..cut]),
        None => collapsed,
    }
}

/// `YYYY-MM-DD` date at midnight UTC: the convention the ICS parser uses for all-day events.
fn date_at_midnight(date: &str) -> Option<DateTime<Utc>> {
    chrono::NaiveDate::parse_from_str(date.get(..10)?, "%Y-%m-%d")
        .ok()?
        .and_hms_opt(0, 0, 0)
        .map(|dt| dt.and_utc())
}

/// `end` if it is usable, otherwise `start` plus a day (all-day) or an hour.
fn end_or_default(
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    all_day: bool,
) -> DateTime<Utc> {
    let default_length = if all_day {
        Duration::days(1)
    } else {
        Duration::hours(1)
    };
    end.filter(|end| *end >= start)
        .or_else(|| start.checked_add_signed(default_length))
        .unwrap_or(start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_spans_past_month_and_next_year() {
        let now = DateTime::parse_from_rfc3339("2026-01-31T12:34:56.789Z")
            .unwrap()
            .with_timezone(&Utc);
        let w = SyncWindow::around(now);
        assert_eq!(rfc3339_z(w.start), "2026-01-01T12:34:56Z");
        assert_eq!(rfc3339_z(w.end), "2027-01-31T12:34:56Z");
    }

    #[test]
    fn redact_drops_credentials_and_query() {
        let url = reqwest::Url::parse("https://user:pw@dav.example:8443/cal/?token=x").unwrap();
        assert_eq!(redact(&url), "https://dav.example:8443/cal/");
    }

    #[tokio::test]
    async fn read_body_stops_at_the_limit() {
        use axum::body::Body;
        use axum::routing::get;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            // Content-Length announced up front.
            .route("/sized", get(|| async { "x".repeat(100) }))
            // Chunked, no length: only counting the chunks catches it.
            .route(
                "/streamed",
                get(|| async {
                    let chunks =
                        (0..4).map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from("y".repeat(25))));
                    Body::from_stream(futures_util::stream::iter(chunks))
                }),
            );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();
        for path in ["/sized", "/streamed"] {
            let get = || http.get(format!("{}{}", base, path)).send();
            let body = read_body(get().await.unwrap(), 100, "t").await.unwrap();
            assert_eq!(body.len(), 100, "{}", path);
            let err = read_body(get().await.unwrap(), 99, "t").await.unwrap_err();
            assert!(
                err.to_string().contains("larger than 99 bytes"),
                "{}: {}",
                path,
                err
            );
        }
    }

    #[test]
    fn snippet_truncates_on_char_boundary() {
        let body = "é".repeat(400);
        let s = snippet(&body);
        assert!(s.ends_with("...") && s.chars().count() == 303);
    }
}
