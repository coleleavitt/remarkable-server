//! OAuth 2.0 access-token handling shared by the Google and Microsoft Graph providers.
//!
//! Tokens are obtained out of band (the admin adds the calendar with an access and/or refresh
//! token). A [`Session`] refreshes the access token with the `refresh_token` grant when it is
//! missing, about to expire, or rejected with 401, and writes the new tokens into the
//! calendar's config fields it borrows so the caller can persist them.

use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use serde::Deserialize;

use super::{MAX_TOKEN_BODY_BYTES, network_error, read_body, snippet};
use crate::calendar::{CalendarError, Result};

/// Refresh this long before the recorded expiry, so a token does not lapse mid-sync.
const EXPIRY_MARGIN_SECS: i64 = 60;

/// The OAuth client a refresh token belongs to.
pub(super) struct Client<'a> {
    /// Provider name for error messages.
    pub provider: &'static str,
    pub token_url: String,
    pub client_id: Option<&'a str>,
    pub client_secret: Option<&'a str>,
    /// Sent with the refresh grant (Microsoft requires it; Google must not get one).
    pub scope: Option<&'static str>,
}

/// Token fields of the calendar config being synced.
pub(super) struct Tokens<'a> {
    pub access_token: &'a mut Option<String>,
    pub refresh_token: &'a mut Option<String>,
    pub expires_at: &'a mut Option<DateTime<Utc>>,
}

pub(super) struct Session<'a> {
    http: &'a reqwest::Client,
    client: Client<'a>,
    tokens: Tokens<'a>,
    /// At most one refresh per sync: a token rejected right after refreshing will not be
    /// fixed by refreshing again.
    refreshed: bool,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    /// Seconds; a number per RFC 6749, but some endpoints send a string.
    #[serde(default)]
    expires_in: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct TokenError {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

impl<'a> Session<'a> {
    pub fn new(http: &'a reqwest::Client, client: Client<'a>, tokens: Tokens<'a>) -> Self {
        Self {
            http,
            client,
            tokens,
            refreshed: false,
        }
    }

    pub fn http(&self) -> &'a reqwest::Client {
        self.http
    }

    fn can_refresh(&self) -> bool {
        !self.refreshed && self.tokens.refresh_token.is_some() && self.client.client_id.is_some()
    }

    /// A usable access token: the stored one unless it is missing or about to expire.
    async fn access_token(&mut self) -> Result<String> {
        let expiring = self
            .tokens
            .expires_at
            .is_some_and(|at| at <= Utc::now() + Duration::seconds(EXPIRY_MARGIN_SECS));
        match self.tokens.access_token.as_deref() {
            Some(token) if !token.is_empty() && !(expiring && self.can_refresh()) => {
                Ok(token.to_string())
            }
            _ => self.refresh().await,
        }
    }

    /// Exchange the refresh token for a new access token and record both.
    async fn refresh(&mut self) -> Result<String> {
        let provider = self.client.provider;
        let refresh_token = self.tokens.refresh_token.clone().ok_or_else(|| {
            CalendarError::AuthRequired(format!(
                "{}: no usable access token and no refresh_token configured",
                provider
            ))
        })?;
        let client_id = self.client.client_id.ok_or_else(|| {
            CalendarError::AuthRequired(format!(
                "{}: client_id is required to refresh the access token",
                provider
            ))
        })?;
        let mut form = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token.as_str()),
            ("client_id", client_id),
        ];
        if let Some(secret) = self.client.client_secret {
            form.push(("client_secret", secret));
        }
        if let Some(scope) = self.client.scope {
            form.push(("scope", scope));
        }
        self.refreshed = true;
        let context = format!("{}: token refresh", provider);
        let response = self
            .http
            .post(&self.client.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| network_error(&context, e))?;
        let status = response.status();
        let body = read_body(response, MAX_TOKEN_BODY_BYTES, &context).await?;
        if !status.is_success() {
            let reason = match serde_json::from_str::<TokenError>(&body) {
                Ok(e) => match e.error_description {
                    Some(d) => format!("{}: {}", e.error, snippet(&d)),
                    None => e.error,
                },
                Err(_) => snippet(&body),
            };
            return Err(CalendarError::AuthRequired(format!(
                "{} failed (HTTP {}): {}",
                context,
                status.as_u16(),
                reason
            )));
        }
        let token: TokenResponse = serde_json::from_str(&body)
            .map_err(|e| CalendarError::Parse(format!("{} response: {}", context, e)))?;
        let lifetime = token
            .expires_in
            .as_ref()
            .and_then(|v| v.as_i64().or_else(|| v.as_str()?.trim().parse().ok()));
        *self.tokens.expires_at = lifetime
            .and_then(Duration::try_seconds)
            .and_then(|d| Utc::now().checked_add_signed(d));
        *self.tokens.access_token = Some(token.access_token.clone());
        // Microsoft rotates refresh tokens; Google keeps the old one valid and omits it.
        if let Some(rotated) = token.refresh_token.filter(|t| !t.is_empty()) {
            *self.tokens.refresh_token = Some(rotated);
        }
        Ok(token.access_token)
    }

    /// Send the request `build` makes for an access token; on 401, refresh once and resend.
    pub async fn send(
        &mut self,
        context: &str,
        build: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let token = self.access_token().await?;
        let response = build(&token)
            .send()
            .await
            .map_err(|e| network_error(context, e))?;
        if response.status() != StatusCode::UNAUTHORIZED || !self.can_refresh() {
            return Ok(response);
        }
        let token = self.refresh().await?;
        build(&token)
            .send()
            .await
            .map_err(|e| network_error(context, e))
    }
}

/// Error for a non-success API response: 401/403 mean the credentials are not good enough.
pub(super) fn api_error(context: &str, status: StatusCode, body: &str) -> CalendarError {
    let message = format!(
        "{} failed (HTTP {}): {}",
        context,
        status.as_u16(),
        snippet(body)
    );
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        CalendarError::AuthRequired(message)
    } else {
        CalendarError::Backend(message)
    }
}
