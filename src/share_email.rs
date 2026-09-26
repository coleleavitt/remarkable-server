//! `POST /share/v1/email`: the tablet's "Send by email". Sends via SMTP (STARTTLS).
//!
//! Configured by env: `SMTP_HOST`, `SMTP_PORT` (default 587), `SMTP_USER`,
//! `SMTP_PASSWORD`, `SMTP_FROM`. Without them the endpoint returns 400 `config_error`,
//! before reading the form.
//! Form fields (xochitl 3.3.2, sub_A2310): `from`, `reply-to`, `to`, `subject`, `html`,
//! `attachment` (multipart), plus `?hwc=true` on the URL. Mail goes out From `SMTP_FROM`
//! with Reply-To set to the tablet's `reply-to` (falling back to its `from`).
//!
//! A request is limited to [`MAX_BODY`] (413 over it): lettre builds the whole message in
//! memory, and mail servers refuse bigger messages anyway.

use axum::extract::multipart::MultipartError;
use axum::extract::{DefaultBodyLimit, Multipart, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{MethodRouter, post};
use lettre::message::header::ContentType;
use lettre::message::{Attachment, Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::api::AppState;
use crate::error::{Result, ServerError};

/// Footer the tablet appends to every mail body.
const AD_MARKER: &str = "Sent from my reMarkable paper tablet";

/// Largest request (all fields and attachments together; the route's `DefaultBodyLimit`).
/// Attachments are base64-encoded into the mail (4/3 the size plus line breaks), so this
/// makes a message of about 34 MiB, which is what common submission servers take at most
/// (Gmail's SMTP advertises `SIZE 35882577`; Microsoft 365 defaults to 35 MB). The
/// attachments and the encoded message are both held in memory while sending.
pub(crate) const MAX_BODY: usize = 25 * 1024 * 1024;

struct SmtpConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    from: Mailbox,
}

impl SmtpConfig {
    fn from_env() -> Result<Self> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    /// From the `SMTP_*` variables `lookup` finds (an empty value counts as unset).
    fn from_vars(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let var = |k: &str| lookup(k).filter(|v| !v.is_empty());
        let (Some(host), Some(user), Some(password), Some(from)) = (
            var("SMTP_HOST"),
            var("SMTP_USER"),
            var("SMTP_PASSWORD"),
            var("SMTP_FROM"),
        ) else {
            return Err(ServerError::Config(
                "email not configured (SMTP_HOST/USER/PASSWORD/FROM)".into(),
            ));
        };
        let port = var("SMTP_PORT").and_then(|p| p.parse().ok()).unwrap_or(587);
        let from = from
            .parse()
            .map_err(|e| ServerError::Config(format!("SMTP_FROM: {e}")))?;
        Ok(Self {
            host,
            port,
            user,
            password,
            from,
        })
    }
}

fn bad(e: impl std::fmt::Display) -> ServerError {
    ServerError::Config(e.to_string())
}

fn too_large() -> ServerError {
    ServerError::PayloadTooLarge(format!(
        "share by email is limited to {} MiB including attachments; mail servers refuse larger messages",
        MAX_BODY >> 20
    ))
}

/// A form read error: over [`MAX_BODY`] is the 413 above, anything else a 400 as before.
fn form_error(e: MultipartError) -> ServerError {
    if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
        too_large()
    } else {
        bad(e)
    }
}

/// The tablet's form fields.
#[derive(Default)]
struct ShareForm {
    to: String,
    from: String,
    reply_to: String,
    subject: String,
    html: String,
    /// (file name, content type, data)
    attachments: Vec<(String, String, bytes::Bytes)>,
}

impl ShareForm {
    async fn read(mut form: Multipart) -> Result<Self> {
        let mut f = Self::default();
        while let Some(field) = form.next_field().await.map_err(form_error)? {
            match field.name().unwrap_or_default() {
                "to" => f.to = field.text().await.map_err(form_error)?,
                "from" => f.from = field.text().await.map_err(form_error)?,
                "reply-to" => f.reply_to = field.text().await.map_err(form_error)?,
                "subject" => f.subject = field.text().await.map_err(form_error)?,
                "html" => f.html = field.text().await.map_err(form_error)?,
                "attachment" => {
                    let name = field.file_name().unwrap_or("attachment").to_owned();
                    let ct = field
                        .content_type()
                        .unwrap_or("application/octet-stream")
                        .to_owned();
                    f.attachments
                        .push((name, ct, field.bytes().await.map_err(form_error)?));
                }
                _ => {}
            }
        }
        Ok(f)
    }
}

fn strip_ad(body: &str) -> &str {
    body.find(AD_MARKER).map_or(body, |i| &body[..i])
}

/// `POST /share/v1/email`, limited to [`MAX_BODY`] (`Multipart` enforces the route's
/// `DefaultBodyLimit`), with SMTP configured from the environment.
pub(crate) fn route() -> MethodRouter<AppState> {
    route_with(SmtpConfig::from_env)
}

/// [`route`] with the SMTP settings from `smtp` instead of the environment.
fn route_with(smtp: fn() -> Result<SmtpConfig>) -> MethodRouter<AppState> {
    post(
        move |State(state): State<AppState>, headers: HeaderMap, form: Multipart| async move {
            send(&state, &headers, form, smtp).await
        },
    )
    .layer(DefaultBodyLimit::max(MAX_BODY))
}

async fn send(
    state: &AppState,
    headers: &HeaderMap,
    form: Multipart,
    smtp: fn() -> Result<SmtpConfig>,
) -> Result<StatusCode> {
    state.auth_user(headers)?;
    // Before the form is read, so a server without email never takes in the upload.
    let smtp = smtp().map_err(|e| {
        tracing::warn!("share by email requested but SMTP is not configured");
        e
    })?;
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > MAX_BODY as u64) {
        return Err(too_large());
    }
    let ShareForm {
        to,
        from,
        reply_to,
        subject,
        html,
        attachments,
    } = ShareForm::read(form).await?;

    let mut msg = Message::builder().from(smtp.from.clone()).subject(subject);
    for addr in to
        .split([',', ';'])
        .map(str::trim)
        .filter(|a| !a.is_empty())
    {
        msg = msg.to(addr
            .parse()
            .map_err(|e| bad(format!("bad recipient {addr:?}: {e}")))?);
    }
    let reply_to = if reply_to.trim().is_empty() {
        from
    } else {
        reply_to
    };
    if let Ok(r) = reply_to.trim().parse::<Mailbox>() {
        msg = msg.reply_to(r);
    }
    let mut parts = MultiPart::mixed().singlepart(SinglePart::html(strip_ad(&html).to_owned()));
    let attachment_count = attachments.len();
    // By value: `Vec::from(Bytes)` reuses the buffer instead of copying each attachment.
    for (name, ct, data) in attachments {
        let ct = ContentType::parse(&ct)
            .unwrap_or(ContentType::parse("application/octet-stream").map_err(bad)?);
        parts = parts.singlepart(Attachment::new(name).body(Vec::from(data), ct));
    }
    let email = msg.multipart(parts).map_err(bad)?;

    let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp.host)
        .map_err(bad)?
        .port(smtp.port)
        .credentials(Credentials::new(smtp.user, smtp.password))
        .build();
    mailer
        .send(email)
        .await
        .map_err(|e| ServerError::Email(e.to_string()))?;
    tracing::info!(recipients = %to, attachments = attachment_count, "shared by email");
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use futures_util::StreamExt;
    use tower::ServiceExt;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    const B: &str = "shareboundary";
    const URI: &str = "/share/v1/email";

    fn setup() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        (AppState::new(storage, devices), format!("Bearer {tk}"), tmp)
    }

    /// SMTP settings for a server that is never contacted: every form below has a
    /// recipient that is not an address, which is refused before a connection is made.
    fn configured() -> Result<SmtpConfig> {
        SmtpConfig::from_vars(|k| match k {
            "SMTP_HOST" => Some("smtp.invalid".into()),
            "SMTP_FROM" => Some("tablet@example.invalid".into()),
            "SMTP_USER" | "SMTP_PASSWORD" => Some("x".into()),
            _ => None,
        })
    }

    /// No SMTP variables set.
    fn unconfigured() -> Result<SmtpConfig> {
        SmtpConfig::from_vars(|_| None)
    }

    /// The share route with SMTP settings from `smtp` (the environment is never read).
    fn router(state: &AppState, smtp: fn() -> Result<SmtpConfig>) -> Router {
        Router::new()
            .route(URI, route_with(smtp))
            .with_state(state.clone())
    }

    /// A share form with an `attachment_len`-byte PDF, to a recipient that is not an
    /// address.
    fn form(attachment_len: usize) -> Vec<u8> {
        let mut body = format!(
            "--{B}\r\nContent-Disposition: form-data; name=\"to\"\r\n\r\nnot an address\r\n--{B}\r\nContent-Disposition: form-data; name=\"subject\"\r\n\r\nNotes\r\n--{B}\r\nContent-Disposition: form-data; name=\"attachment\"; filename=\"n.pdf\"\r\nContent-Type: application/pdf\r\n\r\n"
        )
        .into_bytes();
        body.resize(body.len() + attachment_len, b'%');
        body.extend_from_slice(format!("\r\n--{B}--\r\n").as_bytes());
        body
    }

    /// The body in 64 KiB frames without a content-length, as a chunked upload arrives.
    fn chunked(d: Vec<u8>) -> Body {
        let frames: Vec<std::io::Result<bytes::Bytes>> = d
            .chunks(64 * 1024)
            .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
            .collect();
        Body::from_stream(futures_util::stream::iter(frames))
    }

    /// `body` as a request body that notes whether it was ever read.
    fn watched(body: Vec<u8>) -> (Body, Arc<AtomicBool>) {
        let read = Arc::new(AtomicBool::new(false));
        let flag = read.clone();
        let frames = futures_util::stream::iter([body]).map(move |frame| {
            flag.store(true, Ordering::SeqCst);
            Ok::<_, std::io::Error>(frame)
        });
        (Body::from_stream(frames), read)
    }

    async fn post(
        router: Router,
        auth: &str,
        len: Option<usize>,
        body: Body,
    ) -> (StatusCode, String) {
        let mut req = Request::post(URI)
            .header("authorization", auth)
            .header("content-type", format!("multipart/form-data; boundary={B}"));
        if let Some(len) = len {
            req = req.header("content-length", len);
        }
        let resp = router.oneshot(req.body(body).unwrap()).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn without_smtp_the_form_is_never_read() {
        let (state, auth, _tmp) = setup();
        // Not configured: the 400 it always was, without taking in the upload.
        let (body, read) = watched(form(1024));
        let (status, resp) = post(router(&state, unconfigured), &auth, None, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
        assert!(resp.contains("email not configured"), "{resp}");
        assert!(!read.load(Ordering::SeqCst), "form read without SMTP");
        // Even one declared too big: email isn't set up, so that is what the tablet hears.
        let (body, read) = watched(form(1024));
        let len = Some(MAX_BODY + 1);
        let (status, resp) = post(router(&state, unconfigured), &auth, len, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
        assert!(resp.contains("email not configured"), "{resp}");
        assert!(!read.load(Ordering::SeqCst));
        // Unauthenticated, through the real router: 401 first, body unread.
        for smtp in [None, Some(unconfigured as fn() -> _), Some(configured)] {
            let router = match smtp {
                None => crate::create_router(state.clone()),
                Some(smtp) => router(&state, smtp),
            };
            let (body, read) = watched(form(1024));
            let (status, _) = post(router, "Bearer nope", Some(MAX_BODY + 1), body).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(!read.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn oversized_share_is_a_clear_413() {
        let (state, auth, _tmp) = setup();
        // Declared too big: refused before the body is read.
        let (body, read) = watched(form(1024));
        let len = Some(MAX_BODY + 1);
        let (status, resp) = post(router(&state, configured), &auth, len, body).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(resp.contains("limited to 25 MiB"), "{resp}");
        assert!(!read.load(Ordering::SeqCst), "body read");
        // Too big without a declared length: refused while reading the form.
        let body = chunked(form(MAX_BODY));
        let (status, resp) = post(router(&state, configured), &auth, None, body).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(resp.contains("limited to 25 MiB"), "{resp}");
    }

    #[tokio::test]
    async fn malformed_form_is_still_a_400() {
        let (state, auth, _tmp) = setup();
        // No closing boundary: the multipart reader's own error, a 400 as before (not
        // the 413, and not the later recipient check).
        let mut body = form(1024);
        body.truncate(body.len() - format!("\r\n--{B}--\r\n").len());
        let (status, body) = post(router(&state, configured), &auth, None, body.into()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("config_error"), "{body}");
        assert!(body.contains("multipart/form-data"), "{body}");
        assert!(!body.contains("payload_too_large"), "{body}");
    }

    #[tokio::test]
    async fn attachments_past_axums_default_are_read() {
        let (state, auth, _tmp) = setup();
        // 3 MiB (over axum's 2 MiB default): the form is read to the end, and the request
        // then fails on its recipient, which is checked after the whole form is in.
        let body = form(3 * 1024 * 1024);
        let len = body.len();
        for (len, body) in [(None, chunked(body.clone())), (Some(len), Body::from(body))] {
            let (status, body) = post(router(&state, configured), &auth, len, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert!(body.contains("bad recipient"), "{body}");
        }
    }
}
