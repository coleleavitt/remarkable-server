//! `POST /share/v1/email`: the tablet's "Send by email". Sends via SMTP (STARTTLS).
//!
//! Configured by env: `SMTP_HOST`, `SMTP_PORT` (default 587), `SMTP_USER`,
//! `SMTP_PASSWORD`, `SMTP_FROM`. Without them the endpoint returns 503.
//! Form fields (xochitl 3.3.2, sub_A2310): `from`, `reply-to`, `to`, `subject`, `html`,
//! `attachment` (multipart), plus `?hwc=true` on the URL. Mail goes out From `SMTP_FROM`
//! with Reply-To set to the tablet's `reply-to` (falling back to its `from`).
//!
//! A request is limited to [`MAX_BODY`] (413 over it): lettre builds the whole message in
//! memory, and mail servers refuse bigger messages anyway.

use axum::extract::multipart::MultipartError;
use axum::extract::{Multipart, State};
use axum::http::{HeaderMap, StatusCode, header};
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
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
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

pub async fn send(
    State(state): State<AppState>,
    headers: HeaderMap,
    form: Multipart,
) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > MAX_BODY as u64) {
        return Err(too_large());
    }
    // The form is read (bounded by MAX_BODY) before the SMTP check, so an oversized
    // request is a 413 whether or not email is configured.
    let ShareForm {
        to,
        from,
        reply_to,
        subject,
        html,
        attachments,
    } = ShareForm::read(form).await?;
    let smtp = SmtpConfig::from_env().map_err(|e| {
        tracing::warn!("share by email requested but SMTP is not configured");
        e
    })?;

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
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::device::DeviceManager;
    use crate::storage::Storage;

    const B: &str = "shareboundary";

    fn setup() -> (AppState, String, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let devices =
            DeviceManager::new(tmp.path().join("devices.db"), "local", "local.test").unwrap();
        let tk = devices.create_user_token("u@test").unwrap();
        (AppState::new(storage, devices), format!("Bearer {tk}"), tmp)
    }

    /// A share form with an `attachment_len`-byte PDF. The recipient is not an address,
    /// so even with SMTP configured in the environment nothing is ever sent.
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

    async fn post(
        state: &AppState,
        auth: &str,
        len: Option<usize>,
        body: Body,
    ) -> (StatusCode, String) {
        let mut req = Request::post("/share/v1/email")
            .header("authorization", auth)
            .header("content-type", format!("multipart/form-data; boundary={B}"));
        if let Some(len) = len {
            req = req.header("content-length", len);
        }
        let resp = crate::create_router(state.clone())
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn oversized_share_is_a_clear_413() {
        let (state, auth, _tmp) = setup();
        // Declared too big: refused before the body is read.
        let (status, body) = post(&state, &auth, Some(MAX_BODY + 1), Body::empty()).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(body.contains("limited to 25 MiB"), "{body}");
        // Too big without a declared length: refused while reading the form.
        let (status, body) = post(&state, &auth, None, chunked(form(MAX_BODY))).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(body.contains("limited to 25 MiB"), "{body}");
        // Unauthenticated: 401 first.
        let (status, _) = post(&state, "Bearer nope", Some(MAX_BODY + 1), Body::empty()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn malformed_form_is_still_a_400() {
        let (state, auth, _tmp) = setup();
        // No closing boundary: the multipart reader's own error, a 400 as before (not
        // the 413, and not the later recipient or SMTP check).
        let mut body = form(1024);
        body.truncate(body.len() - format!("\r\n--{B}--\r\n").len());
        let (status, body) = post(&state, &auth, None, Body::from(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("config_error"), "{body}");
        assert!(body.contains("multipart/form-data"), "{body}");
        assert!(!body.contains("payload_too_large"), "{body}");
    }

    #[tokio::test]
    async fn attachments_past_axums_default_are_read() {
        let (state, auth, _tmp) = setup();
        // 3 MiB (over axum's 2 MiB default): the form is read in full and the request
        // then fails on its recipient, or on SMTP not being configured, not on size.
        let body = form(3 * 1024 * 1024);
        let len = body.len();
        for (len, body) in [(None, chunked(body.clone())), (Some(len), Body::from(body))] {
            let (status, body) = post(&state, &auth, len, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert!(body.contains("config_error"), "{body}");
        }
    }
}
