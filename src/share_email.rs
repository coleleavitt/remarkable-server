//! `POST /share/v1/email`: the tablet's "Send by email". Sends via SMTP (STARTTLS).
//!
//! Configured by env: `SMTP_HOST`, `SMTP_PORT` (default 587), `SMTP_USER`,
//! `SMTP_PASSWORD`, `SMTP_FROM`. Without them the endpoint returns 503.
//! Form fields (xochitl 3.3.2, sub_A2310): `from`, `reply-to`, `to`, `subject`, `html`,
//! `attachment` (multipart), plus `?hwc=true` on the URL. Mail goes out From `SMTP_FROM`
//! with Reply-To set to the tablet's `reply-to` (falling back to its `from`).

use axum::{extract::{Multipart, State}, http::{HeaderMap, StatusCode}};
use lettre::{
    message::{header::ContentType, Attachment, Mailbox, MultiPart, SinglePart},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};

use crate::{api::AppState, error::{Result, ServerError}};

/// Footer the tablet appends to every mail body.
const AD_MARKER: &str = "Sent from my reMarkable paper tablet";

struct SmtpConfig { host: String, port: u16, user: String, password: String, from: Mailbox }

impl SmtpConfig {
    fn from_env() -> Result<Self> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let (Some(host), Some(user), Some(password), Some(from)) =
            (var("SMTP_HOST"), var("SMTP_USER"), var("SMTP_PASSWORD"), var("SMTP_FROM"))
        else {
            return Err(ServerError::Config("email not configured (SMTP_HOST/USER/PASSWORD/FROM)".into()));
        };
        let port = var("SMTP_PORT").and_then(|p| p.parse().ok()).unwrap_or(587);
        let from = from.parse().map_err(|e| ServerError::Config(format!("SMTP_FROM: {e}")))?;
        Ok(Self { host, port, user, password, from })
    }
}

fn bad(e: impl std::fmt::Display) -> ServerError { ServerError::Config(e.to_string()) }

fn strip_ad(body: &str) -> &str {
    body.find(AD_MARKER).map_or(body, |i| &body[..i])
}

pub async fn send(State(state): State<AppState>, headers: HeaderMap, mut form: Multipart) -> Result<StatusCode> {
    state.auth_user(&headers)?;
    let smtp = SmtpConfig::from_env().map_err(|e| {
        tracing::warn!("share by email requested but SMTP is not configured");
        e
    })?;

    let (mut to, mut from, mut reply_to, mut subject, mut html) = (String::new(), String::new(), String::new(), String::new(), String::new());
    let mut attachments = Vec::new();
    while let Some(field) = form.next_field().await.map_err(bad)? {
        match field.name().unwrap_or_default() {
            "to" => to = field.text().await.map_err(bad)?,
            "from" => from = field.text().await.map_err(bad)?,
            "reply-to" => reply_to = field.text().await.map_err(bad)?,
            "subject" => subject = field.text().await.map_err(bad)?,
            "html" => html = field.text().await.map_err(bad)?,
            "attachment" => {
                let name = field.file_name().unwrap_or("attachment").to_owned();
                let ct = field.content_type().unwrap_or("application/octet-stream").to_owned();
                attachments.push((name, ct, field.bytes().await.map_err(bad)?));
            }
            _ => {}
        }
    }

    let mut msg = Message::builder().from(smtp.from.clone()).subject(subject);
    for addr in to.split([',', ';']).map(str::trim).filter(|a| !a.is_empty()) {
        msg = msg.to(addr.parse().map_err(|e| bad(format!("bad recipient {addr:?}: {e}")))?);
    }
    let reply_to = if reply_to.trim().is_empty() { from } else { reply_to };
    if let Ok(r) = reply_to.trim().parse::<Mailbox>() {
        msg = msg.reply_to(r);
    }
    let mut parts = MultiPart::mixed().singlepart(SinglePart::html(strip_ad(&html).to_owned()));
    for (name, ct, data) in &attachments {
        let ct = ContentType::parse(ct).unwrap_or(ContentType::parse("application/octet-stream").map_err(bad)?);
        parts = parts.singlepart(Attachment::new(name.clone()).body(data.to_vec(), ct));
    }
    let email = msg.multipart(parts).map_err(bad)?;

    let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp.host)
        .map_err(bad)?
        .port(smtp.port)
        .credentials(Credentials::new(smtp.user, smtp.password))
        .build();
    mailer.send(email).await.map_err(|e| ServerError::Email(e.to_string()))?;
    tracing::info!(recipients = %to, attachments = attachments.len(), "shared by email");
    Ok(StatusCode::OK)
}
