//! Email-to-device integration for remarkable-server
//!
//! Receives emails via SMTP and syncs attachments (PDF, EPUB) to device folders.
//!
//! Addressing: send@{device-id}.remarkable.local
//! - Attachments are extracted and converted to remarkable format
//! - Confirmation email sent back to sender
//!
//! Example:
//! ```text
//! To: send@RM110-123-45678.remarkable.local
//! Subject: Quarterly Report
//! Attachment: report.pdf
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{
    AsyncBufRead,
    AsyncBufReadExt,
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
    BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use crate::device::DeviceManager;
use crate::error::{Result, ServerError};
use crate::storage::Storage;

/// Supported attachment types
const SUPPORTED_EXTENSIONS: &[&str] = &["pdf", "epub"];

/// Maximum message size, advertised in EHLO (`SIZE`) and enforced during DATA.
const MAX_MESSAGE_BYTES: usize = 50 * 1024 * 1024;
/// Longest DATA piece read at once; longer lines are consumed in pieces (appended
/// verbatim), so one newline-less stream can't grow the line buffer without bound.
const MAX_LINE_BYTES: u64 = 1024 * 1024;
/// Longest accepted command line (incl. CRLF). RFC 5321 §4.5.3.1.4 sets 512; allow
/// some slack for lenient clients. Longer lines are discarded whole with `500`.
const MAX_COMMAND_LINE_BYTES: u64 = 4096;

/// SMTP server configuration
#[derive(Debug, Clone)]
pub struct EmailConfig {
    /// SMTP bind address (default: 0.0.0.0:25)
    pub smtp_bind: String,
    /// Domain for email addresses (default: remarkable.local)
    pub domain: String,
    /// SMTP relay for sending confirmations (optional)
    pub relay_host: Option<String>,
    /// Relay port (default: 587)
    pub relay_port: u16,
    /// Relay username
    pub relay_user: Option<String>,
    /// Relay password
    pub relay_pass: Option<String>,
    /// From address for confirmations
    pub from_address: String,
}

impl Default for EmailConfig {
    fn default() -> Self {
        Self {
            smtp_bind: "0.0.0.0:2525".into(), // Non-privileged port
            domain: "remarkable.local".into(),
            relay_host: None,
            relay_port: 587,
            relay_user: None,
            relay_pass: None,
            from_address: "noreply@remarkable.local".into(),
        }
    }
}

/// Received email record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailRecord {
    pub id: String,
    pub from: String,
    pub to: String,
    pub subject: String,
    pub device_id: String,
    pub received_at: DateTime<Utc>,
    pub attachments: Vec<AttachmentRecord>,
    pub status: EmailStatus,
    pub confirmation_sent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentRecord {
    pub filename: String,
    pub content_type: String,
    pub size: u64,
    pub hash: String,
    pub synced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmailStatus {
    Received,
    Processing,
    Synced,
    Failed,
}

/// Email server manager
#[derive(Clone)]
pub struct EmailServer {
    inner: Arc<EmailServerInner>,
}

struct EmailServerInner {
    config: EmailConfig,
    storage: Storage,
    devices: DeviceManager,
    db: Mutex<Connection>,
    /// Pending emails being received
    pending: RwLock<HashMap<String, PendingEmail>>,
}

#[derive(Debug)]
struct PendingEmail {
    from: String,
    to: Vec<String>,
    data: Vec<u8>,
}

impl EmailServer {
    /// Create new email server
    pub fn new(
        config: EmailConfig,
        storage: Storage,
        devices: DeviceManager,
        db_path: &Path,
    ) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS emails (
                id TEXT PRIMARY KEY,
                from_addr TEXT NOT NULL,
                to_addr TEXT NOT NULL,
                subject TEXT,
                device_id TEXT NOT NULL,
                received_at TEXT NOT NULL,
                status TEXT NOT NULL,
                confirmation_sent INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS email_attachments (
                id TEXT PRIMARY KEY,
                email_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                content_type TEXT,
                size INTEGER NOT NULL,
                hash TEXT NOT NULL,
                synced INTEGER DEFAULT 0,
                FOREIGN KEY (email_id) REFERENCES emails(id)
            );
            CREATE INDEX IF NOT EXISTS idx_email_device ON emails(device_id);
            CREATE INDEX IF NOT EXISTS idx_email_status ON emails(status);",
        )?;

        Ok(Self {
            inner: Arc::new(EmailServerInner {
                config,
                storage,
                devices,
                db: Mutex::new(conn),
                pending: RwLock::new(HashMap::new()),
            }),
        })
    }

    /// Get server configuration
    pub fn config(&self) -> &EmailConfig {
        &self.inner.config
    }

    /// Start the SMTP server
    pub async fn run(&self) -> Result<()> {
        let addr: SocketAddr = self
            .inner
            .config
            .smtp_bind
            .parse()
            .map_err(|e| ServerError::Config(format!("Invalid SMTP bind address: {}", e)))?;

        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| ServerError::Io(e.to_string()))?;

        tracing::info!("SMTP server listening on {}", addr);
        tracing::info!("Email domain: {}", self.inner.config.domain);

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    tracing::debug!("SMTP connection from {}", peer);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream, peer).await {
                            tracing::error!("SMTP session error from {}: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("SMTP accept error: {}", e);
                }
            }
        }
    }

    /// Handle a single SMTP connection
    async fn handle_connection(&self, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let (reader, writer) = stream.into_split();
        self.session(reader, writer, peer).await
    }

    /// Run one SMTP session over any byte stream (a TCP connection in production).
    async fn session<R, W>(&self, reader: R, mut writer: W, peer: SocketAddr) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut reader = SmtpReader::new(BufReader::new(reader));

        // Session state
        let session_id = Uuid::new_v4().to_string();
        let mut mail_from: Option<String> = None;
        let mut rcpt_to: Vec<String> = Vec::new();
        let mut data_buf = DataBuf::new(MAX_MESSAGE_BYTES);

        // Send greeting
        let greeting = format!(
            "220 {} remarkable-server SMTP\r\n",
            self.inner.config.domain
        );
        writer.write_all(greeting.as_bytes()).await?;

        loop {
            match reader.read_command().await? {
                CommandRead::Eof => break, // Connection closed
                CommandRead::TooLong => {
                    tracing::warn!("SMTP [{session_id}] command line too long, discarded");
                    writer.write_all(b"500 Line too long\r\n").await?;
                    continue;
                }
                CommandRead::Line => {}
            }

            // Owned so the reader can be borrowed again (DATA) while this is alive.
            let text = String::from_utf8_lossy(reader.line()).into_owned();
            let line_trimmed = text.trim();
            tracing::trace!("SMTP [{session_id}] <- {}", line_trimmed);

            match parse_command(line_trimmed) {
                SmtpCommand::Hello => {
                    let response = format!(
                        "250-{} Hello {}\r\n250-SIZE {MAX_MESSAGE_BYTES}\r\n250-8BITMIME\r\n250 OK\r\n",
                        self.inner.config.domain,
                        peer.ip()
                    );
                    writer.write_all(response.as_bytes()).await?;
                }
                SmtpCommand::MailFrom(arg) => {
                    let from = extract_email_address(arg);
                    if from.is_empty() {
                        writer.write_all(b"501 Invalid sender address\r\n").await?;
                    } else {
                        mail_from = Some(from);
                        writer.write_all(b"250 OK\r\n").await?;
                    }
                }
                SmtpCommand::RcptTo(arg) => {
                    let to = extract_email_address(arg);
                    if to.is_empty() {
                        writer
                            .write_all(b"501 Invalid recipient address\r\n")
                            .await?;
                    } else if !self.validate_recipient(&to) {
                        writer.write_all(b"550 Unknown recipient\r\n").await?;
                    } else {
                        rcpt_to.push(to);
                        writer.write_all(b"250 OK\r\n").await?;
                    }
                }
                SmtpCommand::Data => {
                    if mail_from.is_none() || rcpt_to.is_empty() {
                        writer
                            .write_all(b"503 MAIL FROM and RCPT TO required first\r\n")
                            .await?;
                    } else {
                        writer
                            .write_all(b"354 End data with <CR><LF>.<CR><LF>\r\n")
                            .await?;
                        data_buf.clear();
                        if !reader.read_data(&mut data_buf).await? {
                            break; // Connection closed mid-DATA
                        }
                        if data_buf.overflowed() {
                            tracing::warn!(
                                "SMTP [{session_id}] message over {MAX_MESSAGE_BYTES} bytes discarded"
                            );
                            writer
                                .write_all(
                                    b"552 Message size exceeds fixed maximum message size\r\n",
                                )
                                .await?;
                        } else if let Some(from) = &mail_from {
                            match self.process_email(from, &rcpt_to, data_buf.bytes()).await {
                                Ok(count) => {
                                    let msg = format!(
                                        "250 OK: {count} attachment(s) queued for sync\r\n"
                                    );
                                    writer.write_all(msg.as_bytes()).await?;
                                }
                                Err(e) => {
                                    tracing::error!("Email processing failed: {}", e);
                                    writer.write_all(b"451 Email processing failed\r\n").await?;
                                }
                            }
                        }
                        // Reset for next message
                        mail_from = None;
                        rcpt_to.clear();
                        data_buf.clear();
                    }
                }
                SmtpCommand::Quit => {
                    writer.write_all(b"221 Bye\r\n").await?;
                    break;
                }
                SmtpCommand::Rset => {
                    mail_from = None;
                    rcpt_to.clear();
                    data_buf.clear();
                    writer.write_all(b"250 OK\r\n").await?;
                }
                SmtpCommand::Noop => {
                    writer.write_all(b"250 OK\r\n").await?;
                }
                SmtpCommand::Unknown => {
                    writer.write_all(b"500 Unknown command\r\n").await?;
                }
            }
        }

        tracing::debug!("SMTP session {} closed", session_id);
        Ok(())
    }

    /// Validate recipient address format: send@{device-id}.remarkable.local
    fn validate_recipient(&self, address: &str) -> bool {
        let parts: Vec<&str> = address.split('@').collect();
        if parts.len() != 2 {
            return false;
        }

        let local = parts[0].to_lowercase();
        let domain = parts[1].to_lowercase();

        // Must be "send@..." or "sync@..."
        if local != "send" && local != "sync" {
            return false;
        }

        // Domain must be {device-id}.{our-domain}
        let expected_suffix = format!(".{}", self.inner.config.domain);
        if !domain.ends_with(&expected_suffix) {
            return false;
        }

        // Extract device ID
        let device_id = &domain[..domain.len() - expected_suffix.len()];

        // Verify device exists
        match self.inner.devices.get_device(device_id) {
            Ok(Some(_)) => true,
            _ => {
                tracing::warn!("Email to unknown device: {}", device_id);
                false
            }
        }
    }

    /// Extract device ID from recipient address
    fn extract_device_id(&self, address: &str) -> Option<String> {
        let parts: Vec<&str> = address.split('@').collect();
        if parts.len() != 2 {
            return None;
        }

        let domain = parts[1].to_lowercase();
        let expected_suffix = format!(".{}", self.inner.config.domain);

        if !domain.ends_with(&expected_suffix) {
            return None;
        }

        Some(domain[..domain.len() - expected_suffix.len()].to_uppercase())
    }

    /// Process received email data
    async fn process_email(&self, from: &str, to: &[String], data: &[u8]) -> Result<usize> {
        // Parse the email
        let parsed = mailparse::parse_mail(data)
            .map_err(|e| ServerError::Email(format!("Parse error: {}", e)))?;

        let subject = parsed
            .headers
            .iter()
            .find(|h| h.get_key().eq_ignore_ascii_case("subject"))
            .map(|h| h.get_value())
            .unwrap_or_else(|| "No Subject".into());

        tracing::info!("Processing email from {} subject: {}", from, subject);

        let mut total_attachments = 0;

        // Process each recipient (device)
        for recipient in to {
            let device_id = match self.extract_device_id(recipient) {
                Some(id) => id,
                None => continue,
            };

            // Create email record
            let email_id = Uuid::new_v4().to_string();
            let now = Utc::now();

            {
                let db = self.inner.db.lock().unwrap();
                db.execute(
                    "INSERT INTO emails (id, from_addr, to_addr, subject, device_id, received_at, status)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![email_id, from, recipient, subject, device_id, now.to_rfc3339(), "processing"],
                )?;
            }

            // Extract and sync attachments
            let attachments = self.extract_attachments(&parsed)?;
            let synced_count = self
                .sync_attachments(&email_id, &device_id, &attachments, &subject)
                .await?;
            total_attachments += synced_count;

            // Update status
            {
                let db = self.inner.db.lock().unwrap();
                let status = if synced_count > 0 {
                    "synced"
                } else {
                    "no_attachments"
                };
                db.execute(
                    "UPDATE emails SET status = ?1 WHERE id = ?2",
                    params![status, email_id],
                )?;
            }

            // Send confirmation
            if synced_count > 0 {
                if let Err(e) = self
                    .send_confirmation(from, &subject, synced_count, &device_id)
                    .await
                {
                    tracing::warn!("Failed to send confirmation: {}", e);
                }
            }
        }

        Ok(total_attachments)
    }

    /// Extract attachments from parsed email
    fn extract_attachments(&self, mail: &mailparse::ParsedMail) -> Result<Vec<Attachment>> {
        let mut attachments = Vec::new();
        self.extract_attachments_recursive(mail, &mut attachments)?;
        Ok(attachments)
    }

    fn extract_attachments_recursive(
        &self,
        mail: &mailparse::ParsedMail,
        attachments: &mut Vec<Attachment>,
    ) -> Result<()> {
        // Check content disposition
        let content_type = mail.ctype.mimetype.to_lowercase();
        let filename = mail
            .get_content_disposition()
            .params
            .get("filename")
            .cloned()
            .or_else(|| mail.ctype.params.get("name").cloned());

        if let Some(name) = filename {
            let ext = Path::new(&name)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();

            if SUPPORTED_EXTENSIONS.contains(&ext.as_str()) {
                let body = mail
                    .get_body_raw()
                    .map_err(|e| ServerError::Email(format!("Body decode error: {}", e)))?;

                // Calculate hash
                let mut hasher = Sha256::new();
                hasher.update(&body);
                let hash = hex::encode(hasher.finalize());

                attachments.push(Attachment {
                    filename: name,
                    content_type: content_type.clone(),
                    data: body,
                    hash,
                });
            }
        }

        // Recurse into multipart parts
        for subpart in &mail.subparts {
            self.extract_attachments_recursive(subpart, attachments)?;
        }

        Ok(())
    }

    /// Sync attachments to device storage
    async fn sync_attachments(
        &self,
        email_id: &str,
        device_id: &str,
        attachments: &[Attachment],
        subject: &str,
    ) -> Result<usize> {
        let mut synced = 0;

        for attachment in attachments {
            tracing::info!(
                "Syncing attachment: {} ({} bytes) to device {}",
                attachment.filename,
                attachment.data.len(),
                device_id
            );

            // Only PDF/EPUB become documents; the tablet can't open anything else.
            let Some(ext) = document_ext(&attachment.filename, &attachment.content_type) else {
                tracing::warn!(
                    "Skipping attachment {} ({}): not a PDF/EPUB",
                    attachment.filename,
                    attachment.content_type
                );
                continue;
            };
            // Adds the document to the sync tree and commits a new root, so the device pulls it.
            let (doc_id, generation) = crate::documents::create_document(
                &self.inner.storage,
                &strip_extension(&attachment.filename),
                ext,
                &attachment.data,
            )?;
            tracing::info!(
                "Emailed {} became document {} (subject {:?}, root generation {})",
                attachment.filename,
                doc_id,
                subject,
                generation
            );

            // Record attachment in database (the hash column holds the new document id)
            let attachment_id = Uuid::new_v4().to_string();
            {
                let db = self.inner.db.lock().unwrap();
                db.execute(
                    "INSERT INTO email_attachments (id, email_id, filename, content_type, size, hash, synced)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
                    params![
                        attachment_id,
                        email_id,
                        attachment.filename,
                        attachment.content_type,
                        attachment.data.len() as i64,
                        doc_id,
                    ],
                )?;
            }

            synced += 1;
        }

        Ok(synced)
    }

    /// Send confirmation email to sender
    async fn send_confirmation(
        &self,
        to: &str,
        subject: &str,
        count: usize,
        device_id: &str,
    ) -> Result<()> {
        let relay = match &self.inner.config.relay_host {
            Some(host) => host,
            None => {
                tracing::debug!("No relay configured, skipping confirmation");
                return Ok(());
            }
        };

        let message = format!(
            "Your email \"{}\r\n\" has been received and {} attachment(s) are being synced to your reMarkable device ({}).\r\n\r\n--\r\nreMarkable Server",
            subject, count, device_id
        );

        // Build email using lettre
        use lettre::transport::smtp::authentication::Credentials;
        use lettre::{Message, SmtpTransport, Transport};

        let email = Message::builder()
            .from(
                self.inner
                    .config
                    .from_address
                    .parse()
                    .map_err(|e| ServerError::Email(format!("Invalid from address: {}", e)))?,
            )
            .to(to
                .parse()
                .map_err(|e| ServerError::Email(format!("Invalid to address: {}", e)))?)
            .subject(format!("reMarkable: {} synced", count))
            .body(message)
            .map_err(|e| ServerError::Email(format!("Build email error: {}", e)))?;

        let mut mailer = SmtpTransport::relay(relay)
            .map_err(|e| ServerError::Email(format!("SMTP relay error: {}", e)))?
            .port(self.inner.config.relay_port);

        if let (Some(user), Some(pass)) =
            (&self.inner.config.relay_user, &self.inner.config.relay_pass)
        {
            mailer = mailer.credentials(Credentials::new(user.clone(), pass.clone()));
        }

        let mailer = mailer.build();

        mailer
            .send(&email)
            .map_err(|e| ServerError::Email(format!("Send error: {}", e)))?;

        // Mark confirmation sent
        {
            let db = self.inner.db.lock().unwrap();
            db.execute(
                "UPDATE emails SET confirmation_sent = 1 WHERE id IN (
                    SELECT id FROM emails WHERE from_addr = ?1 ORDER BY received_at DESC LIMIT 1
                )",
                params![to],
            )?;
        }

        tracing::info!("Confirmation sent to {}", to);
        Ok(())
    }

    /// List emails for a device
    pub fn list_emails(&self, device_id: &str, limit: usize) -> Result<Vec<EmailRecord>> {
        let db = self.inner.db.lock().unwrap();
        let mut stmt = db.prepare(
            "SELECT e.id, e.from_addr, e.to_addr, e.subject, e.device_id, e.received_at, e.status, e.confirmation_sent
             FROM emails e
             WHERE e.device_id = ?1
             ORDER BY e.received_at DESC
             LIMIT ?2"
        )?;

        let rows = stmt.query_map(params![device_id, limit as i64], |row| {
            Ok(EmailRecord {
                id: row.get(0)?,
                from: row.get(1)?,
                to: row.get(2)?,
                subject: row.get(3)?,
                device_id: row.get(4)?,
                received_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(5)?)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                attachments: Vec::new(), // Loaded separately if needed
                status: match row.get::<_, String>(6)?.as_str() {
                    "processing" => EmailStatus::Processing,
                    "synced" => EmailStatus::Synced,
                    "failed" => EmailStatus::Failed,
                    _ => EmailStatus::Received,
                },
                confirmation_sent: row.get::<_, i32>(7)? != 0,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ServerError::Database(e.to_string()))
    }

    /// Get email statistics
    pub fn stats(&self) -> Result<EmailStats> {
        let db = self.inner.db.lock().unwrap();
        let total: i64 = db.query_row("SELECT COUNT(*) FROM emails", [], |r| r.get(0))?;
        let synced: i64 = db.query_row(
            "SELECT COUNT(*) FROM emails WHERE status = 'synced'",
            [],
            |r| r.get(0),
        )?;
        let attachments: i64 =
            db.query_row("SELECT COUNT(*) FROM email_attachments", [], |r| r.get(0))?;
        let bytes: i64 = db.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM email_attachments",
            [],
            |r| r.get(0),
        )?;

        Ok(EmailStats {
            total_emails: total as u64,
            synced_emails: synced as u64,
            total_attachments: attachments as u64,
            total_bytes: bytes as u64,
        })
    }
}

#[derive(Debug)]
struct Attachment {
    filename: String,
    content_type: String,
    data: Vec<u8>,
    hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmailStats {
    pub total_emails: u64,
    pub synced_emails: u64,
    pub total_attachments: u64,
    pub total_bytes: u64,
}

/// One parsed SMTP command line; `MailFrom`/`RcptTo` carry the text after the colon.
#[derive(Debug, PartialEq, Eq)]
enum SmtpCommand<'a> {
    Hello,
    MailFrom(&'a str),
    RcptTo(&'a str),
    Data,
    Quit,
    Rset,
    Noop,
    Unknown,
}

/// `s` without `prefix`, matched ASCII-case-insensitively on the original string.
/// Uses `get` so a multi-byte char straddling the prefix length can't panic.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &s[prefix.len()..])
}

/// Parse a (trimmed) SMTP command line. Never panics, whatever the input.
fn parse_command(line: &str) -> SmtpCommand<'_> {
    if strip_prefix_ci(line, "HELO").is_some() || strip_prefix_ci(line, "EHLO").is_some() {
        SmtpCommand::Hello
    } else if let Some(arg) = strip_prefix_ci(line, "MAIL FROM:") {
        SmtpCommand::MailFrom(arg)
    } else if let Some(arg) = strip_prefix_ci(line, "RCPT TO:") {
        SmtpCommand::RcptTo(arg)
    } else if line.eq_ignore_ascii_case("DATA") {
        SmtpCommand::Data
    } else if line.eq_ignore_ascii_case("QUIT") {
        SmtpCommand::Quit
    } else if line.eq_ignore_ascii_case("RSET") {
        SmtpCommand::Rset
    } else if line.eq_ignore_ascii_case("NOOP") {
        SmtpCommand::Noop
    } else {
        SmtpCommand::Unknown
    }
}

/// Outcome of [`SmtpReader::read_command`]; the line itself is in [`SmtpReader::line`].
#[derive(Debug, PartialEq, Eq)]
enum CommandRead {
    /// A complete command line (or a final unterminated one before EOF).
    Line,
    /// The line exceeded the command cap; all of it, up to and including its newline,
    /// was discarded so no part of it can be parsed as a command.
    TooLong,
    Eof,
}

/// Line framing for an SMTP session with bounded reads.
///
/// Reads are capped per call, so a line longer than the cap arrives in pieces. The
/// reader tracks whether each piece completed a line (ended in `\n`), so a piece of an
/// over-long line is never mistaken for the start of a new command or for the
/// end-of-data `.` line.
struct SmtpReader<R> {
    inner: R,
    buf: Vec<u8>,
    command_cap: u64,
    data_cap: u64,
}

impl<R: AsyncBufRead + Unpin> SmtpReader<R> {
    fn new(inner: R) -> Self {
        Self::with_caps(inner, MAX_COMMAND_LINE_BYTES, MAX_LINE_BYTES)
    }

    fn with_caps(inner: R, command_cap: u64, data_cap: u64) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            command_cap,
            data_cap,
        }
    }

    /// The line from the last [`CommandRead::Line`].
    fn line(&self) -> &[u8] {
        &self.buf
    }

    /// Read up to `cap` bytes, stopping after the first `\n`. Returns bytes read.
    async fn read_piece(&mut self, cap: u64) -> std::io::Result<usize> {
        self.buf.clear();
        (&mut self.inner)
            .take(cap)
            .read_until(b'\n', &mut self.buf)
            .await
    }

    fn piece_ends_line(&self) -> bool {
        self.buf.last() == Some(&b'\n')
    }

    async fn read_command(&mut self) -> std::io::Result<CommandRead> {
        let n = self.read_piece(self.command_cap).await?;
        if n == 0 {
            return Ok(CommandRead::Eof);
        }
        if self.piece_ends_line() || (n as u64) < self.command_cap {
            return Ok(CommandRead::Line);
        }
        // Over-long: swallow the rest of this line so none of it runs as a command.
        loop {
            let n = self.read_piece(self.command_cap).await?;
            if n == 0 || self.piece_ends_line() {
                break;
            }
        }
        self.buf.clear();
        Ok(CommandRead::TooLong)
    }

    /// Read DATA into `data` until the end-of-data line. Returns `false` on EOF first.
    /// The `.` terminator is only recognised as a whole line that starts at a real line
    /// start; pieces of longer lines are appended verbatim.
    async fn read_data(&mut self, data: &mut DataBuf) -> std::io::Result<bool> {
        let mut at_line_start = true;
        loop {
            if self.read_piece(self.data_cap).await? == 0 {
                return Ok(false);
            }
            let complete = self.piece_ends_line();
            if at_line_start && matches!(self.buf.as_slice(), b".\r\n" | b".\n") {
                return Ok(true);
            }
            data.push(&self.buf, at_line_start);
            at_line_start = complete;
        }
    }
}

/// DATA accumulator: undoes dot-stuffing and stops buffering once the message would
/// exceed `max` bytes (the rest is read and discarded; the caller then replies 552).
#[derive(Debug)]
struct DataBuf {
    buf: Vec<u8>,
    max: usize,
    overflow: bool,
}

impl DataBuf {
    fn new(max: usize) -> Self {
        Self {
            buf: Vec::new(),
            max,
            overflow: false,
        }
    }

    /// Append one piece of DATA. `at_line_start` says whether `piece` begins a new
    /// line; only then is a leading dot a stuffed dot (RFC 5321 §4.5.2) and removed.
    /// Continuation pieces of an over-long line are appended verbatim.
    fn push(&mut self, piece: &[u8], at_line_start: bool) {
        if self.overflow {
            return;
        }
        let content = match piece {
            [b'.', rest @ ..] if at_line_start => rest,
            _ => piece,
        };
        if self.buf.len() + content.len() > self.max {
            self.overflow = true;
            self.buf = Vec::new(); // release the memory now, not at end of DATA
        } else {
            self.buf.extend_from_slice(content);
        }
    }

    fn overflowed(&self) -> bool {
        self.overflow
    }

    fn bytes(&self) -> &[u8] {
        &self.buf
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.overflow = false;
    }
}

/// Extract email address from SMTP command argument
fn extract_email_address(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('<') && s.ends_with('>') {
        s[1..s.len() - 1].to_string()
    } else {
        // Handle "user@domain" or "<user@domain> SIZE=..."
        s.split_whitespace()
            .next()
            .map(|addr| {
                if addr.starts_with('<') && addr.ends_with('>') {
                    addr[1..addr.len() - 1].to_string()
                } else {
                    addr.to_string()
                }
            })
            .unwrap_or_default()
    }
}

/// Sanitize filename for filesystem use
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-' || *c == '_')
        .take(50)
        .collect::<String>()
        .trim()
        .to_string()
}

/// Strip file extension
fn strip_extension(filename: &str) -> String {
    Path::new(filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(filename)
        .to_string()
}

/// Detect file type from extension
fn detect_file_type(filename: &str) -> &'static str {
    match Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .as_deref()
    {
        Some("pdf") => "pdf",
        Some("epub") => "epub",
        _ => "pdf",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_email_address() {
        assert_eq!(
            extract_email_address("<user@example.com>"),
            "user@example.com"
        );
        assert_eq!(
            extract_email_address("user@example.com"),
            "user@example.com"
        );
        assert_eq!(
            extract_email_address("<user@example.com> SIZE=1234"),
            "user@example.com"
        );
    }

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(
            sanitize_filename("Quarterly Report 2024"),
            "Quarterly Report 2024"
        );
        assert_eq!(sanitize_filename("Report<>|:*?"), "Report");
        assert_eq!(sanitize_filename("a".repeat(100).as_str()).len(), 50);
    }

    #[test]
    fn parse_command_is_case_insensitive_and_keeps_argument() {
        assert_eq!(parse_command("EHLO client"), SmtpCommand::Hello);
        assert_eq!(parse_command("helo x"), SmtpCommand::Hello);
        assert_eq!(
            parse_command("mail from:<A@b.c>"),
            SmtpCommand::MailFrom("<A@b.c>")
        );
        assert_eq!(
            parse_command("Rcpt To: <send@x.remarkable.local>"),
            SmtpCommand::RcptTo(" <send@x.remarkable.local>")
        );
        assert_eq!(parse_command("data"), SmtpCommand::Data);
        assert_eq!(parse_command("QUIT"), SmtpCommand::Quit);
        assert_eq!(parse_command("rset"), SmtpCommand::Rset);
        assert_eq!(parse_command("NoOp"), SmtpCommand::Noop);
        assert_eq!(parse_command("DATAX"), SmtpCommand::Unknown);
        assert_eq!(parse_command(""), SmtpCommand::Unknown);
    }

    #[test]
    fn parse_command_never_panics_on_non_ascii() {
        // `to_uppercase` changes byte lengths ('ß' -> "SS", 'ı' 2 bytes -> 'I' 1 byte),
        // which made the old `line[10..]` slice land off a char boundary.
        for line in [
            "maıl from:<x@y>",
            "MAIL FROMß",
            "mail from:ßßß",
            "rcpt to:ﬀ",
            "MAIL FRO\u{e9}:",
            "RCPT T\u{f6}:x",
            "ä",
            "RCPT TO",
        ] {
            let _ = parse_command(line);
        }
        assert_eq!(
            parse_command("maıl from:<x@y>"),
            SmtpCommand::Unknown,
            "dotless i is not ASCII 'i'"
        );
        assert_eq!(
            parse_command("MAIL FROM:<ü@y>"),
            SmtpCommand::MailFrom("<ü@y>")
        );
    }

    #[test]
    fn data_buf_unstuffs_dots() {
        let mut d = DataBuf::new(1000);
        d.push(b"Subject: x\r\n", true);
        d.push(b"..leading dot\r\n", true);
        assert_eq!(d.bytes(), b"Subject: x\r\n.leading dot\r\n");
        assert!(!d.overflowed());
    }

    #[test]
    fn data_buf_enforces_size_limit() {
        let mut d = DataBuf::new(10);
        d.push(b"12345\r\n", true); // 7
        d.push(b"abc", true); // 10: exactly at the limit is fine
        assert!(!d.overflowed());
        d.push(b"x", true); // 11 > 10
        assert!(d.overflowed());
        assert!(d.bytes().is_empty(), "buffer released on overflow");
        assert_eq!(d.buf.capacity(), 0);
        d.push(&[b'y'; 100], true); // further lines discarded
        assert!(d.bytes().is_empty());
        d.clear(); // next message (after 552 / RSET) starts fresh
        assert!(!d.overflowed());
        d.push(b"ok", true);
        assert_eq!(d.bytes(), b"ok");
    }

    #[test]
    fn data_buf_unstuffs_only_at_line_start() {
        let mut d = DataBuf::new(1000);
        d.push(b".x\r\n", true);
        d.push(b"abc", true);
        d.push(b"..continued\r\n", false); // piece of a long line: verbatim
        assert_eq!(d.bytes(), b"x\r\nabc..continued\r\n");
    }

    fn reader(input: &[u8], command_cap: u64, data_cap: u64) -> SmtpReader<&[u8]> {
        SmtpReader::with_caps(input, command_cap, data_cap)
    }

    #[tokio::test]
    async fn over_long_command_is_discarded_whole() {
        // With an 8-byte cap, "NOOP AAA" is the first piece and "QUIT\r\n" the rest of
        // the same line; it must not be parsed as a command.
        let mut r = reader(b"NOOP AAAQUIT\r\nNOOP\r\n", 8, 64);
        assert_eq!(r.read_command().await.unwrap(), CommandRead::TooLong);
        assert_eq!(r.read_command().await.unwrap(), CommandRead::Line);
        assert_eq!(r.line(), b"NOOP\r\n");
        assert_eq!(r.read_command().await.unwrap(), CommandRead::Eof);

        // Remainder spanning several pieces is swallowed too.
        let long = [&[b'N'; 30][..], b"QUIT\r\nRSET\r\n"].concat();
        let mut r = reader(&long, 8, 64);
        assert_eq!(r.read_command().await.unwrap(), CommandRead::TooLong);
        assert_eq!(r.read_command().await.unwrap(), CommandRead::Line);
        assert_eq!(r.line(), b"RSET\r\n");

        // Exactly at the cap (newline included) is still a line.
        let mut r = reader(b"ABCDEF\r\n", 8, 64);
        assert_eq!(r.read_command().await.unwrap(), CommandRead::Line);
        assert_eq!(r.line(), b"ABCDEF\r\n");
    }

    #[tokio::test]
    async fn data_piece_boundary_does_not_end_data() {
        // The 8-byte cap splits the first line right before ".\r\n".
        let mut r = reader(b"xxxxxxxx.\r\nmore\r\n.\r\nNOOP\r\n", 64, 8);
        let mut d = DataBuf::new(1000);
        assert!(r.read_data(&mut d).await.unwrap());
        assert_eq!(d.bytes(), b"xxxxxxxx.\r\nmore\r\n");
        assert_eq!(r.read_command().await.unwrap(), CommandRead::Line);
        assert_eq!(r.line(), b"NOOP\r\n");
    }

    #[tokio::test]
    async fn data_unstuffs_dots_only_at_real_line_starts() {
        // "..z" lands at a piece boundary mid-line: kept verbatim. "..w" starts a line.
        let mut r = reader(b"yyyyyyyy..z\r\n..w\r\n.\r\n", 64, 8);
        let mut d = DataBuf::new(1000);
        assert!(r.read_data(&mut d).await.unwrap());
        assert_eq!(d.bytes(), b"yyyyyyyy..z\r\n.w\r\n");
    }

    #[tokio::test]
    async fn data_long_lines_count_toward_size_limit() {
        let mut input = vec![b'a'; 100];
        input.extend_from_slice(b"\r\n.\r\n");
        let mut r = reader(&input, 64, 8);
        let mut d = DataBuf::new(50);
        assert!(r.read_data(&mut d).await.unwrap(), "terminator still found");
        assert!(d.overflowed());
    }

    #[tokio::test]
    async fn data_eof_before_terminator() {
        let mut r = reader(b"partial\r\n", 64, 8);
        let mut d = DataBuf::new(1000);
        assert!(!r.read_data(&mut d).await.unwrap());
    }

    fn test_server(tmp: &Path) -> EmailServer {
        let storage = Storage::new(tmp.join("storage")).unwrap();
        let devices = DeviceManager::new(tmp.join("devices.db"), "us", "test").unwrap();
        let code = devices.create_pairing_code("user").unwrap();
        devices.exchange_code(&code, "dev", "remarkable").unwrap();
        EmailServer::new(
            EmailConfig::default(),
            storage,
            devices,
            &tmp.join("email.db"),
        )
        .unwrap()
    }

    /// Feed `input` to a session and return everything it wrote back.
    async fn run_session(server: &EmailServer, input: Vec<u8>) -> String {
        let (mut client, conn) = tokio::io::duplex(1 << 20);
        let (r, w) = tokio::io::split(conn);
        let peer: SocketAddr = "127.0.0.1:2525".parse().unwrap();
        let client_side = async move {
            client.write_all(&input).await.unwrap();
            client.shutdown().await.unwrap();
            let mut out = Vec::new();
            client.read_to_end(&mut out).await.unwrap();
            out
        };
        let (res, out) = tokio::join!(server.session(r, w, peer), client_side);
        res.unwrap();
        String::from_utf8(out).unwrap()
    }

    #[tokio::test]
    async fn session_rejects_over_long_command_without_running_embedded_command() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = test_server(tmp.path());
        // First read piece is exactly the command cap; "QUIT" follows on the same line.
        let pad = MAX_COMMAND_LINE_BYTES as usize - "NOOP ".len();
        let mut input = format!("NOOP {}", "A".repeat(pad)).into_bytes();
        input.extend_from_slice(b"QUIT\r\nNOOP\r\nQUIT\r\n");
        let out = run_session(&server, input).await;
        let lines: Vec<&str> = out.split("\r\n").filter(|l| !l.is_empty()).collect();
        assert_eq!(
            &lines[1..],
            ["500 Line too long", "250 OK", "221 Bye"],
            "{out}"
        );
    }

    #[tokio::test]
    async fn session_over_size_message_gets_552() {
        let tmp = tempfile::TempDir::new().unwrap();
        let server = test_server(tmp.path());
        let mut input =
            b"EHLO c\r\nMAIL FROM:<a@b.c>\r\nRCPT TO:<send@dev.remarkable.local>\r\nDATA\r\n"
                .to_vec();
        // One newline-less line well past the data piece cap, then over the size cap.
        let line = vec![b'z'; MAX_LINE_BYTES as usize * 3];
        while input.len() <= MAX_MESSAGE_BYTES + line.len() {
            input.extend_from_slice(&line);
        }
        input.extend_from_slice(b"\r\n.\r\nNOOP\r\nQUIT\r\n");
        let out = run_session(&server, input).await;
        assert!(
            out.ends_with(
                "552 Message size exceeds fixed maximum message size\r\n250 OK\r\n221 Bye\r\n"
            ),
            "{out}"
        );
    }

    #[test]
    fn data_buf_default_limit_matches_advertised_size() {
        assert_eq!(MAX_MESSAGE_BYTES, 52_428_800);
    }

    #[test]
    fn test_strip_extension() {
        assert_eq!(strip_extension("report.pdf"), "report");
        assert_eq!(strip_extension("my.book.epub"), "my.book");
        assert_eq!(strip_extension("noext"), "noext");
    }
}

/// Document type for an attachment, from its extension or MIME type.
fn document_ext(filename: &str, content_type: &str) -> Option<&'static str> {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".pdf") || content_type.eq_ignore_ascii_case("application/pdf") {
        Some("pdf")
    } else if lower.ends_with(".epub") || content_type.eq_ignore_ascii_case("application/epub+zip")
    {
        Some("epub")
    } else {
        None
    }
}
