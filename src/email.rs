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

use crate::error::{Result, ServerError};
use crate::storage::Storage;
use crate::device::DeviceManager;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::Mutex;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

/// Supported attachment types
const SUPPORTED_EXTENSIONS: &[&str] = &["pdf", "epub"];

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
            CREATE INDEX IF NOT EXISTS idx_email_status ON emails(status);"
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
        let addr: SocketAddr = self.inner.config.smtp_bind.parse()
            .map_err(|e| ServerError::Config(format!("Invalid SMTP bind address: {}", e)))?;
        
        let listener = TcpListener::bind(&addr).await
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
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        
        // Session state
        let session_id = Uuid::new_v4().to_string();
        let mut mail_from: Option<String> = None;
        let mut rcpt_to: Vec<String> = Vec::new();
        let mut in_data = false;
        let mut data_buf: Vec<u8> = Vec::new();

        // Send greeting
        let greeting = format!("220 {} remarkable-server SMTP\r\n", self.inner.config.domain);
        writer.write_all(greeting.as_bytes()).await?;

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break; // Connection closed
            }

            let line_trimmed = line.trim();
            tracing::trace!("SMTP [{session_id}] <- {}", line_trimmed);

            if in_data {
                // Check for end of data
                if line_trimmed == "." {
                    in_data = false;
                    
                    // Process the email
                    if let Some(from) = &mail_from {
                        match self.process_email(from, &rcpt_to, &data_buf).await {
                            Ok(count) => {
                                let msg = format!("250 OK: {} attachment(s) queued for sync\r\n", count);
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
                } else {
                    // Handle dot-stuffing
                    let content = if line.starts_with("..") {
                        &line[1..]
                    } else {
                        &line
                    };
                    data_buf.extend_from_slice(content.as_bytes());
                }
                continue;
            }

            // Parse SMTP commands
            let cmd = line_trimmed.to_uppercase();
            
            if cmd.starts_with("HELO") || cmd.starts_with("EHLO") {
                let response = format!(
                    "250-{} Hello {}\r\n250-SIZE 52428800\r\n250-8BITMIME\r\n250 OK\r\n",
                    self.inner.config.domain,
                    peer.ip()
                );
                writer.write_all(response.as_bytes()).await?;
            } else if cmd.starts_with("MAIL FROM:") {
                let from = extract_email_address(&line_trimmed[10..]);
                if from.is_empty() {
                    writer.write_all(b"501 Invalid sender address\r\n").await?;
                } else {
                    mail_from = Some(from);
                    writer.write_all(b"250 OK\r\n").await?;
                }
            } else if cmd.starts_with("RCPT TO:") {
                let to = extract_email_address(&line_trimmed[8..]);
                if to.is_empty() {
                    writer.write_all(b"501 Invalid recipient address\r\n").await?;
                } else if !self.validate_recipient(&to) {
                    writer.write_all(b"550 Unknown recipient\r\n").await?;
                } else {
                    rcpt_to.push(to);
                    writer.write_all(b"250 OK\r\n").await?;
                }
            } else if cmd == "DATA" {
                if mail_from.is_none() || rcpt_to.is_empty() {
                    writer.write_all(b"503 MAIL FROM and RCPT TO required first\r\n").await?;
                } else {
                    in_data = true;
                    writer.write_all(b"354 End data with <CR><LF>.<CR><LF>\r\n").await?;
                }
            } else if cmd == "QUIT" {
                writer.write_all(b"221 Bye\r\n").await?;
                break;
            } else if cmd == "RSET" {
                mail_from = None;
                rcpt_to.clear();
                data_buf.clear();
                writer.write_all(b"250 OK\r\n").await?;
            } else if cmd == "NOOP" {
                writer.write_all(b"250 OK\r\n").await?;
            } else {
                writer.write_all(b"500 Unknown command\r\n").await?;
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
        
        let subject = parsed.headers.iter()
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
            let synced_count = self.sync_attachments(&email_id, &device_id, &attachments, &subject).await?;
            total_attachments += synced_count;
            
            // Update status
            {
                let db = self.inner.db.lock().unwrap();
                let status = if synced_count > 0 { "synced" } else { "no_attachments" };
                db.execute(
                    "UPDATE emails SET status = ?1 WHERE id = ?2",
                    params![status, email_id],
                )?;
            }
            
            // Send confirmation
            if synced_count > 0 {
                if let Err(e) = self.send_confirmation(from, &subject, synced_count, &device_id).await {
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
        attachments: &mut Vec<Attachment>
    ) -> Result<()> {
        // Check content disposition
        let content_type = mail.ctype.mimetype.to_lowercase();
        let filename = mail.get_content_disposition().params
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
                let body = mail.get_body_raw()
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
                tracing::warn!("Skipping attachment {} ({}): not a PDF/EPUB", attachment.filename, attachment.content_type);
                continue;
            };
            // Adds the document to the sync tree and commits a new root, so the device pulls it.
            let (doc_id, generation) = crate::documents::create_document(&self.inner.storage, &strip_extension(&attachment.filename), ext, &attachment.data)?;
            tracing::info!("Emailed {} became document {} (subject {:?}, root generation {})", attachment.filename, doc_id, subject, generation);

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
        use lettre::{Message, SmtpTransport, Transport};
        use lettre::transport::smtp::authentication::Credentials;
        
        let email = Message::builder()
            .from(self.inner.config.from_address.parse().map_err(|e| ServerError::Email(format!("Invalid from address: {}", e)))?)
            .to(to.parse().map_err(|e| ServerError::Email(format!("Invalid to address: {}", e)))?)
            .subject(format!("reMarkable: {} synced", count))
            .body(message)
            .map_err(|e| ServerError::Email(format!("Build email error: {}", e)))?;
        
        let mut mailer = SmtpTransport::relay(relay)
            .map_err(|e| ServerError::Email(format!("SMTP relay error: {}", e)))?
            .port(self.inner.config.relay_port);
        
        if let (Some(user), Some(pass)) = (&self.inner.config.relay_user, &self.inner.config.relay_pass) {
            mailer = mailer.credentials(Credentials::new(user.clone(), pass.clone()));
        }
        
        let mailer = mailer.build();
        
        mailer.send(&email)
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
        let synced: i64 = db.query_row("SELECT COUNT(*) FROM emails WHERE status = 'synced'", [], |r| r.get(0))?;
        let attachments: i64 = db.query_row("SELECT COUNT(*) FROM email_attachments", [], |r| r.get(0))?;
        let bytes: i64 = db.query_row("SELECT COALESCE(SUM(size), 0) FROM email_attachments", [], |r| r.get(0))?;
        
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

/// Extract email address from SMTP command argument
fn extract_email_address(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('<') && s.ends_with('>') {
        s[1..s.len()-1].to_string()
    } else {
        // Handle "user@domain" or "<user@domain> SIZE=..."
        s.split_whitespace()
            .next()
            .map(|addr| {
                if addr.starts_with('<') && addr.ends_with('>') {
                    addr[1..addr.len()-1].to_string()
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
        _ => "pdf"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_email_address() {
        assert_eq!(extract_email_address("<user@example.com>"), "user@example.com");
        assert_eq!(extract_email_address("user@example.com"), "user@example.com");
        assert_eq!(extract_email_address("<user@example.com> SIZE=1234"), "user@example.com");
    }
    
    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("Quarterly Report 2024"), "Quarterly Report 2024");
        assert_eq!(sanitize_filename("Report<>|:*?"), "Report");
        assert_eq!(sanitize_filename("a".repeat(100).as_str()).len(), 50);
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
    } else if lower.ends_with(".epub") || content_type.eq_ignore_ascii_case("application/epub+zip") {
        Some("epub")
    } else {
        None
    }
}
