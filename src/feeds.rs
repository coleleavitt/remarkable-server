//! RSS/Newsletter feed sync for reMarkable.
//!
//! Features:
//! - RSS/Atom feed subscription
//! - Newsletter email parsing (IMAP)
//! - Article extraction (readability)
//! - Convert to EPUB
//! - Auto-sync to device folder
//! - Scheduled fetch
//! - OPML import/export

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use reqwest::Url;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};
use uuid::Uuid;

use crate::error::{Result, ServerError};
use crate::storage::Storage;

// ============================================================================
// Types
// ============================================================================

/// Feed subscription types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FeedType {
    Rss,
    Atom,
    Newsletter,
}

impl std::fmt::Display for FeedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeedType::Rss => write!(f, "rss"),
            FeedType::Atom => write!(f, "atom"),
            FeedType::Newsletter => write!(f, "newsletter"),
        }
    }
}

/// Feed subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub name: String,
    pub url: String,
    #[serde(rename = "type")]
    pub feed_type: FeedType,
    pub folder: String,
    pub enabled: bool,
    #[serde(rename = "fetchIntervalMins")]
    pub fetch_interval_mins: u32,
    #[serde(rename = "lastFetch")]
    pub last_fetch: Option<DateTime<Utc>>,
    #[serde(rename = "lastError")]
    pub last_error: Option<String>,
    #[serde(rename = "articleCount")]
    pub article_count: u32,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updatedAt")]
    pub updated_at: DateTime<Utc>,
}

/// Create subscription request
#[derive(Debug, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub url: String,
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub feed_type: Option<FeedType>,
    pub folder: Option<String>,
    #[serde(rename = "fetchIntervalMins")]
    pub fetch_interval_mins: Option<u32>,
}

/// Article from a feed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Article {
    pub id: String,
    #[serde(rename = "subscriptionId")]
    pub subscription_id: String,
    pub title: String,
    pub url: String,
    pub author: Option<String>,
    pub summary: Option<String>,
    #[serde(rename = "contentHtml")]
    pub content_html: Option<String>,
    #[serde(rename = "contentText")]
    pub content_text: Option<String>,
    #[serde(rename = "publishedAt")]
    pub published_at: Option<DateTime<Utc>>,
    #[serde(rename = "fetchedAt")]
    pub fetched_at: DateTime<Utc>,
    pub read: bool,
    pub synced: bool,
    #[serde(rename = "epubPath")]
    pub epub_path: Option<String>,
    #[serde(rename = "wordCount")]
    pub word_count: Option<u32>,
    #[serde(rename = "readingTimeMins")]
    pub reading_time_mins: Option<u32>,
}

/// Article list query
#[derive(Debug, Default, Deserialize)]
pub struct ArticleQuery {
    #[serde(rename = "subscriptionId")]
    pub subscription_id: Option<String>,
    pub unread: Option<bool>,
    pub unsynced: Option<bool>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// Refresh request
#[derive(Debug, Default, Deserialize)]
pub struct RefreshRequest {
    #[serde(rename = "subscriptionIds")]
    pub subscription_ids: Option<Vec<String>>,
    #[serde(rename = "forceAll")]
    pub force_all: Option<bool>,
}

/// Refresh response
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub refreshed: u32,
    #[serde(rename = "newArticles")]
    pub new_articles: u32,
    pub errors: Vec<RefreshError>,
}

#[derive(Debug, Serialize)]
pub struct RefreshError {
    #[serde(rename = "subscriptionId")]
    pub subscription_id: String,
    pub error: String,
}

/// IMAP configuration for newsletter parsing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImapConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub mailbox: String,
    pub tls: bool,
    #[serde(rename = "deleteAfterSync")]
    pub delete_after_sync: bool,
}

/// OPML outline element
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpmlOutline {
    pub text: String,
    #[serde(rename = "xmlUrl")]
    pub xml_url: Option<String>,
    #[serde(rename = "htmlUrl")]
    pub html_url: Option<String>,
    #[serde(rename = "type")]
    pub feed_type: Option<String>,
    pub children: Vec<OpmlOutline>,
}

/// OPML document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpmlDocument {
    pub title: String,
    #[serde(rename = "dateCreated")]
    pub date_created: Option<DateTime<Utc>>,
    pub outlines: Vec<OpmlOutline>,
}

/// Feed manager statistics
#[derive(Debug, Clone, Serialize)]
pub struct FeedStats {
    #[serde(rename = "subscriptionCount")]
    pub subscription_count: u32,
    #[serde(rename = "articleCount")]
    pub article_count: u32,
    #[serde(rename = "unreadCount")]
    pub unread_count: u32,
    #[serde(rename = "unsyncedCount")]
    pub unsynced_count: u32,
    #[serde(rename = "totalEpubBytes")]
    pub total_epub_bytes: u64,
}

// ============================================================================
// Feed Manager
// ============================================================================

/// Manages feed subscriptions and articles
pub struct FeedManager {
    db: Arc<Mutex<Connection>>,
    storage: Storage,
    epub_dir: PathBuf,
    http_client: reqwest::Client,
    /// Article extraction requests allowed to fetch or hold a page at once.
    fetch_slots: Arc<Semaphore>,
    /// Article extractions allowed to run at once (see `run_blocking_in_slot`).
    extract_slots: Arc<Semaphore>,
}

/// Commands for the background refresh task. Dropping every sender stops the task,
/// so keep one alive (see `FeedState::scheduler`).
pub enum SchedulerCommand {
    Refresh(Vec<String>),
    Stop,
}

impl FeedManager {
    /// Create a new feed manager
    pub fn new(db_path: &Path, storage: Storage, epub_dir: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Self::init_db(&conn)?;

        std::fs::create_dir_all(epub_dir).map_err(|e| ServerError::Storage(e))?;

        let http_client = reqwest::Client::builder()
            .user_agent("remarkable-server/0.1 (RSS Reader)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| ServerError::Internal(e.to_string()))?;

        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            storage,
            epub_dir: epub_dir.to_path_buf(),
            http_client,
            fetch_slots: Arc::new(Semaphore::new(FETCHES_AT_ONCE)),
            extract_slots: Arc::new(Semaphore::new(EXTRACTIONS_AT_ONCE)),
        })
    }

    fn init_db(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS subscriptions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                feed_type TEXT NOT NULL,
                folder TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                fetch_interval_mins INTEGER NOT NULL DEFAULT 60,
                last_fetch TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS articles (
                id TEXT PRIMARY KEY,
                subscription_id TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                author TEXT,
                summary TEXT,
                content_html TEXT,
                content_text TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL,
                read INTEGER NOT NULL DEFAULT 0,
                synced INTEGER NOT NULL DEFAULT 0,
                epub_path TEXT,
                word_count INTEGER,
                FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_articles_subscription ON articles(subscription_id);
            CREATE INDEX IF NOT EXISTS idx_articles_read ON articles(read);
            CREATE INDEX IF NOT EXISTS idx_articles_synced ON articles(synced);
            CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
            
            CREATE TABLE IF NOT EXISTS imap_configs (
                id TEXT PRIMARY KEY,
                subscription_id TEXT NOT NULL UNIQUE,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                mailbox TEXT NOT NULL DEFAULT 'INBOX',
                tls INTEGER NOT NULL DEFAULT 1,
                delete_after_sync INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
            );
        "#,
        )?;
        Ok(())
    }

    // ========================================================================
    // Subscriptions CRUD
    // ========================================================================

    /// List all subscriptions
    pub fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
        let db = self.db.lock();
        let mut stmt = db.prepare(
            r#"
            SELECT s.id, s.name, s.url, s.feed_type, s.folder, s.enabled,
                   s.fetch_interval_mins, s.last_fetch, s.last_error,
                   s.created_at, s.updated_at,
                   (SELECT COUNT(*) FROM articles WHERE subscription_id = s.id) as article_count
            FROM subscriptions s
            ORDER BY s.name
        "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(Subscription {
                id: row.get(0)?,
                name: row.get(1)?,
                url: row.get(2)?,
                feed_type: parse_feed_type(&row.get::<_, String>(3)?),
                folder: row.get(4)?,
                enabled: row.get::<_, i32>(5)? != 0,
                fetch_interval_mins: row.get(6)?,
                last_fetch: row.get::<_, Option<String>>(7)?.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|d| d.with_timezone(&Utc))
                }),
                last_error: row.get(8)?,
                created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(9)?)
                    .unwrap()
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(10)?)
                    .unwrap()
                    .with_timezone(&Utc),
                article_count: row.get(11)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Get a subscription by ID
    pub fn get_subscription(&self, id: &str) -> Result<Subscription> {
        let db = self.db.lock();
        let mut stmt = db.prepare(
            r#"
            SELECT s.id, s.name, s.url, s.feed_type, s.folder, s.enabled,
                   s.fetch_interval_mins, s.last_fetch, s.last_error,
                   s.created_at, s.updated_at,
                   (SELECT COUNT(*) FROM articles WHERE subscription_id = s.id) as article_count
            FROM subscriptions s
            WHERE s.id = ?
        "#,
        )?;

        stmt.query_row([id], |row| {
            Ok(Subscription {
                id: row.get(0)?,
                name: row.get(1)?,
                url: row.get(2)?,
                feed_type: parse_feed_type(&row.get::<_, String>(3)?),
                folder: row.get(4)?,
                enabled: row.get::<_, i32>(5)? != 0,
                fetch_interval_mins: row.get(6)?,
                last_fetch: row.get::<_, Option<String>>(7)?.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|d| d.with_timezone(&Utc))
                }),
                last_error: row.get(8)?,
                created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(9)?)
                    .unwrap()
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(10)?)
                    .unwrap()
                    .with_timezone(&Utc),
                article_count: row.get(11)?,
            })
        })
        .map_err(|_| ServerError::NotFound(format!("Subscription {}", id)))
    }

    /// Create a new subscription
    pub async fn create_subscription(
        &self,
        req: CreateSubscriptionRequest,
    ) -> Result<Subscription> {
        // Auto-detect feed type and title if not provided
        let (feed_type, detected_title) = self.detect_feed(&req.url).await?;
        let feed_type = req.feed_type.unwrap_or(feed_type);
        let name = req.name.unwrap_or(detected_title);

        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let folder = req.folder.unwrap_or_default();
        let fetch_interval = req.fetch_interval_mins.unwrap_or(60);

        {
            let db = self.db.lock();
            db.execute(
                r#"INSERT INTO subscriptions (id, name, url, feed_type, folder, enabled, fetch_interval_mins, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)"#,
                params![id, name, req.url, feed_type.to_string(), folder, fetch_interval, now.to_rfc3339(), now.to_rfc3339()]
            )?;
        }

        self.get_subscription(&id)
    }

    /// Delete a subscription
    pub fn delete_subscription(&self, id: &str) -> Result<()> {
        let db = self.db.lock();
        let affected = db.execute("DELETE FROM subscriptions WHERE id = ?", [id])?;
        if affected == 0 {
            return Err(ServerError::NotFound(format!("Subscription {}", id)));
        }
        Ok(())
    }

    /// Update subscription settings
    pub fn update_subscription(
        &self,
        id: &str,
        updates: serde_json::Value,
    ) -> Result<Subscription> {
        let current = self.get_subscription(id)?;
        let now = Utc::now();

        let name = updates
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.name);
        let folder = updates
            .get("folder")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.folder);
        let enabled = updates
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(current.enabled);
        let interval = updates
            .get("fetchIntervalMins")
            .and_then(|v| v.as_u64())
            .unwrap_or(current.fetch_interval_mins as u64) as u32;

        {
            let db = self.db.lock();
            db.execute(
                "UPDATE subscriptions SET name = ?, folder = ?, enabled = ?, fetch_interval_mins = ?, updated_at = ? WHERE id = ?",
                params![name, folder, enabled as i32, interval, now.to_rfc3339(), id]
            )?;
        }

        self.get_subscription(id)
    }

    // ========================================================================
    // Feed Detection & Parsing
    // ========================================================================

    /// Detect feed type from URL
    async fn detect_feed(&self, url: &str) -> Result<(FeedType, String)> {
        let response = self
            .http_client
            .get(url)
            .send()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to fetch feed: {}", e)))?;

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();

        let body = response
            .text()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to read feed: {}", e)))?;

        // Try parsing as RSS/Atom
        match feed_rs::parser::parse(body.as_bytes()) {
            Ok(feed) => {
                let feed_type = if content_type.contains("atom") || body.contains("<feed") {
                    FeedType::Atom
                } else {
                    FeedType::Rss
                };
                let title = feed
                    .title
                    .map(|t| t.content)
                    .unwrap_or_else(|| "Untitled Feed".to_string());
                Ok((feed_type, title))
            }
            Err(_) => {
                // Try to find feed links in HTML
                if let Some(feed_url) = self.find_feed_link(&body) {
                    return Box::pin(self.detect_feed(&feed_url)).await;
                }
                Err(ServerError::Internal("Could not detect feed type".into()))
            }
        }
    }

    /// Find feed link in HTML page
    fn find_feed_link(&self, html: &str) -> Option<String> {
        // Simple regex-free extraction
        for line in html.lines() {
            if line.contains("application/rss+xml") || line.contains("application/atom+xml") {
                if let Some(href_start) = line.find("href=\"") {
                    let rest = &line[href_start + 6..];
                    if let Some(href_end) = rest.find('"') {
                        return Some(rest[..href_end].to_string());
                    }
                }
            }
        }
        None
    }

    /// Fetch and parse a feed
    async fn fetch_feed(&self, subscription: &Subscription) -> Result<Vec<Article>> {
        let response = self
            .http_client
            .get(&subscription.url)
            .send()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to fetch feed: {}", e)))?;

        let body = response
            .bytes()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to read feed: {}", e)))?;

        let feed = feed_rs::parser::parse(&body[..])
            .map_err(|e| ServerError::Internal(format!("Failed to parse feed: {}", e)))?;

        let now = Utc::now();
        let mut articles = Vec::new();

        for entry in feed.entries {
            let id = Uuid::new_v4().to_string();
            let url = entry
                .links
                .first()
                .map(|l| l.href.clone())
                .unwrap_or_default();

            // Skip if we already have this article
            if self.article_exists_by_url(&subscription.id, &url)? {
                continue;
            }

            let content_html = entry
                .content
                .and_then(|c| c.body)
                .or_else(|| entry.summary.as_ref().map(|s| s.content.clone()));

            let content_text = content_html.as_ref().map(|h| strip_html(h));
            let word_count = content_text
                .as_ref()
                .map(|t| t.split_whitespace().count() as u32);
            let reading_time = word_count.map(|w| (w / 200).max(1));

            articles.push(Article {
                id,
                subscription_id: subscription.id.clone(),
                title: entry
                    .title
                    .map(|t| t.content)
                    .unwrap_or_else(|| "Untitled".to_string()),
                url,
                author: entry.authors.first().map(|a| a.name.clone()),
                summary: entry.summary.map(|s| s.content),
                content_html,
                content_text,
                published_at: entry.published.or(entry.updated),
                fetched_at: now,
                read: false,
                synced: false,
                epub_path: None,
                word_count,
                reading_time_mins: reading_time,
            });
        }

        Ok(articles)
    }

    fn article_exists_by_url(&self, subscription_id: &str, url: &str) -> Result<bool> {
        let db = self.db.lock();
        let count: i32 = db.query_row(
            "SELECT COUNT(*) FROM articles WHERE subscription_id = ? AND url = ?",
            params![subscription_id, url],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    // ========================================================================
    // Article Extraction (Readability)
    // ========================================================================

    /// Extract article content using readability
    pub async fn extract_article(&self, url: &str) -> Result<ExtractedArticle> {
        // Bounds the pages held in memory, each up to `ARTICLE_LIMITS.bytes`, while requests
        // wait for the extraction slot.
        let _fetch_slot = self
            .fetch_slots
            .acquire()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to fetch article: {e}")))?;
        let response = self
            .http_client
            .get(url)
            .send()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to fetch article: {}", e)))?;
        // Relative links in the page are relative to where it was served from, after redirects.
        let page_url = response.url().clone();
        let html = read_page(response, ARTICLE_LIMITS.bytes).await?;

        // Parsing and scoring are CPU-bound, so keep them off the async workers that serve the
        // tablet, and run one extraction at a time.
        run_blocking_in_slot(&self.extract_slots, move || {
            extract_readable(&html, &page_url)
        })
        .await?
    }

    // ========================================================================
    // EPUB Generation
    // ========================================================================

    /// Convert an article to EPUB
    pub fn article_to_epub(&self, article: &Article) -> Result<PathBuf> {
        let safe_title = sanitize_filename(&article.title);
        let epub_path = self
            .epub_dir
            .join(format!("{}_{}.epub", safe_title, &article.id[..8]));

        let mut builder =
            epub_builder::EpubBuilder::new(epub_builder::ZipLibrary::new().unwrap()).unwrap();

        builder.metadata("title", &article.title).unwrap();
        if let Some(ref author) = article.author {
            builder.metadata("author", author).unwrap();
        }
        builder.metadata("generator", "remarkable-server").unwrap();

        // Build XHTML content
        let content = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>{}</title>
    <style>
        body {{ font-family: serif; line-height: 1.6; margin: 2em; }}
        h1 {{ font-size: 1.5em; margin-bottom: 0.5em; }}
        .meta {{ color: #666; font-size: 0.9em; margin-bottom: 2em; }}
        p {{ margin: 1em 0; }}
    </style>
</head>
<body>
    <h1>{}</h1>
    <div class="meta">
        {}{}
    </div>
    <article>{}</article>
</body>
</html>"#,
            html_escape(&article.title),
            html_escape(&article.title),
            article
                .author
                .as_ref()
                .map(|a| format!("By {} • ", html_escape(a)))
                .unwrap_or_default(),
            article
                .published_at
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
            article
                .content_html
                .as_deref()
                .unwrap_or("<p>No content available.</p>")
        );

        builder
            .add_content(
                epub_builder::EpubContent::new("content.xhtml", content.as_bytes())
                    .title(&article.title),
            )
            .unwrap();

        let mut file = std::fs::File::create(&epub_path)?;
        builder
            .generate(&mut file)
            .map_err(|e| ServerError::Internal(format!("EPUB generation failed: {:?}", e)))?;

        Ok(epub_path)
    }

    /// Sync unsynced articles (of `subscription_id`, or all when `None`) as EPUBs
    /// into the device folder `folder` (see [`crate::documents::ensure_folder`]).
    pub fn sync_articles_to_folder(
        &self,
        subscription_id: Option<&str>,
        folder: &str,
    ) -> Result<u32> {
        let articles = self.list_articles(ArticleQuery {
            subscription_id: subscription_id.map(String::from),
            unsynced: Some(true),
            ..Default::default()
        })?;
        if articles.is_empty() {
            return Ok(0);
        }
        let parent = crate::documents::ensure_folder(&self.storage, folder)?;

        let mut synced = 0;
        for article in articles {
            // Generate EPUB if not exists
            let epub_path = if let Some(ref path) = article.epub_path {
                PathBuf::from(path)
            } else {
                let path = self.article_to_epub(&article)?;
                self.update_article_epub(&article.id, &path)?;
                path
            };

            // Add to the sync tree as a real document so the device pulls it.
            let epub_data = std::fs::read(&epub_path)?;
            let (doc_id, _) = crate::documents::create_document_in(
                &self.storage,
                &article.title,
                "epub",
                &epub_data,
                &parent,
            )?;
            tracing::info!(
                "Feed article {:?} ({}) synced as document {}",
                article.title,
                folder,
                doc_id
            );

            // Mark as synced
            self.mark_article_synced(&article.id)?;
            synced += 1;
        }

        Ok(synced)
    }

    fn update_article_epub(&self, article_id: &str, path: &Path) -> Result<()> {
        let db = self.db.lock();
        db.execute(
            "UPDATE articles SET epub_path = ? WHERE id = ?",
            params![path.to_string_lossy(), article_id],
        )?;
        Ok(())
    }

    fn mark_article_synced(&self, article_id: &str) -> Result<()> {
        let db = self.db.lock();
        db.execute("UPDATE articles SET synced = 1 WHERE id = ?", [article_id])?;
        Ok(())
    }

    // ========================================================================
    // Articles CRUD
    // ========================================================================

    /// List articles with optional filtering
    pub fn list_articles(&self, query: ArticleQuery) -> Result<Vec<Article>> {
        let db = self.db.lock();

        let mut sql = String::from(
            r#"
            SELECT id, subscription_id, title, url, author, summary, content_html, content_text,
                   published_at, fetched_at, read, synced, epub_path, word_count
            FROM articles WHERE 1=1
        "#,
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(ref sub_id) = query.subscription_id {
            sql.push_str(" AND subscription_id = ?");
            params_vec.push(Box::new(sub_id.clone()));
        }
        if let Some(unread) = query.unread {
            sql.push_str(if unread {
                " AND read = 0"
            } else {
                " AND read = 1"
            });
        }
        if let Some(unsynced) = query.unsynced {
            sql.push_str(if unsynced {
                " AND synced = 0"
            } else {
                " AND synced = 1"
            });
        }

        sql.push_str(" ORDER BY published_at DESC, fetched_at DESC");

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let mut stmt = db.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let word_count: Option<u32> = row.get(13)?;
            Ok(Article {
                id: row.get(0)?,
                subscription_id: row.get(1)?,
                title: row.get(2)?,
                url: row.get(3)?,
                author: row.get(4)?,
                summary: row.get(5)?,
                content_html: row.get(6)?,
                content_text: row.get(7)?,
                published_at: row.get::<_, Option<String>>(8)?.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|d| d.with_timezone(&Utc))
                }),
                fetched_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(9)?)
                    .unwrap()
                    .with_timezone(&Utc),
                read: row.get::<_, i32>(10)? != 0,
                synced: row.get::<_, i32>(11)? != 0,
                epub_path: row.get(12)?,
                word_count,
                reading_time_mins: word_count.map(|w| (w / 200).max(1)),
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Get a single article
    pub fn get_article(&self, id: &str) -> Result<Article> {
        let db = self.db.lock();
        let mut stmt = db.prepare(
            r#"
            SELECT id, subscription_id, title, url, author, summary, content_html, content_text,
                   published_at, fetched_at, read, synced, epub_path, word_count
            FROM articles WHERE id = ?
        "#,
        )?;

        stmt.query_row([id], |row| {
            let word_count: Option<u32> = row.get(13)?;
            Ok(Article {
                id: row.get(0)?,
                subscription_id: row.get(1)?,
                title: row.get(2)?,
                url: row.get(3)?,
                author: row.get(4)?,
                summary: row.get(5)?,
                content_html: row.get(6)?,
                content_text: row.get(7)?,
                published_at: row.get::<_, Option<String>>(8)?.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|d| d.with_timezone(&Utc))
                }),
                fetched_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(9)?)
                    .unwrap()
                    .with_timezone(&Utc),
                read: row.get::<_, i32>(10)? != 0,
                synced: row.get::<_, i32>(11)? != 0,
                epub_path: row.get(12)?,
                word_count,
                reading_time_mins: word_count.map(|w| (w / 200).max(1)),
            })
        })
        .map_err(|_| ServerError::NotFound(format!("Article {}", id)))
    }

    /// Mark article as read
    pub fn mark_read(&self, id: &str, read: bool) -> Result<()> {
        let db = self.db.lock();
        db.execute(
            "UPDATE articles SET read = ? WHERE id = ?",
            params![read as i32, id],
        )?;
        Ok(())
    }

    /// Save articles to database
    fn save_articles(&self, articles: &[Article]) -> Result<u32> {
        let db = self.db.lock();
        let mut saved = 0;

        for article in articles {
            let result = db.execute(
                r#"INSERT OR IGNORE INTO articles 
                   (id, subscription_id, title, url, author, summary, content_html, content_text,
                    published_at, fetched_at, read, synced, epub_path, word_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
                params![
                    article.id,
                    article.subscription_id,
                    article.title,
                    article.url,
                    article.author,
                    article.summary,
                    article.content_html,
                    article.content_text,
                    article.published_at.map(|d| d.to_rfc3339()),
                    article.fetched_at.to_rfc3339(),
                    article.read as i32,
                    article.synced as i32,
                    article.epub_path,
                    article.word_count
                ],
            )?;
            if result > 0 {
                saved += 1;
            }
        }

        Ok(saved)
    }

    // ========================================================================
    // Refresh & Scheduling
    // ========================================================================

    /// Refresh subscriptions
    pub async fn refresh(&self, req: RefreshRequest) -> Result<RefreshResponse> {
        let subscriptions = if let Some(ref ids) = req.subscription_ids {
            ids.iter()
                .filter_map(|id| self.get_subscription(id).ok())
                .collect()
        } else {
            self.list_subscriptions()?
                .into_iter()
                .filter(|s| s.enabled)
                .collect::<Vec<_>>()
        };

        let mut response = RefreshResponse {
            refreshed: 0,
            new_articles: 0,
            errors: Vec::new(),
        };

        for sub in subscriptions {
            // Skip if not due yet (unless forced)
            if !req.force_all.unwrap_or(false) {
                if let Some(last) = sub.last_fetch {
                    let due = last + chrono::Duration::minutes(sub.fetch_interval_mins as i64);
                    if Utc::now() < due {
                        continue;
                    }
                }
            }

            match self.fetch_feed(&sub).await {
                Ok(articles) => {
                    let new_count = self.save_articles(&articles)?;
                    response.new_articles += new_count;
                    response.refreshed += 1;
                    self.update_last_fetch(&sub.id, None)?;
                }
                Err(e) => {
                    self.update_last_fetch(&sub.id, Some(&e.to_string()))?;
                    response.errors.push(RefreshError {
                        subscription_id: sub.id,
                        error: e.to_string(),
                    });
                }
            }
        }

        Ok(response)
    }

    fn update_last_fetch(&self, subscription_id: &str, error: Option<&str>) -> Result<()> {
        let db = self.db.lock();
        let now = Utc::now().to_rfc3339();
        db.execute(
            "UPDATE subscriptions SET last_fetch = ?, last_error = ?, updated_at = ? WHERE id = ?",
            params![now, error, now, subscription_id],
        )?;
        Ok(())
    }

    /// Start background scheduler
    pub fn start_scheduler(self: Arc<Self>, interval_secs: u64) -> mpsc::Sender<SchedulerCommand> {
        let (tx, mut rx) = mpsc::channel::<SchedulerCommand>(32);
        let manager = self.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = manager.refresh(RefreshRequest::default()).await {
                            tracing::error!("Scheduled refresh failed: {}", e);
                        }
                    }
                    cmd = rx.recv() => {
                        match cmd {
                            Some(SchedulerCommand::Refresh(ids)) => {
                                let req = RefreshRequest { subscription_ids: Some(ids), force_all: Some(true) };
                                if let Err(e) = manager.refresh(req).await {
                                    tracing::error!("Manual refresh failed: {}", e);
                                }
                            }
                            Some(SchedulerCommand::Stop) | None => break,
                        }
                    }
                }
            }
        });

        tx
    }

    // ========================================================================
    // OPML Import/Export
    // ========================================================================

    /// Import subscriptions from OPML
    pub async fn import_opml(&self, opml_content: &str) -> Result<Vec<Subscription>> {
        let doc = parse_opml(opml_content)?;
        let mut imported = Vec::new();

        for outline in flatten_outlines(&doc.outlines) {
            if let Some(url) = outline.xml_url {
                let req = CreateSubscriptionRequest {
                    url,
                    name: Some(outline.text.clone()),
                    feed_type: outline.feed_type.as_deref().and_then(|t| match t {
                        "rss" => Some(FeedType::Rss),
                        "atom" => Some(FeedType::Atom),
                        _ => None,
                    }),
                    folder: None,
                    fetch_interval_mins: None,
                };
                match self.create_subscription(req).await {
                    Ok(sub) => imported.push(sub),
                    Err(e) => tracing::warn!("Failed to import {}: {}", outline.text, e),
                }
            }
        }

        Ok(imported)
    }

    /// Export subscriptions to OPML
    pub fn export_opml(&self) -> Result<String> {
        let subscriptions = self.list_subscriptions()?;
        let now = Utc::now();

        let mut opml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<opml version="2.0">
<head>
    <title>remarkable-server feeds</title>
    <dateCreated>"#,
        );
        opml.push_str(&now.to_rfc2822());
        opml.push_str(
            r#"</dateCreated>
</head>
<body>
"#,
        );

        // Group by folder
        let mut by_folder: HashMap<String, Vec<&Subscription>> = HashMap::new();
        for sub in &subscriptions {
            by_folder.entry(sub.folder.clone()).or_default().push(sub);
        }

        for (folder, subs) in by_folder {
            if folder.is_empty() {
                for sub in subs {
                    opml.push_str(&format!(
                        r#"    <outline text="{}" type="{}" xmlUrl="{}" />
"#,
                        html_escape(&sub.name),
                        sub.feed_type,
                        html_escape(&sub.url)
                    ));
                }
            } else {
                opml.push_str(&format!(
                    r#"    <outline text="{}">
"#,
                    html_escape(&folder)
                ));
                for sub in subs {
                    opml.push_str(&format!(
                        r#"        <outline text="{}" type="{}" xmlUrl="{}" />
"#,
                        html_escape(&sub.name),
                        sub.feed_type,
                        html_escape(&sub.url)
                    ));
                }
                opml.push_str(
                    "    </outline>
",
                );
            }
        }

        opml.push_str(
            "</body>
</opml>",
        );
        Ok(opml)
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get feed statistics
    pub fn stats(&self) -> Result<FeedStats> {
        let db = self.db.lock();

        let subscription_count: u32 =
            db.query_row("SELECT COUNT(*) FROM subscriptions", [], |r| r.get(0))?;
        let article_count: u32 = db.query_row("SELECT COUNT(*) FROM articles", [], |r| r.get(0))?;
        let unread_count: u32 =
            db.query_row("SELECT COUNT(*) FROM articles WHERE read = 0", [], |r| {
                r.get(0)
            })?;
        let unsynced_count: u32 =
            db.query_row("SELECT COUNT(*) FROM articles WHERE synced = 0", [], |r| {
                r.get(0)
            })?;

        // Calculate total EPUB size
        let total_epub_bytes = std::fs::read_dir(&self.epub_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0);

        Ok(FeedStats {
            subscription_count,
            article_count,
            unread_count,
            unsynced_count,
            total_epub_bytes,
        })
    }
}

/// Extracted article content
#[derive(Debug, Clone, Serialize)]
pub struct ExtractedArticle {
    pub title: String,
    #[serde(rename = "contentHtml")]
    pub content_html: String,
    #[serde(rename = "contentText")]
    pub content_text: String,
    #[serde(rename = "wordCount")]
    pub word_count: u32,
    #[serde(rename = "readingTimeMins")]
    pub reading_time_mins: u32,
}

// ============================================================================
// IMAP Newsletter Support
// ============================================================================

impl FeedManager {
    /// Configure IMAP for a newsletter subscription
    pub fn set_imap_config(&self, subscription_id: &str, config: ImapConfig) -> Result<()> {
        let db = self.db.lock();
        db.execute(
            r#"INSERT OR REPLACE INTO imap_configs
               (id, subscription_id, host, port, username, password, mailbox, tls, delete_after_sync)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            params![
                Uuid::new_v4().to_string(),
                subscription_id,
                config.host,
                config.port,
                config.username,
                config.password,
                config.mailbox,
                config.tls as i32,
                config.delete_after_sync as i32
            ]
        )?;
        Ok(())
    }

    /// Fetch newsletters from IMAP
    pub async fn fetch_newsletters(&self, subscription_id: &str) -> Result<Vec<Article>> {
        let config = self.get_imap_config(subscription_id)?;
        let mut articles = Vec::new();

        // Connect to IMAP: implicit TLS when `tls` is set, otherwise STARTTLS (never plaintext).
        let mode = if config.tls {
            imap::ConnectionMode::Tls
        } else {
            imap::ConnectionMode::StartTls
        };
        let client = imap::ClientBuilder::new(config.host.as_str(), config.port)
            .mode(mode)
            .connect()
            .map_err(|e| ServerError::Internal(format!("IMAP connect error: {}", e)))?;

        let mut session = client
            .login(&config.username, &config.password)
            .map_err(|(e, _)| ServerError::Internal(format!("IMAP login error: {}", e)))?;

        session
            .select(&config.mailbox)
            .map_err(|e| ServerError::Internal(format!("IMAP select error: {}", e)))?;

        // Fetch unread messages
        let uids = session
            .uid_search("UNSEEN")
            .map_err(|e| ServerError::Internal(format!("IMAP search error: {}", e)))?;

        let now = Utc::now();

        for uid in uids {
            let messages = session
                .uid_fetch(uid.to_string(), "(RFC822 ENVELOPE)")
                .map_err(|e| ServerError::Internal(format!("IMAP fetch error: {}", e)))?;

            for message in messages.iter() {
                if let Some(body) = message.body() {
                    let parsed = mailparse::parse_mail(body)
                        .map_err(|e| ServerError::Internal(format!("Mail parse error: {}", e)))?;

                    let subject = parsed
                        .headers
                        .iter()
                        .find(|h| h.get_key_ref() == "Subject")
                        .map(|h| h.get_value())
                        .unwrap_or_else(|| "No Subject".to_string());

                    let from = parsed
                        .headers
                        .iter()
                        .find(|h| h.get_key_ref() == "From")
                        .map(|h| h.get_value());

                    let content_html = extract_html_body(&parsed);
                    let content_text = content_html.as_ref().map(|h| strip_html(h));
                    let word_count = content_text
                        .as_ref()
                        .map(|t| t.split_whitespace().count() as u32);

                    articles.push(Article {
                        id: Uuid::new_v4().to_string(),
                        subscription_id: subscription_id.to_string(),
                        title: subject,
                        url: format!(
                            "imap://{}:{}/{}/{}",
                            config.host, config.port, config.mailbox, uid
                        ),
                        author: from,
                        summary: None,
                        content_html,
                        content_text,
                        published_at: Some(now),
                        fetched_at: now,
                        read: false,
                        synced: false,
                        epub_path: None,
                        word_count,
                        reading_time_mins: word_count.map(|w| (w / 200).max(1)),
                    });
                }

                // Mark as read and optionally delete
                if config.delete_after_sync {
                    session
                        .uid_store(uid.to_string(), "+FLAGS (\\Deleted)")
                        .ok();
                } else {
                    session.uid_store(uid.to_string(), "+FLAGS (\\Seen)").ok();
                }
            }
        }

        if config.delete_after_sync {
            session.expunge().ok();
        }

        session.logout().ok();
        Ok(articles)
    }

    fn get_imap_config(&self, subscription_id: &str) -> Result<ImapConfig> {
        let db = self.db.lock();
        db.query_row(
            "SELECT host, port, username, password, mailbox, tls, delete_after_sync FROM imap_configs WHERE subscription_id = ?",
            [subscription_id],
            |row| Ok(ImapConfig {
                host: row.get(0)?,
                port: row.get(1)?,
                username: row.get(2)?,
                password: row.get(3)?,
                mailbox: row.get(4)?,
                tls: row.get::<_, i32>(5)? != 0,
                delete_after_sync: row.get::<_, i32>(6)? != 0,
            })
        ).map_err(|_| ServerError::NotFound(format!("IMAP config for subscription {}", subscription_id)))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn parse_feed_type(s: &str) -> FeedType {
    match s {
        "atom" => FeedType::Atom,
        "newsletter" => FeedType::Newsletter,
        _ => FeedType::Rss,
    }
}

fn strip_html(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    let in_script = false;

    for c in html.chars() {
        if c == '<' {
            in_tag = true;
            continue;
        }
        if c == '>' {
            in_tag = false;
            continue;
        }
        if in_tag {
            continue;
        }
        if !in_script {
            result.push(c);
        }
    }

    // Decode common entities
    result
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

// Limits for article extraction. `POST /feeds/v1/extract` fetches a page the caller names and
// runs a Readability.js port (dom_smoothie) over it, and parts of that work grow much faster
// than the page does:
// - html5ever's tree builder walks its stack of open elements for most tags, so parsing is
//   quadratic in nesting depth (servo/html5ever#788): 160 KB of `<ul><li>` took 4 s to parse,
//   400 KB of `<div>`s 23 s. Its tokenizer compares each attribute of a tag with the ones
//   before it: one tag with 80,000 attributes (790 KB) took 11 s. And dom_query compares each
//   attribute a repeated `<body>` tag adds with the body's: 100,000 of them (1.5 MB) took 12 s.
// - dom_smoothie measures the text under an element again for each element above it, on each of
//   its passes: 100 tables each holding a chain of 503 nested `<div>`s (250 KB) took 33 s, 400
//   of them over two minutes, and 50,000 elements in chains 128 deep took 4 s. The 512-level
//   depth cap alone allowed all of these.
// - dom_smoothie compares its whole title with each `<h1>` and `<h2>` until one resembles it, on
//   each of its passes, and the title can be as long as the page: a 1 MB og:title before 200
//   short headings took 3 s, and a page within every other limit can hold a title of megabytes
//   and a hundred thousand headings, hours of work.
// So a page is read up to a size limit, parsed in chunks with the parser's work counted
// (`parse_page`), and measured in one pass before dom_smoothie runs (`page_shape`), a measure
// checked again once the title dom_smoothie will use is known. A page over any limit is
// answered by `plain_text_article` instead: its title and text, which cost time linear in the
// page's size whatever its shape. Timings in this section are release builds on one core of a
// desktop CPU (Core Ultra 9 275HX). The real pages measured to set the limits were 45 news,
// blog, forum, documentation and Wikipedia pages, Project Gutenberg books and a 6.5 MB
// LessWrong post.

/// Limits on one extraction. The server uses `ARTICLE_LIMITS`; tests use smaller ones.
#[derive(Clone, Copy, Debug)]
struct ExtractLimits {
    /// Bytes of the page that are parsed. The page is read up to one byte more, to tell that it
    /// is longer.
    bytes: usize,
    /// Elements and comments the parser may create. Text nodes are not counted: adjacent text is
    /// merged, so there are at most about twice as many of them.
    nodes: usize,
    /// How deep elements may nest.
    depth: usize,
    /// Parser work that grows faster than the page (see `MeteredSink`), in steps of about 3 ns.
    parse_steps: u64,
    /// Backstop for parser work that cannot be counted from outside html5ever, such as its
    /// tokenizer comparing the attributes of a tag it has not finished yet.
    parse_time: Duration,
    /// dom_smoothie's work, as estimated by `page_shape`.
    work: u64,
}

const ARTICLE_LIMITS: ExtractLimits = ExtractLimits {
    // The largest real pages measured were 6.5 MB (LessWrong, mostly inline JSON) and 5.6 MB
    // (CNN, mostly script). Sizes are as sent: this client does not ask for compression.
    bytes: 16 << 20,
    // dom_smoothie spends up to about 4 µs per node, which `work` also counts; this bounds
    // memory and parse time before it. The most on a real page measured was 31,000.
    nodes: 250_000,
    // Chromium and WebKit stop nesting at 512 levels (deeper elements go to the nearest allowed
    // ancestor), so pages written for browsers stay within this; the deepest real page measured
    // nests 60. It also keeps dom_query's recursive selector matching far from the depth, around
    // 27,000 levels, at which it overflowed a 2 MiB thread stack and aborted the process.
    depth: 512,
    // About 0.1 s. The most on a real page measured was 570,000.
    parse_steps: 32_000_000,
    // Real pages measured parsed in 50 ms or less.
    parse_time: Duration::from_secs(1),
    // The pages built to cost the most per unit, chains of `<div>`s 128 deep around a letter of
    // text, took 1.3 to 1.6 s at this limit (10 to 13 ns per unit); the largest real pages
    // measured ran at 1 to 6 ns per unit. The most on a real page measured was 61 million
    // (Wikipedia's "Fourier transform", with its MathML), and the next 36 million.
    work: 128_000_000,
};

/// Input the parser takes between checks of the limits: a page over one costs at most one chunk
/// more parsing before the parse stops. Small, because a chunk can create far more nodes than it
/// has bytes: with hundreds of formatting elements open, html5ever reopens all of them before
/// each paragraph's text (40 KB made 2 million elements). Real pages parse as fast in 1 KiB
/// chunks as in 8 KiB ones.
const PARSE_CHUNK: usize = 1 << 10;

/// Extractions that run at once. Each is CPU-bound and cannot be cancelled once started, so
/// further requests wait for a slot rather than take more cores from the async workers that
/// serve the tablet.
const EXTRACTIONS_AT_ONCE: usize = 1;

/// Extraction requests that fetch or hold a page at once, each up to `ARTICLE_LIMITS.bytes`
/// (plus the one extraction a dropped request may leave running). Others wait before fetching.
const FETCHES_AT_ONCE: usize = 4;

/// The limit a page went over, so that it did not go to dom_smoothie.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OverLimit {
    Bytes,
    Nodes,
    Depth,
    ParseSteps,
    ParseTime,
    Work,
}

/// Reads the body of `response`, at most `limit + 1` bytes of it (enough to tell that the page
/// is over `limit`), and decodes it as `Response::text` does in this build (no `charset`
/// feature): as UTF-8, with invalid sequences replaced.
async fn read_page(mut response: reqwest::Response, limit: usize) -> Result<String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to read article: {e}")))?
    {
        let room = limit + 1 - body.len();
        body.extend_from_slice(&chunk[..chunk.len().min(room)]);
        if body.len() > limit {
            break;
        }
    }
    Ok(String::from_utf8(body)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
}

/// Runs `work` on the blocking pool once one of `slots` is free. The slot is given back when
/// `work` returns, not when the caller stops waiting for it: a request dropped part-way (the
/// client went away) cannot stop blocking work, so that work keeps its slot until it ends.
async fn run_blocking_in_slot<T: Send + 'static>(
    slots: &Arc<Semaphore>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T> {
    let slot = slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to extract article: {e}")))?;
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        work()
    })
    .await
    .map_err(|e| ServerError::Internal(format!("Failed to extract article: {e}")))
}

/// Readability (Mozilla Readability.js algorithm) over already-fetched HTML; no network. Keeps
/// the main content and drops page furniture (navigation, ads, sidebars, comments, scripts).
/// Relative URLs in the result are resolved the way a browser resolves them, against the page's
/// `<base href>` if that is an http(s) URL and against `page_url` otherwise. A page over
/// `ARTICLE_LIMITS` gets its title and plain text instead. CPU-bound: run it off the async
/// runtime.
fn extract_readable(html: &str, page_url: &Url) -> Result<ExtractedArticle> {
    let (article, over) = extract_page(html, page_url, &ARTICLE_LIMITS)?;
    if let Some(limit) = over {
        tracing::warn!(
            url = %page_url,
            ?limit,
            "Page is over the article extraction limits; answering with its plain text"
        );
    }
    Ok(article)
}

/// `extract_readable` under `limits`, also telling which limit the page went over, if any.
fn extract_page(
    html: &str,
    page_url: &Url,
    limits: &ExtractLimits,
) -> Result<(ExtractedArticle, Option<OverLimit>)> {
    let (doc, over) = parse_page(html, limits);
    let shape = match over {
        None => page_shape(&doc),
        Some(_) => return Ok((plain_text_article(&doc), over)),
    };
    // Checked before anything else reads the page: dom_query's selector matching recurses per
    // level, and finding the title below reads the text under each heading again.
    if let Some(limit) = shape.over(limits, 0) {
        return Ok((plain_text_article(&doc), Some(limit)));
    }

    let base = document_base(&doc, page_url);
    // dom_smoothie gets neither a URL nor a <base>, so it leaves every URL as written, and they
    // are resolved below with `Url::join`. Its own resolver leaves relative links that start
    // with "http" (`http2-explained/`) relative, applies `..` inside query strings, drops the
    // trailing slash of `..` and `.`, does not percent-encode, and takes any <base>, even a
    // `javascript:` one.
    doc.select("base").remove();

    let failed = |e: dom_smoothie::ReadabilityError| {
        ServerError::Internal(format!("Failed to extract article: {e}"))
    };
    let cfg = dom_smoothie::Config {
        // JSON-LD would only add metadata, and it is parsed with gjson, which can turn crafted
        // escapes in a hostile page into a String that is not UTF-8. Title and body come from
        // the DOM.
        disable_json_ld: true,
        ..Default::default()
    };
    let mut readability =
        dom_smoothie::Readability::with_document(doc, None, Some(cfg)).map_err(failed)?;
    // `parse` starts by finding the title this way too (og:title and other meta tags, else the
    // `<title>`, else the first `<h1>`), in time linear in the page, then compares it with the
    // headings.
    let title = readability.get_article_metadata(None).title;
    if let Some(limit) = shape.over(limits, title.len()) {
        return Ok((plain_text_article(&readability.doc), Some(limit)));
    }
    let (title, content_html) = match readability.parse() {
        Ok(article) => {
            // `parse` leaves the cleaned-up article in `readability.doc`, under the root it
            // serialized into `article.content`. Resolve the URLs there and serialize it again.
            let root = readability.doc.select_single("#readability-page-1");
            let content = if root.exists() {
                absolutize_urls(&root, &base, base == *page_url);
                root.html().to_string()
            } else {
                article.content.to_string()
            };
            (article.title, content)
        }
        // No readable text at all (empty page, empty body). The previous extractor still
        // answered with the page title and a content-free body, so keep answering, with the
        // title `parse` found.
        Err(dom_smoothie::ReadabilityError::GrabFailed) => (title, String::new()),
        Err(e) => return Err(failed(e)),
    };

    let text = strip_html(&content_html);
    let word_count = text.split_whitespace().count() as u32;

    Ok((
        ExtractedArticle {
            title,
            content_html,
            content_text: text,
            word_count,
            reading_time_mins: (word_count / 200).max(1),
        },
        None,
    ))
}

/// What `page_shape` finds.
#[derive(Debug)]
struct PageShape {
    /// How deep the deepest element sits; the document's children are at depth 1.
    deepest: usize,
    /// dom_smoothie's estimated work, in units of 1 to 13 ns (see `ARTICLE_LIMITS.work`), except
    /// for comparing the title with the headings, which `PageShape::over` adds.
    work: u64,
    /// `<h1>` and `<h2>` elements.
    headings: u64,
}

/// Work per byte of the title for each `<h1>` and `<h2>`. Until a heading resembles the title,
/// dom_smoothie lowercases the whole title for each one, searches it for the heading's text and
/// splits it into words, and it does this on each of its passes (up to four, when it finds
/// little text). Of the titles built to test this, distinct Chinese characters, each a word,
/// cost the most: up to 90 ns per title byte per heading over four passes, in a run where
/// chains of `<div>`s took 10 ns per unit, so under 6 ns per unit. Real titles are under 200
/// bytes; on the real pages measured, this adds at most 0.06% of `ARTICLE_LIMITS.work`.
const TITLE_WORK: u64 = 16;

impl PageShape {
    /// The limit the page is over, if any, when dom_smoothie's title is `title_bytes` long.
    fn over(&self, limits: &ExtractLimits, title_bytes: usize) -> Option<OverLimit> {
        let title_work = self
            .headings
            .saturating_mul(title_bytes as u64)
            .saturating_mul(TITLE_WORK);
        if self.deepest > limits.depth {
            Some(OverLimit::Depth)
        } else if self.work.saturating_add(title_work) > limits.work {
            Some(OverLimit::Work)
        } else {
            None
        }
    }
}

/// Measures `doc` in one pass, to estimate dom_smoothie's work before running it. For each node
/// at depth d, the work counts:
/// - d², because measuring an element's text visits every node under it, and dom_smoothie
///   measures an element again for each element above it, so it visits the node about d²/2
///   times on each pass. Chains of nested elements with little text in them ran at up to 13 ns
///   per unit.
/// - 300 for the node itself and 40 per attribute: 200,000 flat elements took 0.7 to 2 s, and
///   dom_smoothie looks attributes up one by one on each pass.
/// - for text outside `<script>` and `<style>` (which dom_smoothie removes first), 3 + d/3 per
///   byte: 16 MB of text took 0.7 s at depth 4 and 3.7 s at depth 500, and each `<h1>` or
///   `<h2>` above text compares that text with the title (4 MB under 250 of them took 8 s).
///
/// It also counts the `<h1>` and `<h2>` elements, for `PageShape::over` to add the work of
/// comparing each with the title.
///
/// Iterative, because it has to handle pages too deep for recursive tree walks. `<template>`
/// contents are a separate fragment, which neither this walk nor dom_smoothie visits
/// (`parse_page` counts them).
fn page_shape(doc: &dom_query::Document) -> PageShape {
    use dom_query::NodeData;

    let mut shape = PageShape {
        deepest: 0,
        work: 0,
        headings: 0,
    };
    // `depth` is the depth of `next`; the document's children are at depth 1.
    let (mut next, mut depth) = (doc.root().first_child(), 1usize);
    while let Some(node) = next {
        let d = depth as u64;
        let (attrs, text) = node.query_or((0, 0), |n| match &n.data {
            NodeData::Element(e) => (e.attrs.len() as u64, 0),
            NodeData::Text { contents } => (0, contents.len() as u64),
            _ => (0, 0),
        });
        let in_code = text > 0
            && node
                .parent()
                .is_some_and(|p| p.has_name("script") || p.has_name("style"));
        let text_work = if in_code { 0 } else { text * (3 + d / 3) };
        shape.work = shape
            .work
            .saturating_add(300 + d * d + 40 * attrs + text_work);
        if node.is_element() {
            shape.deepest = shape.deepest.max(depth);
            if node.has_name("h1") || node.has_name("h2") {
                shape.headings += 1;
            }
        }
        next = if let Some(child) = node.first_child() {
            depth += 1;
            Some(child)
        } else {
            // The next node in document order: the next sibling of `node`, or of its nearest
            // ancestor that has one.
            let mut cur = node;
            loop {
                if let Some(sibling) = cur.next_sibling() {
                    break Some(sibling);
                }
                match cur.parent() {
                    Some(parent) if depth > 1 => {
                        depth -= 1;
                        cur = parent;
                    }
                    _ => break None,
                }
            }
        };
    }
    shape
}

/// The answer for a page over the limits: its title and its text in paragraphs, found in time
/// linear in the size of `doc` whatever its shape (`doc` may be the part of the page parsed
/// before a limit stopped the parser).
fn plain_text_article(doc: &dom_query::Document) -> ExtractedArticle {
    let paragraphs = plain_paragraphs(doc);
    let content_html = paragraphs
        .iter()
        .map(|p| format!("<p>{}</p>", html_escape(p)))
        .collect::<Vec<_>>()
        .join("\n");
    let content_text = paragraphs.join("\n\n");
    let word_count = content_text.split_whitespace().count() as u32;
    ExtractedArticle {
        title: plain_title(doc),
        content_html,
        content_text,
        word_count,
        reading_time_mins: (word_count / 200).max(1),
    }
}

/// The page's og:title, or else its `<title>`, looking only at the children of `<head>`.
/// Readability's own choice also compares the title with every `<h1>` and `<h2>`, which on
/// these pages could take as long as the extraction it replaces.
fn plain_title(doc: &dom_query::Document) -> String {
    let collapse = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut title = String::new();
    let mut child = doc.tree.head().and_then(|head| head.first_child());
    while let Some(node) = child {
        if node.has_name("meta")
            && node
                .attr("property")
                .is_some_and(|p| p.trim().eq_ignore_ascii_case("og:title"))
        {
            let og = collapse(&node.attr("content").unwrap_or_default());
            if !og.is_empty() {
                return og;
            }
        } else if title.is_empty() && node.has_name("title") {
            title = collapse(&node.text());
        }
        child = node.next_sibling();
    }
    title
}

/// The text of the page's `<body>` in paragraphs, with whitespace collapsed, a break at each
/// block-level element and `<br>`, and without the contents of scripts, styles and other
/// elements that do not show text. Iterative, like `page_shape`.
fn plain_paragraphs(doc: &dom_query::Document) -> Vec<String> {
    const SKIPPED: &[&str] = &[
        "script", "style", "noscript", "template", "iframe", "object", "svg", "select", "textarea",
    ];
    const BLOCKS: &[&str] = &[
        "address",
        "article",
        "aside",
        "blockquote",
        "br",
        "caption",
        "dd",
        "details",
        "div",
        "dl",
        "dt",
        "fieldset",
        "figcaption",
        "figure",
        "footer",
        "form",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "hr",
        "li",
        "main",
        "nav",
        "ol",
        "p",
        "pre",
        "section",
        "summary",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    ];
    fn is_one_of(node: &dom_query::NodeRef, names: &[&str]) -> bool {
        node.qual_name_ref()
            .is_some_and(|name| names.contains(&name.local.as_ref()))
    }

    let mut paragraphs = Vec::new();
    let Some(body) = doc.tree.body() else {
        return paragraphs;
    };
    let mut current = String::new();
    let mut space = false;
    let mut end_paragraph = |current: &mut String, space: &mut bool| {
        if !current.is_empty() {
            paragraphs.push(std::mem::take(current));
        }
        *space = false;
    };
    // `depth` counts the levels below `body` of `next`.
    let (mut next, mut depth) = (body.first_child(), 1usize);
    while let Some(node) = next {
        let mut enter = false;
        if node.is_element() {
            if !is_one_of(&node, SKIPPED) {
                if is_one_of(&node, BLOCKS) {
                    end_paragraph(&mut current, &mut space);
                }
                enter = true;
            }
        } else if node.is_text() {
            for c in node.text().chars() {
                if c.is_whitespace() {
                    space = !current.is_empty();
                } else {
                    if space {
                        current.push(' ');
                        space = false;
                    }
                    current.push(c);
                }
            }
        }
        next = match node.first_child() {
            Some(child) if enter => {
                depth += 1;
                Some(child)
            }
            _ => {
                let mut cur = node;
                loop {
                    if let Some(sibling) = cur.next_sibling() {
                        break Some(sibling);
                    }
                    match cur.parent() {
                        Some(parent) if depth > 1 => {
                            depth -= 1;
                            if is_one_of(&parent, BLOCKS) {
                                end_paragraph(&mut current, &mut space);
                            }
                            cur = parent;
                        }
                        _ => break None,
                    }
                }
            }
        };
    }
    end_paragraph(&mut current, &mut space);
    paragraphs
}

/// Parsing under the limits above.
mod page_parser {
    use std::borrow::Cow;
    use std::cell::Cell;
    use std::time::Instant;

    use dom_query::{Document, NodeData, NodeId};
    use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, Tracer, TreeSink};
    use html5ever::tendril::{StrTendril, TendrilSink};
    use html5ever::{Attribute, QualName};

    use super::{ExtractLimits, OverLimit, PARSE_CHUNK};

    /// Parses `html` as `dom_query::Document::from` does, but feeds html5ever `PARSE_CHUNK`
    /// bytes at a time and checks the limits after each chunk. At the first limit reached it
    /// stops, and returns the part parsed so far and that limit. Nothing can interrupt html5ever
    /// inside a chunk, so a page over a limit costs at most one chunk more. Only the first
    /// `limits.bytes` of a longer page are parsed; it is over `OverLimit::Bytes` unless another
    /// limit stops the parse first.
    pub(super) fn parse_page(html: &str, limits: &ExtractLimits) -> (Document, Option<OverLimit>) {
        let deadline = Instant::now() + limits.parse_time;
        let truncated = html.len() > limits.bytes;
        let mut rest = &html[..html.floor_char_boundary(limits.bytes)];
        // The options dom_query::Document::from uses.
        let opts = html5ever::ParseOpts {
            tree_builder: html5ever::tree_builder::TreeBuilderOpts {
                scripting_enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let sink = MeteredSink {
            doc: Document::default(),
            nodes: Cell::new(0),
            steps: Cell::new(0),
        };
        let mut parser = html5ever::parse_document(sink, opts);
        let mut over = None;
        while over.is_none() && !rest.is_empty() {
            let (chunk, tail) = rest.split_at(rest.ceil_char_boundary(PARSE_CHUNK));
            parser.process(StrTendril::from(chunk));
            rest = tail;

            let builder = &parser.tokenizer.sink;
            // The tree builder holds its stack of open elements, its list of active formatting
            // elements and a few single pointers. Past twice the depth limit, either the page
            // nests deeper than the limit or it keeps hundreds of formatting elements open,
            // which the parser reopens, one inside the next, wherever content follows. This
            // also caps the stack html5ever walks for each tag in the next chunk.
            let held = HandleCount(Cell::new(0));
            builder.trace_handles(&held);
            let sink = &builder.sink;
            over = if held.0.get() > 2 * limits.depth {
                Some(OverLimit::Depth)
            } else if sink.nodes.get() > limits.nodes {
                Some(OverLimit::Nodes)
            } else if sink.steps.get() > limits.parse_steps {
                Some(OverLimit::ParseSteps)
            } else if Instant::now() >= deadline {
                Some(OverLimit::ParseTime)
            } else {
                None
            };
        }
        let over = over.or(truncated.then_some(OverLimit::Bytes));
        (parser.finish(), over)
    }

    /// dom_query's tree sink, counting the nodes it creates and the parser work that grows
    /// faster than the page:
    /// - element-name lookups: html5ever looks up the name of each element it passes when it
    ///   walks its stack of open elements;
    /// - the attribute comparisons the tokenizer made for each finished tag (each attribute with
    ///   the ones before it), and the ones dom_query makes when a repeated `<html>` or `<body>`
    ///   tag adds attributes;
    /// - children moved to a new parent, which misnested formatting tags can repeat.
    ///
    /// Everything else is passed on as is, except parse errors, which dom_query would keep in a
    /// list that nothing reads and that can grow by an entry every few bytes.
    struct MeteredSink {
        doc: Document,
        nodes: Cell<usize>,
        steps: Cell<u64>,
    }

    impl MeteredSink {
        fn step(&self, n: u64) {
            self.steps.set(self.steps.get().saturating_add(n));
        }

        fn created(&self, node: NodeId) -> NodeId {
            self.nodes.set(self.nodes.get() + 1);
            node
        }
    }

    impl TreeSink for MeteredSink {
        type Handle = NodeId;
        type Output = Document;
        type ElemName<'a>
            = <Document as TreeSink>::ElemName<'a>
        where
            Self: 'a;

        fn finish(self) -> Document {
            TreeSink::finish(self.doc)
        }

        fn parse_error(&self, _msg: Cow<'static, str>) {}

        fn get_document(&self) -> NodeId {
            TreeSink::get_document(&self.doc)
        }

        fn elem_name<'a>(&'a self, target: &'a NodeId) -> Self::ElemName<'a> {
            self.step(1);
            TreeSink::elem_name(&self.doc, target)
        }

        fn create_element(
            &self,
            name: QualName,
            attrs: Vec<Attribute>,
            flags: ElementFlags,
        ) -> NodeId {
            let n = attrs.len() as u64;
            self.step(n * n.saturating_sub(1) / 2);
            self.created(TreeSink::create_element(&self.doc, name, attrs, flags))
        }

        fn create_comment(&self, text: StrTendril) -> NodeId {
            self.created(TreeSink::create_comment(&self.doc, text))
        }

        fn create_pi(&self, target: StrTendril, data: StrTendril) -> NodeId {
            self.created(TreeSink::create_pi(&self.doc, target, data))
        }

        fn append(&self, parent: &NodeId, child: NodeOrText<NodeId>) {
            TreeSink::append(&self.doc, parent, child)
        }

        fn append_based_on_parent_node(
            &self,
            element: &NodeId,
            prev_element: &NodeId,
            child: NodeOrText<NodeId>,
        ) {
            TreeSink::append_based_on_parent_node(&self.doc, element, prev_element, child)
        }

        fn append_doctype_to_document(
            &self,
            name: StrTendril,
            public_id: StrTendril,
            system_id: StrTendril,
        ) {
            TreeSink::append_doctype_to_document(&self.doc, name, public_id, system_id)
        }

        fn mark_script_already_started(&self, node: &NodeId) {
            TreeSink::mark_script_already_started(&self.doc, node)
        }

        fn pop(&self, node: &NodeId) {
            TreeSink::pop(&self.doc, node)
        }

        fn get_template_contents(&self, target: &NodeId) -> NodeId {
            TreeSink::get_template_contents(&self.doc, target)
        }

        fn same_node(&self, x: &NodeId, y: &NodeId) -> bool {
            TreeSink::same_node(&self.doc, x, y)
        }

        fn set_quirks_mode(&self, mode: QuirksMode) {
            TreeSink::set_quirks_mode(&self.doc, mode)
        }

        fn append_before_sibling(&self, sibling: &NodeId, new_node: NodeOrText<NodeId>) {
            TreeSink::append_before_sibling(&self.doc, sibling, new_node)
        }

        fn add_attrs_if_missing(&self, target: &NodeId, attrs: Vec<Attribute>) {
            let had = self.doc.tree.get(target).map_or(0, |node| {
                node.query_or(0, |n| match &n.data {
                    NodeData::Element(e) => e.attrs.len() as u64,
                    _ => 0,
                })
            });
            self.step(had + had * attrs.len() as u64);
            TreeSink::add_attrs_if_missing(&self.doc, target, attrs)
        }

        fn associate_with_form(
            &self,
            target: &NodeId,
            form: &NodeId,
            nodes: (&NodeId, Option<&NodeId>),
        ) {
            TreeSink::associate_with_form(&self.doc, target, form, nodes)
        }

        fn remove_from_parent(&self, target: &NodeId) {
            TreeSink::remove_from_parent(&self.doc, target)
        }

        fn reparent_children(&self, node: &NodeId, new_parent: &NodeId) {
            self.step(self.doc.tree.child_ids_of_it(node, false).count() as u64);
            TreeSink::reparent_children(&self.doc, node, new_parent)
        }

        fn is_mathml_annotation_xml_integration_point(&self, handle: &NodeId) -> bool {
            TreeSink::is_mathml_annotation_xml_integration_point(&self.doc, handle)
        }

        fn set_current_line(&self, line_number: u64) {
            TreeSink::set_current_line(&self.doc, line_number)
        }

        fn allow_declarative_shadow_roots(&self, intended_parent: &NodeId) -> bool {
            TreeSink::allow_declarative_shadow_roots(&self.doc, intended_parent)
        }

        fn attach_declarative_shadow(
            &self,
            location: &NodeId,
            template: &NodeId,
            attrs: &[Attribute],
        ) -> bool {
            TreeSink::attach_declarative_shadow(&self.doc, location, template, attrs)
        }

        fn maybe_clone_an_option_into_selectedcontent(&self, option: &NodeId) {
            TreeSink::maybe_clone_an_option_into_selectedcontent(&self.doc, option)
        }
    }

    /// Counts the handles html5ever's tree builder holds (`TreeBuilder::trace_handles`).
    struct HandleCount(Cell<usize>);

    impl Tracer for HandleCount {
        type Handle = NodeId;

        fn trace_handle(&self, _node: &NodeId) {
            self.0.set(self.0.get() + 1);
        }
    }
}
use page_parser::parse_page;

/// The URL that relative references in the page resolve against: the first `<base href>`,
/// itself resolved against `page_url`, when that gives an http(s) URL; otherwise `page_url`. A
/// `javascript:` or `data:` base would turn every relative link and image into script or inline
/// data.
fn document_base(doc: &dom_query::Document, page_url: &Url) -> Url {
    doc.select_single("base[href]")
        .attr("href")
        .and_then(|href| page_url.join(&href).ok())
        .filter(|base| matches!(base.scheme(), "http" | "https"))
        .unwrap_or_else(|| page_url.clone())
}

/// Makes the URLs under `root` absolute against `base` with WHATWG `Url::join`, as a browser
/// would: `a`/`area` link targets, every `src` and `poster`, and each `srcset` candidate. URLs
/// that are already absolute (any scheme, such as `mailto:` or `data:`) are left as written
/// (see `resolve_url`).
/// With `keep_fragments`, a bare `#anchor` link stays pointing into the article, as Readability.js
/// leaves it when the base is the page itself.
fn absolutize_urls(root: &dom_query::Selection, base: &Url, keep_fragments: bool) {
    let resolve = |value: &str| resolve_url(base, value);
    for node in root.select("a[href], area[href]").nodes() {
        let Some(href) = node.attr("href") else {
            continue;
        };
        if keep_fragments && href.trim_start().starts_with('#') {
            continue;
        }
        if let Some(abs) = resolve(&href) {
            node.set_attr("href", &abs);
        }
    }
    for attr in ["src", "poster"] {
        for node in root.select(&format!("[{attr}]")).nodes() {
            if let Some(abs) = node.attr(attr).and_then(|value| resolve(&value)) {
                node.set_attr(attr, &abs);
            }
        }
    }
    for node in root.select("[srcset]").nodes() {
        if let Some(srcset) = node.attr("srcset") {
            node.set_attr("srcset", &absolutize_srcset(&srcset, resolve));
        }
    }
}

/// `value` resolved against `base` with `Url::join`, or `None` when it cannot be resolved or
/// resolves the same against any base (an absolute URL such as `https://host/x`, `mailto:` or
/// `data:`), and is then kept as written. That `value` parses on its own is not enough:
/// `https:img/a.png` parses, as `https://img/a.png`, but against an https base it is relative,
/// like `img/a.png`.
fn resolve_url(base: &Url, value: &str) -> Option<String> {
    let joined = base.join(value).ok()?;
    if Url::parse(value).is_ok_and(|parsed| parsed == joined) {
        return None;
    }
    Some(joined.into())
}

/// Resolves each candidate URL of a `srcset` and keeps its descriptors as written. As in HTML's
/// srcset parsing, a candidate's URL runs to the next whitespace (so it may contain commas, as
/// image CDN URLs often do) and its descriptors run to the next comma outside parentheses.
fn absolutize_srcset(srcset: &str, resolve: impl Fn(&str) -> Option<String>) -> String {
    let mut candidates = Vec::new();
    let mut rest = srcset;
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ',');
        if rest.is_empty() {
            break;
        }
        let (url, after) = rest.split_at(
            rest.find(|c: char| c.is_ascii_whitespace())
                .unwrap_or(rest.len()),
        );
        let (url, descriptors, after) = if let Some(url) = url.strip_suffix(',') {
            (url.trim_end_matches(','), "", after)
        } else {
            let mut in_parens = false;
            let end = after
                .char_indices()
                .find(|&(_, c)| {
                    match c {
                        '(' => in_parens = true,
                        ')' => in_parens = false,
                        ',' => return !in_parens,
                        _ => {}
                    }
                    false
                })
                .map_or(after.len(), |(i, _)| i);
            (url, after[..end].trim(), &after[end..])
        };
        let url = resolve(url).unwrap_or_else(|| url.to_string());
        candidates.push(if descriptors.is_empty() {
            url
        } else {
            format!("{url} {descriptors}")
        });
        rest = after;
    }
    candidates.join(", ")
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(50)
        .collect()
}

fn parse_opml(content: &str) -> Result<OpmlDocument> {
    // Simple XML parsing for OPML
    let title = extract_xml_value(content, "title").unwrap_or_else(|| "Imported".to_string());
    let outlines = parse_outlines(content);

    Ok(OpmlDocument {
        title,
        date_created: None,
        outlines,
    })
}

fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&start_tag) {
        let rest = &content[start + start_tag.len()..];
        if let Some(end) = rest.find(&end_tag) {
            return Some(rest[..end].to_string());
        }
    }
    None
}

fn parse_outlines(content: &str) -> Vec<OpmlOutline> {
    let mut outlines = Vec::new();

    for line in content.lines() {
        if line.contains("<outline") {
            let text = extract_attr(line, "text").unwrap_or_default();
            let xml_url = extract_attr(line, "xmlUrl");
            let html_url = extract_attr(line, "htmlUrl");
            let feed_type = extract_attr(line, "type");

            outlines.push(OpmlOutline {
                text,
                xml_url,
                html_url,
                feed_type,
                children: Vec::new(),
            });
        }
    }

    outlines
}

fn extract_attr(line: &str, attr: &str) -> Option<String> {
    let needle = format!("{}=\"", attr);
    if let Some(start) = line.find(&needle) {
        let rest = &line[start + needle.len()..];
        if let Some(end) = rest.find('"') {
            return Some(
                rest[..end]
                    .replace("&amp;", "&")
                    .replace("&lt;", "<")
                    .replace("&gt;", ">"),
            );
        }
    }
    None
}

fn flatten_outlines(outlines: &[OpmlOutline]) -> Vec<OpmlOutline> {
    let mut result = Vec::new();
    for outline in outlines {
        result.push(outline.clone());
        result.extend(flatten_outlines(&outline.children));
    }
    result
}

fn extract_html_body(mail: &mailparse::ParsedMail) -> Option<String> {
    // Try to find HTML part
    if mail.ctype.mimetype == "text/html" {
        return mail.get_body().ok();
    }

    for subpart in &mail.subparts {
        if let Some(html) = extract_html_body(subpart) {
            return Some(html);
        }
    }

    // Fall back to text/plain
    if mail.ctype.mimetype == "text/plain" {
        return mail
            .get_body()
            .ok()
            .map(|t| format!("<pre>{}</pre>", html_escape(&t)));
    }

    None
}

// ============================================================================
// Axum API Handlers
// ============================================================================

use axum::Json;
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::StatusCode;

#[derive(Clone)]
pub struct FeedState {
    pub manager: Arc<FeedManager>,
    /// Keeps the scheduler task alive (it exits when all senders drop).
    pub scheduler: Option<mpsc::Sender<SchedulerCommand>>,
    /// Device push channel (`AppState::notification_tx`): new EPUBs are announced with SyncComplete.
    pub notification_tx: tokio::sync::broadcast::Sender<crate::notifications::WsMessage>,
}

/// GET /feeds/v1/subscriptions - List all subscriptions
pub async fn list_subscriptions(State(state): State<FeedState>) -> Result<Json<Vec<Subscription>>> {
    let subscriptions = state.manager.list_subscriptions()?;
    Ok(Json(subscriptions))
}

/// POST /feeds/v1/subscriptions - Create subscription
pub async fn create_subscription(
    State(state): State<FeedState>,
    Json(req): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<Subscription>)> {
    let subscription = state.manager.create_subscription(req).await?;
    Ok((StatusCode::CREATED, Json(subscription)))
}

/// GET /feeds/v1/subscriptions/:id - Get subscription
pub async fn get_subscription(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<Subscription>> {
    let subscription = state.manager.get_subscription(&id)?;
    Ok(Json(subscription))
}

/// DELETE /feeds/v1/subscriptions/:id - Delete subscription
pub async fn delete_subscription(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
) -> Result<StatusCode> {
    state.manager.delete_subscription(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

/// PATCH /feeds/v1/subscriptions/:id - Update subscription
pub async fn update_subscription(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
    Json(updates): Json<serde_json::Value>,
) -> Result<Json<Subscription>> {
    let subscription = state.manager.update_subscription(&id, updates)?;
    Ok(Json(subscription))
}

/// POST /feeds/v1/refresh - Refresh feeds
pub async fn refresh_feeds(
    State(state): State<FeedState>,
    Json(req): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>> {
    let response = state.manager.refresh(req).await?;
    Ok(Json(response))
}

/// GET /feeds/v1/articles - List articles
pub async fn list_articles(
    State(state): State<FeedState>,
    Query(query): Query<ArticleQuery>,
) -> Result<Json<Vec<Article>>> {
    let articles = state.manager.list_articles(query)?;
    Ok(Json(articles))
}

/// GET /feeds/v1/articles/:id - Get article
pub async fn get_article(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<Article>> {
    let article = state.manager.get_article(&id)?;
    Ok(Json(article))
}

/// POST /feeds/v1/articles/:id/read - Mark article read
pub async fn mark_article_read(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
) -> Result<StatusCode> {
    state.manager.mark_read(&id, true)?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /feeds/v1/articles/:id/epub - Generate EPUB
pub async fn generate_epub(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<serde_json::Value>> {
    let article = state.manager.get_article(&id)?;
    let path = state.manager.article_to_epub(&article)?;
    Ok(Json(serde_json::json!({
        "path": path.to_string_lossy()
    })))
}

/// POST /feeds/v1/extract - Extract article from URL
pub async fn extract_article(
    State(state): State<FeedState>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<ExtractedArticle>> {
    let url = req
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ServerError::MissingHeader("url".into()))?;
    let extracted = state.manager.extract_article(url).await?;
    Ok(Json(extracted))
}

/// POST /feeds/v1/import/opml - Import OPML
pub async fn import_opml(
    State(state): State<FeedState>,
    body: String,
) -> Result<Json<Vec<Subscription>>> {
    let imported = state.manager.import_opml(&body).await?;
    Ok(Json(imported))
}

/// GET /feeds/v1/export/opml - Export OPML
pub async fn export_opml(
    State(state): State<FeedState>,
) -> Result<(
    StatusCode,
    [(axum::http::header::HeaderName, &'static str); 1],
    String,
)> {
    let opml = state.manager.export_opml()?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/xml")],
        opml,
    ))
}

/// GET /feeds/v1/stats - Get statistics
pub async fn get_stats(State(state): State<FeedState>) -> Result<Json<FeedStats>> {
    let stats = state.manager.stats()?;
    Ok(Json(stats))
}

/// POST /feeds/v1/subscriptions/:id/sync - Sync articles to device
pub async fn sync_to_device(
    State(state): State<FeedState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<serde_json::Value>> {
    let sub = state.manager.get_subscription(&id)?;
    let before = state.manager.storage.get_root().generation;
    // Only this subscription's articles, so each lands in its own configured folder.
    let result = state
        .manager
        .sync_articles_to_folder(Some(&sub.id), &sub.folder);
    // Articles (and the folder) are committed one by one and marked synced, so a later
    // failure must not swallow the push for what already landed: retries skip those.
    let generation = state.manager.storage.get_root().generation;
    if generation != before {
        // Tell connected devices to pull the new root, as document uploads do.
        let _ = state
            .notification_tx
            .send(crate::notifications::WsMessage::sync_complete(
                generation,
                "local-server",
                "local-user",
            ));
    }
    Ok(Json(serde_json::json!({ "synced": result? })))
}

// ============================================================================
// Router
// ============================================================================

use axum::Router;
use axum::routing::{delete, get, patch, post};

pub fn feeds_router(state: FeedState) -> Router {
    Router::new()
        .route("/subscriptions", get(list_subscriptions))
        .route("/subscriptions", post(create_subscription))
        .route("/subscriptions/{id}", get(get_subscription))
        .route("/subscriptions/{id}", delete(delete_subscription))
        .route("/subscriptions/{id}", patch(update_subscription))
        .route("/subscriptions/{id}/sync", post(sync_to_device))
        .route("/refresh", post(refresh_feeds))
        .route("/articles", get(list_articles))
        .route("/articles/{id}", get(get_article))
        .route("/articles/{id}/read", post(mark_article_read))
        .route("/articles/{id}/epub", post(generate_epub))
        .route("/extract", post(extract_article))
        .route("/import/opml", post(import_opml))
        .route("/export/opml", get(export_opml))
        .route("/stats", get(get_stats))
        .with_state(state)
}

#[cfg(test)]
mod folder_sync_tests {
    use super::*;

    fn article(id: &str, sub: &str) -> Article {
        // article_to_epub slices the first 8 chars of the id (real ids are uuids).
        Article {
            id: format!("{id}-000000000"),
            subscription_id: sub.into(),
            title: format!("Title {id}"),
            url: format!("https://example.com/{id}"),
            author: None,
            summary: None,
            content_html: Some("<p>hi</p>".into()),
            content_text: None,
            published_at: None,
            fetched_at: Utc::now(),
            read: false,
            synced: false,
            epub_path: None,
            word_count: None,
            reading_time_mins: None,
        }
    }

    /// (visibleName, type, parent) of every node in the current root.
    fn tree(storage: &Storage) -> Vec<(String, String, String, String)> {
        let lines = |b: Vec<u8>| {
            String::from_utf8_lossy(&b)
                .lines()
                .skip(1)
                .map(|l| l.split(':').map(String::from).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };
        lines(storage.get(&storage.get_root().hash).unwrap())
            .into_iter()
            .map(|node| {
                let meta = lines(storage.get(&node[0]).unwrap())
                    .into_iter()
                    .find(|f| f[2] == format!("{}.metadata", node[2]))
                    .unwrap();
                let m: serde_json::Value =
                    serde_json::from_slice(&storage.get(&meta[0]).unwrap()).unwrap();
                (
                    node[2].clone(),
                    m["visibleName"].as_str().unwrap().into(),
                    m["type"].as_str().unwrap().into(),
                    m["parent"].as_str().unwrap().into(),
                )
            })
            .collect()
    }

    #[test]
    fn sync_places_articles_in_configured_folder() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let manager = FeedManager::new(
            &tmp.path().join("feeds.db"),
            storage.clone(),
            &tmp.path().join("epub"),
        )
        .unwrap();
        for sub in ["news", "other"] {
            manager.db.lock().execute("INSERT INTO subscriptions (id, name, url, feed_type, created_at, updated_at) VALUES (?1, ?1, ?1, 'rss', '', '')", [sub]).unwrap();
        }
        manager
            .save_articles(&[article("a1", "news"), article("b1", "other")])
            .unwrap();

        assert_eq!(
            manager
                .sync_articles_to_folder(Some("news"), "News")
                .unwrap(),
            1,
            "only this subscription's articles"
        );
        let t = tree(&storage);
        let folder = t
            .iter()
            .find(|n| n.2 == "CollectionType")
            .expect("folder created");
        assert_eq!((folder.1.as_str(), folder.3.as_str()), ("News", ""));
        let doc = t.iter().find(|n| n.1 == "Title a1").unwrap();
        assert_eq!(doc.3, folder.0, "document parent is the folder id");

        // Next sync reuses the folder; an empty folder setting keeps the top level.
        manager.save_articles(&[article("a2", "news")]).unwrap();
        manager
            .sync_articles_to_folder(Some("news"), "News")
            .unwrap();
        manager.sync_articles_to_folder(Some("other"), "").unwrap();
        let t = tree(&storage);
        assert_eq!(t.iter().filter(|n| n.2 == "CollectionType").count(), 1);
        assert_eq!(t.iter().find(|n| n.1 == "Title a2").unwrap().3, folder.0);
        assert_eq!(t.iter().find(|n| n.1 == "Title b1").unwrap().3, "");
    }

    #[tokio::test]
    async fn sync_to_device_notifies_devices_only_when_something_synced() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let manager = Arc::new(
            FeedManager::new(
                &tmp.path().join("feeds.db"),
                storage.clone(),
                &tmp.path().join("epub"),
            )
            .unwrap(),
        );
        manager.db.lock().execute("INSERT INTO subscriptions (id, name, url, feed_type, created_at, updated_at) VALUES ('news', 'news', 'news', 'rss', ?1, ?1)", [Utc::now().to_rfc3339()]).unwrap();
        manager.save_articles(&[article("a1", "news")]).unwrap();
        let (notification_tx, mut rx) = tokio::sync::broadcast::channel(4);
        let state = FeedState {
            manager,
            scheduler: None,
            notification_tx,
        };

        let _ = sync_to_device(State(state.clone()), UrlPath("news".into()))
            .await
            .unwrap();
        let msg = rx.try_recv().expect("SyncComplete after new EPUBs");
        assert_eq!(msg.message.attributes.event, "SyncComplete");
        let _ = sync_to_device(State(state), UrlPath("news".into()))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err(), "nothing new, no push");
    }

    #[tokio::test]
    async fn sync_to_device_notifies_committed_articles_even_when_a_later_one_fails() {
        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let manager = Arc::new(
            FeedManager::new(
                &tmp.path().join("feeds.db"),
                storage.clone(),
                &tmp.path().join("epub"),
            )
            .unwrap(),
        );
        manager.db.lock().execute("INSERT INTO subscriptions (id, name, url, feed_type, created_at, updated_at, folder) VALUES ('news', 'news', 'news', 'rss', ?1, ?1, 'News')", [Utc::now().to_rfc3339()]).unwrap();
        // Newest first: a1 syncs, then a2's EPUB can't be read.
        let mut ok = article("a1", "news");
        ok.published_at = Some(Utc::now());
        let mut bad = article("a2", "news");
        bad.published_at = Some(Utc::now() - chrono::Duration::days(1));
        bad.epub_path = Some(tmp.path().join("missing.epub").to_string_lossy().into());
        manager.save_articles(&[ok, bad]).unwrap();
        let (notification_tx, mut rx) = tokio::sync::broadcast::channel(4);
        let state = FeedState {
            manager,
            scheduler: None,
            notification_tx,
        };

        assert!(
            sync_to_device(State(state.clone()), UrlPath("news".into()))
                .await
                .is_err(),
            "the failure is still reported"
        );
        let msg = rx
            .try_recv()
            .expect("SyncComplete for the article that did land");
        assert_eq!(msg.message.attributes.event, "SyncComplete");
        assert!(tree(&storage).iter().any(|n| n.1 == "Title a1"));
        assert!(rx.try_recv().is_err());
    }
}

#[cfg(test)]
mod readability_tests {
    use super::*;

    fn extract(html: &str, page_url: &str) -> Result<ExtractedArticle> {
        extract_readable(html, &Url::parse(page_url).unwrap())
    }

    const BLOG: &str = include_str!("../tests/fixtures/articles/engineering_blog.html");
    const BLOG_URL: &str = "https://blog.northwind.example/2026/03/ci-build-times/";
    const NEWS: &str = include_str!("../tests/fixtures/articles/news_div_soup.html");
    const NEWS_URL: &str = "https://gazette.example/news/2026/09/ferry-schedule";

    fn assert_absent(content: &str, furniture: &[&str]) {
        for f in furniture {
            assert!(
                !content.contains(f),
                "{f:?} should have been stripped:\n{content}"
            );
        }
    }

    #[test]
    fn blog_post_keeps_the_article_and_drops_page_furniture() {
        let a = extract(BLOG, BLOG_URL).unwrap();
        assert_eq!(a.title, "How We Cut Our CI Build Times in Half");
        for kept in [
            "Twelve months ago, a clean build",
            "<h2>Measuring before optimizing</h2>",
            "The cache hit rate on the first day was 83 percent.",
            "parallelism: 4",
            "<figcaption>Where the forty-one minutes went",
            "without the data we would have spent weeks",
        ] {
            assert!(
                a.content_html.contains(kept),
                "missing {kept:?}:\n{}",
                a.content_html
            );
        }
        assert_absent(
            &a.content_html,
            &[
                "Accept all",             // cookie banner
                "Careers",                // site navigation
                "Sponsored: Ship faster", // sidebar ad
                "ads.example.net",        // ad banner image
                "Popular posts",          // sidebar widget
                "Tweet",                  // share buttons
                "Get new posts by email", // newsletter form
                "<form",                  // newsletter form
                "devops_dan",             // comments
                "Related articles",       // related posts rail
                "All rights reserved",    // footer
                "<script",                // trackers
                "dataLayer",              // inline tracker
                "class=\"post-content\"", // page classes
            ],
        );
        // Relative image and link are resolved against the page URL, absolute ones kept.
        assert!(a.content_html.contains(
            r#"src="https://blog.northwind.example/images/2026/ci-stage-breakdown.png""#
        ));
        assert!(
            a.content_html
                .contains(r#"href="https://blog.northwind.example/2026/docs/remote-cache.html""#)
        );
        assert!(
            a.content_html
                .contains(r#"href="https://buildtool.example.org/docs/caching""#)
        );

        assert!(a.content_text.contains("Twelve months ago"));
        assert!(!a.content_text.contains('<'));
        assert_eq!(
            a.word_count as usize,
            a.content_text.split_whitespace().count()
        );
        // The article body is 423 words; with the page furniture it would be 554.
        assert!((410..=435).contains(&a.word_count), "{}", a.word_count);
        assert_eq!(a.reading_time_mins, a.word_count / 200);
    }

    #[test]
    fn div_soup_news_story_is_extracted() {
        let a = extract(NEWS, NEWS_URL).unwrap();
        assert_eq!(a.title, "Harbor Council Approves New Ferry Schedule");
        for kept in [
            "voted five to two on Tuesday night",
            "the first boat will leave the island terminal at 5:40 a.m.",
            "said council chair Denise Okafor",
            "a lifeline for restaurant and hospitality workers",
            "results expected before the summer season",
        ] {
            assert!(
                a.content_html.contains(kept),
                "missing {kept:?}:\n{}",
                a.content_html
            );
        }
        assert_absent(
            &a.content_html,
            &[
                "E-paper",       // top bar
                "Obituaries",    // menu
                "<iframe",       // leaderboard ad
                "Most read",     // right rail
                "Island Realty", // sponsor box
                "Print",         // share tools
                "Copyright 2026",
                "_gaq",
            ],
        );
        // A path-relative image resolves against the article's directory, a root-relative link
        // against the host.
        assert!(
            a.content_html.contains(
                r#"src="https://gazette.example/news/2026/09/photos/ferry-terminal.jpg""#
            )
        );
        assert!(
            a.content_html
                .contains(r#"href="https://gazette.example/ferry/schedule.pdf""#)
        );
    }

    #[test]
    fn page_without_readable_text_still_answers_with_its_title() {
        let a = extract(
            "<html><head><title>Nothing here</title></head><body></body></html>",
            "https://example.com/empty",
        )
        .unwrap();
        assert_eq!(a.title, "Nothing here");
        assert_eq!(a.content_html, "");
        assert_eq!((a.word_count, a.reading_time_mins), (0, 1));

        let a = extract("", "https://example.com/blank").unwrap();
        assert_eq!((a.title.as_str(), a.content_html.as_str()), ("", ""));

        // No article, only navigation: like the old extractor (which fell back to the whole
        // document), answer with what is there instead of failing, links made absolute.
        let a = extract(
            include_str!("../tests/fixtures/articles/link_farm.html"),
            "https://example.com/sitemap",
        )
        .unwrap();
        assert_eq!(a.title, "Site map");
        assert!(a.content_html.contains(r#"href="https://example.com/b""#));
    }

    /// gjson (dom_smoothie's JSON-LD parser) builds a non-UTF-8 `String` from `\u000` followed by
    /// a multi-byte character. JSON-LD is off, so a hostile block cannot reach the title.
    #[test]
    fn hostile_json_ld_does_not_reach_the_output() {
        let html = r#"<html><head><title>Ferry schedule approved</title>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"NewsArticle","headline":"\u000é x","name":"\u000é x"}
</script></head><body><article>
<p>The Port Ellis harbor council voted five to two on Tuesday night to adopt a new ferry timetable
that adds early-morning crossings on weekdays and cuts two of the least-used evening sailings.</p>
<p>Commuters who work on the mainland have asked for an earlier departure for years, and the first
boat will now leave the island terminal twenty minutes earlier than it does today.</p>
</article></body></html>"#;
        let a = extract(html, "https://gazette.example/ferry").unwrap();
        assert_eq!(a.title, "Ferry schedule approved");
        for s in [&a.title, &a.content_html, &a.content_text] {
            assert!(std::str::from_utf8(s.as_bytes()).is_ok());
        }
        assert!(a.content_html.contains("voted five to two"));
    }

    const FERRY: &str = r##"<html><head><title>Ferry schedule approved</title></head><body><article>
<p>The Port Ellis harbor council voted five to two on Tuesday night to adopt a new ferry timetable
that adds early-morning crossings on weekdays and cuts two of the least-used evening sailings.</p>
<p>Background: <a href="httpd-notes.html">server notes</a>, <a href="https-migration/">the
migration</a>, <a href="a.php?next=/../b">the form</a>, <a href="..">the archive</a>,
<a href=".">this month</a>, <a href="#timetable">the timetable</a>,
<a href="//cdn.example/map.pdf">the map</a>, <a href="mailto:desk@gazette.example">the desk</a>
and <a href="https://ferries.example/schedule">the operator</a>.</p>
<p><img src="img/terminal at dawn.jpg" srcset="img/t-1x.jpg 1x, /img/w_600,h_400/t.jpg 2x"
alt="The island terminal"></p>
<p id="timetable">Commuters who work on the mainland have asked for an earlier departure for
years, and the first boat will now leave the island terminal twenty minutes earlier than it does
today. The last evening sailing moves from 11:15 p.m. to 10:30 p.m.</p>
</article></body></html>"##;

    fn assert_contains_all(content: &str, wanted: &[&str]) {
        for w in wanted {
            assert!(content.contains(w), "missing {w:?}:\n{content}");
        }
    }

    /// Resolution follows WHATWG `Url::join`, as the old extractor and browsers do, including
    /// the cases dom_smoothie's own resolver gets wrong.
    #[test]
    fn relative_urls_resolve_like_a_browser() {
        let a = extract(FERRY, "https://gazette.example/news/2026/ferry.html?id=7").unwrap();
        assert_contains_all(
            &a.content_html,
            &[
                // Relative, though they start with "http".
                r#"href="https://gazette.example/news/2026/httpd-notes.html""#,
                r#"href="https://gazette.example/news/2026/https-migration/""#,
                // `/../` in the query string is not a path segment.
                r#"href="https://gazette.example/news/2026/a.php?next=/../b""#,
                // `..` and `.` name directories, so they keep the trailing slash.
                r#"href="https://gazette.example/news/""#,
                r#"href="https://gazette.example/news/2026/""#,
                // An in-page anchor stays in-page, as in Readability.js.
                r##"href="#timetable""##,
                r#"href="https://cdn.example/map.pdf""#,
                r#"href="mailto:desk@gazette.example""#,
                r#"href="https://ferries.example/schedule""#,
                // Percent-encoded, like a browser request.
                r#"src="https://gazette.example/news/2026/img/terminal%20at%20dawn.jpg""#,
                // Each candidate, commas inside a URL included.
                "srcset=\"https://gazette.example/news/2026/img/t-1x.jpg 1x, \
                 https://gazette.example/img/w_600,h_400/t.jpg 2x\"",
            ],
        );
    }

    #[test]
    fn base_href_sets_the_base_unless_it_is_not_http() {
        let page =
            |base: &str| FERRY.replace("</title>", &format!("</title><base href=\"{base}\">"));
        let url = "https://gazette.example/news/2026/ferry.html";

        let a = extract(&page("/archive/2026/"), url).unwrap();
        assert_contains_all(
            &a.content_html,
            &[
                r#"href="https://gazette.example/archive/2026/httpd-notes.html""#,
                r#"href="https://gazette.example/archive/""#,
                // With a base, a bare anchor points at the base, as in a browser.
                r##"href="https://gazette.example/archive/2026/#timetable""##,
                r#"src="https://gazette.example/archive/2026/img/terminal%20at%20dawn.jpg""#,
            ],
        );

        // A `javascript:` or `data:` base is ignored, and URLs resolve against the page.
        for hostile in ["javascript:alert(5)//", "data:text/html,x/"] {
            let a = extract(&page(hostile), url).unwrap();
            assert_contains_all(
                &a.content_html,
                &[
                    r#"href="https://gazette.example/news/2026/httpd-notes.html""#,
                    r#"src="https://gazette.example/news/2026/img/terminal%20at%20dawn.jpg""#,
                    r##"href="#timetable""##,
                ],
            );
            assert_absent(&a.content_html, &["javascript:", "data:text"]);
        }
    }

    #[test]
    fn srcset_candidates_are_resolved_one_by_one() {
        let base = Url::parse("https://img.example/posts/1/").unwrap();
        let resolve = |srcset: &str| absolutize_srcset(srcset, |u: &str| resolve_url(&base, u));
        assert_eq!(resolve(""), "");
        assert_eq!(resolve("a.jpg"), "https://img.example/posts/1/a.jpg");
        assert_eq!(
            resolve(" a.jpg 1x,b.jpg  2x ,"),
            "https://img.example/posts/1/a.jpg 1x, https://img.example/posts/1/b.jpg 2x"
        );
        // A URL runs to whitespace, so commas inside it stay; a trailing comma ends it.
        assert_eq!(
            resolve("/w_300,h_200/a.jpg 300w, b.jpg,, c.jpg 2x"),
            "https://img.example/w_300,h_200/a.jpg 300w, https://img.example/posts/1/b.jpg, \
             https://img.example/posts/1/c.jpg 2x"
        );
        // Absolute and data: URLs are kept; a comma inside parentheses is part of the descriptor.
        assert_eq!(
            resolve("https://cdn.example/x.jpg 1x, data:image/png;base64,AAAA 2x, d.jpg (a, b) 3x"),
            "https://cdn.example/x.jpg 1x, data:image/png;base64,AAAA 2x, \
             https://img.example/posts/1/d.jpg (a, b) 3x"
        );
    }

    proptest::proptest! {
        /// Any list of candidates, however separated, comes back as the same candidates in
        /// order, each URL resolved with `Url::join`, and resolving again changes nothing.
        #[test]
        fn srcset_resolution_keeps_every_candidate(
            candidates in proptest::collection::vec(
                (
                    "[a-z0-9_][a-z0-9_,]{0,6}(/[a-z0-9_,]{1,6}){0,2}\\.jpg",
                    proptest::option::of("[1-4]x|[1-9][0-9]{1,3}w"),
                ),
                0..6,
            ),
            separators in proptest::collection::vec(
                proptest::sample::select(vec![", ", " , ", " ,", ",\t", "\n,  "]),
                6,
            ),
            leading in "[ ,]{0,3}",
        ) {
            let base = Url::parse("https://img.example/posts/1/").unwrap();
            let resolve = |srcset: &str| absolutize_srcset(srcset, |u: &str| resolve_url(&base, u));
            let mut srcset = leading;
            let mut expected = Vec::new();
            for (i, (url, descriptor)) in candidates.iter().enumerate() {
                if i > 0 {
                    srcset.push_str(separators[i]);
                }
                srcset.push_str(url);
                let abs = base.join(url).unwrap().to_string();
                match descriptor {
                    Some(d) => {
                        srcset.push_str(&format!(" {d}"));
                        expected.push(format!("{abs} {d}"));
                    }
                    None => expected.push(abs),
                }
            }
            let resolved = resolve(&srcset);
            proptest::prop_assert_eq!(&resolved, &expected.join(", "));
            proptest::prop_assert_eq!(resolve(&resolved), resolved);
        }
    }

    fn extract_under(html: &str, limits: &ExtractLimits) -> (ExtractedArticle, Option<OverLimit>) {
        extract_page(html, &Url::parse("https://e.example/a/").unwrap(), limits).unwrap()
    }

    /// dom_smoothie's cost grows about with the cube of the nesting depth and its selector
    /// matching recurses per level, so a page nested deeper than the limit gets its plain text
    /// instead. The answer does not depend on timing: an unguarded run takes minutes, or aborts
    /// the test process.
    #[test]
    fn deeply_nested_pages_get_their_plain_text() {
        let page = |open: &str, n: usize| {
            format!(
                "<html><head><title>T</title></head><body>{}<p>Some article text, with commas, \
                 and a few more words.</p></body></html>",
                open.repeat(n)
            )
        };
        let depth = ARTICLE_LIMITS.depth;
        // html and body are two levels, the paragraph one more.
        let (a, over) = extract_under(&page("<span>", depth - 3), &ARTICLE_LIMITS);
        assert_eq!(over, None);
        assert!(
            a.content_html
                .starts_with(r#"<div id="readability-page-1""#),
            "{}",
            a.content_html
        );

        let (a, over) = extract_under(&page("<span>", depth - 2), &ARTICLE_LIMITS);
        assert_eq!(over, Some(OverLimit::Depth));
        assert_eq!(a.title, "T");
        assert_eq!(
            a.content_html,
            "<p>Some article text, with commas, and a few more words.</p>"
        );
        assert_eq!(
            a.content_text,
            "Some article text, with commas, and a few more words."
        );
        assert_eq!((a.word_count, a.reading_time_mins), (10, 1));

        // Far deeper, the parser stops soon after passing twice the limit, before the text.
        for (open, n) in [
            // 16 s of CPU in a release build if extracted.
            ("<div>", 2_000),
            // About 30,000 levels: dom_query's recursive matching overflowed the stack here.
            ("<b><i><u>", 10_000),
            // html5ever's tree builder walks its whole stack for each <li>: 160 KB of these took
            // 4 s to parse.
            ("<ul><li>", 20_000),
        ] {
            let (a, over) = extract_under(&page(open, n), &ARTICLE_LIMITS);
            assert_eq!(over, Some(OverLimit::Depth), "{open} x {n}");
            assert_eq!((a.title.as_str(), a.word_count), ("T", 0), "{open} x {n}");
        }

        let depth_of = |html: &str| page_shape(&dom_query::Document::from(html)).deepest;
        assert_eq!(depth_of(""), 2); // html > body (head is a sibling)
        assert_eq!(depth_of("<p>x"), 3);
        assert_eq!(depth_of("<div><p>x</p></div><p>y</p>"), 4);
        assert_eq!(depth_of("<ul><li><ul><li>x</ul></ul><p>y"), 6);
        // Implied end tags close each <p>, so these are siblings.
        assert_eq!(depth_of("<p>a<p>b<p>c"), 3);
    }

    /// Sibling subtrees each nested just under the depth limit: in a release build, 20 of these
    /// took dom_smoothie 6 s and 100 took 10 s. The work estimate counts every node at its depth,
    /// so a page gets one or two such subtrees, not forty.
    #[test]
    fn many_deep_sibling_subtrees_are_over_the_work_limit() {
        let chain = format!(
            "{}<p>Hi there.</p>{}",
            "<div>".repeat(507),
            "</div>".repeat(507)
        );
        let page = |n: usize| {
            format!(
                "<html><head><title>T</title></head><body>{}</body></html>",
                chain.repeat(n)
            )
        };
        let work = |n: usize| page_shape(&dom_query::Document::from(page(n).as_str())).work;
        assert!(work(1) < ARTICLE_LIMITS.work);
        assert!(work(3) > ARTICLE_LIMITS.work);

        let (a, over) = extract_under(&page(40), &ARTICLE_LIMITS);
        assert_eq!(over, Some(OverLimit::Work));
        assert_eq!(a.title, "T");
        assert_eq!(a.content_text, vec!["Hi there."; 40].join("\n\n"));
    }

    /// Until a heading resembles the title, dom_smoothie compares the whole title with each
    /// `<h1>` and `<h2>`, on each of its passes. In a release build, a 1 MB og:title before 200
    /// short headings took 3 s, and a page within every other limit could hold a 12 MB title
    /// before 185,000 headings. Wherever dom_smoothie takes the title from, a long one before
    /// many headings sends the page to its plain text.
    #[test]
    fn long_titles_before_many_headings_are_over_the_work_limit() {
        let long = "lorem ipsum dolor sit amet ".repeat(800);
        let long = long.trim();
        let sections: String = (0..600)
            .map(|i| format!("<h2>Part {i}</h2><p>Text of part {i}.</p>"))
            .collect();
        let page = |head: &str, h1: &str| {
            format!("<html><head>{head}</head><body>{sections}{h1}</body></html>")
        };
        for (head, h1, title) in [
            // og:title and other title meta tags come first.
            (
                format!(r#"<meta property="og:title" content="{long}"><title>Short</title>"#),
                String::new(),
                long,
            ),
            // Then the <title>, here with no separators to cut it at, and no <h1>.
            (format!("<title>{long}</title>"), String::new(), long),
            // A <title> under 15 characters gives way to the first <h1>, here after the others.
            (
                "<title>Hello</title>".to_string(),
                format!("<h1>{long}</h1>"),
                "Hello",
            ),
        ] {
            let (a, over) = extract_under(&page(&head, &h1), &ARTICLE_LIMITS);
            assert_eq!(over, Some(OverLimit::Work), "{head:.40} {h1:.40}");
            assert_eq!(a.title, title);
            assert!(
                a.content_text
                    .starts_with("Part 0\n\nText of part 0.\n\nPart 1\n\n"),
                "{:.100}",
                a.content_text
            );
        }

        // A real title before the same headings, or the long one before a few, is extracted.
        let og = |title: &str| format!(r#"<meta property="og:title" content="{title}">"#);
        let title = "Harbor council adopts a new ferry timetable";
        let (a, over) = extract_under(&page(&og(title), ""), &ARTICLE_LIMITS);
        assert_eq!((over, a.title.as_str()), (None, title));
        assert!(a.content_text.contains("Text of part 599."));
        let few = format!(
            "<html><head>{}</head><body><h2>Part 0</h2><p>Text of part 0.</p></body></html>",
            og(long)
        );
        let (a, over) = extract_under(&few, &ARTICLE_LIMITS);
        assert_eq!((over, a.title.as_str()), (None, long));
        assert!(
            a.content_html
                .starts_with(r#"<div id="readability-page-1""#),
            "{}",
            a.content_html
        );
    }

    /// The estimate counts each `<h1>` and `<h2>`, nested or not, for the title comparisons: a
    /// page with 2,000 headings stays well within the limit with a real title.
    #[test]
    fn headings_are_counted_for_the_title_comparisons() {
        let shape = |body: &str| {
            page_shape(&dom_query::Document::from(
                format!("<html><body>{body}</body></html>").as_str(),
            ))
        };
        let nested = shape("<h1>a</h1><h2>b<div><h2>c</h2></div></h2><h3>d</h3><p>e</p>");
        assert_eq!(nested.headings, 3);

        let sections: String = (0..2_000)
            .map(|i| format!("<h2>Part {i}</h2><p>Text of part {i}.</p>"))
            .collect();
        let page = shape(&sections);
        assert_eq!(page.headings, 2_000);
        // Real titles are under 200 bytes.
        assert_eq!(page.over(&ARTICLE_LIMITS, 0), None);
        assert_eq!(page.over(&ARTICLE_LIMITS, 200), None);
        assert_eq!(page.over(&ARTICLE_LIMITS, 1_000), None);
        assert_eq!(page.over(&ARTICLE_LIMITS, 5_000), Some(OverLimit::Work));
    }

    /// Each limit, set low, sends a page to the plain-text answer, made from what was parsed
    /// before the limit stopped the parser.
    #[test]
    fn each_limit_sends_a_page_to_the_plain_text_answer() {
        let paragraphs: String = (0..200)
            .map(|i| format!("<p>Paragraph {i} of the story.</p>"))
            .collect();
        let page =
            format!("<html><head><title>Story</title></head><body>{paragraphs}</body></html>");
        assert_eq!(extract_under(&page, &ARTICLE_LIMITS).1, None);

        let check = |limits: ExtractLimits, want: OverLimit| {
            let (a, over) = extract_under(&page, &limits);
            assert_eq!(over, Some(want));
            assert_eq!(a.title, "Story");
            assert!(
                a.content_html
                    .starts_with("<p>Paragraph 0 of the story.</p>\n<p>Paragraph 1 of"),
                "{}",
                a.content_html
            );
            a
        };
        // Only the first 2,000 bytes are parsed.
        let a = check(
            ExtractLimits {
                bytes: 2_000,
                ..ARTICLE_LIMITS
            },
            OverLimit::Bytes,
        );
        assert!(!a.content_text.contains("Paragraph 199"));
        let a = check(
            ExtractLimits {
                nodes: 50,
                ..ARTICLE_LIMITS
            },
            OverLimit::Nodes,
        );
        assert!(!a.content_text.contains("Paragraph 199"));
        check(
            ExtractLimits {
                parse_steps: 10,
                ..ARTICLE_LIMITS
            },
            OverLimit::ParseSteps,
        );
        // The deadline has passed by the first check, whatever the machine.
        check(
            ExtractLimits {
                parse_time: Duration::ZERO,
                ..ARTICLE_LIMITS
            },
            OverLimit::ParseTime,
        );
        // The work estimate is made on the whole page.
        let a = check(
            ExtractLimits {
                work: 1_000,
                ..ARTICLE_LIMITS
            },
            OverLimit::Work,
        );
        assert!(a.content_text.ends_with("\n\nParagraph 199 of the story."));
        assert_eq!(a.word_count, 1_000);
    }

    /// html5ever compares each attribute of a tag with the ones before it (one tag with 80,000
    /// attributes took 11 s to parse), and dom_query compares each attribute a repeated `<body>`
    /// adds with the ones the body has (100,000 of these took 12 s). Both count as parser work.
    #[test]
    fn attribute_comparisons_count_as_parser_work() {
        let one_tag = format!(
            "<p{}>x</p>",
            (0..500).map(|i| format!(" a{i}")).collect::<String>()
        );
        let bodies: String = (0..500).map(|i| format!("<body b{i}>")).collect();
        let plain = format!("<p>{}</p>", "x ".repeat(2_000));
        let over = |html: &str, parse_steps: u64| {
            parse_page(
                html,
                &ExtractLimits {
                    parse_steps,
                    ..ARTICLE_LIMITS
                },
            )
            .1
        };
        // 124,750 comparisons for the tag, 249,500 for the bodies.
        for html in [&one_tag, &bodies] {
            assert_eq!(over(html, 100_000), Some(OverLimit::ParseSteps));
            assert_eq!(over(html, 400_000), None);
        }
        assert_eq!(over(&plain, 100_000), None);
    }

    /// The plain-text answer: og:title or else `<title>`, the body's text in paragraphs split at
    /// block elements, without script, style and other non-text contents, and escaped.
    #[test]
    fn plain_text_answer_keeps_the_text_in_paragraphs() {
        let html = r#"<html><head><title>  Raw
  title </title><meta property="og:title" content=" The  OG title ">
<style>p { color: red }</style></head>
<body><script>var hidden = 1;</script><h1>Head<b>line</b></h1>
<div>First line<br>second line, a &lt; b &amp; c</div>
<ul><li>one</li><li>two <i>three</i></li></ul><noscript>Enable scripts</noscript>
<p>Last   paragraph<svg><text>chart</text></svg>.</p></body></html>"#;
        let a = plain_text_article(&dom_query::Document::from(html));
        assert_eq!(a.title, "The OG title");
        assert_eq!(
            a.content_text,
            "Headline\n\nFirst line\n\nsecond line, a < b & c\n\none\n\ntwo three\n\nLast \
             paragraph."
        );
        assert_eq!(
            a.content_html,
            "<p>Headline</p>\n<p>First line</p>\n<p>second line, a &lt; b &amp; c</p>\n\
             <p>one</p>\n<p>two three</p>\n<p>Last paragraph.</p>"
        );
        assert_eq!((a.word_count, a.reading_time_mins), (15, 1));

        // Without og:title, the <title>, whose contents are text even when they look like tags.
        let a = plain_text_article(&dom_query::Document::from(
            "<title>Only <b>title</b></title><p>x",
        ));
        assert_eq!(
            (a.title.as_str(), a.content_text.as_str()),
            ("Only <b>title</b>", "x")
        );
    }

    /// A reference that repeats the base's scheme without an authority (`https:img/a.png`) is
    /// relative in a browser, though on its own it parses as `https://img/a.png`.
    #[test]
    fn same_scheme_references_without_an_authority_are_relative() {
        let base = Url::parse("https://gazette.example/news/2026/ferry.html").unwrap();
        let resolve = |v: &str| resolve_url(&base, v);
        assert_eq!(
            resolve("https:img/map.png").as_deref(),
            Some("https://gazette.example/news/2026/img/map.png")
        );
        assert_eq!(
            resolve("HTTPS:/img/map.png").as_deref(),
            Some("https://gazette.example/img/map.png")
        );
        assert_eq!(
            resolve("//cdn.example/map.png").as_deref(),
            Some("https://cdn.example/map.png")
        );
        // These mean the same against any base, so they are kept as written.
        for absolute in [
            "http:img/map.png",
            "https://cdn.example/map.png",
            "mailto:desk@gazette.example",
            "data:image/png;base64,AAAA",
        ] {
            assert_eq!(resolve(absolute), None, "{absolute}");
        }

        let page = FERRY
            .replace(
                r#"href="httpd-notes.html""#,
                r#"href="https:httpd-notes.html""#,
            )
            .replace(
                r#"src="img/terminal at dawn.jpg""#,
                r#"src="https:img/terminal.jpg""#,
            );
        let a = extract(&page, "https://gazette.example/news/2026/ferry.html").unwrap();
        assert_contains_all(
            &a.content_html,
            &[
                r#"href="https://gazette.example/news/2026/httpd-notes.html""#,
                r#"src="https://gazette.example/news/2026/img/terminal.jpg""#,
            ],
        );
    }

    /// Long pages of the kind the endpoint is for stay well within the limits and are extracted
    /// by Readability. (The real pages measured are listed in `ARTICLE_LIMITS`.)
    #[test]
    fn long_realistic_pages_are_extracted_normally() {
        let news = fixtures::long_news_page();
        let encyclopedia = fixtures::long_encyclopedia_article();
        for (html, url, start, end, furniture) in [
            (
                &news,
                "https://gazette.example/news/2026/09/ferry-schedule",
                "STORY-START",
                "STORY-END",
                &[
                    "Accept all",
                    "reader59",
                    "/most-read/",
                    "ads.example.net",
                    "/footer/",
                ][..],
            ),
            (
                &encyclopedia,
                "https://en.example.org/wiki/Harbor_ferry_service",
                "ARTICLE-START",
                "ARTICLE-END",
                &[
                    "/wiki/Special/",
                    "/wiki/Nav0_0/",
                    "/wiki/Footer/",
                    "mw.config",
                ][..],
            ),
        ] {
            assert!(html.len() > 250_000, "{}", html.len());
            let shape = page_shape(&dom_query::Document::from(html.as_str()));
            assert!(
                shape.work < ARTICLE_LIMITS.work / 4,
                "{url}: {shape:?}, limit {}",
                ARTICLE_LIMITS.work
            );
            let (a, over) = extract_page(html, &Url::parse(url).unwrap(), &ARTICLE_LIMITS).unwrap();
            assert_eq!(over, None, "{url}");
            assert_contains_all(&a.content_html, &[start, end]);
            assert_absent(&a.content_html, furniture);
        }
    }

    /// Extractions share one slot. A request that stops waiting leaves its extraction running,
    /// and that extraction keeps the slot until it ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn extractions_run_one_at_a_time() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        let slots = Arc::new(Semaphore::new(EXTRACTIONS_AT_ONCE));
        let running = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..6)
            .map(|_| {
                let (slots, running, most) = (slots.clone(), running.clone(), most.clone());
                tokio::spawn(async move {
                    run_blocking_in_slot(&slots, move || {
                        most.fetch_max(running.fetch_add(1, SeqCst) + 1, SeqCst);
                        std::thread::sleep(Duration::from_millis(10));
                        running.fetch_sub(1, SeqCst);
                    })
                    .await
                    .unwrap()
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(most.load(SeqCst), 1);

        // The caller goes away while its work runs.
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let caller = tokio::spawn({
            let slots = slots.clone();
            async move {
                run_blocking_in_slot(&slots, move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
            }
        });
        tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
            .await
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available_permits(), 0);
        release_tx.send(()).unwrap();
        let _slot = slots.acquire().await.unwrap();
    }

    /// The body is read up to one byte past the limit, enough to tell that the page is over it.
    #[tokio::test]
    async fn read_page_stops_one_byte_past_the_limit() {
        use axum::routing::get;

        let app = axum::Router::new()
            .route("/big", get(|| async { "x".repeat(100_000) }))
            .route("/small", get(|| async { "y".repeat(500) }));
        let origin = crate::readlater::test_support::spawn_server(app).await;
        let client = reqwest::Client::new();
        let read = |path: &'static str, limit: usize| {
            let request = client.get(format!("{origin}{path}")).send();
            async move { read_page(request.await.unwrap(), limit).await.unwrap() }
        };
        assert_eq!(read("/big", 1_000).await, "x".repeat(1_001));
        assert_eq!(read("/small", 1_000).await, "y".repeat(500));
        assert_eq!(read("/small", 500).await, "y".repeat(500));
    }

    /// A page over the limits is answered, not failed: title and plain text, and never a 500.
    #[tokio::test]
    async fn extract_article_answers_a_page_over_the_limits_with_its_plain_text() {
        use axum::response::Html;
        use axum::routing::get;

        let deep = format!(
            "<html><head><title>Deep</title></head><body><p>Before the nesting.</p>{}</body></html>",
            "<div>".repeat(5_000)
        );
        let app = axum::Router::new().route("/deep", get(move || async move { Html(deep) }));
        let origin = crate::readlater::test_support::spawn_server(app).await;

        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let manager = FeedManager::new(
            &tmp.path().join("feeds.db"),
            storage,
            &tmp.path().join("epub"),
        )
        .unwrap();
        let a = manager
            .extract_article(&format!("{origin}/deep"))
            .await
            .unwrap();
        assert_eq!(a.title, "Deep");
        assert_eq!(a.content_html, "<p>Before the nesting.</p>");
    }

    /// Both paths take the title from the same place, og:title before <title>.
    #[test]
    fn fallback_title_is_chosen_like_the_article_title() {
        let head = r#"<head><title>Raw title - Site</title>
<meta property="og:title" content="OG Title"></head>"#;
        let url = "https://e.example/post";
        let empty = extract(&format!("<html>{head}<body></body></html>"), url).unwrap();
        let text = extract(
            &format!("<html>{head}<body><p>Some text here.</p></body></html>"),
            url,
        )
        .unwrap();
        assert_eq!(empty.content_html, "");
        assert_eq!(text.word_count, 3);
        assert_eq!(
            (empty.title.as_str(), text.title.as_str()),
            ("OG Title", "OG Title")
        );
    }

    /// Links resolve against where the page was served from after redirects, not against the
    /// URL that was asked for.
    #[tokio::test]
    async fn extract_article_resolves_links_against_the_redirect_target() {
        use axum::http::{StatusCode, header};
        use axum::response::Html;
        use axum::routing::get;

        let app = axum::Router::new()
            .route(
                "/s/ferry",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, "/news/2026/ferry.html")],
                    )
                }),
            )
            .route("/news/2026/ferry.html", get(|| async { Html(FERRY) }));
        let origin = crate::readlater::test_support::spawn_server(app).await;

        let tmp = tempfile::TempDir::new().unwrap();
        let storage = Storage::new(tmp.path().join("storage")).unwrap();
        let manager = FeedManager::new(
            &tmp.path().join("feeds.db"),
            storage,
            &tmp.path().join("epub"),
        )
        .unwrap();
        let a = manager
            .extract_article(&format!("{origin}/s/ferry"))
            .await
            .unwrap();
        assert_eq!(a.title, "Ferry schedule approved");
        assert_contains_all(
            &a.content_html,
            &[
                &format!(r#"href="{origin}/news/2026/httpd-notes.html""#),
                &format!(r#"src="{origin}/news/2026/img/terminal%20at%20dawn.jpg""#),
            ],
        );
        assert!(!a.content_html.contains("/s/"), "{}", a.content_html);
    }
    /// Long realistic pages, built in code rather than kept as megabytes of fixture files.
    mod fixtures {
        /// Deterministic filler text: `n` words cycled from a fixed list, with a comma now and then
        /// and a full stop every twelfth word.
        fn words(seed: usize, n: usize) -> String {
            const W: &[&str] = &[
                "harbor",
                "council",
                "ferry",
                "timetable",
                "commuters",
                "mainland",
                "island",
                "terminal",
                "weekday",
                "crossing",
                "schedule",
                "evening",
                "sailing",
                "operator",
                "passengers",
                "vote",
                "budget",
                "season",
                "service",
                "morning",
                "residents",
                "workers",
                "report",
                "change",
                "route",
                "vessel",
                "summer",
                "winter",
                "fares",
                "survey",
                "hearing",
                "plan",
            ];
            let mut s = String::new();
            for i in 0..n {
                let w = W[(seed * 7 + i * 13 + i / 5) % W.len()];
                if i % 12 == 0 {
                    let mut c = w.chars();
                    s.extend(c.next().map(|f| f.to_ascii_uppercase()));
                    s.push_str(c.as_str());
                } else {
                    s.push_str(w);
                }
                s.push_str(match i % 12 {
                    11 => ". ",
                    5 => ", ",
                    _ => " ",
                });
            }
            s.trim_end().to_string()
        }

        fn links(prefix: &str, n: usize) -> String {
            (0..n)
                .map(|i| {
                    format!(
                        r#"<li><a href="/{prefix}/{i}">{} {i}</a></li>"#,
                        words(i, 2)
                    )
                })
                .collect()
        }

        /// A long news story as big news sites serve it, about 400 KB: 2,000 words of story with
        /// figures and pull quotes inside a page mostly made of scripts, styles, a mega-menu, a right
        /// rail, 60 nested comments and a large footer.
        pub(super) fn long_news_page() -> String {
            let mut h = String::from(
                r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
        <title>Harbor Council Approves New Ferry Schedule | The Coastal Gazette</title>
        <meta property="og:title" content="Harbor Council Approves New Ferry Schedule">"#,
            );
            for i in 0..40 {
                h += &format!(r#"<meta name="x-meta-{i}" content="{}">"#, words(i, 6));
            }
            h += "<style>";
            for i in 0..600 {
                h += &format!(
                    ".c{i} {{ margin: {i}px; padding: 0 {i}px; color: #{:06x}; }}\n",
                    i * 997
                );
            }
            h += "</style><script>window.__STATE__ = {\"stories\": [";
            for i in 0..500 {
                h += &format!(
                    r#"{{"id": {i}, "headline": "{}", "url": "/news/{i}", "tags": ["a", "b"]}},"#,
                    words(i, 18)
                );
            }
            h += "{}]};</script></head><body>";
            h += r#"<div class="cookie-banner">We use cookies. <button>Accept all</button></div>"#;
            h += r#"<header class="site-header"><nav class="mega-menu"><ul>"#;
            for s in 0..20 {
                h += &format!(
                    r#"<li class="menu-section"><div class="menu-panel"><div class="menu-col"><ul>{}</ul></div></div></li>"#,
                    links(&format!("section{s}"), 15)
                );
            }
            h += "</ul></nav></header>";
            h += r#"<div class="page"><div class="layout"><main class="main-col"><article class="story">
        <h1>Harbor Council Approves New Ferry Schedule</h1><p class="byline">By Dana Reyes</p>
        <div class="story-body">"#;
            for i in 0..48 {
                if i == 0 {
                    h += "<p>STORY-START The Port Ellis harbor council voted five to two on Tuesday \
                          night to adopt a new ferry timetable.</p>";
                }
                h += &format!(
                    r#"<p>{} <a href="/topics/{i}">{}</a> {}</p>"#,
                    words(i, 20),
                    words(i + 100, 2),
                    words(i + 200, 20)
                );
                if i % 12 == 5 {
                    h += &format!(
                        r#"<figure><img src="photos/ferry-{i}.jpg" srcset="photos/ferry-{i}-2x.jpg 2x" alt="Ferry"><figcaption>{}</figcaption></figure>"#,
                        words(i, 12)
                    );
                }
                if i % 16 == 9 {
                    h += &format!("<blockquote><p>{}</p></blockquote>", words(i, 25));
                }
                if i == 24 {
                    h += &format!(
                        r#"<aside class="related-inline"><h3>Related</h3><ul>{}</ul></aside>"#,
                        links("related", 4)
                    );
                }
            }
            h += "<p>The new schedule takes effect on the first Monday of June. STORY-END</p>";
            h += "</div></article>";
            h += r#"<section class="comments"><h2>Comments</h2>"#;
            for c in 0..60 {
                let depth = c % 6;
                h += &r#"<div class="comment-thread"><div class="comment">"#.repeat(depth + 1);
                h += &format!(
                    r#"<span class="author">reader{c}</span><p>{}</p><button>Reply</button>"#,
                    words(c, 30)
                );
                h += &"</div></div>".repeat(depth + 1);
            }
            h += "</section></main>";
            h += r#"<aside class="right-rail"><div class="most-read"><h3>Most read</h3><ol>"#;
            h += &links("most-read", 10);
            h += "</ol></div>";
            for a in 0..6 {
                h += &format!(
                    r#"<div class="ad-slot"><iframe src="https://ads.example.net/slot/{a}"></iframe></div>"#
                );
            }
            h += r#"<form class="newsletter"><input type="email"><button>Sign up</button></form></aside>"#;
            h += r#"</div></div><footer class="site-footer"><ul>"#;
            h += &links("footer", 100);
            h += "</ul><p>Copyright 2026 The Coastal Gazette</p></footer>";
            for i in 0..40 {
                h += &format!(
                    "<script>(function(){{var t{i} = {:?}; window.dataLayer = window.dataLayer || []; }})();</script>",
                    words(i, 150)
                );
            }
            h += "</body></html>";
            h
        }

        /// A long encyclopedia article shaped like Wikipedia's, about 1.3 MB: 11,000 words in 50
        /// sections with inline MathML, citations after most sentences, tables, an infobox, 600
        /// references, navigation boxes and sidebars.
        pub(super) fn long_encyclopedia_article() -> String {
            let math = |i: usize| {
                format!(
                    r#"<span class="mwe-math-element"><span class="mwe-math-mathml-inline mwe-math-mathml-a11y"><math xmlns="http://www.w3.org/1998/Math/MathML"><semantics><mrow class="MJX-TeXAtom-ORD"><mstyle displaystyle="true"><mi>f</mi><mo stretchy="false">(</mo><mi>x</mi><mo>)</mo><mo>=</mo><munderover><mo>&#x2211;</mo><mrow><mi>n</mi><mo>=</mo><mn>0</mn></mrow><mi mathvariant="normal">&#x221E;</mi></munderover><mfrac><msup><mi>x</mi><mrow><mi>n</mi><mo>+</mo><mn>{i}</mn></mrow></msup><mrow><mi>n</mi><mo>!</mo></mrow></mfrac></mstyle></mrow><annotation encoding="application/x-tex">f(x)=\sum x^{{n+{i}}}/n!</annotation></semantics></math></span><img src="https://wikimedia.org/api/rest_v1/media/math/render/svg/{i:x}" class="mwe-math-fallback-image-inline" alt="f(x)"></span>"#
                )
            };
            let mut h = String::from(
                r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
        <title>Harbor ferry service - Encyclopedia</title>"#,
            );
            h += "<script>";
            for i in 0..300 {
                h += &format!("mw.config.set({{\"wgKey{i}\": {:?}}});\n", words(i, 20));
            }
            h += "</script></head><body class=\"skin-vector\">";
            h += r#"<div class="vector-header"><nav class="vector-main-menu"><ul>"#;
            h += &links("wiki/Special", 40);
            h += "</ul></nav></div>";
            h += r#"<div class="mw-page-container"><div class="mw-page-container-inner"><div class="vector-sitenotice-container"></div>"#;
            h += r#"<nav class="vector-toc"><ul>"#;
            for s in 0..50 {
                h += &format!(
                    r##"<li class="vector-toc-list-item"><a href="#s{s}">{}</a></li>"##,
                    words(s, 3)
                );
            }
            h += "</ul></nav>";
            h += r#"<main id="content" class="mw-body"><h1 id="firstHeading">Harbor ferry service</h1>
        <div id="bodyContent" class="vector-body"><div id="mw-content-text" class="mw-body-content"><div class="mw-content-ltr mw-parser-output" lang="en" dir="ltr">"#;
            h += r#"<table class="infobox"><tbody>"#;
            for r in 0..30 {
                h += &format!(
                    r#"<tr><th scope="row" class="infobox-label">{}</th><td class="infobox-data">{}</td></tr>"#,
                    words(r, 2),
                    words(r + 50, 5)
                );
            }
            h += "</tbody></table>";
            let mut cite = 0;
            for s in 0..50 {
                h += &format!(
                    r#"<div class="mw-heading mw-heading2"><h2 id="s{s}">{}</h2><span class="mw-editsection"><a href="/w/edit?section={s}">edit</a></span></div>"#,
                    words(s, 3)
                );
                for p in 0..8 {
                    h += "<p>";
                    if s == 0 && p == 0 {
                        h += "ARTICLE-START ";
                    }
                    for sentence in 0..3 {
                        cite += 1;
                        h += &words(s * 31 + p * 7 + sentence, 9);
                        if (s + p + sentence) % 4 == 0 {
                            h += &format!(" where {} holds, ", math(cite));
                        }
                        h += &format!(
                            r##" <a href="/wiki/{}">{}</a>.<sup id="cite_ref-{cite}" class="reference"><a href="#cite_note-{cite}"><span class="cite-bracket">[</span>{cite}<span class="cite-bracket">]</span></a></sup> "##,
                            words(cite, 1),
                            words(cite + 3, 2)
                        );
                    }
                    if s == 49 && p == 7 {
                        h += "ARTICLE-END";
                    }
                    h += "</p>";
                }
                if s % 5 == 2 {
                    h += r#"<table class="wikitable sortable"><tbody><tr><th>Year</th><th>Route</th><th>Crossings</th><th>Passengers</th><th>Notes</th></tr>"#;
                    for r in 0..10 {
                        h += &format!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            1990 + r,
                            words(r, 2),
                            r * 37,
                            r * 1301,
                            words(r + s, 4)
                        );
                    }
                    h += "</tbody></table>";
                }
            }
            h += r#"<div class="mw-heading mw-heading2"><h2 id="References">References</h2></div><div class="reflist"><div class="mw-references-wrap mw-references-columns"><ol class="references">"#;
            for c in 1..=600 {
                h += &format!(
                    r##"<li id="cite_note-{c}"><span class="mw-cite-backlink"><b><a href="#cite_ref-{c}">^</a></b></span> <span class="reference-text"><cite class="citation book cs1">{}. <a class="external text" href="https://books.example/{c}"><i>{}</i></a>. Port Ellis Press. p.&nbsp;{c}.</cite></span></li>"##,
                    words(c, 3),
                    words(c + 9, 5)
                );
            }
            h += "</ol></div></div>";
            for n in 0..3 {
                h += r#"<div role="navigation" class="navbox"><table class="nowraplinks navbox-inner"><tbody>"#;
                for g in 0..8 {
                    h += &format!(
                        r#"<tr><th scope="row" class="navbox-group">{}</th><td class="navbox-list"><div><ul>{}</ul></div></td></tr>"#,
                        words(g + n, 2),
                        links(&format!("wiki/Nav{n}_{g}"), 25)
                    );
                }
                h += "</tbody></table></div>";
            }
            h += "</div></div></div></main></div></div>";
            h += r#"<footer id="footer"><ul>"#;
            h += &links("wiki/Footer", 30);
            h += "</ul></footer></body></html>";
            h
        }
    }
}
