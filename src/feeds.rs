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

use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
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
    scheduler_tx: Option<mpsc::Sender<SchedulerCommand>>,
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
            scheduler_tx: None,
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
        let response = self
            .http_client
            .get(url)
            .send()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to fetch article: {}", e)))?;

        let html = response
            .text()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to read article: {}", e)))?;

        // Use readability to extract main content
        let extracted =
            readability::extractor::extract(&mut html.as_bytes(), &url.parse().unwrap())
                .map_err(|e| ServerError::Internal(format!("Failed to extract article: {}", e)))?;

        let text = strip_html(&extracted.content);
        let word_count = text.split_whitespace().count() as u32;

        Ok(ExtractedArticle {
            title: extracted.title,
            content_html: extracted.content,
            content_text: text,
            word_count,
            reading_time_mins: (word_count / 200).max(1),
        })
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

    /// Sync articles as EPUBs to device folder
    pub fn sync_articles_to_folder(&self, folder: &str) -> Result<u32> {
        let articles = self.list_articles(ArticleQuery {
            unsynced: Some(true),
            ..Default::default()
        })?;

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
            let (doc_id, _) = crate::documents::create_document(
                &self.storage,
                &article.title,
                "epub",
                &epub_data,
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
    let mut in_script = false;

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
    let synced = state.manager.sync_articles_to_folder(&sub.folder)?;
    Ok(Json(serde_json::json!({ "synced": synced })))
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
