//! Read-later sync: puts new articles from each account's provider on the tablet, on a
//! schedule ([`ReadLaterSyncer::spawn_scheduler`]) and on demand (`POST
//! /integrations/v2/readlater/sync`, `.../accounts/{id}/sync`).
//!
//! One account's sync:
//! 1. refresh the credentials, persisting new tokens at once (the old ones may have been
//!    rotated out), then fetch the articles changed since the account's `last_sync`;
//! 2. apply the account's filters and `max_articles`, record the articles, and skip the ones
//!    already on the device (`synced_to_device`);
//! 3. render each new article (EPUB or PDF) and add it to the sync tree with
//!    [`documents::create_document_in`], inside `folder_id` (resolved or created by
//!    [`documents::ensure_folder`]; none = top level). Both refuse a root index they don't
//!    fully understand, so the tablet's library is never rewritten lossily. The article is
//!    marked delivered right after its document is committed, on the same blocking thread;
//! 4. if the root changed, tell connected devices to pull it (SyncComplete);
//! 5. push read/archived status back to the provider (`sync_read_status`);
//! 6. only if the fetch and every delivery succeeded, advance `last_sync` to when the fetch
//!    started. Otherwise the next sync fetches the same window again and retries what is
//!    missing; delivered articles are skipped, never added twice.
//!
//! Accounts with `convert_format: html` are recorded but not delivered: the tablet opens only
//! PDF and EPUB.
//!
//! At most one sync of an account runs at a time. The scheduler syncs enabled `auto_sync`
//! accounts every `sync_interval_minutes` (at least [`MIN_INTERVAL_MINUTES`]), one after
//! another, and backs off an account whose syncs keep failing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::documents;
use crate::notifications::WsMessage;
use crate::readlater::{
    Article,
    ArticleContent,
    ArticleConverter,
    ArticleFormat,
    ProviderAccount,
    ReadLaterManager,
    ReadLaterProvider,
    ReadLaterProviderTrait,
    SyncResult,
    provider_for,
    select_articles_for_sync,
};
use crate::storage::Storage;

/// Account whose devices are told about new documents: the single local account (the one
/// `--pair` issues codes for; feed syncs notify it too).
const DEVICE_USER: &str = "local-user";
/// `sourceDeviceID` of the SyncComplete sent after a sync, as for other server-side writes.
const SOURCE_DEVICE: &str = "local-server";
/// Shortest time between scheduled syncs of one account, whatever it is configured to.
pub const MIN_INTERVAL_MINUTES: i64 = 5;
/// Longest the scheduler waits to retry an account whose syncs keep failing (unless its own
/// interval is longer).
const MAX_BACKOFF_HOURS: i64 = 24;
/// Default time between scheduler passes looking for due accounts.
const DEFAULT_TICK_SECS: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("read-later account not found: {0}")]
    AccountNotFound(String),
    #[error("a sync of read-later account {0} is already running")]
    AlreadyRunning(String),
}

/// Results of syncing every enabled account.
#[derive(Debug, Default, Serialize)]
pub struct SyncAllReport {
    pub results: Vec<SyncResult>,
    /// Accounts skipped because a sync of them was already running.
    pub already_running: Vec<String>,
}

/// Scheduler settings from the environment: `READLATER_AUTO_SYNC=0|false|off|no` turns
/// scheduled syncs off (`POST .../sync` still works), and `READLATER_SYNC_TICK_SECS` is how
/// often due accounts are looked for (default 60).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub enabled: bool,
    pub tick: std::time::Duration,
}

impl SchedulerConfig {
    pub fn from_env() -> Self {
        Self::from_vars(
            std::env::var("READLATER_AUTO_SYNC").ok().as_deref(),
            std::env::var("READLATER_SYNC_TICK_SECS").ok().as_deref(),
        )
    }

    fn from_vars(auto_sync: Option<&str>, tick_secs: Option<&str>) -> Self {
        let off = auto_sync.is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        });
        let tick = tick_secs
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&secs| secs > 0)
            .unwrap_or(DEFAULT_TICK_SECS);
        Self {
            enabled: !off,
            tick: std::time::Duration::from_secs(tick),
        }
    }
}

/// This process's memory of an account's latest sync attempt, for scheduling and backoff.
#[derive(Debug, Clone, Copy)]
struct Attempt {
    at: DateTime<Utc>,
    /// Consecutive attempts that ended with errors.
    failures: u32,
}

/// Runs read-later syncs into the device's sync tree. Shared by the scheduler and the sync
/// endpoints.
pub struct ReadLaterSyncer {
    manager: Arc<Mutex<ReadLaterManager>>,
    storage: Storage,
    notification_tx: broadcast::Sender<WsMessage>,
    /// Accounts with a sync in progress.
    running: Mutex<HashSet<String>>,
    attempts: Mutex<HashMap<String, Attempt>>,
}

/// An account's place in [`ReadLaterSyncer::running`], released however the sync ends
/// (including a panic or a dropped future).
struct Running<'a> {
    set: &'a Mutex<HashSet<String>>,
    id: String,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.set.lock().remove(&self.id);
    }
}

impl ReadLaterSyncer {
    pub fn new(
        manager: Arc<Mutex<ReadLaterManager>>,
        storage: Storage,
        notification_tx: broadcast::Sender<WsMessage>,
    ) -> Self {
        Self {
            manager,
            storage,
            notification_tx,
            running: Mutex::new(HashSet::new()),
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// Sync one account now, whether or not it is enabled or due. Fails only if the account
    /// doesn't exist or is already syncing; everything else is reported in the result.
    pub async fn sync_account(&self, account_id: &str) -> Result<SyncResult, SyncError> {
        self.sync_started_at(account_id, Utc::now()).await
    }

    /// [`sync_account`](Self::sync_account), recording the attempt for the scheduler as made
    /// at `started` (intervals run from one attempt's start to the next).
    async fn sync_started_at(
        &self,
        account_id: &str,
        started: DateTime<Utc>,
    ) -> Result<SyncResult, SyncError> {
        let _running = self.claim(account_id)?;
        let account = self
            .manager
            .lock()
            .get_account(account_id)
            .ok_or_else(|| SyncError::AccountNotFound(account_id.into()))?;
        let name = account.name.clone();
        let result = self.run(account).await;
        let failures = self.record_attempt(&result, started);
        if result.errors.is_empty() {
            tracing::info!(
                account = %name,
                provider = %result.provider,
                fetched = result.articles_fetched,
                synced = result.articles_synced,
                already_synced = result.articles_already_synced,
                "read-later sync done"
            );
        } else {
            tracing::warn!(
                account = %name,
                provider = %result.provider,
                fetched = result.articles_fetched,
                synced = result.articles_synced,
                failures_in_a_row = failures,
                errors = ?result.errors,
                "read-later sync had errors"
            );
        }
        Ok(result)
    }

    /// Sync every enabled account, one after another (oldest account first).
    pub async fn sync_all(&self) -> SyncAllReport {
        let mut accounts: Vec<ProviderAccount> = self
            .manager
            .lock()
            .list_accounts()
            .into_iter()
            .filter(|a| a.enabled)
            .collect();
        accounts.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        let mut report = SyncAllReport::default();
        for account in accounts {
            match self.sync_account(&account.id).await {
                Ok(result) => report.results.push(result),
                Err(SyncError::AlreadyRunning(id)) => report.already_running.push(id),
                // Deleted since it was listed.
                Err(SyncError::AccountNotFound(_)) => {}
            }
        }
        report
    }

    /// One scheduler pass: sync each account that is due at `now` (enabled, `auto_sync`, and
    /// its interval, stretched by backoff after failures, has passed), one after another, and
    /// return the results. With no accounts it returns at once, touching neither the network
    /// nor the sync tree.
    pub async fn run_due(&self, now: DateTime<Utc>) -> Vec<SyncResult> {
        let accounts = self.manager.lock().list_accounts();
        let due: Vec<String> = {
            let mut attempts = self.attempts.lock();
            attempts.retain(|id, _| accounts.iter().any(|a| a.id == *id));
            accounts
                .iter()
                .filter(|a| is_due(a, attempts.get(&a.id), now))
                .map(|a| a.id.clone())
                .collect()
        };
        let mut results = Vec::new();
        for id in due {
            match self.sync_started_at(&id, now).await {
                Ok(result) => results.push(result),
                Err(e) => tracing::debug!("scheduled read-later sync skipped: {e}"),
            }
        }
        results
    }

    /// Run [`run_due`](Self::run_due) every `tick` (which must be non-zero), the first time one
    /// tick after startup rather than while the server and the tablet reconnect. Passes never
    /// overlap.
    pub fn spawn_scheduler(
        self: Arc<Self>,
        tick: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + tick, tick);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                self.run_due(Utc::now()).await;
            }
        })
    }

    fn claim(&self, account_id: &str) -> Result<Running<'_>, SyncError> {
        if !self.running.lock().insert(account_id.to_owned()) {
            return Err(SyncError::AlreadyRunning(account_id.into()));
        }
        Ok(Running {
            set: &self.running,
            id: account_id.to_owned(),
        })
    }

    /// Remember this attempt for the scheduler; returns the account's failures in a row.
    fn record_attempt(&self, result: &SyncResult, at: DateTime<Utc>) -> u32 {
        let mut attempts = self.attempts.lock();
        let failures = if result.errors.is_empty() {
            0
        } else {
            attempts
                .get(&result.account_id)
                .map_or(0, |a| a.failures)
                .saturating_add(1)
        };
        attempts.insert(result.account_id.clone(), Attempt { at, failures });
        failures
    }

    async fn run(&self, mut account: ProviderAccount) -> SyncResult {
        let started = std::time::Instant::now();
        let mut result = SyncResult {
            account_id: account.id.clone(),
            provider: account.provider,
            articles_fetched: 0,
            articles_synced: 0,
            articles_converted: 0,
            articles_already_synced: 0,
            read_status_synced: 0,
            errors: Vec::new(),
            duration_ms: 0,
            completed_at: Utc::now(),
        };
        // A discontinued provider (Omnivore) is an error for this account only.
        match provider_for(account.provider) {
            Ok(provider) => {
                self.run_with(provider.as_ref(), &mut account, &mut result)
                    .await
            }
            Err(e) => result.errors.push(e.to_string()),
        }
        result.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        result.completed_at = Utc::now();
        result
    }

    async fn run_with(
        &self,
        provider: &dyn ReadLaterProviderTrait,
        account: &mut ProviderAccount,
        result: &mut SyncResult,
    ) {
        // Refresh once (a no-op unless the token is missing, expired or about to be) so the
        // calls below don't each refresh. A failed refresh ends the sync before anything else.
        match provider.refresh_auth(&account.config).await {
            Ok(config) => {
                let refreshed = provider.take_refreshed_config().is_some();
                account.config = config;
                if refreshed {
                    self.persist_config(account, &mut result.errors);
                }
            }
            Err(e) => {
                result.errors.push(format!("Auth refresh: {e}"));
                return;
            }
        }

        let window_start = Utc::now();
        let fetched = provider
            .fetch_articles(&account.config, account.last_sync)
            .await;
        self.absorb_refreshed(provider, account, &mut result.errors);
        let complete = match fetched {
            Ok(articles) => {
                result.articles_fetched = u32::try_from(articles.len()).unwrap_or(u32::MAX);
                self.deliver_new(provider, account, articles, result).await
            }
            Err(e) => {
                result.errors.push(format!("Fetch: {e}"));
                false
            }
        };

        if account.sync_settings.sync_read_status {
            self.push_read_status(provider, account, result).await;
        }

        if complete {
            let saved = self.manager.lock().set_last_sync(&account.id, window_start);
            if let Err(e) = saved {
                result.errors.push(format!("Save last sync: {e}"));
            }
        }
    }

    /// Adopt a token refresh the provider did inside its last call (e.g. a retry after a 401)
    /// and persist it at once.
    fn absorb_refreshed(
        &self,
        provider: &dyn ReadLaterProviderTrait,
        account: &mut ProviderAccount,
        errors: &mut Vec<String>,
    ) {
        if let Some(config) = provider.take_refreshed_config() {
            account.config = config;
            self.persist_config(account, errors);
        }
    }

    fn persist_config(&self, account: &ProviderAccount, errors: &mut Vec<String>) {
        let saved = self
            .manager
            .lock()
            .update_account_config(&account.id, &account.config);
        if let Err(e) = saved {
            errors.push(format!("Persist refreshed credentials: {e}"));
        }
    }

    /// Record the selected articles and put the ones not yet on the device there. Returns
    /// whether every selected article is now recorded and, unless the account's format is
    /// HTML, on the device.
    async fn deliver_new(
        &self,
        provider: &dyn ReadLaterProviderTrait,
        account: &mut ProviderAccount,
        articles: Vec<Article>,
        result: &mut SyncResult,
    ) -> bool {
        let mut complete = true;
        let mut pending = Vec::new();
        {
            let mut manager = self.manager.lock();
            for mut article in select_articles_for_sync(articles, &account.sync_settings) {
                // Content is fetched per article on delivery; don't keep it in memory.
                article.content = None;
                // Keeps the id and device state of an article seen before.
                match manager.upsert_article(&article) {
                    Ok(stored) if stored.synced_to_device => result.articles_already_synced += 1,
                    Ok(stored) => pending.push(stored),
                    Err(e) => {
                        result
                            .errors
                            .push(format!("Save {}: {e}", article.provider_id));
                        complete = false;
                    }
                }
            }
        }
        let format = account.sync_settings.convert_format;
        if pending.is_empty() || format == ArticleFormat::Html {
            return complete;
        }

        let folder = account.sync_settings.folder_id.clone().unwrap_or_default();
        let generation_before = self.storage.get_root().generation;
        let mut parent = None;
        for article in pending {
            let content = provider
                .fetch_article_content(&account.config, &article)
                .await;
            self.absorb_refreshed(provider, account, &mut result.errors);
            let content = match content {
                Ok(content) => content,
                Err(e) => {
                    result.errors.push(format!("Content {}: {e}", article.id));
                    complete = false;
                    continue;
                }
            };
            let delivery = Delivery {
                storage: self.storage.clone(),
                manager: Arc::clone(&self.manager),
                folder: folder.clone(),
                parent: parent.clone(),
                article,
                content,
                format,
            };
            let outcome = match tokio::task::spawn_blocking(move || delivery.run()).await {
                Ok(outcome) => outcome,
                Err(e) => {
                    result.errors.push(format!("Deliver: {e}"));
                    complete = false;
                    continue;
                }
            };
            result.articles_converted += u32::from(outcome.converted);
            result.articles_synced += u32::from(outcome.document_id.is_some());
            if outcome.parent.is_some() {
                parent = outcome.parent;
            }
            if let Some(failure) = outcome.failure {
                result.errors.push(failure.message);
                complete = false;
                // The tree refused the change (e.g. a root index we won't rewrite); the rest
                // would be refused the same way.
                if failure.stage == Stage::Tree {
                    break;
                }
            }
        }

        let root = self.storage.get_root();
        if root.generation != generation_before {
            // Tell connected devices to pull the new root, as document uploads do.
            let _ = self.notification_tx.send(WsMessage::sync_complete(
                root.generation,
                SOURCE_DEVICE,
                DEVICE_USER,
            ));
        }
        complete
    }

    async fn push_read_status(
        &self,
        provider: &dyn ReadLaterProviderTrait,
        account: &mut ProviderAccount,
        result: &mut SyncResult,
    ) {
        let articles = self.manager.lock().read_status_candidates(account.provider);
        for article in articles {
            let updated = provider
                .update_read_status(&account.config, &article.provider_id, article.status)
                .await;
            self.absorb_refreshed(provider, account, &mut result.errors);
            match updated {
                Ok(()) => result.read_status_synced += 1,
                Err(e) => result.errors.push(format!("Status {}: {e}", article.id)),
            }
        }
    }
}

/// Whether the scheduler should sync `account` at `now`: it is enabled with `auto_sync`, its
/// provider still exists, and its interval (at least [`MIN_INTERVAL_MINUTES`]) has passed since
/// this process last tried it — stretched by [`retry_after`] after failures — or, if not tried
/// yet, since its last successful sync.
fn is_due(account: &ProviderAccount, attempt: Option<&Attempt>, now: DateTime<Utc>) -> bool {
    if !account.enabled
        || !account.sync_settings.auto_sync
        || account.provider == ReadLaterProvider::Omnivore
    {
        return false;
    }
    let interval = Duration::minutes(
        i64::from(account.sync_settings.sync_interval_minutes).max(MIN_INTERVAL_MINUTES),
    );
    match attempt {
        Some(attempt) => now - attempt.at >= retry_after(interval, attempt.failures),
        None => account.last_sync.is_none_or(|last| now - last >= interval),
    }
}

/// Wait after an attempt: the interval, doubled for each failure in a row past the first,
/// capped at [`MAX_BACKOFF_HOURS`] (or the interval, if that is longer).
fn retry_after(interval: Duration, failures: u32) -> Duration {
    let cap = interval.num_minutes().max(MAX_BACKOFF_HOURS * 60);
    let doublings = failures.saturating_sub(1).min(16);
    Duration::minutes(
        interval
            .num_minutes()
            .saturating_mul(1 << doublings)
            .min(cap),
    )
}

/// The step of a [`Delivery`] that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Render,
    /// Resolving the folder or committing the document to the sync tree.
    Tree,
    Record,
}

struct Failure {
    stage: Stage,
    message: String,
}

#[derive(Default)]
struct Delivered {
    converted: bool,
    /// The folder's collection id, once resolved (reused for the rest of the sync).
    parent: Option<String>,
    document_id: Option<String>,
    failure: Option<Failure>,
}

impl Delivered {
    fn failed(mut self, stage: Stage, message: String) -> Self {
        self.failure = Some(Failure { stage, message });
        self
    }
}

/// One article's trip onto the device; blocking, so it runs on a blocking thread.
struct Delivery {
    storage: Storage,
    manager: Arc<Mutex<ReadLaterManager>>,
    folder: String,
    parent: Option<String>,
    article: Article,
    content: ArticleContent,
    format: ArticleFormat,
}

impl Delivery {
    /// Render the article, resolve the folder (once per sync), commit the document, and mark
    /// the article delivered. Nothing awaits between the commit and the mark, and a blocking
    /// task runs to completion even if the sync that started it is dropped, so a committed
    /// document is recorded unless the process dies or the database write fails (reported).
    fn run(self) -> Delivered {
        let mut out = Delivered::default();
        let id = &self.article.id;
        let rendered = match ArticleConverter::render(&self.article, &self.content, self.format) {
            Ok(rendered) => rendered,
            Err(e) => return out.failed(Stage::Render, format!("Convert {id}: {e}")),
        };
        out.converted = true;
        let parent = match self.parent {
            Some(parent) => parent,
            None => match documents::ensure_folder(&self.storage, &self.folder) {
                Ok(parent) => parent,
                Err(e) => {
                    return out.failed(Stage::Tree, format!("Folder {:?}: {e}", self.folder));
                }
            },
        };
        out.parent = Some(parent.clone());
        let created = documents::create_document_in(
            &self.storage,
            document_name(&self.article),
            rendered.ext,
            &rendered.bytes,
            &parent,
        );
        let (document_id, generation) = match created {
            Ok(created) => created,
            Err(e) => return out.failed(Stage::Tree, format!("Add {id} to the device: {e}")),
        };
        tracing::info!(
            article = %id,
            document = %document_id,
            generation,
            title = %self.article.title,
            "read-later article added to the device"
        );
        out.document_id = Some(document_id.clone());
        let marked = self.manager.lock().mark_delivered(id, &document_id);
        if let Err(e) = marked {
            tracing::error!(
                article = %id,
                document = %document_id,
                "document committed but not recorded as delivered; a later sync may add it again: {e}"
            );
            return out.failed(Stage::Record, format!("Record {id} as delivered: {e}"));
        }
        out
    }
}

/// The document's visible name: the article title, or its URL when the title is blank.
fn document_name(article: &Article) -> &str {
    let title = article.title.trim();
    if title.is_empty() {
        &article.url
    } else {
        title
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use axum::Json;
    use axum::body::Body;
    use axum::extract::{Path as UrlPath, State};
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::response::Response;
    use axum::routing::{get, post};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::readlater::test_support::{spawn_server, test_account, wallabag_config};
    use crate::readlater::{ProviderConfig, SyncSettings};
    use crate::readlater_api::{ReadLaterState, readlater_router};

    /// A Wallabag instance. `/api/entries.json` lists `entries` whatever `since` says, so every
    /// sync sees them all again; `/api/entries/{id}.json` serves one entry's content.
    #[derive(Default)]
    struct MockWallabag {
        entries: Mutex<Vec<Value>>,
        /// Access token the API accepts, and the token endpoint issues.
        valid_token: String,
        fail_list: AtomicBool,
        /// Entry ids whose content request fails.
        fail_content: Mutex<HashSet<i64>>,
        list_calls: AtomicUsize,
        content_calls: AtomicUsize,
        token_requests: Mutex<Vec<HashMap<String, String>>>,
        /// When set, a list request signals `listing`, then waits for a permit.
        gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
        listing: tokio::sync::Notify,
    }

    impl MockWallabag {
        fn authorized(&self, headers: &HeaderMap) -> bool {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v == format!("Bearer {}", self.valid_token))
        }
    }

    async fn list_entries(
        State(m): State<Arc<MockWallabag>>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, StatusCode> {
        if !m.authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        m.list_calls.fetch_add(1, Ordering::SeqCst);
        let gate = m.gate.lock().clone();
        if let Some(gate) = gate {
            m.listing.notify_one();
            let _permit = gate.acquire().await.unwrap();
        }
        if m.fail_list.load(Ordering::SeqCst) {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        let items = m.entries.lock().clone();
        Ok(Json(json!({"_embedded": {"items": items}})))
    }

    async fn get_entry(
        State(m): State<Arc<MockWallabag>>,
        headers: HeaderMap,
        UrlPath(file): UrlPath<String>,
    ) -> Result<Json<Value>, StatusCode> {
        if !m.authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let id: i64 = file
            .strip_suffix(".json")
            .and_then(|id| id.parse().ok())
            .ok_or(StatusCode::NOT_FOUND)?;
        m.content_calls.fetch_add(1, Ordering::SeqCst);
        if m.fail_content.lock().contains(&id) {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Ok(Json(json!({"content": format!("<p>Body of {id}</p>")})))
    }

    async fn issue_token(
        State(m): State<Arc<MockWallabag>>,
        axum::Form(form): axum::Form<HashMap<String, String>>,
    ) -> Json<Value> {
        m.token_requests.lock().push(form);
        Json(json!({
            "access_token": m.valid_token,
            "refresh_token": "rotated-refresh",
            "expires_in": 3600,
            "token_type": "bearer",
        }))
    }

    /// Entry `id` (1..=28), added on day `id` of January 2025 (so higher ids are newer).
    fn entry(id: i64) -> Value {
        let at = format!("2025-01-{id:02}T00:00:00+00:00");
        json!({
            "id": id, "url": format!("https://ex.com/{id}"), "title": format!("Article {id}"),
            "content": format!("<p>{id}</p>"), "reading_time": 1, "is_archived": 0,
            "is_starred": 0, "tags": [], "created_at": at, "updated_at": at,
            "preview_picture": null
        })
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        db: std::path::PathBuf,
        storage: Storage,
        state: ReadLaterState,
        rx: broadcast::Receiver<WsMessage>,
        mock: Arc<MockWallabag>,
        base: String,
    }

    impl Fixture {
        async fn new(entries: Vec<Value>) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("rl.db");
            let storage = Storage::new(dir.path().join("storage")).unwrap();
            let (tx, rx) = broadcast::channel(16);
            let state =
                ReadLaterState::new(ReadLaterManager::new(&db).unwrap(), storage.clone(), tx);
            let mock = Arc::new(MockWallabag {
                entries: Mutex::new(entries),
                valid_token: "tok".into(),
                ..Default::default()
            });
            let app = axum::Router::new()
                .route("/oauth/v2/token", post(issue_token))
                .route("/api/entries.json", get(list_entries))
                .route("/api/entries/{file}", get(get_entry))
                .with_state(Arc::clone(&mock));
            let base = spawn_server(app).await;
            Self {
                _dir: dir,
                db,
                storage,
                state,
                rx,
                mock,
                base,
            }
        }

        fn syncer(&self) -> &ReadLaterSyncer {
            &self.state.syncer
        }

        /// Add a Wallabag account with a valid token; `settings` adjusts its sync settings.
        fn add_account(&self, id: &str, settings: impl FnOnce(&mut SyncSettings)) {
            let config = wallabag_config(
                &self.base,
                Some("tok"),
                None,
                Some(Utc::now() + Duration::hours(1)),
            );
            let mut account = test_account(id, ReadLaterProvider::Wallabag, config, None);
            settings(&mut account.sync_settings);
            self.state.manager.lock().add_account(account).unwrap();
        }

        fn account(&self, id: &str) -> ProviderAccount {
            self.state.manager.lock().get_account(id).unwrap()
        }

        fn articles(&self) -> Vec<Article> {
            self.state
                .manager
                .lock()
                .query_articles(&Default::default())
        }

        /// Notifications sent to devices since the last call.
        fn pushes(&mut self) -> Vec<WsMessage> {
            std::iter::from_fn(|| self.rx.try_recv().ok()).collect()
        }

        /// Visible names of the documents in the current root, sorted.
        fn document_names(&self) -> Vec<String> {
            let mut names: Vec<String> = tree(&self.storage)
                .into_iter()
                .filter(|n| n.kind == "DocumentType")
                .map(|n| n.name)
                .collect();
            names.sort();
            names
        }
    }

    #[derive(Debug)]
    struct Node {
        id: String,
        name: String,
        kind: String,
        parent: String,
        files: Vec<String>,
    }

    /// Every node in the current root, with its metadata and file names.
    fn tree(storage: &Storage) -> Vec<Node> {
        let root = storage.get_root();
        if root.hash.is_empty() {
            return Vec::new();
        }
        let entries = |hash: &str| -> Vec<Vec<String>> {
            String::from_utf8(storage.get(hash).unwrap())
                .unwrap()
                .lines()
                .skip(1)
                .map(|l| l.split(':').map(String::from).collect())
                .collect()
        };
        entries(&root.hash)
            .into_iter()
            .map(|node| {
                let files = entries(&node[0]);
                let meta = files
                    .iter()
                    .find(|f| f[2] == format!("{}.metadata", node[2]))
                    .unwrap();
                let m: Value = serde_json::from_slice(&storage.get(&meta[0]).unwrap()).unwrap();
                Node {
                    id: node[2].clone(),
                    name: m["visibleName"].as_str().unwrap().into(),
                    kind: m["type"].as_str().unwrap().into(),
                    parent: m["parent"].as_str().unwrap().into(),
                    files: files.iter().map(|f| f[2].clone()).collect(),
                }
            })
            .collect()
    }

    fn post_request(uri: &str) -> Request<Body> {
        Request::post(uri).body(Body::empty()).unwrap()
    }

    async fn json_body(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// A scheduler pass syncs a due account: each article becomes an EPUB document in the
    /// configured folder and is recorded as delivered, `last_sync` advances, and devices get
    /// one SyncComplete for the local account. The account isn't due again until its interval
    /// has passed.
    #[tokio::test]
    async fn scheduler_pass_puts_new_articles_on_the_device() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |s| s.folder_id = Some("Read Later".into()));
        let before = Utc::now();

        let results = f.syncer().run_due(Utc::now()).await;
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.account_id, "wb");
        assert_eq!(
            (r.articles_fetched, r.articles_converted, r.articles_synced),
            (2, 2, 2)
        );

        let tree = tree(&f.storage);
        let folder = tree
            .iter()
            .find(|n| n.kind == "CollectionType")
            .expect("folder created");
        assert_eq!(
            (folder.name.as_str(), folder.parent.as_str()),
            ("Read Later", "")
        );
        let docs: Vec<&Node> = tree.iter().filter(|n| n.kind == "DocumentType").collect();
        assert_eq!(docs.len(), 2);
        for doc in &docs {
            assert_eq!(doc.parent, folder.id, "document inside the folder");
            assert!(
                doc.files.contains(&format!("{}.epub", doc.id)),
                "{:?}",
                doc.files
            );
        }
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);

        let articles = f.articles();
        assert_eq!(articles.len(), 2);
        for a in &articles {
            assert!(a.synced_to_device);
            assert!(docs.iter().any(|d| a.document_id.as_ref() == Some(&d.id)));
        }
        let last_sync = f.account("wb").last_sync.expect("last_sync advanced");
        assert!(before <= last_sync && last_sync <= Utc::now());

        let pushes = f.pushes();
        assert_eq!(pushes.len(), 1);
        let attrs = &pushes[0].message.attributes;
        assert_eq!(
            (attrs.event.as_str(), attrs.auth0_user_id.as_str()),
            ("SyncComplete", "local-user")
        );

        assert!(
            f.syncer().run_due(Utc::now()).await.is_empty(),
            "not due yet"
        );
        assert_eq!(f.mock.list_calls.load(Ordering::SeqCst), 1);
        let later = f.syncer().run_due(Utc::now() + Duration::minutes(61)).await;
        assert_eq!(later.len(), 1, "due again after its 60 minutes");
        assert_eq!(later[0].articles_synced, 0);
    }

    /// Nothing new: articles already on the device are skipped (no content fetched, no
    /// document added), the root is untouched and no device is notified.
    #[tokio::test]
    async fn sync_with_nothing_new_is_a_no_op() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        let first = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(first.articles_synced, 2);
        assert_eq!(f.pushes().len(), 1);
        let root = f.storage.get_root();
        let content_calls = f.mock.content_calls.load(Ordering::SeqCst);

        let again = f.syncer().sync_account("wb").await.unwrap();
        assert!(again.errors.is_empty(), "{:?}", again.errors);
        assert_eq!(
            (
                again.articles_fetched,
                again.articles_synced,
                again.articles_already_synced
            ),
            (2, 0, 2)
        );
        assert_eq!(f.mock.content_calls.load(Ordering::SeqCst), content_calls);
        let after = f.storage.get_root();
        assert_eq!((after.hash, after.generation), (root.hash, root.generation));
        assert!(f.pushes().is_empty());
        assert_eq!(f.articles().len(), 2);
        assert_eq!(f.document_names().len(), 2);
    }

    /// `max_articles` caps what one sync puts on the device (newest first); with no folder
    /// the documents go to the top level.
    #[tokio::test]
    async fn max_articles_caps_delivery() {
        let f = Fixture::new(vec![entry(1), entry(2), entry(3)]).await;
        f.add_account("wb", |s| s.max_articles = 2);
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_fetched, r.articles_synced), (3, 2));
        assert_eq!(f.document_names(), ["Article 2", "Article 3"]);
        assert!(tree(&f.storage).iter().all(|n| n.parent.is_empty()));
    }

    /// A failed refresh or fetch changes nothing: no article, document or push, and
    /// `last_sync` stays put so the next sync covers the same window.
    #[tokio::test]
    async fn failed_refresh_or_fetch_changes_nothing() {
        let mut f = Fixture::new(vec![entry(1)]).await;
        let last = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        // No token and no way to get one: the refresh fails before any request.
        let no_credentials = ProviderConfig::Wallabag {
            instance_url: f.base.clone(),
            client_id: "c".into(),
            client_secret: None,
            access_token: None,
            refresh_token: None,
            token_expires_at: None,
            username: None,
            password: None,
        };
        let account = test_account(
            "wb",
            ReadLaterProvider::Wallabag,
            no_credentials,
            Some(last),
        );
        f.state.manager.lock().add_account(account).unwrap();
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert!(r.errors[0].starts_with("Auth refresh"), "{:?}", r.errors);
        assert_eq!(f.mock.list_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.account("wb").last_sync, Some(last));

        // A valid token, but the list request fails.
        f.mock.fail_list.store(true, Ordering::SeqCst);
        let config = wallabag_config(
            &f.base,
            Some("tok"),
            None,
            Some(Utc::now() + Duration::hours(1)),
        );
        let account = test_account("wb2", ReadLaterProvider::Wallabag, config, Some(last));
        f.state.manager.lock().add_account(account).unwrap();
        let r = f.syncer().sync_account("wb2").await.unwrap();
        assert!(
            r.errors.iter().any(|e| e.starts_with("Fetch")),
            "{:?}",
            r.errors
        );
        assert_eq!(f.account("wb2").last_sync, Some(last));
        assert!(f.articles().is_empty());
        assert!(tree(&f.storage).is_empty());
        assert!(f.pushes().is_empty());
    }

    /// An article whose delivery fails stays undelivered and `last_sync` doesn't advance; the
    /// next sync delivers just that one and never re-adds what already landed. What did land
    /// is announced at once.
    #[tokio::test]
    async fn failed_delivery_is_retried_without_duplicates() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        f.mock.fail_content.lock().insert(2);

        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(r.articles_synced, 1);
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert!(r.errors[0].starts_with("Content"), "{:?}", r.errors);
        assert_eq!(f.account("wb").last_sync, None);
        assert_eq!(f.document_names(), ["Article 1"]);
        assert_eq!(f.pushes().len(), 1, "the delivered article is announced");

        f.mock.fail_content.lock().clear();
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_synced, r.articles_already_synced), (1, 1));
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);
        assert!(f.account("wb").last_sync.is_some());
        assert_eq!(f.pushes().len(), 1);
    }

    /// A root index the server doesn't fully understand is never rewritten: the first refused
    /// document stops the delivery, nothing is marked delivered and no push is sent.
    #[tokio::test]
    async fn refused_root_is_left_untouched() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |s| s.folder_id = Some("Read Later".into()));
        let hash = "a".repeat(64);
        f.storage
            .put_with_hash(b"3\nnot a valid entry\n", &hash, "root.docSchema")
            .unwrap();
        let root = f.storage.set_root(hash.clone()).unwrap();

        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(r.articles_synced, 0);
        assert!(
            r.errors
                .iter()
                .any(|e| e.contains("refusing to modify root index")),
            "{:?}",
            r.errors
        );
        assert_eq!(
            f.mock.content_calls.load(Ordering::SeqCst),
            1,
            "stopped after the first refusal"
        );
        let after = f.storage.get_root();
        assert_eq!((after.hash, after.generation), (hash, root.generation));
        assert!(f.articles().iter().all(|a| !a.synced_to_device));
        assert_eq!(f.account("wb").last_sync, None);
        assert!(f.pushes().is_empty());
    }

    /// Syncs of one account never overlap: while one runs, a second request (409 from the
    /// endpoint), sync-all and the scheduler all skip it, and each article lands exactly once.
    #[tokio::test]
    async fn concurrent_syncs_of_an_account_never_overlap() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *f.mock.gate.lock() = Some(Arc::clone(&gate));

        let syncer = Arc::clone(&f.state.syncer);
        let first = tokio::spawn(async move { syncer.sync_account("wb").await });
        f.mock.listing.notified().await; // the first sync is waiting on its fetch

        assert!(matches!(
            f.syncer().sync_account("wb").await,
            Err(SyncError::AlreadyRunning(_))
        ));
        assert!(f.syncer().run_due(Utc::now()).await.is_empty());
        let all = f.syncer().sync_all().await;
        assert!(all.results.is_empty());
        assert_eq!(all.already_running, ["wb"]);
        let response = readlater_router(f.state.clone())
            .oneshot(post_request("/accounts/wb/sync"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(json_body(response).await["error"], "already_running");
        assert_eq!(f.mock.list_calls.load(Ordering::SeqCst), 1);

        gate.add_permits(1);
        let r = first.await.unwrap().unwrap();
        assert_eq!(r.articles_synced, 2);
        *f.mock.gate.lock() = None;
        // Released: the next sync runs and finds nothing new.
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!((r.articles_synced, r.articles_already_synced), (0, 2));
        assert_eq!(f.document_names().len(), 2);
        assert_eq!(f.pushes().len(), 1);
    }

    /// The sync endpoints run the real sync and answer with its real counts.
    #[tokio::test]
    async fn sync_endpoints_report_real_counts() {
        let f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        let app = readlater_router(f.state.clone());

        let response = app
            .clone()
            .oneshot(post_request("/accounts/wb/sync"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["account_id"], "wb");
        assert_eq!(body["articles_fetched"], 2);
        assert_eq!(body["articles_synced"], 2);
        assert_eq!(body["errors"], json!([]));

        f.mock.entries.lock().push(entry(3));
        let response = app.clone().oneshot(post_request("/sync")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(
            [
                &body["total_fetched"],
                &body["total_synced"],
                &body["total_errors"]
            ],
            [&json!(3), &json!(1), &json!(0)]
        );
        assert_eq!(body["results"][0]["articles_already_synced"], 2);
        assert_eq!(body["already_running"], json!([]));
        assert_eq!(f.document_names().len(), 3);

        let response = app
            .oneshot(post_request("/accounts/nope/sync"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Through the production routers the sync endpoints still need a token, and with no
    /// accounts a sync touches nothing.
    #[tokio::test]
    async fn production_routes_keep_auth_and_do_nothing_without_accounts() {
        let dir = tempfile::tempdir().unwrap();
        let devices =
            crate::DeviceManager::new(dir.path().join("devices.db"), "local", "local.test")
                .unwrap();
        let token = devices.create_user_token("local-user").unwrap();
        let state =
            crate::AppState::new(Storage::new(dir.path().join("storage")).unwrap(), devices);
        let mut rx = state.notification_tx.subscribe();
        let app = crate::create_router(state.clone())
            .merge(crate::feature_routes(state.clone(), dir.path(), None).unwrap());
        let sync = |token: Option<&str>| {
            let mut req = Request::post("/integrations/v2/readlater/sync");
            if let Some(token) = token {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            app.clone().oneshot(req.body(Body::empty()).unwrap())
        };

        assert_eq!(sync(None).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let response = sync(Some(&token)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["results"], json!([]));
        assert_eq!(body["total_synced"], 0);
        assert!(state.storage.get_root().hash.is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn scheduler_pass_without_accounts_does_nothing() {
        let mut f = Fixture::new(vec![entry(1)]).await;
        assert!(f.syncer().run_due(Utc::now()).await.is_empty());
        assert_eq!(f.mock.list_calls.load(Ordering::SeqCst), 0);
        assert!(f.storage.get_root().hash.is_empty());
        assert!(f.pushes().is_empty());
    }

    /// A failing account is retried after its interval, then after doubling waits, not on
    /// every pass; a success resets the wait.
    #[tokio::test]
    async fn scheduler_backs_off_a_failing_account() {
        let f = Fixture::new(vec![entry(1)]).await;
        f.add_account("wb", |s| s.sync_interval_minutes = 60);
        f.mock.fail_list.store(true, Ordering::SeqCst);
        let now = Utc::now();
        let pass = |minutes: i64| f.syncer().run_due(now + Duration::minutes(minutes));

        assert_eq!(pass(0).await.len(), 1);
        assert!(pass(30).await.is_empty());
        assert_eq!(pass(60).await.len(), 1, "first retry after the interval");
        assert!(pass(179).await.is_empty(), "then after twice the interval");
        assert_eq!(pass(180).await.len(), 1);
        assert!(pass(419).await.is_empty(), "then four times");
        assert_eq!(f.mock.list_calls.load(Ordering::SeqCst), 3);

        f.mock.fail_list.store(false, Ordering::SeqCst);
        let r = pass(420).await;
        assert!(r.len() == 1 && r[0].errors.is_empty(), "{r:?}");
        assert!(pass(479).await.is_empty(), "back to the plain interval");
        assert_eq!(pass(480).await.len(), 1);
        assert_eq!(f.mock.list_calls.load(Ordering::SeqCst), 5);
    }

    /// A revoked token gets a 401: the provider refreshes once and retries, and the sync
    /// persists the new tokens at once, so they survive a restart.
    #[tokio::test]
    async fn refreshed_credentials_are_persisted() {
        let f = Fixture::new(vec![entry(1)]).await;
        let config = wallabag_config(
            &f.base,
            Some("revoked"),
            Some("old-refresh"),
            Some(Utc::now() + Duration::hours(1)),
        );
        let account = test_account("wb", ReadLaterProvider::Wallabag, config, None);
        f.state.manager.lock().add_account(account).unwrap();

        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_fetched, r.articles_synced), (1, 1));
        {
            let requests = f.mock.token_requests.lock();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0]["grant_type"], "refresh_token");
            assert_eq!(requests[0]["refresh_token"], "old-refresh");
            assert_eq!(requests[0]["client_secret"], "csecret");
        }

        let reopened = ReadLaterManager::new(&f.db).unwrap();
        let ProviderConfig::Wallabag {
            access_token,
            refresh_token,
            token_expires_at,
            ..
        } = reopened.get_account("wb").unwrap().config
        else {
            panic!("wrong variant")
        };
        assert_eq!(access_token.as_deref(), Some("tok"));
        assert_eq!(refresh_token.as_deref(), Some("rotated-refresh"));
        assert!(token_expires_at.unwrap() > Utc::now());
    }

    /// Accounts with the HTML format record their articles but put nothing on the device,
    /// which opens only PDF and EPUB.
    #[tokio::test]
    async fn html_accounts_record_articles_without_delivering() {
        let mut f = Fixture::new(vec![entry(1)]).await;
        f.add_account("wb", |s| s.convert_format = ArticleFormat::Html);
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_fetched, r.articles_synced), (1, 0));
        assert_eq!(f.articles().len(), 1);
        assert!(tree(&f.storage).is_empty());
        assert!(f.pushes().is_empty());
        assert!(f.account("wb").last_sync.is_some());
    }

    /// A discontinued provider is reported for its own account and never scheduled.
    #[tokio::test]
    async fn discontinued_provider_is_reported_and_not_scheduled() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("rl.db");
        drop(ReadLaterManager::new(&db).unwrap()); // create the schema
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute(
                "INSERT INTO readlater_accounts (id, name, provider, enabled, config, sync_settings, created_at) VALUES ('om', 'Omni', 'omnivore', 1, '{\"type\":\"omnivore\"}', ?1, ?2)",
                rusqlite::params![
                    serde_json::to_string(&SyncSettings::default()).unwrap(),
                    Utc::now().to_rfc3339()
                ],
            )
            .unwrap();
        let state = ReadLaterState::new(
            ReadLaterManager::new(&db).unwrap(),
            Storage::new(dir.path().join("storage")).unwrap(),
            broadcast::channel(4).0,
        );

        let r = state.syncer.sync_account("om").await.unwrap();
        assert!(
            r.errors.iter().any(|e| e.contains("Omnivore shut down")),
            "{:?}",
            r.errors
        );
        assert!(state.syncer.run_due(Utc::now()).await.is_empty());
    }

    #[test]
    fn due_respects_flags_interval_and_backoff() {
        let now = Utc::now();
        let config = wallabag_config("http://wb.invalid", Some("t"), None, None);
        let mut a = test_account("a", ReadLaterProvider::Wallabag, config, None);
        assert!(is_due(&a, None, now), "never synced");
        a.last_sync = Some(now - Duration::minutes(59));
        assert!(!is_due(&a, None, now));
        a.last_sync = Some(now - Duration::minutes(60));
        assert!(is_due(&a, None, now));

        // Intervals below the minimum are raised to it.
        a.sync_settings.sync_interval_minutes = 0;
        a.last_sync = Some(now - Duration::minutes(MIN_INTERVAL_MINUTES - 1));
        assert!(!is_due(&a, None, now));
        a.sync_settings.sync_interval_minutes = 60;

        // This process's attempts count over `last_sync`; failures stretch the wait.
        let attempt = |minutes_ago: i64, failures: u32| Attempt {
            at: now - Duration::minutes(minutes_ago),
            failures,
        };
        assert!(!is_due(&a, Some(&attempt(30, 0)), now));
        assert!(is_due(&a, Some(&attempt(60, 0)), now));
        assert!(is_due(&a, Some(&attempt(60, 1)), now));
        assert!(!is_due(&a, Some(&attempt(100, 2)), now));
        assert!(is_due(&a, Some(&attempt(120, 2)), now));
        assert!(!is_due(&a, Some(&attempt(23 * 60, 40)), now), "capped");
        assert!(is_due(&a, Some(&attempt(24 * 60, 40)), now));

        let mut off = a.clone();
        off.enabled = false;
        assert!(!is_due(&off, None, now));
        let mut off = a.clone();
        off.sync_settings.auto_sync = false;
        assert!(!is_due(&off, None, now));
        let mut off = a;
        off.provider = ReadLaterProvider::Omnivore;
        assert!(!is_due(&off, None, now));
    }

    #[test]
    fn retry_wait_is_capped_but_never_below_the_interval() {
        let hour = Duration::hours(1);
        assert_eq!(retry_after(hour, 0), hour);
        assert_eq!(retry_after(hour, 1), hour);
        assert_eq!(retry_after(hour, 3), hour * 4);
        assert_eq!(
            retry_after(hour, u32::MAX),
            Duration::hours(MAX_BACKOFF_HOURS)
        );
        let week = Duration::days(7);
        assert_eq!(retry_after(week, 5), week);
    }

    #[test]
    fn scheduler_config_from_env_values() {
        let secs = std::time::Duration::from_secs;
        assert_eq!(
            SchedulerConfig::from_vars(None, None),
            SchedulerConfig {
                enabled: true,
                tick: secs(60)
            }
        );
        for off in ["0", "false", "OFF", " no "] {
            assert!(
                !SchedulerConfig::from_vars(Some(off), None).enabled,
                "{off}"
            );
        }
        assert!(SchedulerConfig::from_vars(Some("1"), None).enabled);
        assert_eq!(SchedulerConfig::from_vars(None, Some("15")).tick, secs(15));
        assert_eq!(SchedulerConfig::from_vars(None, Some("0")).tick, secs(60));
        assert_eq!(SchedulerConfig::from_vars(None, Some("x")).tick, secs(60));
    }

    #[test]
    fn document_name_falls_back_to_the_url() {
        let mut article: Article = serde_json::from_value(json!({
            "id": "a", "provider": "wallabag", "provider_id": "1", "url": "https://ex.com/1",
            "title": "  Title  ", "excerpt": null, "author": null, "word_count": null,
            "reading_time_minutes": null, "tags": [], "status": "unread", "favorite": false,
            "added_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z",
            "read_at": null, "content": null, "image_url": null, "document_id": null,
            "synced_to_device": false, "last_sync": null
        }))
        .unwrap();
        assert_eq!(document_name(&article), "Title");
        article.title = " ".into();
        assert_eq!(document_name(&article), "https://ex.com/1");
    }
}
