//! Read-later sync: puts new articles from each account's provider on the tablet, on a
//! schedule ([`ReadLaterSyncer::spawn_scheduler`]) and on demand (`POST
//! /integrations/v2/readlater/sync`, `.../accounts/{id}/sync`).
//!
//! One account's sync:
//! 1. refresh the credentials, persisting new tokens at once (the old ones may have been
//!    rotated out);
//! 2. with `sync_read_status`, send the provider the status changes made here (`PUT
//!    /articles/{id}`) to this account's articles, each once. This comes before the fetch, so
//!    the fetch already reflects them; a change not sent yet also survives the fetch;
//! 3. fetch the articles changed since the account's `last_sync`;
//! 4. apply the account's filters and `max_articles`, record the articles (one transaction, off
//!    the async threads; articles are unique per account and provider id), and skip the ones
//!    already on the device (`synced_to_device`, or found in the tree under the document an
//!    earlier sync recorded before its commit);
//! 5. render each new article (EPUB or PDF) and add them to the sync tree in batches of up to
//!    [`BATCH_ARTICLES`], each in one root commit ([`documents::stage_documents`]), inside
//!    `folder_id` (resolved or created by [`documents::ensure_folder`]; none = top level). These
//!    refuse a root index they don't fully understand, so the tablet's library is never
//!    rewritten lossily; such a root is found before any content is fetched. Each article is
//!    recorded with its document's id before the commit and marked delivered right after it,
//!    on the same blocking thread;
//! 6. if the root changed, tell connected devices to pull it (SyncComplete);
//! 7. advance `last_sync` to when the fetch started, unless something is to be retried over the
//!    same window: the fetch or recording failed, the tree refused a document, no PDF converter
//!    works on this host, the provider could not be reached, or an article failed that hasn't
//!    yet failed [`MAX_ITEM_FAILURES`] syncs in a row. Delivered articles are skipped, never
//!    added twice.
//!
//! Accounts with `convert_format: html` are recorded but not delivered: the tablet opens only
//! PDF and EPUB.
//!
//! At most one sync of an account runs at a time. The scheduler syncs enabled `auto_sync`
//! accounts every `sync_interval_minutes` (at least [`MIN_INTERVAL_MINUTES`]), one after
//! another, and backs off an account whose syncs keep failing as a whole (credentials, fetch,
//! recording, the tree, no working converter, the provider unreachable). An article or status
//! change the provider or converter rejects is reported and retried on the next sync as usual,
//! without delaying the account; after [`MAX_ITEM_FAILURES`] syncs in a row it stops holding
//! the account back. A converter rejects an article only if it still prints a test page: when
//! none can (none installed, too old, broken), the sync fails as a whole and keeps `last_sync`,
//! so the articles are delivered once one works rather than dropped as the articles' fault.
//! Provider requests time out ([`HttpTimeouts`]) and PDF converters are killed after a
//! deadline, so nothing a provider does can stall the scheduler.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::documents::{self, NewDocument};
use crate::notifications::WsMessage;
use crate::readlater::{
    Article,
    ArticleConverter,
    ArticleFormat,
    HttpTimeouts,
    PdfPrograms,
    ProviderAccount,
    ReadLaterError,
    ReadLaterManager,
    ReadLaterProvider,
    ReadLaterProviderTrait,
    RenderedArticle,
    SyncResult,
    distinct_articles,
    provider_with_timeouts,
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
/// Syncs in a row an article (or a status change) may be rejected before it stops holding the
/// account back: a failed article then no longer keeps `last_sync` where it is (it is still
/// retried while the provider lists it), and a status change is dropped.
pub const MAX_ITEM_FAILURES: u32 = 3;
/// Provider requests in a row that may fail to get through before a sync stops, taking the
/// provider for unreachable.
const MAX_UNREACHABLE: u32 = 2;
/// Most articles added to the device in one root commit. A sync delivering many (the first
/// import of an account) then moves the root a few times rather than once per article, so a
/// tablet syncing meanwhile has its root update refused (and retried) a few times at most, and
/// a commit the tree refuses holds back at most this many.
const BATCH_ARTICLES: usize = 20;
/// Rendered bytes after which a batch is committed early, so a batch of large PDFs isn't held
/// in memory all at once.
const BATCH_BYTES: usize = 64 << 20;

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
    /// Consecutive attempts in which the account as a whole failed.
    failures: u32,
}

/// What started a sync, which decides whether it may (still) run.
#[derive(Debug, Clone, Copy)]
enum Trigger {
    /// `POST .../accounts/{id}/sync`: runs whatever the account's settings.
    Manual,
    /// `POST .../sync`: enabled accounts.
    All,
    /// A scheduler pass at this time: enabled `auto_sync` accounts that are due.
    Scheduled(DateTime<Utc>),
}

impl Trigger {
    /// Whether a sync of `account` started this way may go on (checked again before each
    /// delivery, so disabling an account stops a sync in progress). Timing is checked once, when
    /// a scheduled sync starts.
    fn allows(self, account: &ProviderAccount) -> bool {
        match self {
            Self::Manual => true,
            Self::All => account.enabled,
            Self::Scheduled(_) => schedulable(account),
        }
    }
}

/// Something of one account that can fail on its own, sync after sync.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Item {
    /// Delivering the article with this provider id.
    Article(String),
    /// Sending the status change of the article with this id.
    Status(String),
}

/// Runs read-later syncs into the device's sync tree. Shared by the scheduler and the sync
/// endpoints.
pub struct ReadLaterSyncer {
    manager: Arc<Mutex<ReadLaterManager>>,
    storage: Storage,
    notification_tx: broadcast::Sender<WsMessage>,
    http: HttpTimeouts,
    pdf: PdfPrograms,
    /// Accounts with a sync in progress.
    running: Mutex<HashSet<String>>,
    attempts: Mutex<HashMap<String, Attempt>>,
    /// Consecutive failed syncs of each (account, item) that failed last time.
    item_failures: Mutex<HashMap<(String, Item), u32>>,
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
            http: HttpTimeouts::default(),
            pdf: PdfPrograms::default(),
            running: Mutex::new(HashSet::new()),
            attempts: Mutex::new(HashMap::new()),
            item_failures: Mutex::new(HashMap::new()),
        }
    }

    /// Limit provider requests by `http` rather than the default [`HttpTimeouts`].
    pub fn with_http_timeouts(mut self, http: HttpTimeouts) -> Self {
        self.http = http;
        self
    }

    /// Print PDFs with `pdf` rather than the converters on `PATH`.
    #[cfg(test)]
    fn with_pdf_programs(mut self, pdf: PdfPrograms) -> Self {
        self.pdf = pdf;
        self
    }

    /// Sync one account now, whether or not it is enabled or due. Fails only if the account
    /// doesn't exist or is already syncing; everything else is reported in the result.
    pub async fn sync_account(&self, account_id: &str) -> Result<SyncResult, SyncError> {
        let (_running, account) = self.claim(account_id)?;
        Ok(self.run_logged(account, Trigger::Manual, Utc::now()).await)
    }

    /// Sync every enabled account, one after another (oldest account first).
    pub async fn sync_all(&self) -> SyncAllReport {
        let accounts: Vec<ProviderAccount> = self
            .accounts_oldest_first()
            .into_iter()
            .filter(|a| a.enabled)
            .collect();
        let mut report = SyncAllReport::default();
        for account in accounts {
            match self.sync_if_allowed(&account.id, Trigger::All).await {
                Ok(Some(result)) => report.results.push(result),
                // Disabled since it was listed.
                Ok(None) => {}
                Err(SyncError::AlreadyRunning(id)) => report.already_running.push(id),
                // Deleted since it was listed.
                Err(SyncError::AccountNotFound(_)) => {}
            }
        }
        report
    }

    /// One scheduler pass: sync each account that is due at `now` (enabled, `auto_sync`, and
    /// its interval, stretched by backoff after failures, has passed), one after another (oldest
    /// account first), and return the results. Each account is checked again just before its
    /// sync, so one disabled, or synced by request, while an earlier one ran is skipped. With no
    /// accounts it returns at once, touching neither the network nor the sync tree.
    pub async fn run_due(&self, now: DateTime<Utc>) -> Vec<SyncResult> {
        let accounts = self.accounts_oldest_first();
        let due: Vec<String> = {
            let mut attempts = self.attempts.lock();
            attempts.retain(|id, _| accounts.iter().any(|a| a.id == *id));
            self.item_failures
                .lock()
                .retain(|(id, _), _| accounts.iter().any(|a| a.id == *id));
            accounts
                .iter()
                .filter(|a| is_due(a, attempts.get(&a.id), now))
                .map(|a| a.id.clone())
                .collect()
        };
        let mut results = Vec::new();
        for id in due {
            match self.sync_if_allowed(&id, Trigger::Scheduled(now)).await {
                Ok(Some(result)) => results.push(result),
                Ok(None) => tracing::debug!(account = %id, "no longer due; not synced"),
                Err(e) => tracing::debug!("scheduled read-later sync skipped: {e}"),
            }
        }
        results
    }

    /// Run [`run_due`](Self::run_due) every `tick` (which must be non-zero), the first time one
    /// tick after startup rather than while the server and the tablet reconnect. Passes never
    /// overlap. Logs each account's schedule first.
    pub fn spawn_scheduler(
        self: Arc<Self>,
        tick: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        self.log_schedule(tick);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + tick, tick);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                self.run_due(Utc::now()).await;
            }
        })
    }

    fn log_schedule(&self, tick: std::time::Duration) {
        let accounts = self.accounts_oldest_first();
        tracing::info!(
            tick_secs = tick.as_secs(),
            accounts = accounts.len(),
            "read-later scheduler started; first pass after one tick"
        );
        for a in accounts {
            let s = &a.sync_settings;
            tracing::info!(
                account = %a.name,
                id = %a.id,
                provider = %a.provider,
                scheduled = schedulable(&a),
                every_minutes = interval(&a).num_minutes(),
                max_articles = s.max_articles,
                format = ?s.convert_format,
                folder = s.folder_id.as_deref().unwrap_or("(top level)"),
                last_sync = ?a.last_sync,
                "read-later account"
            );
        }
    }

    fn accounts_oldest_first(&self) -> Vec<ProviderAccount> {
        let mut accounts = self.manager.lock().list_accounts();
        accounts.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        accounts
    }

    /// Take `account_id`'s place in [`running`](Self::running) and load the account as it is
    /// now.
    fn claim(&self, account_id: &str) -> Result<(Running<'_>, ProviderAccount), SyncError> {
        if !self.running.lock().insert(account_id.to_owned()) {
            return Err(SyncError::AlreadyRunning(account_id.into()));
        }
        let running = Running {
            set: &self.running,
            id: account_id.to_owned(),
        };
        let account = self
            .manager
            .lock()
            .get_account(account_id)
            .ok_or_else(|| SyncError::AccountNotFound(account_id.into()))?;
        Ok((running, account))
    }

    /// Sync `account_id` if `trigger` still allows it once claimed; `None` if not.
    async fn sync_if_allowed(
        &self,
        account_id: &str,
        trigger: Trigger,
    ) -> Result<Option<SyncResult>, SyncError> {
        let (_running, account) = self.claim(account_id)?;
        let (allowed, started) = match trigger {
            Trigger::Scheduled(now) => (
                is_due(&account, self.attempts.lock().get(account_id), now),
                now,
            ),
            _ => (trigger.allows(&account), Utc::now()),
        };
        if !allowed {
            return Ok(None);
        }
        Ok(Some(self.run_logged(account, trigger, started).await))
    }

    /// Run a claimed account's sync, record the attempt for the scheduler as made at `started`
    /// (intervals run from one attempt's start to the next) and log the outcome.
    async fn run_logged(
        &self,
        account: ProviderAccount,
        trigger: Trigger,
        started: DateTime<Utc>,
    ) -> SyncResult {
        let name = account.name.clone();
        let (result, failed) = self.run(account, trigger).await;
        let failures = self.record_attempt(&result.account_id, failed, started);
        if result.errors.is_empty() {
            tracing::info!(
                account = %name,
                provider = %result.provider,
                fetched = result.articles_fetched,
                synced = result.articles_synced,
                already_synced = result.articles_already_synced,
                statuses_sent = result.read_status_synced,
                "read-later sync done"
            );
        } else {
            tracing::warn!(
                account = %name,
                provider = %result.provider,
                fetched = result.articles_fetched,
                synced = result.articles_synced,
                statuses_sent = result.read_status_synced,
                account_failed = failed,
                failures_in_a_row = failures,
                errors = ?result.errors,
                "read-later sync had errors"
            );
        }
        result
    }

    /// Remember this attempt for the scheduler; returns the account's failures in a row.
    fn record_attempt(&self, account_id: &str, failed: bool, at: DateTime<Utc>) -> u32 {
        let mut attempts = self.attempts.lock();
        let failures = if failed {
            attempts
                .get(account_id)
                .map_or(0, |a| a.failures)
                .saturating_add(1)
        } else {
            0
        };
        attempts.insert(account_id.to_owned(), Attempt { at, failures });
        failures
    }

    /// Count one more failed sync for `item`; returns its failures in a row.
    fn item_failed(&self, account_id: &str, item: Item) -> u32 {
        let mut failures = self.item_failures.lock();
        let n = failures.entry((account_id.to_owned(), item)).or_insert(0);
        *n = n.saturating_add(1);
        *n
    }

    fn item_done(&self, account_id: &str, item: Item) {
        self.item_failures
            .lock()
            .remove(&(account_id.to_owned(), item));
    }

    /// Run the sync; returns its result and whether the account as a whole failed.
    async fn run(&self, account: ProviderAccount, trigger: Trigger) -> (SyncResult, bool) {
        let started = std::time::Instant::now();
        let result = SyncResult {
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
        let (mut result, failed) = match provider_with_timeouts(account.provider, self.http) {
            Ok(provider) => {
                let mut run = Run {
                    syncer: self,
                    provider: provider.as_ref(),
                    account,
                    trigger,
                    result,
                    failed: false,
                    retry: false,
                    unreachable: 0,
                };
                run.sync().await;
                (run.result, run.failed)
            }
            // A discontinued provider (Omnivore) is an error for this account only.
            Err(e) => {
                let mut result = result;
                result.errors.push(e.to_string());
                (result, true)
            }
        };
        result.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        result.completed_at = Utc::now();
        (result, failed)
    }
}

/// One account's sync in progress.
struct Run<'a> {
    syncer: &'a ReadLaterSyncer,
    provider: &'a dyn ReadLaterProviderTrait,
    account: ProviderAccount,
    trigger: Trigger,
    result: SyncResult,
    /// The account as a whole failed (the scheduler backs off).
    failed: bool,
    /// Something is to be retried over the same window, so `last_sync` stays.
    retry: bool,
    /// Provider requests in a row that didn't get through.
    unreachable: u32,
}

impl Run<'_> {
    async fn sync(&mut self) {
        // Refresh once (a no-op unless the token is missing, expired or about to be) so the
        // calls below don't each refresh. A failed refresh ends the sync before anything else.
        match self.provider.refresh_auth(&self.account.config).await {
            Ok(config) => {
                let refreshed = self.provider.take_refreshed_config().is_some();
                self.account.config = config;
                if refreshed {
                    self.persist_config();
                }
            }
            Err(e) => return self.fail(format!("Auth refresh: {e}")),
        }

        if self.account.sync_settings.sync_read_status {
            self.send_read_status().await;
            if self.failed {
                return; // the provider seems unreachable
            }
        }

        let window_start = Utc::now();
        let fetched = self
            .provider
            .fetch_articles(&self.account.config, self.account.last_sync)
            .await;
        self.absorb_refreshed();
        match fetched {
            Ok(articles) => {
                // Before the count and `max_articles`: a repeat would take a distinct
                // article's place, and that one might then never be synced.
                let listed = articles.len();
                let articles = distinct_articles(articles);
                if articles.len() < listed {
                    tracing::warn!(
                        account = %self.account.name,
                        repeats = listed - articles.len(),
                        "read-later provider listed some articles more than once; each is \
                         synced once"
                    );
                }
                self.result.articles_fetched = count(articles.len());
                self.deliver_new(articles).await;
            }
            Err(e) => return self.fail(format!("Fetch: {e}")),
        }

        if !self.retry {
            let saved = self
                .syncer
                .manager
                .lock()
                .set_last_sync(&self.account.id, window_start);
            if let Err(e) = saved {
                self.result.errors.push(format!("Save last sync: {e}"));
            }
        }
    }

    /// The account as a whole failed: report `message`, retry the window, back off.
    fn fail(&mut self, message: String) {
        self.result.errors.push(message);
        self.failed = true;
        self.retry = true;
    }

    /// A provider request didn't get through: returns whether to stop, the provider seeming
    /// unreachable ([`MAX_UNREACHABLE`] requests in a row failed so), which fails the sync.
    fn unreachable(&mut self) -> bool {
        self.unreachable += 1;
        if self.unreachable < MAX_UNREACHABLE {
            return false;
        }
        self.fail(format!(
            "Stopped: {} requests in a row didn't get through",
            self.unreachable
        ));
        true
    }

    /// An article (or status change) the provider or converter rejected, reported as `message`.
    /// It holds `last_sync` back until it has failed [`MAX_ITEM_FAILURES`] syncs in a row;
    /// returns whether it has now.
    fn item_rejected(&mut self, item: Item, message: String) -> bool {
        self.result.errors.push(message);
        let failures = self.syncer.item_failed(&self.account.id, item.clone());
        if failures < MAX_ITEM_FAILURES {
            if matches!(item, Item::Article(_)) {
                self.retry = true;
            }
            return false;
        }
        if failures == MAX_ITEM_FAILURES {
            tracing::warn!(
                account = %self.account.name,
                ?item,
                failures,
                "read-later item keeps failing; it no longer holds the account back"
            );
        }
        true
    }

    /// Adopt a token refresh the provider did inside its last call (e.g. a retry after a 401)
    /// and persist it at once.
    fn absorb_refreshed(&mut self) {
        if let Some(config) = self.provider.take_refreshed_config() {
            self.account.config = config;
            self.persist_config();
        }
    }

    fn persist_config(&mut self) {
        let saved = self
            .syncer
            .manager
            .lock()
            .update_account_config(&self.account.id, &self.account.config);
        if let Err(e) = saved {
            self.result
                .errors
                .push(format!("Persist refreshed credentials: {e}"));
        }
    }

    /// Whether the account may still be synced this way: not if it was deleted, or disabled
    /// (or, for a scheduled sync, its `auto_sync` turned off) since the sync started.
    fn still_allowed(&mut self) -> bool {
        let current = self.syncer.manager.lock().get_account(&self.account.id);
        let why = match current {
            Some(account) if self.trigger.allows(&account) => return true,
            Some(_) => "the account was disabled (or its auto_sync turned off)",
            None => "the account was deleted",
        };
        self.result
            .errors
            .push(format!("Stopped: {why} during the sync"));
        self.retry = true;
        false
    }

    /// Send the provider the status changes made here to this account's articles.
    async fn send_read_status(&mut self) {
        let changes = self
            .syncer
            .manager
            .lock()
            .pending_read_status(&self.account.id);
        for article in changes {
            let sent = self
                .provider
                .update_read_status(&self.account.config, &article.provider_id, article.status)
                .await;
            self.absorb_refreshed();
            let item = Item::Status(article.id.clone());
            let e = match sent {
                Ok(()) => {
                    self.unreachable = 0;
                    self.syncer.item_done(&self.account.id, item);
                    self.result.read_status_synced += 1;
                    let recorded = self
                        .syncer
                        .manager
                        .lock()
                        .read_status_sent(&article.id, article.status);
                    if let Err(e) = recorded {
                        self.result
                            .errors
                            .push(format!("Record status of {}: {e}", article.id));
                    }
                    continue;
                }
                Err(e) => e,
            };
            let message = format!("Status {}: {e}", article.id);
            if matches!(e, ReadLaterError::Network(_)) {
                // Not the change's fault; it stays pending for the next sync.
                self.result.errors.push(message);
                if self.unreachable() {
                    return;
                }
                continue;
            }
            self.unreachable = 0;
            if self.item_rejected(item.clone(), message) {
                self.syncer.item_done(&self.account.id, item);
                let dropped = self
                    .syncer
                    .manager
                    .lock()
                    .drop_read_status_change(&article.id);
                if let Err(e) = dropped {
                    self.result
                        .errors
                        .push(format!("Drop status change of {}: {e}", article.id));
                }
            }
        }
    }

    /// Record the selected articles and put the ones not yet on the device there.
    async fn deliver_new(&mut self, articles: Vec<Article>) {
        let selected: Vec<Article> =
            select_articles_for_sync(articles, &self.account.sync_settings)
                .into_iter()
                .map(|mut article| {
                    // Content is fetched per article on delivery; don't keep it in memory.
                    article.content = None;
                    article
                })
                .collect();
        if selected.is_empty() {
            return;
        }
        let manager = Arc::clone(&self.syncer.manager);
        let account_id = self.account.id.clone();
        let keep_pending = self.account.sync_settings.sync_read_status;
        let recorded = tokio::task::spawn_blocking(move || {
            manager
                .lock()
                .record_fetched(&account_id, keep_pending, selected)
        })
        .await;
        let recorded = match recorded {
            Ok(Ok(recorded)) => recorded,
            Ok(Err(e)) => return self.fail(format!("Save articles: {e}")),
            Err(e) => return self.fail(format!("Save articles: {e}")),
        };
        let (on_device, pending): (Vec<Article>, Vec<Article>) =
            recorded.into_iter().partition(|a| a.synced_to_device);
        self.result.articles_already_synced += count(on_device.len());
        let format = self.account.sync_settings.convert_format;
        if pending.is_empty() || format == ArticleFormat::Html {
            return;
        }

        let generation_before = self.syncer.storage.get_root().generation;
        if let Some(pending) = self.not_on_device(pending).await {
            self.deliver(pending, format).await;
        }
        let root = self.syncer.storage.get_root();
        if root.generation != generation_before {
            // Tell connected devices to pull the new root, as document uploads do.
            let _ = self.syncer.notification_tx.send(WsMessage::sync_complete(
                root.generation,
                SOURCE_DEVICE,
                DEVICE_USER,
            ));
        }
    }

    /// The articles of `pending` not on the device yet. Before a batch is committed its
    /// articles are recorded with the ids of the documents they are being added as, so one an
    /// earlier sync added without recording it (the process died between the two, or the
    /// database write failed) is recognised here, its document being in the tree, and marked
    /// delivered rather than added again. `None` (reported; the sync fails) if the tree is one
    /// the server won't add documents to, found before any content is fetched, or if what was
    /// found can't be recorded.
    async fn not_on_device(&mut self, pending: Vec<Article>) -> Option<Vec<Article>> {
        let storage = self.syncer.storage.clone();
        let listed = tokio::task::spawn_blocking(move || documents::root_node_ids(&storage)).await;
        let listed = match listed {
            Ok(Ok(listed)) => listed,
            Ok(Err(e)) => {
                self.fail(format!("Add articles to the device: {e}"));
                return None;
            }
            Err(e) => {
                self.fail(format!("Read the sync tree: {e}"));
                return None;
            }
        };
        let (found, rest): (Vec<Article>, Vec<Article>) = pending.into_iter().partition(|a| {
            a.document_id
                .as_ref()
                .is_some_and(|document| listed.contains(document))
        });
        if found.is_empty() {
            return Some(rest);
        }
        let deliveries: Vec<(String, String)> = found
            .iter()
            .filter_map(|a| Some((a.id.clone(), a.document_id.clone()?)))
            .collect();
        let marked = self.syncer.manager.lock().mark_delivered_all(&deliveries);
        if let Err(e) = marked {
            self.fail(format!(
                "Record {} articles found on the device as delivered: {e}",
                found.len()
            ));
            return None;
        }
        for (article, document) in &deliveries {
            tracing::info!(
                %article,
                %document,
                "read-later article found on the device (added by an earlier sync that didn't \
                 record it); recorded as delivered"
            );
        }
        for article in &found {
            self.syncer
                .item_done(&self.account.id, Item::Article(article.provider_id.clone()));
        }
        self.result.articles_already_synced += count(found.len());
        Some(rest)
    }

    /// Render `pending` as `format` and put them on the device in batches, each in one root
    /// commit of at most [`BATCH_ARTICLES`] documents (and about [`BATCH_BYTES`]), inside the
    /// account's folder. The account is checked before each article and each commit: once it
    /// may no longer be synced, what is rendered and not committed is dropped (delivered by a
    /// later sync). When the sync stops otherwise (the provider unreachable, no working
    /// converter), what is rendered is still committed; when a commit fails, nothing more is.
    async fn deliver(&mut self, pending: Vec<Article>, format: ArticleFormat) {
        let folder = self
            .account
            .sync_settings
            .folder_id
            .clone()
            .unwrap_or_default();
        let mut parent = None;
        let mut batch = Batch::default();
        for article in pending {
            if !self.still_allowed() {
                return;
            }
            match self.render(article, format).await {
                Rendering::Done(article, document) => {
                    batch.push(article, document);
                    if batch.is_full() {
                        let full = std::mem::take(&mut batch);
                        if !self.commit(full, &folder, &mut parent).await {
                            return;
                        }
                    }
                }
                Rendering::Skipped => {}
                Rendering::Stop => break,
            }
        }
        if !batch.is_empty() && self.still_allowed() {
            self.commit(batch, &folder, &mut parent).await;
        }
    }

    /// Fetch `article`'s content and render it as `format` (on a blocking thread).
    async fn render(&mut self, article: Article, format: ArticleFormat) -> Rendering {
        let content = self
            .provider
            .fetch_article_content(&self.account.config, &article)
            .await;
        self.absorb_refreshed();
        let item = Item::Article(article.provider_id.clone());
        let content = match content {
            Ok(content) => {
                self.unreachable = 0;
                content
            }
            Err(e) => {
                let message = format!("Content {}: {e}", article.id);
                if matches!(e, ReadLaterError::Network(_)) {
                    // Not the article's fault: retried over the same window.
                    self.result.errors.push(message);
                    self.retry = true;
                    if self.unreachable() {
                        return Rendering::Stop;
                    }
                } else {
                    self.unreachable = 0;
                    self.item_rejected(item, message);
                }
                return Rendering::Skipped;
            }
        };
        let pdf = self.syncer.pdf.clone();
        let rendered = tokio::task::spawn_blocking(move || {
            let rendered = ArticleConverter::render_with(&article, &content, format, &pdf);
            (article, rendered)
        })
        .await;
        match rendered {
            Ok((article, Ok(document))) => {
                self.result.articles_converted += 1;
                Rendering::Done(article, document)
            }
            // No converter works here, so none of the rest would render either; they are
            // delivered once one does.
            Ok((article, Err(e @ ReadLaterError::ConverterUnavailable(_)))) => {
                self.fail(format!("Convert {}: {e}", article.id));
                Rendering::Stop
            }
            Ok((article, Err(e))) => {
                self.item_rejected(item, format!("Convert {}: {e}", article.id));
                Rendering::Skipped
            }
            Err(e) => {
                self.fail(format!("Convert: {e}"));
                Rendering::Stop
            }
        }
    }

    /// Put `batch` on the device in one root commit and mark its articles delivered, on a
    /// blocking thread; returns whether it went through. If not, the sync fails (reported): the
    /// tree refused the change (e.g. a root index we won't rewrite), and the rest would be
    /// refused the same way; or the batch couldn't be recorded, and the next wouldn't be either.
    async fn commit(&mut self, batch: Batch, folder: &str, parent: &mut Option<String>) -> bool {
        let commit = BatchCommit {
            storage: self.syncer.storage.clone(),
            manager: Arc::clone(&self.syncer.manager),
            folder: folder.to_owned(),
            parent: parent.clone(),
            documents: batch.documents,
        };
        let outcome = match tokio::task::spawn_blocking(move || commit.run()).await {
            Ok(outcome) => outcome,
            Err(e) => {
                self.fail(format!("Deliver: {e}"));
                return false;
            }
        };
        if outcome.parent.is_some() {
            *parent = outcome.parent;
        }
        self.result.articles_synced += count(outcome.added.len());
        for provider_id in outcome.added {
            self.syncer
                .item_done(&self.account.id, Item::Article(provider_id));
        }
        match outcome.failure {
            None => true,
            Some(message) => {
                self.fail(message);
                false
            }
        }
    }
}

/// `n` as a count in a [`SyncResult`].
fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// What became of one article's rendering.
enum Rendering {
    Done(Article, RenderedArticle),
    /// Not delivered this sync (reported); go on with the next article.
    Skipped,
    /// The sync stops (reported).
    Stop,
}

/// Whether the scheduler syncs `account` at all: enabled, `auto_sync`, and its provider still
/// exists.
fn schedulable(account: &ProviderAccount) -> bool {
    account.enabled
        && account.sync_settings.auto_sync
        && account.provider != ReadLaterProvider::Omnivore
}

/// The account's time between scheduled syncs: its `sync_interval_minutes`, at least
/// [`MIN_INTERVAL_MINUTES`].
fn interval(account: &ProviderAccount) -> Duration {
    Duration::minutes(
        i64::from(account.sync_settings.sync_interval_minutes).max(MIN_INTERVAL_MINUTES),
    )
}

/// Whether the scheduler should sync `account` at `now`: it is [`schedulable`] and its
/// [`interval`] has passed since this process last tried it — stretched by [`retry_after`]
/// after failures — or, if not tried yet, since its last successful sync.
fn is_due(account: &ProviderAccount, attempt: Option<&Attempt>, now: DateTime<Utc>) -> bool {
    if !schedulable(account) {
        return false;
    }
    let interval = interval(account);
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

/// Rendered articles waiting to be added to the device in one root commit.
#[derive(Default)]
struct Batch {
    documents: Vec<(Article, RenderedArticle)>,
    bytes: usize,
}

impl Batch {
    fn push(&mut self, article: Article, document: RenderedArticle) {
        self.bytes = self.bytes.saturating_add(document.bytes.len());
        self.documents.push((article, document));
    }

    /// Whether the batch is to be committed now: it holds [`BATCH_ARTICLES`] documents, or
    /// [`BATCH_BYTES`] of them.
    fn is_full(&self) -> bool {
        self.documents.len() >= BATCH_ARTICLES || self.bytes >= BATCH_BYTES
    }

    fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }
}

/// What a [`BatchCommit`] did.
#[derive(Default)]
struct Committed {
    /// The folder's collection id, once resolved (reused for the rest of the sync).
    parent: Option<String>,
    /// Provider ids of the articles whose documents were committed.
    added: Vec<String>,
    failure: Option<String>,
}

impl Committed {
    fn failed(mut self, message: String) -> Self {
        self.failure = Some(message);
        self
    }
}

/// A batch's trip onto the device; blocking, so it runs on a blocking thread.
struct BatchCommit {
    storage: Storage,
    manager: Arc<Mutex<ReadLaterManager>>,
    folder: String,
    parent: Option<String>,
    documents: Vec<(Article, RenderedArticle)>,
}

impl BatchCommit {
    /// Resolve the folder (once per sync), store the documents' blobs, record the document each
    /// article is being added as, add them all to the tree in one root commit, and mark the
    /// articles delivered. Every step refuses a root index it doesn't fully understand, leaving
    /// it untouched. Nothing awaits between the commit and the mark, and a blocking task runs
    /// to completion even if the sync that started it is dropped; if the mark still doesn't
    /// happen (the process dies, the database write fails), the next sync finds the recorded
    /// documents in the tree and marks their articles instead of adding them again.
    fn run(self) -> Committed {
        let mut out = Committed::default();
        let n = self.documents.len();
        let parent = match self.parent {
            Some(parent) => parent,
            None => match documents::ensure_folder(&self.storage, &self.folder) {
                Ok(parent) => parent,
                Err(e) => return out.failed(format!("Folder {:?}: {e}", self.folder)),
            },
        };
        out.parent = Some(parent.clone());
        let new: Vec<NewDocument<'_>> = self
            .documents
            .iter()
            .map(|(article, document)| NewDocument {
                name: document_name(article),
                ext: document.ext,
                data: &document.bytes,
            })
            .collect();
        let staged = match documents::stage_documents(&self.storage, &new, &parent) {
            Ok(staged) => staged,
            Err(e) => return out.failed(format!("Add {n} articles to the device: {e}")),
        };
        let deliveries: Vec<(String, String)> = self
            .documents
            .iter()
            .map(|(article, _)| article.id.clone())
            .zip(staged.ids())
            .collect();
        if let Err(e) = self.manager.lock().plan_deliveries(&deliveries) {
            return out.failed(format!("Record the documents of {n} articles: {e}"));
        }
        let generation = match staged.commit(&self.storage) {
            Ok(generation) => generation,
            Err(e) => return out.failed(format!("Add {n} articles to the device: {e}")),
        };
        for ((article, _), (_, document)) in self.documents.iter().zip(&deliveries) {
            tracing::info!(
                article = %article.id,
                %document,
                generation,
                title = %article.title,
                "read-later article added to the device"
            );
        }
        out.added = self
            .documents
            .iter()
            .map(|(article, _)| article.provider_id.clone())
            .collect();
        if let Err(e) = self.manager.lock().mark_delivered_all(&deliveries) {
            tracing::error!(
                articles = n,
                generation,
                "documents committed but not recorded as delivered; the next sync finds them in \
                 the tree: {e}"
            );
            return out.failed(format!("Record {n} articles as delivered: {e}"));
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
    use axum::extract::{Path as UrlPath, Query, State};
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::response::Response;
    use axum::routing::{get, post};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::readlater::test_support::{spawn_server, test_account, wallabag_config};
    use crate::readlater::{ProviderConfig, ReadStatus, SyncSettings};
    use crate::readlater_api::{ReadLaterState, readlater_router};

    /// A Wallabag instance. `/api/entries.json` lists `entries` whatever `since` says (unless
    /// `honor_since`), so every sync sees them all again; `/api/entries/{id}.json` serves one
    /// entry's content (GET) and archives or unarchives it (PATCH).
    #[derive(Default)]
    struct MockWallabag {
        entries: Mutex<Vec<Value>>,
        /// List only the entries updated since `since`, as Wallabag does.
        honor_since: AtomicBool,
        /// Access token the API accepts, and the token endpoint issues.
        valid_token: String,
        fail_list: AtomicBool,
        /// List requests never get an answer.
        hang_list: AtomicBool,
        /// Entry ids whose content request fails.
        fail_content: Mutex<HashSet<i64>>,
        /// Content requests never get an answer.
        hang_content: AtomicBool,
        /// Entry ids whose PATCH fails.
        fail_patch: Mutex<HashSet<i64>>,
        /// `(entry id, archive)` of every PATCH that succeeded.
        patches: Mutex<Vec<(i64, i64)>>,
        list_calls: AtomicUsize,
        content_calls: AtomicUsize,
        patch_calls: AtomicUsize,
        token_requests: Mutex<Vec<HashMap<String, String>>>,
        /// When set, a list request signals `listing`, then waits for a permit.
        gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
        listing: tokio::sync::Notify,
        /// When set, a content request signals `fetching`, then waits for (and uses up) a
        /// permit.
        content_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
        fetching: tokio::sync::Notify,
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
        Query(query): Query<HashMap<String, String>>,
    ) -> Result<Json<Value>, StatusCode> {
        if !m.authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        m.list_calls.fetch_add(1, Ordering::SeqCst);
        if m.hang_list.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let gate = m.gate.lock().clone();
        if let Some(gate) = gate {
            m.listing.notify_one();
            let _permit = gate.acquire().await.unwrap();
        }
        if m.fail_list.load(Ordering::SeqCst) {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        let mut items = m.entries.lock().clone();
        let since = query.get("since").and_then(|s| s.parse::<i64>().ok());
        if let Some(since) = since.filter(|_| m.honor_since.load(Ordering::SeqCst)) {
            items.retain(|e| {
                let updated = DateTime::parse_from_rfc3339(e["updated_at"].as_str().unwrap());
                updated.unwrap().timestamp() >= since
            });
        }
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
        if m.hang_content.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let gate = m.content_gate.lock().clone();
        if let Some(gate) = gate {
            m.fetching.notify_one();
            gate.acquire().await.unwrap().forget();
        }
        if m.fail_content.lock().contains(&id) {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Ok(Json(json!({"content": format!("<p>Body of {id}</p>")})))
    }

    async fn patch_entry(
        State(m): State<Arc<MockWallabag>>,
        headers: HeaderMap,
        UrlPath(file): UrlPath<String>,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, StatusCode> {
        if !m.authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let id: i64 = file
            .strip_suffix(".json")
            .and_then(|id| id.parse().ok())
            .ok_or(StatusCode::NOT_FOUND)?;
        m.patch_calls.fetch_add(1, Ordering::SeqCst);
        if m.fail_patch.lock().contains(&id) {
            return Err(StatusCode::NOT_FOUND);
        }
        let archive = body["archive"].as_i64().ok_or(StatusCode::BAD_REQUEST)?;
        m.patches.lock().push((id, archive));
        for e in m.entries.lock().iter_mut().filter(|e| e["id"] == id) {
            e["is_archived"] = json!(archive);
        }
        Ok(Json(json!({"id": id, "is_archived": archive})))
    }

    /// Serve a [`MockWallabag`] listing `entries`; returns it and its base URL.
    async fn spawn_mock(entries: Vec<Value>) -> (Arc<MockWallabag>, String) {
        let mock = Arc::new(MockWallabag {
            entries: Mutex::new(entries),
            valid_token: "tok".into(),
            ..Default::default()
        });
        let app = axum::Router::new()
            .route("/oauth/v2/token", post(issue_token))
            .route("/api/entries.json", get(list_entries))
            .route("/api/entries/{file}", get(get_entry).patch(patch_entry))
            .with_state(Arc::clone(&mock));
        (mock, spawn_server(app).await)
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
            Self::with_timeouts(entries, HttpTimeouts::default()).await
        }

        /// A fixture whose syncer limits provider requests by `http`.
        async fn with_timeouts(entries: Vec<Value>, http: HttpTimeouts) -> Self {
            Self::with_syncer(entries, |s| s.with_http_timeouts(http)).await
        }

        /// A fixture whose syncer prints PDFs with `pdf`.
        async fn with_pdf_programs(entries: Vec<Value>, pdf: PdfPrograms) -> Self {
            Self::with_syncer(entries, |s| s.with_pdf_programs(pdf)).await
        }

        /// A fixture whose syncer is set up by `configure`.
        async fn with_syncer(
            entries: Vec<Value>,
            configure: impl FnOnce(ReadLaterSyncer) -> ReadLaterSyncer,
        ) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("rl.db");
            let storage = Storage::new(dir.path().join("storage")).unwrap();
            let (tx, rx) = broadcast::channel(64);
            let manager = Arc::new(Mutex::new(ReadLaterManager::new(&db).unwrap()));
            let syncer = configure(ReadLaterSyncer::new(
                Arc::clone(&manager),
                storage.clone(),
                tx,
            ));
            let state = ReadLaterState {
                manager,
                syncer: Arc::new(syncer),
                scheduler: None,
            };
            let (mock, base) = spawn_mock(entries).await;
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

        /// Add a Wallabag account on the fixture's mock with a valid token; `settings` adjusts
        /// its sync settings.
        fn add_account(&self, id: &str, settings: impl FnOnce(&mut SyncSettings)) {
            self.add_account_at(id, &self.base, settings);
        }

        /// [`add_account`](Self::add_account) on the Wallabag at `base`.
        fn add_account_at(&self, id: &str, base: &str, settings: impl FnOnce(&mut SyncSettings)) {
            let config = wallabag_config(
                base,
                Some("tok"),
                None,
                Some(Utc::now() + Duration::hours(1)),
            );
            let mut account = test_account(id, ReadLaterProvider::Wallabag, config, None);
            settings(&mut account.sync_settings);
            self.state.manager.lock().add_account(account).unwrap();
        }

        /// The recorded article with this Wallabag entry id (only one account has it).
        fn article(&self, entry_id: i64) -> Article {
            let mut found: Vec<Article> = self
                .articles()
                .into_iter()
                .filter(|a| a.provider_id == entry_id.to_string())
                .collect();
            assert_eq!(found.len(), 1, "{found:?}");
            found.remove(0)
        }

        /// `account`'s recorded article with this Wallabag entry id.
        fn article_of(&self, account: &str, entry_id: i64) -> Article {
            self.articles()
                .into_iter()
                .find(|a| {
                    a.provider_id == entry_id.to_string()
                        && a.account_id.as_deref() == Some(account)
                })
                .unwrap()
        }

        /// Consecutive failures the scheduler holds against the account.
        fn account_failures(&self, id: &str) -> Option<u32> {
            self.syncer().attempts.lock().get(id).map(|a| a.failures)
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

    /// A Wallabag config with no token and no way to get one: a refresh fails before any
    /// request.
    fn no_credentials(base: &str) -> ProviderConfig {
        ProviderConfig::Wallabag {
            instance_url: base.into(),
            client_id: "c".into(),
            client_secret: None,
            access_token: None,
            refresh_token: None,
            token_expires_at: None,
            username: None,
            password: None,
        }
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

    /// An entry the provider lists twice is one article: one row, one content fetch, one
    /// document, and it takes one `max_articles` place, so the distinct article after it is
    /// still delivered. The next sync adds nothing.
    #[tokio::test]
    async fn an_entry_listed_twice_is_delivered_once() {
        let f = Fixture::new(vec![entry(1), entry(1)]).await;
        f.add_account("wb", |_| {});
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(
            (r.articles_fetched, r.articles_converted, r.articles_synced),
            (1, 1, 1)
        );
        assert_eq!(f.document_names(), ["Article 1"]);
        let article = f.article(1);
        let doc = tree(&f.storage)
            .into_iter()
            .find(|n| n.kind == "DocumentType")
            .unwrap();
        assert!(article.synced_to_device);
        assert_eq!(article.document_id, Some(doc.id));
        assert_eq!(f.mock.content_calls.load(Ordering::SeqCst), 1);

        // Capped at two, the repeat of 3 must not push 2 out.
        let f = Fixture::new(vec![entry(3), entry(3), entry(2), entry(1)]).await;
        f.add_account("wb", |s| s.max_articles = 2);
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_fetched, r.articles_synced), (3, 2));
        assert_eq!(f.document_names(), ["Article 2", "Article 3"]);
        assert_eq!(f.articles().len(), 2);
        let again = f.syncer().sync_account("wb").await.unwrap();
        assert!(again.errors.is_empty(), "{:?}", again.errors);
        assert_eq!(
            (again.articles_synced, again.articles_already_synced),
            (0, 2)
        );
        assert_eq!(f.document_names(), ["Article 2", "Article 3"]);
    }

    /// A failed refresh or fetch changes nothing: no article, document or push, and
    /// `last_sync` stays put so the next sync covers the same window.
    #[tokio::test]
    async fn failed_refresh_or_fetch_changes_nothing() {
        let mut f = Fixture::new(vec![entry(1)]).await;
        let last = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let account = test_account(
            "wb",
            ReadLaterProvider::Wallabag,
            no_credentials(&f.base),
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

    /// A root index the server doesn't fully understand is never rewritten: it is found before
    /// any content is fetched, nothing is marked delivered and no push is sent.
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
            0,
            "stopped before fetching any content"
        );
        assert_eq!(f.account_failures("wb"), Some(1), "an account-wide failure");
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

    /// An article `id` with Wallabag entry id `provider_id`, titled "  Title  ".
    fn test_article(id: &str, provider_id: &str) -> Article {
        serde_json::from_value(json!({
            "id": id, "provider": "wallabag", "provider_id": provider_id,
            "url": format!("https://ex.com/{provider_id}"),
            "title": "  Title  ", "excerpt": null, "author": null, "word_count": null,
            "reading_time_minutes": null, "tags": [], "status": "unread", "favorite": false,
            "added_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z",
            "read_at": null, "content": null, "image_url": null, "document_id": null,
            "synced_to_device": false, "last_sync": null
        }))
        .unwrap()
    }

    #[test]
    fn document_name_falls_back_to_the_url() {
        let mut article = test_article("a", "1");
        assert_eq!(document_name(&article), "Title");
        article.title = " ".into();
        assert_eq!(document_name(&article), "https://ex.com/1");
    }

    #[test]
    fn a_batch_is_full_at_its_article_or_byte_limit() {
        let rendered = |size: usize| RenderedArticle {
            ext: "epub",
            bytes: vec![0; size],
        };
        let mut batch = Batch::default();
        for _ in 1..BATCH_ARTICLES {
            batch.push(test_article("a", "1"), rendered(1));
            assert!(!batch.is_full());
        }
        batch.push(test_article("a", "1"), rendered(1));
        assert!(batch.is_full());

        let mut large = Batch::default();
        large.push(test_article("a", "1"), rendered(1));
        large.bytes = BATCH_BYTES - 1;
        assert!(!large.is_full());
        large.push(test_article("a", "1"), rendered(1));
        assert!(large.is_full());
    }

    /// A batch the tree refuses commits nothing and marks nothing: the root index (one the
    /// server doesn't fully understand) is left byte for byte, no blob is written, and the
    /// article is neither delivered nor given a document.
    #[test]
    fn a_refused_batch_commits_and_marks_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().join("storage")).unwrap();
        let manager = Arc::new(Mutex::new(
            ReadLaterManager::new(&dir.path().join("rl.db")).unwrap(),
        ));
        let article = test_article("a", "1");
        manager.lock().save_article(&article).unwrap();
        let (index, hash) = (b"3\nnot a valid entry\n", "a".repeat(64));
        storage
            .put_with_hash(index, &hash, "root.docSchema")
            .unwrap();
        let root = storage.set_root(hash.clone()).unwrap();
        let blobs = storage.list_hashes().unwrap().len();

        let out = BatchCommit {
            storage: storage.clone(),
            manager: Arc::clone(&manager),
            folder: String::new(),
            parent: None,
            documents: vec![(
                article,
                RenderedArticle {
                    ext: "epub",
                    bytes: b"PK".to_vec(),
                },
            )],
        }
        .run();
        let failure = out.failure.expect("refused");
        assert!(
            failure.contains("refusing to modify root index"),
            "{failure}"
        );
        assert!(out.added.is_empty());
        let after = storage.get_root();
        assert_eq!(
            (after.hash, after.generation),
            (hash.clone(), root.generation)
        );
        assert_eq!(storage.get(&hash).unwrap(), index);
        assert_eq!(storage.list_hashes().unwrap().len(), blobs);
        let a = manager.lock().get_article("a").unwrap();
        assert_eq!((a.synced_to_device, a.document_id), (false, None));
    }

    /// Articles are added in batches, each in one root commit: a sync delivering 3 moves the
    /// root once, and a first import of 21 moves it twice (20, then 1). Devices get one push
    /// either way.
    #[tokio::test]
    async fn articles_are_added_one_commit_per_batch() {
        for (entries, commits) in [(3, 1), (BATCH_ARTICLES as i64 + 1, 2)] {
            let mut f = Fixture::new((1..=entries).map(entry).collect()).await;
            f.add_account("wb", |s| s.max_articles = 0);
            let before = f.storage.get_root().generation;

            let r = f.syncer().sync_account("wb").await.unwrap();
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            let n = u32::try_from(entries).unwrap();
            assert_eq!(
                (r.articles_fetched, r.articles_converted, r.articles_synced),
                (n, n, n)
            );
            let after = f.storage.get_root().generation;
            assert_eq!(after, before + commits, "{entries} articles");
            assert_eq!(f.document_names().len(), entries as usize);
            assert!(f.articles().iter().all(|a| a.synced_to_device));
            assert_eq!(f.pushes().len(), 1);
        }
    }

    /// A sync that committed a batch but didn't record it (the process died in between, or
    /// the database write failed) leaves each article with the document it was being added
    /// as. The next sync finds that document in the tree and marks the article delivered
    /// instead of adding it again (its content isn't even fetched); an article whose document
    /// never landed is delivered as usual.
    #[tokio::test]
    async fn a_committed_but_unrecorded_batch_is_not_added_again() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        f.mock.fail_content.lock().extend([1, 2]);
        assert_eq!(
            f.syncer().sync_account("wb").await.unwrap().articles_synced,
            0
        );
        let (a1, a2) = (f.article(1), f.article(2));
        let one = [NewDocument {
            name: "Article 1",
            ext: "epub",
            data: b"PK",
        }];
        let staged = documents::stage_documents(&f.storage, &one, "").unwrap();
        let landed = staged.ids().remove(0);
        f.state
            .manager
            .lock()
            .plan_deliveries(&[
                (a1.id.clone(), landed.clone()),
                (a2.id.clone(), "never-committed".into()),
            ])
            .unwrap();
        staged.commit(&f.storage).unwrap();
        f.pushes();
        let content_calls = f.mock.content_calls.load(Ordering::SeqCst);

        f.mock.fail_content.lock().clear();
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_synced, r.articles_already_synced), (1, 1));
        assert_eq!(
            f.mock.content_calls.load(Ordering::SeqCst),
            content_calls + 1
        );
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);
        let a1 = f.article(1);
        assert_eq!(
            (a1.synced_to_device, a1.document_id.as_deref()),
            (true, Some(landed.as_str()))
        );
        let a2 = f.article(2);
        let document = a2.document_id.expect("delivered as a new document");
        assert!(a2.synced_to_device);
        assert!(tree(&f.storage).iter().any(|n| n.id == document));
        assert_eq!(f.pushes().len(), 1);

        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!((r.articles_synced, r.articles_already_synced), (0, 2));
    }

    /// The database refusing to record a committed batch as delivered (a trigger stands in for,
    /// say, a full disk) fails the sync, but each article was recorded with its document before
    /// the commit, so the next sync finds them in the tree and adds nothing again.
    #[tokio::test]
    async fn a_batch_the_database_fails_to_record_is_not_added_again() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        let db = rusqlite::Connection::open(&f.db).unwrap();
        db.execute_batch(
            "CREATE TRIGGER no_marks BEFORE UPDATE OF synced_to_device ON readlater_articles
             WHEN NEW.synced_to_device = 1 BEGIN SELECT RAISE(ABORT, 'disk full'); END;",
        )
        .unwrap();

        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(r.articles_synced, 2, "on the device");
        assert!(
            r.errors
                .iter()
                .any(|e| e.starts_with("Record 2 articles as delivered")),
            "{:?}",
            r.errors
        );
        assert_eq!(f.account_failures("wb"), Some(1));
        assert_eq!(f.account("wb").last_sync, None);
        assert!(
            f.articles()
                .iter()
                .all(|a| !a.synced_to_device && a.document_id.is_some())
        );
        assert_eq!(f.document_names().len(), 2);
        assert_eq!(f.pushes().len(), 1);

        db.execute_batch("DROP TRIGGER no_marks").unwrap();
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_synced, r.articles_already_synced), (0, 2));
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);
        assert!(f.articles().iter().all(|a| a.synced_to_device));
        assert!(f.account("wb").last_sync.is_some());
        assert!(f.pushes().is_empty());
    }

    /// An account disabled while a scheduled sync renders its articles stops the sync before
    /// the commit: what was rendered is dropped (delivered by a later sync), not added.
    #[tokio::test]
    async fn disabling_the_account_before_the_commit_adds_nothing() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *f.mock.content_gate.lock() = Some(Arc::clone(&gate));
        let syncer = Arc::clone(&f.state.syncer);
        let pass = tokio::spawn(async move { syncer.run_due(Utc::now()).await });
        f.mock.fetching.notified().await; // the first article's content
        gate.add_permits(1);
        f.mock.fetching.notified().await; // the second's: the first is rendered

        let mut account = f.account("wb");
        account.enabled = false;
        f.state.manager.lock().update_account(account).unwrap();
        gate.add_permits(1);

        let results = pass.await.unwrap();
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!((r.articles_converted, r.articles_synced), (2, 0));
        assert!(
            r.errors
                .iter()
                .any(|e| e.starts_with("Stopped: the account was disabled")),
            "{:?}",
            r.errors
        );
        assert!(tree(&f.storage).is_empty());
        assert!(
            f.articles()
                .iter()
                .all(|a| !a.synced_to_device && a.document_id.is_none())
        );
        assert_eq!(f.account("wb").last_sync, None);
        assert!(f.pushes().is_empty());
    }
    /// An article the provider keeps refusing is reported on every sync but doesn't back the
    /// account off, and holds `last_sync` back only until it has failed MAX_ITEM_FAILURES syncs
    /// in a row. It is still retried while the provider lists it.
    #[tokio::test]
    async fn a_failing_article_does_not_back_off_the_account() {
        let f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |s| s.sync_interval_minutes = 60);
        f.mock.fail_content.lock().insert(2);
        let now = Utc::now();
        let pass = |minutes: i64| f.syncer().run_due(now + Duration::minutes(minutes));

        for (failures, minutes) in (1..=MAX_ITEM_FAILURES).zip([0, 60, 120]) {
            let r = pass(minutes).await;
            assert_eq!(r.len(), 1, "due after the plain interval at {minutes}");
            assert_eq!(r[0].errors.len(), 1, "{:?}", r[0].errors);
            assert!(r[0].errors[0].starts_with("Content"), "{:?}", r[0].errors);
            assert_eq!(f.account_failures("wb"), Some(0), "no backoff");
            assert_eq!(
                f.account("wb").last_sync.is_none(),
                failures < MAX_ITEM_FAILURES,
                "last_sync held back after {failures} failures"
            );
        }
        assert_eq!(f.document_names(), ["Article 1"]);

        f.mock.fail_content.lock().clear();
        let r = pass(180).await;
        assert!(r.len() == 1 && r[0].errors.is_empty(), "{r:?}");
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);
    }

    /// With no working PDF converter (here a WeasyPrint too old for `--allowed-protocols`,
    /// which exits with a usage error, and no wkhtmltopdf) no article can be rendered: each sync
    /// fails as a whole at the first one, backing the account off and keeping `last_sync`
    /// however often it happens, so the articles are delivered once a converter works, even
    /// though the provider lists only what changed since `last_sync`.
    #[cfg(unix)]
    #[tokio::test]
    async fn no_working_converter_keeps_the_articles_for_later() {
        use std::os::unix::fs::PermissionsExt;
        let bin = tempfile::tempdir().unwrap();
        let install = |name: &str, script: &str| {
            let path = bin.path().join(name);
            std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        };
        install("weasyprint", "exit 2");
        let programs = PdfPrograms {
            weasyprint: bin.path().join("weasyprint").into(),
            wkhtmltopdf: bin.path().join("wkhtmltopdf").into(),
        };
        let f = Fixture::with_pdf_programs(vec![entry(1), entry(2)], programs).await;
        f.mock.honor_since.store(true, Ordering::SeqCst);
        f.add_account("wb", |s| s.convert_format = ArticleFormat::Pdf);

        for failures in 1..=MAX_ITEM_FAILURES + 1 {
            let r = f.syncer().sync_account("wb").await.unwrap();
            assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
            assert!(
                r.errors[0].contains("Converter unavailable")
                    && r.errors[0].contains("wkhtmltopdf not installed"),
                "{:?}",
                r.errors
            );
            assert_eq!(
                (r.articles_fetched, r.articles_synced, r.articles_converted),
                (2, 0, 0)
            );
            assert_eq!(f.account_failures("wb"), Some(failures), "backs off");
            assert_eq!(f.account("wb").last_sync, None);
        }
        assert_eq!(
            f.mock.content_calls.load(Ordering::SeqCst),
            usize::try_from(MAX_ITEM_FAILURES + 1).unwrap(),
            "each sync stops at the first article"
        );
        assert!(f.document_names().is_empty());

        // The operator installs wkhtmltopdf; it writes its last argument.
        install(
            "wkhtmltopdf",
            r#"for out; do :; done; printf '%%PDF-1.4' > "$out""#,
        );
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.articles_synced, 2);
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);
        assert_eq!(f.account_failures("wb"), Some(0));
        assert!(f.account("wb").last_sync.is_some());
    }

    /// Another Wallabag's entry under the id of a deleted account's delivered one is another
    /// page: the new account takes the row over and delivers its article. An entry with the
    /// same page isn't delivered twice.
    #[tokio::test]
    async fn a_deleted_accounts_rows_keep_their_device_state_only_for_the_same_page() {
        let f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("old", |_| {});
        f.syncer().sync_account("old").await.unwrap();
        let old_document = f.article(1).document_id.unwrap();
        f.state.manager.lock().delete_account("old").unwrap();

        let mut other_entry = entry(1);
        other_entry["url"] = json!("https://elsewhere.example/1");
        other_entry["title"] = json!("Another instance's 1");
        let (_other, other_base) = spawn_mock(vec![other_entry, entry(2)]).await;
        f.add_account_at("new", &other_base, |_| {});
        let r = f.syncer().sync_account("new").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.articles_synced, r.articles_already_synced), (1, 1));
        assert_eq!(
            f.document_names(),
            ["Another instance's 1", "Article 1", "Article 2"]
        );
        let a = f.article(1);
        assert_eq!(
            (a.account_id.as_deref(), a.synced_to_device),
            (Some("new"), true)
        );
        assert_ne!(a.document_id.unwrap(), old_document);
        assert_eq!(f.articles().len(), 2);
    }

    /// A status changed here (`PUT /articles/{id}`) goes to the provider of the account that
    /// recorded the article, once. Another account of the same provider (a second Wallabag
    /// numbering its entries alike) never sends it, nor takes the article over: it records and
    /// delivers its own entry with that id.
    #[tokio::test]
    async fn status_changes_are_sent_once_by_their_own_account() {
        let f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        assert_eq!(
            f.syncer().sync_account("wb").await.unwrap().articles_synced,
            2
        );
        let app = readlater_router(f.state.clone());
        let set_status = |status: &str| {
            let body = json!({ "status": status }).to_string();
            let request = Request::put(format!("/articles/{}", f.article_of("wb", 1).id))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            app.clone().oneshot(request)
        };
        assert_eq!(
            set_status("archived").await.unwrap().status(),
            StatusCode::OK
        );
        assert!(f.article_of("wb", 1).read_status_pending);

        let mut other_entry = entry(1);
        other_entry["title"] = json!("Another instance's 1");
        let (other, other_base) = spawn_mock(vec![other_entry, entry(3)]).await;
        f.add_account_at("wb2", &other_base, |_| {});
        let r = f.syncer().sync_account("wb2").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.read_status_synced, r.articles_synced), (0, 2));
        assert_eq!(f.account_failures("wb2"), Some(0));
        assert!(f.account("wb2").last_sync.is_some());
        assert_eq!(other.patch_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.mock.patch_calls.load(Ordering::SeqCst), 0);
        let a = f.article_of("wb", 1);
        assert_eq!(
            (
                a.title.as_str(),
                a.account_id.as_deref(),
                a.read_status_pending
            ),
            ("Article 1", Some("wb"), true)
        );
        let theirs = f.article_of("wb2", 1);
        assert_eq!(
            (theirs.title.as_str(), theirs.synced_to_device),
            ("Another instance's 1", true)
        );

        let r = f.syncer().sync_account("wb").await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.read_status_synced, 1);
        assert_eq!(*f.mock.patches.lock(), [(1, 1)]);
        let a = f.article_of("wb", 1);
        assert_eq!(
            (a.status, a.read_status_pending),
            (ReadStatus::Archived, false)
        );

        // Sent once, and setting the same status again is no change.
        assert_eq!(
            set_status("archived").await.unwrap().status(),
            StatusCode::OK
        );
        assert!(!f.article_of("wb", 1).read_status_pending);
        let r = f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(r.read_status_synced, 0);
        assert_eq!(f.mock.patch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.document_names(),
            [
                "Another instance's 1",
                "Article 1",
                "Article 2",
                "Article 3"
            ]
        );
        assert_eq!(f.articles().len(), 4);
    }

    /// A status change the provider refuses stays pending and wins over the provider's status
    /// when the article is fetched again, without backing the account off or holding
    /// `last_sync` back; after MAX_ITEM_FAILURES refusals in a row it is dropped.
    #[tokio::test]
    async fn a_refused_status_change_survives_fetches_then_is_dropped() {
        let f = Fixture::new(vec![entry(1)]).await;
        f.add_account("wb", |_| {});
        f.syncer().sync_account("wb").await.unwrap();
        let mut article = f.article(1);
        article.status = ReadStatus::Read;
        article.read_status_pending = true;
        f.state.manager.lock().save_article(&article).unwrap();
        f.mock.fail_patch.lock().insert(1);

        for failures in 1..=MAX_ITEM_FAILURES {
            let r = f.syncer().sync_account("wb").await.unwrap();
            assert_eq!(r.read_status_synced, 0);
            assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
            assert!(r.errors[0].starts_with("Status"), "{:?}", r.errors);
            assert_eq!(f.account_failures("wb"), Some(0));
            assert!(f.account("wb").last_sync.is_some());
            let a = f.article(1);
            if failures < MAX_ITEM_FAILURES {
                // Listed unread again, but the change made here is kept.
                assert_eq!((a.status, a.read_status_pending), (ReadStatus::Read, true));
            } else {
                // Dropped: the provider's status is taken again.
                assert_eq!(
                    (a.status, a.read_status_pending),
                    (ReadStatus::Unread, false)
                );
            }
        }
        let sent = f.mock.patch_calls.load(Ordering::SeqCst);
        assert_eq!(sent, MAX_ITEM_FAILURES as usize);
        f.syncer().sync_account("wb").await.unwrap();
        assert_eq!(
            f.mock.patch_calls.load(Ordering::SeqCst),
            sent,
            "not sent again"
        );
    }

    /// Provider requests time out: a Wallabag that accepts connections and never answers can't
    /// stall a scheduler pass or keep the account claimed. Content requests that don't get
    /// through stop the sync after MAX_UNREACHABLE of them and back the account off.
    #[tokio::test]
    async fn a_hung_provider_times_out() {
        let http = HttpTimeouts {
            connect: std::time::Duration::from_secs(5),
            request: std::time::Duration::from_millis(300),
        };
        let f = Fixture::with_timeouts(vec![entry(1), entry(2), entry(3)], http).await;
        f.add_account("wb", |_| {});
        let limit = std::time::Duration::from_secs(20);

        f.mock.hang_list.store(true, Ordering::SeqCst);
        let results = tokio::time::timeout(limit, f.syncer().run_due(Utc::now()))
            .await
            .expect("the pass ends");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].errors.len(), 1, "{:?}", results[0].errors);
        assert!(results[0].errors[0].starts_with("Fetch"));
        assert_eq!(f.account_failures("wb"), Some(1));

        f.mock.hang_list.store(false, Ordering::SeqCst);
        f.mock.hang_content.store(true, Ordering::SeqCst);
        let r = tokio::time::timeout(limit, f.syncer().sync_account("wb"))
            .await
            .expect("the sync ends")
            .expect("the account was released");
        assert_eq!(
            f.mock.content_calls.load(Ordering::SeqCst),
            MAX_UNREACHABLE as usize
        );
        assert!(
            r.errors.last().unwrap().starts_with("Stopped"),
            "{:?}",
            r.errors
        );
        assert_eq!(r.articles_synced, 0);
        assert_eq!(f.account_failures("wb"), Some(2));
        assert_eq!(f.account("wb").last_sync, None);
    }

    /// The scheduler's first pass comes one tick after it starts (not at once, while the
    /// tablet reconnects), and passes keep coming every tick.
    #[tokio::test(start_paused = true)]
    async fn scheduler_passes_start_one_tick_after_startup() {
        let dir = tempfile::tempdir().unwrap();
        let state = ReadLaterState::new(
            ReadLaterManager::new(&dir.path().join("rl.db")).unwrap(),
            Storage::new(dir.path().join("storage")).unwrap(),
            broadcast::channel(4).0,
        );
        // Syncs of these fail before any request, so no I/O races the paused clock.
        let add = |id: &str| {
            let config = no_credentials("http://wb.invalid");
            let account = test_account(id, ReadLaterProvider::Wallabag, config, None);
            state.manager.lock().add_account(account).unwrap();
        };
        let tried = |id: &str| state.syncer.attempts.lock().contains_key(id);
        let second = std::time::Duration::from_secs(1);
        let tick = second * 60;
        add("a");
        let scheduler = Arc::clone(&state.syncer).spawn_scheduler(tick);

        tokio::time::sleep(tick - second).await;
        assert!(!tried("a"), "no pass before the first tick");
        tokio::time::sleep(second * 2).await;
        assert!(tried("a"), "a pass one tick after startup");
        add("b");
        tokio::time::sleep(tick).await;
        assert!(tried("b"), "and one every tick");
        scheduler.abort();
    }

    /// `with_scheduler` (what `feature_routes` calls) starts the scheduler unless it is off or
    /// there is no runtime.
    #[tokio::test]
    async fn with_scheduler_starts_it_unless_off() {
        let dir = tempfile::tempdir().unwrap();
        let state = |enabled: bool| {
            ReadLaterState::new(
                ReadLaterManager::new(&dir.path().join(format!("{enabled}.db"))).unwrap(),
                Storage::new(dir.path().join("storage")).unwrap(),
                broadcast::channel(4).0,
            )
            .with_scheduler(SchedulerConfig {
                enabled,
                tick: std::time::Duration::from_secs(60),
            })
        };
        let on = state(true);
        assert!(!on.scheduler.as_ref().expect("started").is_finished());
        assert!(state(false).scheduler.is_none());
    }

    #[test]
    fn with_scheduler_needs_a_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let state = ReadLaterState::new(
            ReadLaterManager::new(&dir.path().join("rl.db")).unwrap(),
            Storage::new(dir.path().join("storage")).unwrap(),
            broadcast::channel(4).0,
        )
        .with_scheduler(SchedulerConfig::from_vars(None, None));
        assert!(state.scheduler.is_none());
    }

    /// A sync requested over HTTP runs to completion even if the client goes away mid-sync.
    #[tokio::test]
    async fn a_dropped_request_does_not_cut_the_sync_short() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("wb", |_| {});
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *f.mock.gate.lock() = Some(Arc::clone(&gate));

        let mut request =
            Box::pin(readlater_router(f.state.clone()).oneshot(post_request("/accounts/wb/sync")));
        tokio::select! {
            _ = &mut request => panic!("answered before the fetch"),
            () = f.mock.listing.notified() => {}
        }
        drop(request); // the client goes away
        *f.mock.gate.lock() = None;
        gate.add_permits(1);

        let next = async {
            loop {
                match f.syncer().sync_account("wb").await {
                    Err(SyncError::AlreadyRunning(_)) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    other => break other.unwrap(),
                }
            }
        };
        let r = tokio::time::timeout(std::time::Duration::from_secs(10), next)
            .await
            .expect("the first sync ends");
        // The first sync delivered both articles, so this one finds nothing new.
        assert_eq!((r.articles_synced, r.articles_already_synced), (0, 2));
        assert_eq!(f.document_names(), ["Article 1", "Article 2"]);
        assert!(f.articles().iter().all(|a| a.synced_to_device));
        assert_eq!(f.pushes().len(), 1);
    }

    /// `POST /sync` still answers 200 when an account fails, with the failure in its result
    /// and the totals.
    #[tokio::test]
    async fn sync_all_reports_a_failing_account() {
        let f = Fixture::new(vec![entry(1)]).await;
        f.add_account("wb", |_| {});
        f.mock.fail_list.store(true, Ordering::SeqCst);
        let response = readlater_router(f.state.clone())
            .oneshot(post_request("/sync"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(
            (&body["total_errors"], &body["total_synced"]),
            (&json!(1), &json!(0))
        );
        let errors = body["results"][0]["errors"].as_array().unwrap();
        assert!(
            errors[0].as_str().unwrap().starts_with("Fetch"),
            "{errors:?}"
        );
    }

    /// A sync writes back only `last_sync` (and refreshed credentials), so edits to the
    /// account made while it runs are kept.
    #[tokio::test]
    async fn edits_made_during_a_sync_are_kept() {
        let f = Fixture::new(vec![entry(1)]).await;
        f.add_account("wb", |_| {});
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *f.mock.gate.lock() = Some(Arc::clone(&gate));
        let syncer = Arc::clone(&f.state.syncer);
        let sync = tokio::spawn(async move { syncer.sync_account("wb").await });
        f.mock.listing.notified().await;

        let mut edited = f.account("wb");
        edited.name = "Renamed".into();
        edited.sync_settings.max_articles = 7;
        f.state.manager.lock().update_account(edited).unwrap();
        gate.add_permits(1);
        let r = sync.await.unwrap().unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);

        let reopened = ReadLaterManager::new(&f.db).unwrap();
        let account = reopened.get_account("wb").unwrap();
        assert_eq!(
            (account.name.as_str(), account.sync_settings.max_articles),
            ("Renamed", 7)
        );
        assert!(account.last_sync.is_some());
    }

    /// A scheduled pass checks each account again right before its sync and before each
    /// delivery: an account disabled during its own sync stops delivering, and one whose
    /// `auto_sync` was turned off while an earlier account ran is skipped.
    #[tokio::test]
    async fn disabling_accounts_stops_a_scheduled_pass() {
        let mut f = Fixture::new(vec![entry(1), entry(2)]).await;
        f.add_account("a", |_| {});
        let (other, other_base) = spawn_mock(vec![entry(3)]).await;
        f.add_account_at("b", &other_base, |_| {});
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *f.mock.gate.lock() = Some(Arc::clone(&gate));
        let syncer = Arc::clone(&f.state.syncer);
        let now = Utc::now();
        let pass = tokio::spawn(async move { syncer.run_due(now).await });
        f.mock.listing.notified().await; // "a", the older account, is fetching

        let mut a = f.account("a");
        a.enabled = false;
        f.state.manager.lock().update_account(a).unwrap();
        let mut b = f.account("b");
        b.sync_settings.auto_sync = false;
        f.state.manager.lock().update_account(b).unwrap();
        gate.add_permits(1);

        let results = pass.await.unwrap();
        assert_eq!(results.len(), 1, "b was skipped");
        assert_eq!(results[0].account_id, "a");
        assert!(
            results[0]
                .errors
                .iter()
                .any(|e| e.starts_with("Stopped: the account was disabled")),
            "{:?}",
            results[0].errors
        );
        assert_eq!(results[0].articles_synced, 0);
        assert!(tree(&f.storage).is_empty());
        assert!(f.pushes().is_empty());
        assert_eq!(f.account("a").last_sync, None);
        assert_eq!(other.list_calls.load(Ordering::SeqCst), 0);
    }
}
