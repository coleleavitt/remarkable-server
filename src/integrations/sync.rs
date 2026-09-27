//! Bidirectional sync engine
//!
//! Handles sync between local remarkable storage and cloud providers.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::integrations::conflict::{
    Conflict,
    ConflictResolution,
    ConflictResolver,
    ConflictStrategy,
    ConflictType,
};
use crate::integrations::{
    CloudFile,
    CloudProvider,
    IntegrationError,
    ProviderType,
    Result,
    SyncFolderConfig,
};

/// Longest path segment, in bytes, that a local path may have: `NAME_MAX` on the usual Linux
/// filesystems (ext4, XFS, Btrfs). Dropbox and OneDrive allow names of 255 *characters*, so
/// a name with multi-byte characters can be valid remotely but impossible to create here.
pub(crate) const MAX_NAME_BYTES: usize = 255;

/// Split a *relative* sync path into components, rejecting anything that could escape
/// the sync root: absolute paths, `..`/`.`/empty segments, backslashes, NUL, drive prefixes.
/// Remote file names are attacker-controlled, so every local path is built from this.
/// Segments longer than [`MAX_NAME_BYTES`] are rejected too: they could never be written
/// locally, and rejecting them up front makes that a permanent failure rather than a write
/// error on every sync.
pub(crate) fn safe_components(path: &str) -> Result<Vec<&str>> {
    let bad = |why: &str| {
        Err(IntegrationError::InvalidPath(format!(
            "{:?}: {}",
            path, why
        )))
    };
    if path.is_empty() {
        return bad("empty");
    }
    if path.contains('\0') {
        return bad("NUL byte");
    }
    if path.contains('\\') {
        return bad("backslash");
    }
    if path.starts_with('/') {
        return bad("absolute path");
    }
    let parts: Vec<&str> = path.split('/').collect();
    if let Some(first) = parts.first() {
        let b = first.as_bytes();
        if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
            return bad("drive prefix");
        }
    }
    for part in &parts {
        if part.is_empty() || *part == "." || *part == ".." {
            return bad("empty, '.' or '..' segment");
        }
        if part.len() > MAX_NAME_BYTES {
            return bad("segment longer than 255 bytes");
        }
        let mut comps = Path::new(part).components();
        if !matches!(
            (comps.next(), comps.next()),
            (Some(std::path::Component::Normal(_)), None)
        ) {
            return bad("not a plain path segment");
        }
    }
    Ok(parts)
}

/// Whether a remote item name is usable as one local path segment. Providers build listing
/// paths from names, so an item (or a folder on its path) failing this is skipped.
pub(crate) fn is_safe_name(name: &str) -> bool {
    matches!(safe_components(name).as_deref(), Ok([_]))
}

/// Deepest relative path (in components) a provider listing or change feed returns; deeper
/// items are skipped, so a pathological tree can't make a sync walk forever.
pub(crate) const MAX_LIST_DEPTH: usize = 64;

/// Whether a provider listing can return a file at the sync path of `parts` (as
/// [`cloud_path_components`] splits it): no deeper than [`MAX_LIST_DEPTH`], and every
/// component a [safe name](is_safe_name) on its own. Listings check each name alone, so they
/// skip `Q:A.pdf` (a drive prefix) at any depth, while [`safe_components`] only takes one at
/// the start of a path for a drive prefix and so lets `sub/Q:A.pdf` be uploaded.
fn listable(parts: &[&str]) -> bool {
    parts.len() <= MAX_LIST_DEPTH && parts.iter().all(|p| is_safe_name(p))
}

/// Components of a provider path. Providers root paths at `/` (e.g. `/Notes/a.pdf`), meaning
/// the sync root, so exactly one leading slash is stripped before validation.
pub(crate) fn cloud_path_components(cloud_path: &str) -> Result<Vec<&str>> {
    safe_components(cloud_path.strip_prefix('/').unwrap_or(cloud_path))
}

/// Local destination for a provider path, guaranteed (lexically) to stay under `base`.
pub(crate) fn local_path_for(base: &Path, cloud_path: &str) -> Result<PathBuf> {
    let joined = cloud_path_components(cloud_path)?
        .iter()
        .fold(base.to_path_buf(), |p, c| p.join(c));
    if !joined.starts_with(base) || joined == base {
        return Err(IntegrationError::InvalidPath(format!(
            "{:?} escapes sync root",
            cloud_path
        )));
    }
    Ok(joined)
}

/// Create `root/rel` one level at a time from the canonical root, resolving each existing
/// entry and refusing (`Ok(None)`) any that lands outside the root or isn't a directory, so
/// a symlinked subdirectory can't make us create directories elsewhere. Returns the
/// canonical directory. `rel` must already be validated (plain `Normal` components).
pub(crate) async fn create_dirs_within(root: &Path, rel: &Path) -> Result<Option<PathBuf>> {
    let root = fs::canonicalize(root).await?;
    let mut cur = root.clone();
    for c in rel.components() {
        let next = cur.join(c);
        match fs::create_dir(&next).await {
            Ok(()) => {
                cur = next;
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let real = fs::canonicalize(&next).await?;
        if !real.starts_with(&root) || !fs::metadata(&real).await?.is_dir() {
            return Ok(None);
        }
        cur = real;
    }
    Ok(Some(cur))
}

/// Replace `target` (in `dir`) via a fresh temp file + rename: `create_new` never follows a
/// symlink and `rename` replaces the directory entry rather than writing through it, so a
/// symlink swapped in after our checks can't redirect the content. Also makes writes atomic.
/// Returns the metadata of the file written, taken before the rename (which keeps it), so it
/// describes what was written whatever happens to `target` afterwards.
async fn write_replace(dir: &Path, target: &Path, content: &[u8]) -> Result<std::fs::Metadata> {
    let tmp = dir.join(format!(".rms-sync-{}.tmp", uuid::Uuid::new_v4()));
    let res = async {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        write_durably(&mut f, content).await?;
        let written = f.metadata().await?;
        fs::rename(&tmp, target).await?;
        Ok::<_, std::io::Error>(written)
    }
    .await;
    if res.is_err() {
        let _ = fs::remove_file(&tmp).await;
    }
    Ok(res?)
}

/// Write all of `content` to `f` and fsync it. tokio's `File` returns from a write before the
/// data is written: the write runs in a background task, and its failure is reported only by
/// the next write or `flush`. `sync_all` waits for that task but drops its error, so without
/// the `flush` a short write (disk full, file size limit) would be renamed into place as a
/// complete file.
async fn write_durably(f: &mut fs::File, content: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    f.write_all(content).await?;
    f.flush().await?;
    f.sync_all().await
}

/// Classify a failed local write of the remote file at `cloud_path`: errors that come back the
/// same on every retry (something that isn't a directory where one is needed or the other way
/// round, a name the filesystem rejects) become the permanent
/// [`LocalPathUnusable`](IntegrationError::LocalPathUnusable), so they don't hold a delta
/// cursor forever. Other errors (disk full, permission denied) are left as they are.
fn local_write_error(cloud_path: &str, e: IntegrationError) -> IntegrationError {
    use std::io::ErrorKind::{InvalidFilename, IsADirectory, NotADirectory};
    match e {
        IntegrationError::Io(io)
            if matches!(io.kind(), IsADirectory | NotADirectory | InvalidFilename) =>
        {
            IntegrationError::LocalPathUnusable(format!("{:?}: {}", cloud_path, io))
        }
        e => e,
    }
}

/// File at the top of the local sync directory that records which old-layout directories (see
/// [`CloudProvider::legacy_layout_dir`]) full sync has already dealt with.
pub(crate) const LAYOUT_MARKER: &str = ".rms-sync-layout";

/// Directory at the top of the local sync directory that old-layout directories are moved into.
pub(crate) const OLD_LAYOUT_DIR: &str = ".rms-old-layout";

/// File at the top of the local sync directory that keeps the [manifest](SyncManifest) of the
/// last sync of each cloud folder synced into it, with [`SyncConfig::persist_state`].
pub(crate) const MANIFEST_FILE: &str = ".rms-sync-state.json";

/// Directory at the top of the local sync directory that local copies of files deleted remotely
/// are moved into, as `<run>/<path>` (`<run>` is the time of the sync, in UTC:
/// `20260926T101500Z`). A sync never deletes a local file.
pub(crate) const QUARANTINE_DIR: &str = ".rms-remote-deleted";

/// Whether `name` is one of the sync engine's own entries ([`LAYOUT_MARKER`],
/// [`OLD_LAYOUT_DIR`], [`MANIFEST_FILE`], [`QUARANTINE_DIR`]).
fn is_reserved_name(name: &str) -> bool {
    [LAYOUT_MARKER, OLD_LAYOUT_DIR, MANIFEST_FILE, QUARANTINE_DIR]
        .iter()
        .any(|r| name.eq_ignore_ascii_case(r))
}

/// Whether the sync path `path` (`/x/…`) is, or is in, one of the sync engine's own entries.
/// Such a path is never uploaded, and a remote file there is never written over them. Any
/// component counts, not just the first, so the entries of a sync directory nested in another
/// are the outer sync's as well.
fn is_reserved(path: &str) -> bool {
    path.strip_prefix('/')
        .unwrap_or(path)
        .split('/')
        .any(is_reserved_name)
}

/// `name` as the old layout is matched: case-insensitively (Dropbox and OneDrive paths are),
/// and percent-decoded (Graph's `parentReference.path`, which that layout came from, may be
/// percent-encoded).
fn fold_name(name: &str) -> String {
    urlencoding::decode(name)
        .map(|d| d.to_lowercase())
        .unwrap_or_else(|_| name.to_lowercase())
}

/// Whether the sync path `path` is `dir` (components, matched with [`fold_name`]) or below it.
fn is_under(path: &str, dir: &[String]) -> bool {
    let mut parts = path.strip_prefix('/').unwrap_or(path).split('/');
    dir.iter()
        .all(|d| parts.next().is_some_and(|p| fold_name(p) == fold_name(d)))
}

/// Contents of [`LAYOUT_MARKER`].
#[derive(Debug, Default, Serialize, Deserialize)]
struct LayoutMarker {
    /// Old-layout directories dealt with (moved aside, or found absent), as `/` and the
    /// [folded](fold_name) components joined by `/`.
    handled: Vec<String>,
}

/// Move every directory at `dir` under `root` into [`OLD_LAYOUT_DIR`], keeping its relative
/// path. Each component is matched with [`fold_name`], so every casing the old layout may have
/// written is found; symlinks aren't followed or moved. A name already taken in
/// [`OLD_LAYOUT_DIR`] gets a ` (2)`, ` (3)`… suffix. Returns what moved where, relative to
/// `root`.
async fn move_old_layout(root: &Path, dir: &[String]) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut found = vec![PathBuf::new()];
    for part in dir {
        let want = fold_name(part);
        let mut next = Vec::new();
        for rel in &found {
            let mut entries = match fs::read_dir(root.join(rel)).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if rel.as_os_str().is_empty() && is_reserved_name(name) {
                    continue;
                }
                if entry.file_type().await?.is_dir() && fold_name(name) == want {
                    next.push(rel.join(name));
                }
            }
        }
        found = next;
    }
    found.sort();

    let mut moved = Vec::new();
    for rel in found {
        let first = Path::new(OLD_LAYOUT_DIR).join(&rel);
        let mut to = first.clone();
        for n in 2.. {
            if fs::symlink_metadata(root.join(&to)).await.is_err() {
                break;
            }
            let name = first.file_name().unwrap_or_default().to_string_lossy();
            to = first.with_file_name(format!("{} ({})", name, n));
        }
        if let Some(parent) = to.parent() {
            fs::create_dir_all(root.join(parent)).await?;
        }
        fs::rename(root.join(&rel), root.join(&to)).await?;
        moved.push((rel, to));
    }
    Ok(moved)
}

/// Version of the [`MANIFEST_FILE`] format written by this server.
const MANIFEST_VERSION: u32 = 1;

/// What each file of a synced folder looked like, on each side, at the end of the last sync that
/// dealt with it, keyed by sync path (`/dir/file.pdf`). Full sync compares both sides with it (a
/// three-way reconciliation): a side that still matches its entry hasn't changed since, so a file
/// missing on one side and unchanged on the other was deleted there, not created here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncManifest {
    pub files: BTreeMap<String, ManifestEntry>,
}

/// One file of a [`SyncManifest`]. A side that is `None` was absent there: the file was deleted
/// on that side and the copy on the other kept (a sync never deletes remote files). It isn't
/// brought back while that copy stays as it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RemoteIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalIdentity>,
}

impl ManifestEntry {
    /// Both sides in step: `remote` as listed, and the local file that has its content.
    fn synced(remote: &CloudFile, local: LocalIdentity) -> Self {
        Self {
            remote: Some(RemoteIdentity::of(remote)),
            local: Some(local),
        }
    }
}

/// A remote file as its listing (or upload) described it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteIdentity {
    pub id: String,
    /// [`CloudFile::content_hash`]: Dropbox `content_hash`, Drive `md5Checksum`, OneDrive
    /// `sha256Hash` or `quickXorHash`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub size: u64,
    pub modified_at: i64,
}

impl RemoteIdentity {
    fn of(f: &CloudFile) -> Self {
        Self {
            id: f.id.clone(),
            content_hash: f.content_hash.clone(),
            size: f.size,
            modified_at: f.modified_at,
        }
    }

    /// Whether `f` is still this file: the same content hash and size when both carry a hash,
    /// otherwise the same id, size and modification time. Anything else counts as a change,
    /// which at worst transfers a file again.
    fn matches(&self, f: &CloudFile) -> bool {
        self.same(&Self::of(f))
    }

    /// Whether `other` is the same version of the file, as [`matches`](Self::matches) tells.
    fn same(&self, other: &Self) -> bool {
        match (&self.content_hash, &other.content_hash) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b) && self.size == other.size,
            _ => {
                self.id == other.id
                    && self.size == other.size
                    && self.modified_at == other.modified_at
            }
        }
    }
}

/// A local file as it was when last synced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalIdentity {
    pub size: u64,
    /// Modification time, in nanoseconds since the Unix epoch (`None` if not available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ns: Option<i64>,
    /// SHA-256 of the content, in hex: tells a file that was only touched from one edited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// A file found by the local scan.
#[derive(Debug, Clone)]
struct LocalFile {
    path: PathBuf,
    /// Modification time in whole seconds, as the conflict strategy compares it.
    mtime: i64,
    mtime_ns: Option<i64>,
    size: u64,
}

impl LocalFile {
    fn identity(&self, sha256: Option<String>) -> LocalIdentity {
        LocalIdentity {
            size: self.size,
            mtime_ns: self.mtime_ns,
            sha256,
        }
    }

    /// Whether this file is still as `base` recorded it: the same size and either the same
    /// modification time or (touched since) the same content. The file is only read in the
    /// second case.
    async fn matches(&self, base: &LocalIdentity) -> bool {
        if self.size != base.size {
            return false;
        }
        if self.mtime_ns.is_some() && self.mtime_ns == base.mtime_ns {
            return true;
        }
        let Some(hash) = &base.sha256 else {
            return false;
        };
        fs::read(&self.path)
            .await
            .is_ok_and(|content| sha256_hex(&content) == *hash)
    }
}

/// `meta`'s modification time in nanoseconds since the Unix epoch (negative before it).
fn mtime_ns(meta: &std::fs::Metadata) -> Option<i64> {
    let ns = match meta.modified().ok()?.duration_since(std::time::UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).ok()?,
        Err(before) => -i128::try_from(before.duration().as_nanos()).ok()?,
    };
    i64::try_from(ns).ok()
}

fn sha256_hex(content: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(content))
}

/// Contents of [`MANIFEST_FILE`]: a manifest for each provider, account and cloud folder synced
/// into the directory.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ManifestFile {
    version: u32,
    #[serde(default)]
    syncs: Vec<StoredManifest>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredManifest {
    provider: ProviderType,
    account: String,
    cloud_folder: String,
    /// When it was written (Unix seconds).
    synced_at: i64,
    files: BTreeMap<String, ManifestEntry>,
}

impl StoredManifest {
    fn is(&self, key: &ManifestKey) -> bool {
        self.provider == key.provider
            && self.account == key.account
            && self.cloud_folder == key.cloud_folder
    }
}

/// Which manifest in [`MANIFEST_FILE`] is a sync's: the provider, the account the token is for
/// ([`CloudProvider::account_id`]) and the cloud folder as configured (without a trailing
/// slash). The local directory is the one the file is in. A folder spelled another way gets a
/// manifest of its own, starting with a sync that infers no deletions.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestKey {
    provider: ProviderType,
    account: String,
    cloud_folder: String,
}

/// The [`MANIFEST_FILE`] in `root`; empty if there is none. One this server can't read (not
/// JSON, or written by a newer version) is an error: syncing as if there were none would bring
/// back what was deleted.
async fn read_manifest_file(root: &Path) -> Result<ManifestFile> {
    let path = root.join(MANIFEST_FILE);
    let bytes = match fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ManifestFile::default()),
        Err(e) => return Err(e.into()),
    };
    let bad = |why: String| IntegrationError::Serialization(format!("{}: {}", path.display(), why));
    let file: ManifestFile = serde_json::from_slice(&bytes).map_err(|e| bad(e.to_string()))?;
    if file.version > MANIFEST_VERSION {
        return Err(bad(format!(
            "version {} is newer than this server's {}",
            file.version, MANIFEST_VERSION
        )));
    }
    Ok(file)
}

/// `n` files, for a notice.
fn count(n: usize) -> String {
    match n {
        1 => "1 file".to_string(),
        n => format!("{} files", n),
    }
}

/// Up to 20 of `paths`, for a notice.
fn some_of(paths: &[String]) -> String {
    const SHOWN: usize = 20;
    let mut list = paths
        .iter()
        .take(SHOWN)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if paths.len() > SHOWN {
        list.push_str(&format!(" and {} more", paths.len() - SHOWN));
    }
    list
}

/// Move the local file `src` (the scan found it at the sync path `path`) to
/// `<root>/QUARANTINE_DIR/<run>/<path>`, adding ` (2)`, ` (3)`… if that name is taken. Only a
/// file whose directory resolves inside `root` is moved (not one reached through a symlink to
/// elsewhere), and the quarantine directories are created with [`create_dirs_within`]. The
/// directories the move leaves empty are removed (only empty ones), so a remote folder that
/// replaced the file, or a file that replaced its folder, can be downloaded. Returns where the
/// file went, relative to `root`.
async fn quarantine(root: &Path, run: &str, path: &str, src: &Path) -> Result<PathBuf> {
    let parts = cloud_path_components(path)?;
    let Some((name, dirs)) = parts.split_last() else {
        return Err(IntegrationError::InvalidPath(format!("{:?}: empty", path)));
    };
    let root = fs::canonicalize(root).await?;
    let outside =
        || IntegrationError::InvalidPath(format!("{:?} resolves outside sync root", path));
    let src_dir = fs::canonicalize(src.parent().ok_or_else(outside)?).await?;
    if !src_dir.starts_with(&root) {
        return Err(outside());
    }
    if fs::symlink_metadata(src).await?.is_dir() {
        return Err(IntegrationError::LocalPathUnusable(format!(
            "{:?}: a directory, not a file",
            path
        )));
    }

    let rel: PathBuf = [QUARANTINE_DIR, run].iter().chain(dirs).collect();
    let dir = create_dirs_within(&root, &rel).await?.ok_or_else(outside)?;
    let mut to = dir.join(name);
    for n in 2.. {
        if fs::symlink_metadata(&to).await.is_err() {
            break;
        }
        to = dir.join(format!("{} ({})", name, n));
    }
    fs::rename(src, &to).await?;

    let mut emptied = src_dir;
    while emptied != root && fs::remove_dir(&emptied).await.is_ok() {
        emptied.pop();
    }
    Ok(to.strip_prefix(&root).unwrap_or(&to).to_path_buf())
}

/// What [`match_case`] found, for the sync's result.
#[derive(Debug, Default)]
struct CaseMatch {
    /// The local paths of each clash, for an error.
    clashes: Vec<Vec<String>>,
    /// Files taken for changed on both sides because they were recorded under several
    /// spellings (see [`case_base`]), for a notice.
    undecided: Vec<String>,
}

/// For a provider whose paths ignore letter case ([`CloudProvider::ignores_case`]), line the
/// listing `cloud` and the last sync's manifest `base` up with the local tree, which tells
/// case apart, so that a file spelled otherwise on one side is taken for the same file rather
/// than for one deleted and another new. Such a provider can go on listing a file (or a folder
/// above it) under its old spelling after a case-only rename here or an upload into a folder
/// spelled otherwise, so a path recorded with the local spelling would look deleted remotely
/// at the next sync, and its local copy would be moved aside.
///
/// Paths are grouped by their lowercase form: the local files (`local`, and those over the
/// size limit, `local_too_large`), the listed files and the manifest entries. Where one local
/// file has the path, the listed file takes its spelling. Where none has it, the listed file
/// keeps its own, but for the directories above it that the local tree spells otherwise
/// ([`in_local_dirs`]): it comes down into those rather than into new ones beside them.
///
/// Several local files with the path are a clash: the provider keeps one file for them all, so
/// each upload would replace the others' content there, and each download would replace theirs
/// here with another's. The group is left out of the sync (taken out of `local` and `cloud` and
/// put in `out_of_view`, so its entries are kept as they were) until renamed apart, but for the
/// file spelled as the listing spells it, when no other spelling in the group has an entry:
/// then the others have never been synced, and that file syncs as it would alone. Were it synced
/// while another spelling has an entry, that entry would describe an older version of the one
/// remote file than the last synced (or the last written, when the state of the sync that wrote
/// it is lost: a lost reply, a crash before the state is saved). Compared with it once the clash
/// is settled, its local file, whose content the remote file may no longer have, would look
/// unchanged since the last sync and the remote file changed, and be overwritten. So a full sync
/// leaves a group one entry at most, which describes the last sync of the remote file.
///
/// The file synced is compared with its group's entry ([`case_base`]), put under its spelling,
/// and the group's other entries, if any, go: the file is recorded under one spelling from then
/// on.
fn match_case(
    local: &mut HashMap<String, LocalFile>,
    local_too_large: &[String],
    cloud: &mut HashMap<String, CloudFile>,
    mut base: Option<&mut SyncManifest>,
    out_of_view: &mut HashSet<String>,
) -> CaseMatch {
    #[derive(Default)]
    struct Spellings {
        local: BTreeSet<String>,
        cloud: BTreeSet<String>,
        recorded: BTreeSet<String>,
    }
    fn group<'a>(groups: &'a mut BTreeMap<String, Spellings>, path: &str) -> &'a mut Spellings {
        groups.entry(path.to_lowercase()).or_default()
    }
    let mut groups: BTreeMap<String, Spellings> = BTreeMap::new();
    for path in local.keys().chain(local_too_large) {
        group(&mut groups, path).local.insert(path.clone());
    }
    for (path, f) in cloud.iter() {
        if !f.is_folder {
            group(&mut groups, path).cloud.insert(path.clone());
        }
    }
    for path in base.iter().flat_map(|b| b.files.keys()) {
        group(&mut groups, path).recorded.insert(path.clone());
    }
    let dirs = local_dirs(local.keys().chain(local_too_large));

    let mut found = CaseMatch::default();
    for g in groups.into_values() {
        let clash = g.local.len() > 1 || g.cloud.len() > 1;
        let spelling = if clash {
            let listed = match (g.cloud.first(), g.cloud.len()) {
                (Some(c), 1) if g.local.contains(c) && g.recorded.iter().all(|r| r == c) => Some(c),
                _ => None,
            };
            let spellings: BTreeSet<&String> =
                g.local.iter().chain(&g.cloud).chain(&g.recorded).collect();
            for path in spellings.into_iter().filter(|p| Some(*p) != listed) {
                local.remove(path);
                cloud.remove(path);
                out_of_view.insert(path.clone());
            }
            found
                .clashes
                .push(g.local.union(&g.cloud).cloned().collect());
            match listed {
                Some(listed) => listed.clone(),
                None => continue,
            }
        } else if let Some(here) = g.local.first() {
            here.clone()
        } else if let Some(listed) = g.cloud.first() {
            in_local_dirs(listed, &dirs)
        } else {
            continue;
        };

        if let Some(listed) = g.cloud.first().filter(|c| **c != spelling) {
            if let Some(mut f) = cloud.remove(listed) {
                f.path = spelling.clone();
                cloud.insert(spelling.clone(), f);
            }
        }
        let Some(base) = base.as_deref_mut() else {
            continue;
        };
        match case_base(&base.files, &g.recorded, &spelling) {
            CaseBase::Own => {}
            CaseBase::Other(entry) => {
                base.files.insert(spelling.clone(), entry);
            }
            CaseBase::Undecided => {
                base.files.insert(
                    spelling.clone(),
                    ManifestEntry {
                        remote: None,
                        local: None,
                    },
                );
                found.undecided.push(spelling.clone());
            }
        }
        for other in g.recorded.iter().filter(|r| **r != spelling) {
            base.files.remove(other);
        }
    }
    found
}

/// Which entry [`case_base`] picks.
#[derive(Debug)]
enum CaseBase {
    /// The one recorded under the spelling synced, if any.
    Own,
    /// The one recorded under another spelling, to take its place.
    Other(ManifestEntry),
    /// None: there are several. An entry with neither side takes their place, which makes the
    /// file look changed on both sides.
    Undecided,
}

/// Which of the entries of a group of spellings that differ only in case (`recorded`, in
/// `files`) the file synced as `spelling` is compared with, where the provider ignores case.
///
/// [`match_case`] keeps a group to one entry, which describes the last sync of the one remote
/// file: that entry is taken, whatever its spelling. An entry taken from another spelling
/// describes another local file, or this one before a rename: it tells the file unchanged by its
/// content only, never by its modification time.
///
/// Several entries can only have been left by an earlier build, which synced a clash's listed
/// file while the others kept theirs, or by a [delta sync](CloudSync::delta_sync), which records
/// a download under the spelling listed. Then an entry can describe an older version of the
/// remote file than the last synced. Which is the newest can't be told, and compared with an
/// older one, a local file whose content the remote file no longer has would look unchanged and
/// be overwritten. So none is taken ([`CaseBase::Undecided`]): the file is taken for changed on
/// both sides until it is synced, and the conflict strategy decides (unless the content is the
/// same); a file on one side only is copied to the other.
fn case_base(
    files: &BTreeMap<String, ManifestEntry>,
    recorded: &BTreeSet<String>,
    spelling: &str,
) -> CaseBase {
    let mut entries = recorded.iter().filter_map(|r| files.get(r).map(|e| (r, e)));
    match (entries.next(), entries.next()) {
        (None, _) => CaseBase::Own,
        (Some((r, _)), None) if r == spelling => CaseBase::Own,
        (Some((_, e)), None) => {
            let mut e = e.clone();
            if let Some(local) = e.local.as_mut() {
                local.mtime_ns = None;
            }
            CaseBase::Other(e)
        }
        (Some(_), Some(_)) => CaseBase::Undecided,
    }
}

/// The directories above the local files `paths`, by lowercase path, with the spellings the
/// local tree has for each.
fn local_dirs<'a>(paths: impl Iterator<Item = &'a String>) -> HashMap<String, BTreeSet<String>> {
    let mut dirs: HashMap<String, BTreeSet<String>> = HashMap::new();
    for path in paths {
        let mut dir = path.as_str();
        while let Some(end) = dir.rfind('/').filter(|&end| end > 0) {
            dir = &path[..end];
            dirs.entry(dir.to_lowercase())
                .or_default()
                .insert(dir.to_string());
        }
    }
    dirs
}

/// `path`, a listed file that no local file has under any spelling, with the directories above
/// it spelled as the local tree spells them ([`local_dirs`]), so that it comes down into the
/// local directory rather than into a new one, spelled as the remote folder, beside it. The
/// deepest directory the local tree has decides; if the local tree has it under several
/// spellings, `path` is left as listed.
fn in_local_dirs(path: &str, dirs: &HashMap<String, BTreeSet<String>>) -> String {
    let mut dir = path;
    while let Some(end) = dir.rfind('/').filter(|&end| end > 0) {
        dir = &path[..end];
        if let Some(spellings) = dirs.get(&dir.to_lowercase()) {
            if let (Some(here), 1) = (spellings.first(), spellings.len()) {
                return format!("{}{}", here, &path[end..]);
            }
            break;
        }
    }
    path.to_string()
}

/// Sync direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirection {
    /// Upload only (local → cloud)
    Upload,
    /// Download only (cloud → local)
    Download,
    /// Bidirectional sync
    Bidirectional,
}

impl Default for SyncDirection {
    fn default() -> Self {
        Self::Bidirectional
    }
}

/// Sync operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub status: SyncStatus,
    pub uploaded: usize,
    pub downloaded: usize,
    /// Local copies of files deleted remotely, moved into [`QUARANTINE_DIR`] (a sync deletes
    /// nothing).
    pub deleted: usize,
    pub conflicts: Vec<Conflict>,
    pub errors: Vec<String>,
    /// Things done that the user should know about but that aren't failures (an old local
    /// layout moved aside).
    #[serde(default)]
    pub notices: Vec<String>,
    pub duration_ms: u64,
}

impl SyncResult {
    fn new() -> Self {
        Self {
            status: SyncStatus::Success,
            uploaded: 0,
            downloaded: 0,
            deleted: 0,
            conflicts: Vec::new(),
            errors: Vec::new(),
            notices: Vec::new(),
            duration_ms: 0,
        }
    }
}

/// Sync status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    Success,
    PartialSuccess,
    Failed,
    Cancelled,
}

/// Sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Local base path for sync
    pub local_path: PathBuf,
    /// Cloud folder ID or path (None = root)
    pub cloud_folder: Option<String>,
    /// Sync direction
    pub direction: SyncDirection,
    /// Conflict resolution strategy
    pub conflict_strategy: ConflictStrategy,
    /// Selective folder configs
    pub folder_configs: Vec<SyncFolderConfig>,
    /// File patterns to exclude (glob)
    pub exclude_patterns: Vec<String>,
    /// Maximum file size to sync (bytes)
    pub max_file_size: Option<u64>,
    /// Sync hidden files (starting with .)
    pub sync_hidden: bool,
    /// Keep the [manifest](SyncManifest) of the last sync in [`MANIFEST_FILE`] at the top of
    /// `local_path` (per provider, account and `cloud_folder`), so that a later `CloudSync`
    /// (`POST /sync` makes one per request) reconciles against it. Otherwise the manifest lasts
    /// only as long as this `CloudSync`.
    #[serde(default)]
    pub persist_state: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            local_path: PathBuf::from("."),
            cloud_folder: None,
            direction: SyncDirection::Bidirectional,
            conflict_strategy: ConflictStrategy::NewerWins,
            folder_configs: Vec::new(),
            exclude_patterns: vec![
                "*.tmp".into(),
                "*.temp".into(),
                ".DS_Store".into(),
                "Thumbs.db".into(),
            ],
            max_file_size: Some(100 * 1024 * 1024), // 100MB default
            sync_hidden: false,
            persist_state: false,
        }
    }
}

/// Sync state for tracking changes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncState {
    /// Last sync timestamp
    pub last_sync: Option<i64>,
    /// Provider-specific cursor for delta sync
    pub cursor: Option<String>,
    /// Map of local path to cloud file ID
    pub file_map: HashMap<String, String>,
    /// Map of local path to last known hash
    pub hash_map: HashMap<String, String>,
    /// Map of local path to last sync modification time
    pub mtime_map: HashMap<String, i64>,
    /// Manifest of the last full sync (see [`CloudSync::sync`]); `None` before the first, which
    /// therefore infers no deletions. Loaded from and saved to [`MANIFEST_FILE`] with
    /// [`SyncConfig::persist_state`].
    #[serde(default)]
    pub manifest: Option<SyncManifest>,
}

/// Cloud sync engine
pub struct CloudSync<P: CloudProvider> {
    provider: P,
    config: SyncConfig,
    state: SyncState,
    conflict_resolver: ConflictResolver,
    /// With [`SyncConfig::persist_state`], which stored manifest is this sync's; set once it is
    /// loaded.
    manifest_key: Option<ManifestKey>,
}

impl<P: CloudProvider> CloudSync<P> {
    pub fn new(provider: P, config: SyncConfig) -> Self {
        Self::with_state(provider, config, SyncState::default())
    }

    pub fn with_state(provider: P, config: SyncConfig, state: SyncState) -> Self {
        let conflict_resolver = ConflictResolver::new(config.conflict_strategy);
        Self {
            provider,
            config,
            state,
            conflict_resolver,
            manifest_key: None,
        }
    }

    /// Get current sync state
    pub fn state(&self) -> &SyncState {
        &self.state
    }

    /// Perform full sync, a three-way reconciliation of the remote listing and the local tree
    /// against the [manifest](SyncManifest) of the last full sync, path by path:
    ///
    /// - Unchanged on both sides since: nothing is done (no transfer, no conflict strategy).
    /// - Changed on one side only: that side's version is sent across, as far as the direction
    ///   allows (an upload-only sync never downloads, a download-only one never uploads; the
    ///   change then waits for a sync that can send it).
    /// - Changed on both: left alone if the provider vouches that the content is now the same
    ///   ([`CloudProvider::content_matches`]), otherwise the conflict strategy decides.
    /// - Deleted remotely and unchanged here: not uploaded again. The local copy is moved into
    ///   [`QUARANTINE_DIR`] (never deleted) and leaves the manifest; an upload-only sync leaves
    ///   it where it is, and the next sync that may download moves it. Deleted remotely but
    ///   changed here: a conflict, which keeps the local copy and uploads it again. When the
    ///   listing is empty, though, nothing is moved aside (an error says so instead): a sync
    ///   folder that was trashed, unshared or moved may list as empty.
    /// - Deleted here and unchanged remotely: not downloaded again, and not deleted remotely
    ///   either (a sync never deletes remote files); the manifest records it. Deleted here but
    ///   changed remotely: downloaded again.
    ///
    /// A path the manifest doesn't know (all of them on the first sync, or every sync without
    /// [`SyncConfig::persist_state`] and a new `CloudSync`) is synced as with no state: a file
    /// only present locally is uploaded, one only present remotely is downloaded, and one on
    /// both sides is left alone when the provider vouches for its content, or else goes
    /// through the conflict strategy. No deletion is inferred for such a path.
    ///
    /// The new manifest records every file that ended the sync in step on both sides, and the
    /// deletions kept as above. A file whose transfer failed keeps its old entry (or stays
    /// out), so the next sync tries again; so does a file uploaded over the one listed at its
    /// path that the provider stored as another file (a new id), since the listing would go on
    /// showing the old one. A path out of view (over the size limit on either side, hidden,
    /// excluded, one no listing returns, see [`visible`](Self::visible)) keeps its entry as it
    /// was. Nothing is recorded, or saved, when the sync fails as a whole (the listing, the
    /// local scan, the old-layout move below).
    ///
    /// With a provider that [ignores case](CloudProvider::ignores_case), a local path and a
    /// listed or recorded one that differ only in letter case are one file, kept under the
    /// local spelling (see [`match_case`]): a case-only rename here is neither a deletion nor
    /// an upload of a new file, and a remote one changes nothing here. A new remote file in a
    /// folder spelled otherwise here comes down into the local directory. Local files that
    /// differ only in case from each other are left alone, with an error, until renamed apart
    /// or all but one removed, but for the one spelled as listed as long as none of the others
    /// was synced before: so the one remote file is never written from one of them while
    /// another's record describes it, and once the clash is settled, the file left is compared
    /// with the one record of the last sync of the remote file. A file recorded under several
    /// such spellings (by an earlier build, or a delta sync) is taken for changed on both
    /// sides, with a notice (see [`case_base`]).
    ///
    /// The first full sync of a folder the provider kept elsewhere locally before #34 (see
    /// [`CloudProvider::legacy_layout_dir`]) moves that directory aside first; see
    /// [`move_legacy_layout`](Self::move_legacy_layout). That sync goes without a manifest.
    pub async fn sync(&mut self) -> Result<SyncResult> {
        Ok(self.reconcile(LocalOnly::Upload).await?.result)
    }

    /// With [`SyncConfig::persist_state`], find out which account the provider is signed in to
    /// and load the manifest of this folder's last sync from [`MANIFEST_FILE`], once per
    /// `CloudSync`. If there is none (the first sync of this folder with this account), the
    /// state is left as it is.
    async fn load_manifest(&mut self) -> Result<()> {
        if !self.config.persist_state || self.manifest_key.is_some() {
            return Ok(());
        }
        let key = ManifestKey {
            provider: self.provider.provider_type(),
            account: self.provider.account_id().await?.unwrap_or_default(),
            cloud_folder: self
                .config
                .cloud_folder
                .as_deref()
                .unwrap_or("")
                .trim_end_matches('/')
                .to_string(),
        };
        if self.state.manifest.is_none() {
            let stored = read_manifest_file(&self.config.local_path).await?;
            self.state.manifest = stored
                .syncs
                .into_iter()
                .find(|s| s.is(&key))
                .map(|s| SyncManifest { files: s.files });
        }
        self.manifest_key = Some(key);
        Ok(())
    }

    /// Write the manifest to [`MANIFEST_FILE`] in place of this folder's previous one (the other
    /// folders' are kept), atomically, and fsync it and its directory. Nothing to do unless it
    /// was [loaded](Self::load_manifest).
    async fn save_manifest(&self) -> Result<()> {
        let (Some(key), Some(manifest)) = (&self.manifest_key, &self.state.manifest) else {
            return Ok(());
        };
        let root = &self.config.local_path;
        let mut stored = read_manifest_file(root).await?;
        stored.version = MANIFEST_VERSION;
        stored.syncs.retain(|s| !s.is(key));
        stored.syncs.push(StoredManifest {
            provider: key.provider,
            account: key.account.clone(),
            cloud_folder: key.cloud_folder.clone(),
            synced_at: chrono::Utc::now().timestamp(),
            files: manifest.files.clone(),
        });
        let json = serde_json::to_vec(&stored)
            .map_err(|e| IntegrationError::Serialization(e.to_string()))?;
        write_replace(root, &root.join(MANIFEST_FILE), &json).await?;
        fs::File::open(root).await?.sync_all().await?;
        Ok(())
    }

    /// Whether the local scan would list a file at the sync path `path` if there was one (within
    /// the size limit): not one of the sync's own entries, no hidden component unless hidden
    /// files are synced, and none excluded by pattern or by selective sync. Only such paths are
    /// kept in the manifest: for any other, a missing local copy tells nothing.
    ///
    /// So must a provider listing be able to return a file there ([`listable`]): a file the
    /// listing never shows would look deleted remotely right after its upload, and be moved
    /// aside. A local file at such a path is synced as with no state (uploaded by every sync
    /// that uploads, as before there was any), and never taken for deleted.
    fn visible(&self, path: &str) -> bool {
        let Ok(parts) = cloud_path_components(path) else {
            return false;
        };
        if !listable(&parts) {
            return false;
        }
        let mut dir = self.config.local_path.clone();
        for (i, part) in parts.iter().enumerate() {
            if (!self.config.sync_hidden && part.starts_with('.')) || self.should_exclude(part) {
                return false;
            }
            if i + 1 < parts.len() {
                dir.push(part);
                if !self.should_sync_folder(&dir) {
                    return false;
                }
            }
        }
        !is_reserved(path)
    }

    /// Record in the manifest, if there is one, that `f` was just downloaded as `local`, so the
    /// next full sync finds both sides unchanged instead of changed on both.
    fn record_download(&mut self, f: &CloudFile, local: LocalIdentity) {
        if !self.visible(&f.path) {
            return;
        }
        if let Some(manifest) = self.state.manifest.as_mut() {
            manifest
                .files
                .insert(f.path.clone(), ManifestEntry::synced(f, local));
        }
    }

    /// Move the local directory where this folder's files were kept before #34 (see
    /// [`CloudProvider::legacy_layout_dir`]) into [`OLD_LAYOUT_DIR`], once. In the layout used
    /// now, that directory is a subfolder of the same name (`<local>/Notes/a.pdf` is
    /// `/Notes/Notes/a.pdf`), so syncing it would copy the old files into the folder one level
    /// down. Which directories were dealt with is recorded in [`LAYOUT_MARKER`], also when
    /// nothing was there to move: after the first full sync, a directory of that name is a
    /// real subfolder and is synced like any other.
    ///
    /// Returns notices for the result: what was moved, and whether `cloud` (the folder's
    /// listing) has a subfolder at that path, which may be the duplicate that versions before
    /// #34 uploaded from the second sync on. Also whether anything was moved.
    async fn move_legacy_layout(
        &self,
        cloud: &HashMap<String, CloudFile>,
    ) -> Result<(Vec<String>, bool)> {
        let Some(dir) = self
            .provider
            .legacy_layout_dir(self.config.cloud_folder.as_deref())
            .await?
            .filter(|dir| !dir.is_empty())
        else {
            return Ok((Vec::new(), false));
        };
        let root = &self.config.local_path;
        let marker_path = root.join(LAYOUT_MARKER);
        let mut marker: LayoutMarker = match fs::read(&marker_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                IntegrationError::Serialization(format!("{}: {}", marker_path.display(), e))
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => LayoutMarker::default(),
            Err(e) => return Err(e.into()),
        };
        let folded: Vec<String> = dir.iter().map(|c| fold_name(c)).collect();
        let key = format!("/{}", folded.join("/"));
        if marker.handled.contains(&key) {
            return Ok((Vec::new(), false));
        }

        let mut notices = Vec::new();
        let moved = move_old_layout(root, &dir).await?;
        for (from, to) in &moved {
            notices.push(format!(
                "Moved {} to {}: versions before #34 kept this folder's files there, under the \
                 folder's own path from the drive root, and they now go at their path inside \
                 the folder. Copy back anything changed there since the last sync, then delete it",
                from.display(),
                to.display()
            ));
        }
        if cloud.keys().any(|p| is_under(p, &dir)) {
            let shown = format!("/{}", dir.join("/"));
            let what_to_do = if self.config.persist_state {
                format!(
                    "delete it remotely: the next sync then moves its local copy {shown}, if one \
                     was downloaded, into {QUARANTINE_DIR}/"
                )
            } else {
                format!(
                    "delete it remotely, and its local copy {shown} here if a sync downloaded \
                     one, before the next sync: either copy left behind brings the other back"
                )
            };
            notices.push(format!(
                "This folder has a subfolder {shown}. From their second sync on, versions before \
                 #34 uploaded the folder's files into it ({shown}/…). If that is what it holds, \
                 {what_to_do}"
            ));
        }

        marker.handled.push(key);
        let json = serde_json::to_vec_pretty(&marker)
            .map_err(|e| IntegrationError::Serialization(e.to_string()))?;
        write_replace(root, &marker_path, &json).await?;
        for notice in &notices {
            tracing::warn!("cloud sync: {}", notice);
        }
        Ok((notices, !moved.is_empty()))
    }

    /// Whether `f` is a file over `max_file_size`. Such remote files are never fetched: a
    /// download is held in memory whole and lands on the server's own disk (the local scan
    /// skips oversized files the same way).
    fn too_large(&self, f: &CloudFile) -> bool {
        !f.is_folder && self.config.max_file_size.is_some_and(|max| f.size > max)
    }

    /// Whether the local file `local` (at the sync path `path`) has the content of `cloud_file`:
    /// the same size, and the provider [vouches for its hash](CloudProvider::content_matches).
    /// The local file is only read when the sizes match. If so, the state records the file as
    /// in sync, and its identity for the manifest is returned.
    async fn same_on_both_sides(
        &mut self,
        path: &str,
        local: &LocalFile,
        cloud_file: &CloudFile,
    ) -> Option<LocalIdentity> {
        if cloud_file.is_folder
            || cloud_file.content_hash.is_none()
            || cloud_file.size != local.size
        {
            return None;
        }
        let content = fs::read(&local.path).await.ok()?;
        if !self.provider.content_matches(cloud_file, &content) {
            return None;
        }
        self.state
            .file_map
            .insert(path.to_string(), cloud_file.id.clone());
        if let Some(hash) = &cloud_file.content_hash {
            self.state.hash_map.insert(path.to_string(), hash.clone());
        }
        self.state.mtime_map.insert(path.to_string(), local.mtime);
        Some(LocalIdentity {
            size: content.len() as u64,
            mtime_ns: local.mtime_ns,
            sha256: Some(sha256_hex(&content)),
        })
    }

    /// Full sync: compare the whole listing and the local tree with the manifest of the last
    /// sync, transfer what differs, and record the new manifest (see [`sync`](Self::sync)).
    async fn reconcile(&mut self, local_only: LocalOnly) -> Result<Reconciled> {
        let start = std::time::Instant::now();
        let mut result = SyncResult::new();
        let failed = |mut result: SyncResult, error: String| -> Result<Reconciled> {
            result.status = SyncStatus::Failed;
            result.errors.push(error);
            result.duration_ms = start.elapsed().as_millis() as u64;
            Ok(Reconciled {
                result,
                retry_needed: true,
            })
        };

        // Without the manifest a deletion can't be told from a new file: rather than sync as if
        // there were none (bringing deleted files back), nothing is synced.
        if let Err(e) = self.load_manifest().await {
            return failed(result, format!("Failed to load the sync state: {}", e));
        }

        // Get cloud files
        let cloud_files = match self
            .provider
            .list_files(self.config.cloud_folder.as_deref())
            .await
        {
            Ok(files) => files,
            Err(e) => return failed(result, format!("Failed to list cloud files: {}", e)),
        };

        // An empty listing may be a sync folder that was trashed, unshared or moved rather than
        // one whose files were all deleted: then nothing is taken for deleted remotely.
        let listed_nothing = cloud_files.is_empty();

        // Build cloud file map
        let mut cloud_map: HashMap<String, CloudFile> = cloud_files
            .into_iter()
            .map(|f| (f.path.clone(), f))
            .collect();

        // Before the local tree is read: syncing the old layout would copy it into the folder
        // one level down, so nothing is synced until it has been moved aside.
        let moved_old_layout = match self.move_legacy_layout(&cloud_map).await {
            Ok((notices, moved)) => {
                result.notices = notices;
                moved
            }
            Err(e) => {
                return failed(
                    result,
                    format!("Failed to move the old local layout aside: {}", e),
                );
            }
        };
        cloud_map.retain(|path, _| {
            let keep = !is_reserved(path);
            if !keep {
                tracing::warn!(
                    "cloud sync: skipping {:?}: a name the sync keeps for itself",
                    path
                );
            }
            keep
        });

        // Get local files
        let (mut local_files, local_too_large) = match self.list_local_files().await {
            Ok((mut files, too_large)) => {
                files.retain(|path, _| !is_reserved(path));
                (files, too_large)
            }
            Err(e) => return failed(result, format!("Failed to list local files: {}", e)),
        };

        // Moving the old layout aside changed the local tree the manifest describes, so that
        // sync goes without one (as a first sync does) and records a fresh one.
        let mut base = match moved_old_layout {
            true => None,
            false => self.state.manifest.take(),
        };

        // Paths left alone in both directions, whose manifest entries stay as they were: they
        // are never taken for deleted.
        let mut out_of_view: HashSet<String> = HashSet::new();

        // A provider that ignores case may list a file under another spelling than the local
        // one; before the size limit is applied, so that applies to the file whatever its
        // spelling on each side.
        let case = match self.provider.ignores_case() {
            true => match_case(
                &mut local_files,
                &local_too_large,
                &mut cloud_map,
                base.as_mut(),
                &mut out_of_view,
            ),
            false => CaseMatch::default(),
        };

        // A path whose file is too large on either side is left alone in both directions: a
        // remote one isn't downloaded and a local file there isn't uploaded over it, a local one
        // isn't uploaded and a remote file there isn't downloaded over it. Out of view this way,
        // it isn't taken for deleted either: its manifest entry stays as it was.
        cloud_map.retain(|path, f| {
            let keep = !self.too_large(f);
            if !keep {
                tracing::warn!(
                    "cloud sync: skipping {:?}: {} bytes is over the size limit",
                    path,
                    f.size
                );
                local_files.remove(path);
                out_of_view.insert(path.clone());
            }
            keep
        });
        for path in local_too_large {
            if cloud_map.remove(&path).is_some() {
                tracing::warn!(
                    "cloud sync: skipping {:?}: the local file is over the size limit",
                    path
                );
            }
            out_of_view.insert(path);
        }

        // Set when a remote file wasn't fetched for a reason that may go away; a resync then
        // keeps its old cursor so the fetch is retried (see `resync`).
        let mut retry_needed = false;

        let can_upload = self.config.direction != SyncDirection::Download;
        let can_download = self.config.direction != SyncDirection::Upload;
        let upload_new = can_upload && local_only == LocalOnly::Upload;

        let mut manifest = SyncManifest::default();
        let mut plan: Vec<Planned> = Vec::new();
        // Deleted here, unchanged remotely, and seen for the first time.
        let mut kept_remotely: Vec<String> = Vec::new();
        // Missing from an empty listing and unchanged here: left in place (see `listed_nothing`).
        let mut not_moved: Vec<String> = Vec::new();

        let mut paths: BTreeSet<String> = local_files.keys().cloned().collect();
        paths.extend(cloud_map.keys().cloned());
        if let Some(base) = &base {
            paths.extend(base.files.keys().cloned());
        }
        for path in paths {
            let local = local_files.get(&path);
            let cloud = cloud_map.get(&path);
            let tracked = !out_of_view.contains(&path) && self.visible(&path);
            let entry = match (tracked, base.as_ref().and_then(|b| b.files.get(&path))) {
                (true, Some(entry)) => entry,
                (tracked, entry) => {
                    if let Some(entry) = entry {
                        // Out of view: kept as it was.
                        manifest.files.insert(path.clone(), entry.clone());
                    }
                    // Unknown to the manifest: synced as with no state.
                    let step = match (local, cloud) {
                        (Some(_), None) if upload_new => Step::Upload,
                        (None, Some(f)) if !f.is_folder && can_download => Step::Download,
                        (Some(_), Some(_)) => Step::Both { known: false },
                        _ => continue,
                    };
                    plan.push(Planned {
                        path,
                        step,
                        track: tracked,
                        old: None,
                    });
                    continue;
                }
            };

            // Only files are recorded: a folder where the file was means the file is gone.
            let file = cloud.filter(|f| !f.is_folder);
            let local_changed = match (local, &entry.local) {
                (Some(l), Some(base)) => !l.matches(base).await,
                (None, None) => false,
                _ => true,
            };
            let remote_changed = match (file, &entry.remote) {
                (Some(f), Some(base)) => !base.matches(f),
                (None, None) => false,
                _ => true,
            };
            let step = match (local_changed, remote_changed, local, file) {
                // Gone from both sides: the entry goes too. First, for an entry with neither side
                // (see `case_base`), which would otherwise look unchanged on both.
                (_, _, None, None) => continue,
                // Unchanged since the last sync on both sides (a deletion kept included). The
                // entry is refreshed, so a file only touched isn't read again next time.
                (false, false, ..) => {
                    let sha256 = entry.local.as_ref().and_then(|l| l.sha256.clone());
                    manifest.files.insert(
                        path,
                        ManifestEntry {
                            remote: file.map(RemoteIdentity::of),
                            local: local.map(|l| l.identity(sha256)),
                        },
                    );
                    continue;
                }
                // Changed here only.
                (true, false, Some(_), _)
                    if can_upload && (file.is_some() || local_only == LocalOnly::Upload) =>
                {
                    Step::Upload
                }
                (true, false, None, Some(f)) => {
                    tracing::info!(
                        "cloud sync: {:?} was deleted here; keeping the remote copy",
                        path
                    );
                    kept_remotely.push(path.clone());
                    manifest.files.insert(
                        path,
                        ManifestEntry {
                            remote: Some(RemoteIdentity::of(f)),
                            local: None,
                        },
                    );
                    continue;
                }
                // Changed remotely only.
                (false, true, _, Some(_)) if can_download => Step::Download,
                (false, true, Some(_), None) if can_download && listed_nothing => {
                    not_moved.push(path.clone());
                    manifest.files.insert(path, entry.clone());
                    continue;
                }
                (false, true, Some(_), None) if can_download => Step::Quarantine,
                (false, true, Some(l), None) => {
                    // Upload-only never touches local files: the copy stays, and isn't uploaded
                    // again unless it changes. The remote side stays as the last sync saw it,
                    // so the next sync that may download still finds it deleted and moves the
                    // copy aside.
                    let sha256 = entry.local.as_ref().and_then(|l| l.sha256.clone());
                    manifest.files.insert(
                        path,
                        ManifestEntry {
                            remote: entry.remote.clone(),
                            local: Some(l.identity(sha256)),
                        },
                    );
                    continue;
                }
                // Changed on both sides.
                (true, true, Some(_), Some(_)) => Step::Both { known: true },
                (true, true, Some(_), None) if upload_new => {
                    tracing::warn!(
                        "cloud sync: {:?} was deleted remotely but changed here since the last \
                         sync; uploading it again",
                        path
                    );
                    Step::Reupload
                }
                (true, true, None, Some(_)) if can_download => Step::Download,
                // A change the direction doesn't send (or a resync, which leaves files only
                // present here alone): the entry stays, so it is still a change next time.
                _ => {
                    manifest.files.insert(path, entry.clone());
                    continue;
                }
            };
            plan.push(Planned {
                path,
                step,
                track: true,
                old: Some(entry.clone()),
            });
        }

        // Stable, so paths stay sorted within a step. Local copies are moved aside first, so a
        // remote folder that replaced a file, or a file that replaced a folder, can come down.
        plan.sort_by_key(|p| p.step.order());
        let run = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let mut quarantined = Vec::new();
        for Planned {
            path,
            step,
            track,
            old,
        } in plan
        {
            let recorded = match step {
                Step::Quarantine => {
                    let src = &local_files[&path].path;
                    match quarantine(&self.config.local_path, &run, &path, src).await {
                        Ok(to) => {
                            tracing::info!(
                                "cloud sync: {:?} was deleted remotely; moved the local copy to {}",
                                path,
                                to.display()
                            );
                            result.deleted += 1;
                            quarantined.push(path);
                            continue;
                        }
                        Err(e) => {
                            result.errors.push(format!(
                                "Moving aside {} (deleted remotely) failed: {}",
                                path, e
                            ));
                            None
                        }
                    }
                }
                Step::Upload | Step::Reupload => {
                    let over = cloud_map.get(&path).filter(|f| !f.is_folder);
                    let entry = self
                        .upload_counted(&path, &local_files[&path], over, &mut result)
                        .await;
                    if entry.is_some() && step == Step::Reupload {
                        result.notices.push(format!(
                            "{} was deleted remotely but changed here since the last sync: \
                             uploaded it again",
                            path
                        ));
                    }
                    entry
                }
                Step::Download => {
                    self.download_counted(&cloud_map[&path], &mut result, &mut retry_needed)
                        .await
                }
                Step::Both { known } => {
                    self.sync_both(
                        &path,
                        &local_files[&path],
                        &cloud_map[&path],
                        known,
                        &mut result,
                        &mut retry_needed,
                    )
                    .await
                }
            };
            // A failure keeps the old entry (or none), so the next sync sees the same change.
            if let Some(entry) = recorded.or(old).filter(|_| track) {
                manifest.files.insert(path, entry);
            }
        }

        if !quarantined.is_empty() {
            result.notices.push(format!(
                "{} deleted remotely since the last sync and unchanged here, moved to {}/{}/ \
                 rather than deleted: {}",
                count(quarantined.len()),
                QUARANTINE_DIR,
                run,
                some_of(&quarantined)
            ));
        }
        if !not_moved.is_empty() {
            result.errors.push(format!(
                "The cloud folder listed nothing, but {} there at the last sync and unchanged \
                 here since were left in place rather than moved aside as deleted remotely: \
                 check that the folder still exists and is shared with this account. If they \
                 were deleted remotely, delete the local copies: {}",
                count(not_moved.len()),
                some_of(&not_moved)
            ));
        }
        if !case.clashes.is_empty() {
            let shown: Vec<String> = case.clashes.iter().map(|c| c.join(" and ")).collect();
            tracing::warn!(
                "cloud sync: paths that differ only in letter case, left alone: {}",
                shown.join("; ")
            );
            result.errors.push(format!(
                "The cloud folder takes paths that differ only in letter case for one file, but \
                 here they name different files: {}. Such a set is left alone (nothing uploaded, \
                 downloaded or moved aside) until renamed apart or all but one removed, but for \
                 the file spelled as the cloud folder lists it, synced as long as none of the \
                 others was synced before",
                some_of(&shown)
            ));
        }
        if !case.undecided.is_empty() {
            result.notices.push(format!(
                "{} recorded under several spellings that differ only in letter case, so which \
                 record tells the version last synced can't be told: each was taken for changed \
                 on both sides (the conflict strategy decides, unless the content is the same; a \
                 file on one side only is copied to the other): {}",
                count(case.undecided.len()),
                some_of(&case.undecided)
            ));
        }
        if !kept_remotely.is_empty() {
            result.notices.push(format!(
                "{} deleted here since the last sync and unchanged remotely, kept there (a sync \
                 never deletes remote files) and not downloaded again unless changed there: {}",
                count(kept_remotely.len()),
                some_of(&kept_remotely)
            ));
        }

        // Update state
        self.state.last_sync = Some(chrono::Utc::now().timestamp());
        self.state.manifest = Some(manifest);
        if let Err(e) = self.save_manifest().await {
            tracing::error!("cloud sync: failed to save the sync state: {}", e);
            result
                .errors
                .push(format!("Failed to save the sync state: {}", e));
        }

        // Determine final status
        if !result.errors.is_empty() {
            result.status = if result.uploaded > 0 || result.downloaded > 0 || result.deleted > 0 {
                SyncStatus::PartialSuccess
            } else {
                SyncStatus::Failed
            };
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(Reconciled {
            result,
            retry_needed,
        })
    }

    /// A file present on both sides and not unchanged on both since the last sync. `known`: the
    /// manifest says both changed since; without an entry (`false`), conflicts are detected as
    /// with no state. Returns the manifest entry when the two sides end up in step.
    async fn sync_both(
        &mut self,
        path: &str,
        local: &LocalFile,
        cloud_file: &CloudFile,
        known: bool,
        result: &mut SyncResult,
        retry_needed: &mut bool,
    ) -> Option<ManifestEntry> {
        // Nothing to send either way. Without this, a sync with no state from an earlier one
        // sees every such file as changed on both sides and, as a download is stamped with the
        // time it was written, uploads each one again.
        if let Some(identity) = self.same_on_both_sides(path, local, cloud_file).await {
            return Some(ManifestEntry::synced(cloud_file, identity));
        }

        // Check for conflicts
        let conflict = match known {
            true => Some(Conflict {
                local_path: local.path.clone(),
                cloud_file: cloud_file.clone(),
                local_modified_at: local.mtime,
                local_size: local.size,
                conflict_type: ConflictType::BothModified,
                resolution: None,
            }),
            false => ConflictResolver::detect_conflict(
                &local.path,
                local.mtime,
                local.size,
                true,
                Some(cloud_file),
                self.state.last_sync,
            ),
        };
        let direction = self.config.direction;

        let Some(mut conflict) = conflict else {
            // No conflict - sync based on modification time
            let last_sync = self.state.mtime_map.get(path).copied().unwrap_or(0);
            if local.mtime > last_sync && direction != SyncDirection::Download {
                // Local is newer
                return self
                    .upload_counted(path, local, Some(cloud_file), result)
                    .await;
            } else if cloud_file.modified_at > last_sync && direction != SyncDirection::Upload {
                // Cloud is newer
                return self
                    .download_counted(cloud_file, result, retry_needed)
                    .await;
            }
            return None;
        };

        match self.conflict_resolver.resolve(&mut conflict) {
            ConflictResolution::UseLocal if direction != SyncDirection::Download => {
                self.upload_counted(path, local, Some(cloud_file), result)
                    .await
            }
            ConflictResolution::UseCloud if direction != SyncDirection::Upload => {
                self.download_counted(cloud_file, result, retry_needed)
                    .await
            }
            ConflictResolution::KeepBoth { renamed_to } => {
                // Download cloud version with new name. The two sides still differ at `path`,
                // so it isn't recorded: the local version there was never sent.
                let mut renamed_file = cloud_file.clone();
                renamed_file.name = renamed_to;
                renamed_file.path = format!(
                    "{}/{}",
                    cloud_file
                        .path
                        .rsplit_once('/')
                        .map(|(p, _)| p)
                        .unwrap_or(""),
                    renamed_file.name
                );

                if direction != SyncDirection::Upload {
                    match self.download_file(&renamed_file).await {
                        Ok(_) => result.downloaded += 1,
                        Err(e) => {
                            *retry_needed |= !e.is_permanent();
                            result
                                .errors
                                .push(format!("Download conflict copy failed: {}", e));
                        }
                    }
                }
                None
            }
            ConflictResolution::ManualRequired => {
                result.conflicts.push(conflict);
                None
            }
            _ => None,
        }
    }

    /// [Upload](Self::upload_file) `local`, counting it in `result`; its manifest entry, if it
    /// went. `over` is the remote file listed at `path`, if any, which the upload replaces.
    ///
    /// If the provider stored the upload as a file other than `over` (another id), the listing
    /// may go on showing `over`, so recording the upload would make the next sync take `over`
    /// for a remote change and download it over the local file. Then no entry is returned (the
    /// old one is kept, so the next sync sees the same local change) and the result has an
    /// error.
    async fn upload_counted(
        &mut self,
        path: &str,
        local: &LocalFile,
        over: Option<&CloudFile>,
        result: &mut SyncResult,
    ) -> Option<ManifestEntry> {
        match self.upload_file(path, local).await {
            Ok(entry) => {
                result.uploaded += 1;
                let stored = entry.remote.as_ref().map(|r| r.id.as_str());
                if let Some(over) = over.filter(|over| stored != Some(over.id.as_str())) {
                    tracing::warn!(
                        "cloud sync: {:?} was uploaded as {:?}, not over the listed {:?}",
                        path,
                        stored,
                        over.id
                    );
                    result.errors.push(format!(
                        "Uploaded {}, but it was stored as a new file ({}) next to the one listed \
                         there ({}) rather than replacing it; the next sync sends it again",
                        path,
                        stored.unwrap_or("?"),
                        over.id
                    ));
                    return None;
                }
                Some(entry)
            }
            Err(e) => {
                result.errors.push(format!("Upload {} failed: {}", path, e));
                None
            }
        }
    }

    /// [Download](Self::download_file) `cloud_file`, counting it in `result`; its manifest
    /// entry, if it came. A failure that may go away sets `retry_needed`.
    async fn download_counted(
        &mut self,
        cloud_file: &CloudFile,
        result: &mut SyncResult,
        retry_needed: &mut bool,
    ) -> Option<ManifestEntry> {
        match self.download_file(cloud_file).await {
            Ok(local) => {
                result.downloaded += 1;
                Some(ManifestEntry::synced(cloud_file, local))
            }
            Err(e) => {
                *retry_needed |= !e.is_permanent();
                result
                    .errors
                    .push(format!("Download {} failed: {}", cloud_file.path, e));
                None
            }
        }
    }

    /// The local files by sync path (`/dir/file.pdf`), and the paths of those skipped for
    /// being over the size limit.
    async fn list_local_files(&self) -> Result<(HashMap<String, LocalFile>, Vec<String>)> {
        let mut files = HashMap::new();
        let mut too_large = Vec::new();
        let root = &self.config.local_path;
        self.scan_directory(root, root, &mut files, &mut too_large)
            .await?;
        Ok((files, too_large))
    }

    /// Recursively scan directory
    async fn scan_directory(
        &self,
        base: &Path,
        dir: &Path,
        files: &mut HashMap<String, LocalFile>,
        too_large: &mut Vec<String>,
    ) -> Result<()> {
        let mut entries = fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Skip hidden files if configured
            if !self.config.sync_hidden && name.starts_with('.') {
                continue;
            }

            // Check exclude patterns
            if self.should_exclude(&name) {
                continue;
            }

            let metadata = entry.metadata().await?;

            if metadata.is_dir() {
                // Check if folder is in selective sync
                if self.should_sync_folder(&path) {
                    Box::pin(self.scan_directory(base, &path, files, too_large)).await?;
                }
            } else {
                let relative_path = path
                    .strip_prefix(base)
                    .map(|p| format!("/{}", p.to_string_lossy()))
                    .unwrap_or_else(|_| path.to_string_lossy().to_string());

                // Check file size limit
                if self
                    .config
                    .max_file_size
                    .is_some_and(|max| metadata.len() > max)
                {
                    too_large.push(relative_path);
                    continue;
                }

                let mtime_ns = mtime_ns(&metadata);
                files.insert(
                    relative_path,
                    LocalFile {
                        path,
                        mtime: mtime_ns.map_or(0, |ns| ns.div_euclid(1_000_000_000)),
                        mtime_ns,
                        size: metadata.len(),
                    },
                );
            }
        }

        Ok(())
    }

    /// Check if a path matches exclude patterns
    fn should_exclude(&self, name: &str) -> bool {
        for pattern in &self.config.exclude_patterns {
            if Self::glob_match(pattern, name) {
                return true;
            }
        }
        false
    }

    /// Simple glob matching (supports * and ?)
    fn glob_match(pattern: &str, name: &str) -> bool {
        let mut pattern_chars = pattern.chars().peekable();
        let mut name_chars = name.chars().peekable();

        while let Some(p) = pattern_chars.next() {
            match p {
                '*' => {
                    // Match any sequence
                    if pattern_chars.peek().is_none() {
                        return true; // Trailing * matches everything
                    }
                    // Try matching remaining pattern at each position
                    let remaining: String = pattern_chars.collect();
                    let remaining_name: String = name_chars.collect();
                    for i in 0..=remaining_name.len() {
                        if Self::glob_match(&remaining, &remaining_name[i..]) {
                            return true;
                        }
                    }
                    return false;
                }
                '?' => {
                    // Match any single character
                    if name_chars.next().is_none() {
                        return false;
                    }
                }
                c => {
                    // Match exact character
                    if name_chars.next() != Some(c) {
                        return false;
                    }
                }
            }
        }

        name_chars.next().is_none()
    }

    /// Check if a folder should be synced based on selective sync config
    fn should_sync_folder(&self, path: &Path) -> bool {
        if self.config.folder_configs.is_empty() {
            return true; // Sync everything if no selective config
        }

        for config in &self.config.folder_configs {
            if path.starts_with(&config.local_path) || config.local_path.starts_with(path) {
                return true;
            }
        }

        false
    }

    /// Upload a file to cloud. Returns its manifest entry: the remote file as the upload
    /// answered, and the local one with the scan's modification time (from before the read, so
    /// an edit made since is a change next time) and the size and hash of what was sent.
    async fn upload_file(&mut self, path: &str, local: &LocalFile) -> Result<ManifestEntry> {
        // Keep the relative path (not just the basename) so nested files with the same
        // name don't collide remotely; validated like downloads.
        let components = cloud_path_components(path)?;
        let content = fs::read(&local.path).await?;

        // Determine parent folder
        let parent_id = self.config.cloud_folder.as_deref();

        let cloud_file = self
            .provider
            .upload_file_at(parent_id, &components, &content, None)
            .await?;
        let entry = ManifestEntry {
            remote: Some(RemoteIdentity::of(&cloud_file)),
            local: Some(LocalIdentity {
                size: content.len() as u64,
                mtime_ns: local.mtime_ns,
                sha256: Some(sha256_hex(&content)),
            }),
        };

        // Update state
        self.state.file_map.insert(path.to_string(), cloud_file.id);
        if let Some(hash) = cloud_file.content_hash {
            self.state.hash_map.insert(path.to_string(), hash);
        }
        self.state.mtime_map.insert(path.to_string(), local.mtime);

        Ok(entry)
    }

    /// Download a file from cloud. Returns the identity of the local file written, for the
    /// manifest.
    async fn download_file(&mut self, cloud_file: &CloudFile) -> Result<LocalIdentity> {
        // Validate before touching the network or the filesystem.
        let local_path =
            local_path_for(&self.config.local_path, &cloud_file.path).inspect_err(|e| {
                tracing::error!(
                    "cloud sync: skipping download of {:?}: {}",
                    cloud_file.path,
                    e
                );
            })?;
        // A local directory where the file goes (the remote folder was replaced by a file, and
        // the directory still holds files that weren't moved aside as deleted remotely) would
        // fail the write every time. Catch it before fetching the body rather than after.
        if fs::symlink_metadata(&local_path)
            .await
            .is_ok_and(|m| m.is_dir())
        {
            return Err(IntegrationError::LocalPathUnusable(format!(
                "{:?}: a local directory is in the way",
                cloud_file.path
            )));
        }
        let content = self.provider.download_file(&cloud_file.id).await?;

        // Symlinks already inside the sync root must not redirect the write (or any mkdir) elsewhere.
        let escape = || {
            tracing::error!(
                "cloud sync: {:?} resolves outside sync root, skipping",
                cloud_file.path
            );
            IntegrationError::InvalidPath(format!(
                "{:?} resolves outside sync root",
                cloud_file.path
            ))
        };
        let (rel_dir, name) = match (
            local_path
                .parent()
                .and_then(|p| p.strip_prefix(&self.config.local_path).ok()),
            local_path.file_name(),
        ) {
            (Some(d), Some(n)) => (d, n),
            _ => return Err(escape()),
        };
        let unusable = |e| local_write_error(&cloud_file.path, e);
        let dir = create_dirs_within(&self.config.local_path, rel_dir)
            .await
            .map_err(unusable)?
            .ok_or_else(escape)?;
        let target = dir.join(name);
        if fs::symlink_metadata(&target)
            .await
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(escape());
        }
        let written = write_replace(&dir, &target, &content)
            .await
            .map_err(unusable)?;

        // Update state
        self.state
            .file_map
            .insert(cloud_file.path.clone(), cloud_file.id.clone());
        if let Some(ref hash) = cloud_file.content_hash {
            self.state
                .hash_map
                .insert(cloud_file.path.clone(), hash.clone());
        }
        self.state
            .mtime_map
            .insert(cloud_file.path.clone(), cloud_file.modified_at);

        Ok(LocalIdentity {
            size: written.len(),
            mtime_ns: mtime_ns(&written),
            sha256: Some(sha256_hex(&content)),
        })
    }

    /// Perform delta sync using provider's change API: download what changed remotely. Remote
    /// deletions are reported by the provider but not applied, and local changes wait for a
    /// full [`sync`](Self::sync). Without a usable cursor (none yet, or rejected by the
    /// provider) it runs a [`resync`](Self::resync) instead.
    ///
    /// Downloads are recorded in the full sync's [manifest](SyncManifest), if there is one (and
    /// saved with [`SyncConfig::persist_state`]), so the next full sync doesn't take them for
    /// changes on both sides. A remote deletion leaves its entry alone: that full sync finds
    /// the file gone remotely and moves the local copy aside if it is unchanged.
    ///
    /// With [`SyncDirection::Upload`] there is nothing to do: the change feed only brings
    /// remote changes down. The provider isn't asked, and the cursor is neither taken nor
    /// advanced. It marks how far remote changes have been applied, and none were, so a
    /// direction widened later still gets them (or a resync, if the cursor has expired by then).
    pub async fn delta_sync(&mut self) -> Result<SyncResult> {
        if self.config.direction == SyncDirection::Upload {
            tracing::debug!("cloud sync: upload-only, so a delta sync has nothing to download");
            return Ok(SyncResult::new());
        }
        let Some(cursor) = self.state.cursor.clone() else {
            // A change feed only lists what changes after its cursor, so the files already
            // there have to come from a full listing first.
            tracing::info!("cloud sync: no delta cursor yet; running a full sync");
            return self.resync().await;
        };
        self.load_manifest().await?;

        let start = std::time::Instant::now();
        let mut result = SyncResult::new();

        // Get changes since last cursor
        let (changes, new_cursor) = match self
            .provider
            .get_changes_in(self.config.cloud_folder.as_deref(), Some(&cursor))
            .await
        {
            Ok(page) => page,
            Err(IntegrationError::ResyncRequired(why)) => {
                tracing::warn!(
                    "cloud sync: delta cursor rejected ({}); running a full sync",
                    why
                );
                return self.resync().await;
            }
            Err(e) => return Err(e),
        };

        // Set when a change failed for a reason that may go away (network, I/O, rate limit);
        // the cursor is then held so the provider re-sends the page. Re-applying the changes
        // that did succeed is idempotent (atomic overwrite with the same content).
        let mut retry_needed = false;
        let mut record = |result: &mut SyncResult, what: &str, e: IntegrationError| {
            if !e.is_permanent() {
                retry_needed = true;
            }
            result.errors.push(format!("{}: {}", what, e));
        };

        for cloud_file in changes {
            if cloud_file.deleted {
                // Not propagated: a local copy may hold edits the remote never saw, and the
                // local mtime can't tell us (downloads are stamped with the write time).
                tracing::info!(
                    "cloud sync: {:?} was deleted remotely; keeping the local copy",
                    cloud_file.path
                );
                continue;
            }
            if cloud_file.is_folder {
                continue;
            }
            if is_reserved(&cloud_file.path) {
                tracing::warn!(
                    "cloud sync: skipping change {:?}: a name the sync keeps for itself",
                    cloud_file.path
                );
                continue;
            }
            if self.too_large(&cloud_file) {
                // Permanent until the limit changes, so it doesn't hold the cursor.
                tracing::warn!(
                    "cloud sync: skipping change {:?}: {} bytes is over the size limit",
                    cloud_file.path,
                    cloud_file.size
                );
                continue;
            }

            let local_path = match local_path_for(&self.config.local_path, &cloud_file.path) {
                Ok(p) => p,
                Err(e) => {
                    // Validation reject: permanent, so it must not block the cursor forever.
                    tracing::error!("cloud sync: skipping change {:?}: {}", cloud_file.path, e);
                    record(&mut result, &format!("Skipped {}", cloud_file.path), e);
                    continue;
                }
            };

            // Check if local file exists
            let local_exists = local_path.exists();

            let outcome = if local_exists {
                // Check for conflict
                let metadata = match fs::metadata(&local_path).await {
                    Ok(m) => m,
                    Err(e) => {
                        record(&mut result, &format!("Stat {}", cloud_file.path), e.into());
                        continue;
                    }
                };
                let local_mtime = mtime_ns(&metadata).map_or(0, |ns| ns.div_euclid(1_000_000_000));

                let last_sync_time = self.state.mtime_map.get(&cloud_file.path).copied();

                if local_mtime > last_sync_time.unwrap_or(0) {
                    // Local modified since last sync - conflict
                    let mut conflict = Conflict {
                        local_path: local_path.clone(),
                        cloud_file: cloud_file.clone(),
                        local_modified_at: local_mtime,
                        local_size: metadata.len(),
                        conflict_type: ConflictType::BothModified,
                        resolution: None,
                    };

                    // Conflicts are reported, not retried: re-fetching the change can't
                    // resolve them, so they don't hold the cursor.
                    match self.conflict_resolver.resolve(&mut conflict) {
                        ConflictResolution::UseCloud => Some(self.download_file(&cloud_file).await),
                        ConflictResolution::ManualRequired => {
                            result.conflicts.push(conflict);
                            None
                        }
                        _ => None,
                    }
                } else {
                    // Cloud is newer, download
                    Some(self.download_file(&cloud_file).await)
                }
            } else {
                // New file from cloud
                Some(self.download_file(&cloud_file).await)
            };

            match outcome {
                Some(Ok(local)) => {
                    result.downloaded += 1;
                    self.record_download(&cloud_file, local);
                }
                Some(Err(e)) => record(&mut result, "Download failed", e),
                None => {}
            }
        }

        // Only advance past this page once every change in it was applied or failed
        // permanently; otherwise the transiently failed files would never be retried.
        if retry_needed {
            tracing::warn!("cloud sync: holding delta cursor; some changes will be retried");
        } else {
            self.state.cursor = new_cursor;
        }
        self.state.last_sync = Some(chrono::Utc::now().timestamp());
        if let Err(e) = self.save_manifest().await {
            tracing::error!("cloud sync: failed to save the sync state: {}", e);
            result
                .errors
                .push(format!("Failed to save the sync state: {}", e));
        }

        if !result.errors.is_empty() {
            result.status = SyncStatus::PartialSuccess;
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Catch up when there is no usable delta cursor (none yet, or the provider rejected it):
    /// take a fresh cursor, then do a full sync. The cursor is taken first so changes made
    /// during the full sync are replayed by the next delta rather than missed. It is only
    /// stored once nothing remote is left to retry: if the listing failed or a download failed
    /// for a reason that may go away, the old cursor is kept, so the next delta runs the resync
    /// again instead of silently skipping those files (a fresh cursor never lists changes made
    /// before it). Permanent failures (deleted, not downloadable, unsafe or over-long name, a
    /// local directory in the way) don't hold it, and neither do failed uploads, which no
    /// cursor covers.
    ///
    /// Like the delta it stands in for, a resync never uploads files that exist only locally
    /// (see [`LocalOnly::Keep`]).
    async fn resync(&mut self) -> Result<SyncResult> {
        let (_, fresh) = self
            .provider
            .get_changes_in(self.config.cloud_folder.as_deref(), None)
            .await?;
        let Reconciled {
            result,
            retry_needed,
        } = self.reconcile(LocalOnly::Keep).await?;
        if retry_needed {
            tracing::warn!(
                "cloud sync: keeping the old delta cursor; the full sync will be retried"
            );
        } else {
            self.state.cursor = fresh;
        }
        Ok(result)
    }
}

/// What a full sync does with files that exist only locally, in an uploading direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalOnly {
    /// Upload them: a requested full sync.
    Upload,
    /// Leave them. A resync stands in for a delta, which reports remote deletions but keeps
    /// the local copies; a file missing from the listing may be one of those, and uploading it
    /// would bring back what was deleted remotely. A later full sync uploads new local files.
    Keep,
}

/// What a full sync does with a path (see [`CloudSync::reconcile`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Deleted remotely and unchanged here: move the local copy into [`QUARANTINE_DIR`].
    Quarantine,
    Upload,
    /// Deleted remotely but changed here: upload it again.
    Reupload,
    Download,
    /// Present on both sides, and not unchanged on both. `known`: the manifest says both
    /// changed since the last sync.
    Both {
        known: bool,
    },
}

impl Step {
    /// The order steps run in. Local copies are moved aside first, so a remote folder that
    /// replaced a file, or a file that replaced a folder, can be downloaded in the same sync.
    fn order(self) -> u8 {
        match self {
            Step::Quarantine => 0,
            Step::Upload | Step::Reupload => 1,
            Step::Download => 2,
            Step::Both { .. } => 3,
        }
    }
}

/// A path's [`Step`] in a full sync.
struct Planned {
    path: String,
    step: Step,
    /// Whether the path goes in the manifest ([`CloudSync::visible`], and within the size
    /// limit on both sides).
    track: bool,
    /// Its manifest entry from the last sync, kept if the step fails or leaves the two sides
    /// apart.
    old: Option<ManifestEntry>,
}

/// Outcome of a full sync ([`CloudSync::reconcile`]).
struct Reconciled {
    result: SyncResult,
    /// Something remote wasn't fetched for a reason that may go away (a listing failed, or a
    /// download failed with a non-permanent error).
    retry_needed: bool,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::integrations::{CloudFolder, OAuthToken, ProviderType, StorageQuota};

    /// In-memory provider: serves `files` (content = id bytes) and records upload names and
    /// downloaded ids. Downloads of ids in `fail_ids` fail with a (transient) network error,
    /// of ids in `gone_ids` with a (permanent) not-found; `get_changes` hands out
    /// `next_cursor`, except for `stale_cursor`, which it rejects as expired. While
    /// `list_fails` is set, the full listing fails. `legacy_dir` answers `legacy_layout_dir`
    /// (an error while `legacy_fails` is set), and a file's content matches when it equals its
    /// `content_hash`.
    #[derive(Default)]
    struct MockProvider {
        legacy_dir: Option<Vec<String>>,
        legacy_fails: bool,
        files: Vec<CloudFile>,
        uploads: Mutex<Vec<String>>,
        downloads: Mutex<Vec<String>>,
        fail_ids: Mutex<HashSet<String>>,
        gone_ids: HashSet<String>,
        next_cursor: Option<String>,
        stale_cursor: Option<String>,
        list_fails: std::sync::atomic::AtomicBool,
        /// How often `list_files` and `get_changes` were called.
        list_calls: std::sync::atomic::AtomicUsize,
        changes_calls: std::sync::atomic::AtomicUsize,
    }

    fn cf(id: &str, path: &str) -> CloudFile {
        CloudFile {
            id: id.into(),
            name: path.rsplit('/').next().unwrap().into(),
            mime_type: None,
            size: 1,
            modified_at: 1,
            content_hash: None,
            parent_id: None,
            is_folder: false,
            path: path.into(),
            deleted: false,
        }
    }

    #[async_trait]
    impl CloudProvider for MockProvider {
        fn provider_type(&self) -> ProviderType {
            ProviderType::Dropbox
        }
        fn is_authenticated(&self) -> bool {
            true
        }
        fn get_token(&self) -> Option<&OAuthToken> {
            None
        }
        fn set_token(&mut self, _: OAuthToken) {}
        async fn refresh_token(&mut self) -> Result<()> {
            Ok(())
        }
        async fn list_files(&self, _: Option<&str>) -> Result<Vec<CloudFile>> {
            self.list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.list_fails.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(IntegrationError::Network("listing failed".into()));
            }
            Ok(self.files.clone())
        }
        async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
            Ok(vec![])
        }
        async fn get_file_metadata(&self, id: &str) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        async fn download_file(&self, id: &str) -> Result<Vec<u8>> {
            self.downloads.lock().unwrap().push(id.into());
            if self.fail_ids.lock().unwrap().contains(id) {
                return Err(IntegrationError::Network("connection reset".into()));
            }
            if self.gone_ids.contains(id) {
                return Err(IntegrationError::NotFound(id.into()));
            }
            Ok(id.as_bytes().to_vec())
        }
        async fn upload_file(
            &self,
            _: Option<&str>,
            name: &str,
            _: &[u8],
            _: Option<&str>,
        ) -> Result<CloudFile> {
            self.uploads.lock().unwrap().push(name.into());
            // Over a listed file, the upload replaces it (the same id), as the providers do.
            let path = format!("/{}", name);
            let id = self
                .files
                .iter()
                .find(|f| f.path == path)
                .map_or(name, |f| f.id.as_str());
            Ok(cf(id, &path))
        }
        async fn create_folder(&self, _: Option<&str>, _: &str) -> Result<CloudFolder> {
            unimplemented!()
        }
        async fn delete(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn move_file(&self, id: &str, _: &str, _: Option<&str>) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        async fn get_changes(
            &self,
            cursor: Option<&str>,
        ) -> Result<(Vec<CloudFile>, Option<String>)> {
            self.changes_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if cursor.is_some() && cursor == self.stale_cursor.as_deref() {
                return Err(IntegrationError::ResyncRequired("expired".into()));
            }
            Ok((self.files.clone(), self.next_cursor.clone()))
        }
        async fn get_quota(&self) -> Result<StorageQuota> {
            Ok(StorageQuota {
                used: 0,
                total: None,
                trash: None,
            })
        }
        async fn legacy_layout_dir(&self, _: Option<&str>) -> Result<Option<Vec<String>>> {
            if self.legacy_fails {
                return Err(IntegrationError::Network("metadata failed".into()));
            }
            Ok(self.legacy_dir.clone())
        }
        fn content_matches(&self, file: &CloudFile, content: &[u8]) -> bool {
            file.content_hash.as_deref().map(str::as_bytes) == Some(content)
        }
    }

    fn cfg(dir: &Path, direction: SyncDirection) -> SyncConfig {
        SyncConfig {
            local_path: dir.to_path_buf(),
            direction,
            ..Default::default()
        }
    }

    const EVIL: &[&str] = &[
        "../x",
        "/../x",
        "//etc/x",
        "a/../../x",
        "..\\x",
        "a\\..\\..\\x",
        "C:/x",
        "c:x",
        "a/./b",
        "a//b",
        "",
        "/",
        "x\0y",
        "..",
    ];

    #[test]
    fn traversal_names_rejected() {
        for p in [
            "../x",
            "/etc/x",
            "a/../../x",
            "..\\x",
            "C:\\x",
            "a\0b",
            "./x",
            "a//b",
            "",
            "..",
        ] {
            assert!(safe_components(p).is_err(), "accepted {:?}", p);
        }
        let base = Path::new("/srv/sync");
        for p in EVIL {
            assert!(local_path_for(base, p).is_err(), "accepted {:?}", p);
        }
        // A provider-rooted path maps under the base, never to the real /etc.
        assert_eq!(local_path_for(base, "/etc/x").unwrap(), base.join("etc/x"));
        assert_eq!(
            local_path_for(base, "/Notes/a b.pdf").unwrap(),
            base.join("Notes").join("a b.pdf")
        );
        assert_eq!(
            safe_components("a/b/c.pdf").unwrap(),
            vec!["a", "b", "c.pdf"]
        );
    }

    #[tokio::test]
    async fn download_skips_traversal_and_keeps_syncing() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let mut files: Vec<CloudFile> = EVIL
            .iter()
            .enumerate()
            .map(|(i, p)| cf(&format!("evil{}", i), p))
            .collect();
        files.push(cf("good", "/sub/ok.txt"));
        let mut sync = CloudSync::new(
            MockProvider {
                files: files.clone(),
                ..Default::default()
            },
            cfg(&root, SyncDirection::Download),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!(r.downloaded, 1);
        assert_eq!(r.status, SyncStatus::PartialSuccess);
        assert_eq!(r.errors.len(), EVIL.len());
        assert_eq!(std::fs::read(root.join("sub/ok.txt")).unwrap(), b"good");
        assert!(!outer.path().join("x").exists());
        // Delta sync applies the same validation (fresh root so nothing conflicts).
        let root2 = outer.path().join("root2");
        std::fs::create_dir(&root2).unwrap();
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::with_state(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root2, SyncDirection::Download),
            state,
        );
        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.downloaded, 1);
        assert_eq!(r.errors.len(), EVIL.len());
        assert!(root2.join("sub/ok.txt").exists());
        assert!(!outer.path().join("x").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn download_refuses_symlinked_dir_escape() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::os::unix::fs::symlink(outer.path(), root.join("link")).unwrap();
        let files = vec![cf("evil", "/link/pwned.txt")];
        let mut sync = CloudSync::new(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root, SyncDirection::Download),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!(r.downloaded, 0);
        assert!(!outer.path().join("pwned.txt").exists());
    }

    /// No directory may be created through a symlinked subdirectory, a symlinked target file
    /// is never written through, and symlinks that stay inside the root still work.
    #[cfg(unix)]
    #[tokio::test]
    async fn download_never_creates_or_writes_through_escaping_symlinks() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir_all(root.join("real")).unwrap();
        std::os::unix::fs::symlink(outer.path(), root.join("link")).unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join("alias")).unwrap();
        // Hidden, so the local scan skips it and the cloud copy is a pure download.
        std::fs::write(outer.path().join("victim.txt"), "orig").unwrap();
        std::os::unix::fs::symlink(outer.path().join("victim.txt"), root.join(".f.txt")).unwrap();
        let files = vec![
            cf("evil1", "/link/new/deep/pwned.txt"),
            cf("evil2", "/.f.txt"),
            cf("ok", "/alias/sub/ok.txt"),
        ];
        let mut sync = CloudSync::new(
            MockProvider {
                files,
                ..Default::default()
            },
            cfg(&root, SyncDirection::Download),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 2), "{:?}", r.errors);
        assert!(
            !outer.path().join("new").exists(),
            "mkdir escaped through symlink"
        );
        assert_eq!(
            std::fs::read(outer.path().join("victim.txt")).unwrap(),
            b"orig"
        );
        assert_eq!(std::fs::read(root.join("real/sub/ok.txt")).unwrap(), b"ok");
        // Temp files from the atomic write don't linger.
        assert!(
            std::fs::read_dir(root.join("real/sub"))
                .unwrap()
                .all(|e| e.unwrap().file_name() == "ok.txt")
        );
    }

    /// A transient download failure holds the delta cursor so the change is re-fetched; once
    /// it succeeds the cursor advances. Permanent (validation) rejects never hold it.
    #[tokio::test]
    async fn delta_cursor_held_until_transient_failures_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("ok", "/ok.txt"), cf("flaky", "/sub/flaky.txt")],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        provider.fail_ids.lock().unwrap().insert("flaky".into());
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync =
            CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Download), state);

        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 1), "{:?}", r.errors);
        assert_eq!(r.status, SyncStatus::PartialSuccess);
        assert_eq!(
            sync.state().cursor.as_deref(),
            Some("c1"),
            "cursor advanced past a failure"
        );
        assert!(!dir.path().join("sub/flaky.txt").exists());

        // The failure clears (e.g. network back): the retried page applies and the cursor moves.
        sync.provider.fail_ids.lock().unwrap().clear();
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.status, SyncStatus::Success);
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        assert_eq!(
            std::fs::read(dir.path().join("sub/flaky.txt")).unwrap(),
            b"flaky"
        );
    }

    #[tokio::test]
    async fn delta_cursor_advances_past_permanent_rejects() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let mut files: Vec<CloudFile> = EVIL
            .iter()
            .enumerate()
            .map(|(i, p)| cf(&format!("evil{}", i), p))
            .collect();
        files.push(cf("good", "/good.txt"));
        let provider = MockProvider {
            files,
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::with_state(provider, cfg(&root, SyncDirection::Download), state);
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, EVIL.len()));
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        assert!(!outer.path().join("x").exists());
    }

    /// Remote deletions are never downloaded and never remove the local copy, and they don't
    /// hold the cursor.
    #[tokio::test]
    async fn delta_skips_remote_deletions() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.txt"), "mine").unwrap();
        let mut gone = cf("", "/gone.txt");
        gone.deleted = true;
        let provider = MockProvider {
            files: vec![gone, cf("new", "/new.txt")],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync =
            CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Download), state);
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.downloaded, r.deleted), (1, 0));
        assert_eq!(std::fs::read(dir.path().join("gone.txt")).unwrap(), b"mine");
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
    }

    /// A rejected cursor triggers a full sync; the fresh cursor is only stored once that
    /// sync works, so a failed attempt is retried by the next delta instead of skipping the
    /// changes made while the cursor was stale.
    #[tokio::test]
    async fn delta_resync_holds_cursor_until_full_sync_works() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("a", "/a.txt"), cf("b", "/sub/b.txt")],
            next_cursor: Some("fresh".into()),
            stale_cursor: Some("stale".into()),
            list_fails: true.into(),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("stale".into()),
            ..Default::default()
        };
        let mut sync =
            CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Download), state);

        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.status, SyncStatus::Failed, "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("stale"));

        sync.provider
            .list_fails
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.downloaded, 2);
        assert_eq!(std::fs::read(dir.path().join("sub/b.txt")).unwrap(), b"b");
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));
    }

    fn stale(
        provider: MockProvider,
        dir: &Path,
        direction: SyncDirection,
    ) -> CloudSync<MockProvider> {
        let state = SyncState {
            cursor: Some("stale".into()),
            ..Default::default()
        };
        let provider = MockProvider {
            next_cursor: Some("fresh".into()),
            stale_cursor: Some("stale".into()),
            ..provider
        };
        CloudSync::with_state(provider, cfg(dir, direction), state)
    }

    /// A resync keeps the old cursor while a download failed transiently, whatever the
    /// status (here PartialSuccess): the fresh cursor would never list that file again.
    #[tokio::test]
    async fn resync_holds_cursor_on_transient_download_failure() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("ok", "/ok.txt"), cf("flaky", "/sub/flaky.txt")],
            ..Default::default()
        };
        provider.fail_ids.lock().unwrap().insert("flaky".into());
        let mut sync = stale(provider, dir.path(), SyncDirection::Download);

        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.status, r.downloaded), (SyncStatus::PartialSuccess, 1));
        assert_eq!(sync.state().cursor.as_deref(), Some("stale"));

        sync.provider.fail_ids.lock().unwrap().clear();
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));
        assert_eq!(
            std::fs::read(dir.path().join("sub/flaky.txt")).unwrap(),
            b"flaky"
        );
    }

    /// Only permanent failures and nothing else to transfer is status Failed, yet the resync
    /// did all it ever can: the fresh cursor is stored instead of resyncing on every delta.
    #[tokio::test]
    async fn resync_advances_cursor_past_permanent_failures() {
        let dir = tempfile::tempdir().unwrap();
        let provider = MockProvider {
            files: vec![cf("gone", "/gone.txt")],
            gone_ids: HashSet::from(["gone".to_string()]),
            ..Default::default()
        };
        let mut sync = stale(provider, dir.path(), SyncDirection::Download);
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.status, r.errors.len()), (SyncStatus::Failed, 1));
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));
    }

    /// A resync (bidirectional here) never uploads files missing from the listing: they may be
    /// remote deletions that delta sync reported and deliberately kept locally. A requested
    /// full sync still uploads them.
    #[tokio::test]
    async fn resync_does_not_upload_local_only_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("deleted-remotely.txt"), "mine").unwrap();
        let provider = MockProvider {
            files: vec![cf("a", "/a.txt")],
            ..Default::default()
        };
        let mut sync = stale(provider, dir.path(), SyncDirection::Bidirectional);
        sync.state
            .file_map
            .insert("/deleted-remotely.txt".into(), "old-id".into());

        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.downloaded, r.uploaded), (1, 0));
        assert!(sync.provider.uploads.lock().unwrap().is_empty());
        assert_eq!(
            std::fs::read(dir.path().join("deleted-remotely.txt")).unwrap(),
            b"mine"
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));

        sync.sync().await.unwrap();
        assert!(
            sync.provider
                .uploads
                .lock()
                .unwrap()
                .contains(&"deleted-remotely.txt".to_string())
        );
    }

    /// With no cursor yet, delta sync takes one and then runs a full sync, so files that
    /// existed before the cursor aren't silently skipped.
    #[tokio::test]
    async fn delta_without_cursor_runs_a_full_sync_first() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("local-only.txt"), "mine").unwrap();
        let provider = MockProvider {
            files: vec![cf("a", "/a.txt"), cf("b", "/sub/b.txt")],
            next_cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::new(provider, cfg(dir.path(), SyncDirection::Bidirectional));
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.downloaded, r.uploaded), (2, 0));
        assert_eq!(std::fs::read(dir.path().join("sub/b.txt")).unwrap(), b"b");
        assert_eq!(sync.state().cursor.as_deref(), Some("c1"));
    }

    /// Upload-only: a delta sync downloads nothing and doesn't ask the provider for changes or
    /// a listing, with a cursor, an expired one or none. The cursor stays as it was (a resync
    /// would store one past remote changes that were never applied), so widening the direction
    /// later still brings those changes down. Local changes wait for a full sync, which does
    /// upload them.
    #[tokio::test]
    async fn upload_only_delta_sync_downloads_nothing_and_keeps_the_cursor() {
        use std::sync::atomic::Ordering::SeqCst;
        for cursor in [Some("c1"), Some("stale"), None] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("local.txt"), "mine").unwrap();
            let provider = MockProvider {
                files: vec![cf("r", "/remote.txt")],
                next_cursor: Some("c2".into()),
                stale_cursor: Some("stale".into()),
                ..Default::default()
            };
            let state = SyncState {
                cursor: cursor.map(Into::into),
                ..Default::default()
            };
            let mut sync =
                CloudSync::with_state(provider, cfg(dir.path(), SyncDirection::Upload), state);

            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{cursor:?}: {:?}", r.errors);
            assert_eq!(
                (r.status, r.downloaded, r.uploaded),
                (SyncStatus::Success, 0, 0),
                "{cursor:?}"
            );
            assert!(sync.provider.downloads.lock().unwrap().is_empty());
            assert!(!dir.path().join("remote.txt").exists());
            assert_eq!(sync.state().cursor.as_deref(), cursor);
            assert_eq!(sync.provider.changes_calls.load(SeqCst), 0, "{cursor:?}");
            assert_eq!(sync.provider.list_calls.load(SeqCst), 0, "{cursor:?}");

            sync.sync().await.unwrap();
            assert_eq!(*sync.provider.uploads.lock().unwrap(), vec!["local.txt"]);
            assert!(!dir.path().join("remote.txt").exists());

            sync.config.direction = SyncDirection::Download;
            let r = sync.delta_sync().await.unwrap();
            assert!(r.errors.is_empty(), "{cursor:?}: {:?}", r.errors);
            assert_eq!(std::fs::read(dir.path().join("remote.txt")).unwrap(), b"r");
            assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        }
    }

    /// Remote files over `max_file_size` are never downloaded, by full or delta sync, and a
    /// local file at such a path isn't uploaded over the remote one; neither is an error.
    #[tokio::test]
    async fn oversized_cloud_files_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("big.bin"), "small local").unwrap();
        let mut big = cf("big", "/big.bin");
        big.size = 1 << 20;
        let mut huge = cf("huge", "/sub/huge.bin");
        huge.size = u64::MAX;
        let provider = MockProvider {
            files: vec![big, huge, cf("ok", "/ok.txt")],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let config = SyncConfig {
            max_file_size: Some(1024),
            ..cfg(dir.path(), SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(provider, config);

        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(
            (r.status, r.downloaded, r.uploaded),
            (SyncStatus::Success, 1, 0)
        );
        assert_eq!(*sync.provider.downloads.lock().unwrap(), vec!["ok"]);
        assert_eq!(
            std::fs::read(dir.path().join("big.bin")).unwrap(),
            b"small local"
        );
        assert!(!dir.path().join("sub/huge.bin").exists());

        sync.state.cursor = Some("c1".into());
        sync.provider.downloads.lock().unwrap().clear();
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(
            !sync
                .provider
                .downloads
                .lock()
                .unwrap()
                .iter()
                .any(|id| id != "ok"),
            "{:?}",
            sync.provider.downloads
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
    }

    /// A remote folder replaced by a file of the same name leaves the local directory in place
    /// (remote deletions aren't applied). Writing the file there fails every time, so it is a
    /// permanent failure: caught before any download, and neither a resync nor a delta holds
    /// its cursor for it.
    #[tokio::test]
    async fn remote_file_over_local_directory_does_not_hold_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("Dir")).unwrap();
        std::fs::write(dir.path().join("Dir/kept.txt"), "mine").unwrap();
        let provider = MockProvider {
            files: vec![cf("was-a-folder", "/Dir"), cf("ok", "/ok.txt")],
            next_cursor: Some("fresh".into()),
            stale_cursor: Some("stale".into()),
            ..Default::default()
        };
        let state = SyncState {
            cursor: Some("stale".into()),
            ..Default::default()
        };
        // CloudWins so the delta below tries to replace the (newer) local directory.
        let config = SyncConfig {
            conflict_strategy: ConflictStrategy::CloudWins,
            ..cfg(dir.path(), SyncDirection::Download)
        };
        let mut sync = CloudSync::with_state(provider, config, state);

        // Resync (stale cursor): the file is skipped as unusable, the rest syncs, and the fresh
        // cursor is stored instead of repeating the full listing on every delta.
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 1), "{:?}", r.errors);
        assert!(
            r.errors[0].contains("Local path unusable"),
            "{:?}",
            r.errors
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("fresh"));

        // Delta: the same change fails permanently again, and the cursor still advances.
        sync.provider.next_cursor = Some("c3".into());
        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert!(
            r.errors[0].contains("Local path unusable"),
            "{:?}",
            r.errors
        );
        assert_eq!(sync.state().cursor.as_deref(), Some("c3"));

        // The body was never fetched, and the local directory is untouched.
        assert!(
            !sync
                .provider
                .downloads
                .lock()
                .unwrap()
                .contains(&"was-a-folder".to_string())
        );
        assert_eq!(
            std::fs::read(dir.path().join("Dir/kept.txt")).unwrap(),
            b"mine"
        );
    }

    /// Dropbox and OneDrive allow 255 characters per name, which can be more than the 255 bytes
    /// a local name may have. Such a file (or folder) is rejected up front like an unsafe name:
    /// never downloaded, and the cursor from a first sync is stored and then advanced.
    #[tokio::test]
    async fn names_too_long_for_the_local_filesystem_do_not_hold_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let long = format!("{}.pdf", "\u{6587}".repeat(90)); // 94 characters, 274 bytes
        assert!(long.chars().count() <= 255 && long.len() > MAX_NAME_BYTES);
        let provider = MockProvider {
            files: vec![
                cf("long-file", &format!("/{}", long)),
                cf("in-long-folder", &format!("/{}/x.pdf", long)),
                cf("ok", "/ok.txt"),
            ],
            next_cursor: Some("c1".into()),
            ..Default::default()
        };
        let mut sync = CloudSync::new(provider, cfg(dir.path(), SyncDirection::Download));

        // No cursor yet: the full sync runs and its fresh cursor is kept.
        let r = sync.delta_sync().await.unwrap();
        assert_eq!((r.downloaded, r.errors.len()), (1, 2), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("c1"));

        sync.provider.next_cursor = Some("c2".into());
        let r = sync.delta_sync().await.unwrap();
        assert_eq!(r.errors.len(), 2, "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("c2"));
        assert!(
            sync.provider
                .downloads
                .lock()
                .unwrap()
                .iter()
                .all(|id| id == "ok")
        );
    }

    #[test]
    fn overlong_segments_rejected() {
        let max = "a".repeat(MAX_NAME_BYTES);
        let over = "a".repeat(MAX_NAME_BYTES + 1);
        // Measured in bytes, not characters: 86 three-byte characters are 258 bytes.
        let wide = "\u{6587}".repeat(86);
        assert!(is_safe_name(&max));
        assert!(safe_components(&format!("{max}/{max}")).is_ok());
        for bad in [&over, &wide] {
            assert!(!is_safe_name(bad), "accepted {} bytes", bad.len());
            assert!(safe_components(&format!("ok/{bad}/x")).is_err());
            assert!(local_path_for(Path::new("/srv/sync"), &format!("/{bad}")).is_err());
        }
    }

    /// Local write failures that come back on every retry are classified permanent; the
    /// others (fixable on the server) stay transient. Checked against real filesystem errors.
    #[cfg(unix)]
    #[tokio::test]
    async fn local_write_errors_classified() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/x"), "x").unwrap();

        // A file renamed over a (non-empty) directory: IsADirectory.
        let e = write_replace(dir.path(), &dir.path().join("sub"), b"new")
            .await
            .unwrap_err();
        let e = local_write_error("/sub", e);
        assert!(matches!(e, IntegrationError::LocalPathUnusable(_)), "{e:?}");
        assert!(e.is_permanent());
        assert_eq!(std::fs::read(dir.path().join("sub/x")).unwrap(), b"x");

        // A name the filesystem refuses (ENAMETOOLONG): InvalidFilename.
        let long = dir.path().join("a".repeat(MAX_NAME_BYTES + 1));
        let e = write_replace(dir.path(), &long, b"new").await.unwrap_err();
        let e = local_write_error("/aaa", e);
        assert!(e.is_permanent(), "{e:?}");

        // No temp files are left behind by the failed writes.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::StorageFull,
            std::io::ErrorKind::Other,
        ] {
            let e = local_write_error("/x", IntegrationError::Io(kind.into()));
            assert!(
                matches!(e, IntegrationError::Io(_)) && !e.is_permanent(),
                "{e:?}"
            );
        }
        let e = local_write_error("/x", IntegrationError::Network("reset".into()));
        assert!(matches!(e, IntegrationError::Network(_)));
    }

    /// A write that fails in tokio's background task fails `write_durably`, so `write_replace`
    /// never renames a short file into place as a finished download. Every write to a file
    /// opened read-only fails (EBADF), while on Linux its fsync still succeeds: that is the
    /// case where `sync_all` on its own dropped the error.
    #[tokio::test]
    async fn failed_writes_are_not_reported_as_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, "").unwrap();
        let mut f = fs::File::from_std(std::fs::File::open(&path).unwrap());
        write_durably(&mut f, b"abcdef").await.unwrap_err();
        assert_eq!(std::fs::read(&path).unwrap(), b"");
    }

    #[test]
    fn error_permanence() {
        assert!(IntegrationError::InvalidPath("x".into()).is_permanent());
        assert!(IntegrationError::NotFound("x".into()).is_permanent());
        assert!(IntegrationError::NotDownloadable("x".into()).is_permanent());
        assert!(IntegrationError::LocalPathUnusable("x".into()).is_permanent());
        for e in [
            IntegrationError::Network("x".into()),
            IntegrationError::Io(std::io::Error::other("x")),
            IntegrationError::RateLimited {
                retry_after_secs: 1,
            },
            IntegrationError::TokenExpired,
            IntegrationError::Api("500".into()),
            IntegrationError::ResyncRequired("410".into()),
        ] {
            assert!(!e.is_permanent(), "{e}");
        }
    }

    #[tokio::test]
    async fn upload_preserves_nested_paths() {
        let dir = tempfile::tempdir().unwrap();
        for sub in ["a", "b/c"] {
            std::fs::create_dir_all(dir.path().join(sub)).unwrap();
            std::fs::write(dir.path().join(sub).join("same.pdf"), sub).unwrap();
        }
        std::fs::write(dir.path().join("top.pdf"), "t").unwrap();
        let mut sync = CloudSync::new(
            MockProvider::default(),
            cfg(dir.path(), SyncDirection::Upload),
        );
        let r = sync.sync().await.unwrap();
        assert_eq!(r.uploaded, 3, "{:?}", r.errors);
        let mut names = sync.provider.uploads.lock().unwrap().clone();
        names.sort();
        assert_eq!(names, vec!["a/same.pdf", "b/c/same.pdf", "top.pdf"]);
    }

    fn remote_folder(path: &str) -> CloudFile {
        CloudFile {
            is_folder: true,
            ..cf(path, path)
        }
    }

    /// A remote file whose content (its id, as `MockProvider` serves it) the listing vouches for
    /// with a hash, as Dropbox and OneDrive listings do.
    fn hashed(id: &str, path: &str) -> CloudFile {
        CloudFile {
            content_hash: Some(id.into()),
            size: id.len() as u64,
            ..cf(id, path)
        }
    }

    fn write(root: &Path, rel: &str, body: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    fn read(root: &Path, rel: &str) -> String {
        std::fs::read_to_string(root.join(rel)).unwrap()
    }

    fn uploads(provider: &MockProvider) -> Vec<String> {
        let mut names = provider.uploads.lock().unwrap().clone();
        names.sort();
        names
    }

    fn marker(root: &Path) -> Vec<String> {
        let marker: LayoutMarker =
            serde_json::from_str(&read(root, LAYOUT_MARKER)).expect("layout marker");
        marker.handled
    }

    /// Upgrading from before #34, where the folder was kept under its own path from the drive
    /// root (`<local>/Notes/…`). The first full sync, in any direction, moves that directory
    /// aside (in every casing) before anything else, so none of it is uploaded into the folder
    /// one level down: not the copies of remote files, not a file deleted remotely since, and
    /// not in a later sync either. The user's other files sync as usual. From then on a
    /// directory of that name is an ordinary subfolder: nothing is held back or moved again.
    #[tokio::test]
    async fn old_layout_is_moved_aside_once_then_synced_like_any_folder() {
        for first in [SyncDirection::Bidirectional, SyncDirection::Download] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            write(root, "Notes/a.pdf", "old a");
            write(root, "Notes/gone.pdf", "deleted remotely since");
            write(root, "notes/Sub/b.pdf", "old b"); // Dropbox's casing of a parent may vary
            write(root, "mine.pdf", "mine");
            write(root, "Archive/2024/a.pdf", "not a copy of /a.pdf");
            let provider = || MockProvider {
                legacy_dir: Some(vec!["Notes".into()]),
                files: vec![
                    hashed("a", "/a.pdf"),
                    remote_folder("/Sub"),
                    hashed("b", "/Sub/b.pdf"),
                ],
                ..Default::default()
            };

            let mut sync = CloudSync::new(provider(), cfg(root, first));
            let r = sync.sync().await.unwrap();
            assert!(r.errors.is_empty(), "{first:?}: {:?}", r.errors);
            assert_eq!(r.downloaded, 2, "{first:?}");
            assert_eq!(r.notices.len(), 2, "{first:?}: {:?}", r.notices);
            assert!(r.notices[0].starts_with("Moved Notes to .rms-old-layout/Notes: "));
            assert!(r.notices[1].starts_with("Moved notes to .rms-old-layout/notes: "));
            assert_eq!(read(root, ".rms-old-layout/Notes/a.pdf"), "old a");
            assert_eq!(
                read(root, ".rms-old-layout/Notes/gone.pdf"),
                "deleted remotely since"
            );
            assert_eq!(read(root, ".rms-old-layout/notes/Sub/b.pdf"), "old b");
            assert!(!root.join("Notes").exists() && !root.join("notes").exists());
            assert_eq!(
                (read(root, "a.pdf"), read(root, "Sub/b.pdf")),
                ("a".to_string(), "b".to_string())
            );
            let expected: Vec<&str> = match first {
                SyncDirection::Download => vec![],
                _ => vec!["Archive/2024/a.pdf", "mine.pdf"],
            };
            assert_eq!(uploads(&sync.provider), expected, "{first:?}");
            assert_eq!(marker(root), vec!["/notes"]);

            // A later sync, with fresh state as `POST /sync` runs it: the files that are the same
            // on both sides stay put, and a new `Notes` directory is a subfolder like any other.
            write(root, "Notes/new.pdf", "a real subfolder");
            let mut sync = CloudSync::new(provider(), cfg(root, SyncDirection::Bidirectional));
            let r = sync.sync().await.unwrap();
            assert!(r.errors.is_empty(), "{first:?}: {:?}", r.errors);
            assert!(r.notices.is_empty(), "{first:?}: {:?}", r.notices);
            assert_eq!(r.downloaded, 0, "{first:?}");
            assert_eq!(
                uploads(&sync.provider),
                vec!["Archive/2024/a.pdf", "Notes/new.pdf", "mine.pdf"],
                "{first:?}"
            );
            assert_eq!(read(root, "Notes/new.pdf"), "a real subfolder");
            assert_eq!(marker(root), vec!["/notes"]);
        }
    }

    /// The marker records each old-layout directory by its folded path, so the same folder
    /// spelled another way isn't moved again, another folder synced into the same directory is,
    /// and one with nothing to move is recorded all the same. Names are matched however they
    /// are cased or percent-encoded, and a name already taken in the old-layout directory isn't
    /// overwritten.
    #[tokio::test]
    async fn old_layout_moves_are_recorded_per_directory() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let sync_with = |legacy: &[&str]| {
            CloudSync::new(
                MockProvider {
                    legacy_dir: Some(legacy.iter().map(|s| s.to_string()).collect()),
                    ..Default::default()
                },
                cfg(root, SyncDirection::Download),
            )
        };
        let moved = |notices: &[String]| -> Vec<String> {
            notices
                .iter()
                .filter_map(|n| n.strip_prefix("Moved "))
                .map(|n| n.split(':').next().unwrap().to_string())
                .collect()
        };
        write(root, "Notes/x.pdf", "x");
        write(root, "Books/y.pdf", "y");
        write(root, ".rms-old-layout/Books/by-hand.pdf", "kept");
        write(root, "documents/My Notes/q.pdf", "q");

        let r = sync_with(&["Notes"]).sync().await.unwrap();
        assert_eq!(moved(&r.notices), vec!["Notes to .rms-old-layout/Notes"]);

        write(root, "Notes/z.pdf", "made after the upgrade");
        let r = sync_with(&["NOTES"]).sync().await.unwrap();
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert_eq!(read(root, "Notes/z.pdf"), "made after the upgrade");

        let r = sync_with(&["Books"]).sync().await.unwrap();
        assert_eq!(
            moved(&r.notices),
            vec!["Books to .rms-old-layout/Books (2)"]
        );
        assert_eq!(read(root, ".rms-old-layout/Books/by-hand.pdf"), "kept");
        assert_eq!(read(root, ".rms-old-layout/Books (2)/y.pdf"), "y");

        let r = sync_with(&["Documents", "My%20Notes"])
            .sync()
            .await
            .unwrap();
        assert_eq!(
            moved(&r.notices),
            vec!["documents/My Notes to .rms-old-layout/documents/My Notes"]
        );
        assert_eq!(read(root, ".rms-old-layout/documents/My Notes/q.pdf"), "q");

        let r = sync_with(&["Nothing", "Here"]).sync().await.unwrap();
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert_eq!(
            marker(root),
            vec!["/notes", "/books", "/documents/my notes", "/nothing/here"]
        );

        // No old layout at all (the drive root, Google Drive): no marker either.
        let other = tempfile::tempdir().unwrap();
        write(other.path(), "Notes/a.pdf", "mine");
        let mut sync = CloudSync::new(
            MockProvider::default(),
            cfg(other.path(), SyncDirection::Upload),
        );
        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty() && r.notices.is_empty(), "{r:?}");
        assert_eq!(uploads(&sync.provider), vec!["Notes/a.pdf"]);
        assert!(!other.path().join(LAYOUT_MARKER).exists());
    }

    /// If the old layout can't be looked up or its marker can't be read, nothing is synced: the
    /// sync fails (and a resync keeps no cursor) rather than upload the old layout.
    #[tokio::test]
    async fn old_layout_failures_stop_the_sync() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "Notes/a.pdf", "old a");
        let mut sync = CloudSync::new(
            MockProvider {
                legacy_fails: true,
                files: vec![hashed("a", "/a.pdf")],
                next_cursor: Some("c1".into()),
                ..Default::default()
            },
            cfg(root, SyncDirection::Bidirectional),
        );
        for r in [sync.sync().await.unwrap(), sync.delta_sync().await.unwrap()] {
            assert_eq!(
                (r.status, r.uploaded, r.downloaded),
                (SyncStatus::Failed, 0, 0)
            );
            assert!(
                r.errors[0].starts_with("Failed to move the old local layout aside: "),
                "{:?}",
                r.errors
            );
        }
        assert_eq!(sync.state().cursor, None);
        assert!(sync.provider.downloads.lock().unwrap().is_empty());

        sync.provider.legacy_fails = false;
        sync.provider.legacy_dir = Some(vec!["Notes".into()]);
        write(root, LAYOUT_MARKER, "not json");
        let r = sync.sync().await.unwrap();
        assert_eq!(r.status, SyncStatus::Failed);
        assert!(r.errors[0].contains(LAYOUT_MARKER), "{:?}", r.errors);
        assert!(uploads(&sync.provider).is_empty());
        assert_eq!(read(root, "Notes/a.pdf"), "old a");
    }

    /// Versions before #34 uploaded the folder's files into it one level down from their second
    /// sync on (`/Notes/Notes/a.pdf`). The sync that moves the old layout aside says when the
    /// folder has a subfolder at that path, so it can be checked and deleted; it is otherwise
    /// synced as it is (downloaded here), and the old copies go nowhere.
    #[tokio::test]
    async fn remote_copy_of_the_old_layout_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "Notes/a.pdf", "old a");
        write(root, "Notes/c.pdf", "old c");
        let mut sync = CloudSync::new(
            MockProvider {
                legacy_dir: Some(vec!["Notes".into()]),
                files: vec![
                    hashed("a", "/a.pdf"),
                    hashed("c", "/c.pdf"),
                    remote_folder("/notes"),
                    hashed("twin", "/notes/a.pdf"),
                ],
                ..Default::default()
            },
            cfg(root, SyncDirection::Bidirectional),
        );
        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!((r.uploaded, r.downloaded), (0, 3));
        assert_eq!(r.notices.len(), 2, "{:?}", r.notices);
        assert!(
            r.notices[1].starts_with("This folder has a subfolder /Notes. "),
            "{:?}",
            r.notices
        );
        assert_eq!(read(root, "notes/a.pdf"), "twin");
        assert_eq!(read(root, ".rms-old-layout/Notes/c.pdf"), "old c");
    }

    /// The marker and the old-layout directory are the sync's own: never uploaded (even with
    /// hidden files synced), and a remote file there is never written over them, by a full sync
    /// or a delta.
    #[tokio::test]
    async fn reserved_names_are_never_synced() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, LAYOUT_MARKER, r#"{"handled":[]}"#);
        write(root, ".rms-old-layout/x.pdf", "old");
        write(root, "ok.txt", "ok");
        let provider = MockProvider {
            files: vec![
                cf("marker", "/.rms-sync-layout"),
                cf("old", "/.RMS-OLD-LAYOUT/x.pdf"),
                cf("remote", "/remote.txt"),
            ],
            next_cursor: Some("c2".into()),
            ..Default::default()
        };
        let config = SyncConfig {
            sync_hidden: true,
            ..cfg(root, SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(provider, config);
        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(uploads(&sync.provider), vec!["ok.txt"]);

        sync.state.cursor = Some("c1".into());
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(*sync.provider.downloads.lock().unwrap(), vec!["remote"]);
        assert_eq!(read(root, LAYOUT_MARKER), r#"{"handled":[]}"#);
        assert_eq!(read(root, ".rms-old-layout/x.pdf"), "old");
        assert!(!root.join(".RMS-OLD-LAYOUT").exists());
    }

    /// A file whose content the provider vouches is the same on both sides is left alone, even
    /// with no state from an earlier sync; one that differs, or whose hash isn't known, goes
    /// through conflict resolution as before (the local copy is newer here, so it's uploaded).
    #[tokio::test]
    async fn files_the_same_on_both_sides_are_not_transferred() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "same.txt", "same");
        write(root, "sub/same.txt", "deep");
        write(root, "edit.txt", "wxyz"); // same size as the remote, other content
        write(root, "grown.txt", "longer");
        write(root, "nohash.txt", "nohash");
        let mut sync = CloudSync::new(
            MockProvider {
                files: vec![
                    hashed("same", "/same.txt"),
                    hashed("deep", "/sub/same.txt"),
                    hashed("abcd", "/edit.txt"),
                    hashed("r", "/grown.txt"),
                    cf("nohash", "/nohash.txt"),
                ],
                ..Default::default()
            },
            cfg(root, SyncDirection::Bidirectional),
        );
        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.downloaded, 0);
        assert_eq!(
            uploads(&sync.provider),
            vec!["edit.txt", "grown.txt", "nohash.txt"]
        );
        assert!(sync.provider.downloads.lock().unwrap().is_empty());
        assert_eq!(
            sync.state()
                .file_map
                .get("/sub/same.txt")
                .map(String::as_str),
            Some("deep")
        );
    }

    /// A provider that keeps what is uploaded, as a real one does, by sync path. Every write is
    /// a new revision (a later `modified_at`, the same id). `hashed` listings carry a content
    /// hash that `content_matches` checks, as all three providers' listings do for ordinary
    /// files; without it, the listing is one with no hash to go by (files in Google's own
    /// formats have none), so only ids and times tell a change. Downloads of paths in `fail`
    /// fail with a (transient) network error, and while `list_fails` is set the listing does.
    /// `account` answers `account_id`, `legacy` answers `legacy_layout_dir`, and remote
    /// deletions made by the sync (there should be none) are recorded in `deletes`.
    ///
    /// With `creates_new`, an upload to a path already there is stored as another file (a new
    /// id) in `shadowed`, and the listing goes on showing the old one: what Google Drive did
    /// when every upload created a file, and its listing kept the oldest of same-named ones.
    ///
    /// With `ignores_case`, paths are compared ignoring letter case, as Dropbox and OneDrive
    /// compare them: an upload to a path spelled otherwise than a file there is a new revision
    /// of that file, which keeps its spelling.
    ///
    /// An upload to a path in `lose_reply` (as the sync spells it) is stored, and then fails
    /// with a (transient) network error, as when the reply times out; once.
    #[derive(Default)]
    struct Store {
        hashed: bool,
        creates_new: bool,
        ignores_case: bool,
        shadowed: Mutex<Vec<(CloudFile, Vec<u8>)>>,
        account: Option<String>,
        legacy: Option<Vec<String>>,
        files: Mutex<BTreeMap<String, (CloudFile, Vec<u8>)>>,
        revision: std::sync::atomic::AtomicI64,
        fail: Mutex<HashSet<String>>,
        list_fails: std::sync::atomic::AtomicBool,
        uploads: Mutex<Vec<String>>,
        downloads: Mutex<Vec<String>>,
        deletes: Mutex<Vec<String>>,
        lose_reply: Mutex<HashSet<String>>,
    }

    impl Store {
        fn hashed() -> Self {
            Self {
                hashed: true,
                ..Default::default()
            }
        }

        /// Write `content` at `path`, as a new revision.
        fn put(&self, path: &str, content: &str) -> CloudFile {
            let (file, content) = self.revision_of(path, content);
            self.files
                .lock()
                .unwrap()
                .insert(path.into(), (file.clone(), content));
            file
        }

        /// A new revision of `path` with `content`: the same id as the file there, if any.
        fn revision_of(&self, path: &str, content: &str) -> (CloudFile, Vec<u8>) {
            let rev = self
                .revision
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let files = self.files.lock().unwrap();
            let id = files
                .get(path)
                .map_or_else(|| format!("id-{rev}"), |(f, _)| f.id.clone());
            let file = CloudFile {
                id,
                name: path.rsplit('/').next().unwrap().into(),
                mime_type: None,
                size: content.len() as u64,
                modified_at: 1_000 + rev,
                content_hash: self.hashed.then(|| sha256_hex(content.as_bytes())),
                parent_id: None,
                is_folder: false,
                path: path.into(),
                deleted: false,
            };
            (file, content.into())
        }

        fn remove(&self, path: &str) {
            assert!(self.files.lock().unwrap().remove(path).is_some(), "{path}");
        }

        fn content(&self, path: &str) -> Option<String> {
            let files = self.files.lock().unwrap();
            files
                .get(path)
                .map(|(_, c)| String::from_utf8(c.clone()).unwrap())
        }

        fn uploads(&self) -> Vec<String> {
            self.uploads.lock().unwrap().clone()
        }

        fn downloads(&self) -> Vec<String> {
            self.downloads.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CloudProvider for Store {
        fn provider_type(&self) -> ProviderType {
            ProviderType::Dropbox
        }
        fn is_authenticated(&self) -> bool {
            true
        }
        fn get_token(&self) -> Option<&OAuthToken> {
            None
        }
        fn set_token(&mut self, _: OAuthToken) {}
        async fn refresh_token(&mut self) -> Result<()> {
            Ok(())
        }
        async fn list_files(&self, _: Option<&str>) -> Result<Vec<CloudFile>> {
            if self.list_fails.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(IntegrationError::Network("listing failed".into()));
            }
            let files = self.files.lock().unwrap();
            Ok(files.values().map(|(f, _)| f.clone()).collect())
        }
        async fn list_folders(&self) -> Result<Vec<CloudFolder>> {
            Ok(vec![])
        }
        async fn get_file_metadata(&self, id: &str) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        async fn download_file(&self, id: &str) -> Result<Vec<u8>> {
            let (path, content) = {
                let files = self.files.lock().unwrap();
                let (f, content) = files
                    .values()
                    .find(|(f, _)| f.id == id)
                    .ok_or_else(|| IntegrationError::NotFound(id.into()))?;
                (f.path.clone(), content.clone())
            };
            self.downloads.lock().unwrap().push(path.clone());
            if self.fail.lock().unwrap().contains(&path) {
                return Err(IntegrationError::Network("connection reset".into()));
            }
            Ok(content)
        }
        async fn upload_file(
            &self,
            _: Option<&str>,
            name: &str,
            content: &[u8],
            _: Option<&str>,
        ) -> Result<CloudFile> {
            let mut path = format!("/{name}");
            self.uploads.lock().unwrap().push(path.clone());
            let content = std::str::from_utf8(content).unwrap();
            if self.ignores_case {
                let files = self.files.lock().unwrap();
                if let Some(there) = files
                    .keys()
                    .find(|p| p.to_lowercase() == path.to_lowercase())
                {
                    path = there.clone();
                }
            }
            let there = self.files.lock().unwrap().contains_key(&path);
            if self.creates_new && there {
                let (mut file, content) = self.revision_of(&path, content);
                file.id = format!("{}-new", file.id);
                self.shadowed.lock().unwrap().push((file.clone(), content));
                return Ok(file);
            }
            let stored = self.put(&path, content);
            if self.lose_reply.lock().unwrap().remove(&format!("/{name}")) {
                return Err(IntegrationError::Network("timed out".into()));
            }
            Ok(stored)
        }
        async fn create_folder(&self, _: Option<&str>, _: &str) -> Result<CloudFolder> {
            unimplemented!()
        }
        async fn delete(&self, id: &str) -> Result<()> {
            self.deletes.lock().unwrap().push(id.into());
            Ok(())
        }
        async fn move_file(&self, id: &str, _: &str, _: Option<&str>) -> Result<CloudFile> {
            Err(IntegrationError::NotFound(id.into()))
        }
        /// Every file as changed since any cursor; nothing (and a cursor) without one.
        async fn get_changes(
            &self,
            cursor: Option<&str>,
        ) -> Result<(Vec<CloudFile>, Option<String>)> {
            let Some(_) = cursor else {
                return Ok((vec![], Some("c1".into())));
            };
            let files = self.files.lock().unwrap();
            let changes = files.values().map(|(f, _)| f.clone()).collect();
            Ok((changes, Some("c2".into())))
        }
        async fn get_quota(&self) -> Result<StorageQuota> {
            Ok(StorageQuota {
                used: 0,
                total: None,
                trash: None,
            })
        }
        async fn legacy_layout_dir(&self, _: Option<&str>) -> Result<Option<Vec<String>>> {
            Ok(self.legacy.clone())
        }
        async fn account_id(&self) -> Result<Option<String>> {
            Ok(self.account.clone())
        }
        fn content_matches(&self, file: &CloudFile, content: &[u8]) -> bool {
            self.hashed && file.content_hash.as_deref() == Some(sha256_hex(content).as_str())
        }
        fn ignores_case(&self) -> bool {
            self.ignores_case
        }
    }

    /// `cfg`, keeping the manifest between syncs as `POST /sync` does.
    fn kept(root: &Path, direction: SyncDirection) -> SyncConfig {
        SyncConfig {
            persist_state: true,
            ..cfg(root, direction)
        }
    }

    /// A new `CloudSync` over the same provider and config, as `POST /sync` makes one for each
    /// request: all it knows of the last sync is what that saved.
    fn again<P: CloudProvider>(sync: CloudSync<P>) -> CloudSync<P> {
        again_with(sync, |_| {})
    }

    /// [`again`], with the config changed by `change`.
    fn again_with<P: CloudProvider>(
        sync: CloudSync<P>,
        change: impl FnOnce(&mut SyncConfig),
    ) -> CloudSync<P> {
        let mut config = sync.config;
        change(&mut config);
        CloudSync::new(sync.provider, config)
    }

    /// A full sync that must report no errors.
    async fn clean<P: CloudProvider>(sync: &mut CloudSync<P>) -> SyncResult {
        let r = sync.sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        r
    }

    /// Uploaded, downloaded, moved aside as deleted remotely.
    fn counts(r: &SyncResult) -> (usize, usize, usize) {
        (r.uploaded, r.downloaded, r.deleted)
    }

    /// The files moved aside as deleted remotely, by their path in their sync's directory.
    fn quarantined(root: &Path) -> Vec<String> {
        fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let name = format!("{prefix}{}", entry.file_name().to_string_lossy());
                if entry.file_type().unwrap().is_dir() {
                    walk(&entry.path(), &format!("{name}/"), out);
                } else {
                    out.push(name);
                }
            }
        }
        let mut out = Vec::new();
        if let Ok(runs) = std::fs::read_dir(root.join(QUARANTINE_DIR)) {
            for run in runs {
                walk(&run.unwrap().path(), "", &mut out);
            }
        }
        out.sort();
        out
    }

    fn has_notice(r: &SyncResult, start: &str) -> bool {
        r.notices.iter().any(|n| n.starts_with(start))
    }

    /// A file deleted remotely isn't uploaded again from its local copy by the next sync (a new
    /// `CloudSync`, going by the manifest the last one saved): the copy, unchanged since, is
    /// moved aside rather than deleted, and the folders it leaves empty go. After that there is
    /// nothing to do.
    #[tokio::test]
    async fn remote_deletions_are_moved_aside_not_uploaded_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        store.put("/gone.txt", "gone");
        store.put("/sub/deep/gone.txt", "deep");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 3, 0));
        assert!(root.join(MANIFEST_FILE).is_file());

        sync.provider.remove("/gone.txt");
        sync.provider.remove("/sub/deep/gone.txt");
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 2));
        assert!(sync.provider.uploads().is_empty());
        assert_eq!(quarantined(root), vec!["gone.txt", "sub/deep/gone.txt"]);
        assert!(!root.join("gone.txt").exists() && !root.join("sub").exists());
        assert_eq!(read(root, "a.txt"), "a");
        assert!(
            has_notice(&r, "2 files deleted remotely"),
            "{:?}",
            r.notices
        );

        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert_eq!(quarantined(root).len(), 2);
        assert!(sync.provider.uploads().is_empty());
        assert!(sync.provider.deletes.lock().unwrap().is_empty());
    }

    /// A file deleted remotely but changed here since the last sync is a conflict that keeps the
    /// local copy: it is uploaded again, and the result says so.
    #[tokio::test]
    async fn a_file_deleted_remotely_but_changed_here_is_uploaded_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        clean(&mut sync).await;

        sync.provider.remove("/a.txt");
        write(root, "a.txt", "edited here");
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (1, 0, 0));
        assert_eq!(
            sync.provider.content("/a.txt").as_deref(),
            Some("edited here")
        );
        assert!(quarantined(root).is_empty());
        assert!(
            has_notice(&r, "/a.txt was deleted remotely but changed here"),
            "{:?}",
            r.notices
        );

        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
    }

    /// A file deleted here isn't downloaded again while it is unchanged remotely, nor deleted
    /// remotely; the result says so the first time. Once changed remotely, it comes back.
    #[tokio::test]
    async fn local_deletions_are_not_downloaded_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        store.put("/sub/b.txt", "b");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 2, 0));

        std::fs::remove_file(root.join("sub/b.txt")).unwrap();
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(has_notice(&r, "1 file deleted here"), "{:?}", r.notices);
        assert_eq!(sync.provider.downloads(), vec!["/a.txt", "/sub/b.txt"]);
        assert_eq!(sync.provider.content("/sub/b.txt").as_deref(), Some("b"));
        assert!(sync.provider.deletes.lock().unwrap().is_empty());

        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert!(!root.join("sub/b.txt").exists());

        sync.provider.put("/sub/b.txt", "b, edited remotely");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));
        assert_eq!(read(root, "sub/b.txt"), "b, edited remotely");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
    }

    /// Files unchanged on both sides since the last sync are left alone, with no transfer and no
    /// conflict strategy, even when the listing carries no hash the provider checks (Google
    /// Drive), and when the local file was only touched (same content, another time). Without
    /// the manifest, `AskUser` reports each of them as a conflict.
    #[tokio::test]
    async fn unchanged_files_skip_the_conflict_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::default();
        store.put("/a.txt", "a");
        write(root, "mine.txt", "mine");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (1, 1, 0));

        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::AskUser);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);

        let touched = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        std::fs::File::options()
            .write(true)
            .open(root.join("a.txt"))
            .unwrap()
            .set_modified(touched)
            .unwrap();
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);

        std::fs::remove_file(root.join(MANIFEST_FILE)).unwrap();
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert_eq!(r.conflicts.len(), 2, "{:?}", r.conflicts);
    }

    /// A file changed on one side only is sent across without the conflict strategy (`AskUser`
    /// would report it), as far as the direction allows: a change the direction doesn't send
    /// waits for a sync that does.
    #[tokio::test]
    async fn a_change_on_one_side_is_sent_across() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        store.put("/b.txt", "b");
        let config = SyncConfig {
            conflict_strategy: ConflictStrategy::AskUser,
            ..kept(root, SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(store, config);
        assert_eq!(counts(&clean(&mut sync).await), (0, 2, 0));

        write(root, "a.txt", "a, edited here");
        sync.provider.put("/b.txt", "b, edited remotely");
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (1, 1, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);
        assert_eq!(
            sync.provider.content("/a.txt").as_deref(),
            Some("a, edited here")
        );
        assert_eq!(read(root, "b.txt"), "b, edited remotely");

        // Download-only doesn't upload the next local edit, nor forget it.
        write(root, "a.txt", "a, edited here again");
        let mut sync = again_with(sync, |c| c.direction = SyncDirection::Download);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        // Upload-only sends it, and doesn't download the next remote edit.
        sync.provider.put("/b.txt", "b, edited remotely again");
        let mut sync = again_with(sync, |c| c.direction = SyncDirection::Upload);
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        assert_eq!(read(root, "b.txt"), "b, edited remotely");
        // Both ways, that one comes down.
        let mut sync = again_with(sync, |c| c.direction = SyncDirection::Bidirectional);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 1, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);
        assert_eq!(read(root, "b.txt"), "b, edited remotely again");
        assert_eq!(
            sync.provider.content("/a.txt").as_deref(),
            Some("a, edited here again")
        );
    }

    /// A file changed on both sides since the last sync goes through the conflict strategy:
    /// reported by `AskUser` and left as it was in the manifest, so it is reported again until
    /// resolved (here by `LocalWins`). Changed on both sides to the same content (as the
    /// provider vouches), it is no conflict.
    #[tokio::test]
    async fn a_change_on_both_sides_goes_through_the_conflict_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        let config = SyncConfig {
            conflict_strategy: ConflictStrategy::AskUser,
            ..kept(root, SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(store, config);
        clean(&mut sync).await;

        write(root, "a.txt", "a, edited here");
        sync.provider.put("/a.txt", "a, edited remotely");
        for _ in 0..2 {
            sync = again(sync);
            let r = clean(&mut sync).await;
            assert_eq!(counts(&r), (0, 0, 0));
            assert_eq!(r.conflicts.len(), 1, "{:?}", r.conflicts);
        }
        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::LocalWins);
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        assert_eq!(
            sync.provider.content("/a.txt").as_deref(),
            Some("a, edited here")
        );

        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::AskUser);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);

        write(root, "a.txt", "the same on both sides");
        sync.provider.put("/a.txt", "the same on both sides");
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);
    }

    /// Moving a local copy aside stays inside the sync root: with the quarantine directory a
    /// symlink to elsewhere, the copy is left where it is (an error, and it stays in the
    /// manifest, so it is neither forgotten nor uploaded) and nothing is written elsewhere. The
    /// sync's own entries (the manifest, what was moved aside) are never uploaded or written
    /// over by a remote file of that name, even with hidden files synced.
    #[cfg(unix)]
    #[tokio::test]
    async fn moving_aside_stays_inside_the_sync_root() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        let elsewhere = outer.path().join("elsewhere");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&elsewhere).unwrap();
        let store = Store::hashed();
        store.put("/gone.txt", "gone");
        store.put("/kept.txt", "kept"); // an empty listing moves nothing aside
        let mut sync = CloudSync::new(store, kept(&root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 2, 0));

        std::os::unix::fs::symlink(&elsewhere, root.join(QUARANTINE_DIR)).unwrap();
        sync.provider.remove("/gone.txt");
        for _ in 0..2 {
            sync = again(sync);
            let r = sync.sync().await.unwrap();
            assert_eq!(counts(&r), (0, 0, 0));
            assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
            assert!(
                r.errors[0].starts_with("Moving aside /gone.txt (deleted remotely) failed: ")
                    && r.errors[0].contains("resolves outside sync root"),
                "{:?}",
                r.errors
            );
            assert_eq!(read(&root, "gone.txt"), "gone");
            assert_eq!(std::fs::read_dir(&elsewhere).unwrap().count(), 0);
            assert!(sync.provider.uploads().is_empty());
        }
        std::fs::remove_file(root.join(QUARANTINE_DIR)).unwrap();

        for path in [
            "/.rms-sync-state.json",
            "/.RMS-REMOTE-DELETED/x/gone.txt",
            "/sub/.rms-sync-state.json",
        ] {
            sync.provider.put(path, "remote");
        }
        let mut sync = again_with(sync, |c| c.sync_hidden = true);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 1));
        assert_eq!(quarantined(&root), vec!["gone.txt"]);
        assert!(sync.provider.uploads().is_empty());
        assert_eq!(sync.provider.downloads(), vec!["/gone.txt", "/kept.txt"]);
        assert!(!root.join("sub").exists());
        assert!(!root.join(".RMS-REMOTE-DELETED").exists());
        let stored = std::fs::read(root.join(MANIFEST_FILE)).unwrap();
        serde_json::from_slice::<ManifestFile>(&stored).expect("the manifest");
    }

    /// `quarantine` moves only a file inside the root, named by a safe sync path, and never over
    /// a file already moved aside.
    #[cfg(unix)]
    #[tokio::test]
    async fn quarantine_moves_only_files_inside_the_root() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        write(&root, "dir/a.txt", "a");
        write(&root, "b.txt", "first");
        write(outer.path(), "elsewhere/x.txt", "x");
        std::os::unix::fs::symlink(outer.path().join("elsewhere"), root.join("link")).unwrap();

        let err = quarantine(&root, "run", "/link/x.txt", &root.join("link/x.txt"))
            .await
            .unwrap_err();
        assert!(matches!(err, IntegrationError::InvalidPath(_)), "{err}");
        assert_eq!(read(outer.path(), "elsewhere/x.txt"), "x");
        for bad in ["/../b.txt", "/dir/../b.txt", ""] {
            let err = quarantine(&root, "run", bad, &root.join("b.txt"))
                .await
                .unwrap_err();
            assert!(
                matches!(err, IntegrationError::InvalidPath(_)),
                "{bad:?}: {err}"
            );
        }
        let err = quarantine(&root, "run", "/dir", &root.join("dir"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, IntegrationError::LocalPathUnusable(_)),
            "{err}"
        );

        let to = quarantine(&root, "run", "/b.txt", &root.join("b.txt")).await;
        assert_eq!(to.unwrap(), Path::new(QUARANTINE_DIR).join("run/b.txt"));
        write(&root, "b.txt", "second");
        let to = quarantine(&root, "run", "/b.txt", &root.join("b.txt")).await;
        assert_eq!(to.unwrap(), Path::new(QUARANTINE_DIR).join("run/b.txt (2)"));
        assert_eq!(read(&root, ".rms-remote-deleted/run/b.txt"), "first");
        assert_eq!(read(&root, ".rms-remote-deleted/run/b.txt (2)"), "second");
        // The directory it leaves empty goes; the root never does.
        quarantine(&root, "run", "/dir/a.txt", &root.join("dir/a.txt"))
            .await
            .unwrap();
        assert!(!root.join("dir").exists() && root.is_dir());
    }

    /// A sync that fails as a whole (here the listing) changes nothing and leaves the saved
    /// manifest as it was. One that fails for some files records the rest and keeps the failed
    /// files' old entries, so the next sync sees the same change and tries again (rather than
    /// take the file for unchanged, or for a conflict).
    #[tokio::test]
    async fn failures_never_record_what_did_not_happen() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        store.put("/b.txt", "b");
        let config = SyncConfig {
            conflict_strategy: ConflictStrategy::AskUser,
            ..kept(root, SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(store, config);
        clean(&mut sync).await;
        let saved = std::fs::read(root.join(MANIFEST_FILE)).unwrap();

        sync.provider
            .list_fails
            .store(true, std::sync::atomic::Ordering::SeqCst);
        sync.provider.remove("/a.txt");
        let mut sync = again(sync);
        let r = sync.sync().await.unwrap();
        assert_eq!(r.status, SyncStatus::Failed);
        assert_eq!(std::fs::read(root.join(MANIFEST_FILE)).unwrap(), saved);
        assert_eq!(read(root, "a.txt"), "a");

        sync.provider
            .list_fails
            .store(false, std::sync::atomic::Ordering::SeqCst);
        sync.provider.put("/b.txt", "b, edited remotely");
        sync.provider.put("/c.txt", "c");
        sync.provider.fail.lock().unwrap().insert("/b.txt".into());
        let mut sync = again(sync);
        let r = sync.sync().await.unwrap();
        assert_eq!(r.status, SyncStatus::PartialSuccess);
        assert_eq!(counts(&r), (0, 1, 1));
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert!(
            r.errors[0].starts_with("Download /b.txt failed"),
            "{:?}",
            r.errors
        );
        assert_eq!(read(root, "b.txt"), "b");
        assert_eq!(quarantined(root), vec!["a.txt"]);

        sync.provider.fail.lock().unwrap().clear();
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 1, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);
        assert_eq!(read(root, "b.txt"), "b, edited remotely");
    }

    /// The manifest is kept per account and cloud folder (a trailing slash aside): when the
    /// provider is connected to another account, whose files these aren't, or another folder is
    /// synced into the same directory, that sync infers no deletions, and each keeps its own.
    #[tokio::test]
    async fn manifests_are_kept_per_account_and_folder() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store {
            account: Some("alice".into()),
            ..Store::hashed()
        };
        store.put("/a.txt", "a");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));

        sync.provider.account = Some("bob".into());
        sync.provider.remove("/a.txt");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));

        sync.provider.remove("/a.txt");
        let mut sync = again_with(sync, |c| c.cloud_folder = Some("/Other".into()));
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        let mut sync = again_with(sync, |c| c.cloud_folder = Some("/Other/".into()));
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert!(quarantined(root).is_empty());

        let stored: ManifestFile =
            serde_json::from_slice(&std::fs::read(root.join(MANIFEST_FILE)).unwrap()).unwrap();
        let keys: Vec<(&str, &str)> = stored
            .syncs
            .iter()
            .map(|s| (s.account.as_str(), s.cloud_folder.as_str()))
            .collect();
        assert_eq!(keys, vec![("alice", ""), ("bob", ""), ("bob", "/Other")]);
        assert_eq!(stored.version, MANIFEST_VERSION);
    }

    /// A file over the size limit on either side is out of view, not deleted: the local copy of
    /// a remote file that grew past the limit isn't moved aside, and a local file past it is
    /// neither uploaded nor written over by the remote file. Back under the limit, it syncs
    /// against the manifest as before.
    #[tokio::test]
    async fn files_over_the_size_limit_are_out_of_view_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/big.txt", "small");
        let config = SyncConfig {
            max_file_size: Some(16),
            ..kept(root, SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(store, config);
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));

        sync.provider.put("/big.txt", &"x".repeat(100));
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "big.txt"), "small");
        assert!(quarantined(root).is_empty());

        sync.provider.put("/big.txt", "tiny");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));
        assert_eq!(read(root, "big.txt"), "tiny");

        write(root, "big.txt", &"y".repeat(100));
        sync.provider.put("/big.txt", "remote edit");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "big.txt"), "y".repeat(100));
        assert_eq!(
            sync.provider.content("/big.txt").as_deref(),
            Some("remote edit")
        );
    }

    /// Upload-only never touches local files: the local copy of a file deleted remotely stays,
    /// and isn't uploaded again unless it changes.
    #[tokio::test]
    async fn upload_only_keeps_the_local_copy_of_a_remote_deletion() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "a.txt", "a");
        let mut sync = CloudSync::new(Store::hashed(), kept(root, SyncDirection::Upload));
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));

        sync.provider.remove("/a.txt");
        for _ in 0..2 {
            sync = again(sync);
            assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
            assert_eq!(read(root, "a.txt"), "a");
        }
        assert!(quarantined(root).is_empty());

        write(root, "a.txt", "a, edited here");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        assert_eq!(
            sync.provider.content("/a.txt").as_deref(),
            Some("a, edited here")
        );
    }

    /// A remote deletion an upload-only sync saw (and, never touching local files, left alone)
    /// is still one for the next sync that may download: that sync moves the unchanged local
    /// copy aside, as it would have without the upload-only sync in between.
    #[tokio::test]
    async fn a_remote_deletion_left_by_an_upload_only_sync_is_applied_later() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        store.put("/b.txt", "b");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 2, 0));

        sync.provider.remove("/a.txt");
        let mut sync = again_with(sync, |c| c.direction = SyncDirection::Upload);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "a.txt"), "a");
        assert!(quarantined(root).is_empty());

        let mut sync = again_with(sync, |c| c.direction = SyncDirection::Bidirectional);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 1));
        assert_eq!(quarantined(root), vec!["a.txt"]);
        assert!(sync.provider.uploads().is_empty());
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
    }

    /// An empty listing moves nothing aside: a sync folder that was trashed, unshared or moved
    /// can list as empty, and taking that for the deletion of every file in it would move the
    /// whole local copy aside. The sync says so, and keeps the state, so once the listing shows
    /// what is there again, only real deletions are moved aside.
    #[tokio::test]
    async fn an_empty_listing_moves_nothing_aside() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/a.txt", "a");
        store.put("/sub/b.txt", "b");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 2, 0));

        let listed = std::mem::take(&mut *sync.provider.files.lock().unwrap());
        for _ in 0..2 {
            sync = again(sync);
            let r = sync.sync().await.unwrap();
            assert_eq!(r.status, SyncStatus::Failed);
            assert_eq!(counts(&r), (0, 0, 0));
            assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
            assert!(
                r.errors[0].starts_with("The cloud folder listed nothing, but 2 files there")
                    && r.errors[0].ends_with(": /a.txt, /sub/b.txt"),
                "{:?}",
                r.errors
            );
            assert!(quarantined(root).is_empty());
            assert_eq!(
                (read(root, "a.txt"), read(root, "sub/b.txt")),
                ("a".into(), "b".into())
            );
            assert!(sync.provider.uploads().is_empty());
        }

        *sync.provider.files.lock().unwrap() = listed;
        sync.provider.remove("/a.txt");
        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 1));
        assert_eq!(quarantined(root), vec!["a.txt"]);
        assert_eq!(read(root, "sub/b.txt"), "b");
    }

    /// An upload the provider stores as a new file next to the one listed at its path, which
    /// the listing goes on showing (Google Drive did that), isn't recorded: the next sync would
    /// take the listed file for a change made remotely and download it over the local edit.
    /// The edit is kept and sent again, and each sync says why.
    #[tokio::test]
    async fn an_upload_stored_as_another_file_never_reverts_the_local_edit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store {
            creates_new: true,
            ..Store::hashed()
        };
        store.put("/a.txt", "a");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));

        write(root, "a.txt", "a, edited here");
        for n in 1..=3 {
            sync = again(sync);
            let r = sync.sync().await.unwrap();
            assert_eq!(r.status, SyncStatus::PartialSuccess);
            assert_eq!(counts(&r), (1, 0, 0));
            assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
            assert!(
                r.errors[0].starts_with("Uploaded /a.txt, but it was stored as a new file"),
                "{:?}",
                r.errors
            );
            assert_eq!(read(root, "a.txt"), "a, edited here");
            assert_eq!(sync.provider.shadowed.lock().unwrap().len(), n);
        }
        assert_eq!(sync.provider.downloads(), vec!["/a.txt"]);
    }

    /// Delta sync records its downloads in the manifest, so the next full sync finds those files
    /// unchanged on both sides, not changed on both (which `AskUser` would report: this
    /// provider's listing has no hash to vouch for the content).
    #[tokio::test]
    async fn delta_downloads_are_recorded_for_the_next_full_sync() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::default();
        store.put("/a.txt", "a");
        // A delta takes a local file stamped after the last sync for a local edit; the remote
        // version wins that here.
        let config = SyncConfig {
            conflict_strategy: ConflictStrategy::CloudWins,
            ..kept(root, SyncDirection::Bidirectional)
        };
        let mut sync = CloudSync::new(store, config);
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(sync.state().cursor.as_deref(), Some("c1"));

        sync.provider.put("/a.txt", "a, edited remotely");
        let r = sync.delta_sync().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(r.downloaded, 1);
        assert_eq!(read(root, "a.txt"), "a, edited remotely");

        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::AskUser);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.conflicts.is_empty(), "{:?}", r.conflicts);
    }

    /// A manifest this server can't read stops the sync rather than sync without it (which would
    /// bring back what was deleted), and is left as it is.
    #[tokio::test]
    async fn an_unreadable_manifest_stops_the_sync() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "a.txt", "a");
        for bad in ["not json", r#"{"version":2,"syncs":[]}"#] {
            write(root, MANIFEST_FILE, bad);
            let mut sync =
                CloudSync::new(Store::hashed(), kept(root, SyncDirection::Bidirectional));
            let r = sync.sync().await.unwrap();
            assert_eq!(r.status, SyncStatus::Failed);
            assert!(
                r.errors[0].starts_with("Failed to load the sync state: "),
                "{:?}",
                r.errors
            );
            assert!(sync.provider.uploads().is_empty());
            assert_eq!(read(root, MANIFEST_FILE), bad);
        }
    }

    /// The sync that moves the old layout aside (see `move_legacy_layout`) goes without the
    /// manifest: the files it moved aren't deletions made here, so the remote copies of those
    /// the manifest knew come down again.
    #[tokio::test]
    async fn moving_the_old_layout_aside_starts_a_fresh_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/Notes/a.txt", "a");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));

        sync.provider.legacy = Some(vec!["Notes".into()]);
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert!(
            has_notice(&r, "Moved Notes to .rms-old-layout/Notes"),
            "{:?}",
            r.notices
        );
        assert_eq!(counts(&r), (0, 1, 0));
        assert_eq!(read(root, "Notes/a.txt"), "a");
        assert_eq!(read(root, ".rms-old-layout/Notes/a.txt"), "a");

        let mut sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
    }

    /// A provider that ignores case lists a file under the spelling it was created with, however
    /// it is written to later. A case-only rename here (of a file, or of a directory above one)
    /// is the same file under another spelling: nothing is transferred or moved aside, and no
    /// deletion is recorded. An edit then goes to that file, and its remote deletion is still
    /// seen. A case-only rename there changes nothing here.
    #[tokio::test]
    async fn a_case_only_rename_is_the_same_file_where_case_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store {
            hashed: true,
            ignores_case: true,
            ..Default::default()
        };
        store.put("/report.pdf", "report");
        store.put("/sub/a.txt", "a");
        store.put("/other.txt", "other");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 3, 0));

        std::fs::rename(root.join("report.pdf"), root.join("Report.pdf")).unwrap();
        std::fs::rename(root.join("sub"), root.join("Sub")).unwrap();
        for _ in 0..3 {
            sync = again(sync);
            let r = clean(&mut sync).await;
            assert_eq!(counts(&r), (0, 0, 0));
            assert!(r.notices.is_empty(), "{:?}", r.notices);
        }
        assert_eq!(read(root, "Report.pdf"), "report");
        assert_eq!(read(root, "Sub/a.txt"), "a");
        assert!(!root.join("report.pdf").exists() && !root.join("sub").exists());
        assert!(quarantined(root).is_empty());
        assert!(sync.provider.uploads().is_empty());
        assert_eq!(sync.provider.downloads().len(), 3);

        // Renamed there, only in case: still the same file.
        {
            let mut files = sync.provider.files.lock().unwrap();
            let (file, content) = files.remove("/sub/a.txt").unwrap();
            let path = "/SUB/A.txt".to_string();
            files.insert(path.clone(), (CloudFile { path, ..file }, content));
        }
        sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert_eq!(read(root, "Sub/a.txt"), "a");

        // Edited here: uploaded over the one file, which keeps its spelling there.
        write(root, "Report.pdf", "report, edited here");
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        assert_eq!(sync.provider.uploads(), vec!["/Report.pdf"]);
        assert_eq!(
            sync.provider.content("/report.pdf").as_deref(),
            Some("report, edited here")
        );
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));

        // Deleted there: the local copy, unchanged since, is moved aside.
        sync.provider.remove("/report.pdf");
        sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 1));
        assert!(r.notices[0].ends_with(": /Report.pdf"), "{:?}", r.notices);
        assert_eq!(quarantined(root), vec!["Report.pdf"]);

        // Renamed here and deleted there since the last sync: the entry follows the rename, so
        // the local copy is moved aside as deleted remotely, not uploaded as a new file.
        std::fs::rename(root.join("Sub/a.txt"), root.join("Sub/A.TXT")).unwrap();
        sync.provider.remove("/SUB/A.txt");
        sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 1));
        assert!(r.notices[0].ends_with(": /Sub/A.TXT"), "{:?}", r.notices);
        assert_eq!(quarantined(root), vec!["Report.pdf", "Sub/A.TXT"]);
        assert_eq!(sync.provider.uploads(), vec!["/Report.pdf"]);
        assert_eq!(sync.provider.downloads().len(), 3);
    }

    /// Local files whose paths differ only in letter case would all be written to one file
    /// where the provider ignores case, each over the others. The one spelled as the listing
    /// spells it is synced; the others are left alone, with an error, until renamed: never
    /// uploaded or moved aside. With no such spelling, none is synced, and the listed file isn't
    /// downloaded as yet another. A provider that tells case apart syncs each as its own file.
    #[tokio::test]
    async fn local_files_differing_only_in_case_are_left_alone_where_case_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store {
            hashed: true,
            ignores_case: true,
            ..Default::default()
        };
        store.put("/report.pdf", "report");
        store.put("/y.txt", "y");
        write(root, "report.pdf", "report");
        write(root, "Report.pdf", "another report, here");
        write(root, "x.txt", "x");
        write(root, "X.txt", "another x");
        write(root, "Y.txt", "y");
        write(root, "y.TXT", "another y");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        for round in 0..3 {
            let r = sync.sync().await.unwrap();
            assert_eq!(counts(&r), (0, 0, 0), "round {round}");
            assert_eq!(r.errors.len(), 1, "round {round}: {:?}", r.errors);
            assert!(
                r.errors[0].contains(
                    "here they name different files: /Report.pdf and /report.pdf, /X.txt and \
                     /x.txt, /Y.txt and /y.TXT and /y.txt. "
                ),
                "{:?}",
                r.errors
            );
            assert!(r.notices.is_empty(), "round {round}: {:?}", r.notices);
            assert!(sync.provider.uploads().is_empty());
            assert!(sync.provider.downloads().is_empty());
            sync = again(sync);
        }
        assert!(quarantined(root).is_empty());
        assert_eq!(read(root, "Report.pdf"), "another report, here");
        assert_eq!(
            sync.provider.content("/report.pdf").as_deref(),
            Some("report")
        );

        // Renamed apart: each is synced as its own file from then on.
        std::fs::rename(root.join("Report.pdf"), root.join("Report 2.pdf")).unwrap();
        std::fs::rename(root.join("X.txt"), root.join("x2.txt")).unwrap();
        std::fs::remove_file(root.join("y.TXT")).unwrap();
        let r = clean(&mut sync).await;
        assert_eq!(counts(&r), (3, 0, 0));
        let mut uploaded = sync.provider.uploads();
        uploaded.sort();
        assert_eq!(uploaded, vec!["/Report 2.pdf", "/x.txt", "/x2.txt"]);
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));

        // Where case tells files apart, they were never one file.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store::hashed();
        store.put("/report.pdf", "report");
        write(root, "report.pdf", "report");
        write(root, "Report.pdf", "another report, here");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        assert_eq!(sync.provider.uploads(), vec!["/Report.pdf"]);
        assert_eq!(
            sync.provider.content("/report.pdf").as_deref(),
            Some("report")
        );
    }

    /// Where the provider ignores case: `/report.pdf` ("v1") synced down and renamed here to
    /// `Report.pdf`, whose entry moves to that spelling. Returns the next sync.
    async fn tracked_then_renamed(root: &Path) -> CloudSync<Store> {
        let store = Store {
            hashed: true,
            ignores_case: true,
            ..Default::default()
        };
        store.put("/report.pdf", "v1");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));
        std::fs::rename(root.join("report.pdf"), root.join("Report.pdf")).unwrap();
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(recorded(&sync), ["/Report.pdf"]);
        again(sync)
    }

    /// The paths the manifest of `sync`'s last sync records.
    fn recorded<P: CloudProvider>(sync: &CloudSync<P>) -> Vec<String> {
        let manifest = sync.state().manifest.as_ref().unwrap();
        manifest.files.keys().cloned().collect()
    }

    /// A sync that reports the clash of `Report.pdf` and `report.pdf`, and nothing else.
    async fn clash_sync(sync: &mut CloudSync<Store>) -> SyncResult {
        let r = sync.sync().await.unwrap();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert!(
            r.errors[0].contains("here they name different files: /Report.pdf and /report.pdf."),
            "{:?}",
            r.errors
        );
        r
    }

    /// [`tracked_then_renamed`], then a second local file spelled as listed, `report.pdf`
    /// ("other"). That sync reports the clash and sends nothing either way: `Report.pdf` was
    /// synced under its spelling, so `report.pdf`, though spelled as listed, isn't synced
    /// either. The one entry, `Report.pdf`'s, still describes the remote file. Returns the next
    /// sync.
    async fn clashing_newcomer(root: &Path) -> CloudSync<Store> {
        let mut sync = tracked_then_renamed(root).await;
        write(root, "report.pdf", "other");
        let r = clash_sync(&mut sync).await;
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
        assert_eq!(read(root, "Report.pdf"), "v1");
        assert_eq!(read(root, "report.pdf"), "other");
        assert_eq!(recorded(&sync), ["/Report.pdf"]);
        again(sync)
    }

    /// A clash settled by removing the newcomer: `Report.pdf` is where it was, and the remote
    /// file still has its content (review of #40, verification round 2).
    #[tokio::test]
    async fn a_clash_settled_by_removing_the_newcomer_keeps_the_tracked_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = clashing_newcomer(root).await;
        std::fs::remove_file(root.join("report.pdf")).unwrap();
        for _ in 0..2 {
            let r = clean(&mut sync).await;
            assert_eq!(counts(&r), (0, 0, 0));
            assert!(r.notices.is_empty(), "{:?}", r.notices);
            sync = again(sync);
        }
        assert_eq!(read(root, "Report.pdf"), "v1");
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
        assert!(sync.provider.uploads().is_empty());
        assert_eq!(sync.provider.downloads(), ["/report.pdf"]);
        assert!(!root.join("report.pdf").exists());
        assert!(quarantined(root).is_empty());
    }

    /// The other ways to settle the clash: removing the tracked file, or renaming either file
    /// apart. Whichever file stays at the path keeps its content and ends up in the remote
    /// file; one renamed apart is uploaded as its own.
    #[tokio::test]
    async fn a_clash_settled_otherwise_keeps_the_file_that_stays() {
        // The tracked file removed: the newcomer, compared with its entry by content, changed
        // here, and is uploaded.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = clashing_newcomer(root).await;
        std::fs::remove_file(root.join("Report.pdf")).unwrap();
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "report.pdf"), "other");
        assert!(!root.join("Report.pdf").exists());
        assert_eq!(
            sync.provider.content("/report.pdf").as_deref(),
            Some("other")
        );
        assert_eq!(sync.provider.downloads(), ["/report.pdf"]);

        // The newcomer renamed apart: uploaded as its own file; the tracked one is unchanged.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = clashing_newcomer(root).await;
        std::fs::rename(root.join("report.pdf"), root.join("report 2.pdf")).unwrap();
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Report.pdf"), "v1");
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
        assert_eq!(
            sync.provider.content("/report 2.pdf").as_deref(),
            Some("other")
        );

        // The tracked file renamed apart: uploaded as its own file, and the newcomer over the
        // remote file.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = clashing_newcomer(root).await;
        std::fs::rename(root.join("Report.pdf"), root.join("Report old.pdf")).unwrap();
        assert_eq!(counts(&clean(&mut sync).await), (2, 0, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "report.pdf"), "other");
        assert_eq!(read(root, "Report old.pdf"), "v1");
        assert_eq!(
            sync.provider.content("/report.pdf").as_deref(),
            Some("other")
        );
        assert_eq!(
            sync.provider.content("/Report old.pdf").as_deref(),
            Some("v1")
        );

        // The newcomer renamed only in case: still a clash, until the newcomer goes.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = clashing_newcomer(root).await;
        std::fs::rename(root.join("report.pdf"), root.join("REPORT.pdf")).unwrap();
        let r = sync.sync().await.unwrap();
        assert_eq!(counts(&r), (0, 0, 0));
        assert!(
            r.errors[0].contains("/REPORT.pdf and /Report.pdf and /report.pdf."),
            "{:?}",
            r.errors
        );
        std::fs::remove_file(root.join("REPORT.pdf")).unwrap();
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Report.pdf"), "v1");
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
        assert!(sync.provider.uploads().is_empty());
    }

    /// Whatever the remote file does during the clash (edited there, or edited and restored),
    /// and whatever the conflict strategy, the direction, or a download failing, the clash sync
    /// sends nothing either way and leaves the one entry as it was. Once the newcomer is
    /// removed, `Report.pdf`, unchanged since the remote file last had its content, gets the
    /// remote change, as any file unchanged here does: the remote file was never written from
    /// the newcomer, so the content it replaced there was the synced one. Renamed there in case
    /// to `Report.pdf`'s spelling, the remote file syncs with it, the one synced before; renamed
    /// back, it is left alone again.
    ///
    /// Before, the clash sync uploaded the newcomer. A conflict it then left pending with the
    /// remote file edited there (with AskUser, KeepBoth, an upload-only direction, a failed
    /// download) saved an entry for the listed spelling that copied the older record's remote
    /// side, so the group's entries agreed on "v1" again. Once the newcomer was removed,
    /// `Report.pdf` was taken for unchanged since then and the remote file downloaded over it,
    /// though "v1" had been replaced there by the newcomer, not by that edit (review of #40,
    /// verification round 3).
    #[tokio::test]
    async fn a_clash_leaves_the_remote_file_alone_until_settled() {
        type Change = fn(&CloudSync<Store>);
        let edited: Change = |s| {
            s.provider.put("/report.pdf", "other2");
        };
        let restored: Change = |s| {
            s.provider.put("/report.pdf", "v0");
            s.provider.put("/report.pdf", "v1");
        };
        for (strategy, direction, failing, change, becomes) in [
            (
                ConflictStrategy::AskUser,
                SyncDirection::Bidirectional,
                false,
                edited,
                "other2",
            ),
            (
                ConflictStrategy::KeepBoth,
                SyncDirection::Bidirectional,
                false,
                edited,
                "other2",
            ),
            (
                ConflictStrategy::CloudWins,
                SyncDirection::Upload,
                false,
                edited,
                "other2",
            ),
            (
                ConflictStrategy::CloudWins,
                SyncDirection::Bidirectional,
                true,
                edited,
                "other2",
            ),
            (
                ConflictStrategy::LocalWins,
                SyncDirection::Bidirectional,
                false,
                edited,
                "other2",
            ),
            (
                ConflictStrategy::AskUser,
                SyncDirection::Bidirectional,
                false,
                restored,
                "v1",
            ),
        ] {
            let case = format!("{strategy:?} {direction:?} failing {failing} -> {becomes}");
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let sync = clashing_newcomer(root).await;
            change(&sync);
            let remote = sync.provider.content("/report.pdf");
            if failing {
                sync.provider
                    .fail
                    .lock()
                    .unwrap()
                    .insert("/report.pdf".into());
            }
            let mut sync = again_with(sync, |c| {
                c.conflict_strategy = strategy;
                c.direction = direction;
            });
            for _ in 0..2 {
                let r = clash_sync(&mut sync).await;
                assert_eq!((counts(&r), r.conflicts.len()), ((0, 0, 0), 0), "{case}");
                assert!(r.notices.is_empty(), "{case}: {:?}", r.notices);
                assert_eq!(recorded(&sync), ["/Report.pdf"], "{case}");
                sync = again(sync);
            }
            assert_eq!(sync.provider.content("/report.pdf"), remote, "{case}");
            assert_eq!(read(root, "Report.pdf"), "v1", "{case}");
            assert_eq!(read(root, "report.pdf"), "other", "{case}");
            assert!(sync.provider.uploads().is_empty(), "{case}");
            assert_eq!(sync.provider.downloads(), ["/report.pdf"], "{case}");

            sync.provider.fail.lock().unwrap().clear();
            std::fs::remove_file(root.join("report.pdf")).unwrap();
            let mut sync = again_with(sync, |c| {
                c.conflict_strategy = ConflictStrategy::AskUser;
                c.direction = SyncDirection::Bidirectional;
            });
            let r = clean(&mut sync).await;
            let downloaded = usize::from(becomes != "v1");
            assert_eq!(counts(&r), (0, downloaded, 0), "{case}");
            assert!(r.conflicts.is_empty() && r.notices.is_empty(), "{case}");
            sync = again(sync);
            assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0), "{case}");
            assert_eq!(read(root, "Report.pdf"), becomes, "{case}");
            assert_eq!(sync.provider.content("/report.pdf"), remote, "{case}");
            assert!(sync.provider.uploads().is_empty(), "{case}");
        }

        // Renamed there to the newcomer's spelling, and edited: the same.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = tracked_then_renamed(root).await;
        {
            let mut files = sync.provider.files.lock().unwrap();
            let (file, content) = files.remove("/report.pdf").unwrap();
            let path = "/Report.pdf".to_string();
            files.insert(path.clone(), (CloudFile { path, ..file }, content));
        }
        write(root, "report.pdf", "other");
        // Listed as `Report.pdf`, which is the one synced before: it syncs, and the newcomer is
        // left alone.
        assert_eq!(counts(&clash_sync(&mut sync).await), (0, 0, 0));
        sync.provider.put("/Report.pdf", "v2");
        sync = again(sync);
        assert_eq!(counts(&clash_sync(&mut sync).await), (0, 1, 0));
        assert_eq!(read(root, "Report.pdf"), "v2");
        // Renamed back there: listed as the newcomer, and nothing is synced.
        {
            let mut files = sync.provider.files.lock().unwrap();
            let (file, content) = files.remove("/Report.pdf").unwrap();
            let path = "/report.pdf".to_string();
            files.insert(path.clone(), (CloudFile { path, ..file }, content));
        }
        sync.provider.put("/report.pdf", "v3");
        sync = again(sync);
        assert_eq!(counts(&clash_sync(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Report.pdf"), "v2");
        assert_eq!(read(root, "report.pdf"), "other");
        assert!(sync.provider.uploads().is_empty());
    }

    /// Both files edited during the clash: nothing is sent. Once the newcomer is renamed apart,
    /// the tracked file changed here only, and is uploaded, as the newcomer is as its own file:
    /// neither edit is overwritten here. The conflict strategy, which would take the remote
    /// file, is never asked.
    #[tokio::test]
    async fn a_clash_with_both_files_edited_keeps_both_edits() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let sync = clashing_newcomer(root).await;
        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::CloudWins);
        write(root, "Report.pdf", "v1, edited");
        write(root, "report.pdf", "other, edited");
        assert_eq!(counts(&clash_sync(&mut sync).await), (0, 0, 0));
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));

        std::fs::rename(root.join("report.pdf"), root.join("other.pdf")).unwrap();
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (2, 0, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Report.pdf"), "v1, edited");
        assert_eq!(read(root, "other.pdf"), "other, edited");
        assert_eq!(
            sync.provider.content("/report.pdf").as_deref(),
            Some("v1, edited")
        );
        assert_eq!(
            sync.provider.content("/other.pdf").as_deref(),
            Some("other, edited")
        );
        assert_eq!(sync.provider.downloads(), ["/report.pdf"]);
    }

    /// The remote file deleted during the clash: nothing is moved aside while it lasts. Once
    /// it is settled, the file that stays is compared with the one entry, which recorded the
    /// version that was deleted: the tracked file, unchanged since, is moved aside as deleted
    /// remotely; the newcomer, changed since, is uploaded again, with a notice. Before, the clash
    /// sync had uploaded the newcomer, and the tracked file, compared with its own older entry,
    /// was moved aside though its content wasn't what was deleted (review of #40, verification
    /// round 3).
    #[tokio::test]
    async fn a_remote_deletion_during_a_clash_waits_until_it_is_settled() {
        for (removed, stays) in [("report.pdf", "Report.pdf"), ("Report.pdf", "report.pdf")] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let sync = clashing_newcomer(root).await;
            sync.provider.put("/keep.txt", "k");
            sync.provider.remove("/report.pdf");
            let mut sync = again(sync);
            let r = clash_sync(&mut sync).await;
            assert_eq!(counts(&r), (0, 1, 0), "{stays}");
            assert!(quarantined(root).is_empty(), "{stays}");
            assert_eq!(recorded(&sync), ["/Report.pdf", "/keep.txt"], "{stays}");

            std::fs::remove_file(root.join(removed)).unwrap();
            let mut sync = again(sync);
            let r = clean(&mut sync).await;
            if stays == "Report.pdf" {
                assert_eq!(counts(&r), (0, 0, 1));
                assert!(r.notices[0].ends_with(": /Report.pdf"), "{:?}", r.notices);
                assert_eq!(quarantined(root), ["Report.pdf"]);
                assert_eq!(sync.provider.content("/report.pdf"), None);
            } else {
                assert_eq!(counts(&r), (1, 0, 0));
                assert!(
                    has_notice(&r, "/report.pdf was deleted remotely but changed here"),
                    "{:?}",
                    r.notices
                );
                assert!(quarantined(root).is_empty());
                assert_eq!(
                    sync.provider.content("/report.pdf").as_deref(),
                    Some("other")
                );
            }
            sync = again(sync);
            assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0), "{stays}");
        }
    }

    /// A clash settled by removing the newcomer, then a sync whose listing lacks the file: one
    /// that lists nothing (the folder trashed or unshared, then back), or an upload-only one
    /// while the file is deleted there (then restored from the provider's deleted files).
    /// Neither loses the one entry, so the next full sync finds `Report.pdf` unchanged, as is
    /// the remote file. Before, the clash sync had uploaded the newcomer, and such a sync
    /// dropped the newcomer's entry, the only one that showed `Report.pdf` changed since, so the
    /// newcomer's content came down over it (review of #40, verification round 3).
    #[tokio::test]
    async fn a_settled_clash_keeps_its_state_through_a_sync_without_the_file() {
        for upload_only in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let sync = clashing_newcomer(root).await;
            std::fs::remove_file(root.join("report.pdf")).unwrap();
            if upload_only {
                sync.provider.put("/unrelated.txt", "u");
            }
            let listed = std::mem::take(&mut *sync.provider.files.lock().unwrap());
            if upload_only {
                let mut files = sync.provider.files.lock().unwrap();
                files.insert("/unrelated.txt".into(), listed["/unrelated.txt"].clone());
            }
            let mut sync = again_with(sync, |c| {
                if upload_only {
                    c.direction = SyncDirection::Upload;
                }
            });
            let r = sync.sync().await.unwrap();
            assert_eq!(counts(&r), (0, 0, 0), "upload only {upload_only}");
            *sync.provider.files.lock().unwrap() = listed;

            let mut sync = again_with(sync, |c| c.direction = SyncDirection::Bidirectional);
            let r = clean(&mut sync).await;
            assert_eq!(counts(&r), (0, usize::from(upload_only), 0));
            assert!(r.conflicts.is_empty() && r.notices.is_empty());
            sync = again(sync);
            assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
            assert_eq!(read(root, "Report.pdf"), "v1", "upload only {upload_only}");
            assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
            assert!(sync.provider.uploads().is_empty());
        }
    }

    /// Nothing is written to the remote file during the clash, so no state that is lost can
    /// turn against the tracked file: not an upload stored whose reply is lost, nor the
    /// state of the clash sync lost to a crash (or a failed write) after it. Before, the clash
    /// sync uploaded the newcomer; with its record lost, both entries described "v1", and once
    /// the newcomer was removed its content came down over `Report.pdf` (review of #40,
    /// verification round 3).
    #[tokio::test]
    async fn a_clash_sends_nothing_that_lost_state_could_turn_against_the_tracked_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut sync = tracked_then_renamed(root).await;
        let saved = std::fs::read(root.join(MANIFEST_FILE)).unwrap();
        write(root, "report.pdf", "other");
        sync.provider
            .lose_reply
            .lock()
            .unwrap()
            .insert("/report.pdf".into());
        assert_eq!(counts(&clash_sync(&mut sync).await), (0, 0, 0));
        assert!(sync.provider.uploads().is_empty());
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
        // As a crash before the state is written would leave it.
        std::fs::write(root.join(MANIFEST_FILE), saved).unwrap();

        std::fs::remove_file(root.join("report.pdf")).unwrap();
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Report.pdf"), "v1");
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
    }

    /// The file spelled as listed goes on syncing through a clash while it is the only one of
    /// the set ever synced: a newcomer spelled otherwise is left alone, but the tracked file's
    /// edits go up and the remote file's come down. Once the newcomer goes, nothing more
    /// happens; once the tracked file goes instead, the newcomer is compared with its entry by
    /// content, and uploaded.
    #[tokio::test]
    async fn a_clash_syncs_the_file_spelled_as_listed_while_only_it_was_synced() {
        for removed in ["Report.pdf", "report.pdf"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let store = Store {
                hashed: true,
                ignores_case: true,
                ..Default::default()
            };
            store.put("/report.pdf", "v1");
            let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
            assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));

            write(root, "Report.pdf", "newcomer");
            sync.provider.put("/report.pdf", "v2");
            sync = again(sync);
            assert_eq!(counts(&clash_sync(&mut sync).await), (0, 1, 0));
            write(root, "report.pdf", "v3, edited here");
            sync = again(sync);
            assert_eq!(counts(&clash_sync(&mut sync).await), (1, 0, 0));
            assert_eq!(
                sync.provider.content("/report.pdf").as_deref(),
                Some("v3, edited here")
            );
            assert_eq!(read(root, "Report.pdf"), "newcomer");
            assert_eq!(recorded(&sync), ["/report.pdf"]);

            std::fs::remove_file(root.join(removed)).unwrap();
            sync = again(sync);
            let uploaded = usize::from(removed == "report.pdf");
            assert_eq!(
                counts(&clean(&mut sync).await),
                (uploaded, 0, 0),
                "{removed}"
            );
            sync = again(sync);
            assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0), "{removed}");
            let (stays, content) = match removed {
                "report.pdf" => ("Report.pdf", "newcomer"),
                _ => ("report.pdf", "v3, edited here"),
            };
            assert_eq!(read(root, stays), content);
            assert_eq!(
                sync.provider.content("/report.pdf").as_deref(),
                Some(content)
            );
        }
    }

    /// Adds an entry for `/report.pdf` to the state [`tracked_then_renamed`] saved, as an
    /// earlier build's clash sync left it once `report.pdf` was synced there: a conflict left
    /// pending saved it with the remote side of `Report.pdf`'s entry, and no local side.
    fn record_another_spelling(root: &Path) {
        let path = root.join(MANIFEST_FILE);
        let mut stored: ManifestFile =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let files = &mut stored.syncs[0].files;
        let remote = files["/Report.pdf"].remote.clone();
        files.insert(
            "/report.pdf".into(),
            ManifestEntry {
                remote,
                local: None,
            },
        );
        std::fs::write(path, serde_json::to_vec(&stored).unwrap()).unwrap();
    }

    /// State an earlier build left: an entry for each of two spellings of one file, which
    /// disagree on the version synced last ([`record_another_spelling`]). Which is the newest
    /// can't be told, so the file is taken for changed on both sides, with a notice: a conflict
    /// until the strategy settles it, rather than `Report.pdf` taken for unchanged since "v1"
    /// and the remote file downloaded over it (review of #40, verification round 3). The entry
    /// with neither side that stands for them goes once the file is gone from both sides.
    #[tokio::test]
    async fn entries_left_under_several_spellings_make_a_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let sync = tracked_then_renamed(root).await;
        record_another_spelling(root);
        sync.provider.put("/report.pdf", "other2");

        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::AskUser);
        let r = clean(&mut sync).await;
        assert_eq!((counts(&r), r.conflicts.len()), ((0, 0, 0), 1));
        assert_eq!(r.conflicts[0].local_path, root.join("Report.pdf"));
        assert!(
            has_notice(
                &r,
                "1 file recorded under several spellings that differ only in letter case"
            ),
            "{:?}",
            r.notices
        );
        assert!(r.notices[0].ends_with(": /Report.pdf"), "{:?}", r.notices);
        assert_eq!(recorded(&sync), ["/Report.pdf"]);

        // Still a conflict until it is settled, without the notice.
        let mut sync = again(sync);
        let r = clean(&mut sync).await;
        assert_eq!((counts(&r), r.conflicts.len()), ((0, 0, 0), 1));
        assert!(r.notices.is_empty(), "{:?}", r.notices);
        assert_eq!(read(root, "Report.pdf"), "v1");
        assert_eq!(sync.provider.downloads(), ["/report.pdf"]);

        // Settled by the strategy: the local file wins.
        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::LocalWins);
        assert_eq!(counts(&clean(&mut sync).await), (1, 0, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));

        // Left pending again, then gone from both sides: the entry goes.
        record_another_spelling(root);
        sync.provider.put("/report.pdf", "other3");
        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::AskUser);
        assert_eq!(clean(&mut sync).await.conflicts.len(), 1);
        assert_eq!(recorded(&sync), ["/Report.pdf"]);
        std::fs::remove_file(root.join("Report.pdf")).unwrap();
        sync.provider.remove("/report.pdf");
        sync.provider.put("/keep.txt", "k");
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));
        assert_eq!(recorded(&sync), ["/keep.txt"]);
        assert!(quarantined(root).is_empty());
    }

    /// Three local spellings of one path: `Report.pdf` (tracked), then `report.pdf` (spelled as
    /// listed) and `REPORT.pdf`. None is synced until only one is left, which keeps its
    /// content: `Report.pdf` is unchanged, and `REPORT.pdf` is uploaded.
    #[tokio::test]
    async fn three_spellings_of_one_path_keep_the_one_that_stays() {
        for (removed, stays, content) in [
            ("REPORT.pdf", "Report.pdf", "v1"),
            ("Report.pdf", "REPORT.pdf", "three"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let mut sync = tracked_then_renamed(root).await;
            write(root, "report.pdf", "two");
            write(root, "REPORT.pdf", "three");
            for _ in 0..2 {
                let r = sync.sync().await.unwrap();
                assert_eq!(counts(&r), (0, 0, 0));
                assert!(
                    r.errors[0].contains("/REPORT.pdf and /Report.pdf and /report.pdf."),
                    "{:?}",
                    r.errors
                );
                assert_eq!(sync.provider.content("/report.pdf").as_deref(), Some("v1"));
                std::fs::remove_file(root.join("report.pdf")).ok();
                sync = again(sync);
            }

            std::fs::remove_file(root.join(removed)).unwrap();
            let r = clean(&mut sync).await;
            let uploaded = usize::from(stays == "REPORT.pdf");
            assert_eq!(counts(&r), (uploaded, 0, 0), "{stays}");
            assert!(r.notices.is_empty(), "{:?}", r.notices);
            sync = again(sync);
            assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0), "{stays}");
            assert_eq!(read(root, stays), content);
            assert_eq!(
                sync.provider.content("/report.pdf").as_deref(),
                Some(content)
            );
            assert_eq!(sync.provider.downloads(), ["/report.pdf"]);
        }
    }

    /// An entry taken over from another spelling tells the file unchanged by its content only,
    /// not by its modification time: `report.pdf` deleted here and another `Report.pdf` of the
    /// same size put in its place with the same time (as a copy that keeps times would) is a
    /// change here. With the remote file changed too, it is a conflict, not overwritten.
    #[tokio::test]
    async fn an_entry_taken_from_another_spelling_goes_by_content_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store {
            hashed: true,
            ignores_case: true,
            ..Default::default()
        };
        store.put("/report.pdf", "v1");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));
        let synced_at = std::fs::metadata(root.join("report.pdf"))
            .unwrap()
            .modified()
            .unwrap();
        std::fs::remove_file(root.join("report.pdf")).unwrap();
        write(root, "Report.pdf", "v2");
        std::fs::File::options()
            .write(true)
            .open(root.join("Report.pdf"))
            .unwrap()
            .set_modified(synced_at)
            .unwrap();
        sync.provider.put("/report.pdf", "edited there");

        let mut sync = again_with(sync, |c| c.conflict_strategy = ConflictStrategy::AskUser);
        let r = clean(&mut sync).await;
        assert_eq!((counts(&r), r.conflicts.len()), ((0, 0, 0), 1));
        assert_eq!(read(root, "Report.pdf"), "v2");
        assert_eq!(sync.provider.downloads(), ["/report.pdf"]);
    }

    /// Where the provider ignores case, a remote file new since a directory above it was renamed
    /// here only in case comes down into that directory, not into a new one spelled as the
    /// remote folder beside it; then it syncs like the others there (review of #40,
    /// verification round 2).
    #[tokio::test]
    async fn a_new_remote_file_comes_down_into_the_directory_spelled_otherwise_here() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = Store {
            hashed: true,
            ignores_case: true,
            ..Default::default()
        };
        store.put("/sub/a.txt", "a");
        let mut sync = CloudSync::new(store, kept(root, SyncDirection::Bidirectional));
        assert_eq!(counts(&clean(&mut sync).await), (0, 1, 0));
        std::fs::rename(root.join("sub"), root.join("Sub")).unwrap();
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));

        sync.provider.put("/sub/b.txt", "b");
        sync.provider.put("/sub/new/c.txt", "c");
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 2, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Sub/b.txt"), "b");
        assert_eq!(read(root, "Sub/new/c.txt"), "c");

        sync.provider.put("/sub/b.txt", "b, edited there");
        write(root, "Sub/new/c.txt", "c, edited here");
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (1, 1, 0));
        sync = again(sync);
        assert_eq!(counts(&clean(&mut sync).await), (0, 0, 0));
        assert_eq!(read(root, "Sub/b.txt"), "b, edited there");
        assert_eq!(
            sync.provider.content("/sub/new/c.txt").as_deref(),
            Some("c, edited here")
        );
        let mut top: Vec<String> = std::fs::read_dir(root)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| !n.starts_with('.'))
            .collect();
        top.sort();
        assert_eq!(top, ["Sub"]);
        assert!(quarantined(root).is_empty());
    }

    /// A listed file that no local file has comes down into the deepest directory above it that
    /// the local tree has, spelled as the local tree spells it, unless the local tree has that
    /// directory under several spellings.
    #[test]
    fn listed_files_go_into_local_directories_spelled_otherwise() {
        let local = [
            "/Sub/a.txt",
            "/Sub/Deeper/x.txt",
            "/Two/a.txt",
            "/two/b.txt",
            "/Two/Three/c.txt",
            "/top.txt",
        ]
        .map(String::from);
        let dirs = local_dirs(local.iter());
        for (listed, lands) in [
            ("/sub/b.txt", "/Sub/b.txt"),
            ("/SUB/deeper/c.txt", "/Sub/Deeper/c.txt"),
            ("/sub/new/c.txt", "/Sub/new/c.txt"),
            ("/two/c.txt", "/two/c.txt"),
            ("/TWO/c.txt", "/TWO/c.txt"),
            ("/two/three/d.txt", "/Two/Three/d.txt"),
            ("/TOP.txt/x.txt", "/TOP.txt/x.txt"),
            ("/other/x.txt", "/other/x.txt"),
            ("/x.txt", "/x.txt"),
        ] {
            assert_eq!(in_local_dirs(listed, &dirs), lands, "{listed}");
        }
    }

    /// Only paths a provider listing can return are tracked (see [`listable`]); a file uploaded
    /// where no listing shows it would look deleted remotely at the next sync. A drive prefix
    /// is only an unsafe path at the start of one, but listings check each name on its own.
    #[test]
    fn only_paths_a_listing_can_return_are_tracked() {
        let sync = CloudSync::new(
            MockProvider::default(),
            kept(Path::new("/nonexistent"), SyncDirection::Bidirectional),
        );
        assert!(cloud_path_components("/sub/Q:A.pdf").is_ok());
        for path in ["/a.pdf", "/sub/a.pdf", "/sub/10:30.pdf", "/sub/QA:.pdf"] {
            assert!(sync.visible(path), "{path}");
        }
        let deepest = format!("{}/x.pdf", "/d".repeat(MAX_LIST_DEPTH - 1));
        assert!(sync.visible(&deepest));
        let too_deep = format!("{}/x.pdf", "/d".repeat(MAX_LIST_DEPTH));
        for path in ["/Q:A.pdf", "/sub/Q:A.pdf", "/Q:/a.pdf", too_deep.as_str()] {
            assert!(!sync.visible(path), "{path}");
        }
    }

    #[test]
    fn test_glob_match() {
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*.txt", "test.txt"
            )
        );
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*.txt", "file.txt"
            )
        );
        assert!(
            !CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*.txt", "test.pdf"
            )
        );
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "test?", "test1"
            )
        );
        assert!(
            !CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "test?", "test12"
            )
        );
        assert!(
            CloudSync::<crate::integrations::google_drive::GoogleDrive>::glob_match(
                "*", "anything"
            )
        );
    }
}
