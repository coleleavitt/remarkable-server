//! In-document handwriting search: `GET /handwriting/v1/search?searchTerm=&documentId=`.
//!
//! Contract recovered from xochitl 3.3.2 (`DocumentSearchTask::fetchDocumentSearchResults`,
//! request sub_46E02C, response sub_46F6A8/sub_46EE70, page hits sub_4672EC):
//! GET (method enum 0) with auth; HTTP 200 is required, and the body is
//! `{"results":[{"document-ID","noOfHits",<int > 0>,"pages":[{"id","hits":[{"bbox":{"x","y","w","h"}}]}]}]}`.
//! The tablet highlights each bbox on page `id` (the `.rm` page uuid).
//!
//! Pages are recognised with the local engine (see `handwriting.rs`) and cached by the
//! `.rm` blob hash, so an edited page is re-read automatically. `spawn_indexer` fills the
//! cache in the background after each sync; a search only recognises pages it still lacks.

use axum::{extract::{Query, State}, http::HeaderMap, Json};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{api::AppState, error::{Result, ServerError}, handwriting::{recognize, Points, Word}, storage::Storage};

/// How often the indexer checks whether the sync root moved.
const INDEX_POLL: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Deserialize)]
pub struct SearchQuery {
    #[serde(rename = "searchTerm")]
    search_term: String,
    #[serde(rename = "documentId")]
    document_id: String,
}

/// Recognised words per `.rm` blob hash.
pub struct PageIndex { db: Mutex<Connection> }

impl PageIndex {
    pub fn open(storage: &Storage) -> Result<Self> {
        let conn = Connection::open(storage.base_path().join("hwr_index.db"))?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS pages (rm_hash TEXT PRIMARY KEY, words TEXT NOT NULL)")?;
        Ok(Self { db: Mutex::new(conn) })
    }

    fn get(&self, rm_hash: &str) -> Result<Option<Vec<Word>>> {
        let json: Option<String> = self.db.lock()
            .query_row("SELECT words FROM pages WHERE rm_hash = ?", params![rm_hash], |r| r.get(0)).optional()?;
        Ok(json.map(|j| serde_json::from_str(&j)).transpose()?)
    }

    fn put(&self, rm_hash: &str, words: &[Word]) -> Result<()> {
        self.db.lock().execute("INSERT OR REPLACE INTO pages (rm_hash, words) VALUES (?, ?)", params![rm_hash, serde_json::to_string(words)?])?;
        Ok(())
    }
}

/// Ink strokes from a `.rm` page, without highlighter/eraser marks.
fn page_strokes(data: &[u8]) -> Result<Vec<Points>> {
    use remarkable_core::PenType;
    let strokes = remarkable_lines::parse_rm_file(data).map_err(|e| ServerError::Internal(format!("bad .rm page: {e}")))?;
    Ok(strokes.into_iter()
        .filter(|s| !s.pen.is_highlighter() && !matches!(s.pen, PenType::Eraser | PenType::EraserArea))
        .map(|s| s.points.iter().map(|p| (p.x, p.y)).collect::<Points>())
        .filter(|p| !p.is_empty())
        .collect())
}

/// Every document id in the current sync tree.
fn document_ids(storage: &Storage) -> Result<Vec<String>> {
    let root = storage.get_root();
    if root.hash.is_empty() { return Ok(Vec::new()); }
    Ok(String::from_utf8_lossy(&storage.get(&root.hash)?).lines().skip(1)
        .filter_map(|l| l.split(':').nth(2).map(str::to_owned)).collect())
}

/// Recognised words for a page, from cache or by running the engine (then cached).
async fn page_words(storage: &Storage, index: &PageIndex, rm_hash: &str) -> Result<Vec<Word>> {
    if let Some(words) = index.get(rm_hash)? { return Ok(words); }
    let strokes = page_strokes(&storage.get(rm_hash)?)?;
    let words = if strokes.is_empty() { Vec::new() } else { recognize(&strokes).await? };
    index.put(rm_hash, &words)?;
    Ok(words)
}

/// Background indexer: whenever the sync root changes, recognise every page not yet
/// cached, so searches answer from cache instead of running OCR on request.
pub fn spawn_indexer(storage: Storage) {
    tokio::spawn(async move {
        let mut indexed_root = String::new();
        let mut tick = tokio::time::interval(INDEX_POLL);
        loop {
            tick.tick().await;
            let root = storage.get_root().hash;
            if root.is_empty() || root == indexed_root { continue; }
            let started = std::time::Instant::now();
            let (mut new, mut failed) = (0, 0);
            let result: Result<()> = async {
                let index = PageIndex::open(&storage)?;
                for doc in document_ids(&storage)? {
                    for (_, rm_hash) in document_pages(&storage, &doc)? {
                        if index.get(&rm_hash)?.is_some() { continue; }
                        match page_words(&storage, &index, &rm_hash).await {
                            Ok(_) => new += 1,
                            Err(e) => { failed += 1; tracing::debug!(%rm_hash, "handwriting index: {e}"); }
                        }
                    }
                }
                Ok(())
            }.await;
            match result {
                Ok(()) => {
                    if new + failed > 0 {
                        tracing::info!(new, failed, secs = started.elapsed().as_secs(), "handwriting index updated");
                    }
                    indexed_root = root;
                }
                Err(e) => tracing::warn!("handwriting indexing failed: {e}"),
            }
        }
    });
}

/// `(page uuid, .rm blob hash)` for each page of `doc_id` in the current sync tree.
fn document_pages(storage: &Storage, doc_id: &str) -> Result<Vec<(String, String)>> {
    let root = storage.get_root();
    if root.hash.is_empty() { return Ok(Vec::new()); }
    let root_index = String::from_utf8_lossy(&storage.get(&root.hash)?).into_owned();
    let Some(doc_hash) = root_index.lines().skip(1)
        .find(|l| l.split(':').nth(2) == Some(doc_id))
        .and_then(|l| l.split(':').next()) else { return Ok(Vec::new()) };
    let doc_index = String::from_utf8_lossy(&storage.get(doc_hash)?).into_owned();
    let prefix = format!("{doc_id}/");
    Ok(doc_index.lines().skip(1).filter_map(|l| {
        let mut f = l.split(':');
        let hash = f.next()?;
        let name = f.nth(1)?;
        let page = name.strip_prefix(&prefix)?.strip_suffix(".rm")?;
        Some((page.to_owned(), hash.to_owned()))
    }).collect())
}

fn normalize(s: &str) -> String {
    s.chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect::<String>().to_lowercase()
}

/// Boxes of every match of `term` on a page. Multi-word terms match runs of
/// consecutive words on the same line; the box spans the run.
fn find_hits(words: &[Word], term: &str) -> Vec<Value> {
    let tokens: Vec<String> = normalize(term).split_whitespace().map(str::to_owned).collect();
    if tokens.is_empty() { return Vec::new(); }
    let norm: Vec<String> = words.iter().map(|w| normalize(&w.text)).collect();
    (0..words.len().saturating_sub(tokens.len() - 1)).filter_map(|i| {
        let run = &words[i..i + tokens.len()];
        let same_line = run.iter().all(|w| w.line == run[0].line);
        let matched = tokens.iter().zip(&norm[i..]).enumerate().all(|(k, (t, w))| {
            if tokens.len() == 1 { w.contains(t.as_str()) }
            else if k == 0 { w.ends_with(t.as_str()) }
            else if k == tokens.len() - 1 { w.starts_with(t.as_str()) }
            else { w == t }
        });
        (same_line && matched).then(|| {
            let (x0, y0) = run.iter().fold((f32::MAX, f32::MAX), |(x, y), w| (x.min(w.x), y.min(w.y)));
            let (x1, y1) = run.iter().fold((f32::MIN, f32::MIN), |(x, y), w| (x.max(w.x + w.w), y.max(w.y + w.h)));
            json!({ "bbox": { "x": x0, "y": y0, "w": x1 - x0, "h": y1 - y0 } })
        })
    }).collect()
}

pub async fn search(State(state): State<AppState>, headers: HeaderMap, Query(q): Query<SearchQuery>) -> Result<Json<Value>> {
    state.auth_user(&headers)?;
    let term = q.search_term.trim();
    if term.is_empty() {
        return Err(ServerError::Config("empty search term".into()));
    }
    let index = PageIndex::open(&state.storage)?;
    let mut pages = Vec::new();
    let mut total = 0;
    for (page_id, rm_hash) in document_pages(&state.storage, &q.document_id)? {
        let words = page_words(&state.storage, &index, &rm_hash).await?;
        let hits = find_hits(&words, term);
        if !hits.is_empty() {
            total += hits.len();
            pages.push(json!({ "id": page_id, "hits": hits }));
        }
    }
    tracing::info!(document = %q.document_id, term, hits = total, pages = pages.len(), "handwriting search");
    // The tablet rejects results with noOfHits <= 0, so a miss is an empty list.
    let results = if total > 0 {
        vec![json!({ "document-ID": q.document_id, "noOfHits": total, "pages": pages })]
    } else { Vec::new() };
    Ok(Json(json!({ "results": results })))
}


#[cfg(test)]
mod tests {
    use super::*;

    fn w(text: &str, x: f32, line: u32) -> Word {
        Word { text: text.into(), x, y: 10.0, w: 20.0, h: 8.0, line: (1, 1, line) }
    }

    #[test]
    fn single_word_substring_and_case() {
        let words = [w("her", 0.0, 1), w("Name?", 30.0, 1)];
        let hits = find_hits(&words, "name");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["bbox"]["x"], 30.0);
    }

    #[test]
    fn multi_word_same_line_spans_run() {
        let words = [w("her", 0.0, 1), w("Name?", 30.0, 1)];
        let hits = find_hits(&words, "her name");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["bbox"]["x"], 0.0);
        assert_eq!(hits[0]["bbox"]["w"], 50.0); // 0 .. 30+20
    }

    #[test]
    fn multi_word_does_not_cross_lines() {
        let words = [w("her", 0.0, 1), w("name", 0.0, 2)];
        assert!(find_hits(&words, "her name").is_empty());
    }

    #[test]
    fn empty_or_missing_terms() {
        let words = [w("hello", 0.0, 1)];
        assert!(find_hits(&words, "   ").is_empty());
        assert!(find_hits(&words, "world").is_empty());
        assert!(find_hits(&[], "hello").is_empty());
    }
}
