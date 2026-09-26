//! Search API endpoints for remarkable-server
//!
//! Endpoints:
//! - GET /search/v1/query?q=...&limit=...&offset=...&doc_type=...
//! - GET /search/v1/stats
//! - POST /search/v1/reindex

use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::search::{IndexStats, SearchIndex, SearchQuery, SearchResult};
use crate::storage::Storage;

/// Search API state
#[derive(Clone)]
pub struct SearchState {
    pub index: SearchIndex,
    pub storage: Storage,
}

impl SearchState {
    pub fn new(index: SearchIndex, storage: Storage) -> Self {
        Self { index, storage }
    }
}

/// Search response with results and metadata
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub results: Vec<SearchResult>,
    pub count: usize,
    pub limit: usize,
    pub offset: usize,
}

/// Reindex response
#[derive(Debug, Serialize)]
pub struct ReindexResponse {
    pub indexed: usize,
    pub message: String,
}

/// GET /search/v1/query
///
/// Full-text search across all indexed documents.
///
/// Query parameters:
/// - q: Search query (required)
/// - limit: Max results (default: 20)
/// - offset: Result offset for pagination (default: 0)
/// - doc_type: Filter by type (document, folder, pdf, epub)
///
/// Returns highlighted snippets and relevance ranking.
pub async fn search(
    State(state): State<SearchState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResponse>> {
    let results = state.index.search(&query)?;
    let count = results.len();

    Ok(Json(SearchResponse {
        query: query.q.clone(),
        results,
        count,
        limit: query.limit,
        offset: query.offset,
    }))
}

/// GET /search/v1/stats
///
/// Returns search index statistics.
pub async fn stats(State(state): State<SearchState>) -> Result<Json<IndexStats>> {
    let stats = state.index.stats()?;
    Ok(Json(stats))
}

/// POST /search/v1/reindex
///
/// Rebuild the entire search index from storage.
/// This may take a while for large libraries.
pub async fn reindex(State(state): State<SearchState>) -> Result<Json<ReindexResponse>> {
    let indexed = state.index.rebuild_from_storage(&state.storage)?;

    Ok(Json(ReindexResponse {
        indexed,
        message: format!("Reindexed {} documents", indexed),
    }))
}

/// Query parameters for simple search
#[derive(Debug, Deserialize)]
pub struct SimpleSearchQuery {
    pub q: String,
}

/// GET /search/v1/suggest
///
/// Quick filename suggestions for autocomplete.
pub async fn suggest(
    State(state): State<SearchState>,
    Query(query): Query<SimpleSearchQuery>,
) -> Result<Json<Vec<String>>> {
    let results = state.index.search(&SearchQuery {
        q: query.q,
        limit: 5,
        offset: 0,
        doc_type: None,
    })?;

    let suggestions: Vec<String> = results.into_iter().map(|r| r.filename).collect();

    Ok(Json(suggestions))
}
