# remarkable-server

A local sync server for reMarkable tablets, implementing the sync v3 API.

## Features

- Full sync v3 API compatibility
- Hash-based content-addressable storage
- CRC32C checksum validation
- rm-filename header enforcement
- Token refresh mocking
- **FTS5 full-text search** (document names, folders, PDF content)

## Usage

```bash
# Build
cargo build --release

# Run (default: localhost:8080)
./target/release/remarkable-server

# Custom options
./target/release/remarkable-server --bind 0.0.0.0:8080 --storage ./data
```

## API Endpoints

### Sync API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/v3/root` | GET | Current root hash |
| `/sync/v3/files/{hash}` | GET | Download file |
| `/sync/v3/files/{hash}` | PUT | Upload file |
| `/token/json/2/user/new` | POST | Token refresh |
| `/discovery/v1/endpoints` | GET | Service discovery |
| `/health` | GET | Health check |

### Search API

Full-text search across all synced documents using SQLite FTS5.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search/v1/query` | GET | Full-text search |
| `/search/v1/stats` | GET | Index statistics |
| `/search/v1/reindex` | POST | Rebuild search index |
| `/search/v1/suggest` | GET | Filename suggestions (autocomplete) |

#### Search Query Parameters

```
GET /search/v1/query?q=meeting+notes&limit=20&offset=0&doc_type=pdf
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `q` | string | required | Search query (supports FTS5 syntax) |
| `limit` | int | 20 | Maximum results |
| `offset` | int | 0 | Pagination offset |
| `doc_type` | string | - | Filter by type: `document`, `folder`, `pdf`, `epub` |

#### Search Response

```json
{
  "query": "meeting notes",
  "results": [
    {
      "hash": "abc123...",
      "filename": "Q4 Meeting Notes.content",
      "doc_type": "document",
      "snippet": "...discussion about <mark>meeting</mark> <mark>notes</mark>...",
      "rank": -2.5
    }
  ],
  "count": 1,
  "limit": 20,
  "offset": 0
}
```

#### FTS5 Query Syntax

- `meeting notes` - matches both words (prefix matching enabled)
- `"meeting notes"` - exact phrase
- `meet*` - prefix match
- `meeting OR discussion` - boolean OR
- `meeting AND notes` - boolean AND (default)
- `meeting NOT confidential` - exclude term

#### Index Statistics

```
GET /search/v1/stats
```

```json
{
  "total_documents": 150,
  "documents_with_content": 45,
  "pdf_count": 45
}
```

### PDF Text Extraction

PDFs are automatically indexed using `pdftotext` if available. Install poppler-utils:

```bash
# Ubuntu/Debian
sudo apt install poppler-utils

# macOS
brew install poppler

# Arch
sudo pacman -S poppler
```

### Incremental Indexing

Documents are automatically indexed when synced. The search index is stored in `search.db` in the storage directory.

To manually rebuild the index:

```bash
curl -X POST http://localhost:8080/search/v1/reindex
```

## Device Configuration

To point your device at this server:

1. Generate SSL certificates (see tools/local-sync/ssl_certs.py)
2. Install CA cert on device
3. Redirect traffic via /etc/hosts or mitmproxy
4. Start server with generated certificates

See [remarkable-research](https://github.com/coleleavitt/remarkable-research) for device configuration tools.

## License

MIT
