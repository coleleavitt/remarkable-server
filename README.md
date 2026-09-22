# remarkable-server

A local sync server for reMarkable tablets, implementing **all sync protocol versions** (V1, V1.5, V2, V3, V4).

## Features

- **Multi-version sync protocol support** — serves any device regardless of firmware version
- Full sync v3 API compatibility (current production)
- Legacy V1/V1.5/V2 support for older firmware
- Future V4 protocol with extended metadata
- Hash-based content-addressable storage
- CRC32C checksum validation
- rm-filename header enforcement
- Token refresh and device pairing
- **FTS5 full-text search** (document names, folders, PDF content)
- **Calendar sync** (ICS/CalDAV/Google Calendar)
- **Cloud integrations** (Google Drive, Dropbox, OneDrive)
- **Read-it-later** (Pocket, Instapaper, Wallabag)
- **RSS/Newsletter ingestion**
- **Document versions** with diff/restore

## Usage

```bash
# Build
cargo build --release

# Run (default: localhost:8080)
./target/release/remarkable-server

# Custom options
./target/release/remarkable-server --bind 0.0.0.0:8080 --storage ./data
```

## Protocol Versions

The server automatically serves devices based on their firmware version:

| Version | Firmware | API Paths | Description |
|---------|----------|-----------|-------------|
| **V1** | 1.x-2.x | `/document-storage/json/2/*` | Original JSON API |
| **V1.5** | transitional | `/sync/v1.5/*` | Batch operations |
| **V2** | transitional | `/sync/v2/*` | Binary protocol intro |
| **V3** | 3.x (current) | `/sync/v3/*` | Hash-based CRDT |
| **V4** | future | `/sync/v4/*` | Extended metadata + capabilities |

### V1 Endpoints (Legacy)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/document-storage/json/2/docs` | GET | List all documents |
| `/document-storage/json/2/upload/request` | PUT | Request upload URL |
| `/document-storage/json/2/upload/update-status` | PUT | Update status |
| `/document-storage/json/2/delete` | DELETE | Delete documents |

### V1.5 Endpoints (Batch)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/v1.5/batch` | POST | Batch sync operation |

### V2 Endpoints (Binary)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/v2/root` | GET | Current root hash |
| `/sync/v2/files/{hash}` | GET | Download file |
| `/sync/v2/files/{hash}` | PUT | Upload file |

### V3 Endpoints (Current Production)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/v3/root` | GET | Current root hash |
| `/sync/v3/files/{hash}` | GET | Download file |
| `/sync/v3/files/{hash}` | PUT | Upload file |

### V4 Endpoints (Future)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sync/v4/root` | GET | Root with extended metadata + capabilities |
| `/sync/v4/files/{hash}` | GET | File with metadata headers |
| `/sync/v4/files/{hash}` | PUT | Upload with extended validation |

V4 root response includes capability flags:
```json
{
  "hash": "abc123...",
  "generation": 42,
  "schema_version": 4,
  "features": ["crdt", "sharing", "tags", "search", "calendar"],
  "capabilities": {
    "crdt": true,
    "sharing": true,
    "tags": true,
    "search": true,
    "calendar": true
  }
}
```

## Device Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/devices/v1` | POST | Create pairing code |
| `/devices/v1` | GET | List registered devices |
| `/devices/v1/{id}` | DELETE | Remove device |
| `/token/json/2/user/new` | POST | Refresh user token |
| `/token/json/2/device/new` | POST | Register device |
| `/token/json/3/device/delete` | POST | Delete device token |
| `/discovery/v1/endpoints` | GET | Service discovery |

## Search API

Full-text search across all synced documents using SQLite FTS5.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search/v1/query` | GET | Full-text search |
| `/search/v1/stats` | GET | Index statistics |
| `/search/v1/reindex` | POST | Rebuild search index |
| `/search/v1/suggest` | GET | Filename suggestions (autocomplete) |

### Search Query Parameters

```
GET /search/v1/query?q=meeting+notes&limit=20&offset=0&doc_type=pdf
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `q` | string | required | Search query (supports FTS5 syntax) |
| `limit` | int | 20 | Maximum results |
| `offset` | int | 0 | Pagination offset |
| `doc_type` | string | - | Filter by type: `document`, `folder`, `pdf`, `epub` |

### FTS5 Query Syntax

- `meeting notes` — matches both words (prefix matching enabled)
- `"meeting notes"` — exact phrase
- `meet*` — prefix match
- `meeting OR discussion` — boolean OR
- `meeting AND notes` — boolean AND (default)
- `meeting NOT confidential` — exclude term

## Calendar API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/calendars/v1/` | GET | List calendars |
| `/calendars/v1/` | POST | Add calendar |
| `/calendars/v1/{id}` | GET | Get calendar |
| `/calendars/v1/{id}` | DELETE | Delete calendar |
| `/calendars/v1/{id}/events` | GET | Get events |
| `/calendars/v1/{id}/sync` | POST | Sync calendar |
| `/calendars/v1/upcoming` | GET | Upcoming events |
| `/calendars/v1/sync-all` | POST | Sync all calendars |

## Integrations API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/integrations/v1/providers` | GET | List available providers |
| `/integrations/v1/accounts` | GET | List connected accounts |
| `/integrations/v1/accounts` | POST | Connect account (OAuth) |
| `/integrations/v1/accounts/{id}` | DELETE | Disconnect account |
| `/integrations/v1/accounts/{id}/sync` | POST | Trigger sync |

Supported providers: Google Drive, Dropbox, OneDrive

## Read-it-Later API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/readlater/v1/articles` | GET | List articles |
| `/readlater/v1/articles` | POST | Add article |
| `/readlater/v1/accounts` | GET | List connected services |

Supported services: Pocket, Instapaper, Wallabag, Omnivore

## Versions API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/versions/v1/{doc_id}` | GET | List document versions |
| `/versions/v1/{doc_id}/{version}` | GET | Get specific version |
| `/versions/v1/{doc_id}/{version}/content` | GET | Download version content |
| `/versions/v1/{doc_id}/{version}/restore` | POST | Restore version |

## Device Configuration

To point your device at this server:

1. Generate SSL certificates (see tools/ssl_certs.py)
2. Install CA cert on device
3. Redirect traffic via /etc/hosts or mitmproxy
4. Start server with generated certificates

```bash
# Generate certs
python tools/ssl_certs.py --domain your-server.local

# Redirect on device (SSH required)
echo "192.168.1.100 *.remarkable.com" >> /etc/hosts

# Or use mitmproxy
mitmproxy --mode transparent --ssl-insecure
```

See [remarkable-research](https://github.com/coleleavitt/remarkable-research) for device configuration tools.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     remarkable-server                        │
├─────────────────────────────────────────────────────────────┤
│  Protocol Layer (V1, V1.5, V2, V3, V4)                      │
│  ┌─────────┬─────────┬─────────┬─────────┬─────────┐       │
│  │   V1    │  V1.5   │   V2    │   V3    │   V4    │       │
│  │  JSON   │  Batch  │ Binary  │  CRDT   │  Meta   │       │
│  └────┬────┴────┬────┴────┬────┴────┬────┴────┬────┘       │
│       │         │         │         │         │             │
│       └─────────┴─────────┴────┬────┴─────────┘             │
│                                │                             │
│  ┌─────────────────────────────┴──────────────────────────┐ │
│  │              Unified Storage Backend                    │ │
│  │     (Hash-based, CRC32C, Content-addressable)          │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Feature Modules:                                            │
│  • FTS5 Search  • Calendar  • Integrations  • Versions      │
│  • Read-Later   • RSS/Feeds • Device Mgmt   • GraphQL       │
└─────────────────────────────────────────────────────────────┘
```

## Related Projects

- [remarkable-rs](https://github.com/coleleavitt/remarkable-rs) — Rust client library (15 crates)
- [remarkable-ecosystem](https://github.com/coleleavitt/remarkable-ecosystem) — 11 complete tools
- [remarkable-research](https://github.com/coleleavitt/remarkable-research) — Protocol documentation

## License

MIT
