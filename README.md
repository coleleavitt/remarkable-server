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

The device talks to the server directly over HTTPS on port 443; no proxy runs on the device.

1. Give the host a static IP on the USB network (the device's DHCP lease is 60s and the address drifts):
   ```bash
   nmcli con modify "<usb connection>" ipv4.method manual ipv4.addresses 10.11.99.2/29 ipv4.never-default yes
   nmcli con up "<usb connection>"
   ```
2. Install a local CA on the device (`/usr/local/share/ca-certificates/`, then `update-ca-certificates`) and sign a server cert with it covering `*.remarkable.com`, `*.cloud.remarkable.com`, `*.tectonic.remarkable.com`, `*.internal.cloud.remarkable.com`, `*.appspot.com`. Put it in `certs/server.crt` / `certs/server.key` (git-ignored).
3. Map each cloud hostname to the host in the device's `/etc/hosts` (no wildcards — one line per name):
   ```
   10.11.99.2 my.remarkable.com
   10.11.99.2 internal.cloud.remarkable.com
   10.11.99.2 local.tectonic.remarkable.com
   10.11.99.2 eu.tectonic.remarkable.com
   ...
   ```
4. Allow unprivileged binding to 443 on the host (once):
   ```bash
   echo 'net.ipv4.ip_unprivileged_port_start=443' | sudo tee /etc/sysctl.d/50-remarkable-server.conf
   sudo sysctl --system
   ```
5. Run:
   ```bash
   remarkable-server --bind 10.11.99.2:443 --cert certs/server.crt --key certs/server.key
   ```

`--host` (default `local.tectonic.remarkable.com`) is what discovery hands back to the device; it must be in the device's `/etc/hosts` and covered by the cert.

### Firmware 3.27+ hostnames

Newer firmware reaches more hosts. Add them to the device's `/etc/hosts` (all covered by the
`*.remarkable.com`, `*.cloud.remarkable.com`, `*.tectonic.remarkable.com` and
`*.cloud.remarkable.engineering` cert names):

- `local.tectonic.remarkable.com`: the API and notifications host, built from the user token's
  `https://auth.remarkable.com/tectonic` claim (`local`); discovery is not used for it
- `auth.remarkable.com` (new OAuth login; not implemented yet), `errors.cloud.remarkable.com`
- `backtrace-proxy.cloud.remarkable.engineering` (crash reports; not implemented)

3.27+ endpoints implemented: `/settings/v1/beta` (GET/POST/DELETE), `/search/v1/settings`
(GET/PATCH), `/search/v1/error`, `/share/v1/link`, and MDM instruction polling (always empty).
Not implemented: REST screenshare (`/screenshare/v1`), gentree/v1 and sync v4 (3.28 `rm-sync`),
and the OAuth flow of `user-authenticator-cli`.

### Pairing a device

```bash
remarkable-server --storage ./remarkable-storage --pair   # prints a one-time code (valid 10 min)
```

Enter the code on the tablet under Settings → General → Account → Connect.

### Admin endpoints

Admin endpoints are disabled unless `ADMIN_TOKEN` is set; requests must send it as `x-admin-token`.

| Endpoint | Purpose |
|----------|---------|
| `POST /admin/create-user` | Mint a user token (testing / desktop clients) |
| `POST /admin/passcode/resets/{id}/approve` | Approve a tablet's passcode (PIN) reset request; the id is logged when the tablet asks |

`JWT_SECRET` sets the token signing key (default is a built-in constant; changing it invalidates paired devices).

### Optional features (environment)

| Variable | Enables |
|----------|---------|
| `SMTP_HOST`, `SMTP_PORT` (587), `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` | Tablet "Send by email" (`POST /share/v1/email`, STARTTLS) |
| `SCREENSHARE_BIND` (e.g. `10.11.99.3:443`) | Screenshare signaling broker: MQTT 3.1.1 over TLS. Firmware dials `vernemq-prod.cloud.remarkable.engineering:443`, so give it its own address and point that name at it in the tablet's `/etc/hosts`. Screen data itself is peer-to-peer WebRTC. |
| `SCREENSHARE_ICE_SERVERS` | JSON list of ICE servers for `room-joined` (default `[]`; entries use a singular `url` key) |
| `EMAIL_INBOUND_BIND` (e.g. `127.0.0.1:2525`) | Inbound SMTP: mail PDF/EPUB attachments to `send@{device-id}.remarkable.local` and they appear on the tablet |
| `HWR_CAPTURE_DIR` | Save every handwriting request/response pair |

Handwriting conversion (`POST /convert/v1/handwriting`) runs the local `tesseract` binary: fine for neat print, poor for cursive/maths.

Authenticated feature APIs (Bearer token): `/search/v1/*`, `/versions/v1/*`, `/feeds/v1/*` (RSS/Atom to EPUB), `/integrations/v2/{calendars,readlater,cloud}/*`, `/email/v1/*` (when inbound email is on).

See [remarkable-research](https://github.com/coleleavitt/remarkable-research) for device configuration tools.

## Related Projects

- [remarkable-rs](https://github.com/coleleavitt/remarkable-rs) — Rust client library (15 crates)
- [remarkable-ecosystem](https://github.com/coleleavitt/remarkable-ecosystem) — 11 complete tools
- [remarkable-research](https://github.com/coleleavitt/remarkable-research) — Protocol documentation

## License

MIT


## software 3.27+/3.28 support

Reconstructed from the 3.28 device binaries (xochitl, rm-sync, user-authenticator-cli)
with a decompiler; see the analysis under `/tmp/re-3.28` and `/tmp/xochitl-re-3.28`.
The paired tablet runs 3.3.2, so these paths are unit-tested for shape but not yet
verified against a real 3.28 device. They are additive and do not affect 3.3.2 sync.

### Screenshare (REST, replaces the 3.x MtokenT broker)
`POST /screenshare/v1/rooms`, `POST /screenshare/v1/rooms/join-active`,
`GET|DELETE /screenshare/v1/rooms/{roomId}`, `POST .../keepalive`,
`POST .../messages/broadcast`, `POST .../messages/direct`. Signalling is relayed to the
user's other clients as `ScreenshareMessage` / `ScreenshareRoomCreated` events on the
notifications channel (data = base64 inner JSON). ICE servers come from
`SCREENSHARE_ICE_SERVERS`. Rooms expire 60 s after the last keepalive.

### gentree/v1 delta sync (rm-sync)
`POST /gentree/v1/{GetEntries,GetFiles,GetFile,PutFile,DeleteEntry,EntrySession}`
(`/sync/v3/missing` and `/sync/v3/check-files` are reused). Backed by the same content
store and root as sync v3; `EntrySession` commits a new root under an optimistic
`preEntryGeneration` lock (412 on conflict). rm-sync also still speaks `/sync/v3/*` and
`/sync/v4/root`, which remain the reliable path.

### OAuth2 login (auth.remarkable.com flow)
`POST /oauth/device/code` (device-code grant), `POST /oauth/token` (device_code + refresh_token
grants), `POST /oauth/revoke`, and `POST /token/json/4/device/exchange` (migrate a legacy
device access data). The access access data is the same user access data the sync/gentree auth already
accepts, so no other change is needed; the id access data is an HS512 auth data carrying the
`https://auth.remarkable.com/{tectonic,subscription,mdm,created_at}` claims. Single-user
local server: a minted device-code auto-approves to the local account (no web UI).
