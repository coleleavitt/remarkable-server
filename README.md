# remarkable-server

A local sync server for reMarkable tablets, implementing **all sync protocol versions** (V1, V1.5, V2, V3, V4).

## Features

- **Multi-version sync protocol support** — serves any device regardless of firmware version
- Full sync v3 API compatibility (current production)
- Legacy V1/V1.5/V2 support for older firmware
- Future V4 protocol with extended metadata
- Hash-based content-addressable storage with a SQLite sync index (atomic root commits)
- CRC32C checksum validation
- rm-filename header enforcement
- Token refresh and device pairing
- **FTS5 full-text search** (document names, folders, PDF content)
- **Calendar sync** (local ICS files, CalDAV, Google Calendar and Microsoft 365 via Graph; credentials persisted server-side)
- **Cloud integrations** (Google Drive, Dropbox, OneDrive)
- **Read-it-later** (Pocket, Instapaper, Wallabag accounts; credentials persisted server-side)
- **Push notifications**: JSON WebSocket (`/notifications/ws/json/1`) and MQTT 3.1.1 over WebSocket (`/mqtt`)
- **OAuth2 device-code login** (software 3.28) with owner approval, and **gentree/v1** delta sync
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

### Storage layout

Everything lives under `--storage`:

- `<2-char prefix>/<sha256>` — blob bytes, content-addressed, written atomically (temp → fsync → rename). The tablet is always served exactly the bytes it uploaded.
- `sync.db` — SQLite (WAL, `synchronous=FULL`), the source of truth: the root hash/generation (commits are a compare-and-swap in one transaction, so they are atomic, durable and safe with two processes on one directory), blob filename/size/last-written time, and a parsed projection of the sync index files (root index and `.docSchema`s) used for missing-blob detection and the unreachable-blob report. Index formats it can't parse are recorded as unparsed and never break sync.
- `root.json` — human-readable mirror of the root, rewritten after every commit (also lets an older binary take over after a rollback).
- `devices.db`, `jwt_secret` — pairing state.
- `.uploads/` — staging for streamed uploads. Request bodies are written here as they arrive (checksummed on the way, never held whole in memory) and moved into the store once verified; failed uploads are removed, and leftovers older than 24 h are swept.
- `readlater.db` (+ `articles/`), `calendars.db`, `feeds.db`, `versions/`, `integrations/` — feature state; see below.

Upgrading from the file-only layout is automatic: the first start imports `root.json` and `meta/*.meta`; every start reconciles the blob table with the files on disk and indexes the current tree. Old `meta/` files are left in place but no longer written. Back up `sync.db` with `sqlite3 sync.db ".backup '…'"` (or with the server stopped), not a live `cp`.

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
| `/devices/v1` | POST | Create pairing code (admin only: `x-admin-token`; optional `?user=`, default `local-user`; see "Pairing a device") |
| `/devices/v1` | GET | List the caller's own devices |
| `/devices/v1/{id}` | DELETE | Remove one of the caller's own devices (404 for another user's) |
| `/token/json/2/user/new` | POST | Refresh user token |
| `/token/json/2/device/new` | POST | Register device |
| `/token/json/3/device/delete` | POST | Unregister the calling device (self-revoke) |
| `/discovery/v1/endpoints` | GET | Service discovery |

Deleting a device (either route above) revokes every device token and every user token it has minted, even if it is paired again later, and closes its open `/notifications/ws` and `/mqtt` sessions (immediately, plus a 60 s re-check). Re-pairing a device to a different user revokes the previous owner's tokens the same way. Admin-minted user tokens (`/admin/create-user`) have no device and are not affected. The screenshare MQTT broker (`SCREENSHARE_BIND`) is not tied to this: its open sessions survive a revocation.

## Search API

Full-text search across all synced documents using SQLite FTS5.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search/v1/query` | GET | Full-text search |
| `/search/v1/stats` | GET | Index statistics |
| `/search/v1/reindex` | POST | Rebuild search index (only documents reachable from the current root; deleted ones drop out) |
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

Prefix `/integrations/v2/calendars`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | List calendars |
| `/` | POST | Add calendar |
| `/{id}` | GET | Get calendar |
| `/{id}` | DELETE | Delete calendar |
| `/{id}/events` | GET | Get events |
| `/{id}/sync` | POST | Sync calendar (provider failures answer `success: false` with an `error`) |
| `/upcoming` | GET | Upcoming events |
| `/sync-all` | POST | Sync all calendars |
| `/{id}/meeting-notes`, `/{id}/events/{event_id}/meeting-notes` | GET, POST | Meeting notes |

Calendar sources (`config.type` when adding a calendar):

| Type | Fields | Sync |
|------|--------|------|
| `ics` | `path` | Local `.ics` file |
| `caldav` | `url`, `username` + `password`, or `bearer_token` | `url` may be the calendar collection or a server/principal/home URL to discover it from (a bare server URL tries `/.well-known/caldav`, then the root; the calendar is picked by display name when there are several; the result is remembered). REPORT `calendar-query` with server-side recurrence expansion; from servers that cannot expand, recurring events are expanded here (RRULE with `FREQ` daily to yearly, `INTERVAL`, `COUNT`, `UNTIL`, `BYMONTH`, `BYMONTHDAY`, `BYDAY`, `WKST`, plus RDATE, EXDATE and moved occurrences; other rules keep their first occurrence). Credentials are only sent to the configured host and hosts of the same site (approximated without a public-suffix list, so shared-hosting suffixes such as `*.github.io` count as one site); redirects to other hosts must be HTTPS, and neither they nor the names they use may lead to internal addresses (only the configured host may be on the LAN) |
| `google` | `calendar_id`, `access_token` and/or `refresh_token` + `client_id` (+ `client_secret`) | Google Calendar API v3 `events.list`, recurring events expanded |
| `office365` | `tenant_id`, `access_token` and/or `refresh_token` + `client_id` (+ `client_secret` for confidential apps), optional `calendar_id` | Microsoft Graph `calendarView` (default calendar when `calendar_id` is unset); use it for Exchange Online too. Needs the delegated `Calendars.Read` permission (or `Calendars.ReadWrite`) and `offline_access` for a refresh token; refreshes ask for `https://graph.microsoft.com/.default offline_access`, i.e. whatever Graph permissions were consented |
| `exchange` | `server`, `username`, `password` | Not synced: on-premises EWS is not supported (answers `success: false`) |

Remote calendars sync the window from 30 days back to 365 days ahead: events are upserted and stored events in that window the provider no longer returns are removed (`events_removed`); when a CalDAV server marks its answer as incomplete (e.g. 507 for truncated results), or expanding its recurring events here would go past a budget (100,000 occurrences, 64 MiB of text copied into them, and bounded work on RRULEs and VTIMEZONE rules), nothing is removed and the sync reports `success: false`. OAuth tokens are obtained out of band; the server refreshes them itself (on expiry or a 401). A sync runs to completion on its own task even if the client disconnects, and saves refreshed or rotated tokens (and a discovered CalDAV collection) when it ends, even if fetching events failed. Syncs of the same calendar never overlap: a sync request for a calendar that is already syncing (or a `/sync-all` while one runs) waits for that sync and gets its result rather than starting another. CalDAV responses are capped at 32 MiB and 500,000 XML elements plus attributes. Passwords, tokens and client secrets live in the `secrets` column of `calendars.db` (file mode 0600), never in API responses. `TZID=` local times in CalDAV data and ICS files are converted to UTC with the VTIMEZONE sent along (a TZID without one, which CalDAV forbids, is read as UTC and logged). An event whose VTIMEZONE rules are too costly to go through is left out rather than stored at a guessed time, and the sync reports `success: false`.

## Integrations API

Prefix `/integrations/v2/cloud` (xochitl 3.29 uses the alias `/integrations/v2/storage`).

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/providers` | GET | List available providers |
| `/providers/configure` | POST | Configure a provider's OAuth client |
| `/providers/{provider}/auth` | GET | Start OAuth (browser callback at `/callback`) |
| `/providers/{provider}/status`, `/quota` | GET | Token status, quota |
| `/providers/{provider}/refresh` | POST | Refresh token |
| `/providers/{provider}/disconnect` | DELETE | Disconnect (revokes the provider grant) |
| `/sync` | POST | Trigger sync |

Supported providers: Google Drive (listings and changes are paged), Dropbox, OneDrive. Dropbox and OneDrive full listings are not recursive, and their change feeds are not scoped to the configured folder.

## Read-it-Later API

Prefix `/integrations/v2/readlater`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/accounts` | GET, POST | List / add connected services |
| `/accounts/{id}` | GET, PUT, DELETE | Get / update / remove a service |
| `/oauth/start`, `/oauth/complete` | POST | Provider OAuth |
| `/articles`, `/articles/{id}` | GET, PUT, DELETE | Articles |
| `/accounts/{id}/sync`, `/sync` | POST | Placeholders: they answer `queued` / zero counts and do not run a sync yet |

Supported services: Pocket, Instapaper, Wallabag (Omnivore was removed after the service shut down in November 2024; adding one is 400). Credentials (tokens, Wallabag password) are stored in `readlater.db` separately from the account config, survive restarts (refreshed tokens are saved right away), and are never returned by the API.

## Versions API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/versions/v1/{doc_id}` | GET | List document versions |
| `/versions/v1/{doc_id}/{version}` | GET | Get specific version |
| `/versions/v1/{doc_id}/{version}/content` | GET | Download version content |
| `/versions/v1/{doc_id}/restore/{version}` | POST | Restore version |
| `/versions/v1/{doc_id}/diff/{v1}/{v2}` | GET | Diff two versions |

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
- `auth.remarkable.com` (new OAuth login; see "OAuth2 login" below), `errors.cloud.remarkable.com`
- `backtrace-proxy.cloud.remarkable.engineering` (crash reports: `POST /post`, see "Crash reports" below)

3.27+ endpoints implemented: `/settings/v1/beta` (GET/POST/DELETE), `/search/v1/settings`
(GET/PATCH), `/search/v1/error`, `/share/v1/link`, and MDM instruction polling (always empty).
REST screenshare (`/screenshare/v1`), gentree/v1, the sync v4 root/file routes and the OAuth
device flow of `user-authenticator-cli` are also implemented; see "software 3.27+/3.28 support" below.

### Pairing a device

```bash
remarkable-server --storage ./remarkable-storage --pair   # prints a one-time code (valid 10 min)
```

Enter the code on the tablet under Settings → General → Account → Connect.

Without shell access, the owner can get a code from `POST /devices/v1` with `x-admin-token`
(see Admin endpoints). The code pairs `local-user` (like `--pair`) unless `?user=<id>` names another
account (1-254 printable ASCII chars without spaces, `/` or `\`, e.g. an email; send `+` as `%2B`; anything else is 400). Device and user tokens cannot mint pairing codes: a second paired device
could otherwise approve the first one's passcode reset.

### Admin endpoints

Admin endpoints are disabled unless `ADMIN_TOKEN` is set; requests must send it as `x-admin-token`.
Every endpoint in this table accepts only that header (a device or user token gets 401):

| Endpoint | Purpose |
|----------|---------|
| `POST /admin/create-user` | Mint a user token (testing / desktop clients) |
| `POST /devices/v1[?user=<id>]` | Mint a one-time pairing code for `local-user` (same as `--pair`), or for `<id>` |
| `POST /admin/passcode/resets/{id}/approve` | Approve a tablet's passcode (PIN) reset request; the id is logged when the tablet asks |
| `DELETE /debug/clear` | Delete every blob and reset the root |
| `GET /admin/reports?contains=&limit=` | Stored tablet telemetry (`reports.jsonl`) |
| `POST /admin/mdm/enqueue`, `GET /admin/mdm/instructions` | MDM instruction queue |
| `GET /admin/storage/unreachable?grace_secs=86400` | Read-only JSON list of blobs not reachable from the current root and older than the grace period (default 24 h). Never deletes; returns 500 with a reason if part of the tree couldn't be parsed |
| `POST /admin/storage/gc?grace_secs=604800&dry_run=true` | Delete the blobs the unreachable report lists (default grace 7 days). A dry run unless `dry_run=false`; refuses (500) if the tree isn't fully parsed; stops with 409 if a sync commits mid-run. Back up storage and dry-run first (see DEPLOYMENT.md) |

Two more take either the admin token **or** a paired device's credential (`Authorization: Bearer <device token>`;
short-lived user tokens and tokens of deleted devices are refused): `POST /admin/oauth/approve` and
`POST /oauth/verify` (the OAuth approval form; see "OAuth2 login"). The screenshare browser viewer
(`/screenshare/view`, `SCREENSHARE_VIEWER=1`) is only mounted when `ADMIN_TOKEN` is set and signs in with
it (HttpOnly cookie, or the `x-admin-token` header).

No other route checks the admin token; `/debug/files`, for instance, takes an ordinary device/user token.

`JWT_SECRET` sets the token signing key (default is a built-in constant; changing it invalidates paired devices).

### Optional features (environment)

| Variable | Enables |
|----------|---------|
| `SMTP_HOST`, `SMTP_PORT` (587), `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` | Tablet "Send by email" (`POST /share/v1/email`, STARTTLS) |
| `SCREENSHARE_BIND` (e.g. `10.11.99.3:443`) | Screenshare signaling broker: MQTT 3.1.1 over TLS. Firmware dials `vernemq-prod.cloud.remarkable.engineering:443`, so give it its own address and point that name at it in the tablet's `/etc/hosts`. Screen data itself is peer-to-peer WebRTC. |
| `SCREENSHARE_ICE_SERVERS` | JSON list of ICE servers for `room-joined` (default `[]`; entries use a singular `url` key) |
| `EMAIL_INBOUND_BIND` (e.g. `127.0.0.1:2525`) | Inbound SMTP: mail PDF/EPUB attachments to `send@{device-id}.remarkable.local` and they appear on the tablet |
| `HWR_CAPTURE_DIR` | Save every handwriting request/response pair |
| `HWR_COMMAND` | Replace `tesseract` with another recogniser (e.g. `contrib/hwr/trocr_hwr.py`) |
| `CRASH_DIR` (`./crash-dumps`, relative to the working directory), `CRASH_MAX_TOTAL_BYTES` (512 MiB), `CRASH_MAX_REPORTS` (200) | Crash-report sink storage and quota (see below) |
| `PUBLIC_URL` (e.g. `https://remarkable.unwrap.rs`) | Public base URL for links a person opens: the OAuth `verification_uri`/`verification_uri_complete` (default `https://<--host>`) and firmware archive downloads |

Handwriting conversion (`POST /convert/v1/handwriting`) and handwriting search (`/handwriting/v1/search`) run the
local `tesseract` binary, which must be installed (`apt install tesseract-ocr`); without it both fail. Fine for neat
print, poor for cursive/maths.

Crash reports: the tablet's crash uploader posts to `POST /post` (unauthenticated: its token is a Backtrace
project token baked into the device). Bodies are capped at 32 MiB, each part at 16 MiB (truncated), at most 16
parts are kept per report, and after each report the oldest reports are deleted until the directory is under
`CRASH_MAX_TOTAL_BYTES` and `CRASH_MAX_REPORTS`. New reports are never rejected, so the tablet stops retrying.

Uploads (sync v3 / sync15 / v2 / v4 blob PUTs, document uploads, share links, gentree `PutFile`) are streamed to
`<storage>/.uploads/` rather than buffered, up to 1 GiB per blob. gentree `PutFile` still buffers its JSON body
(the blob is base64 inside it), and handwriting convert and share-by-email still buffer the request.

Authenticated feature APIs (Bearer token): `/search/v1/*`, `/versions/v1/*`, `/feeds/v1/*` (RSS/Atom to EPUB), `/integrations/v2/{calendars,readlater,cloud}/*`, `/email/v1/*` (when inbound email is on).

Cloud sync (`POST /integrations/v2/{cloud,storage}/sync`) only reads/writes under `<storage>/integrations/`: `local_path` must be relative to that directory (default `.` = the directory itself), absolute paths, `..` and symlinks leading outside it are rejected with 400, and missing subdirectories are created. Provider requests time out (10 s connect, 10 min total).

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
`https://auth.remarkable.com/{tectonic,subscription,mdm,created_at}` claims.

Device codes are **not** auto-approved (RFC 8628): `/oauth/token` answers
`authorization_pending` until the owner approves the `user_code` the client shows,
`expired_token` after `expires_in` (600 s), and `invalid_grant` for a `device_code` that was
never issued or was already redeemed. Polling again before `interval` (5 s) has passed gets
`slow_down` and a 5 s longer interval. Pending codes are held in memory, expired ones are
evicted, and at most 256 are kept (unapproved codes are dropped first).

`POST /oauth/device/code` is unauthenticated, so it is rate limited in process: 5 codes per
client IP (an IPv6 /64 counts as one client) per 10 minutes and 20 codes per minute in total;
past either limit it answers `429` with `{"error":"slow_down"}` and `Retry-After`. The client
IP is the socket peer; only when the peer is loopback (a local reverse proxy such as nginx) is
the rightmost `X-Forwarded-For` entry (else `X-Real-IP`) used instead.

To approve (the first two need `ADMIN_TOKEN` set; the third does not):

- open the advertised `verification_uri` (`<PUBLIC_URL or https://<host>>/oauth/verify?user_code=1234-5678`)
  and submit the code with the admin token, or
- `curl -X POST https://<host>/admin/oauth/approve -H "x-admin-token: $ADMIN_TOKEN"
  -H 'content-type: application/json' -d '{"user_code":"1234-5678"}'`, or
- the same request with `Authorization: Bearer <device credential>` of an already-paired
  device instead of the admin token (short-lived access credentials and credentials of
  deleted devices are refused).

The server also logs `OAuth device code requested` with the `user_code` at WARN. Legacy
pairing (`--pair` + `/token/json/2/device/new`) and `/token/json/4/device/exchange` are
unchanged and need no approval.
