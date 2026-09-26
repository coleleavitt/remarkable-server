# reMarkable Server Implementation Gap Analysis

*Updated via firmware 3.29 binary analysis*

## ✅ IMPLEMENTED - Verified against xochitl firmware

### Sync Protocol (all versions)
- V1: /document-storage/json/2/* (firmware 1.x-2.x)
- V1.5: /sync/v1.5/batch (transitional)
- V2: /sync/v2/root, /sync/v2/files/{hash}
- V3: /sync/v3/root, /sync/v3/files/{hash}, check-files, files-list, missing (current production)
- V4: /sync/v4/* (future, full sync15 protocol)
- /sync/reports/v1 - Sync reports endpoint ✓

### Authentication/Device Management
- /token/json/2/device/new - Device registration ✓
- /token/json/2/user/new - Token refresh ✓
- /token/json/{2,3}/device/delete - Device self-revoke ✓
- /token/json/4/device/exchange - Legacy credential → OAuth bundle (3.28) ✓
- /devices/v1 - Pairing code generation (admin token only), own-device listing/deletion ✓
- Deleting a device revokes its device + user tokens and closes its /notifications/ws and (when enabled) /mqtt sessions ✓
- /oauth/device/code, /oauth/token, /oauth/revoke - OAuth2 device-code flow (3.28); owner approval via /oauth/verify or POST /admin/oauth/approve, per-IP + global rate limit, `PUBLIC_URL` for the verification URI ✓

### Discovery & Settings
- /discovery/v1/endpoints - Service discovery ✓
- /service/json/1/document-storage - Legacy discovery ✓
- /settings/v1/beta - Beta feature flags ✓
- /settings/v1/features - Feature flags ✓
- /updates/v1/check, /updates/check - Update checks ✓

### Notifications
- /notifications/ws/json/1 - WebSocket for real-time notifications ✓ — **what the tablet uses** (verified, see "MQTT: what the tablet actually uses")
- /mqtt - MQTT 3.1.1 over WebSocket push (auth: `Authorization` header on the upgrade, or the token as the CONNECT password/username) — **opt-in** (`MQTT_WS_NOTIFICATIONS=1`), off by default: no tablet uses it; path and topic are unverified guesses

### Passcode
- /passcode/v1/resets/{uuid} - Create/get reset request ✓
- /passcode/v1/reset/{uuid}/approve - Device approve ✓
- /passcode/v1/reset/{uuid}/deny - Device deny ✓
- /admin/passcode/resets/{uuid}/approve - Admin approve ✓

### Handwriting/Convert
- /convert/v1/handwriting - Handwriting recognition (needs the `tesseract` binary, or `HWR_COMMAND`) ✓
- /handwriting/v1/search - Handwriting search ✓
- /api/v1/page - Alternative handwriting endpoint ✓

### Screenshare
- MQTT 3.1.1-over-TLS signalling broker (`SCREENSHARE_BIND`; :8883 on the Linode) ✓
- /screenshare/view - Browser viewer (`SCREENSHARE_VIEWER=1`, ADMIN_TOKEN) ✓
- /screenshare/v1/rooms - Room management ✓
- /screenshare/v1/rooms/join-active - Join active room ✓
- /screenshare/v1/rooms/{roomId} - Get/delete room ✓

### Search
- /search/v1 - Full-text search ✓
- /search/v1/error - Search error reporting ✓
- /search/v1/settings - Search settings ✓

### Share
- /share/v1/email - Share via email ✓
- /share/v1/link - Share via link ✓

### Report/Analytics
- /report/v1 - Client reports ✓
- /analytics/v2/events - Analytics event ingestion ✓
- /post - Crash-report sink (backtrace-proxy host; 32 MiB body, `CRASH_MAX_TOTAL_BYTES` / `CRASH_MAX_REPORTS` quota) ✓

### Gentree (Document Tree)
- /gentree/v1/DeleteEntry - Delete document ✓
- /gentree/v1/EntrySession - Session management ✓
- /gentree/v1/GetEntries - List entries ✓
- /gentree/v1/GetFile, /GetFiles - Get file(s) ✓
- /gentree/v1/PutFile - Upload file ✓

### Integrations
- /integrations/v1/ - List integrations ✓
- /integrations/v2/instances - Integration instances ✓
- /integrations/v2/calendars/* - Calendar integration ✓ (only ICS files sync; CalDAV/Google/Office 365 sync answers "not implemented")
- /integrations/v2/readlater/* - Read-it-later accounts/articles ✓ (credentials persisted; the sync endpoints are placeholders that don't sync)
- /integrations/v2/cloud/* - Cloud storage OAuth (Google Drive, Dropbox, OneDrive) ✓ (sync confined to `<storage>/integrations`)


## ⚠️ GAPS - Based on firmware analysis

### ~~1. Messaging Integration~~ ✅ FIXED
**Route added:** `POST /integrations/v2/messaging/{instance_id}/message`
**Handler:** `service::send_integration_message` (stub - accepts request, returns success)

### ~~2. Storage vs Cloud Path Mismatch~~ ✅ FIXED
**Route added:** `/integrations/v2/storage` now aliases `/integrations/v2/cloud`

## ✅ PREVIOUSLY LISTED AS MISSING - NOW IMPLEMENTED

These were in the old gap analysis but are actually implemented:
- ~~WebSocket Notifications~~ → /notifications/ws/json/1 ✓
- ~~Handwriting Recognition~~ → /convert/v1/handwriting ✓
- ~~Passcode Reset Flow~~ → Full flow implemented ✓
- ~~Integrations API~~ → Calendar, ReadLater, Cloud storage ✓
- ~~Screen Share~~ → REST API + room management ✓


## 📝 IMPLEMENTATION NOTES

### Service Hosts (from firmware)
The firmware references these cloud hosts:
- my.remarkable.com
- auth.remarkable.com
- *.cloud.remarkable.com
- webapp-production-*.cloud.remarkable.com
- errors.cloud.remarkable.com
- Internal: dev.internal.cloud.remarkable.com, qa.internal.cloud.remarkable.com

### MQTT
Two MQTT endpoints exist:
- `/mqtt` — MQTT 3.1.1 over WebSocket on the API host, authenticated with a device/user token, for
  sync push. It forwards the same `WsMessage` JSON as `/notifications/ws/json/1` on every concrete
  topic the client subscribed to. **Off by default**; served only with `MQTT_WS_NOTIFICATIONS=1`
  (`true`/`on`), because the tablet never uses it (below).
- The screenshare broker (`SCREENSHARE_BIND`, MQTT over TLS; :8883 on the Linode). Its sessions
  are not closed when a device is revoked.

#### MQTT: what the tablet actually uses

Checked on 2026-09-26 against the production Linode (read-only: `journalctl -u remarkable-server`,
nginx `access.log*`), with the production tablet (user `local-user`) connected through
rm-proxy (`127.0.0.1:443 → remarkable.unwrap.rs:443`, `127.0.0.2:443 → remarkable.unwrap.rs:8883`).
No firmware was disassembled for this. Third-party IPs and all tokens are redacted.

Sources and windows:
- nginx access logs, 2026-09-12 → 2026-09-26 (15 days, 69,856 requests over all vhosts). The tablet
  (`xochitl/3.3.2.1666 (codex 3.1.158-4)`) shows up on 2026-09-25/26: 282 requests.
- Service journal at `RUST_LOG=remarkable_server=info`, retained 2026-09-25 01:37 → 2026-09-26 04:06
  UTC. `mqtt_ws` logs `MQTT WebSocket upgrade request for notifications` at info on every upgrade.
- `/mqtt` was routed in 6ffa7bb (the only commit that adds the route; making it opt-in is the only change
  that removes it) and was live at least from 2026-09-26 04:05:39 UTC. The running binary, started then,
  contains two log strings that 6ffa7bb added (`MQTT CONNECT without a valid token, refusing`,
  `second MQTT CONNECT, closing`) and not the opt-in change's `MQTT_WS_NOTIFICATIONS` (checked with
  `grep -a -c -F` on `/proc/<pid>/exe`). `MQTT WebSocket upgrade request for notifications` proves
  nothing here: it predates 6ffa7bb, in a handler nothing routed. Before the route existed, a request
  for it would have been a 404, which nginx still logs, so the nginx count below does not depend on
  when the route went live.

Sync notifications: the tablet uses `/notifications/ws/json/1` only.
- nginx: 16 tablet `GET /notifications/ws/json/1` (15 × `101`, 1 × `502` during a restart), e.g.
  ```
  <tablet-ip> - - [25/Sep/2026:21:02:49 +0000] "GET /notifications/ws/json/1 HTTP/1.1" 101 450 "-" "xochitl/3.3.2.1666 (codex 3.1.158-4)"
  ```
  (nginx logs a WebSocket when it closes.) All tablet paths: `/blobstorage` (PUT 56, GET 39),
  `/sync/v2/signed-urls/uploads` 56, `/sync/v2/signed-urls/downloads` 43, `/v1/reports` 42,
  `/notifications/ws/json/1` 16, `/sync/v2/sync-complete` 12, `/token/json/2/user/new` 10,
  `/integrations/v1/` 3, `/service/json/1/webapp` 2, `/discovery/v1/endpoints` 2,
  `/settings/v1/beta` 1.
- nginx: **0** requests from any client whose request line contains `mqtt`, over all 15 days,
  including after the route went live. No raw MQTT (a `\x10` CONNECT) reached :443 from the
  tablet's addresses either.
- journal: **0** lines from `remarkable_server::mqtt_ws`; 16 `New notifications WebSocket connection`.
  After the 04:05:39 deploy (with `/mqtt` live) the tablet reconnected to the JSON endpoint again:
  ```
  2026-09-26T04:05:39.033888Z  INFO remarkable_server::screenshare: screenshare MQTT broker listening (TLS) bind=0.0.0.0:8883
  2026-09-26T04:06:13.787692Z  INFO remarkable_server::notifications: WebSocket upgrade request for notifications
  2026-09-26T04:06:13.787805Z  INFO remarkable_server::notifications: New notifications WebSocket connection session_id=9454c4a0-a5fd-4945-b94d-512addc0a223
  2026-09-26T04:06:13.787867Z  INFO remarkable_server::notifications: Sending initial SyncComplete notification session_id=9454c4a0-a5fd-4945-b94d-512addc0a223
  ```
- Discovery (`GET /discovery/v1/endpoints`, fetched twice by the tablet) returns the same bare host
  for `notifications` and `mqttbroker`; the tablet builds `wss://{notifications}/notifications/ws/json/1`
  from it and never used `mqttbroker` for a WebSocket.

Screen share: the tablet's only observed MQTT traffic, raw MQTT 3.1.1 over TLS to the :8883 broker.
Whether it also subscribes to sync topics there is unconfirmed (see the SUBSCRIBE item below).
- Client ids are `{user_id}-{uuid}`: 30 tablet CONNECTs, 19 distinct ids, all `user=local-user`, e.g.
  ```
  2026-09-26T02:39:28.581429Z  INFO remarkable_server::screenshare: mqtt client connected client=local-user-f08fe3ce-72ad-4fb9-a3a0-8862f4fe278a user=local-user
  2026-09-26T02:39:30.583393Z  INFO remarkable_server::screenshare_viewer: screenshare viewer answered tablet offer room=e19db5c9-70d2-482b-90a2-6f385655fc57 tablet=local-user-f08fe3ce-72ad-4fb9-a3a0-8862f4fe278a via="mqtt"
  2026-09-26T03:00:24.083697Z  INFO remarkable_server::screenshare: screenshare room created room=35d0812d-4b8e-4d2b-a90c-328843f27d9e client=local-user-f08fe3ce-72ad-4fb9-a3a0-8862f4fe278a
  ```
  29 rooms created by tablet clients and 12 viewer answers `via="mqtt"` (0 via REST; the tablet made
  0 requests to `/screenshare/v1/*`), so 3.3.2 signals over the MQTT broker, publishing to
  `remarkable/screenshare/signaling/user/{uid}/client/{cid}`.
- **0** `mqtt subscribe denied` / `mqtt publish denied`: every tablet SUBSCRIBE filter and PUBLISH
  topic fell inside the broker ACL (`user/{uid}/...`, `remarkable/screenshare/signaling/user/{uid}/...`),
  consistent with remarkable-mqtt's `screenshare::subscriptions` (`user/{uid}/signaling`,
  `user/{uid}/client/{cid}/signaling/#`, checked against a tablet there). Accepted filters were not
  logged, so the exact filters, and whether the tablet also subscribes to sync topics on this broker,
  are unconfirmed. Accepted filters are now logged at debug; to see them, run with
  `RUST_LOG=remarkable_server=info,remarkable_server::screenshare=debug`.
- Other clients on :8883 are operator tooling, not the tablet (`viewer-<hex>`, `viewer-probe-<hex>`,
  `capture-local-us`), plus 107 failed TLS handshakes from internet scanners.

remarkable-mqtt (remarkable-rs) comparison: its `topics.rs` sync topics (`user/{uid}/sync`,
`user/{uid}/client/{cid}/{notifications,sync}`) describe reMarkable's cloud VerneMQ broker and are not
marked as verified; its client uses raw MQTT over TLS, not WebSocket. Nothing there, and nothing in
the logs above, points at an MQTT-over-WebSocket `/mqtt` endpoint.

Action taken: `/mqtt` is opt-in (`MQTT_WS_NOTIFICATIONS=1`), not served by default, which removes
an authenticated but unused WebSocket endpoint from the public host. Its tests enable it explicitly.

### Full Sync15 Protocol
The V3/V4 sync handlers exist but may not fully implement:
- Merkle tree validation
- Conflict resolution
- Schema version negotiation

These are edge cases that may not be hit in normal operation.
