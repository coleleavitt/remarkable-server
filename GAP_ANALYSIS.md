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
- Deleting a device revokes its device + user tokens and closes its /notifications/ws and /mqtt sessions ✓
- /oauth/device/code, /oauth/token, /oauth/revoke - OAuth2 device-code flow (3.28); owner approval via /oauth/verify or POST /admin/oauth/approve, per-IP + global rate limit, `PUBLIC_URL` for the verification URI ✓

### Discovery & Settings
- /discovery/v1/endpoints - Service discovery ✓
- /service/json/1/document-storage - Legacy discovery ✓
- /settings/v1/beta - Beta feature flags ✓
- /settings/v1/features - Feature flags ✓
- /updates/v1/check, /updates/check - Update checks ✓

### Notifications
- /notifications/ws/json/1 - WebSocket for real-time notifications ✓
- /mqtt - MQTT 3.1.1 over WebSocket push (auth: `Authorization` header on the upgrade, or the token as the CONNECT password/username) ✓ — topic not yet verified against a real tablet

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
  topic the client subscribed to, because nothing pins the topic xochitl expects; unverified
  against a real tablet.
- The screenshare broker (`SCREENSHARE_BIND`, MQTT over TLS; :8883 on the Linode). Its sessions
  are closed when their device is revoked, like those of `/mqtt` and `/notifications/ws`.

### Full Sync15 Protocol
The V3/V4 sync handlers exist but may not fully implement:
- Merkle tree validation
- Conflict resolution
- Schema version negotiation

These are edge cases that may not be hit in normal operation.
