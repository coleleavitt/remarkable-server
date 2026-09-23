
# reMarkable Server Implementation Gap Analysis

## ✅ IMPLEMENTED (in remarkable-server)

### Sync Protocol (all versions)
- V1: /document-storage/json/2/* (firmware 1.x-2.x)
- V1.5: /sync/v1.5/batch (transitional)
- V2: /sync/v2/root, /sync/v2/files/{hash}
- V3: /sync/v3/root, /sync/v3/files/{hash} (current production)
- V4: /sync/v4/* (future)

### Authentication/Device Management
- /token/json/2/device/new - Device registration
- /token/json/2/user/new - Token refresh
- /token/json/3/device/delete - Device deletion
- /devices/v1 - Pairing code generation, device listing

### Discovery & Settings
- /discovery/v1/endpoints - Service discovery
- /service/json/1/document-storage - Legacy discovery
- /settings/v1/beta - Beta feature flags
- /settings/v1/features - Feature flags
- /updates/v1/check, /updates/check - Update checks

### Calendar (stub)
- Full calendar API routes

### Utilities
- /health, /debug/files, /debug/clear


## ❌ MISSING - CRITICAL FOR DEVICE SYNC

### 1. WebSocket Notifications (/notifications/ws/json/1)
- Currently: stub returning 200
- Needed: Full WebSocket upgrade + message protocol
- Device expects: Connection state notifications, sync triggers
- Protocol: Qt5WebSockets, JSON messages

### 2. MQTT Broker Integration
- Device connects to VerneMQ for real-time sync
- Screen share signaling
- Document change notifications
- Auth: devicetoken as username, usertoken as password

### 3. Handwriting Recognition Proxy
- /convert/v1/handwriting
- Proxies to MyScript cloud or local OCR
- Returns recognized text

### 4. Passcode Reset Flow
- /passcode/v1/reset/ - Request reset
- /passcode/v1/resets/ - Admin approval/denial
- /approve, /deny endpoints

### 5. Integrations API
- Cloud storage OAuth (Google Drive, Dropbox, OneDrive)
- Email document ingestion
- Read-it-later (Pocket, Instapaper)


## ❌ MISSING - IN DOCUMENTATION BUT NOT IMPLEMENTED

### From SYNC_PROTOCOL_COMPLETE.md:
- Full blob traversal and merkle tree validation
- Schema version negotiation
- Conflict resolution

### From MQTT_PROTOCOL.md:
- VerneMQ connection handling
- Topic subscription (device/{id}/notifications)
- Message types: sync_complete, document_changed, etc.
- SSL/TLS with client certs

### From SCREEN_SHARE_PROTOCOL.md:
- WebRTC signaling via MQTT
- RFB/VNC fallback (v1 compat)
- ICE candidate exchange
- DataChannel for screen data

### From API_REFERENCE.md:
- Store API (purchases, subscriptions)
- Enterprise API (organization management)
- Connect subscription validation
- Custom JWT claim validation


## ❌ MISSING - DISCOVERED FROM FIRMWARE STRINGS

### Endpoints found in xochitl binary:
- /sync/v2/sync-complete - Sync completion callback
- /service/json/* - Multiple service discovery variants
- /download/ - Direct file download
- /upload - Direct file upload
- /thumbnail/ - Thumbnail service
- /search/ - Full-text search
- /template-import/ - Template importing
- /remarkable-imports - Import handling

### Internal state management:
- no.remarkable.sync.Synchronizer D-Bus interface
- Sync state machine (syncing, paused, error, complete)
- Token refresh scheduling
- Network availability tracking
