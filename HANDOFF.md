# remarkable-server — Operator Handoff

Self-hosted replacement for the reMarkable cloud, in Rust. The reMarkable 2
tablet talks directly to this server over the USB cable. No proxy.

## Repo
- Path: `~/RustProjects/tools/remarkable-server`  (branch `master`)
- Remote: `github.com:coleleavitt/remarkable-server`
- Pushed:   `881e96e`
- Committed locally, NOT pushed: `ff51ece` (3.27+/3.28 endpoints)
- Depends on sibling crates via relative path `../remarkable/crates/...`
  (remarkable-lines, remarkable-core). A clone elsewhere needs that workspace
  next to it, or switch these to git deps.

## Network (USB)
- Laptop connection "Wired connection 2" is pinned to static addresses:
  - `10.11.99.2/29` — HTTPS API
  - `10.11.99.3/29` — screenshare message queue broker
- Tablet is `10.11.99.1`.
- Port 443 as non-root: `net.ipv4.ip_unprivileged_port_start=443` in
  `/etc/sysctl.d/50-remarkable-server.conf` (already applied).

## Secrets / storage (all git-ignored)
- `certs/server.crt`, `certs/server.key` — TLS, signed by the local CA the
  tablet trusts. Covers *.remarkable.com, *.cloud/tectonic.remarkable.com, etc.
- `certs/admin-token` — auth for the /admin/* approval routes.
- `test-storage/` — YOUR REAL NOTEBOOKS (47 docs, ~595 files, ~120 MB). This is
  the only cloud copy. Back it up. Consider renaming to remarkable-storage.

## Run
```
RUST_LOG=remarkable_server=info \
SCREENSHARE_BIND=10.11.99.3:443 \
ADMIN_TOKEN="$(cat certs/admin-token)" \
./target/release/remarkable-server \
  --bind 10.11.99.2:443 --storage ./test-storage \
  --cert certs/server.crt --key certs/server.key
```
SMTP for share-by-email is read from `~/.config/niri-activity-rs` (SMTP_HOST etc;
mail.unwrap.rs:587 STARTTLS, from cole@unwrap.rs). The server waits for the USB
address instead of crashing when the tablet sleeps/unplugs, and binds on return.

Pair a new device: `./target/release/remarkable-server --pair` prints a code;
enter it on the tablet under Settings > Account > Connect.

## CLI flags
`-b/--bind`, `-H/--host` (default local.tectonic.remarkable.com), `-s/--storage`,
`-c/--cert`, `-k/--key`, `--pair`.

## Env vars
`ADMIN_TOKEN`, `JWT_SECRET` (default is the public built-in constant; setting
your own logs the tablet out — re-pair), `SCREENSHARE_BIND`,
`SCREENSHARE_ICE_SERVERS`, `EMAIL_INBOUND_BIND`, `POCKET_CONSUMER_KEY`,
`INSTAPAPER_CONSUMER_KEY`/`_SECRET`.

## Tablet side
- `/etc/hosts` maps the reMarkable cloud hostnames to `10.11.99.2`
  (screenshare `vernemq-prod...` to `10.11.99.3`). Original saved as
  `/etc/hosts.bak`.
- Local CA installed under `/usr/local/share/ca-certificates/`.
- No proxy services on the device (the three old ones were removed).

## Compatibility

### Firmware 3.3.2 — FULLY WORKING
Sync, pairing, upload, email, handwriting convert + in-notebook search,
passcode reset, MQTT screenshare signalling.

### Firmware 3.28/3.29 — NEARLY COMPLETE
Based on xochitl binary analysis (via `strings`), the server now implements
**all major endpoints** the firmware calls:

✓ Discovery, Settings, Beta flags
✓ Sync v1/v1.5/v2/v3/v4 (all generations)
✓ WebSocket Notifications (/notifications/ws/json/1)
✓ Passcode reset flow (device + admin approval)
✓ Handwriting recognition (/convert/v1/handwriting)
✓ Search (full-text + error/settings)
✓ Screenshare REST API + room management
✓ Share by email/link
✓ Analytics & Reports
✓ Gentree document tree API
✓ Integrations: Calendar, ReadLater, Cloud storage (GDrive/Dropbox/OneDrive)

**Remaining gaps** (see GAP_ANALYSIS.md):
1. `/integrations/v2/messaging/{}/message` — Messaging integration not implemented
2. `/integrations/v2/storage/` vs `/cloud/` — Possible path naming mismatch

**Status:** Should work for sync/pairing. Messaging integrations won't work.
Full verification against live tablet pending.

## Known open items
- Server runs from a shell/session; no systemd unit yet (stops when session ends).
- Handwriting recognition uses Tesseract (weak on cursive/maths). TrOCR (~300 MB)
  would improve convert + search.
- Wi-Fi sync blocked by "Shibam Guest" client isolation; USB only for now.
- MQTT broker uses simple self-hosted vernemq; no clustering.
