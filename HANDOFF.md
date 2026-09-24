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
1. ~~Messaging integration~~ — DONE (4c1cbfe)
2. ~~Storage path alias~~ — DONE (4c1cbfe); /cloud and /storage now share one IntegrationState
3. ~~OTA update module orphaned~~ — DONE: `src/firmware.rs` now compiled + mounted at
   `/firmware/v1/{versions,devices,check,changelog,download/{version}}` when
   `FIRMWARE_ARCHIVE=<dir>` is set (uses PUBLIC_URL for download links).
   Download supports HTTP Range (206/416) for resumable fetches. Verified live against
   the local archive (5 device types, rm2 3.20→3.29 check, byte-exact ranges).

4. ~~Deltas / changelog / diff3 merge / service files / TrOCR hook~~ — DONE (this commit)

**Status:** Should work for sync/pairing. All 3.29 endpoints covered.
Full verification against live tablet pending.

## Known open items
- Service files: `contrib/openrc/remarkable-server` (Gentoo/OpenRC, this box) and
  `contrib/systemd/remarkable-server.service` + `contrib/remarkable-server.env.example`.
  Not installed — see contrib/README.md.
- Handwriting: pluggable. Set `HANDWRITING_OCR_CMD="python3 contrib/trocr_ocr.py"` to use
  TrOCR (needs `pip install transformers torch pillow`; falls back to tesseract otherwise).
  Default remains built-in tesseract.
- OTA deltas: served only if files named `<from>_to_<to>.delta|.bin` exist under
  `<archive>/<device>/deltas/`. No real deltas in the local archive (reMarkable ships full .swu).
- OTA changelogs: read from `<archive>/changelogs/[<device>/]<version>.md|.txt`; synthetic
  text if absent. Local archive has none.
- Wi-Fi sync blocked by "Shibam Guest" client isolation; USB only for now.
- Real-tablet end-to-end verification still pending (needs the device).

## Recent Fixes (2026-09-24)

### TLS close_notify Warning Fix
- **Server-side**: Added `rustls=error,tokio_rustls=error` to tracing filter
  (src/main.rs) to suppress rustls warnings.
- **notifications.rs**: Close_notify errors are detected and logged as DEBUG
  instead of WARN. Clean closes remain INFO.
- xochitl binary patching was attempted but failed (disk space, crashes) —
  server-side fix is the correct approach.

### Commit History
- `9c4da7f`: Downgrade close_notify to DEBUG in notifications
- `e34c02c`: Suppress rustls TLS warnings, add xochitl patch script

## Tablet Setup (Quick Reference)

1. **hosts file** — Edit `/etc/hosts` on tablet:
   ```
   10.11.99.2 local.tectonic.remarkable.com
   10.11.99.2 my.remarkable.com
   10.11.99.2 webapp-production-dot-remarkable-production.appspot.com
   10.11.99.3 vernemq-prod.us-west-2.remarkable.engineering
   ```

2. **CA Certificate** — Copy your CA to tablet:
   ```bash
   scp certs/ca.crt root@10.11.99.1:/usr/local/share/ca-certificates/remarkable-local-ca.crt
   # Then append to bundle:
   ssh root@10.11.99.1 'cat /usr/local/share/ca-certificates/remarkable-local-ca.crt >> /etc/ssl/certs/ca-certificates.crt'
   ```

3. **Pair device** — Run server with `--pair`, enter code on tablet.

4. **Verify** — After xochitl restart, check:
   ```bash
   ss -tnp | grep 10.11.99  # Should show ESTAB connections
   ```

## Current Status
- Generation: 11
- Files: 613
- Storage: ~122 MB
- Server: mon5 on 10.11.99.2:443 + 10.11.99.3:443
