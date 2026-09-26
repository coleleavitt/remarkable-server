# remarkable-server Setup Guide

Complete guide to running your own reMarkable sync server over USB.

## Prerequisites

- reMarkable 2 tablet (tested with software 3.3.2, should work with 3.x)
- Linux host with USB connection to tablet
- Rust toolchain (`cargo`)
- OpenSSL (for certificate generation)
- Optional: `tesseract` (`apt install tesseract-ocr`) for handwriting convert/search, `sqlite3` CLI for backups

## 1. Build the Server

```bash
cd ~/RustProjects/tools/remarkable-server
cargo build --release
```

## 2. Network Configuration (USB)

The tablet appears as a USB Ethernet device at `10.11.99.1`. Configure your host with static IPs on the USB interface:

```bash
# Allow binding to port 443 without root
echo 'net.ipv4.ip_unprivileged_port_start=443' | sudo tee /etc/sysctl.d/50-remarkable-server.conf
sudo sysctl -p /etc/sysctl.d/50-remarkable-server.conf

# Configure the USB interface (NetworkManager example)
nmcli connection modify "Wired connection 2" ipv4.addresses "10.11.99.2/29,10.11.99.3/29"
nmcli connection modify "Wired connection 2" ipv4.method manual
nmcli connection up "Wired connection 2"
```

| Address | Purpose |
|---------|---------|
| `10.11.99.1` | Tablet |
| `10.11.99.2` | Server API (sync, auth, etc.) |
| `10.11.99.3` | Screenshare broker |

## 3. Certificate Setup

The tablet validates TLS certificates against reMarkable's domains. You need a local CA that the tablet trusts.

### Generate Local CA

```bash
mkdir -p certs
cd certs

# Create CA key and cert
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
    -subj "/CN=Local reMarkable CA" -out ca.crt
```

### Generate Server Certificate

Create `server.conf`:
```ini
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = local.tectonic.remarkable.com

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = *.remarkable.com
DNS.2 = *.tectonic.remarkable.com
DNS.3 = *.cloud.remarkable.com
DNS.4 = remarkable.com
DNS.5 = webapp-production-dot-remarkable-production.appspot.com
DNS.6 = *.appspot.com
DNS.7 = *.cloud.remarkable.engineering
```

`*.cloud.remarkable.engineering` covers the screenshare broker name (`vernemq-prod.cloud.remarkable.engineering`,
see below) and, on newer firmware, `backtrace-proxy.cloud.remarkable.engineering`.

```bash
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -config server.conf
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 3650 -sha256 -extfile server.conf -extensions v3_req
```

### Generate Admin Token

```bash
openssl rand -hex 32 > admin-token
```

## 4. Tablet Configuration

SSH into the tablet (`ssh root@10.11.99.1`, default password on bottom of device):

### Install Local CA

```bash
mkdir -p /usr/local/share/ca-certificates
# Copy ca.crt from host to tablet
scp certs/ca.crt root@10.11.99.1:/usr/local/share/ca-certificates/local-remarkable-ca.crt
ssh root@10.11.99.1 update-ca-certificates
```

### Redirect reMarkable Domains to Local Server

Back up and edit `/etc/hosts` on the tablet:

```bash
ssh root@10.11.99.1

cp /etc/hosts /etc/hosts.bak
cat >> /etc/hosts << 'EOF'

# Local remarkable-server
10.11.99.2 my.remarkable.com
10.11.99.2 webapp-production-dot-remarkable-production.appspot.com
10.11.99.2 internal.cloud.remarkable.com
10.11.99.2 local.tectonic.remarkable.com
10.11.99.2 service-manager-production-dot-remarkable-production.appspot.com
10.11.99.2 hwr-production-dot-remarkable-production.appspot.com
10.11.99.3 vernemq-prod.cloud.remarkable.engineering
EOF
```

The screenshare broker name comes from the xochitl 3.3.2 binary, which dials
`vernemq-prod.cloud.remarkable.engineering:443` (`src/screenshare.rs`); remarkable-rs's notes on the
same build (`docs/MQTT_NOTES.md`) record the pattern `vernemq-%1.cloud.remarkable.engineering` and
reMarkable's discovery answering `mqttbroker: vernemq-prod.cloud.remarkable.engineering`. Earlier
versions of this guide listed `vernemq-prod-{1,2,3}.tectonic.remarkable.com` instead; nothing in the
repository or those notes supports them (extra lines are harmless). The production tablet's own
`/etc/hosts` is not in this repository, so which broker names it maps is not recorded here
(GAP_ANALYSIS.md, "Screen share broker hostname").

### Restart xochitl

```bash
systemctl restart xochitl
```

## 5. Run the Server

```bash
cd ~/RustProjects/tools/remarkable-server

RUST_LOG=remarkable_server=info \
SCREENSHARE_BIND=10.11.99.3:443 \
ADMIN_TOKEN="$(cat certs/admin-token)" \
./target/release/remarkable-server \
    --bind 10.11.99.2:443 \
    --storage ./remarkable-storage \
    --cert certs/server.crt \
    --key certs/server.key
```

## 6. Pair the Tablet

If this is a fresh setup (tablet was signed out or never paired):

```bash
./target/release/remarkable-server --pair
```

This prints a one-time code (or `POST /devices/v1` with `x-admin-token` returns one). On the tablet, go to **Settings > Account > Connect** and enter the code.

## 7. Verify Sync

Once paired, the tablet should sync automatically. You'll see:
- Generation numbers incrementing in server logs
- Documents appearing in `remarkable-storage/`
- `New notifications WebSocket connection` when the tablet opens its notifications channel
  (`/notifications/ws/json/1`). The server sends no WebSocket pings of its own, and pings from the
  tablet are only logged at debug (`RUST_LOG=remarkable_server=info,remarkable_server::notifications=debug`).

The tablet gets sync pushes over `/notifications/ws/json/1`. The MQTT-over-WebSocket `/mqtt` endpoint
is off unless you set `MQTT_WS_NOTIFICATIONS=1`; the tablet does not use it (GAP_ANALYSIS.md, "MQTT:
what the tablet actually uses"). Its observed MQTT traffic is screen share signalling on `SCREENSHARE_BIND`;
whether it also subscribes to sync topics there is unconfirmed.

## Troubleshooting

### "Not syncing" badges on home screen

This is normal for documents not opened in the last hour. reMarkable uses a lazy-sync system where inactive documents show this badge. Opening a document updates its `lastOpened` timestamp and clears the badge.

To fix all documents at once, update their `lastOpened` timestamps:
```bash
# On the tablet
NOW=$(echo $(($(date +%s) * 1000)))
cd /home/root/.local/share/remarkable/xochitl
for f in *.metadata; do
    sed -i "s/\"lastOpened\": *\"[0-9]*\"/\"lastOpened\": \"$NOW\"/" "$f"
    sed -i "s/\"lastOpened\": *[0-9]*/\"lastOpened\": $NOW/" "$f"
done
systemctl restart xochitl
```

### TLS close_notify warnings

The server logs may show `DEBUG` messages about "WebSocket closed without TLS close_notify". This is harmless — Qt's socket cleanup sometimes calls `abort()` instead of `close()` when the socket isn't in `UnconnectedState`. The server handles this gracefully.

### No connections after tablet sleep

The server waits for the USB interface to come back. When the tablet wakes, connections resume automatically.

## Storage Layout

```
remarkable-storage/
├── sync.db                # SQLite: root hash/generation (source of truth), blob index
├── root.json              # Human-readable mirror of the root
├── devices.db, jwt_secret # Pairing state
├── .uploads/              # Staging for streamed uploads
├── aa/                    # Content-addressed blob storage
│   └── bb...              # First 2 chars of hash = directory
├── bb/
│   └── cc...
└── ...
```

Each document is a tree of blobs referenced by SHA-256 hash. `sync.db` holds the current root hash, which points to the document index. Back it up with `sqlite3 sync.db ".backup '…'"`, not a live `cp`.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Logging level (e.g., `remarkable_server=debug`) |
| `ADMIN_TOKEN` | Token for admin endpoints (see README "Admin endpoints") |
| `SCREENSHARE_BIND` | Address for screenshare broker |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` | Email sharing (optional) |
| `PUBLIC_URL` | Public base URL for the OAuth verification link and firmware downloads (optional) |
| `FIRMWARE_ARCHIVE` | Path to OTA update files (optional) |
| `CRASH_DIR`, `CRASH_MAX_TOTAL_BYTES`, `CRASH_MAX_REPORTS` | Crash-report storage and quota (default `./crash-dumps`, 512 MiB, 200) |

## Security Notes

- The server stores your notebooks in plain files. **Back up `remarkable-storage/`**.
- The admin token mints pairing codes and user tokens, approves passcode resets and OAuth sign-ins, and can wipe or garbage-collect storage. Keep `certs/admin-token` secret.
- TLS certificates are self-signed but trusted by your tablet only.

## Remote deployment

For running the server on a VPS (remarkable.unwrap.rs) and the two ways to point the tablet at it (on-tablet `rm-proxy` vs. direct `/etc/hosts`), see [DEPLOYMENT.md](DEPLOYMENT.md).

## Working on remarkable-rs at the same time

The `remarkable-*` crates come from `coleleavitt/remarkable-rs` as git dependencies pinned to a commit
(see `Cargo.toml`). To build against a local checkout instead, add an uncommitted override in
`.cargo/config.toml`:

```toml
[patch."https://github.com/coleleavitt/remarkable-rs"]
remarkable-lines = { path = "../remarkable/crates/remarkable-lines" }
remarkable-core = { path = "../remarkable/crates/remarkable-core" }
remarkable-screenshare = { path = "../remarkable/crates/remarkable-screenshare" }
remarkable-mqtt = { path = "../remarkable/crates/remarkable-mqtt" }
```

To move to a newer remarkable-rs, push it, then bump the `rev` in `Cargo.toml` and run `cargo update`.
