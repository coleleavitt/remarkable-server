# Remote deployment (Linode) and tablet connection options

The server runs on the Linode at **remarkable.unwrap.rs**. The tablet has
xochitl hard-coded to talk to `*.remarkable.com` hosts, so something must
redirect those names to our server. Two supported ways:

| | A. `rm-proxy` on the tablet (recommended) | B. `/etc/hosts` → Linode directly |
|---|---|---|
| How | hosts → 127.0.0.1, local proxy terminates TLS with our self-signed `*.remarkable.com` cert, re-encrypts to remarkable.unwrap.rs with a real Let's Encrypt cert | hosts → Linode public IP; Linode presents the self-signed `*.remarkable.com` cert |
| Linode cert | Let's Encrypt only | Self-signed CA cert must be served for `*.remarkable.com` names |
| Survives IP change | yes (resolves by DNS) | no (IP hardcoded in hosts) |
| Upstream verified | yes (webpki roots) | n/a |
| Extra on tablet | 1.1 MB static binary + systemd unit | none |
| Linode nginx file | `nginx-remarkable.unwrap.rs.conf` | `nginx-remarkable-direct.conf` |

This is the same model rmfakecloud uses (`rmfakecloud-proxy`).

## Current live state (as of 2026-09-25)

**Approach A is live.** The tablet syncs over WiFi to the Linode; USB and the
local dev server are no longer involved.

```
xochitl ──/etc/hosts──▶ 127.0.0.1:443 / 127.0.0.2:443  (rm-proxy on tablet, self-signed *.remarkable.com cert)
        ──verified TLS──▶ remarkable.unwrap.rs:443  (nginx, Let's Encrypt) ──▶ 127.0.0.1:3100 remarkable-server (HTTP)
                     └──▶ remarkable.unwrap.rs:8883 (screenshare MQTT broker, Let's Encrypt, direct)
```

- **The Linode is the single source of truth.** Storage was migrated from the
  local `test-storage/` at generation 21 together with `devices.db` and
  `jwt_secret`, so the tablet's existing pairing kept working (no re-pair).
- **Do not run the local server against a copy of the same storage while the
  tablet points at the Linode.** Two servers moving `root.json` independently =
  split-brain. To go back to local, copy Linode storage back first (reverse
  rsync), then revert the tablet's hosts file.
- DNS: Cloudflare `remarkable.unwrap.rs` A `172.232.15.166`, AAAA
  `2600:3c06::f03c:95ff:fe86:3ab6`, **DNS-only (grey cloud)**.
- Cert: Let's Encrypt via certbot `dns-cloudflare`, auto-renews (certbot timer);
  the deploy hook copies it for the MQTT broker and restarts the service.
- Tablet: `/home/root/rm-proxy/{rm-proxy,server.crt,server.key,hosts.usb-backup}`,
  `/etc/systemd/system/rm-proxy.service` (enabled).

## Security model

- Every API route requires a valid device JWT except health, discovery,
  and pairing (which needs a one-time code from `--pair`). The legacy v1/v2
  blob handlers in `protocol.rs` were unauthenticated until commit `a59cf70`
  ("protocol: require device auth on legacy … handlers"); they now use the same `auth_user`
  check as v3.
- JWTs are signed with a per-install random secret in `<storage>/jwt_secret`
  (created 0600 on first start), not a hardcoded default. Deleting it
  invalidates every token → re-pair the tablet.
- `ADMIN_TOKEN` (64 hex, only in `/etc/remarkable-server/env`, 0640 root:remarkable)
  guards admin endpoints.
- Upstream TLS from the tablet relay is verified against webpki roots, so a
  MITM on the WiFi can't impersonate the Linode.

## Operations cheat sheet

```sh
# health / status
curl https://remarkable.unwrap.rs/health
ssh linode 'systemctl status remarkable-server; journalctl -u remarkable-server -n 50 --no-pager'
ssh linode 'grep generation /var/lib/remarkable-server/root.json'
ssh root@<tablet> 'systemctl status rm-proxy; journalctl -u rm-proxy -n 30 --no-pager'
ssh root@<tablet> 'journalctl -u xochitl --since -10min | grep -iE "sync|401|notif"'

# backup Linode storage
ssh linode 'tar -C /var/lib -czf - remarkable-server' > rms-backup-$(date +%F).tgz

# after a tablet software update (/etc may be reset)
#   re-check /etc/hosts entries and that the local CA is still trusted,
#   then re-apply the hosts edit above and `systemctl enable --now rm-proxy`.
```

### Troubleshooting

- **"Not syncing" after switch-over**: check that rm-proxy is running and that a
  `curl --resolve … 127.0.0.1` from the tablet returns 200. If it returns 401 for
  authed calls, `jwt_secret`/`devices.db` weren't migrated together → re-pair.
- **Tablet asleep = no traffic**: normal; it reconnects on wake.
- **BusyBox gotchas** on the tablet: no `cp -n`, no `ss`, no `timeout`; the stock
  `wget` can't do modern TLS (use `curl`).
- **Building for the Linode**: native `cargo build` on Arch links glibc
  newer than 2.39 and won't run there; always use the zigbuild command above.

## Hardening (nginx probe blocking + fail2ban)

The Linode is on the public internet, so it gets a constant stream of scanners.
Checked on 2026-09-25: ~5.3k probe requests from ~100 IPs in one day, none of which got through
(every reMarkable endpoint returned 401 without a valid device identifier). The layers below cut the noise and ban repeat offenders.

### 1. nginx drops probe paths (`contrib/linode/nginx-block-probes.conf`)

Installed as `/etc/nginx/snippets/block-probes.conf` and `include`d in every
`server {}` block (both :80 and :443) of `unwrap.rs` and `remarkable.unwrap.rs`.
Requests for `/.env`, `/.git/…`, `wp-*`, `phpmyadmin`, `*.php`, etc. get
`return 444`: nginx closes the connection without replying.

- `/.well-known/` is **not** blocked (certbot HTTP-01 and other tools need it).
- No real reMarkable endpoint matches any of these patterns (checked against the
  full access log before enabling).

```sh
sudo cp contrib/linode/nginx-block-probes.conf /etc/nginx/snippets/block-probes.conf
# add inside each server {} block:
#   include /etc/nginx/snippets/block-probes.conf;
sudo nginx -t && sudo systemctl reload nginx
curl -s -o /dev/null -w '%{http_code}\n' https://remarkable.unwrap.rs/.env    # 000 (dropped)
curl -s -o /dev/null -w '%{http_code}\n' https://remarkable.unwrap.rs/health  # 200
```

### 2. fail2ban

Files:

| repo | installed as |
|---|---|
| `contrib/linode/fail2ban-jail.local` | `/etc/fail2ban/jail.d/unwrap.local` |
| `contrib/linode/fail2ban-filter-nginx-probe.conf` | `/etc/fail2ban/filter.d/nginx-probe.conf` |
| `contrib/linode/fail2ban-filter-nginx-4xx-flood.conf` | `/etc/fail2ban/filter.d/nginx-4xx-flood.conf` |

Jails:

| jail | triggers on | threshold | ban |
|---|---|---|---|
| `sshd` | failed SSH auth | 5 / 10 min | 1 h |
| `nginx-probe` | requests for probe paths (the 444s above) | 3 / 10 min | 24 h |
| `nginx-4xx-flood` | any 401/403/404 | 40 / 10 min | 1 h |
| `recidive` | IPs banned repeatedly | 3 bans / 1 day | 1 week, all ports |

Why these thresholds: the tablet produces at most ~2 4xx responses per 10 minutes
(a 401 when its auth data expires, right before it refreshes). Scanners do 60–700.
40 leaves a wide margin.

**`ignoreip` must list:**
- your own admin/home IPs, and
- **all Cloudflare ranges**. `unwrap.rs` is Cloudflare-proxied, so nginx sees a Cloudflare
  edge IP as the client for it. Banning one of those takes the site down for everyone.
  Get the current list from https://www.cloudflare.com/ips-v4 and `/ips-v6`.

`remarkable.unwrap.rs` is DNS-only (not proxied), so bans on it hit the real client IP.

```sh
sudo apt install fail2ban
sudo cp contrib/linode/fail2ban-jail.local /etc/fail2ban/jail.d/unwrap.local   # then fill in ignoreip
sudo cp contrib/linode/fail2ban-filter-nginx-probe.conf /etc/fail2ban/filter.d/nginx-probe.conf
sudo cp contrib/linode/fail2ban-filter-nginx-4xx-flood.conf /etc/fail2ban/filter.d/nginx-4xx-flood.conf
sudo fail2ban-client -t && sudo systemctl enable --now fail2ban

# check filters against the real log before trusting them
sudo fail2ban-regex /var/log/nginx/access.log /etc/fail2ban/filter.d/nginx-probe.conf
# status / unban
sudo fail2ban-client status nginx-probe
sudo fail2ban-client set nginx-probe unbanip 1.2.3.4
```

If you lock yourself out of SSH: use the Linode web console (Lish) and run
`fail2ban-client unban --all`.

### Not changed (on purpose)

sshd still has `PasswordAuthentication yes` and `PermitRootLogin yes`. Every successful
login in the logs used a key, so switching to `PasswordAuthentication no` and
`PermitRootLogin prohibit-password` would be a safe next step. It was left alone
to avoid locking anyone out without an explicit decision.

## Linode side (shared by both)

Layout:

- `/opt/remarkable-server/bin/remarkable-server` — built with
  `cargo zigbuild --release --target x86_64-unknown-linux-gnu.2.39 --features vendored-openssl --target-dir target-linode`
  (Linode is Ubuntu 24.04, glibc 2.39; OpenSSL vendored so only libc is needed).
- `/etc/remarkable-server/env` — from `contrib/linode/remarkable-server.env.example`
  (`ADMIN_TOKEN` is random 64 hex; never commit).
- `/var/lib/remarkable-server` — storage (blobs, `root.json`, `devices.db`, `jwt_secret`).
- systemd: `contrib/linode/remarkable-server.service` (runs as `remarkable` user).
- The HTTP API listens on `127.0.0.1:3100` in plain HTTP; **nginx terminates TLS** on :443.
- The screenshare message queue broker listens directly on `0.0.0.0:8883` with TLS
  (`SCREENSHARE_CERT`/`SCREENSHARE_KEY` → a copy of the Let's Encrypt cert,
  refreshed by `contrib/linode/certbot-deploy-hook.sh`). nginx can't
  HTTP-proxy message queue, so it's a separate port (ufw: allow 8883/tcp).
- Cert: `certbot certonly --dns-cloudflare -d remarkable.unwrap.rs`
  (DNS-only A/AAAA records in Cloudflare → Linode; not proxied, because
  Cloudflare's proxy would break message queue and long-lived sockets).

Deploy/update:

```sh
scp target-linode/x86_64-unknown-linux-gnu/release/remarkable-server linode:/opt/remarkable-server/bin/
ssh linode systemctl restart remarkable-server
curl https://remarkable.unwrap.rs/health
```

Migrating storage from a local server: stop both, `rsync -a test-storage/ linode:/var/lib/remarkable-server/`,
`chown -R remarkable:remarkable`, start. `jwt_secret` + `devices.db` must go
together or the tablet's identifiers stop validating (re-pair with `--pair`).

Browser screen share viewer (optional):

The server can join the tablet's screen share itself and show it at
`https://remarkable.unwrap.rs/screenshare/view` (sign in with `ADMIN_TOKEN`;
the cookie is HttpOnly/Secure/SameSite=Strict). A session with the tablet only
runs while a browser is watching. Frames come straight from the tablet to the
Linode over WebRTC, so the Linode must accept UDP on the viewer's port range:

```sh
# /etc/remarkable-server/env
SCREENSHARE_VIEWER=1
SCREENSHARE_VIEWER_UDP_PORTS=50000-50100

sudo ufw allow 50000:50100/udp comment 'screenshare viewer WebRTC'
```

Usage: `GET /screenshare/view/usage` (signed in) lists the viewer's recent
sessions and the tablet's own screen share telemetry. All telemetry the
tablet posts (`/v1/reports`, `/analytics/v2/events`, …) is kept in
`reports.jsonl` in the storage directory (bodies capped at 64 KiB, rotated
at 8 MiB); `GET /admin/reports?contains=…&limit=…` with `x-admin-token`
lists it.

It follows the tablet on either broker: the MQTT one (`SCREENSHARE_BIND`) or
the REST rooms (`/screenshare/v1`, xochitl 3.27+/3.28).

Pairing code on the Linode:

```sh
ssh linode 'sudo -u remarkable /opt/remarkable-server/bin/remarkable-server --storage /var/lib/remarkable-server --pair'
```

## A. Tablet proxy (`rm-proxy/`)

Small static Rust binary (tokio + rustls, ~1.1 MB armv7 musl):

```sh
cd rm-proxy
cargo +stable zigbuild --release --target armv7-unknown-linux-musleabihf
```

Each `--route LISTEN=UPSTREAM` accepts TLS on LISTEN with `--cert/--key`
(our self-signed `*.remarkable.com` cert, CA already trusted on the tablet),
then opens a verified TLS connection to UPSTREAM and pipes bytes both ways.

Install on tablet:

```sh
T=root@10.11.99.1
# BusyBox cp has no -n; only back up if no backup exists yet
ssh $T 'mkdir -p /home/root/rm-proxy && [ -e /home/root/rm-proxy/hosts.usb-backup ] || cp /etc/hosts /home/root/rm-proxy/hosts.usb-backup'
scp rm-proxy/target/armv7-unknown-linux-musleabihf/release/rm-proxy certs/server.crt certs/server.key $T:/home/root/rm-proxy/
scp contrib/tablet/rm-proxy.service $T:/etc/systemd/system/
ssh $T 'chmod 600 /home/root/rm-proxy/server.key && systemctl daemon-reload && systemctl enable --now rm-proxy'
```

Test the relay before touching `/etc/hosts` (from the tablet):

```sh
curl -sk --resolve internal.cloud.remarkable.com:443:127.0.0.1 https://internal.cloud.remarkable.com/health   # 200
```

Then repoint `/etc/hosts` and restart xochitl:

```sh
sed -i -e 's/^10\.11\.99\.2\([[:space:]]\)/127.0.0.1\1/' \
       -e 's/^10\.11\.99\.3\([[:space:]]\)/127.0.0.2\1/' /etc/hosts
systemctl restart xochitl
```

Routes in the unit:
- `127.0.0.1:443 → remarkable.unwrap.rs:443` (HTTP API via nginx)
- `127.0.0.2:443 → remarkable.unwrap.rs:8883` (screenshare message queue)

Revert to USB/local server: `cp /home/root/rm-proxy/hosts.usb-backup /etc/hosts; systemctl restart xochitl`.
Note: `/etc` changes may be lost on a firmware update; re-apply after updates.

## B. Direct `/etc/hosts` → Linode

1. On the Linode, install `contrib/linode/nginx-remarkable-direct.conf`
   and copy the self-signed `certs/server.crt`/`server.key` to
   `/etc/remarkable-server/tablet-cert/`. This vhost matches the
   `*.remarkable.com` server names and serves the self-signed cert.
2. Screenshare: point `SCREENSHARE_CERT/KEY` at the self-signed cert and
   forward 443→8883 for the vernemq name, or accept that screenshare needs
   option A (443 is taken by nginx; message queue can't share it without the
   nginx `stream` module + `ssl_preread`).
3. Tablet `/etc/hosts`: every `10.11.99.2`/`10.11.99.3` → `172.232.15.166`.

Downsides: IP is hard-coded; the Linode publicly serves a cert claiming
`*.remarkable.com` (only trusted by our tablet, but still odd); screenshare
is awkward. Use A unless you can't run a binary on the tablet.
