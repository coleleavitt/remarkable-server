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
ssh $T 'mkdir -p /home/root/rm-proxy && cp -n /etc/hosts /home/root/rm-proxy/hosts.usb-backup'
scp rm-proxy/target/armv7-unknown-linux-musleabihf/release/rm-proxy certs/server.crt certs/server.key $T:/home/root/rm-proxy/
scp contrib/tablet/rm-proxy.service $T:/etc/systemd/system/
ssh $T 'chmod 600 /home/root/rm-proxy/server.key && systemctl daemon-reload && systemctl enable --now rm-proxy'
```

Then in the tablet's `/etc/hosts` replace `10.11.99.2` → `127.0.0.1` and
`10.11.99.3` (vernemq) → `127.0.0.2`, and `systemctl restart xochitl`.

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
