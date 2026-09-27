---
name: linode-deploy
description: Deploy remarkable-server master to the live Linode (the tablet's source of truth) with a backup first and post-deploy verification. Use only when the user asks to deploy.
---

# Deploy remarkable-server to the Linode

Only when explicitly asked. The server holds the tablet's only live copy of its documents.

## Preconditions
- Everything to ship is merged to `master`, and the `CI` workflow on that commit is green
  (`gh run list --branch master --limit 3`).
- Host packages present (see DEPLOYMENT.md "Host packages"): sqlite3, tesseract-ocr, WeasyPrint venv.

## Build (clean worktree, exact lock)
```sh
git -C ~/RustProjects/tools/remarkable-server fetch -q
git -C ~/RustProjects/tools/remarkable-server worktree add --detach ../rms-wt-deploy origin/master
cd ../rms-wt-deploy
cargo zigbuild --release --locked --target x86_64-unknown-linux-gnu.2.39 --features vendored-openssl --target-dir target-linode
```
(Native builds link a newer glibc than the Linode's 2.39.)

## Install (backup first)
```sh
TS=$(date -u +%Y%m%dT%H%M%SZ)
scp target-linode/x86_64-unknown-linux-gnu/release/remarkable-server linode:/opt/remarkable-server/bin/remarkable-server.new
ssh linode "set -e; systemctl stop remarkable-server
  tar -C /var/lib -czf /var/backups/remarkable-server/rms-backup-$TS.tgz remarkable-server
  cp -p /opt/remarkable-server/bin/remarkable-server /opt/remarkable-server/bin/remarkable-server.bak-$TS
  cp -p /etc/remarkable-server/env /etc/remarkable-server/env.bak-$TS
  install -m 755 /opt/remarkable-server/bin/remarkable-server.new /opt/remarkable-server/bin/remarkable-server
  rm /opt/remarkable-server/bin/remarkable-server.new
  systemctl start remarkable-server; sleep 5; systemctl is-active remarkable-server"
scp linode:/var/backups/remarkable-server/rms-backup-$TS.tgz ~/remarkable-backups/
```
Add new env vars (if a release needs them) after backing up the env file, only if absent.

## Verify
```sh
curl -s https://remarkable.unwrap.rs/health            # OK
ssh linode "sqlite3 /var/lib/remarkable-server/sync.db 'select generation from root';
  sqlite3 /var/lib/remarkable-server/devices.db 'select device_id, last_refresh from devices';
  journalctl -u remarkable-server --since '-3min' --no-pager -o cat | grep -iE 'error|warn|panic|Devices:|Listening'"
```
Expect `Devices: 2 registered` (tablet `RM110-…` + viewer), no errors, and the tablet's next
sync/notification connection to succeed (generation advances on its next write).

## Rollback
```sh
ssh linode 'cp /opt/remarkable-server/bin/remarkable-server.bak-$TS /opt/remarkable-server/bin/remarkable-server && systemctl restart remarkable-server'
```
Storage is forward-compatible for one step back (root.json mirror); restore the tarball only if
data itself is damaged (stop the service first).

## After
Remove the deploy worktree (`git worktree remove --force ../rms-wt-deploy`).
