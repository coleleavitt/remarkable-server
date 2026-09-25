#!/bin/sh
# /etc/letsencrypt/renewal-hooks/deploy/remarkable-server.sh
# Copies the renewed cert where the unprivileged service can read it, then
# restarts the service so the screenshare broker picks it up. nginx reads the
# live/ path directly and reloads through certbot's own nginx hook.
set -e
case "$RENEWED_LINEAGE" in */remarkable.unwrap.rs) ;; *) exit 0 ;; esac
install -d -o root -g remarkable -m 0750 /etc/remarkable-server/tls
install -o root -g remarkable -m 0640 "$RENEWED_LINEAGE/fullchain.pem" /etc/remarkable-server/tls/fullchain.pem
install -o root -g remarkable -m 0640 "$RENEWED_LINEAGE/privkey.pem"   /etc/remarkable-server/tls/privkey.pem
systemctl try-restart remarkable-server.service
systemctl reload nginx
