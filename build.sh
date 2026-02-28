#!/bin/bash
set -e

CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o bin/siprec-ws-bridge .

# package using fpm

fpm --force -s dir -t deb -n siprec-ws-bridge -v 0.1.0 \
    --description "SIPREC WebSocket bridge" \
    --depends tmpreaper \
    --config-files /etc/siprec-ws-bridge/siprec-ws-bridge.env \
    --after-install deb/postinst.sh \
    --after-remove deb/postrm.sh \
    ./bin/siprec-ws-bridge=/usr/local/bin/siprec-ws-bridge \
    deb/siprec-ws-bridge.service=/etc/systemd/system/siprec-ws-bridge.service \
    deb/siprec-ws-bridge.env=/etc/siprec-ws-bridge/siprec-ws-bridge.env \
    deb/siprec-ws-bridge-cleanup.cron=/etc/cron.d/siprec-ws-bridge-cleanup




