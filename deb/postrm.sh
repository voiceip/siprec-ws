#!/bin/bash
set -e
systemctl daemon-reload
# Remove cron job on purge
if [ "$1" = "purge" ]; then
    rm -f /etc/cron.d/siprec-ws-bridge-cleanup
fi
