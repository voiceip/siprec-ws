#!/bin/bash
set -e
mkdir -p /var/lib/siprec-ws-bridge/recordings
chown voicebot:voicebot /var/lib/siprec-ws-bridge/recordings
# Cron-based cleanup (optional): install tmpreaper and set CLEANUP_ENABLED=false to use it instead of in-process cleanup
if [ -f /etc/cron.d/siprec-ws-bridge-cleanup ]; then
    chmod 644 /etc/cron.d/siprec-ws-bridge-cleanup
fi
systemctl daemon-reload
systemctl enable siprec-ws-bridge.service
