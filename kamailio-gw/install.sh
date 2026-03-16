#!/bin/bash
# install.sh – Deploy Kamailio SIP filtering gateway on a Debian/Ubuntu VM.
#
# Prerequisites:
#   - siprec-ws-bridge is already installed and running on port 5080
#     (set "sip_ports": [5080] in /etc/siprec-ws-bridge/config.json)
#   - Run as root
#
# After running this script, Kamailio will own port 5060 and forward
# allowed SIPREC INVITEs to siprec-ws-bridge on 127.0.0.1:5080.
#
# To edit filter rules:  $EDITOR /etc/kamailio/kamailio.cfg
# To apply changes:      systemctl restart kamailio
# To view logs:          journalctl -u kamailio -f

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Kamailio..."
apt-get update -q
apt-get install -y kamailio

echo "==> Validating new kamailio.cfg..."
TMP_CFG="$(mktemp /tmp/kamailio.cfg.XXXXXX)"
cp "${SCRIPT_DIR}/kamailio.cfg" "${TMP_CFG}"
if ! kamailio -c -f "${TMP_CFG}" 2>&1; then
    rm -f "${TMP_CFG}"
    echo "ERROR: kamailio config check failed – aborting deployment." >&2
    exit 1
fi
rm -f "${TMP_CFG}"

echo "==> Deploying kamailio.cfg..."
LIVE_CFG="/etc/kamailio/kamailio.cfg"
BACKUP_CFG="${LIVE_CFG}.bak"
if [[ -f "${LIVE_CFG}" ]]; then
    cp "${LIVE_CFG}" "${BACKUP_CFG}"
fi
if ! cp "${SCRIPT_DIR}/kamailio.cfg" "${LIVE_CFG}"; then
    [[ -f "${BACKUP_CFG}" ]] && cp "${BACKUP_CFG}" "${LIVE_CFG}"
    echo "ERROR: failed to install config – original restored." >&2
    exit 1
fi
chmod 644 "${LIVE_CFG}"

echo "==> Enabling and starting Kamailio service..."
systemctl daemon-reload
systemctl enable kamailio
systemctl restart kamailio

echo ""
echo "Done. Kamailio is running."
echo "  Status : systemctl status kamailio"
echo "  Logs   : journalctl -u kamailio -f"
echo "  Config : /etc/kamailio/kamailio.cfg"
