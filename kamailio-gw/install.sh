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

echo "==> Deploying kamailio.cfg..."
cp "${SCRIPT_DIR}/kamailio.cfg" /etc/kamailio/kamailio.cfg
chmod 644 /etc/kamailio/kamailio.cfg

echo "==> Enabling and starting Kamailio service..."
systemctl daemon-reload
systemctl enable kamailio
systemctl restart kamailio

echo ""
echo "Done. Kamailio is running."
echo "  Status : systemctl status kamailio"
echo "  Logs   : journalctl -u kamailio -f"
echo "  Config : /etc/kamailio/kamailio.cfg"
