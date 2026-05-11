#!/bin/bash
set -e

# hxの偷吃 — Child Node Auto-Installer
# Usage: curl -sL .../install.sh | bash -s -- --mother wss://host/ws --key xxx

MOTHER_URL=""
PSK=""
BIN_DIR="/usr/local/bin"
SERVICE_NAME="hx-snack-child"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mother) MOTHER_URL="$2"; shift 2 ;;
    --key) PSK="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [ -z "$MOTHER_URL" ]; then
  echo "Usage: $0 --mother wss://host/ws [--key xxx]"
  exit 1
fi

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
  x86_64) ARCH="amd64" ;;
  aarch64 | arm64) ARCH="arm64" ;;
  *) echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

echo "[install] OS=$OS ARCH=$ARCH"
echo "[install] Mother=$MOTHER_URL"

# Download latest binary
REPO="huanxherta/hx-snack"
DOWNLOAD_URL="https://github.com/$REPO/releases/latest/download/child-${OS}-${ARCH}"

echo "[install] Downloading $DOWNLOAD_URL..."
curl -sL "$DOWNLOAD_URL" -o /tmp/hx-snack-child
chmod +x /tmp/hx-snack-child

# Install
sudo mv /tmp/hx-snack-child "$BIN_DIR/hx-snack-child"

# Create systemd service
sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=hxの偷吃 Child Node
After=network.target

[Service]
Type=simple
ExecStart=$BIN_DIR/hx-snack-child -mother "$MOTHER_URL" -key "$PSK"
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME"

echo "[install] Child installed and started!"
echo "[install] Check status: systemctl status $SERVICE_NAME"
echo "[install] Logs: journalctl -u $SERVICE_NAME -f"