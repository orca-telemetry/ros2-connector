#!/usr/bin/env bash

# --- Configuration (TOKEN is templated by the server) ---
TOKEN="{{TOKEN}}"
BASE_URL="https://github.com/orca-telemetry/ros2-connector/releases/download/v0.0.0"
BINARY_NAME="orca"
INSTALL_DIR="$HOME/.local/bin"
SERVICE_NAME="orca-listen"
mkdir -p "$INSTALL_DIR"

set -euo pipefail

# Detect architecture and select the correct binary
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)         BINARY_SUFFIX="amd64" ;;
    aarch64|arm64)  BINARY_SUFFIX="arm64" ;;
    armv7l)         BINARY_SUFFIX="armv7" ;;
    *)
        echo "Error: Unsupported architecture: $ARCH" >&2
        exit 1
        ;;
esac

BINARY_URL="${BASE_URL}/${BINARY_NAME}-${BINARY_SUFFIX}"
echo "Detected architecture: ${ARCH} (using binary suffix: ${BINARY_SUFFIX})" >&2

# Determine ROS version + Source setup
if [[ -z "${ROS_DISTRO:-}" ]]; then
    echo "Scanning for ROS 2 installation..." >&2
    for _d in /opt/ros/jazzy /opt/ros/humble /opt/ros/rolling; do
        if [[ -f "${_d}/setup.bash" ]]; then
            set +u
            source "${_d}/setup.bash"
            set -u
            echo "Sourced ${_d}" >&2
            break
        fi
    done
fi

if [[ -z "${ROS_VERSION:-}" ]]; then
    echo "Error: ROS 2 environment not detected. Please install ROS 2 or source it manually." >&2
    exit 1
fi

# 2. Download the binary (always fetch latest)
TARGET_PATH="${INSTALL_DIR}/${BINARY_NAME}"
echo "Downloading ${BINARY_NAME}..." >&2
curl -L -sS -o "$TARGET_PATH" "$BINARY_URL"
chmod +x "$TARGET_PATH"

# Provision
FORCE_FLAG=""
if [[ -f "$HOME/.orca/id_ed25519" ]]; then
    echo "Existing keypair detected." >&2
    read -rp "Overwrite existing keys? This will re-provision the robot. [y/N] " answer </dev/tty
    if [[ "${answer,,}" == "y" ]]; then
        FORCE_FLAG="--force"
    else
        echo "Aborted." >&2
        exit 0
    fi
fi

echo "Starting provision step..."
if ! "$TARGET_PATH" provision -t "$TOKEN" $FORCE_FLAG; then
    echo "Error: Provisioning failed with exit code $?" >&2
    exit 1
fi

# Discover
echo "Starting discovery step..."
if ! "$TARGET_PATH" discover; then
    echo "Error: Discovery failed with exit code $?" >&2
    exit 1
fi

# Sync (retry every 5 seconds until success)
echo "Starting sync step..."
until "$TARGET_PATH" sync; do
    echo "Sync not ready, retrying in 5 seconds..." >&2
    sleep 5
done

# Install and start systemd service for `orca listen`
echo "Setting up systemd service..."

UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

cat > "${UNIT_DIR}/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Orca ROS 2 Listener
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=LD_LIBRARY_PATH=/opt/ros/jazzy/lib
ExecStart=${TARGET_PATH} listen
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now "${SERVICE_NAME}.service"

echo "Robot onboarded with Orca. Service '${SERVICE_NAME}' is running."
