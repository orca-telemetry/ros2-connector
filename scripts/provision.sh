#!/usr/bin/env bash

# --- Configuration (TOKEN is templated by the server) ---
TOKEN="{{TOKEN}}"
VERSION="{{VERSION}}"
BASE_URL="https://github.com/orca-telemetry/ros2-connector/releases/download/${VERSION}"
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

cleanup_services() {
    for svc in orca-listen orca-stream; do
        systemctl --user disable --now "${svc}.service" 2>/dev/null || true
        rm -f "$HOME/.config/systemd/user/${svc}.service"
    done
    systemctl --user daemon-reload 2>/dev/null || true
}

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

cleanup_services

# Check if binary already exists and prompt to overwrite
TARGET_PATH="${INSTALL_DIR}/${BINARY_NAME}"
if [[ -f "$TARGET_PATH" ]]; then
    read -rp "Orca binary already exists at ${TARGET_PATH}. Overwrite? (recommended) [Y/n] " answer </dev/tty
    answer="${answer:-Y}"
    if [[ "${answer,,}" != "y" ]]; then
        echo "Skipping download, using existing binary." >&2
    else
        echo "Downloading ${BINARY_NAME}..." >&2
        curl -L -sS -o "$TARGET_PATH" "$BINARY_URL" < /dev/null
        chmod +x "$TARGET_PATH"
    fi
else
    echo "Downloading ${BINARY_NAME}..." >&2
    curl -L -sS -o "$TARGET_PATH" "$BINARY_URL" < /dev/null
    chmod +x "$TARGET_PATH"
fi

# Provision
FORCE_FLAG=""
if [[ -f "$HOME/.orca/id_ed25519" ]]; then
    echo "Existing keypair detected." >&2
    read -rp "Overwrite existing keys? This will re-provision the robot. [y/N] " answer </dev/tty
    if [[ "${answer,,}" == "y" ]]; then
        FORCE_FLAG="--force"
        rm -f "$HOME/.orca/config.json" "$HOME/.orca/collector.json"
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
    EXIT_CODE=$?
    if [[ $EXIT_CODE -eq 2 ]]; then
        echo "Sync not ready, retrying in 5 seconds..." >&2
        sleep 5
    else
        echo "Error: Sync failed with exit code $EXIT_CODE" >&2
        exit 1
    fi
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

# Check cloud availability and optionally set up orca-stream service
echo "Checking cloud availability..."
STREAM_SERVICE_NAME="orca-stream"

# exit code 1 = cloud available, exit code 0 = not available
# We need to suppress set -e for this check since we're branching on exit code
set +e
"$TARGET_PATH" cloud_available
CLOUD_EXIT=$?
set -e

if [[ $CLOUD_EXIT -eq 1 ]]; then
    echo "Cloud is available. Setting up ${STREAM_SERVICE_NAME} service..." >&2

    cat > "${UNIT_DIR}/${STREAM_SERVICE_NAME}.service" <<EOF
[Unit]
Description=Orca ROS 2 Stream
After=network-online.target ${SERVICE_NAME}.service
Wants=network-online.target
Requires=${SERVICE_NAME}.service

[Service]
Type=simple
Environment=LD_LIBRARY_PATH=/opt/ros/jazzy/lib
ExecStart=${TARGET_PATH} stream
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF

    systemctl --user daemon-reload
    systemctl --user enable --now "${STREAM_SERVICE_NAME}.service"
    echo "Robot onboarded with Orca. Services '${SERVICE_NAME}' and '${STREAM_SERVICE_NAME}' are running."

elif [[ $CLOUD_EXIT -eq 0 ]]; then
    echo "Cloud is not available. Skipping ${STREAM_SERVICE_NAME} service." >&2
    echo "Robot onboarded with Orca. Service '${SERVICE_NAME}' is running."

else
    echo "Error: cloud_available exited with unexpected code ${CLOUD_EXIT}." >&2
    exit 1
fi
