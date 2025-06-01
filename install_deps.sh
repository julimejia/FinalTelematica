#!/bin/bash

sudo dnf install -y python3.11 python3.11-pip

set -euo pipefail

REQUIREMENTS_FILE="/home/hadoop/requirements.txt"
S3_REQUIREMENTS_PATH="s3://ha-doop/deps/requirements.txt"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Download requirements.txt
log "Downloading requirements.txt..."
aws s3 cp "$S3_REQUIREMENTS_PATH" "$REQUIREMENTS_FILE"

# Install system dependencies
log "Installing system tools..."
sudo yum install -y python3-devel gcc

# Install Python packages (locally for hadoop user)
log "Installing Python packages..."
pip3 install --user -r "$REQUIREMENTS_FILE"

log "Bootstrap completed successfully."
exit 0