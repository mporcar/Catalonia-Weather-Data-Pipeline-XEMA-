#!/bin/bash

# setup_auth.sh
# Reconstructs Google Cloud Server Account key from GitHub Codespaces secret
# Load .env if exists
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

if [ -n "$GCP_SA_KEY" ]; then
    echo "$GCP_SA_KEY" > /tmp/gcp-key.json
    echo 'export GOOGLE_APPLICATION_CREDENTIALS="/tmp/gcp-key.json"' >> ~/.bashrc
    echo "GCP Auth configured successfully."
else
    echo "GCP_SA_KEY environment variable is not set."
fi
