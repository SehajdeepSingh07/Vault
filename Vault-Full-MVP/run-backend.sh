#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
export VAULT_ADDR="127.0.0.1:8080"
export VAULT_NODE_ID="node-1"
export VAULT_NODE_ADDRESS="http://127.0.0.1:8080"
export VAULT_DATA_DIR="$ROOT/backend/data"
export VAULT_CORS_ORIGINS="http://127.0.0.1:3000,http://localhost:3000"
export VAULT_INTERNAL_TOKEN="dev-only-change-me"
export VAULT_REPLICATION_FACTOR="1"
export VAULT_WRITE_QUORUM="1"
export VAULT_READ_QUORUM="1"
exec "$ROOT/backend/bin/vaultd"
