#!/usr/bin/env bash
# Tear down the full pipeline stack, optionally wiping volumes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Stopping all services..."
docker compose down

if [[ "${1:-}" == "--volumes" ]]; then
    echo "==> Removing volumes (all CDC state will be lost)..."
    docker compose down -v
    echo "    Volumes removed."
fi

echo "Done."
