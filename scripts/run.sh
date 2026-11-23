#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
go run ./cmd/cdnlab serve --config samples/config.json --listen "${LISTEN:-127.0.0.1:18080}" "$@"
