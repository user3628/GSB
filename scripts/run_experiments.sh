#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[1/3] go mod download"
go mod download

echo "[2/3] go test ./..."
go test ./...

echo "[3/3] reproducible anomaly experiment"
go run ./cmd/experiment -root "$ROOT" -out artifacts/experiment

echo "summary: $ROOT/artifacts/experiment/summary.json"
