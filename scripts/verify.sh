#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Go version"
go version

echo "==> go test ./..."
go test ./... -count=1

mkdir -p bin .runtime artifacts
echo "==> Build bin/edge-dist"
go build -o bin/edge-dist ./cmd/edge-dist

echo "==> Deterministic end-to-end verification"
./bin/edge-dist verify \
  --samples samples \
  --db .runtime/verify.db \
  --output artifacts/verification-report.json \
  --force-kill=true
