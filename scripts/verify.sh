#!/usr/bin/env bash
# Reproducible end-to-end verification with the fixed-seed sample:
# full verification, incremental verification, divergence location and
# hard-kill recovery, each executed exactly once.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== go build ./... =="
go build ./...

echo "== go test ./... =="
go test ./...

echo "== end-to-end verification (fixed seed) =="
go run ./cmd/verifyrun
