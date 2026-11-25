#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export GOPROXY=off GOSUMDB=off
go test ./... -count=1
mkdir -p bin
go build -o bin/recallab ./cmd/recallab
echo "All tests and build completed."
