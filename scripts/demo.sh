#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

export CGO_ENABLED=0
export GOSUMDB=off
# If modules are already cached in an offline environment, uncomment:
# export GOPROXY=off

echo "==> Running Go tests"
go test ./...

echo "==> Building platform"
mkdir -p bin artifacts/results
go build -o bin/platform ./cmd/platform

echo "==> Running HTTP/process-kill/recovery end-to-end demo"
go run ./cmd/e2e \
  -platform-bin "${ROOT_DIR}/bin/platform" \
  -workdir "${ROOT_DIR}/artifacts/e2e-runtime" \
  -out "${ROOT_DIR}/artifacts/results/e2e-result.json"

echo "==> Demo report: artifacts/results/e2e-result.json"
