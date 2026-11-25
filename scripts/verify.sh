#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p bin artifacts

echo "==> go version"
go version

echo "==> go test ./..."
go test ./...

echo "==> go vet ./..."
go vet ./...

echo "==> build local executable"
go build -o bin/drift-engine ./cmd/drift-engine

echo "==> cross-build windows/amd64"
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/drift-engine-windows-amd64.exe ./cmd/drift-engine

WORK_DIR="${WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/drift-engine-verify.XXXXXX")}"
REPORT_PATH="${REPORT_PATH:-artifacts/verification-report.json}"
mkdir -p "$(dirname "$REPORT_PATH")" "$WORK_DIR"

echo "==> deterministic acceptance checks"
echo "    workdir: $WORK_DIR"
echo "    report:  $REPORT_PATH"
./bin/drift-engine verify \
  -exe "$ROOT_DIR/bin/drift-engine" \
  -workdir "$WORK_DIR" \
  -report "$REPORT_PATH"

echo "==> verification passed"
